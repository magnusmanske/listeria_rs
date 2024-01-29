use crate::*;
use std::{fs::File, io::BufReader, path::Path};
use anyhow::{Result,anyhow};
use serde_json::Value;
use wikibase::EntityTrait;

#[derive(Debug, Clone)]
pub enum NamespaceGroup {
    All,            // All namespaces forbidden
    List(Vec<i64>), // List of forbidden namespaces
}

impl NamespaceGroup {
    pub fn can_edit_namespace(&self, nsid: i64) -> bool {
        match self {
            Self::All => false,
            Self::List(list) => nsid >= 0 && !list.contains(&nsid),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Configuration {
    namespace_blocks: HashMap<String, NamespaceGroup>,
    default_api: String,
    prefer_preferred: bool,
    default_language: String,
    template_start_sites: HashMap<String, String>,
    template_end_sites: HashMap<String, String>,
    location_templates: HashMap<String, String>,
    shadow_images_check: Vec<String>,
    default_thumbnail_size: Option<u64>,
    location_regions: Vec<String>,
    default_wiki_api_url: String,
    mysql: Option<Value>,
    oauth2_token: String,
    template_start_q: String,
    j: Value,
}

impl Configuration {
    pub async fn new_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let j = serde_json::from_reader(reader)?;
        Self::new_from_json(j).await
    }

    pub async fn new_from_json(j: Value) -> Result<Self> {
        let mut ret: Self = Default::default();

        if let Some(s) = j["default_api"].as_str() {
            ret.default_api = s.to_string()
        }
        if let Some(s) = j["default_language"].as_str() {
            ret.default_language = s.to_string()
        }
        if let Some(b) = j["prefer_preferred"].as_bool() {
            ret.prefer_preferred = b
        }
        if let Some(i) = j["default_thumbnail_size"].as_u64() {
            ret.default_thumbnail_size = Some(i)
        }
        if let Some(sic) = j["shadow_images_check"].as_array() {
            ret.shadow_images_check = sic
                .iter()
                .map(|s| s.as_str().expect("shadow_images_check needs to be a string").to_string())
                .collect()
        }
        if let Some(lr) = j["location_regions"].as_array() {
            ret.location_regions = lr.iter().map(|s| s.as_str().expect("location_regions needs to be a string").to_string()).collect()
        }
        if let Some(s) = j["wiki_login"]["token"].as_str() {
            ret.oauth2_token = s.to_string()
        }
        if j["mysql"].is_object() {
            ret.mysql = Some(j["mysql"].to_owned());
        }

        // Location template patterns
        if let Some(o) = j["location_templates"].as_object() {
            for (k, v) in o.iter() {
                if let (k, Some(v)) = (k.as_str(), v.as_str()) {
                    ret.location_templates.insert(k.to_string(), v.to_string());
                }
            }
        }

        // Namespace blocks on wikis
        if let Some(o) = j["namespace_blocks"].as_object() {
            for (k, v) in o.iter() {
                // Check for string value ("*")
                if let Some(s) = v.as_str() {
                    if s == "*" {
                        // All namespaces
                        ret.namespace_blocks
                            .insert(k.to_string(), NamespaceGroup::All);
                    } else {
                        return Err(anyhow!("Unrecognized string value for namespace_blocks[{k}]:{v}"));
                    }
                }

                // Check for array of integers
                if let Some(a) = v.as_array() {
                    let nsids: Vec<i64> = a
                        .iter()
                        .filter_map(|v| v.as_u64())
                        .map(|x| x as i64)
                        .collect();
                    ret.namespace_blocks
                        .insert(k.to_string(), NamespaceGroup::List(nsids));
                }
            }
        }

        let default_wiki = &ret.default_api;
        ret.default_wiki_api_url = match j["apis"][default_wiki].as_str() {
            Some(url) => url.to_string(),
            None => return Err(anyhow!("No API URL for default wiki '{default_wiki}'")),
        };

        ret.j = j;

        Ok(ret)
    }

    pub async fn fill_template_info(&mut self, api_lock: &ApiLock) -> Result<()> {
        // Start/end template site/page mappings
        let q_start = match self.j["template_start_q"].as_str() {
            Some(q) => q.to_string(),
            None => return Err(anyhow!("No template_start_q in config")),
        };
        let q_end = match self.j["template_end_q"].as_str() {
            Some(q) => q.to_string(), //ret.template_end_sites = ret.get_template(q)?,
            None => return Err(anyhow!("No template_end_q in config")),
        };
        let entities = wikibase::entity_container::EntityContainer::new();
        entities
            .load_entities(&(*api_lock.read().await), &vec![q_start.clone(), q_end.clone()])
            .await
            .map_err(|e|anyhow!("{e}"))?;
        self.template_start_sites = self.get_sitelink_mapping(&entities, &q_start)?;
        self.template_end_sites = self.get_sitelink_mapping(&entities, &q_end)?;
        self.template_start_q = q_start;
        Ok(())
    }

    pub fn get_default_wiki_api_url(&self) -> &str {
        &self.default_wiki_api_url
    }

    pub fn oauth2_token(&self) -> &String {
        &self.oauth2_token
    }

    pub fn mysql(&self, key: &str) -> Value {
        match &self.mysql {
            Some(mysql) => mysql[key].to_owned(),
            None => Value::Null,
        }
    }

    fn get_sitelink_mapping(
        &self,
        entities: &wikibase::entity_container::EntityContainer,
        q: &str,
    ) -> Result<HashMap<String, String>> {
        let entity = entities
            .get_entity(q.to_owned())
            .ok_or(anyhow!("Entity {q} not found"))?;
        match entity.sitelinks() {
            Some(sl) => Ok(sl
                .iter()
                .map(|s| (s.site().to_owned(), s.title().to_owned()))
                .collect()),
            None => Err(anyhow!("No sitelink in {q}")),
        }
    }

    pub fn check_for_shadow_images(&self, wiki: &String) -> bool {
        self.shadow_images_check.contains(wiki)
    }

    pub fn get_local_template_title_start(&self, wiki: &str) -> Result<String> {
        let ret = self
            .template_start_sites
            .get(wiki)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Cannot find local start template"))?;
        match ret.split(':').last() {
            Some(x) => Ok(x.to_string()),
            None => Err(anyhow!("get_local_template_title_start: no match")),
        }
    }

    pub fn get_local_template_title_end(&self, wiki: &str) -> Result<String> {
        let ret = self
            .template_end_sites
            .get(wiki)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Cannot find local end template"))?;
        match ret.split(':').last() {
            Some(x) => Ok(x.to_string()),
            None => Err(anyhow!("get_local_template_title_end: no match")),
        }
    }

    pub fn can_edit_namespace(&self, wiki: &str, nsid: i64) -> bool {
        match self.namespace_blocks.get(wiki) {
            Some(nsg) => nsg.can_edit_namespace(nsid),
            None => true, // Default
        }
    }

    pub fn get_location_template(&self, wiki: &str) -> String {
        match self.location_templates.get(wiki) {
            Some(s) => s.to_owned(),
            None => self.location_templates.get(&"default".to_string()).map(|s|s.to_owned()).unwrap_or_default()
        }
    }

    pub fn get_template_start_q(&self) -> String {
        self.template_start_q.to_owned()
    }

    pub fn prefer_preferred(&self) -> bool {
        self.prefer_preferred
    }

    pub fn default_language(&self) -> &str {
        &self.default_language
    }

    pub fn default_thumbnail_size(&self) -> u64 {
        self.default_thumbnail_size.unwrap_or(128)
    }

    pub fn location_regions(&self) -> &Vec<String> {
        &self.location_regions
    }

    pub fn default_api(&self) -> &str {
        &self.default_api
    }

}
