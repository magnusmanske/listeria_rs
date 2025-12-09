use crate::database_pool::DatabasePool;
use crate::*;
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::time::Duration;
use std::{fs::File, io::BufReader, path::Path};
use wiki::Wiki;
use wikimisc::mediawiki::api::Api;
use wikimisc::wikibase::EntityTrait;
use wikimisc::wikibase::entity_container::EntityContainer;

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
    wb_apis: HashMap<String, Arc<Api>>,
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
    mysql: Option<Value>,
    oauth2_token: String,
    template_start_q: String,
    pattern_string_start: String,
    pattern_string_end: String,
    max_mw_apis_per_wiki: Option<usize>,
    max_mw_apis_total: Option<usize>,
    max_local_cached_entities: usize,
    max_concurrent_entry_queries: usize,
    api_timeout: u64,
    ms_delay_after_edit: Option<u64>,
    max_threads: usize,
    pool: Option<Arc<DatabasePool>>,
    max_sparql_simultaneous: u64,
    max_sparql_attempts: u64,
    profiling: bool,
    wikis: HashMap<String, Wiki>,
    is_single_wiki: bool,
    query_endpoint: Option<String>, // For single wiki mode, the SPARQL endpoint
    sparql_prefix: Option<String>,  // For single wiki mode, a prefix for all SPARQL queries
    main_item_prefix: String,       // For single wiki mode, the prefix for items
}

impl Configuration {
    pub async fn new_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let j = serde_json::from_reader(reader)?;
        Self::new_from_json(j).await
    }

    pub fn set_max_local_cached_entities(&mut self, max_local_cached_entities: usize) {
        self.max_local_cached_entities = max_local_cached_entities;
    }

    pub fn set_wikis(&mut self, wikis: HashMap<String, Wiki>) {
        self.wikis = wikis;
    }

    pub fn get_wiki(&self, wiki: &str) -> Option<&Wiki> {
        self.wikis.get(wiki)
    }

    pub async fn new_from_json(j: Value) -> Result<Self> {
        let mut ret: Self = Self {
            max_mw_apis_per_wiki: j["max_mw_apis_per_wiki"].as_u64().map(|u| u as usize),
            is_single_wiki: j["template_start"].as_str().is_some()
                && j["template_end"].as_str().is_some()
                && j["apis"]["wiki"].as_str().is_some(),
            ..Default::default()
        };
        ret.new_from_json_misc(&j);
        ret.new_from_json_locations(&j);
        ret.new_from_json_wikibase_apis(&j).await?;
        ret.new_from_json_namespace_blocks(&j)?;
        ret.new_from_json_start_end_tempate_mappings(&j).await?;
        if j["mysql"].as_object().is_some() {
            ret.pool = Some(Arc::new(DatabasePool::new(&ret)?));
        }
        Ok(ret)
    }

    pub fn query_endpoint(&self) -> Option<String> {
        self.query_endpoint.to_owned()
    }

    pub fn sparql_prefix(&self) -> Option<&str> {
        self.sparql_prefix.as_deref()
    }

    fn new_from_json_namespace_blocks(&mut self, j: &Value) -> Result<()> {
        // Namespace blocks on wikis
        if let Some(o) = j["namespace_blocks"].as_object() {
            for (k, v) in o.iter() {
                // Check for string value ("*")
                if let Some(s) = v.as_str() {
                    if s == "*" {
                        // All namespaces
                        self.namespace_blocks
                            .insert(k.to_string(), NamespaceGroup::All);
                    } else {
                        return Err(anyhow!(
                            "Unrecognized string value for namespace_blocks[{k}]:{v}"
                        ));
                    }
                }

                // Check for array of integers
                if let Some(a) = v.as_array() {
                    let nsids: Vec<i64> = a
                        .iter()
                        .filter_map(|v| v.as_u64())
                        .map(|x| x as i64)
                        .collect();
                    self.namespace_blocks
                        .insert(k.to_string(), NamespaceGroup::List(nsids));
                }
            }
        }
        Ok(())
    }

    pub fn max_sparql_attempts(&self) -> u64 {
        self.max_sparql_attempts
    }

    pub fn max_sparql_simultaneous(&self) -> u64 {
        self.max_sparql_simultaneous
    }

    pub fn profiling(&self) -> bool {
        self.profiling
    }

    pub fn set_profiling(&mut self, profiling: bool) {
        self.profiling = profiling;
    }

    pub fn pool(&self) -> &Arc<DatabasePool> {
        match &self.pool {
            Some(pool) => pool,
            None => panic!("Configuration::pool(): pool not defined"),
        }
    }

    pub fn max_threads(&self) -> usize {
        self.max_threads
    }

    pub fn ms_delay_after_edit(&self) -> Option<u64> {
        self.ms_delay_after_edit
    }

    pub fn api_timeout(&self) -> Duration {
        Duration::from_secs(self.api_timeout)
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

    pub fn max_local_cached_entities(&self) -> usize {
        self.max_local_cached_entities
    }

    fn get_sitelink_mapping(
        &self,
        entities: &EntityContainer,
        q: &str,
    ) -> Result<HashMap<String, String>> {
        let entity = entities
            .get_entity(q)
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
        match ret.split(':').next_back() {
            Some(x) => Ok(x.to_string()),
            None => Err(anyhow!("get_local_template_title_start: no match")),
        }
    }

    pub fn main_item_prefix(&self) -> String {
        self.main_item_prefix.to_owned()
    }

    pub fn get_max_mw_apis_per_wiki(&self) -> &Option<usize> {
        &self.max_mw_apis_per_wiki
    }

    pub fn get_max_mw_apis_total(&self) -> &Option<usize> {
        &self.max_mw_apis_total
    }

    pub fn get_local_template_title_end(&self, wiki: &str) -> Result<String> {
        let ret = self
            .template_end_sites
            .get(wiki)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Cannot find local end template"))?;
        match ret.split(':').next_back() {
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
            None => self
                .location_templates
                .get("default")
                .map(|s| s.to_owned())
                .unwrap_or_default(),
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

    pub async fn wbapi_login(&mut self, key: &str) -> bool {
        let oauth2_token = self.oauth2_token().to_owned();
        match self.wb_apis.get_mut(key) {
            Some(api) => {
                if let Some(api) = Arc::get_mut(api) {
                    api.set_oauth2(&oauth2_token);
                }
                true
            }
            None => false,
        }
    }

    pub fn get_wbapi(&self, key: &str) -> Option<&Arc<Api>> {
        self.wb_apis.get(key)
    }

    pub fn get_default_api(&self) -> &str {
        &self.default_api
    }

    pub fn get_default_wbapi(&self) -> Result<&Arc<Api>> {
        self.wb_apis
            .get(&self.default_api)
            .ok_or_else(|| anyhow!("No default API set in config file"))
    }

    pub fn pattern_string_start(&self) -> &str {
        &self.pattern_string_start
    }

    pub fn pattern_string_end(&self) -> &str {
        &self.pattern_string_end
    }

    pub fn is_single_wiki(&self) -> bool {
        self.is_single_wiki
    }

    async fn new_from_json_start_end_tempate_mappings(&mut self, j: &Value) -> Result<()> {
        // Try hardcoded first
        if let Some(template_start) = j["template_start"].as_str()
            && let Some(template_end) = j["template_end"].as_str()
        {
            self.template_start_sites
                .insert("wiki".to_string(), template_start.replace('_', " "));
            self.template_end_sites
                .insert("wiki".to_string(), template_end.replace('_', " "));
            return Ok(());
        }

        // Get list from central wikibase
        let api = self.get_default_wbapi()?;
        let q_start = match j["template_start_q"].as_str() {
            Some(q) => q.to_string(),
            None => return Err(anyhow!("No template_start_q in config")),
        };
        let q_end = match j["template_end_q"].as_str() {
            Some(q) => q.to_string(), //ret.template_end_sites = ret.get_template(q)?,
            None => return Err(anyhow!("No template_end_q in config")),
        };
        let to_load = vec![q_start.clone(), q_end.clone()];
        let entity_container = EntityContainer::new();
        if let Err(e) = entity_container.load_entities(api, &to_load).await {
            return Err(anyhow!("Error loading entities: {e}"));
        }

        self.template_start_sites = self.get_sitelink_mapping(&entity_container, &q_start)?;
        self.template_end_sites = self.get_sitelink_mapping(&entity_container, &q_end)?;
        self.template_start_q = q_start;
        Ok(())
    }

    async fn new_from_json_wikibase_apis(&mut self, j: &Value) -> Result<()> {
        self.oauth2_token = j["wiki_login"]["token"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if j["mysql"].is_object() {
            self.mysql = Some(j["mysql"].to_owned());
        }

        let oauth2_token = self.oauth2_token.to_owned();
        if let Some(o) = j["apis"].as_object() {
            for (k, v) in o.iter() {
                if let (name, Some(url)) = (k.as_str(), v.as_str()) {
                    let mut api = Api::new(url).await?;
                    api.set_oauth2(&oauth2_token);
                    self.wb_apis.insert(name.to_string(), Arc::new(api));
                }
            }
        }
        Ok(())
    }

    fn new_from_json_locations(&mut self, j: &Value) {
        // Location regions
        if let Some(lr) = j["location_regions"].as_array() {
            self.location_regions = lr
                .iter()
                .map(|s| {
                    s.as_str()
                        .expect("location_regions needs to be a string")
                        .to_string()
                })
                .collect()
        }

        // Location template patterns
        if let Some(o) = j["location_templates"].as_object() {
            for (k, v) in o.iter() {
                if let (k, Some(v)) = (k.as_str(), v.as_str()) {
                    self.location_templates.insert(k.to_string(), v.to_string());
                }
            }
        }
    }

    fn new_from_json_misc(&mut self, j: &Value) {
        self.max_mw_apis_total = j["max_mw_apis_total"].as_u64().map(|u| u as usize);
        self.default_api = j["default_api"].as_str().unwrap_or_default().to_string();
        self.query_endpoint = j["query_endpoint"].as_str().map(|s| s.to_string());
        self.default_language = j["default_language"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.prefer_preferred = j["prefer_preferred"].as_bool().unwrap_or_default();
        self.max_sparql_simultaneous = j["max_sparql_simultaneous"].as_u64().unwrap_or(10);
        self.max_sparql_attempts = j["max_sparql_attempts"].as_u64().unwrap_or(5);
        self.default_thumbnail_size = j["default_thumbnail_size"].as_u64();
        self.max_local_cached_entities =
            j["max_local_cached_entities"].as_u64().unwrap_or(5000) as usize;
        self.max_concurrent_entry_queries =
            j["max_concurrent_entry_queries"].as_u64().unwrap_or(5) as usize;
        self.api_timeout = j["api_timeout"].as_u64().unwrap_or(360);
        self.ms_delay_after_edit = j["ms_delay_after_edit"].as_u64();
        self.max_threads = j["max_threads"].as_u64().unwrap_or(8) as usize;
        self.profiling = j["profiling"].as_bool().unwrap_or_default();
        self.pattern_string_start = j["pattern_string_start"]
            .as_str()
            .unwrap_or(r#"\{\{(Wikidata[ _]list[^\|]*|"#)
            .to_string();
        self.pattern_string_end = j["pattern_string_start"]
            .as_str()
            .unwrap_or(r#"\{\{(Wikidata[ _]list[ _]end|"#)
            .to_string();
        self.main_item_prefix = j["main_item_prefix"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.sparql_prefix = j["sparql_prefix"].as_str().map(|s| s.to_string());
        if let Some(sic) = j["shadow_images_check"].as_array() {
            self.shadow_images_check = sic
                .iter()
                .map(|s| {
                    s.as_str()
                        .expect("shadow_images_check needs to be a string")
                        .to_string()
                })
                .collect()
        }
    }
}
