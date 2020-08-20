#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod listeria_page;
pub mod listeria_list;
pub mod render_wikitext;
pub mod render_tabbed_data;
pub mod result_cell;
pub mod result_row;
pub mod column;

use tokio::sync::Mutex;
pub use crate::listeria_page::ListeriaPage;
pub use crate::listeria_list::ListeriaList;
pub use crate::render_wikitext::RendererWikitext;
pub use crate::render_tabbed_data::RendererTabbedData;
pub use crate::result_row::ResultRow;
pub use crate::result_row::*;
pub use crate::column::*;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use wikibase::entity::EntityTrait;
use wikibase::mediawiki::api::Api;

#[derive(Debug, Clone)]
pub enum NamespaceGroup {
    All, // All namespaces forbidden
    List(Vec<i64>), // List of forbidden namespaces
}

impl NamespaceGroup {
    pub fn can_edit_namespace(&self,nsid: i64) -> bool {
        match self {
            Self::All => false ,
            Self::List(list) => nsid>=0 && !list.contains(&nsid)
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Configuration {
    wb_apis: HashMap<String,String>,
    namespace_blocks: HashMap<String,NamespaceGroup>,
    default_api:String,
    prefer_preferred: bool,
}

impl Configuration {
    pub fn new_from_file<P: AsRef<Path>>(path: P) -> Result<Self,String> {
        let file = File::open(path).map_err(|e|format!("{:?}",e))?;
        let reader = BufReader::new(file);
        let j = serde_json::from_reader(reader).map_err(|e|format!("{:?}",e))?;
        Self::new_from_json(j)
    }

    pub fn can_edit_namespace(&self, wiki:&str, nsid:i64) -> bool {
        match self.namespace_blocks.get(wiki) {
            Some(nsg) => nsg.can_edit_namespace(nsid),
            None => true // Default
        }
    }

    pub fn new_from_json ( j:Value ) -> Result<Self,String> {
        let mut ret : Self = Default::default();

        if let Some(s) = j["default_api"].as_str() { ret.default_api = s.to_string() }

        // valid WikiBase APIs
        if let Some(o) = j["apis"].as_object() {
            for (k,v) in o.iter() {
                if let (k,Some(v)) = (k.as_str(),v.as_str()) {
                    ret.wb_apis.insert(k.to_string(),v.to_string());
                }
                
            }
        }

        // Namespace blocks on wikis
        if let Some(o) = j["namespace_blocks"].as_object() {
            for (k,v) in o.iter() {
                // Check for string value ("*")
                if let Some(s) = v.as_str() {
                    if s == "*" { // All namespaces
                        ret.namespace_blocks.insert(k.to_string(),NamespaceGroup::All);
                    } else {
                        return Err(format!("Unrecognized string value for namespace_blocks[{}]:{}",k,v));
                    }
                }

                // Check for array of integers
                if let Some(a) = v.as_array() {
                    let nsids : Vec<i64> = a.iter().filter_map(|v|v.as_u64()).map(|x|x as i64).collect();
                    ret.namespace_blocks.insert(k.to_string(),NamespaceGroup::List(nsids));
                }
            }
        }

        if let Some(b) = j["prefer_preferred"].as_bool() { ret.prefer_preferred = b }

        Ok(ret)
    }

    pub fn prefer_preferred(&self) -> bool {
        self.prefer_preferred
    }

    pub async fn get_default_wbapi(&self) -> Api {
        let url = match self.wb_apis.get(&self.default_api) {
            Some(url) => url.to_string(),
            None => "https://www.wikidata.org/w/api.php".to_string()
        };
        wikibase::mediawiki::api::Api::new(&url).await.unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct PageParams {
    pub language: String,
    pub wiki: String,
    pub page: String,
    pub mw_api: Arc<Mutex<Api>>,
    pub wb_api: Api,
    pub simulate: bool,
    pub simulated_text: Option<String>,
    pub simulated_sparql_results: Option<String>,
    pub config: Arc<Configuration>,
    template_title_start: Option<String>,
    template_title_end: Option<String>,
}

impl PageParams {
    pub async fn new ( config: Arc<Configuration>, mw_api: Arc<Mutex<Api>>, page: String ) -> Result<Self,String> {
        let api = mw_api.lock().await;
        let mut ret = Self {
            wiki: api.get_site_info_string("general", "wikiid")?.to_string(),
            page,
            language: api.get_site_info_string("general", "lang")?.to_string(),
            mw_api: mw_api.clone(),
            wb_api: config.get_default_wbapi().await,
            simulate: false,
            simulated_text: None,
            simulated_sparql_results: None,
            config: config.clone(),
            template_title_start: None,
            template_title_end: None,
        } ;
        ret.template_title_start = Some(ret.get_local_template_title("Q19860885").await?) ;
        ret.template_title_end = Some(ret.get_local_template_title("Q19860887").await?) ;
        Ok(ret)
    }

    pub fn local_file_namespace_prefix(&self) -> String {
        "File".to_string() // TODO
    }

    pub fn get_local_template_title_start(&self) -> Result<String,String> {
        match &self.template_title_start {
            Some(s) => return Ok(s.to_owned()) ,
            None => Err("Cannot find local start template".to_string())
        }
    }

    pub fn get_local_template_title_end(&self) -> Result<String,String> {
        match &self.template_title_end {
            Some(s) => return Ok(s.to_owned()) ,
            None => Err("Cannot find local end template".to_string())
        }
    }

    async fn get_local_template_title(&self,entity_id: &str) -> Result<String,String> {
        let entities = wikibase::entity_container::EntityContainer::new();
        entities.load_entities(&self.wb_api, &vec![entity_id.to_string()]).await.map_err(|e|e.to_string())?;
        let entity = entities.get_entity(entity_id.to_owned()).ok_or(format!("Entity {} not found",&entity_id))?;
        match entity.sitelinks() {
            Some(sl) => sl.iter()
                .filter(|s|*s.site()==self.wiki)
                .map(|s|s.title())
                .map(|s|wikibase::mediawiki::title::Title::new_from_full(s,&self.wb_api))
                .map(|t|t.pretty().to_string())
                .next()
                .ok_or(format!("No sitelink to {} in {}",&self.wiki,&entity_id)),
            None => Err(format!("No sitelink in {}",&entity_id))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LatLon {
    pub lat: f64,
    pub lon: f64,
}

impl LatLon {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SparqlValue {
    Entity(String),
    File(String),
    Uri(String),
    Time(String),
    Location(LatLon),
    Literal(String),
}

impl SparqlValue {
    pub fn new_from_json(j: &Value) -> Option<Self> {
        lazy_static! {
            static ref RE_ENTITY: Regex =
                Regex::new(r#"^https{0,1}://www.wikidata.org/entity/([A-Z]\d+)$"#).unwrap();
            static ref RE_FILE: Regex =
                Regex::new(r#"^https{0,1}://commons.wikimedia.org/wiki/Special:FilePath/(.+?)$"#)
                    .unwrap();
            static ref RE_POINT: Regex =
                Regex::new(r#"^Point\((-{0,1}\d+[\.0-9]+) (-{0,1}\d+[\.0-9]+)\)$"#).unwrap();
        }
        let value = match j["value"].as_str() {
            Some(v) => v,
            None => return None,
        };
        match j["type"].as_str() {
            Some("uri") => match RE_ENTITY.captures(&value) {
                Some(caps) => Some(SparqlValue::Entity(
                    caps.get(1).unwrap().as_str().to_string(),
                )),
                None => match RE_FILE.captures(&value) {
                    Some(caps) => {
                        let file = caps.get(1).unwrap().as_str().to_string();
                        let file = urlencoding::decode(&file).ok()?;
                        let file = file.replace("_", " ");
                        Some(SparqlValue::File(file))
                    }
                    None => Some(SparqlValue::Uri(value.to_string())),
                },
            },
            Some("literal") => match j["datatype"].as_str() {
                Some("http://www.opengis.net/ont/geosparql#wktLiteral") => {
                    match RE_POINT.captures(&value) {
                        Some(caps) => {
                            let lat: f64 = caps.get(2)?.as_str().parse().ok()?;
                            let lon: f64 = caps.get(1)?.as_str().parse().ok()?;
                            Some(SparqlValue::Location(LatLon::new(lat, lon)))
                        }
                        None => None,
                    }
                }
                Some("http://www.w3.org/2001/XMLSchema#dateTime") => {
                    Some(SparqlValue::Time(value.to_string()))
                }
                None => Some(SparqlValue::Literal(value.to_string())),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Template {
    pub title: String,
    pub params: HashMap<String, String>,
}

impl Template {
    pub fn new_from_xml(node: &roxmltree::Node) -> Option<Self> {
        let mut title: Option<String> = None;

        let mut parts: HashMap<String, String> = HashMap::new();
        for n in node.children().filter(|n| n.is_element()) {
            if n.tag_name().name() == "title" {
                n.children().for_each(|c| {
                    let t = c.text().unwrap_or("").replace("_", " ");
                    let t = t.trim();
                    title = Some(t.to_string());
                });
            } else if n.tag_name().name() == "part" {
                let mut k: Option<String> = None;
                let mut v: Option<String> = None;
                n.children().for_each(|c| {
                    let tag = c.tag_name().name();
                    match tag {
                        "name" => {
                            let txt: Vec<String> = c
                                .children()
                                .map(|c| c.text().unwrap_or("").trim().to_string())
                                .collect();
                            let txt = txt.join("");
                            if txt.is_empty() {
                                if let Some(i) = c.attribute("index") { k = Some(i.to_string()) }
                            } else {
                                k = Some(txt);
                            }
                        }
                        "value" => {
                            let txt: Vec<String> = c
                                .children()
                                .map(|c| c.text().unwrap_or("").trim().to_string())
                                .collect();
                            v = Some(txt.join(""));
                        }
                        _ => {}
                    }
                });

                if let (Some(k), Some(v)) = (k, v) {
                    parts.insert(k, v);
                }
            }
        }

        match title {
            Some(t) => Some(Self {
                title: t,
                params: parts,
            }),
            None => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LinksType {
    All,
    Local,
    Red,
    RedOnly,
    Text,
    Reasonator,
}

impl LinksType {
    pub fn new_from_string(s: String) -> Self {
        match s.trim().to_uppercase().as_str() {
            "LOCAL" => Self::Local,
            "RED" => Self::Red,
            "RED_ONLY" => Self::RedOnly,
            "TEXT" => Self::Text,
            "REASONATOR" => Self::Reasonator,
            _ => Self::All, // Fallback, default
        }
    }
}

#[derive(Debug, Clone)]
pub enum SortMode {
    Label,
    FamilyName,
    Property(String),
    None,
}

impl SortMode {
    pub fn new(os: Option<&String>) -> Self {
        let os = os.map(|s| s.trim().to_uppercase());
        match os {
            Some(s) => match s.as_str() {
                "LABEL" => Self::Label,
                "FAMILY_NAME" => Self::FamilyName,
                prop => {
                    let re_prop = Regex::new(r"^P\d+$").unwrap();
                    if re_prop.is_match(prop) {
                        Self::Property(prop.to_string())
                    } else {
                        Self::None
                    }
                }
            },
            _ => Self::None,
        }
    }
}


#[derive(Debug, Clone)]
pub struct TemplateParams {
    links: LinksType,
    sort: SortMode,
    section: Option<String>, // TODO SectionType
    min_section:u64,
    row_template: Option<String>,
    header_template: Option<String>,
    autodesc: Option<String>,
    summary: Option<String>,
    skip_table: bool,
    wdedit: bool,
    references: bool,
    one_row_per_item: bool,
    sort_ascending: bool,
}

impl Default for TemplateParams {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateParams {
    pub fn new() -> Self {
         Self {
            links:LinksType::All,
            sort:SortMode::None,
            section: None,
            min_section:2,
            row_template: None,
            header_template: None,
            autodesc: None,
            summary: None,
            skip_table: false,
            wdedit: false,
            references: false,
            one_row_per_item: false,
            sort_ascending: true,
         }
    }

    pub fn new_from_params(template:&Template) -> Self {
        Self {
            links:LinksType::All,
            sort: SortMode::new(template.params.get("sort")),
            section: template.params.get("section").map(|s|s.trim().to_uppercase()),
            min_section: template
                            .params
                            .get("min_section")
                            .map(|s|
                                s.parse::<u64>().ok().or(Some(2)).unwrap_or(2)
                                )
                            .unwrap_or(2),
            row_template: template.params.get("row_template").map(|s|s.trim().to_string()),
            header_template: template.params.get("header_template").map(|s|s.trim().to_string()),
            autodesc: template.params.get("autolist").map(|s|s.trim().to_uppercase()).or_else(|| template.params.get("autodesc").map(|s|s.trim().to_uppercase())) ,
            summary: template.params.get("summary").map(|s|s.trim().to_uppercase()) ,
            skip_table: template.params.get("skip_table").is_some(),
            one_row_per_item: template.params.get("one_row_per_item").map(|s|s.trim().to_uppercase())!=Some("NO".to_string()),
            wdedit: template.params.get("wdedit").map(|s|s.trim().to_uppercase())==Some("YES".to_string()),
            references: template.params.get("references").map(|s|s.trim().to_uppercase())==Some("ALL".to_string()),
            sort_ascending: template.params.get("sort_order").map(|s|s.trim().to_uppercase())!=Some("DESC".to_string()),
        }
    }
}


#[derive(Debug, Clone)]
pub enum SectionType {
    None,
    Property(String),
    SparqlVariable(String),
}

impl SectionType {
    pub fn new_from_string(s: &str) -> Self {
        let s = s.trim();
        let re_prop = Regex::new(r"^[Pp]\d+$").unwrap();
        if re_prop.is_match(s) {
            return Self::Property(s.to_uppercase());
        }
        let re_sparql = Regex::new(r"^@.+$").unwrap();
        if re_sparql.is_match(s) {
            return Self::SparqlVariable(s.to_uppercase());
        }
        Self::None
    }
}

pub trait Renderer {
    fn new() -> Self ;
    fn render(&mut self,page:&ListeriaList) -> Result<String,String> ;
    fn get_new_wikitext(&self,wikitext: &str, page:&ListeriaPage ) -> Result<Option<String>,String> ;
}
