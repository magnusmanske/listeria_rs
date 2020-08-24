#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod configuration;
pub mod listeria_page;
pub mod listeria_list;
pub mod render_wikitext;
pub mod render_tabbed_data;
pub mod result_cell;
pub mod result_row;
pub mod column;

pub use crate::configuration::Configuration;
pub use crate::listeria_page::ListeriaPage;
pub use crate::listeria_list::ListeriaList;
pub use crate::render_wikitext::RendererWikitext;
pub use crate::render_tabbed_data::RendererTabbedData;
pub use crate::result_row::ResultRow;
pub use crate::result_row::*;
pub use crate::column::*;
use tokio::sync::Mutex;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use wikibase::entity::EntityTrait;
use wikibase::mediawiki::api::Api;

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
    local_file_namespace_prefix: String,
}

impl PageParams {
    pub async fn new ( config: Arc<Configuration>, mw_api: Arc<Mutex<Api>>, page: String ) -> Result<Self,String> {
        let api = mw_api.lock().await;
        let ret = Self {
            wiki: api.get_site_info_string("general", "wikiid")?.to_string(),
            page,
            language: api.get_site_info_string("general", "lang")?.to_string(),
            mw_api: mw_api.clone(),
            wb_api: config.get_default_wbapi().await?,
            simulate: false,
            simulated_text: None,
            simulated_sparql_results: None,
            config: config.clone(),
            local_file_namespace_prefix: api.get_canonical_namespace_name(6).unwrap_or("File").to_string()
        } ;
        Ok(ret)
    }

    pub fn local_file_namespace_prefix(&self) -> &String {
        &self.local_file_namespace_prefix
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
                Regex::new(r#"^https{0,1}://[^/]+/entity/([A-Z]\d+)$"#).unwrap();
            static ref RE_FILE: Regex =
                Regex::new(r#"^https{0,1}://[^/]+/wiki/Special:FilePath/(.+?)$"#)
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

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending
}

impl SortOrder {
    pub fn new(os: Option<&String>) -> Self {
        match os {
            Some(s) => {
                if s.to_uppercase().trim() == "DESC" {
                    Self::Descending
                } else {
                    Self::Ascending
                }
            }
            None => Self::Ascending
        }
    }
}

#[derive(Debug, Clone)]
pub struct TemplateParams {
    links: LinksType,
    sort: SortMode,
    section: SectionType,
    min_section:u64,
    row_template: Option<String>,
    header_template: Option<String>,
    autodesc: Option<String>,
    summary: Option<String>,
    skip_table: bool,
    wdedit: bool,
    references: bool,
    one_row_per_item: bool,
    sort_order: SortOrder,
    wikibase: String,
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
            section: SectionType::None,
            min_section:2,
            row_template: None,
            header_template: None,
            autodesc: None,
            summary: None,
            skip_table: false,
            wdedit: false,
            references: false,
            one_row_per_item: false,
            sort_order: SortOrder::Ascending,
            wikibase: String::new(),
         }
    }

    pub fn new_from_params(template:&Template) -> Self {
        Self {
            links:LinksType::All,
            sort: SortMode::new(template.params.get("sort")),
            section: SectionType::new_from_string_option(template.params.get("section")),
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
            sort_order: SortOrder::new(template.params.get("sort_order")),
            wikibase: template.params.get("wikibase").map(|s|s.trim().to_uppercase()).unwrap_or("wikidata".to_string()) , // TODO config
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
    pub fn new_from_string_option(s: Option<&String>) -> Self {
        let s = match s {
            Some(s) => s,
            None => return Self::None,
        };
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
