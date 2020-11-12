#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod configuration;
pub mod listeria_page;
pub mod listeria_list;
pub mod render_wikitext;
pub mod render_tabbed_data;
pub mod result_cell_part;
pub mod result_cell;
pub mod result_row;
pub mod column;
pub mod entity_container_wrapper;
pub mod reference;

use crate::column::*;
use crate::listeria_list::ListeriaList;
use crate::listeria_page::ListeriaPage;
use crate::configuration::Configuration;
use crate::render_wikitext::RendererWikitext;
use tokio::sync::RwLock;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use wikibase::entity::EntityTrait;
use wikibase::mediawiki::api::Api;
use regex::RegexBuilder;

#[derive(Debug, Clone)]
pub struct PageParams {
    language: String,
    wiki: String,
    page: String,
    mw_api: Arc<RwLock<Api>>,
    wb_api: Arc<Api>,
    simulate: bool,
    simulated_text: Option<String>,
    simulated_sparql_results: Option<String>,
    config: Arc<Configuration>,
    local_file_namespace_prefix: String,
}

impl PageParams {
    pub async fn new ( config: Arc<Configuration>, mw_api: Arc<RwLock<Api>>, page: String ) -> Result<Self,String> {
        let api = mw_api.read().await;
        let ret = Self {
            wiki: api.get_site_info_string("general", "wikiid")?.to_string(),
            page,
            language: api.get_site_info_string("general", "lang")?.to_string(),
            mw_api: mw_api.clone(),
            wb_api: config.get_default_wbapi()?.clone(),
            simulate: false,
            simulated_text: None,
            simulated_sparql_results: None,
            config: config.clone(),
            local_file_namespace_prefix: api.get_local_namespace_name(6).unwrap_or("File").to_string()
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
            static ref RE_DATE : Regex =
                Regex::new(r#"^([+-]{0,1}\d+-\d{2}-\d{2})T00:00:00Z$"#).unwrap();
        }
        let value = match j["value"].as_str() {
            Some(v) => v,
            None => return None,
        };
        match j["type"].as_str() {
            Some("uri") => match RE_ENTITY.captures(&value) {
                Some(caps) => match caps.get(1) {
                    Some(caps1) => {
                        Some(SparqlValue::Entity(
                            caps1.as_str().to_string(),
                        ))
                    }
                    None => None
                },
                None => match RE_FILE.captures(&value) {
                    Some(caps) => {
                        match caps.get(1) {
                            Some(caps1) => {
                                let file = caps1.as_str().to_string();
                                let file = urlencoding::decode(&file).ok()?;
                                let file = file.replace("_", " ");
                                Some(SparqlValue::File(file))
                            }
                            None => None
                        }
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
                    let time = value.to_string() ;
                    let time = match RE_DATE.captures(&value) {
                        Some(caps) => {
                            let date: String = caps.get(1)?.as_str().to_string();
                            date
                        }
                        None => time,
                    };
                    Some(SparqlValue::Time(time))
                }
                _ => Some(SparqlValue::Literal(value.to_string())),
            },
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Template {
    pub title: String,
    pub params: HashMap<String, String>,
}

impl Template {
    pub fn new_from_params(title: String, text: String) -> Self {
        let mut curly_braces = 0 ;
        let mut parts : Vec<String> = vec![] ;
        let mut part : Vec<char> = vec![] ;
        text
            .chars()
            .for_each(|c|{
                match c {
                    '{' => { curly_braces += 1 ; part.push(c); }
                    '}' => { curly_braces -= 1 ; part.push(c); }
                    '|' => {
                        if curly_braces == 0 {
                            parts.push ( part.iter().collect() ) ;
                            part.clear() ;
                        } else {
                            part.push(c);
                        }
                    }
                    _ => { part.push(c); }
                }
                });
        parts.push ( part.into_iter().collect() ) ;
        
        let params : HashMap<String,String> = parts
            .iter()
            .filter_map(|part|{
                let pos = part.find('=')?;
                let k = part.get(0..pos)?.trim().to_string();
                let v = part.get(pos+1..)?.trim().to_string();
                Some((k,v))
            })
            .collect();
        Self {
            title,
            params,
        }
    }

    pub fn fix_values(&mut self) {
        self.params = self.params.iter().map(|(k,v)|{
            (k.to_owned(),v.replace("{{!}}","|"))
        }).collect();
        // TODO proper template replacement
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
    SparqlVariable(String),
    None,
}

impl SortMode {
    pub fn new(os: Option<&String>) -> Self {
        lazy_static! {
            static ref RE_PROP : Regex = Regex::new(r"^P\d+$").unwrap();
            static ref RE_SPARQL : Regex = Regex::new(r"^?\S+$").unwrap();
        }
        let os = os.map(|s| s.trim().to_uppercase());
        match os {
            Some(s) => match s.as_str() {
                "LABEL" => Self::Label,
                "FAMILY_NAME" => Self::FamilyName,
                other => {
                    if RE_PROP.is_match(other) {
                        Self::Property(other.to_string())
                    } else if RE_SPARQL.is_match(other) {
                        Self::SparqlVariable(other[1..].to_string())
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

#[derive(Debug, Clone, PartialEq)]
pub enum ReferencesParameter {
    None,
    All
}

impl ReferencesParameter {
    pub fn new(os: Option<&String>) -> Self {
        match os {
            Some(s) => {
                if s.to_uppercase().trim() == "ALL" {
                    Self::All
                } else {
                    Self::None
                }
            }
            None => Self::None
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
    pub wdedit: bool,
    references: ReferencesParameter,
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
            references: ReferencesParameter::None,
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
            min_section: template.params.get("min_section").map(|s|s.parse::<u64>().ok().or(Some(2)).unwrap_or(2)).unwrap_or(2),
            row_template: template.params.get("row_template").map(|s|s.trim().to_string()),
            header_template: template.params.get("header_template").map(|s|s.trim().to_string()),
            autodesc: template.params.get("autolist").map(|s|s.trim().to_uppercase()).or_else(|| template.params.get("autodesc").map(|s|s.trim().to_uppercase())) ,
            summary: template.params.get("summary").map(|s|s.trim().to_uppercase()) ,
            skip_table: template.params.get("skip_table").is_some(),
            one_row_per_item: template.params.get("one_row_per_item").map(|s|s.trim().to_uppercase())!=Some("NO".to_string()),
            wdedit: template.params.get("wdedit").map(|s|s.trim().to_uppercase())==Some("YES".to_string()),
            references: ReferencesParameter::new(template.params.get("references")),
            sort_order: SortOrder::new(template.params.get("sort_order")),
            wikibase: template.params.get("wikibase").map(|s|s.trim().to_uppercase()).unwrap_or_else(|| "wikidata".to_string()) , // TODO config
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
        lazy_static! {
            static ref RE_PROP : Regex = Regex::new(r"^[Pp]\d+$").unwrap();
            static ref RE_PROP_NUM : Regex = Regex::new(r"^\d+$").unwrap(); // Yes people do that!
            static ref RE_SPARQL : Regex = Regex::new(r"^@.+$").unwrap();
        }
        let s = match s {
            Some(s) => s,
            None => return Self::None,
        };
        let s = s.trim();
        if RE_PROP.is_match(s) {
            return Self::Property(s.to_uppercase());
        }
        if RE_PROP_NUM.is_match(s) {
            return Self::Property(format!("P{}",&s));
        }
        if RE_SPARQL.is_match(s) {
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

#[derive(Debug, Clone)]
pub struct PageElement {
    before: String,
    template_start: String,
    inside: String,
    template_end: String,
    after: String,
    list: ListeriaList,
    is_just_text: bool,
}

impl PageElement {
    pub fn new_from_text(text: &str, page: &ListeriaPage) -> Option<Self> {
        let start_template = page
            .config()
            .get_local_template_title_start(&page.wiki()).ok()?;
        let end_template = page
            .config()
            .get_local_template_title_end(&page.wiki()).ok()?;
        let pattern_string_start = r#"\{\{([Ww]ikidata[ _]list|"#.to_string()
            + &start_template.replace(" ", "[ _]")
            + r#"[^\|]*)"#;
        let pattern_string_end = r#"\{\{([Ww]ikidata[ _]list[ _]end|"#.to_string()
            + &end_template.replace(" ", "[ _]")
            + r#")(\s*\}\})"#;
        let seperator_start: Regex = RegexBuilder::new(&pattern_string_start)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();
        let seperator_end: Regex = RegexBuilder::new(&pattern_string_end)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();

        let match_start = match seperator_start.find(&text) {
            Some(m) => m,
            None => return None
        };

        let (match_end,single_template) = match seperator_end.find(&text) {
            Some(m) => (m,false),
            None => (match_start,true) // No end template, could be tabbed data
        };

        let remaining = if single_template {
            String::from_utf8(text.as_bytes()[match_start.end()..].to_vec()).ok()?
        } else {
            String::from_utf8(text.as_bytes()[match_start.end()..match_end.start()].to_vec()).ok()?
        };
        let template_start_end_bytes = match Self::get_template_end(remaining) {
            Some(pos) => pos+match_start.end(),
            None => return None
        };
        let inside = if single_template { String::new() } else { String::from_utf8(text.as_bytes()[template_start_end_bytes..match_end.start()].to_vec()).ok()? } ;

        let template = Template::new_from_params("".to_string(),String::from_utf8(text.as_bytes()[match_start.end()..template_start_end_bytes-2].to_vec()).ok()?);

        Some ( Self {
            before:String::from_utf8(text.as_bytes()[0..match_start.start()].to_vec()).ok()?,
            template_start:String::from_utf8(text.as_bytes()[match_start.start()..template_start_end_bytes].to_vec()).ok()?,
            inside,
            template_end:if single_template { String::new() } else { String::from_utf8(text.as_bytes()[match_end.start()..match_end.end()].to_vec()).ok()? },
            after:String::from_utf8(text.as_bytes()[match_end.end()..].to_vec()).ok()?,
            list: ListeriaList::new(template,page.page_params()),
            is_just_text: false
        } )
    }

    pub fn new_just_text(text: &str, page: &ListeriaPage) -> Self {
        let template = Template { title:String::new(), params:HashMap::new() };
        Self {
            before:text.to_string(),
            template_start:String::new(),
            inside:String::new(),
            template_end:String::new(),
            after:String::new(),
            list: ListeriaList::new(template,page.page_params()),
            is_just_text: true
        }
    }

    pub fn get_and_clean_after(&mut self) -> String {
        let ret = self.after.clone() ;
        self.after = String::new();
        ret
    }

    pub fn new_inside(&self) -> Result<String,String> {
        match self.is_just_text {
            true => Ok(String::new()),
            false => {
                let mut renderer = RendererWikitext::new();
                renderer.render(&self.list)        
            }
        }
    }

    pub fn as_wikitext(&self) -> Result<String,String> {
        match self.is_just_text {
            true => Ok(self.before.clone()),
            false => Ok(self.before.clone() + &self.template_start + "\n" + &self.new_inside()? + "\n" + &self.template_end + &self.after),
        }
    }

    pub async fn process(&mut self) -> Result<(),String> {
        match self.is_just_text {
            true => Ok(()),
            false => self.list.process().await,
        }
    }

    pub fn is_just_text(&self) -> bool {
        self.is_just_text
    }

    fn get_template_end(text: String) -> Option<usize> {
        let mut pos : usize = 0 ;
        let mut curly_braces_open : usize = 2;
        let tv = text.as_bytes();
        while pos < tv.len() && curly_braces_open > 0 {
            match tv[pos] as char {
                '{' => curly_braces_open += 1 ,
                '}' => curly_braces_open -= 1 ,
                _ => {}
            }
            pos += 1 ;
        }
        if curly_braces_open == 0 { Some(pos) } else { None }
    }

}