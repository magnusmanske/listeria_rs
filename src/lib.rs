#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod listeria_page;
pub mod listeria_list;
pub mod render_wikitext;
pub mod render_tabbed_data;
pub mod result_row;

pub use crate::listeria_page::ListeriaPage;
pub use crate::listeria_list::ListeriaList;
pub use crate::render_wikitext::RendererWikitext;
pub use crate::render_tabbed_data::RendererTabbedData;
pub use crate::result_row::ResultRow;
use regex::{Regex, RegexBuilder};
use roxmltree;
use serde_json::Value;
use std::collections::HashMap;
use urlencoding;
use wikibase::entity::EntityTrait;
use wikibase::mediawiki::api::Api;

#[derive(Debug, Clone)]
pub struct PageParams {
    pub language: String,
    pub wiki: String,
    pub page: String,
    pub mw_api: Api,
    pub wd_api: Api,
}

impl PageParams {
    pub fn local_file_namespace_prefix(&self) -> String {
        "File".to_string() // TODO
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

#[derive(Debug, Clone)]
pub enum ColumnType {
    Number,
    Label,
    LabelLang(String),
    Description,
    Item,
    Property(String),
    PropertyQualifier((String, String)),
    PropertyQualifierValue((String, String, String)),
    Field(String),
    Unknown,
}

impl ColumnType {
    pub fn new(s: &String) -> Self {
        lazy_static! {
            static ref RE_LABEL_LANG: Regex = RegexBuilder::new(r#"^label/(.+)$"#)
                .case_insensitive(true)
                .build()
                .unwrap();
            static ref RE_PROPERTY: Regex = Regex::new(r#"^([Pp]\d+)$"#).unwrap();
            static ref RE_PROP_QUAL: Regex =
                Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Pp]\d+)\s*$"#).unwrap();
            static ref RE_PROP_QUAL_VAL: Regex =
                Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Qq]\d+)\s*/\s*([Pp]\d+)\s*$"#).unwrap();
            static ref RE_FIELD: Regex = Regex::new(r#"^\?(.+)$"#).unwrap();
        }
        match s.to_lowercase().as_str() {
            "number" => return ColumnType::Number,
            "label" => return ColumnType::Label,
            "description" => return ColumnType::Description,
            "item" => return ColumnType::Item,
            _ => {}
        }
        match RE_LABEL_LANG.captures(&s) {
            Some(caps) => {
                return ColumnType::LabelLang(
                    caps.get(1).unwrap().as_str().to_lowercase().to_string(),
                )
            }
            None => {}
        }
        match RE_PROPERTY.captures(&s) {
            Some(caps) => {
                return ColumnType::Property(
                    caps.get(1).unwrap().as_str().to_uppercase().to_string(),
                )
            }
            None => {}
        }
        match RE_PROP_QUAL.captures(&s) {
            Some(caps) => {
                return ColumnType::PropertyQualifier((
                    caps.get(1).unwrap().as_str().to_uppercase().to_string(),
                    caps.get(2).unwrap().as_str().to_uppercase().to_string(),
                ))
            }
            None => {}
        }
        match RE_PROP_QUAL_VAL.captures(&s) {
            Some(caps) => {
                return ColumnType::PropertyQualifierValue((
                    caps.get(1).unwrap().as_str().to_uppercase().to_string(),
                    caps.get(2).unwrap().as_str().to_uppercase().to_string(),
                    caps.get(3).unwrap().as_str().to_uppercase().to_string(),
                ))
            }
            None => {}
        }
        match RE_FIELD.captures(&s) {
            Some(caps) => return ColumnType::Field(caps.get(1).unwrap().as_str().to_string()),
            None => {}
        }
        ColumnType::Unknown
    }

    pub fn as_key(&self) -> String {
        match self {
            Self::Number => "number".to_string(),
            Self::Label => "label".to_string(),
            //Self::LabelLang(s) => {}
            Self::Description => "desc".to_string(),
            Self::Item => "item".to_string(),
            Self::Property(p) => p.to_lowercase(),
            Self::PropertyQualifier((p, q)) => p.to_lowercase() + "_" + &q.to_lowercase(),
            Self::PropertyQualifierValue((p, q, v)) => {
                p.to_lowercase() + "_" + &q.to_lowercase() + "_" + &v.to_lowercase()
            }
            Self::Field(f) => f.to_lowercase(),
            //Self::Unknown => ""
            _ => "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub obj: ColumnType,
    pub label: String,
}

impl Column {
    pub fn new(s: &String) -> Self {
        lazy_static! {
            static ref RE_COLUMN_LABEL: Regex = Regex::new(r#"^\s*(.+?)\s*:\s*(.+?)\s*$"#).unwrap();
        }
        match RE_COLUMN_LABEL.captures(&s) {
            Some(caps) => Self {
                obj: ColumnType::new(&caps.get(1).unwrap().as_str().to_string()),
                label: caps.get(2).unwrap().as_str().to_string(),
            },
            None => Self {
                obj: ColumnType::new(&s.trim().to_string()),
                label: s.trim().to_string(),
            },
        }
    }

    pub fn generate_label(&mut self, page: &ListeriaList) {
        self.label = match &self.obj {
            ColumnType::Property(prop) => page
                .get_local_entity_label(&prop)
                .unwrap_or(prop.to_string()),
            ColumnType::PropertyQualifier((prop, qual)) => {
                page.get_local_entity_label(&prop)
                    .unwrap_or(prop.to_string())
                    + "/"
                    + &page
                        .get_local_entity_label(&qual)
                        .unwrap_or(qual.to_string())
            }
            ColumnType::PropertyQualifierValue((prop1, qual, prop2)) => {
                page.get_local_entity_label(&prop1)
                    .unwrap_or(prop1.to_string())
                    + "/"
                    + &page
                        .get_local_entity_label(&prop1)
                        .unwrap_or(qual.to_string())
                    + "/"
                    + &page
                        .get_local_entity_label(&prop1)
                        .unwrap_or(prop2.to_string())
            }
            _ => self.label.to_owned(), // Fallback
        }
        .to_owned();
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
                                match c.attribute("index") {
                                    Some(i) => k = Some(i.to_string()),
                                    None => {}
                                }
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

                match (k, v) {
                    (Some(k), Some(v)) => {
                        parts.insert(k, v);
                    }
                    _ => {}
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
pub enum ResultCellPart {
    Number,
    Entity((String, bool)),      // ID, try_localize
    LocalLink((String, String)), // Page, label
    Time(String),
    Location((f64, f64)),
    File(String),
    Uri(String),
    ExternalId((String, String)), // Property, ID
    Text(String),
    SnakList(Vec<ResultCellPart>), // PP and PQP
}

impl ResultCellPart {
    pub fn from_sparql_value(v: &SparqlValue) -> Self {
        match v {
            SparqlValue::Entity(x) => ResultCellPart::Entity((x.to_owned(), true)),
            SparqlValue::File(x) => ResultCellPart::File(x.to_owned()),
            SparqlValue::Uri(x) => ResultCellPart::Uri(x.to_owned()),
            SparqlValue::Time(x) => ResultCellPart::Text(x.to_owned()),
            SparqlValue::Location(x) => ResultCellPart::Location((x.lat, x.lon)),
            SparqlValue::Literal(x) => ResultCellPart::Text(x.to_owned()),
        }
    }

    pub fn from_snak(snak: &wikibase::Snak) -> Self {
        match &snak.data_value() {
            Some(dv) => match dv.value() {
                wikibase::Value::Entity(v) => ResultCellPart::Entity((v.id().to_string(), true)),
                wikibase::Value::StringValue(v) => match snak.datatype() {
                    wikibase::SnakDataType::CommonsMedia => ResultCellPart::File(v.to_string()),
                    wikibase::SnakDataType::ExternalId => {
                        ResultCellPart::ExternalId((snak.property().to_string(), v.to_string()))
                    }
                    _ => ResultCellPart::Text(v.to_string()),
                },
                wikibase::Value::Quantity(v) => ResultCellPart::Text(v.amount().to_string()),
                wikibase::Value::Time(v) => ResultCellPart::Time(ResultCellPart::reduce_time(&v)),
                wikibase::Value::Coordinate(v) => {
                    ResultCellPart::Location((*v.latitude(), *v.longitude()))
                }
                wikibase::Value::MonoLingual(v) => {
                    ResultCellPart::Text(v.language().to_string() + &":" + &v.text())
                }
            },
            _ => ResultCellPart::Text(format!("No/unknown value")),
        }
    }

    pub fn reduce_time(v: &wikibase::TimeValue) -> String {
        lazy_static! {
            static ref RE_DATE: Regex =
                Regex::new(r#"^\+{0,1}(-{0,1}\d+)-(\d{1,2})-(\d{1,2})T"#).unwrap();
        }
        let s = v.time().to_string();
        let (year, month, day) = match RE_DATE.captures(&s) {
            Some(caps) => (
                caps.get(1).unwrap().as_str().to_string(),
                caps.get(2).unwrap().as_str().to_string(),
                caps.get(3).unwrap().as_str().to_string(),
            ),
            None => {
                return s;
            }
        };
        match v.precision() {
            6 => format!("{}th millenium", year[0..year.len() - 4].to_string()),
            7 => format!("{}th century", year[0..year.len() - 3].to_string()),
            8 => format!("{}0s", year[0..year.len() - 2].to_string()),
            9 => year,
            10 => format!("{}-{}", year, month),
            11 => format!("{}-{}-{}", year, month, day),
            _ => s,
        }
    }

    fn tabbed_string_safe(&self, s: String) -> String {
        let ret = s.replace("\n", " ").replace("\t", " ");
        // 400 chars Max
        if ret.len() >= 380 {
            ret[0..380].to_string();
        }
        ret
    }

    pub fn as_wikitext(
        &self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
        partnum: usize,
    ) -> String {
        //format!("CELL ROW {} COL {} PART {}", rownum, colnum, partnum)
        match self {
            ResultCellPart::Number => format!("style='text-align:right'| {}", rownum + 1),
            ResultCellPart::Entity((id, try_localize)) => {
                let entity_id_link = format!("''[[:d:{}|{}]]''", id, id);
                if !try_localize {
                    return entity_id_link;
                }
                match list.get_entity(id.to_owned()) {
                    Some(e) => match e.label_in_locale(list.language()) {
                        Some(l) => {
                            let labeled_entity_link = format!("''[[:d:{}|{}]]''", id, l);
                            let ret = match list.get_links_type() {
                                LinksType::Text => l.to_string(),
                                LinksType::Red | LinksType::RedOnly => {
                                    if list.local_page_exists(l) {
                                        labeled_entity_link
                                    } else {
                                        "[[".to_string() + &l.to_string() + "]]"
                                    }
                                }
                                LinksType::Reasonator => {
                                    format!("[https://reasonator.toolforge.org/?q={} {}]", id, l)
                                }
                                _ => labeled_entity_link,
                            };
                            return ret;
                        }
                        None => entity_id_link,
                    },
                    None => entity_id_link,
                }
            }
            ResultCellPart::LocalLink((title, label)) => {
                if list.normalize_page_title(title) == list.normalize_page_title(label) {
                    "[[".to_string() + &label + "]]"
                } else {
                    "[[".to_string() + &title + "|" + &label + "]]"
                }
            }
            ResultCellPart::Time(time) => time.to_owned(),
            ResultCellPart::Location((lat, lon)) => list.get_location_template(*lat, *lon),
            ResultCellPart::File(file) => {
                let thumb = list.thumbnail_size();
                format!(
                    "[[{}:{}|thumb|{}px|]]",
                    list.local_file_namespace_prefix(),
                    &file,
                    thumb
                )
            }
            ResultCellPart::Uri(url) => url.to_owned(),
            ResultCellPart::ExternalId((property, id)) => {
                match list.external_id_url(property, id) {
                    Some(url) => "[".to_string() + &url + " " + &id + "]",
                    None => id.to_owned(),
                }
            }
            ResultCellPart::Text(text) => text.to_owned(),
            ResultCellPart::SnakList(v) => v
                .iter()
                .map(|rcp| rcp.as_wikitext(list, rownum, colnum, partnum))
                .collect::<Vec<String>>()
                .join(" — "),
        }
    }

    pub fn as_tabbed_data(
        &self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
        partnum: usize,
    ) -> String {
        self.tabbed_string_safe(self.as_wikitext(list, rownum, colnum, partnum))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultCell {
    parts: Vec<ResultCellPart>,
}

impl ResultCell {
    pub fn new() -> Self {
        Self { parts: vec![] }
    }
    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> Value {
        let ret: Vec<String> = self
            .parts
            .iter()
            .enumerate()
            .map(|(partnum, part)| part.as_tabbed_data(list, rownum, colnum, partnum))
            .collect();
        json!(ret.join("<br/>"))
    }

    pub fn as_wikitext(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        self.parts
            .iter()
            .enumerate()
            .map(|(partnum, part)| part.as_wikitext(list, rownum, colnum, partnum))
            .collect::<Vec<String>>()
            .join("<br/>")
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
    autolist: Option<String>,
    summary: Option<String>,
    skip_table: bool,
    wdedit: bool,
    references: bool,
    one_row_per_item: bool,
    sort_ascending: bool,
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
            autolist: None,
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
            autolist: template.params.get("autolist").map(|s|s.trim().to_uppercase()) ,
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
    pub fn new_from_string(s: &String) -> Self {
        let s = s.trim();
        let re_prop = Regex::new(r"^[Pp]\d+$").unwrap();
        if re_prop.is_match(s) {
            return Self::Property(s.to_uppercase());
        }
        let re_sparql = Regex::new(r"^@.+$").unwrap();
        if re_sparql.is_match(s) {
            return Self::SparqlVariable(s.to_uppercase());
        }
        return Self::None;
    }
}

pub trait Renderer {
    fn new() -> Self ;
    fn render(&mut self,page:&ListeriaList) -> Result<String,String> ;
}
