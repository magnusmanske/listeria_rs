#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod listeria_page;

pub use crate::listeria_page::ListeriaPage;
use regex::{Regex, RegexBuilder};
use roxmltree;
use serde_json::Value;
use std::collections::HashMap;
use urlencoding;
use wikibase::entity::EntityTrait;

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

    pub fn generate_label(&mut self, page: &ListeriaPage) {
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
                println!("I'M HAVING A BAD TIME: {}/{}", &s, v.precision());
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
        page: &ListeriaPage,
        rownum: usize,
        _colnum: usize,
        _partnum: usize,
    ) -> String {
        //format!("CELL ROW {} COL {} PART {}", rownum, colnum, partnum)
        match self {
            ResultCellPart::Number => (rownum + 1).to_string(),
            ResultCellPart::Entity((id, try_localize)) => {
                let entity_id_link = "''[[:d:".to_string() + &id + "|" + &id + "]]''";
                if !try_localize {
                    return entity_id_link;
                }
                match page.get_entity(id.to_owned()) {
                    Some(e) => match e.label_in_locale(page.language()) {
                        Some(l) => "''[[:d:".to_string() + &id + "|" + &l.to_string() + "]]''",
                        None => entity_id_link,
                    },
                    None => entity_id_link,
                }
            }
            ResultCellPart::LocalLink((title, label)) => {
                if page.normalize_page_title(title) == page.normalize_page_title(label) {
                    "[[".to_string() + &label + "]]"
                } else {
                    "[[".to_string() + &title + "|" + &label + "]]"
                }
            }
            ResultCellPart::Time(time) => time.to_owned(),
            ResultCellPart::Location((lat, lon)) => page.get_location_template(*lat, *lon),
            ResultCellPart::File(file) => {
                let thumb = page.thumbnail_size();
                // TODO localize "File" and "thumb"
                "[[File:".to_string() + &file + "|thumb|" + &thumb.to_string() + "px|]]"
            }
            ResultCellPart::Uri(url) => url.to_owned(),
            ResultCellPart::ExternalId((property, id)) => {
                match page.external_id_url(property, id) {
                    Some(url) => "[".to_string() + &url + " " + &id + "]",
                    None => id.to_owned(),
                }
            }
            ResultCellPart::Text(text) => text.to_owned(),
        }
    }

    pub fn as_tabbed_data(
        &self,
        page: &ListeriaPage,
        rownum: usize,
        colnum: usize,
        partnum: usize,
    ) -> String {
        self.tabbed_string_safe(self.as_wikitext(page, rownum, colnum, partnum))
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
    pub fn as_tabbed_data(&self, page: &ListeriaPage, rownum: usize, colnum: usize) -> Value {
        let ret: Vec<String> = self
            .parts
            .iter()
            .enumerate()
            .map(|(partnum, part)| part.as_tabbed_data(page, rownum, colnum, partnum))
            .collect();
        json!(ret.join("<br/>"))
    }

    pub fn as_wikitext(&self, page: &ListeriaPage, rownum: usize, colnum: usize) -> String {
        self.parts
            .iter()
            .enumerate()
            .map(|(partnum, part)| part.as_wikitext(page, rownum, colnum, partnum))
            .collect::<Vec<String>>()
            .join("<br/>")
    }
}

#[derive(Debug, Clone)]
pub struct ResultRow {
    cells: Vec<ResultCell>,
    section: usize,
}

impl ResultRow {
    pub fn new() -> Self {
        Self {
            cells: vec![],
            section: 0,
        }
    }

    pub fn as_tabbed_data(&self, page: &ListeriaPage, rownum: usize) -> Value {
        let mut ret: Vec<Value> = self
            .cells
            .iter()
            .enumerate()
            .map(|(colnum, cell)| cell.as_tabbed_data(page, rownum, colnum))
            .collect();
        ret.insert(0, json!(self.section));
        json!(ret)
    }

    pub fn as_wikitext(&self, page: &ListeriaPage, rownum: usize) -> String {
        let cells = self
            .cells
            .iter()
            .enumerate()
            .map(|(colnum, cell)| cell.as_wikitext(page, rownum, colnum))
            .collect::<Vec<String>>();
        match page.get_row_template() {
            Some(t) => {
                "{{".to_string()
                    + &t
                    + "\n| "
                    + &cells
                        .iter()
                        .enumerate()
                        .map(|(colnum, cell)| format!("|{}={}", colnum, &cell))
                        .collect::<Vec<String>>()
                        .join("\n| ")
            }
            None => "| ".to_string() + &cells.join("\n| "),
        }
    }
}

#[derive(Debug, Clone)]
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
