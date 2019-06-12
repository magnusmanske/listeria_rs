#[macro_use]
extern crate lazy_static;
extern crate mediawiki;
//#[macro_use]
extern crate serde_json;

use regex::{Regex, RegexBuilder};
use roxmltree;
use serde_json::Value;
use std::collections::HashMap;
use urlencoding;
use wikibase::entity_container::EntityContainer;

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ListeriaPage {
    mw_api: mediawiki::api::Api,
    wd_api: mediawiki::api::Api,
    page: String,
    template_title_start: String,
    language: String,
    template: Option<Template>,
    sparql_values: Vec<HashMap<String, SparqlValue>>,
    sparql_first_variable: Option<String>,
    columns: Vec<Column>,
    entities: EntityContainer,
}

impl ListeriaPage {
    pub fn new(mw_api: &mediawiki::api::Api, page: String) -> Option<Self> {
        let mut ret = Self {
            mw_api: mw_api.clone(),
            wd_api: mediawiki::api::Api::new("https://www.wikidata.org/w/api.php")
                .expect("Could not connect to Wikidata API"),
            page: page,
            template_title_start: "Wikidata list".to_string(),
            language: mw_api.get_site_info_string("general", "lang").ok()?,
            template: None,
            sparql_values: vec![],
            sparql_first_variable: None,
            columns: vec![],
            entities: EntityContainer::new(),
        };
        ret.init().ok()?;
        Some(ret)
    }

    fn init(self: &mut Self) -> Result<(), String> {
        self.load_page()?;
        self.process_template()?;
        self.run_query()?;
        self.load_entities()?;
        Ok(())
    }

    fn load_page(self: &mut Self) -> Result<(), String> {
        let params: HashMap<String, String> = vec![
            ("action", "parse"),
            ("prop", "parsetree"),
            ("page", self.page.as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();

        let result = self
            .mw_api
            .get_query_api_json(&params)
            .expect("Loading page failed");
        let doc = match result["parse"]["parsetree"]["*"].as_str() {
            Some(text) => roxmltree::Document::parse(&text).unwrap(),
            None => return Err(format!("No parse tree for {}", &self.page)),
        };
        doc.root()
            .descendants()
            .filter(|n| n.is_element() && n.tag_name().name() == "template")
            .for_each(|node| {
                if self.template.is_some() {
                    return;
                }
                match Template::new_from_xml(&node) {
                    Some(t) => {
                        if t.title == self.template_title_start {
                            self.template = Some(t);
                        }
                    }
                    None => {}
                }
            });
        Ok(())
    }

    fn process_template(self: &mut Self) -> Result<(), String> {
        let template = match &self.template {
            Some(t) => t.clone(),
            None => {
                return Err(format!(
                    "No template '{}' found",
                    &self.template_title_start
                ))
            }
        };

        match template.params.get("columns") {
            Some(columns) => {
                columns.split(",").for_each(|part| {
                    let s = part.clone().to_string();
                    self.columns.push(Column::new(&s));
                });
            }
            None => self.columns.push(Column::new(&"item".to_string())),
        }

        println!("Columns: {:?}", &self.columns);
        Ok(())
    }

    pub fn run_query(self: &mut Self) -> Result<(), String> {
        let t = match &self.template {
            Some(t) => t,
            None => return Err(format!("No template found")),
        };
        let sparql = match t.params.get("sparql") {
            Some(s) => s,
            None => return Err(format!("No `sparql` parameter in {:?}", &t)),
        };

        println!("Running SPARQL: {}", &sparql);
        let j = match self.wd_api.sparql_query(sparql) {
            Ok(j) => j,
            Err(e) => return Err(format!("{:?}", &e)),
        };
        self.parse_sparql(j)
    }

    fn parse_sparql(self: &mut Self, j: Value) -> Result<(), String> {
        self.sparql_values.clear();
        self.sparql_first_variable = None;

        // TODO force first_var to be "item" for backwards compatability?
        // Or check if it is, and fail if not?
        let first_var = match j["head"]["vars"].as_array() {
            Some(a) => match a.get(0) {
                Some(v) => v.as_str().ok_or("Can't parse first variable")?.to_string(),
                None => return Err(format!("Bad SPARQL head.vars")),
            },
            None => return Err(format!("Bad SPARQL head.vars")),
        };
        self.sparql_first_variable = Some(first_var.clone());

        let bindings = j["results"]["bindings"]
            .as_array()
            .ok_or("Broken SPARQL results.bindings")?;
        for b in bindings.iter() {
            let mut row: HashMap<String, SparqlValue> = HashMap::new();
            for (k, v) in b.as_object().unwrap().iter() {
                match SparqlValue::new_from_json(&v) {
                    Some(v2) => row.insert(k.to_owned(), v2),
                    None => return Err(format!("Can't parse SPARQL value: {} => {:?}", &k, &v)),
                };
            }
            if row.is_empty() {
                continue;
            }
            self.sparql_values.push(row);
        }
        println!("FIRST: {}", &first_var);
        println!("{:?}", &self.sparql_values);
        Ok(())
    }

    fn load_entities(self: &mut Self) -> Result<(), String> {
        // Any columns that require entities to be loaded?
        if self
            .columns
            .iter()
            .filter(|c| match c.obj {
                ColumnType::Number => false,
                ColumnType::Item => false,
                ColumnType::Field(_) => false,
                _ => true,
            })
            .count()
            == 0
        {
            return Ok(());
        }

        let varname = match &self.sparql_first_variable {
            Some(v) => v,
            None => return Err(format!("load_entities: sparql_first_variable is None")),
        };

        let ids: Vec<String> = self
            .sparql_values
            .iter()
            .filter_map(|row| match row.get(varname) {
                Some(SparqlValue::Entity(id)) => Some(id.to_string()),
                _ => None,
            })
            .collect();
        if ids.is_empty() {
            return Err(format!("No items to show (using variable '{}')", &varname));
        }
        match self.entities.load_entities(&self.wd_api, &ids) {
            Ok(_) => {}
            Err(e) => return Err(format!("Error loading entities: {:?}", &e)),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
