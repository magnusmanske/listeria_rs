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
                println!("BAD TIME: {}/{}", &s, v.precision());
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultCell {
    parts: Vec<ResultCellPart>,
}

impl ResultCell {
    pub fn new() -> Self {
        Self { parts: vec![] }
    }
}

#[derive(Debug, Clone)]
pub struct ResultRow {
    cells: Vec<ResultCell>,
}

impl ResultRow {
    pub fn new() -> Self {
        Self { cells: vec![] }
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

#[derive(Debug, Clone)]
pub struct ListeriaPage {
    mw_api: mediawiki::api::Api,
    wd_api: mediawiki::api::Api,
    page: String,
    template_title_start: String,
    language: String,
    template: Option<Template>,
    sparql_rows: Vec<HashMap<String, SparqlValue>>,
    sparql_first_variable: Option<String>,
    columns: Vec<Column>,
    entities: EntityContainer,
    one_row_per_item: bool,
    links: LinksType,
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
            sparql_rows: vec![],
            sparql_first_variable: None,
            columns: vec![],
            entities: EntityContainer::new(),
            one_row_per_item: false, // TODO make configurable
            links: LinksType::All,   // TODO make configurable
        };
        ret.init().ok()?;
        Some(ret)
    }

    fn init(self: &mut Self) -> Result<(), String> {
        self.load_page()?;
        self.process_template()?;
        self.run_query()?;
        self.load_entities()?;
        dbg!(self.get_results()?);
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
        self.sparql_rows.clear();
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
            self.sparql_rows.push(row);
        }
        println!("FIRST: {}", &first_var);
        println!("{:?}", &self.sparql_rows);
        Ok(())
    }

    fn load_entities(self: &mut Self) -> Result<(), String> {
        // Any columns that require entities to be loaded?
        // TODO also force if self.links is redlinks etc.
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

        let ids = self.get_ids_from_sparql_rows()?;
        if ids.is_empty() {
            return Err(format!("No items to show"));
        }
        match self.entities.load_entities(&self.wd_api, &ids) {
            Ok(_) => {}
            Err(e) => return Err(format!("Error loading entities: {:?}", &e)),
        }

        Ok(())
    }

    fn get_ids_from_sparql_rows(&self) -> Result<Vec<String>, String> {
        let varname = self.get_var_name()?;
        let mut ids: Vec<String> = self
            .sparql_rows
            .iter()
            .filter_map(|row| match row.get(varname) {
                Some(SparqlValue::Entity(id)) => Some(id.to_string()),
                _ => None,
            })
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn get_result_cell(
        &self,
        entity_id: &String,
        sparql_rows: &Vec<&HashMap<String, SparqlValue>>,
        col: &Column,
    ) -> ResultCell {
        let mut ret = ResultCell::new();
        /*
        ret.parts.push(ResultCellPart::Text(format!(
            "{}:{:?} / {:?}",
            &entity_id, col, sparql_rows
        )));
        */

        let entity = self.entities.get_entity(entity_id.to_owned());
        match &col.obj {
            ColumnType::Item => {
                ret.parts
                    .push(ResultCellPart::Entity((entity_id.to_owned(), false)));
            }
            ColumnType::Description => match entity {
                Some(e) => match e.description_in_locale(self.language.as_str()) {
                    Some(s) => {
                        ret.parts.push(ResultCellPart::Text(s.to_string()));
                    }
                    None => {}
                },
                None => {}
            },
            ColumnType::Field(varname) => {
                for row in sparql_rows.iter() {
                    match row.get(varname) {
                        Some(x) => {
                            ret.parts.push(ResultCellPart::from_sparql_value(x));
                        }
                        None => {}
                    }
                }
            }
            ColumnType::Property(property) => match entity {
                Some(e) => {
                    e.claims_with_property(property.to_owned())
                        .iter()
                        .for_each(|statement| {
                            ret.parts
                                .push(ResultCellPart::from_snak(statement.main_snak()));
                        });
                }
                None => {}
            },
            ColumnType::LabelLang(language) => match entity {
                Some(e) => {
                    match e.label_in_locale(language) {
                        Some(s) => {
                            ret.parts.push(ResultCellPart::Text(s.to_string()));
                        }
                        None => match e.label_in_locale(&self.language) {
                            // Fallback
                            Some(s) => {
                                ret.parts.push(ResultCellPart::Text(s.to_string()));
                            }
                            None => {} // No label available
                        },
                    }
                }
                None => {}
            },
            ColumnType::Label => match entity {
                Some(e) => {
                    let wiki = self
                        .mw_api
                        .get_site_info_string("general", "wikiid")
                        .unwrap();
                    println!("Wiki:{}", &wiki);
                    let label = match e.label_in_locale(&self.language) {
                        Some(s) => s.to_string(),
                        None => entity_id.to_string(),
                    };
                    let local_page = match e.sitelinks() {
                        Some(sl) => sl
                            .iter()
                            .filter(|s| *s.site() == wiki)
                            .map(|s| s.title().to_string())
                            .next(),
                        None => None,
                    };
                    match local_page {
                        Some(page) => {
                            ret.parts.push(ResultCellPart::LocalLink((page, label)));
                        }
                        None => {
                            ret.parts
                                .push(ResultCellPart::Entity((entity_id.to_string(), false)));
                        }
                    }
                }
                None => {}
            },
            ColumnType::Unknown => {} // Ignore
            ColumnType::Number => {
                ret.parts.push(ResultCellPart::Number);
            }
            _ => {} /*
                    PropertyQualifier((String, String)),
                    PropertyQualifierValue((String, String, String)),
                    */
        }

        ret
    }

    fn get_result_row(
        &self,
        entity_id: &String,
        sparql_rows: &Vec<&HashMap<String, SparqlValue>>,
    ) -> Option<ResultRow> {
        let mut row = ResultRow::new();
        row.cells = self
            .columns
            .iter()
            .map(|col| self.get_result_cell(entity_id, sparql_rows, col))
            .collect();
        Some(row)
    }

    fn get_var_name(&self) -> Result<&String, String> {
        match &self.sparql_first_variable {
            Some(v) => Ok(v),
            None => return Err(format!("load_entities: sparql_first_variable is None")),
        }
    }

    fn get_results(self: &mut Self) -> Result<Vec<ResultRow>, String> {
        let varname = self.get_var_name()?;
        Ok(match self.one_row_per_item {
            true => self
                .get_ids_from_sparql_rows()?
                .iter()
                .filter_map(|id| {
                    let sparql_rows: Vec<&HashMap<String, SparqlValue>> = self
                        .sparql_rows
                        .iter()
                        .filter(|row| match row.get(varname) {
                            Some(SparqlValue::Entity(v)) => v == id,
                            _ => false,
                        })
                        .collect();
                    if !sparql_rows.is_empty() {
                        self.get_result_row(id, &sparql_rows)
                    } else {
                        None
                    }
                })
                .collect(),
            false => self
                .sparql_rows
                .iter()
                .filter_map(|row| match row.get(varname) {
                    Some(SparqlValue::Entity(id)) => self.get_result_row(id, &vec![&row]),
                    _ => None,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
