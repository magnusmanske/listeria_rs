#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

use regex::{Regex, RegexBuilder};
use roxmltree;
use serde_json::Value;
use std::collections::HashMap;
use urlencoding;
use wikibase::entity::*;
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

    pub fn generate_label(self: &mut Self, page: &ListeriaPage) {
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

    pub fn as_tabbed_data(
        &self,
        page: &ListeriaPage,
        rownum: usize,
        _colnum: usize,
        _partnum: usize,
    ) -> String {
        //format!("CELL ROW {} COL {} PART {}", rownum, colnum, partnum)
        self.tabbed_string_safe(match self {
            ResultCellPart::Number => (rownum + 1).to_string(),
            ResultCellPart::Entity((id, try_localize)) => {
                let entity_id_link = "''[[:d:".to_string() + &id + "|" + &id + "]]''";
                if !try_localize {
                    return entity_id_link;
                }
                match page.get_entity(id.to_owned()) {
                    Some(e) => match e.label_in_locale(&page.language) {
                        Some(l) => "''[[:d:".to_string() + &id + "|" + &l.to_string() + "]]''",
                        None => entity_id_link,
                    },
                    None => entity_id_link,
                }
            }
            ResultCellPart::LocalLink((page, label)) => {
                if page == label {
                    "[[".to_string() + &page + "]]"
                } else {
                    "[[".to_string() + &page + "|" + &label + "]]"
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
        })
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
    mw_api: wikibase::mediawiki::api::Api,
    wd_api: wikibase::mediawiki::api::Api,
    wiki: String,
    page: String,
    template_title_start: String,
    language: String,
    template: Option<Template>,
    sparql_rows: Vec<HashMap<String, SparqlValue>>,
    sparql_first_variable: Option<String>,
    columns: Vec<Column>,
    entities: EntityContainer,
    pub one_row_per_item: bool,
    links: LinksType,
    results: Vec<ResultRow>,
    data_has_changed: bool,
}

impl ListeriaPage {
    pub async fn new(mw_api: &wikibase::mediawiki::api::Api, page: String) -> Option<Self> {
        Some(Self {
            mw_api: mw_api.clone(),
            wd_api: wikibase::mediawiki::api::Api::new("https://www.wikidata.org/w/api.php")
                .await
                .expect("Could not connect to Wikidata API"),
            wiki: mw_api
                .get_site_info_string("general", "wikiid")
                .expect("No wikiid in site info")
                .to_string(),
            page: page,
            template_title_start: "Wikidata list".to_string(),
            language: mw_api
                .get_site_info_string("general", "lang")
                .ok()?
                .to_string(),
            template: None,
            sparql_rows: vec![],
            sparql_first_variable: None,
            columns: vec![],
            entities: EntityContainer::new(),
            one_row_per_item: false,
            links: LinksType::All, // TODO make configurable
            results: vec![],
            data_has_changed: false,
        })
    }

    pub async fn run(self: &mut Self) -> Result<(), String> {
        self.load_page().await?;
        self.process_template()?;
        self.run_query().await?;
        self.load_entities().await?;
        self.results = self.get_results()?;
        self.patch_results().await
    }

    pub fn get_local_entity_label(&self, entity_id: &String) -> Option<String> {
        self.entities
            .get_entity(entity_id.to_owned())?
            .label_in_locale(&self.language)
            .map(|s| s.to_string())
    }

    pub fn thumbnail_size(&self) -> u64 {
        let default: u64 = 128;
        let t = match &self.template {
            Some(t) => t,
            None => return default,
        };
        match t.params.get("thumb") {
            Some(s) => s.parse::<u64>().ok().or(Some(default)).unwrap(),
            None => default,
        }
    }

    pub fn external_id_url(&self, prop: &String, id: &String) -> Option<String> {
        let pi = self.entities.get_entity(prop.to_owned())?;
        pi.claims_with_property("P1630")
            .iter()
            .filter_map(|s| {
                let data_value = s.main_snak().data_value().to_owned()?;
                match data_value.value() {
                    wikibase::Value::StringValue(s) => Some(
                        s.to_owned()
                            .replace("$1", &urlencoding::decode(&id).ok()?.to_string()),
                    ),
                    _ => None,
                }
            })
            .next()
    }

    pub fn get_entity<S: Into<String>>(&self, entity_id: S) -> Option<wikibase::Entity> {
        self.entities.get_entity(entity_id)
    }

    pub fn get_location_template(&self, lat: f64, lon: f64) -> String {
        // TODO use localized geo template
        format!("({},{})", lat, lon)
    }

    async fn load_page(self: &mut Self) -> Result<(), String> {
        let text = self.load_page_as("parsetree").await?.to_owned();
        let doc = roxmltree::Document::parse(&text).unwrap();
        /*
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
        */
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

        //println!("Columns: {:?}", &self.columns);
        Ok(())
    }

    async fn run_query(self: &mut Self) -> Result<(), String> {
        let t = match &self.template {
            Some(t) => t,
            None => return Err(format!("No template found")),
        };
        let sparql = match t.params.get("sparql") {
            Some(s) => s,
            None => return Err(format!("No `sparql` parameter in {:?}", &t)),
        };

        //println!("Running SPARQL: {}", &sparql);
        let j = match self.wd_api.sparql_query(sparql).await {
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
        //println!("FIRST: {}", &first_var);
        //println!("{:?}", &self.sparql_rows);
        Ok(())
    }

    async fn load_entities(self: &mut Self) -> Result<(), String> {
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
        match self.entities.load_entities(&self.wd_api, &ids).await {
            Ok(_) => {}
            Err(e) => return Err(format!("Error loading entities: {:?}", &e)),
        }

        self.label_columns();

        Ok(())
    }

    fn label_columns(self: &mut Self) {
        self.columns = self
            .columns
            .iter()
            .map(|c| {
                let mut c = c.clone();
                c.generate_label(self);
                c
            })
            .collect();
    }

    fn get_ids_from_sparql_rows(&self) -> Result<Vec<String>, String> {
        let varname = self.get_var_name()?;

        // Rows
        let mut ids: Vec<String> = self
            .sparql_rows
            .iter()
            .filter_map(|row| match row.get(varname) {
                Some(SparqlValue::Entity(id)) => Some(id.to_string()),
                _ => None,
            })
            .collect();

        // Column headers
        self.columns.iter().for_each(|c| match &c.obj {
            ColumnType::Property(prop) => {
                ids.push(prop.to_owned());
            }
            ColumnType::PropertyQualifier((prop, qual)) => {
                ids.push(prop.to_owned());
                ids.push(qual.to_owned());
            }
            ColumnType::PropertyQualifierValue((prop1, qual, prop2)) => {
                ids.push(prop1.to_owned());
                ids.push(qual.to_owned());
                ids.push(prop2.to_owned());
            }
            _ => {}
        });

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
                    let label = match e.label_in_locale(&self.language) {
                        Some(s) => s.to_string(),
                        None => entity_id.to_string(),
                    };
                    let local_page = match e.sitelinks() {
                        Some(sl) => sl
                            .iter()
                            .filter(|s| *s.site() == self.wiki)
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
                    // TODO
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
        match self.links {
            LinksType::Local => {
                if !self.entities.has_entity(entity_id.to_owned()) {
                    return None;
                }
            }
            LinksType::RedOnly => {
                if self.entities.has_entity(entity_id.to_owned()) {
                    return None;
                }
            }
            _ => {}
        }

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

    fn entity_to_local_link(&self, item: &String) -> Option<ResultCellPart> {
        let entity = match self.entities.get_entity(item.to_owned()) {
            Some(e) => e,
            None => return None,
        };
        let page = match entity.sitelinks() {
            Some(sl) => sl
                .iter()
                .filter(|s| *s.site() == self.wiki)
                .map(|s| s.title().to_string())
                .next(),
            None => None,
        }?;
        let label = self.get_local_entity_label(item).unwrap_or(page.clone());
        Some(ResultCellPart::LocalLink((page, label)))
    }

    async fn patch_results(self: &mut Self) -> Result<(), String> {
        // Gather items to load
        let mut entities_to_load = vec![];
        for row in self.results.iter() {
            for cell in &row.cells {
                for part in &cell.parts {
                    match part {
                        ResultCellPart::Entity((item, true)) => {
                            entities_to_load.push(item.to_owned());
                        }
                        ResultCellPart::ExternalId((property, _id)) => {
                            entities_to_load.push(property.to_owned());
                        }
                        _ => {}
                    }
                }
            }
        }

        // Load items
        match self.entities.load_entities(&self.wd_api, &entities_to_load).await {
            Ok(_) => {}
            Err(e) => return Err(format!("Error loading entities: {:?}", &e)),
        }

        // Try to change items to local link
        self.results = self
            .results
            .iter()
            .map(|row| ResultRow {
                cells: row
                    .cells
                    .iter()
                    .map(|cell| ResultCell {
                        parts: cell
                            .parts
                            .iter()
                            .map(|part| match part {
                                ResultCellPart::Entity((item, true)) => {
                                    match self.entity_to_local_link(&item) {
                                        Some(ll) => ll,
                                        None => part.to_owned(),
                                    }
                                }
                                _ => part.to_owned(),
                            })
                            .collect(),
                    })
                    .collect(),
                section: 0,
            })
            .collect();
        Ok(())
    }

    pub fn as_tabbed_data(&self) -> Result<Value, String> {
        let mut ret = json!({"license": "CC0-1.0","description": {"en":"Listeria output"},"sources":"https://github.com/magnusmanske/listeria_rs","schema":{"fields":[{ "name": "section", "type": "number", "title": { self.language.to_owned(): "Section"}}]},"data":[]});
        self.columns.iter().enumerate().for_each(|(colnum,col)| {
            ret["schema"]["fields"]
                .as_array_mut()
                .unwrap() // OK, this must exist
                .push(json!({"name":"col_".to_string()+&colnum.to_string(),"type":"string","title":{self.language.to_owned():col.label}}))
        });
        ret["data"] = self
            .results
            .iter()
            .enumerate()
            .map(|(rownum, row)| row.as_tabbed_data(&self, rownum))
            .collect();
        Ok(ret)
    }

    pub fn tabbed_data_page_name(&self) -> Option<String> {
        let ret = "Data:Listeria/".to_string() + &self.wiki + "/" + &self.page + ".tab";
        if ret.len() > 250 {
            return None; // Page title too long
        }
        Some(ret)
    }

    pub async fn write_tabbed_data(
        self: &mut Self,
        tabbed_data_json: Value,
        commons_api: &mut wikibase::mediawiki::api::Api,
    ) -> Result<(), String> {
        let data_page = self
            .tabbed_data_page_name()
            .ok_or("Data page name too long")?;
        let text = ::serde_json::to_string(&tabbed_data_json).unwrap();
        let params: HashMap<String, String> = vec![
            ("action", "edit"),
            ("title", data_page.as_str()),
            ("summary", "Listeria test"),
            ("text", text.as_str()),
            ("minor", "true"),
            ("recreate", "true"),
            ("token", commons_api.get_edit_token().await.unwrap().as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();
        // No need to check if this is the same as the existing data; MW API will return OK but not actually edit
        let _result = match commons_api.post_query_api_json_mut(&params).await {
            Ok(r) => r,
            Err(e) => return Err(format!("{:?}", e)),
        };
        // TODO check ["edit"]["result"] == "Success"
        // TODO set data_has_changed is result is not "same as before"
        self.data_has_changed = true; // Just to make sure to update including page
        Ok(())
    }

    async fn load_page_as(&self, mode: &str) -> Result<String, String> {
        let params: HashMap<String, String> = vec![
            ("action", "parse"),
            ("prop", mode),
            ("page", self.page.as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();

        let result = self
            .mw_api
            .get_query_api_json(&params)
            .await
            .expect("Loading page failed");
        match result["parse"][mode]["*"].as_str() {
            Some(ret) => Ok(ret.to_string()),
            None => return Err(format!("No parse tree for {}", &self.page)),
        }
    }

    fn separate_start_template(&self, blob: &String) -> Option<(String, String)> {
        let mut split_at: Option<usize> = None;
        let mut curly_count: i32 = 0;
        blob.char_indices().for_each(|(pos, c)| {
            match c {
                '{' => {
                    curly_count += 1;
                }
                '}' => {
                    curly_count -= 1;
                }
                _ => {}
            }
            if curly_count == 0 && split_at.is_none() {
                split_at = Some(pos + 1);
            }
        });
        match split_at {
            Some(pos) => {
                let mut template = blob.clone();
                let rest = template.split_off(pos);
                Some((template, rest))
            }
            None => None,
        }
    }

    pub async fn update_source_page(self: &mut Self) -> Result<(), String> {
        let wikitext = self.load_page_as("wikitext").await?;

        // TODO use local template name

        // Start/end template
        let pattern1 =
            r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)(\{\{[Ww]ikidata[ _]list[ _]end\}\})(.*)"#;

        // No end template
        let pattern2 = r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)"#;

        let re_wikitext1: Regex = RegexBuilder::new(pattern1)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();
        let re_wikitext2: Regex = RegexBuilder::new(pattern2)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();

        let (before, blob, end_template, after) = match re_wikitext1.captures(&wikitext) {
            Some(caps) => (
                caps.get(1).unwrap().as_str(),
                caps.get(2).unwrap().as_str(),
                caps.get(3).unwrap().as_str(),
                caps.get(4).unwrap().as_str(),
            ),
            None => match re_wikitext2.captures(&wikitext) {
                Some(caps) => (
                    caps.get(1).unwrap().as_str(),
                    caps.get(2).unwrap().as_str(),
                    "",
                    "",
                ),
                None => return Err(format!("No template/end template found")),
            },
        };

        let (start_template, rest) = match self.separate_start_template(&blob.to_string()) {
            Some(parts) => parts,
            None => return Err(format!("Can't split start template")),
        };

        let append = if end_template.is_empty() {
            rest.to_string()
        } else {
            after.to_string()
        };

        // Remove tabbed data marker
        let start_template = Regex::new(r"\|\s*tabbed_data[^\|\}]*")
            .unwrap()
            .replace(&start_template, "");

        // Add tabbed data marker
        let start_template = start_template[0..start_template.len() - 2]
            .trim()
            .to_string()
            + "\n|tabbed_data=1}}";

        // Create new wikitext
        let new_wikitext = before.to_owned() + &start_template + "\n" + append.trim();

        // Compare to old wikitext
        if wikitext == new_wikitext {
            // All is as it should be
            if self.data_has_changed {
                self.purge_page().await?;
            }
            return Ok(());
        }

        // TODO edit page

        Ok(())
    }

    async fn purge_page(self: &mut Self) -> Result<(), String> {
        let params: HashMap<String, String> =
            vec![("action", "purge"), ("titles", self.page.as_str())]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();

        let _result = match self.mw_api.get_query_api_json(&params).await {
            Ok(r) => r,
            Err(e) => return Err(format!("{:?}", e)),
        };

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
