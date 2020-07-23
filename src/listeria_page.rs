use crate::*;
use regex::{Regex, RegexBuilder};
use roxmltree;
use serde_json::Value;
use std::collections::HashMap;
use urlencoding;
use wikibase::entity::*;
use wikibase::entity_container::EntityContainer;
use wikibase::mediawiki::api::Api;

/* TODO
- Show only preffered values (eg P41 in Q43175)
- Main namespace block

TEMPLATE PARAMETERS
sparql DONE
columns DONE
sort IMPLEMENT?
section IMPLEMENT
min_section IMPLEMENT
autolist IMPLEMENT
language done?
thumb DONE via thumbnail_size()
links IMPLEMENT fully
row_template IMPLEMENT
header_template IMPLEMENT
skip_table IMPLEMENT
wdedit IMPLEMENT
references IMPLEMENT
freq IGNORED
summary DONE
*/


#[derive(Debug, Clone, Default)]
struct TemplateParams {
    sort: Option<String>,
    section: Option<String>,
    min_section:u64,
    row_template: Option<String>,
    header_template: Option<String>,
    autolist: Option<String>,
    summary: Option<String>,
    skip_table: bool,
    wdedit: bool,
    references: bool,
    one_row_per_item: bool,
}

#[derive(Debug, Clone)]
pub struct ListeriaPage {
    mw_api: Api,
    wd_api: Api,
    wiki: String,
    page: String,
    template_title_start: String,
    language: String,
    template: Option<Template>,
    params: TemplateParams,
    sparql_rows: Vec<HashMap<String, SparqlValue>>,
    sparql_first_variable: Option<String>,
    columns: Vec<Column>,
    entities: EntityContainer,
    links: LinksType,
    results: Vec<ResultRow>,
    shadow_files: Vec<String>,
    wikis_to_check_for_shadow_images: Vec<String>,
    data_has_changed: bool,
    simulate: bool,
}

impl ListeriaPage {
    pub async fn new(mw_api: &Api, page: String) -> Option<Self> {
        Some(Self {
            mw_api: mw_api.clone(),
            wd_api: Api::new("https://www.wikidata.org/w/api.php")
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
            params: TemplateParams { min_section:2, ..Default::default() },
            sparql_rows: vec![],
            sparql_first_variable: None,
            columns: vec![],
            entities: EntityContainer::new(),
            links: LinksType::All, // TODO make configurable
            results: vec![],
            shadow_files: vec![],
            wikis_to_check_for_shadow_images: vec!["enwiki".to_string()],
            data_has_changed: false,
            simulate: false,
        })
    }

    pub fn do_simulate(&mut self) {
        self.simulate = true ;
    }

    pub fn language(&self) -> &String {
        return &self.language;
    }

    pub async fn run(&mut self) -> Result<(), String> {
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

    async fn load_page(&mut self) -> Result<(), String> {
        let text = self.load_page_as("parsetree").await?.to_owned();
        let doc = roxmltree::Document::parse(&text).unwrap();
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

    fn process_template(&mut self) -> Result<(), String> {
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

        self.params = TemplateParams {
            sort: template.params.get("sort").map(|s|s.trim().to_uppercase()),
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
        } ;

        match template.params.get("language") {
            Some(l) =>  self.language = l.to_lowercase(),
            None => {}
        }

        match template.params.get("links") {
            Some(s) =>  self.links = LinksType::new_from_string(s.to_string()),
            None => {}
        }

        println!("{:?}",&self.params);
        //println!("Columns: {:?}", &self.columns);
        Ok(())
    }

    async fn run_query(&mut self) -> Result<(), String> {
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

    fn parse_sparql(&mut self, j: Value) -> Result<(), String> {
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

    async fn load_entities(&mut self) -> Result<(), String> {
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

    fn label_columns(&mut self) {
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

    fn get_results(&mut self) -> Result<Vec<ResultRow>, String> {
        let varname = self.get_var_name()?;
        Ok(match self.params.one_row_per_item {
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

    fn patch_items_to_local_links(&mut self) -> Result<(), String> {
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

    async fn gather_and_load_items(&mut self) -> Result<(), String> {
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

        Ok(())
    }

    async fn patch_remove_shadow_files(&mut self) -> Result<(), String> {
        if !self.wikis_to_check_for_shadow_images.contains(&self.wiki) {
            return Ok(())
        }
        let mut files_to_check = vec![] ;
        for row in self.results.iter() {
            for cell in &row.cells {
                for part in &cell.parts {
                    match part {
                        ResultCellPart::File(file) => {
                            files_to_check.push(file);
                        }
                        _ => {}
                    }
                }
            }
        }
        files_to_check.sort_unstable();
        files_to_check.dedup();

        self.shadow_files.clear();

        // TODO better async
        for filename in files_to_check {
            let prefixed_filename = format!("File:{}",&filename) ;
            let params: HashMap<String, String> =
                vec![("action", "query"), ("titles", prefixed_filename.as_str()),("prop","imageinfo")]
                    .iter()
                    .map(|x| (x.0.to_string(), x.1.to_string()))
                    .collect();

            let j = match self.mw_api.get_query_api_json(&params).await {
                Ok(j) => j,
                Err(_e) => json!({})
            };

            let mut could_be_local = false ;
            match j["query"]["pages"].as_object() {
                Some(results) => {
                    results.iter().for_each(|(_k, o)|{
                        match o["imagerepository"].as_str() {
                            Some("shared") => {},
                            _ => { could_be_local = true ; }
                        }
                    })
                }
                None => { could_be_local = true ; }
            };

            if could_be_local {
                self.shadow_files.push(filename.to_string());
            }
        }

        self.shadow_files.sort();

        // Remove shadow files from data table
        // TODO this is less than ideal in terms of pretty code...
        let shadow_files = &self.shadow_files;
        self.results.iter_mut().for_each(|row|{
            row.cells.iter_mut().for_each(|cell|{
                cell.parts = cell.parts.iter().filter(|part|{
                    match part {
                        ResultCellPart::File(file) => !shadow_files.contains(file),
                        _ => true
                    }
                })
                .cloned()
                .collect();
            });
        });

        Ok(())
    }

    async fn patch_results(&mut self) -> Result<(), String> {
        self.gather_and_load_items().await? ;
        self.patch_items_to_local_links()?;
        self.patch_remove_shadow_files().await?;
        Ok(())
    }

    fn get_section_ids(&self) -> Vec<usize> {
        let mut ret : Vec<usize> = self
            .results
            .iter()
            .map(|row|{row.section})
            .collect();
        ret.sort_unstable();
        ret.dedup();
        ret
    }

    fn as_wikitext_section(&self,section_id:usize) -> String {
        let mut wt = String::new() ;

        // TODO: template rendering

        // Headers
        wt += "{!\n" ;
        self.columns.iter().enumerate().for_each(|(_colnum,col)| {
            wt += "!" ;
            wt += &col.label ;
            wt += "\n" ;
        });

        if !self.results.is_empty() {
            wt += "|-\n";
        }

        // Rows
        wt += &self
            .results
            .iter()
            .filter(|row|row.section==section_id)
            .enumerate()
            .map(|(rownum, row)| row.as_wikitext(&self, rownum))
            .collect::<Vec<String>>()
            .join("\n|-\n");

        // End
        wt += "\n|}" ;

        wt
    }

    fn local_file_namespace_prefix(&self) -> String {
        "File".to_string() // TODO
    }

    pub fn as_wikitext(&self) -> Result<String,String> {
        let section_ids = self.get_section_ids() ;
        // TODO section headers
        let mut wt = section_ids
            .iter()
            .map(|section_id|self.as_wikitext_section(*section_id))
            .collect() ;

        if !self.shadow_files.is_empty() {
            wt += "\n----\nThe following local image(s) are not shown in the above list, because they shadow a Commons image of the same name, and might be non-free:\n";
            for file in &self.shadow_files {
                wt += format!("# [[:{}:{}|]]\n",self.local_file_namespace_prefix(),file).as_str();
            }
        }

        match self.params.summary.as_ref().map(|s|s.as_str()) {
            Some("ITEMNUMBER") => {
                wt += format!("\n----\n&sum; {} items.",self.results.len()).as_str();
            }
            _ => {}
        }

        Ok(wt)
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
        &mut self,
        tabbed_data_json: Value,
        commons_api: &mut Api,
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

    pub async fn update_source_page(&self) -> Result<(), String> {
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

    async fn purge_page(&self) -> Result<(), String> {
        if self.simulate {
            println!("SIMULATING: purging [[{}]] on {}", &self.page,self.wiki);
            return Ok(())
        }
        let params: HashMap<String, String> =
            vec![("action", "purge"), ("titles", self.page.as_str())]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();

        match self.mw_api.get_query_api_json(&params).await {
            Ok(_r) => Ok(()),
            Err(e) => return Err(format!("{:?}", e)),
        }
    }
}
