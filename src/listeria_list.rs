use crate::entity_container_wrapper::*;
use crate::page_params::PageParams;
use crate::result_cell::*;
use crate::result_cell_part::ResultCellPart;
use crate::result_row::ResultRow;
use crate::sparql_value::SparqlValue;
use crate::template::Template;
use crate::template_params::LinksType;
use crate::template_params::ReferencesParameter;
use crate::template_params::SectionType;
use crate::template_params::SortMode;
use crate::template_params::SortOrder;
use crate::template_params::TemplateParams;
use crate::column::{Column, ColumnType};
use anyhow::{Result,anyhow};
use chrono::DateTime;
use chrono::Utc;
use serde_json::Value;
use tokio::time::{sleep,Duration};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use wikibase::entity::*;
use wikibase::mediawiki::api::Api;
use wikibase::snak::SnakDataType;
use futures::future::join_all;

#[derive(Debug, Clone)]
pub struct ListeriaList {
    page_params: Arc<PageParams>,
    template: Template,
    columns: Vec<Column>,
    params: TemplateParams,
    sparql_rows: Vec<HashMap<String, SparqlValue>>,
    sparql_main_variable: Option<String>,
    ecw: EntityContainerWrapper,
    results: Vec<ResultRow>,
    shadow_files: Vec<String>,
    local_page_cache: HashMap<String, bool>,
    section_id_to_name: HashMap<usize, String>,
    wb_api: Arc<Api>,
    language: String,
    reference_ids: HashSet<String>,
    profiling:bool,
    last_timestamp: Arc<Mutex<DateTime<Utc>>>,
}

impl ListeriaList {
    pub fn new(template: Template, page_params: Arc<PageParams>) -> Self {
        let wb_api = page_params.wb_api();
        let mut template = template;
        template.fix_values();
        Self {
            page_params: page_params.clone(),
            template,
            columns: vec![],
            params: TemplateParams::new(),
            sparql_rows: vec![],
            sparql_main_variable: None,
            ecw: EntityContainerWrapper::new(page_params.config()),
            results: vec![],
            shadow_files: vec![],
            local_page_cache: HashMap::new(),
            section_id_to_name: HashMap::new(),
            wb_api,
            language: page_params.language().to_string(),
            reference_ids: HashSet::new(),
            profiling: page_params.config().profiling(),
            last_timestamp: Arc::new(Mutex::new(Utc::now())),
        }
    }

    fn profile(&self, msg:&str) {
        if self.profiling {
            let now: DateTime<Utc> = Utc::now();
            let last = *self.last_timestamp.lock().unwrap();
            *self.last_timestamp.lock().unwrap() = now;
            let diff = now-last;
            let timestamp = now.format("%Y%m%d%H%M%S").to_string();
            let time_diff = format!("{}",diff.num_milliseconds());
            let section = format!("{}:{}",self.page_params.wiki(),self.page_params.page());
            println!("{timestamp} {section}: {msg} [{time_diff}ms]");
        }
    }

    pub async fn process(&mut self) -> Result<()> {
        self.profile("START list::process");
        self.process_template().await?;
        self.profile("AFTER list::process process_template");
        self.run_query().await?;
        self.profile("AFTER list::process run_query");
        self.load_entities().await?;
        self.profile("AFTER list::process load_entities");
        self.generate_results().await?;
        self.profile("AFTER list::process generate_results");
        self.process_results().await?;
        self.profile("AFTER list::process process_results");
        self.profile("END list::process");
        Ok(())
    }

    pub async fn external_id_url(&self, prop: &str, id: &str) -> Option<String> {
        self.ecw.external_id_url(prop, id).await
    }

    pub fn results(&self) -> &Vec<ResultRow> {
        &self.results
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn shadow_files(&self) -> &Vec<String> {
        &self.shadow_files
    }

    pub fn reference_ids(&self) -> &HashSet<String> {
        &self.reference_ids
    }

    pub fn sparql_rows(&self) -> &Vec<HashMap<String, SparqlValue>> {
        &self.sparql_rows
    }

    pub fn local_file_namespace_prefix(&self) -> &String {
        self.page_params.local_file_namespace_prefix()
    }

    pub fn section_name(&self, id: usize) -> Option<&String> {
        self.section_id_to_name.get(&id)
    }

    pub async fn process_template(&mut self) -> Result<()> {
        let template = self.template.clone();
        match self.get_template_value(&template, "columns") {
            Some(columns) => {
                columns.split(',')
                    .filter_map(|part| Column::new(&part) )
                    .for_each(|column| self.columns.push(column));
            }
            None => {
                let column = Column::new(&"item".to_string()).ok_or_else(||anyhow!("Bad column: item"))?;
                self.columns.push(column);
            }
        }

        self.params = TemplateParams::new_from_params(&template);
        if let Some(s) = self.get_template_value(&template, "links") {
            self.params.set_links(LinksType::new_from_string(s.to_string()))
        }
        if let Some(l) = self.get_template_value(&template, "language") {
            self.language = l.to_lowercase()
        }

        let wikibase = self.params.wikibase();
        self.wb_api = match self.page_params.config().get_wbapi(&wikibase.to_lowercase()) {
            Some(api) => api.clone(),
            None => return Err(anyhow!("No wikibase setup configured for '{wikibase}'")),
        };

        Ok(())
    }

    pub fn language(&self) -> &String {
        &self.language
    }

    async fn cache_local_pages_exist(&mut self, pages: &[String]) {
        let params: HashMap<String, String> = vec![
            ("action", "query"),
            ("prop", ""),
            ("titles", pages.join("|").as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();

        let result = match self
            .page_params
            .mw_api()
            .read()
            .await
            .get_query_api_json(&params)
            .await
        {
            Ok(r) => r,
            Err(_e) => return,
        };

        let mut normalized = HashMap::new();
        for page in pages {
            normalized.insert(page.to_string(), page.to_string());
        }
        if let Some(query_normalized) = result["query"]["normalized"].as_array() {
            for n in query_normalized {
                let from = match n["from"].as_str() {
                    Some(from) => from,
                    None => continue,
                };
                let to = match n["to"].as_str() {
                    Some(to) => to,
                    None => continue,
                };
                normalized.insert(to.to_string(), from.to_string());
            }
        }

        if let Some(obj) = result["query"]["pages"].as_object() {
            for (_k, v) in obj.iter() {
                if let Some(title) = v["title"].as_str() {
                    if normalized.contains_key(title) {
                        let page_exists = v["missing"].as_str().is_none();
                        self.local_page_cache.insert(title.to_string(), page_exists);
                    }
                }
            }
        };
    }

    pub fn local_page_exists(&self, page: &str) -> bool {
        *self
            .local_page_cache
            .get(&page.to_string())
            .unwrap_or(&false)
    }

    fn first_letter_to_upper_case(&self, s1: &String) -> String {
        let mut c = s1.chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        }
    }

    pub fn normalize_page_title(&self, s: &String) -> String {
        // TODO use page to find out about first character capitalization on the current wiki
        if s.len() < 2 {
            return s.to_owned();
        }
        self.first_letter_to_upper_case(s)
    }

    pub fn get_location_template(
        &self,
        lat: f64,
        lon: f64,
        entity_id: Option<String>,
        region: Option<String>,
    ) -> String {
        self.page_params
            .config()
            .get_location_template(self.page_params.wiki())
            .replace("$LAT$", &format!("{}", lat))
            .replace("$LON$", &format!("{}", lon))
            .replace("$ITEM$", &entity_id.unwrap_or_default())
            .replace("$REGION$", &region.unwrap_or_default())
    }

    pub fn thumbnail_size(&self) -> u64 {
        let default = self.page_params.config().default_thumbnail_size();
        match self.get_template_value(&self.template, "thumb") {
            Some(s) => s.parse::<u64>().ok().or(Some(default)).unwrap_or(default),
            None => default,
        }
    }

    pub async fn run_sparql_query(&self, sparql: &str) -> Result<Value> {
        let endpoint = match self
            .wb_api
            .get_site_info_string("general", "wikibase-sparql")
        {
            Ok(endpoint) => {
                // SPARQL service given by site
                endpoint
            }
            _ => {
                // Override SPARQL service (hardcoded for Commons)
                "https://wcqs-beta.wmflabs.org/sparql"
            }
        };

        // SPARQL might need some retries sometimes, bad server or somesuch
        let mut attempts_left = 2;
        loop {
            let ret = self.wb_api.sparql_query_endpoint(sparql, endpoint).await;//.map_err(|e|anyhow!("{e}"))
            match ret {
                Ok(ret) => return Ok(ret),
                Err(e) => { 
                    match &e {
                        wikibase::mediawiki::media_wiki_error::MediaWikiError::String(s) => {
                            if attempts_left>0 && s=="error decoding response body: expected value at line 1 column 1" {
                                sleep(Duration::from_millis(500)).await;
                                attempts_left -= 1;
                                continue;
                            } else {
                                if s=="error decoding response body: expected value at line 1 column 1" {
                                    return Err(anyhow!("SPARQL is probably broken"));
                                }
                                return Err(anyhow!("{e}"))
                            }
                        },
                        e => return Err(anyhow!("{e}"))
                    }
                }
            }
        }
    }

    async fn expand_sparql_templates(&self, sparql: &mut String) -> Result<()> {
        if !sparql.contains("{{") {
            // No template
            return Ok(());
        }
        let api = self.page_params.mw_api().read().await;
        let params: HashMap<String, String> = vec![
            ("action", "expandtemplates"),
            ("title", self.page_params.page()),
            ("prop", "wikitext"),
            ("text", sparql),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let j = api
            .get_query_api_json(&params)
            .await?;
        if let Some(s) = j["expandtemplates"]["wikitext"].as_str() {
            *sparql = s.to_string();
        }
        Ok(())
    }

    fn get_template_value(&self, template: &Template, key: &str) -> Option<String> {
        // template.params.get(key).map(|s|s.to_owned())
        template.params.iter()
            .filter(|(k,_v)|k.to_lowercase()==key.to_lowercase())
            .map(|(_k,v)|v.to_owned())
            .next()
    }

    pub async fn run_query(&mut self) -> Result<()> {
        let mut sparql = match self.get_template_value(&self.template, "sparql") {
            Some(s) => s,
            None => return Err(anyhow!("No 'sparql' parameter in {:?}", &self.template)),
        }
        .to_string();

        self.expand_sparql_templates(&mut sparql).await.map_err(|e|anyhow!("{e}"))?;

        // Return simulated results
        if self.page_params.simulate() {
            match self.page_params.simulated_sparql_results() {
                Some(json_text) => {
                    let j = serde_json::from_str(&json_text)?;
                    return self.parse_sparql(j);
                }
                None => {}
            }
        }

        self.profile("BEGIN run_query: run_sparql_query");
        let j = self.run_sparql_query(&sparql).await?;
        self.profile("END run_query: run_sparql_query");
        if self.page_params.simulate() {
            println!("{}\n{}\n", &sparql, &j);
        }
        self.parse_sparql(j)
    }

    fn parse_sparql(&mut self, j: Value) -> Result<()> {
        self.sparql_rows.clear();
        self.sparql_main_variable = None;

        if false {
            // Use first variable
            let first_var = match j["head"]["vars"].as_array() {
                Some(a) => match a.get(0) {
                    Some(v) => v.as_str().ok_or(anyhow!("Can't parse first variable"))?.to_string(),
                    None => return Err(anyhow!("Bad SPARQL head.vars")),
                },
                None => return Err(anyhow!("Bad SPARQL head.vars")),
            };
            self.sparql_main_variable = Some(first_var);
        } else if let Some(arr) = j["head"]["vars"].as_array() {
            // Insist on ?item
            let required_variable_name = "item";
            for v in arr {
                if Some(required_variable_name) == v.as_str() {
                    self.sparql_main_variable = Some(required_variable_name.to_string());
                    break;
                }
            }
        }

        let bindings = j["results"]["bindings"]
            .as_array()
            .ok_or(anyhow!("Broken SPARQL results.bindings"))?;
        for b in bindings.iter() {
            let mut row: HashMap<String, SparqlValue> = HashMap::new();
            if let Some(bo) = b.as_object() {
                for (k, v) in bo.iter() {
                    match SparqlValue::new_from_json(&v) {
                        Some(v2) => row.insert(k.to_owned(), v2),
                        None => {
                            return Err(anyhow!("Can't parse SPARQL value: {} => {:?}", &k, &v))
                        }
                    };
                }
            }
            if row.is_empty() {
                continue;
            }
            self.sparql_rows.push(row);
        }
        Ok(())
    }

    pub async fn load_entities(&mut self) -> Result<()> {
        // Any columns that require entities to be loaded?
        // TODO also force if self.links is redlinks etc.
        if self
            .columns
            .iter()
            .filter(
                |c| !matches!(c.obj, ColumnType::Number | ColumnType::Item | ColumnType::Field(_)),
            )
            .count()
            == 0
        {
            return Ok(());
        }

        let ids = self.get_ids_from_sparql_rows()?;
        if ids.is_empty() {
            return Err(anyhow!("No items to show"));
        }
        self.ecw.load_entities(&self.wb_api, &ids).await.map_err(|e|anyhow!("{e}"))?;

        self.label_columns().await;

        Ok(())
    }

    async fn label_columns(&mut self) {
        let mut columns = vec![];
        for c in &self.columns {
            let mut c = c.clone();
            c.generate_label(self).await;
            columns.push(c);
        }
        self.columns = columns;
    }

    fn get_ids_from_sparql_rows(&self) -> Result<Vec<String>> {
        let varname = self.get_var_name()?;

        // Rows
        let ids_tmp: Vec<String> = self
            .sparql_rows
            .iter()
            .filter_map(|row| match row.get(varname) {
                Some(SparqlValue::Entity(id)) => Some(id.to_string()),
                _ => None,
            })
            .collect();

        let mut ids: Vec<String> = vec![];
        ids_tmp.iter().for_each(|id| {
            if !ids.contains(id) {
                ids.push(id.to_string());
            }
        });

        // Can't sort/dedup, need to preserve original order

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

        Ok(ids)
    }

    fn get_var_name(&self) -> Result<&String> {
        match &self.sparql_main_variable {
            Some(v) => Ok(v),
            None => Err(anyhow!("Could not determine SPARQL variable for item")),
        }
    }

    pub async fn get_autodesc_description(&self, e: &Entity) -> Result<String> {
        if self.params.autodesc() != Some("FALLBACK".to_string()) {
            return Err(anyhow!("Not used"));
        }
        match &self.page_params.simulated_autodesc() {
            Some(autodesc) => {
                for ad in autodesc {
                    let parts: Vec<&str> = ad.splitn(3, '|').collect();
                    if parts.len() == 3 && parts[0] == e.id() && parts[1] == self.language {
                        return Ok(parts[2].trim().to_string());
                    }
                }
            }
            None => {}
        }
        let url = format!(
            "https://autodesc.toolforge.org/?q={}&lang={}&mode=short&links=wiki&format=json",
            e.id(),
            self.language
        );
        let api = self.page_params.mw_api().read().await;
        let body = api
            .query_raw(&url, &api.no_params(), "GET")
            .await?;
        let json: Value = serde_json::from_str(&body)?;
        match json["result"].as_str() {
            Some(result) => Ok(result.to_string()),
            None => Err(anyhow!("Not a valid autodesc result")),
        }
    }

    fn get_tmp_rows(&self) -> Result<HashMap<String,Vec<&HashMap<String,SparqlValue>>>> {
        let varname = self.get_var_name()?;
        let sparql_row_ids: HashSet<String> = self.get_ids_from_sparql_rows()?.into_iter().collect();
        let mut tmp_rows: HashMap<String,Vec<&HashMap<String,SparqlValue>>> = HashMap::new();
        for sparql_row in &self.sparql_rows {
            let id = match sparql_row.get(varname) {
                Some(SparqlValue::Entity(id)) => id,
                _ => continue,
            };
            if !sparql_row_ids.contains(id) {
                continue;
            }
            tmp_rows.entry(id.to_owned()).or_default().push(sparql_row);
        }
        Ok(tmp_rows)
}

    pub async fn generate_results(&mut self) -> Result<()> {
        let mut results: Vec<ResultRow> = vec![];
        match self.params.one_row_per_item() {
            true => {
                let tmp_rows = self.get_tmp_rows()?;
                self.profile("BEGIN generate_results join_all");

                // Doesn't seem to be faster with join_all
                let mut tmp_results = vec![];
                for (id,sparql_rows) in &tmp_rows {
                    tmp_results.push(self.ecw.get_result_row(&id, &sparql_rows, &self).await);
                }
                // let mut futures = vec!() ;
                // for (id,sparql_rows) in &tmp_rows {
                //     futures.push(self.ecw.get_result_row(&id, &sparql_rows, &self));
                // }
                // let tmp_results = join_all(futures).await;

                self.profile("END generate_results join_all");
                results = tmp_results
                    .iter()
                    .cloned()
                    .filter_map(|x| x)
                    .collect();

            }
            false => {
                let varname = self.get_var_name()?;
                for row in self.sparql_rows.iter() {
                    if let Some(SparqlValue::Entity(id)) = row.get(varname) {
                        if let Some(x) = self.ecw.get_result_row(id, &[&row], &self).await {
                            results.push(x);
                        }
                    }
                }
            }
        };
        self.results = results;
        Ok(())
    }

    async fn process_items_to_local_links(&mut self) -> Result<()> {
        // Try to change items to local link
        // TODO get rid of clone()
        let mut results = self.results.clone();
        self.results.clear();
        for row in results.iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                ResultCell::localize_item_links_in_parts(self, cell.parts_mut()).await;
            }
        }
        self.results = results;
        Ok(())
    }

    fn process_excess_files(&mut self) {
        self.results.iter_mut().for_each(|row| {
            row.remove_excess_files();
        });
    }

    async fn process_remove_shadow_files(&mut self) -> Result<()> {
        if !self
            .page_params
            .config()
            .check_for_shadow_images(&self.page_params.wiki().to_string())
        {
            return Ok(());
        }
        let mut files_to_check = vec![];
        for row in self.results.iter() {
            for cell in row.cells() {
                for part in cell.parts() {
                    if let ResultCellPart::File(file) = &part.part {
                        files_to_check.push(file);
                    }
                }
            }
        }
        files_to_check.sort_unstable();
        files_to_check.dedup();

        self.shadow_files.clear();

        let param_list : Vec<HashMap<String,String>> = files_to_check
            .iter()
            .map(|filename|{
                let prefixed_filename = format!(
                    "{}:{}",
                    self.page_params.local_file_namespace_prefix(),
                    &filename
                );
                let params: HashMap<String, String> = vec![
                    ("action", "query"),
                    ("titles", prefixed_filename.as_str()),
                    ("prop", "imageinfo"),
                ]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();
                params
            })
            .collect();

        let api_read = self
        .page_params
        .mw_api()
        .read()
        .await;

        let mut futures = vec![] ;
        for params in &param_list {
            futures.push ( api_read.get_query_api_json(&params) ) ;
        }

        let tmp_results = join_all(futures)
            .await;
        
        let tmp_results : Vec<(&String,Value)> = tmp_results
            .iter()
            .zip(files_to_check)
            .filter_map(|(result,filename)| match result {
                Ok(j) => Some((filename,j.to_owned())),
                _ => None
            })
            .collect()
        ;

        for (filename,j) in tmp_results {
            let mut could_be_local = false;
            match j["query"]["pages"].as_object() {
                Some(results) => {
                    results
                        .iter()
                        .for_each(|(_k, o)| match o["imagerepository"].as_str() {
                            Some("shared") => {}
                            _ => {
                                could_be_local = true;
                            }
                        })
                }
                None => {
                    could_be_local = true;
                }
            };

            if could_be_local {
                self.shadow_files.push(filename.to_string());
            }
        }

        self.shadow_files.sort();

        // Remove shadow files from data table
        // TODO this is less than ideal in terms of pretty code...
        let shadow_files = self.shadow_files.clone();
        self.results.iter_mut().for_each(|row| {
            row.remove_shadow_files(&shadow_files);
        });

        Ok(())
    }

    async fn process_redlinks_only(&mut self) -> Result<()> {
        if *self.get_links_type() != LinksType::RedOnly {
            return Ok(());
        }

        // Remove all rows with existing local page
        let wiki = self.page_params.wiki().to_string();
        for row in self.results.iter_mut() {
            row.set_keep(
                match self.ecw.get_entity(row.entity_id()).await {
                    Some(entity) => {
                        match entity.sitelinks() {
                            Some(sl) => sl.iter().filter(|s| *s.site() == wiki).count() == 0,
                            None => true, // No sitelinks, keep
                        }
                    }
                    _ => false,
                },
            );
        }
        self.results.retain(|row| row.keep());
        Ok(())
    }

    async fn process_redlinks(&mut self) -> Result<()> {
        if *self.get_links_type() != LinksType::RedOnly && *self.get_links_type() != LinksType::Red
        {
            return Ok(());
        }

        // Cache if local pages exist
        let mut ids = vec![];
        self.results.iter().for_each(|row| {
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Entity((id, true)) = &part.part {
                        // _try_localize ?
                        ids.push(id);
                    }
                })
            });
        });

        ids.sort();
        ids.dedup();
        let mut labels = vec![];
        for id in ids {
            if let Some(e) = self.get_entity(id).await {
                if let Some(l) = e.label_in_locale(self.language()) {
                    labels.push(l.to_string());
                }
            }
        }

        labels.sort();
        labels.dedup();
        // TODO in parallel
        let labels_per_chunk = if self.page_params.mw_api().read().await.user().is_bot() {
            500
        } else {
            50
        };
        for chunk in labels.chunks(labels_per_chunk) {
            self.cache_local_pages_exist(chunk).await;
        }

        Ok(())
    }

    async fn process_sort_results(&mut self) -> Result<()> {
        let mut sortkeys: Vec<String> = vec![];
        let mut datatype = SnakDataType::String; // Default
        match &self.params.sort() {
            SortMode::Label => {
                self.load_row_entities().await?;
                for row in &self.results {
                    sortkeys.push(row.get_sortkey_label(&self).await);
                }
            }
            SortMode::FamilyName => {
                for row in &self.results {
                    sortkeys.push(row.get_sortkey_family_name(&self).await);
                }
            }
            SortMode::Property(prop) => {
                datatype = self.ecw.get_datatype_for_property(prop).await;
                for row in &self.results {
                    sortkeys.push(row.get_sortkey_prop(&prop, &self, &datatype).await);
                }
            }
            SortMode::SparqlVariable(variable) => {
                sortkeys = self
                    .results
                    .iter()
                    .map(|row| row.get_sortkey_sparql(&variable, &self))
                    .collect();
            }
            SortMode::None => return Ok(()),
        }

        // Apply sortkeys
        if self.results.len() != sortkeys.len() {
            // Paranoia
            return Err(anyhow!("process_sort_results: sortkeys length mismatch"));
        }
        self.results
            .iter_mut()
            .enumerate()
            .for_each(|(rownum, row)| row.set_sortkey(sortkeys[rownum].to_owned()));

        self.results.sort_by(|a, b| a.compare_to(b, &datatype));
        if *self.params.sort_order() == SortOrder::Descending {
            self.results.reverse()
        }

        Ok(())
    }

    async fn load_row_entities(&mut self) -> Result<()> {
        let items_to_load = self
            .results
            .iter()
            .map(|row| row.entity_id())
            .cloned()
            .collect();
        self.ecw.load_entities(&self.wb_api, &items_to_load).await.map_err(|e|anyhow!("{e}"))?;
        Ok(())
    }

    pub async fn process_assign_sections(&mut self) -> Result<()> {
        self.profile("BEFORE list::process_assign_sections");

        // TODO all SectionType options
        let section_property = match self.params.section() {
            SectionType::Property(p) => p,
            SectionType::SparqlVariable(_v) => {
                return Err(anyhow!("SPARQL variable section type not supported yet"))
            }
            SectionType::None => return Ok(()), // Nothing to do
        }.to_owned();
        self.load_row_entities().await?;
        let datatype = self.ecw.get_datatype_for_property(&section_property).await;
        self.profile("AFTER list::process_assign_sections 1");

        let mut section_names_q = vec![];
        for row in &self.results {
            section_names_q.push(row.get_sortkey_prop(&section_property, self, &datatype).await);
        }
        self.profile("AFTER list::process_assign_sections 2");
        
        // Make sure section name items are loaded
        self.ecw.load_entities(&self.wb_api, &section_names_q).await.map_err(|e|anyhow!("{e}"))?;
        self.profile("AFTER list::process_assign_sections 3a");
        let mut section_names = vec![];
        for q in section_names_q {
            let label = self.get_label_with_fallback(&q,None).await;
            section_names.push(label);
        }

        // Count names
        let mut section_count = HashMap::new();
        section_names.iter().for_each(|name| {
            let counter = section_count.entry(name).or_insert(0);
            *counter += 1;
        });
        self.profile("AFTER list::process_assign_sections 4");

        // Remove low counts
        section_count.retain(|&_name, &mut count| count >= self.params.min_section());
        self.profile("AFTER list::process_assign_sections 5");

        // Sort by section name
        let mut valid_section_names: Vec<String> =
            section_count.iter().map(|(k, _v)| k.to_string()).collect();
        valid_section_names.sort();
        self.profile("AFTER list::process_assign_sections 6");


        let misc_id = valid_section_names.len();
        valid_section_names.push("Misc".to_string());

        // TODO skip if no/one section?

        // name to id
        let name2id: HashMap<String, usize> = valid_section_names
            .iter()
            .enumerate()
            .map(|(num, name)| (name.to_string(), num))
            .collect();
        self.profile("AFTER list::process_assign_sections 7");

        self.section_id_to_name = name2id
            .iter()
            .map(|x| (x.1.to_owned(), x.0.to_owned()))
            .collect();
        self.profile("AFTER list::process_assign_sections 8");

        self.results.iter_mut().enumerate().for_each(|(num, row)| {
            let section_name = match section_names.get(num) {
                Some(name) => name,
                None => return,
            };
            let section_id = match name2id.get(section_name) {
                Some(id) => *id,
                None => misc_id,
            };
            row.set_section(section_id);
        });
        self.profile("AFTER list::process_assign_sections 9");

        Ok(())
    }

    async fn get_region_for_entity_id(&self, entity_id: &str) -> Option<String> {
        let sparql = format!(
            "SELECT ?q ?x {{ wd:{} wdt:P131* ?q . ?q wdt:P300 ?x }}",
            entity_id
        );
        let j = self.run_sparql_query(&sparql).await.ok()?;
        match j["results"]["bindings"].as_array() {
            Some(a) => {
                let mut region = String::new();
                a.iter().for_each(|b| {
                    match b["x"]["type"].as_str() {
                        Some("literal") => {}
                        _ => return,
                    }
                    if let Some(r) = b["x"]["value"].as_str() {
                        if r.len() > region.len() {
                            region = r.to_string();
                        }
                    }
                });
                if region.is_empty() {
                    None
                } else {
                    Some(region)
                }
            }
            None => None,
        }
    }

    fn do_get_regions(&self) -> bool {
        self.page_params
            .config()
            .location_regions()
            .contains(&self.wiki().to_string())
    }

    pub async fn process_regions(&mut self) -> Result<()> {
        if !self.do_get_regions() {
            return Ok(());
        }

        let mut entity_ids = HashSet::new();
        self.results.iter().for_each(|row| {
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Location((_lat, _lon, _region)) = &part.part {
                        entity_ids.insert(row.entity_id().to_string());
                        //*region = self.get_region_for_entity_id(row.entity_id()).await ;
                    }
                });
            });
        });

        let mut entity_id2region = HashMap::new();
        for entity_id in entity_ids {
            if let Some(region) = self.get_region_for_entity_id(&entity_id).await {
                entity_id2region.insert(entity_id, region);
            }
        }

        for row in self.results.iter_mut() {
            let the_region = match entity_id2region.get(row.entity_id()) {
                Some(r) => r,
                None => continue,
            };
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    if let ResultCellPart::Location((_lat, _lon, region)) = &mut part.part {
                        *region = Some(the_region.clone());
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_reference_items(&mut self) -> Result<()> {
        let mut items_to_load: Vec<String> = vec![];
        for row in self.results.iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                for part_with_reference in cell.parts_mut().iter_mut() {
                    match &part_with_reference.references {
                        Some(references) => {
                            for reference in references.iter() {
                                match &reference.stated_in {
                                    Some(stated_in) => items_to_load.push(stated_in.to_string()),
                                    None => {}
                                }
                            }
                        }
                        None => {}
                    }
                }
            }
        }
        if !items_to_load.is_empty() {
            items_to_load.sort_unstable();
            items_to_load.dedup();
            self.ecw.load_entities(&self.wb_api, &items_to_load).await.map_err(|e|anyhow!("{e}"))?;
        }
        Ok(())
    }

    async fn fix_local_links(&mut self) -> Result<()> {
        // Set the is_category flag
        let mw_api = self.mw_api();
        let mw_api = mw_api.read().await;
        for row in self.results.iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    if let ResultCellPart::LocalLink((page, _label, is_category)) = &mut part.part {
                        let title = wikibase::mediawiki::title::Title::new_from_full(page, &mw_api);
                        *is_category = title.namespace_id() == 14;
                    } else if let ResultCellPart::SnakList(v) = &mut part.part {
                        for subpart in v.iter_mut() {
                            if let ResultCellPart::LocalLink((page, _label, is_category)) =
                                &mut subpart.part
                            {
                                let title =
                                    wikibase::mediawiki::title::Title::new_from_full(page, &mw_api);
                                *is_category = title.namespace_id() == 14;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn process_results(&mut self) -> Result<()> {
        self.profile("START list::process_results");
        self.gather_and_load_items().await?;
        self.profile("AFTER list::process_results gather_and_load_items");
        self.process_redlinks_only().await?;
        self.profile("AFTER list::process_results process_redlinks_only");
        self.process_items_to_local_links().await?;
        self.profile("AFTER list::process_results process_items_to_local_links");
        self.process_redlinks().await?;
        self.profile("AFTER list::process_results process_redlinks");
        self.process_remove_shadow_files().await?;
        self.profile("AFTER list::process_results process_remove_shadow_files");
        self.process_excess_files();
        self.profile("AFTER list::process_results process_excess_files");
        self.process_reference_items().await?;
        self.profile("AFTER list::process_results process_reference_items");
        self.process_sort_results().await?;
        self.profile("AFTER list::process_results process_sort_results");
        self.process_assign_sections().await?;
        self.profile("AFTER list::process_results process_assign_sections");
        self.process_regions().await?;
        self.profile("AFTER list::process_results process_regions");
        self.fix_local_links().await?;
        self.profile("AFTER list::process_results fix_local_links");
        self.profile("END list::process_results");
        Ok(())
    }

    pub fn get_links_type(&self) -> &LinksType {
        self.params.links()
    }

    pub async fn get_entity(&self, entity_id: &str) -> Option<wikibase::Entity> {
        self.ecw.get_entity(entity_id).await
    }

    pub fn get_row_template(&self) -> &Option<String> {
        self.params.row_template()
    }

    pub fn get_reference_parameter(&self) -> &ReferencesParameter {
        self.params.references()
    }

    async fn gather_items_for_property(&mut self, prop: &str) -> Result<Vec<String>> {
        let mut entities_to_load = vec![];
        for row in self.results.iter() {
            if let Some(entity) = self.ecw.get_entity(row.entity_id()).await {
                self
                    .get_filtered_claims(&entity, prop)
                    .iter()
                    .filter(|statement| statement.property() == prop)
                    .map(|statement| statement.main_snak())
                    .filter(|snak| *snak.datatype() == SnakDataType::WikibaseItem)
                    .filter_map(|snak| snak.data_value().to_owned())
                    .map(|datavalue| datavalue.value().to_owned())
                    .filter_map(|value| match value {
                        wikibase::value::Value::Entity(v) => Some(v.id().to_owned()),
                        _ => None,
                    })
                    .for_each(|id| entities_to_load.push(id));
            }
        }
        Ok(entities_to_load)
    }

    async fn gather_items_section(&mut self) -> Result<Vec<String>> {
        // TODO support all of SectionType
        let prop = match self.params.section() {
            SectionType::Property(p) => p.clone(),
            SectionType::SparqlVariable(_v) => {
                return Err(anyhow!("SPARQL variable section type not supported yet"))
            }
            SectionType::None => return Ok(vec![]), // Nothing to do
        };
        self.gather_items_for_property(&prop).await
    }

    async fn gather_items_sort(&mut self) -> Result<Vec<String>> {
        let prop = match self.params.sort() {
            SortMode::Property(prop) => prop.clone(),
            _ => return Ok(vec![]),
        };
        self.gather_items_for_property(&prop).await
    }

    async fn gather_and_load_items(&mut self) -> Result<()> {
        // Gather items to load
        let mut entities_to_load: Vec<String> = vec![];
        for row in self.results.iter() {
            for cell in row.cells() {
                self.ecw
                    .gather_entities_and_external_properties(cell.parts())
                    .iter()
                    .for_each(|entity_id| entities_to_load.push(entity_id.to_string()));
            }
        }
        if let SortMode::Property(prop) = self.params.sort() {
            entities_to_load.push(prop.to_string());
        }

        match self.params.section() {
            SectionType::Property(prop) => {
                entities_to_load.push(prop.to_owned());
            }
            SectionType::SparqlVariable(_v) => {
                return Err(anyhow!("SPARQL variable section type not supported yet"))
            }
            SectionType::None => {}
        }
        self.ecw
            .load_entities(&self.wb_api, &entities_to_load)
            .await
            .map_err(|e|anyhow!("{e}"))?;

        entities_to_load = self.gather_items_sort().await?;
        let mut v2 = self.gather_items_section().await?;
        entities_to_load.append(&mut v2);
        match self.ecw
            .load_entities(&self.wb_api, &entities_to_load)
            .await {
                Ok(ret) => Ok(ret),
                Err(e) => Err(anyhow!("{e}")),
            }
    }

    pub fn column(&self, column_id: usize) -> Option<&Column> {
        self.columns.get(column_id)
    }

    pub fn skip_table(&self) -> bool {
        self.params.skip_table()
    }

    pub fn get_section_ids(&self) -> Vec<usize> {
        let mut ret: Vec<usize> = self.results.iter().map(|row| row.section()).collect();
        ret.sort_unstable();
        ret.dedup();
        ret
    }

    pub fn wiki(&self) -> &str {
        self.page_params.wiki()
    }

    pub fn page_title(&self) -> &str {
        self.page_params.page()
    }

    pub fn summary(&self) -> &Option<String> {
        self.params.summary()
    }

    pub fn header_template(&self) -> &Option<String> {
        self.params.header_template()
    }

    pub async fn get_label_with_fallback(&self, entity_id: &str, use_language: Option<&str>) -> String {
        let use_language = match use_language {
            Some(l) => l,
            None => self.language(),
        };
        match self.get_entity(entity_id).await {
            Some(entity) => {
                match entity.label_in_locale(use_language).map(|s| s.to_string()) {
                    Some(s) => s,
                    None => {
                        // Try the usual suspects
                        for language in ["en", "de", "fr", "es", "it", "el", "nl"].iter() {
                            if let Some(label) =
                                entity.label_in_locale(language).map(|s| s.to_string())
                            {
                                return label;
                            }
                        }
                        // Try any label, any language
                        if let Some(entity) = self.get_entity(entity_id).await {
                            if let Some(label) = entity.labels().get(0) {
                                return label.value().to_string();
                            }
                        }
                        // Fallback to item ID as label
                        entity_id.to_string()
                    }
                }
            }
            None => entity_id.to_string(), // Fallback
        }
    }

    pub fn is_wikidatawiki(&self) -> bool {
        self.page_params.wiki() == "wikidatawiki"
    }

    pub fn get_item_wiki_target(&self, entity_id: &str) -> String {
        let prefix = if self.is_wikidatawiki() { "" } else { ":d:" };
        if let Some(first_char) = entity_id.chars().next() {
            if first_char == 'p' || first_char == 'P' {
                return format!("{}Property:{}", prefix, entity_id);
            }
        }
        format!("{}{}", prefix, entity_id)
    }

    pub async fn get_item_link_with_fallback(&self, entity_id: &str) -> String {
        let quotes = if self.is_wikidatawiki() { "" } else { "''" };
        let label = self.get_label_with_fallback(entity_id, None).await;
        let label_part = if self.is_wikidatawiki() && entity_id == label {
            String::new()
        } else {
            format!("|{}", label)
        };
        format!(
            "{}[[{}{}]]{}",
            quotes,
            self.get_item_wiki_target(entity_id),
            label_part,
            quotes
        )
    }


    pub fn get_filtered_claims(
        &self,
        e: &wikibase::entity::Entity,
        property: &str,
    ) -> Vec<wikibase::statement::Statement> {
        let mut ret: Vec<wikibase::statement::Statement> = e
            .claims_with_property(property)
            .iter()
            .map(|x| (*x).clone())
            .collect();

        if self.page_params.config().prefer_preferred() {
            let has_preferred = ret
                .iter()
                .any(|x| *x.rank() == wikibase::statement::StatementRank::Preferred);
            if has_preferred {
                ret.retain(|x| *x.rank() == wikibase::statement::StatementRank::Preferred);
            }
            ret
        } else {
            ret
        }
    }

    pub async fn entity_to_local_link(&self, item: &str) -> Option<ResultCellPart> {
        self.ecw
            .entity_to_local_link(item, self.wiki(), &self.language)
            .await
    }

    pub fn default_language(&self) -> String {
        self.page_params.config().default_language().to_string()
    }

    pub fn template_params(&self) -> &TemplateParams {
        &self.params
    }

    pub fn mw_api(&self) -> crate::ApiLock {
        self.page_params.mw_api().clone()
    }
}
