use crate::column::{Column, ColumnType};
use crate::entity_container_wrapper::*;
use crate::page_params::PageParams;
use crate::result_cell::*;
use crate::result_cell_part::ResultCellPart;
use crate::result_row::ResultRow;
use crate::sparql_results::SparqlResults;
use crate::template::Template;
use crate::template_params::LinksType;
use crate::template_params::ReferencesParameter;
use crate::template_params::SectionType;
use crate::template_params::SortMode;
use crate::template_params::SortOrder;
use crate::template_params::TemplateParams;
use anyhow::{anyhow, Result};
use chrono::DateTime;
use chrono::Utc;
use futures::future::join_all;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use wikimisc::file_vec::FileVec;
use wikimisc::mediawiki::api::Api;
use wikimisc::sparql_value::SparqlValue;
use wikimisc::wikibase::entity::*;
use wikimisc::wikibase::{SnakDataType, Statement, StatementRank};

#[derive(Debug, Clone)]
pub struct ListeriaList {
    page_params: Arc<PageParams>,
    template: Template,
    columns: Vec<Column>,
    params: TemplateParams,
    sparql_rows: Vec<HashMap<String, SparqlValue>>,
    sparql_main_variable: Option<String>,
    ecw: EntityContainerWrapper,
    results: FileVec<ResultRow>,
    shadow_files: HashSet<String>,
    local_page_cache: HashMap<String, bool>,
    section_id_to_name: HashMap<usize, String>,
    wb_api: Arc<Api>,
    language: String,
    reference_ids: HashSet<String>,
    profiling: bool,
    last_timestamp: DateTime<Utc>,
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
            sparql_rows: Vec::new(),
            sparql_main_variable: None,
            ecw: EntityContainerWrapper::new(page_params.config()),
            results: FileVec::new(),
            shadow_files: HashSet::new(),
            local_page_cache: HashMap::new(),
            section_id_to_name: HashMap::new(),
            wb_api,
            language: page_params.language().to_string(),
            reference_ids: HashSet::new(),
            profiling: page_params.config().profiling(),
            last_timestamp: Utc::now(),
        }
    }

    fn profile(&mut self, msg: &str) {
        if self.profiling {
            let now: DateTime<Utc> = Utc::now();
            let last = self.last_timestamp.to_owned();
            self.last_timestamp = now;
            let diff = now - last;
            let timestamp = now.format("%Y%m%d%H%M%S").to_string();
            let time_diff = diff.num_milliseconds();
            let section = format!("{}:{}", self.page_params.wiki(), self.page_params.page());
            println!("{timestamp} {section}: {msg} [{time_diff}ms]");
        }
    }

    pub async fn process(&mut self) -> Result<()> {
        self.profile("START list::process");
        self.process_template()?;
        self.profile("AFTER list::process process_template");
        self.run_query().await?;
        self.profile("AFTER list::process run_query");
        self.load_entities().await?;
        self.profile("AFTER list::process load_entities");
        // TODO task::block_in_place(move || { or task::spawn_blocking(move || {
        self.generate_results()?;
        self.profile("AFTER list::process generate_results");
        self.process_results().await?;
        self.profile("AFTER list::process process_results");
        self.profile("END list::process");
        Ok(())
    }

    pub fn external_id_url(&self, prop: &str, id: &str) -> Option<String> {
        self.ecw.external_id_url(prop, id)
    }

    pub fn results(&self) -> &FileVec<ResultRow> {
        &self.results
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn shadow_files(&self) -> &HashSet<String> {
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

    pub fn process_template(&mut self) -> Result<()> {
        let template = self.template.clone();
        match self.get_template_value(&template, "columns") {
            Some(columns) => {
                columns
                    .split(',')
                    .filter_map(Column::new)
                    .for_each(|column| self.columns.push(column));
            }
            None => {
                let column = Column::new("item").ok_or_else(|| anyhow!("Bad column: item"))?;
                self.columns.push(column);
            }
        }

        self.params = TemplateParams::new_from_params(&template, &self.page_params.config());
        if let Some(s) = self.get_template_value(&template, "links") {
            self.params
                .set_links(LinksType::new_from_string(s.to_string()))
        }
        if let Some(l) = self.get_template_value(&template, "language") {
            self.language = l.to_lowercase()
        }

        let wikibase = self.params.wikibase();
        self.wb_api = match self
            .page_params
            .config()
            .get_wbapi(&wikibase.to_lowercase())
        {
            Some(api) => api.clone(),
            None => return Err(anyhow!("No wikibase setup configured for '{wikibase}'")),
        };

        Ok(())
    }

    pub fn language(&self) -> &str {
        &self.language
    }

    async fn cache_local_pages_exist(&mut self, pages: &[String]) {
        let params: HashMap<String, String> = [
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

    fn first_letter_to_upper_case(&self, s1: &str) -> String {
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

    fn get_template_value(&self, template: &Template, key: &str) -> Option<String> {
        template
            .params
            .iter()
            .filter(|(k, _v)| k.to_lowercase() == key.to_lowercase())
            .map(|(_k, v)| v.to_owned())
            .next()
    }

    pub async fn run_query(&mut self) -> Result<()> {
        let wikibase_key = self.params.wikibase().to_lowercase();
        let sparql = match self.get_template_value(&self.template, "sparql") {
            Some(s) => s,
            None => return Err(anyhow!("No 'sparql' parameter in {:?}", &self.template)),
        };
        let mut sparql_results = SparqlResults::new(self.page_params.clone(), &wikibase_key);
        self.sparql_rows = sparql_results.run_query(sparql).await?;
        self.sparql_main_variable = sparql_results.sparql_main_variable();
        Ok(())
    }

    pub async fn load_entities(&mut self) -> Result<()> {
        // Any columns that require entities to be loaded?
        // TODO also force if self.links is redlinks etc.
        if self
            .columns
            .iter()
            .filter(|c| {
                !matches!(
                    c.obj,
                    ColumnType::Number | ColumnType::Item | ColumnType::Field(_)
                )
            })
            .count()
            == 0
        {
            return Ok(());
        }

        let ids = self.get_ids_from_sparql_rows()?;
        if ids.is_empty() {
            return Err(anyhow!("No items to show"));
        }
        self.ecw
            .load_entities(&self.wb_api, &ids)
            .await
            .map_err(|e| anyhow!("{e}"))?;

        self.label_columns();

        Ok(())
    }

    fn label_columns(&mut self) {
        let mut columns = vec![];
        for c in &self.columns {
            let mut c = c.clone();
            c.generate_label(self);
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
        let body = api.query_raw(&url, &api.no_params(), "GET").await?;
        let json: Value = serde_json::from_str(&body)?;
        match json["result"].as_str() {
            Some(result) => Ok(result.to_string()),
            None => Err(anyhow!("Not a valid autodesc result")),
        }
    }

    fn get_tmp_rows(&self) -> Result<HashMap<String, Vec<&HashMap<String, SparqlValue>>>> {
        let varname = self.get_var_name()?;
        let sparql_row_ids: HashSet<String> =
            self.get_ids_from_sparql_rows()?.into_iter().collect();
        let mut tmp_rows: HashMap<String, Vec<&HashMap<String, SparqlValue>>> = HashMap::new();
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

    pub fn generate_results(&mut self) -> Result<()> {
        let mut tmp_results: FileVec<ResultRow> = FileVec::new();
        match self.params.one_row_per_item() {
            true => {
                let sparql_row_ids: Vec<String> =
                    self.get_ids_from_sparql_rows()?.into_iter().collect(); // To preserve the original order
                let tmp_rows = self.get_tmp_rows()?;
                sparql_row_ids
                    .iter()
                    .filter_map(|id| {
                        tmp_rows
                            .get(id)
                            .map(|rows| self.ecw.get_result_row(id, rows, self))
                    })
                    .flatten()
                    .for_each(|row| tmp_results.push(row));
            }
            false => {
                let varname = self.get_var_name()?;
                self.sparql_rows
                    .iter()
                    .filter_map(|row| {
                        if let Some(SparqlValue::Entity(id)) = row.get(varname) {
                            if let Some(x) = self.ecw.get_result_row(id, &[&row], self) {
                                return Some(x);
                            }
                        }
                        None
                    })
                    .for_each(|row| tmp_results.push(row));
            }
        };
        self.results = tmp_results;
        Ok(())
    }

    fn process_items_to_local_links(&mut self) -> Result<()> {
        // Try to change items to local link
        let wiki = self.wiki().to_owned();
        let language = self.language().to_owned();
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
            for cell in row.cells_mut().iter_mut() {
                ResultCell::localize_item_links_in_parts(
                    cell.parts_mut(),
                    &self.ecw,
                    &wiki,
                    &language,
                );
            }
            self.results.set(row_id, row);
        }
        Ok(())
    }

    fn process_excess_files(&mut self) {
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
            row.remove_excess_files();
            self.results.set(row_id, row);
        }
    }

    fn check_this_wiki_for_shadow_images(&self) -> bool {
        self.page_params
            .config()
            .check_for_shadow_images(&self.page_params.wiki().to_string())
    }

    async fn process_remove_shadow_files(&mut self) -> Result<()> {
        if !self.check_this_wiki_for_shadow_images() {
            return Ok(());
        }
        let files_to_check = self.get_files_to_check();
        self.shadow_files.clear();
        let param_list: Vec<HashMap<String, String>> =
            self.get_param_list_for_files(&files_to_check);
        let page_params = self.page_params.clone();
        let api_read = page_params.mw_api().read().await;

        let mut futures = vec![];
        for params in &param_list {
            futures.push(api_read.get_query_api_json(params));
        }
        self.profile(&format!(
            "ListeriaList::process_remove_shadow_files running {} futures",
            futures.len()
        ));

        let tmp_results: Vec<(String, Value)> = join_all(futures)
            .await
            .iter()
            .zip(files_to_check)
            .filter_map(|(result, filename)| match result {
                Ok(j) => Some((filename, j.to_owned())),
                _ => None,
            })
            .collect();

        self.shadow_files = tmp_results
            .into_iter()
            .filter_map(|(filename, j)| {
                let could_be_local = match j["query"]["pages"].as_object() {
                    Some(results) => results
                        .iter()
                        .filter_map(|(_k, o)| o["imagerepository"].as_str())
                        .any(|s| s != "shared"),
                    None => true,
                };

                if could_be_local {
                    Some(filename)
                } else {
                    None
                }
            })
            .collect();

        // Remove shadow files from data table
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
            row.remove_shadow_files(&self.shadow_files);
            self.results.set(row_id, row);
        }

        Ok(())
    }

    fn get_files_to_check(&self) -> Vec<String> {
        let mut files_to_check = vec![];
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            for cell in row.cells() {
                for part in cell.parts() {
                    if let ResultCellPart::File(file) = &part.part {
                        files_to_check.push(file.to_owned());
                    }
                }
            }
        }
        files_to_check.sort_unstable();
        files_to_check.dedup();
        files_to_check
    }

    /// Get parameters for fileinfo API
    fn get_param_list_for_files(&self, files_to_check: &[String]) -> Vec<HashMap<String, String>> {
        files_to_check
            .iter()
            .map(|filename| {
                let prefixed_filename = format!(
                    "{}:{}",
                    self.page_params.local_file_namespace_prefix(),
                    &filename
                );
                let params: HashMap<String, String> = [
                    ("action", "query"),
                    ("titles", prefixed_filename.as_str()),
                    ("prop", "imageinfo"),
                ]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();
                params
            })
            .collect()
    }

    fn process_redlinks_only(&mut self) -> Result<()> {
        if *self.get_links_type() != LinksType::RedOnly {
            return Ok(());
        }

        // Remove all rows with existing local page
        let wiki = self.page_params.wiki().to_string();
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
            row.set_keep(match self.ecw.get_entity(row.entity_id()) {
                Some(entity) => {
                    match entity.sitelinks() {
                        Some(sl) => sl.iter().filter(|s| *s.site() == wiki).count() == 0,
                        None => true, // No sitelinks, keep
                    }
                }
                _ => false,
            });
            self.results.set(row_id, row);
        }
        self.results.retain(|r| r.keep());
        Ok(())
    }

    async fn process_redlinks(&mut self) -> Result<()> {
        if *self.get_links_type() != LinksType::RedOnly && *self.get_links_type() != LinksType::Red
        {
            return Ok(());
        }

        // Cache if local pages exist
        let mut ids = vec![];
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Entity((id, true)) = &part.part {
                        // _try_localize ?
                        ids.push(id.to_owned());
                    }
                })
            });
        }

        ids.sort();
        ids.dedup();
        let mut labels = vec![];
        for id in ids {
            if let Some(e) = self.get_entity(&id) {
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
                for row_id in 0..self.results.len() {
                    let row = self.results.get(row_id).unwrap();
                    sortkeys.push(row.get_sortkey_label(self));
                }
            }
            SortMode::FamilyName => {
                for row_id in 0..self.results.len() {
                    let row = self.results.get(row_id).unwrap();
                    sortkeys.push(row.get_sortkey_family_name(self));
                }
            }
            SortMode::Property(prop) => {
                datatype = self.ecw.get_datatype_for_property(prop);
                for row_id in 0..self.results.len() {
                    let row = self.results.get(row_id).unwrap();
                    sortkeys.push(row.get_sortkey_prop(prop, self, &datatype));
                }
            }
            SortMode::SparqlVariable(variable) => {
                for row_id in 0..self.results.len() {
                    let row = self.results.get(row_id).unwrap();
                    sortkeys.push(row.get_sortkey_sparql(variable, self));
                }
            }
            SortMode::None => return Ok(()),
        }

        // Apply sortkeys
        if self.results.len() != sortkeys.len() {
            // Paranoia
            return Err(anyhow!("process_sort_results: sortkeys length mismatch"));
        }
        (0..self.results.len()).for_each(|row_id| {
            let mut row = self.results.get(row_id).unwrap();
            row.set_sortkey(sortkeys[row_id].to_owned());
            self.results.set(row_id, row);
        });

        self.results.sort_by(|a, b| a.compare_to(b, &datatype))?;
        if *self.params.sort_order() == SortOrder::Descending {
            self.results.reverse()?;
        }

        Ok(())
    }

    async fn load_row_entities(&mut self) -> Result<()> {
        let mut items_to_load = vec![];
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            items_to_load.push(row.entity_id().to_string());
        }
        self.ecw
            .load_entities(&self.wb_api, &items_to_load)
            .await
            .map_err(|e| anyhow!("{e}"))?;
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
        }
        .to_owned();
        self.load_row_entities().await?;
        let datatype = self.ecw.get_datatype_for_property(&section_property);
        self.profile("AFTER list::process_assign_sections 1");

        let mut section_names_q = vec![];
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            section_names_q.push(row.get_sortkey_prop(&section_property, self, &datatype));
        }
        self.profile("AFTER list::process_assign_sections 2");

        // Make sure section name items are loaded
        self.ecw
            .load_entities(&self.wb_api, &section_names_q)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        self.profile("AFTER list::process_assign_sections 3a");
        let mut section_names = vec![];
        for q in section_names_q {
            let label = self.get_label_with_fallback(&q);
            section_names.push(label);
        }

        // Count names
        let mut section_count = HashMap::new();
        section_names.iter().for_each(|name| {
            *section_count.entry(name).or_insert(0) += 1;
        });
        self.profile("AFTER list::process_assign_sections 4");

        // Remove low counts
        section_count.retain(|&_name, &mut count| count >= self.params.min_section());
        self.profile("AFTER list::process_assign_sections 5");

        // Sort by section name
        let mut valid_section_names: Vec<String> =
            section_count.keys().map(|k| (*k).to_owned()).collect();
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

        self.assign_row_section_ids(section_names, name2id, misc_id);
        self.profile("AFTER list::process_assign_sections 9");

        Ok(())
    }

    fn assign_row_section_ids(
        &mut self,
        section_names: Vec<String>,
        name2id: HashMap<String, usize>,
        misc_id: usize,
    ) {
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
            let section_name = match section_names.get(row_id) {
                Some(name) => name,
                None => return,
            };
            let section_id = match name2id.get(section_name) {
                Some(id) => *id,
                None => misc_id,
            };
            row.set_section(section_id);
            self.results.set(row_id, row);
        }
    }

    async fn get_region_for_entity_id(&self, entity_id: &str) -> Option<String> {
        let wikibase_key = self.params.wikibase().to_lowercase();
        let sparql = format!(
            "SELECT ?q ?x {{ wd:{} wdt:P131* ?q . ?q wdt:P300 ?x }}",
            entity_id
        );
        let mut sparql_results = SparqlResults::new(self.page_params.clone(), &wikibase_key);
        sparql_results.simulate = false;
        let mut region = String::new();
        let rows = sparql_results.run_query(sparql).await.ok()?;
        for row in rows {
            match row.get("x") {
                Some(SparqlValue::Literal(r)) => {
                    if r.len() > region.len() {
                        region = r.to_string();
                    }
                }
                _ => return None,
            }
        }
        if region.is_empty() {
            None
        } else {
            Some(region)
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
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Location((_lat, _lon, _region)) = &part.part {
                        entity_ids.insert(row.entity_id().to_string());
                        //*region = self.get_region_for_entity_id(row.entity_id()).await ;
                    }
                });
            });
        }

        let mut entity_id2region = HashMap::new();
        for entity_id in entity_ids {
            if let Some(region) = self.get_region_for_entity_id(&entity_id).await {
                entity_id2region.insert(entity_id, region);
            }
        }

        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
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
            self.results.set(row_id, row);
        }

        Ok(())
    }

    async fn process_reference_items(&mut self) -> Result<()> {
        let mut items_to_load: Vec<String> = vec![];
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
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
            self.results.set(row_id, row);
        }
        if !items_to_load.is_empty() {
            items_to_load.sort_unstable();
            items_to_load.dedup();
            self.ecw
                .load_entities(&self.wb_api, &items_to_load)
                .await
                .map_err(|e| anyhow!("{e}"))?;
        }
        Ok(())
    }

    async fn fix_local_links(&mut self) -> Result<()> {
        // Set the is_category flag
        let mw_api = self.mw_api();
        let mw_api = mw_api.read().await;
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    if let ResultCellPart::LocalLink((page, _label, is_category)) = &mut part.part {
                        let title = wikimisc::mediawiki::title::Title::new_from_full(page, &mw_api);
                        *is_category = title.namespace_id() == 14;
                    } else if let ResultCellPart::SnakList(v) = &mut part.part {
                        for subpart in v.iter_mut() {
                            if let ResultCellPart::LocalLink((page, _label, is_category)) =
                                &mut subpart.part
                            {
                                let title =
                                    wikimisc::mediawiki::title::Title::new_from_full(page, &mw_api);
                                *is_category = title.namespace_id() == 14;
                            }
                        }
                    }
                }
            }
            self.results.set(row_id, row);
        }
        Ok(())
    }

    pub async fn process_results(&mut self) -> Result<()> {
        self.profile("START list::process_results");
        self.gather_and_load_items().await?;
        self.profile("AFTER list::process_results gather_and_load_items");
        self.fill_autodesc().await?;
        self.profile("AFTER list::process_results fill_autodesc");
        self.process_redlinks_only()?;
        self.profile("AFTER list::process_results process_redlinks_only");
        self.process_items_to_local_links()?;
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

    async fn fill_autodesc(&mut self) -> Result<()> {
        // Done in two different steps, otherwise get_autodesc_description() would borrow self when &mut self is already borrowed
        // TODO Maybe gather futures and run get_autodesc_description() in async/parallel?

        // Gather descriptions
        let mut autodescs = HashMap::new();
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            for cell in row.cells() {
                for part_with_reference in cell.parts() {
                    if let ResultCellPart::AutoDesc(ad) = &part_with_reference.part {
                        self.ecw
                            .load_entities(&self.wb_api, &[ad.entity_id().to_owned()])
                            .await
                            .map_err(|e| anyhow!("{e}"))?;
                        if let Some(entity) = self.ecw.get_entity(ad.entity_id()) {
                            if let Ok(desc) = self.get_autodesc_description(&entity).await {
                                autodescs.insert(ad.entity_id().to_owned(), desc);
                            }
                        }
                    }
                }
            }
        }

        // Set descriptions
        for row_id in 0..self.results.len() {
            let mut row = self.results.get(row_id).unwrap();
            for cell in row.cells_mut() {
                for part_with_reference in cell.parts_mut() {
                    if let ResultCellPart::AutoDesc(ad) = &mut part_with_reference.part {
                        if let Some(desc) = autodescs.get(ad.entity_id()) {
                            ad.set_description(desc)
                        }
                    }
                }
            }
            self.results.set(row_id, row);
        }
        Ok(())
    }

    pub fn get_links_type(&self) -> &LinksType {
        self.params.links()
    }

    pub fn get_entity(&self, entity_id: &str) -> Option<wikimisc::wikibase::Entity> {
        self.ecw.get_entity(entity_id)
    }

    pub fn get_row_template(&self) -> &Option<String> {
        self.params.row_template()
    }

    pub fn get_reference_parameter(&self) -> &ReferencesParameter {
        self.params.references()
    }

    async fn gather_items_for_property(&mut self, prop: &str) -> Result<Vec<String>> {
        let mut entities_to_load = vec![];
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            if let Some(entity) = self.ecw.get_entity(row.entity_id()) {
                self.get_filtered_claims(&entity, prop)
                    .iter()
                    .filter(|statement| statement.property() == prop)
                    .map(|statement| statement.main_snak())
                    .filter(|snak| *snak.datatype() == SnakDataType::WikibaseItem)
                    .filter_map(|snak| snak.data_value().to_owned())
                    .map(|datavalue| datavalue.value().to_owned())
                    .filter_map(|value| match value {
                        wikimisc::wikibase::value::Value::Entity(v) => Some(v.id().to_owned()),
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
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            for cell in row.cells() {
                EntityContainerWrapper::gather_entities_and_external_properties(cell.parts())
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
            .map_err(|e| anyhow!("{e}"))?;

        entities_to_load = self.gather_items_sort().await?;
        let mut v2 = self.gather_items_section().await?;
        entities_to_load.append(&mut v2);
        match self
            .ecw
            .load_entities(&self.wb_api, &entities_to_load)
            .await
        {
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
        let mut ret = vec![];
        for row_id in 0..self.results.len() {
            let row = self.results.get(row_id).unwrap();
            ret.push(row.section());
        }
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

    pub fn get_label_with_fallback(&self, entity_id: &str) -> String {
        self.ecw
            .get_entity_label_with_fallback(entity_id, self.language())
    }

    pub fn get_label_with_fallback_lang(&self, entity_id: &str, language: &str) -> String {
        self.ecw.get_entity_label_with_fallback(entity_id, language)
    }

    pub fn is_main_wikibase_wiki(&self) -> bool {
        let default_wiki = format!("{}wiki", self.page_params.config().get_default_api());
        self.page_params.wiki() == default_wiki
    }

    pub fn get_item_wiki_target(&self, entity_id: &str) -> String {
        let prefix = if self.is_main_wikibase_wiki() {
            ""
        } else {
            ":d:"
        };
        if let Some(first_char) = entity_id.chars().next() {
            if first_char == 'p' || first_char == 'P' {
                return format!("{}Property:{}", prefix, entity_id);
            }
        }
        format!("{}{}", prefix, entity_id)
    }

    pub fn get_item_link_with_fallback(&self, entity_id: &str) -> String {
        let quotes = if self.is_main_wikibase_wiki() {
            ""
        } else {
            "''"
        };
        let label = self.get_label_with_fallback(entity_id);
        let label_part = if self.is_main_wikibase_wiki() && entity_id == label {
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
        e: &wikimisc::wikibase::entity::Entity,
        property: &str,
    ) -> Vec<Statement> {
        let mut ret: Vec<Statement> = e
            .claims_with_property(property)
            .iter()
            .map(|x| (*x).clone())
            .collect();

        if self.page_params.config().prefer_preferred() {
            let has_preferred = ret.iter().any(|x| *x.rank() == StatementRank::Preferred);
            if has_preferred {
                ret.retain(|x| *x.rank() == StatementRank::Preferred);
            }
            ret
        } else {
            ret
        }
    }

    pub fn entity_to_local_link(&self, item: &str) -> Option<ResultCellPart> {
        self.ecw
            .entity_to_local_link(item, self.wiki(), &self.language)
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
