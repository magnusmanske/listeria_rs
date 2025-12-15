use crate::column::{Column, ColumnType};
use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::page_params::PageParams;
use crate::result_cell::ResultCell;
use crate::result_cell_part::{LinkTarget, ResultCellPart};
use crate::result_row::ResultRow;
use crate::sparql_results::SparqlResults;
use crate::template::Template;
use crate::template_params::LinksType;
use crate::template_params::ReferencesParameter;
use crate::template_params::SectionType;
use crate::template_params::SortMode;
use crate::template_params::SortOrder;
use crate::template_params::TemplateParams;
use crate::wiki::Wiki;
use anyhow::{Result, anyhow};
use chrono::DateTime;
use chrono::Utc;
use futures::StreamExt;
use futures::future::join_all;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use wikimisc::mediawiki::api::Api;
use wikimisc::sparql_table::SparqlTable;
use wikimisc::sparql_value::SparqlValue;
use wikimisc::wikibase::{Entity, EntityTrait, SnakDataType, Statement, StatementRank};

const MAX_CONCURRENT_REDLINKS_REQUESTS: usize = 5;
const AUTODESC_FALLBACK: &str = "FALLBACK";

#[derive(Debug, Clone)]
pub struct ListeriaList {
    page_params: Arc<PageParams>,
    template: Template,
    columns: Vec<Column>,
    params: TemplateParams,
    sparql_table: SparqlTable,
    ecw: EntityContainerWrapper,
    results: Vec<ResultRow>,
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
    /// Helper iterator for safely iterating over results with their indices
    fn results_iter(&self) -> impl Iterator<Item = (usize, &ResultRow)> + '_ {
        (0..self.results.len()).filter_map(|id| self.results.get(id).map(|row| (id, row)))
    }

    /// Helper iterator for safely iterating over results with mutable access
    fn results_iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut ResultRow)> + '_ {
        let len = self.results.len();
        self.results.iter_mut().enumerate().take(len)
    }

    pub async fn new(template: Template, page_params: Arc<PageParams>) -> Result<Self> {
        let wb_api = page_params.wb_api();
        let mut template = template;
        template.fix_values();
        Ok(Self {
            page_params: page_params.clone(),
            template,
            columns: vec![],
            params: TemplateParams::new(),
            sparql_table: SparqlTable::new(),
            ecw: EntityContainerWrapper::new().await?,
            results: Vec::new(),
            shadow_files: HashSet::new(),
            local_page_cache: HashMap::new(),
            section_id_to_name: HashMap::new(),
            wb_api,
            language: page_params.language().to_string(),
            reference_ids: HashSet::new(),
            profiling: page_params.config().profiling(),
            last_timestamp: Utc::now(),
        })
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

    pub const fn results(&self) -> &Vec<ResultRow> {
        &self.results
    }

    pub const fn results_mut(&mut self) -> &mut Vec<ResultRow> {
        &mut self.results
    }

    pub const fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub const fn shadow_files(&self) -> &HashSet<String> {
        &self.shadow_files
    }

    pub const fn reference_ids(&self) -> &HashSet<String> {
        &self.reference_ids
    }

    pub const fn sparql_table(&self) -> &SparqlTable {
        &self.sparql_table
    }

    pub fn local_file_namespace_prefix(&self) -> &String {
        self.page_params.local_file_namespace_prefix()
    }

    pub fn get_wiki(&self) -> Option<Wiki> {
        let wiki = self.page_params.wiki();
        self.page_params
            .config()
            .get_wiki(wiki)
            .map(|wiki| wiki.to_owned())
    }

    pub fn section_name(&self, id: usize) -> Option<&String> {
        self.section_id_to_name.get(&id)
    }

    pub fn process_template(&mut self) -> Result<()> {
        let template = self.template.clone();
        match Self::get_template_value(&template, "columns") {
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
        if let Some(s) = Self::get_template_value(&template, "links") {
            self.params
                .set_links(LinksType::new_from_string(s.to_string()));
        }
        if let Some(l) = Self::get_template_value(&template, "language") {
            self.language = l.to_lowercase();
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

    async fn cache_local_pages_exist(&self, pages: &[String]) -> Vec<(String, bool)> {
        let params: HashMap<String, String> = [
            ("action", "query"),
            ("prop", ""),
            ("titles", pages.join("|").as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();

        let result = match self.page_params.mw_api().get_query_api_json(&params).await {
            Ok(r) => r,
            Err(_e) => return vec![],
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

        let mut ret = vec![];
        if let Some(obj) = result["query"]["pages"].as_object() {
            for (_k, v) in obj.iter() {
                if let Some(title) = v["title"].as_str()
                    && normalized.contains_key(title)
                {
                    let page_exists = v["missing"].as_str().is_none();
                    ret.push((title.to_string(), page_exists));
                    // self.local_page_cache.insert(title.to_string(), page_exists);
                }
            }
        };
        ret
    }

    pub fn local_page_exists(&self, page: &str) -> bool {
        *self.local_page_cache.get(page).unwrap_or(&false)
    }

    fn first_letter_to_upper_case(s1: &str) -> String {
        let mut c = s1.chars();
        c.next()
            .map(|f| f.to_uppercase().collect::<String>() + c.as_str())
            .unwrap_or_default()
    }

    pub fn normalize_page_title(&self, s: &str) -> String {
        // TODO use page to find out about first character capitalization on the current wiki
        if s.len() < 2 {
            return s.to_owned();
        }
        Self::first_letter_to_upper_case(s)
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
            .replace("$LAT$", &format!("{lat}"))
            .replace("$LON$", &format!("{lon}"))
            .replace("$ITEM$", &entity_id.unwrap_or_default())
            .replace("$REGION$", &region.unwrap_or_default())
    }

    pub fn thumbnail_size(&self) -> u64 {
        let default = self.page_params.config().default_thumbnail_size();
        Self::get_template_value(&self.template, "thumb")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(default)
    }

    fn get_template_value(template: &Template, key: &str) -> Option<String> {
        template
            .params()
            .iter()
            .filter(|(k, _v)| k.to_lowercase() == key.to_lowercase())
            .map(|(_k, v)| v.to_owned())
            .next()
    }

    pub async fn run_query(&mut self) -> Result<()> {
        let wikibase_key = self.params.wikibase().to_lowercase();
        let sparql = match Self::get_template_value(&self.template, "sparql") {
            Some(s) => s,
            None => return Err(anyhow!("No 'sparql' parameter in {:?}", &self.template)),
        };
        let mut sparql_results = SparqlResults::new(self.page_params.clone(), &wikibase_key);
        if self.page_params.config().is_single_wiki()
            && let Some(endpoint) = self.page_params.config().query_endpoint()
        {
            sparql_results = sparql_results.with_query_endpoint(endpoint);
        }
        self.sparql_table = sparql_results.run_query(sparql).await?;
        self.sparql_table
            .set_main_variable(sparql_results.sparql_main_variable());
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
                    c.obj(),
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
        self.ecw.load_entities(&self.wb_api, &ids).await?;

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
        let var_index = self.get_var_index()?;
        let mut ids_tmp = vec![];
        for row_id in 0..self.sparql_table.len() {
            if let Some(SparqlValue::Entity(id)) = self.sparql_table.get_row_col(row_id, var_index)
            {
                ids_tmp.push(id.to_string());
            }
        }

        // Can't sort/dedup, need to preserve original order!
        let mut ids: Vec<String> = vec![];
        ids_tmp.iter().for_each(|id| {
            if !ids.contains(id) {
                ids.push(id.to_string());
            }
        });

        // Column headers
        self.columns.iter().for_each(|c| match c.obj() {
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

    pub async fn get_autodesc_description(&self, e: &Entity) -> Result<String> {
        if self.params.autodesc() != Some(AUTODESC_FALLBACK.to_string()) {
            return Err(anyhow!("Not used"));
        }
        if let Some(autodesc) = &self.page_params.simulated_autodesc() {
            for ad in autodesc {
                let parts: Vec<&str> = ad.splitn(3, '|').collect();
                if let [id, lang, ret] = parts.as_slice()
                    && id == e.id()
                    && *lang == self.language
                {
                    return Ok(ret.trim().to_string());
                }
            }
        }
        let url = format!(
            "https://autodesc.toolforge.org/?q={}&lang={}&mode=short&links=wiki&format=json",
            e.id(),
            self.language
        );
        let api = self.page_params.mw_api();
        let body = api.query_raw(&url, &api.no_params(), "GET").await?;
        let json: Value = serde_json::from_str(&body)?;
        json["result"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Not a valid autodesc result"))
    }

    fn get_var_index(&self) -> Result<usize> {
        self.sparql_table
            .main_column()
            .ok_or_else(|| anyhow!("Could not find SPARQL variable in results"))
    }

    pub async fn generate_results(&mut self) -> Result<()> {
        let mut tmp_results: Vec<ResultRow> = Vec::new();
        if self.params.one_row_per_item() {
            self.generate_results_one_row_per_item(&mut tmp_results)
                .await?;
        } else {
            self.generate_results_multiple_rows_per_item(&mut tmp_results)
                .await?;
        };
        self.results = tmp_results;
        Ok(())
    }

    async fn generate_results_multiple_rows_per_item(
        &mut self,
        tmp_results: &mut Vec<ResultRow>,
    ) -> Result<()> {
        let var_index = self.get_var_index()?;
        for row_id in 0..self.sparql_table.len() {
            let row = match self.sparql_table.get(row_id) {
                Some(row) => row,
                None => {
                    continue;
                }
            };
            let v = row.get(var_index).map(|v| v.to_owned());
            if let Some(Some(SparqlValue::Entity(id))) = v {
                let mut tmp_table = SparqlTable::from_table(&self.sparql_table);
                tmp_table.push(row.to_owned());
                if let Some(x) = self.ecw.get_result_row(&id, &tmp_table, self).await {
                    tmp_results.push(x);
                }
            }
        }
        Ok(())
    }

    async fn generate_results_one_row_per_item(
        &mut self,
        tmp_results: &mut Vec<ResultRow>,
    ) -> Result<()> {
        let var_index = self.get_var_index()?;
        let sparql_row_ids: Vec<String> = self.get_ids_from_sparql_rows()?.into_iter().collect();
        let mut id2rows: HashMap<String, Vec<usize>> = HashMap::new();
        for row_id in 0..self.sparql_table.len() {
            if let Some(SparqlValue::Entity(id)) = self.sparql_table.get_row_col(row_id, var_index)
            {
                id2rows.entry(id.to_string()).or_default().push(row_id);
            };
        }
        for id in &sparql_row_ids {
            let mut tmp_rows = SparqlTable::from_table(&self.sparql_table);
            let row_ids = id2rows.get(id).map(|v| v.to_owned()).unwrap_or_default();
            for row_id in row_ids {
                if let Some(row) = self.sparql_table.get(row_id) {
                    tmp_rows.push(row);
                }
            }
            if let Some(row) = self.ecw.get_result_row(id, &tmp_rows, self).await {
                tmp_results.push(row);
            }
        }
        Ok(())
    }

    async fn process_items_to_local_links(&mut self) -> Result<()> {
        // Try to change items to local link
        let wiki = self.wiki().to_owned();
        let language = self.language().to_owned();
        for row_id in 0..self.results.len() {
            let row = match self.results.get_mut(row_id) {
                Some(row) => row,
                None => continue,
            };
            for cell in row.cells_mut().iter_mut() {
                ResultCell::localize_item_links_in_parts(
                    cell.parts_mut(),
                    &self.ecw, // &ecw,
                    &wiki,
                    &language,
                )
                .await;
            }
            // self.results.set(row_id, row)?;
        }
        Ok(())
    }

    fn process_excess_files(&mut self) -> Result<()> {
        for (_row_id, row) in self.results_iter_mut() {
            row.remove_excess_files();
        }
        Ok(())
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
        let api_read = page_params.mw_api();

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

                if could_be_local { Some(filename) } else { None }
            })
            .collect();

        // Remove shadow files from data table
        for row_id in 0..self.results.len() {
            let row = match self.results.get_mut(row_id) {
                Some(row) => row,
                None => continue,
            };
            row.remove_shadow_files(&self.shadow_files);
        }

        Ok(())
    }

    fn get_files_to_check(&self) -> Vec<String> {
        let mut files_to_check = vec![];
        for (_row_id, row) in self.results_iter() {
            for cell in row.cells() {
                for part in cell.parts() {
                    if let ResultCellPart::File(file) = part.part() {
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

    async fn process_redlinks_only(&mut self) -> Result<()> {
        if *self.get_links_type() != LinksType::RedOnly {
            return Ok(());
        }

        // Remove all rows with existing local page
        let wiki = self.page_params.wiki().to_string();
        for row_id in 0..self.results.len() {
            let row = match self.results.get_mut(row_id) {
                Some(row) => row,
                None => continue,
            };
            row.set_keep(match self.ecw.get_entity(row.entity_id()).await {
                Some(entity) => {
                    match entity.sitelinks() {
                        Some(sl) => sl.iter().filter(|s| *s.site() == wiki).count() == 0,
                        None => true, // No sitelinks, keep
                    }
                }
                _ => false,
            });
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
        for (_row_id, row) in self.results_iter() {
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Entity((id, true)) = part.part() {
                        // _try_localize ?
                        ids.push(id.to_owned());
                    }
                });
            });
        }

        ids.sort();
        ids.dedup();
        let mut labels = vec![];
        for id in ids {
            if let Some(e) = self.get_entity(&id).await
                && let Some(l) = e.label_in_locale(self.language())
            {
                labels.push(l.to_string());
            }
        }

        labels.sort();
        labels.dedup();
        // TODO in parallel
        let labels_per_chunk = if self.page_params.mw_api().user().is_bot() {
            500
        } else {
            50
        };

        // for chunk in labels.chunks(labels_per_chunk) {
        //     self.cache_local_pages_exist(chunk).await;
        // }

        let mut futures = vec![];
        for chunk in labels.chunks(labels_per_chunk) {
            let future = self.cache_local_pages_exist(chunk);
            futures.push(future);
        }
        let stream =
            futures::stream::iter(futures).buffer_unordered(MAX_CONCURRENT_REDLINKS_REQUESTS);
        let results = stream.collect::<Vec<_>>().await;
        for (title, page_exists) in results.into_iter().flatten() {
            self.local_page_cache.insert(title, page_exists);
        }

        Ok(())
    }

    async fn process_sort_results(&mut self) -> Result<()> {
        let mut sortkeys: Vec<String> = vec![];
        // Default
        let mut datatype = SnakDataType::String;
        // println!("Sorting by {:?}", self.params.sort());
        self.profile("BEFORE process_sort_results SORTKEYS");
        match &self.params.sort() {
            SortMode::Label => {
                self.load_row_entities().await?;
                for (_row_id, row) in self.results_iter() {
                    sortkeys.push(row.get_sortkey_label(self).await);
                }
            }
            SortMode::FamilyName => {
                for (_row_id, row) in self.results_iter() {
                    sortkeys.push(row.get_sortkey_family_name(self).await);
                }
            }
            SortMode::Property(prop) => {
                datatype = self.ecw.get_datatype_for_property(prop).await;
                for (_row_id, row) in self.results_iter() {
                    sortkeys.push(row.get_sortkey_prop(prop, self, &datatype).await);
                }
            }
            SortMode::SparqlVariable(variable) => {
                for (_row_id, row) in self.results_iter() {
                    sortkeys.push(row.get_sortkey_sparql(variable, self));
                }
            }
            SortMode::None => return Ok(()),
        }
        self.profile("AFTER process_sort_results SORTKEYS");

        let ret = self.process_sort_results_finish(sortkeys, datatype);
        self.profile("AFTER process_sort_results_finish");
        ret
    }

    fn process_sort_results_finish(
        &mut self,
        sortkeys: Vec<String>,
        datatype: SnakDataType,
    ) -> Result<()> {
        // Apply sortkeys
        if self.results.len() != sortkeys.len() {
            // Paranoia
            return Err(anyhow!("process_sort_results: sortkeys length mismatch"));
        }

        for row_id in 0..self.results.len() {
            if let Some(row) = self.results.get_mut(row_id)
                && let Some(sk) = sortkeys.get(row_id)
            {
                row.set_sortkey(sk.to_owned());
            };
        }

        self.profile(&format!(
            "BEFORE process_sort_results_finish sort of {} items",
            self.results.len()
        ));
        self.results.sort_by(|a, b| a.compare_to(b, &datatype));
        self.profile("AFTER process_sort_results_finish sort");
        if *self.params.sort_order() == SortOrder::Descending {
            self.results.reverse();
        }
        self.profile("AFTER process_sort_results_finish reverse");

        Ok(())
    }

    async fn load_row_entities(&mut self) -> Result<()> {
        let mut items_to_load = vec![];
        for (_row_id, row) in self.results_iter() {
            items_to_load.push(row.entity_id().to_string());
        }
        self.ecw.load_entities(&self.wb_api, &items_to_load).await?;
        Ok(())
    }

    pub async fn process_assign_sections(&mut self) -> Result<()> {
        self.profile("BEFORE list::process_assign_sections");

        // TODO all SectionType options
        let section_property = match self.params.section() {
            SectionType::Property(p) => p,
            SectionType::SparqlVariable(_v) => {
                return Err(anyhow!("SPARQL variable section type not supported yet"));
            }
            SectionType::None => return Ok(()), // Nothing to do
        }
        .to_owned();
        self.load_row_entities().await?;
        let datatype = self.ecw.get_datatype_for_property(&section_property).await;
        self.profile("AFTER list::process_assign_sections 1");

        let mut section_names_q = vec![];
        for (_row_id, row) in self.results_iter() {
            section_names_q.push(
                row.get_sortkey_prop(&section_property, self, &datatype)
                    .await,
            );
        }
        self.profile("AFTER list::process_assign_sections 2");

        // Make sure section name items are loaded
        self.ecw
            .load_entities(&self.wb_api, &section_names_q)
            .await?;
        self.profile("AFTER list::process_assign_sections 3a");
        let mut section_names = vec![];
        for q in section_names_q {
            let label = self.get_label_with_fallback(&q).await;
            section_names.push(label);
        }

        // Count names
        let mut section_count = HashMap::new();
        for name in &section_names {
            *section_count.entry(name).or_insert(0) += 1;
        }
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

        self.assign_row_section_ids(section_names, name2id, misc_id)?;
        self.profile("AFTER list::process_assign_sections 9");

        Ok(())
    }

    fn assign_row_section_ids(
        &mut self,
        section_names: Vec<String>,
        name2id: HashMap<String, usize>,
        misc_id: usize,
    ) -> Result<()> {
        for (row_id, row) in self.results_iter_mut() {
            let section_name = match section_names.get(row_id) {
                Some(name) => name,
                None => continue,
            };
            let section_id = match name2id.get(section_name) {
                Some(id) => *id,
                None => misc_id,
            };
            row.set_section(section_id);
        }
        Ok(())
    }

    async fn get_region_for_entity_id(&self, entity_id: &str) -> Option<String> {
        let wikibase_key = self.params.wikibase().to_lowercase();
        let sparql = format!("SELECT ?q ?x {{ wd:{entity_id} wdt:P131* ?q . ?q wdt:P300 ?x }}");
        let mut sparql_results = SparqlResults::new(self.page_params.clone(), &wikibase_key);
        sparql_results.set_simulate(false);
        let mut region = String::new();
        let sparql_table = sparql_results.run_query(sparql).await.ok()?;
        let x_idx = sparql_table.get_var_index("x")?;
        for row_id in 0..sparql_table.len() {
            match sparql_table.get_row_col(row_id, x_idx) {
                Some(SparqlValue::Literal(r)) => {
                    if r.len() > region.len() {
                        region = r.to_string();
                    }
                }
                _ => continue,
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
        for (_row_id, row) in self.results_iter() {
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Location((_lat, _lon, _region)) = part.part() {
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

        for (_row_id, row) in self.results_iter_mut() {
            let the_region = match entity_id2region.get(row.entity_id()) {
                Some(r) => r,
                None => continue,
            };
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    if let ResultCellPart::Location((_lat, _lon, region)) = part.part_mut() {
                        *region = Some(the_region.clone());
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_reference_items(&mut self) -> Result<()> {
        let mut items_to_load: Vec<String> = vec![];
        for (_row_id, row) in self.results_iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                for part_with_reference in cell.parts_mut().iter_mut() {
                    if let Some(references) = part_with_reference.references() {
                        for reference in references.iter() {
                            if let Some(stated_in) = &reference.stated_in() {
                                items_to_load.push(stated_in.to_string());
                            }
                        }
                    }
                }
            }
        }
        if !items_to_load.is_empty() {
            items_to_load.sort_unstable();
            items_to_load.dedup();
            self.ecw.load_entities(&self.wb_api, &items_to_load).await?;
        }
        Ok(())
    }

    async fn fix_local_links(&mut self) -> Result<()> {
        // Set the is_category flag
        let mw_api = self.mw_api();
        for (_row_id, row) in self.results_iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    if let ResultCellPart::LocalLink((page, _label, link_target)) = part.part_mut()
                    {
                        let title = wikimisc::mediawiki::title::Title::new_from_full(page, &mw_api);
                        *link_target = match title.namespace_id() {
                            14 => LinkTarget::Category,
                            _ => LinkTarget::Page,
                        }
                    } else if let ResultCellPart::SnakList(v) = part.part_mut() {
                        for subpart in v.iter_mut() {
                            if let ResultCellPart::LocalLink((page, _label, link_target)) =
                                subpart.part_mut()
                            {
                                let title =
                                    wikimisc::mediawiki::title::Title::new_from_full(page, &mw_api);
                                *link_target = match title.namespace_id() {
                                    14 => LinkTarget::Category,
                                    _ => LinkTarget::Page,
                                }
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
        self.fill_autodesc().await?;
        self.profile("AFTER list::process_results fill_autodesc");
        self.process_redlinks_only().await?;
        self.profile("AFTER list::process_results process_redlinks_only");
        self.process_items_to_local_links().await?;
        self.profile("AFTER list::process_results process_items_to_local_links");
        self.process_redlinks().await?;
        self.profile("AFTER list::process_results process_redlinks");
        self.process_remove_shadow_files().await?;
        self.profile("AFTER list::process_results process_remove_shadow_files");
        self.process_excess_files()?;
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
        let autodescs = self.fill_autodesc_gather_descriptions().await?;
        self.fill_autodesc_set_descriptions(autodescs)?;
        Ok(())
    }

    fn fill_autodesc_set_descriptions(&mut self, autodescs: HashMap<String, String>) -> Result<()> {
        for (_row_id, row) in self.results_iter_mut() {
            for cell in row.cells_mut() {
                for part_with_reference in cell.parts_mut() {
                    if let ResultCellPart::AutoDesc(ad) = part_with_reference.part_mut()
                        && let Some(desc) = autodescs.get(ad.entity_id())
                    {
                        ad.set_description(desc);
                    }
                }
            }
        }
        Ok(())
    }

    async fn fill_autodesc_gather_descriptions(&mut self) -> Result<HashMap<String, String>> {
        let mut autodescs = HashMap::new();
        for (_row_id, row) in self.results_iter() {
            for cell in row.cells() {
                for part_with_reference in cell.parts() {
                    if let ResultCellPart::AutoDesc(ad) = part_with_reference.part() {
                        self.ecw
                            .load_entities(&self.wb_api, &[ad.entity_id().to_owned()])
                            .await?;
                        if let Some(entity) = self.ecw.get_entity(ad.entity_id()).await
                            && let Ok(desc) = self.get_autodesc_description(&entity).await
                        {
                            autodescs.insert(ad.entity_id().to_owned(), desc);
                        }
                    }
                }
            }
        }
        Ok(autodescs)
    }

    pub const fn get_links_type(&self) -> &LinksType {
        self.params.links()
    }

    pub async fn get_entity(&self, entity_id: &str) -> Option<wikimisc::wikibase::Entity> {
        self.ecw.get_entity(entity_id).await
    }

    pub const fn get_row_template(&self) -> &Option<String> {
        self.params.row_template()
    }

    pub const fn get_reference_parameter(&self) -> &ReferencesParameter {
        self.params.references()
    }

    async fn gather_items_for_property(&mut self, prop: &str) -> Result<Vec<String>> {
        let mut entities_to_load = vec![];
        for (_row_id, row) in self.results_iter() {
            if let Some(entity) = self.ecw.get_entity(row.entity_id()).await {
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
                return Err(anyhow!("SPARQL variable section type not supported yet"));
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
        for (_row_id, row) in self.results_iter() {
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
                return Err(anyhow!("SPARQL variable section type not supported yet"));
            }
            SectionType::None => {}
        }
        self.ecw
            .load_entities(&self.wb_api, &entities_to_load)
            .await?;

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

    pub const fn skip_table(&self) -> bool {
        self.params.skip_table()
    }

    pub fn get_section_ids(&self) -> Vec<usize> {
        let mut ret = vec![];
        for (_row_id, row) in self.results_iter() {
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

    pub const fn summary(&self) -> &Option<String> {
        self.params.summary()
    }

    pub const fn header_template(&self) -> &Option<String> {
        self.params.header_template()
    }

    pub async fn get_label_with_fallback(&self, entity_id: &str) -> String {
        self.ecw
            .get_entity_label_with_fallback(entity_id, self.language())
            .await
    }

    pub async fn get_label_with_fallback_lang(&self, entity_id: &str, language: &str) -> String {
        self.ecw
            .get_entity_label_with_fallback(entity_id, language)
            .await
    }

    pub fn is_main_wikibase_wiki(&self) -> bool {
        if self.page_params.config().is_single_wiki() {
            true
        } else {
            let default_wiki = format!("{}wiki", self.page_params.config().get_default_api());
            self.page_params.wiki() == default_wiki
        }
    }

    pub fn get_item_wiki_target(&self, entity_id: &str) -> String {
        let prefix = if self.is_main_wikibase_wiki() {
            self.page_params.config().main_item_prefix()
        } else {
            ":d:".to_string()
        };
        if let Some(first_char) = entity_id.chars().next()
            && (first_char == 'p' || first_char == 'P')
        {
            return format!("{prefix}Property:{entity_id}");
        }
        format!("{prefix}{entity_id}")
    }

    pub async fn get_item_link_with_fallback(&self, entity_id: &str) -> String {
        let quotes = if self.is_main_wikibase_wiki() {
            ""
        } else {
            "''"
        };
        let label = self.get_label_with_fallback(entity_id).await;
        let label_part = if self.is_main_wikibase_wiki() && entity_id == label {
            String::new()
        } else {
            format!("|{label}")
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

    pub async fn entity_to_local_link(&self, item: &str) -> Option<ResultCellPart> {
        self.ecw
            .entity_to_local_link(item, self.wiki(), &self.language)
            .await
    }

    pub fn default_language(&self) -> String {
        self.page_params.config().default_language().to_string()
    }

    pub const fn template_params(&self) -> &TemplateParams {
        &self.params
    }

    pub fn mw_api(&self) -> crate::ApiArc {
        self.page_params.mw_api().clone()
    }
}
