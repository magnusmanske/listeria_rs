//! Core list processing logic.
//!
//! Handles the full pipeline from SPARQL query execution to rendered output.

use crate::column::Column;
use crate::column_type::ColumnType;
use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::list_processor::ListProcessor;
use crate::page_params::PageParams;
use crate::result_cell_part::ResultCellPart;
use crate::result_generator::ResultGenerator;
use crate::result_row::ResultRow;
use crate::sparql_results::SparqlResults;
use crate::template::Template;
use crate::template_params::LinksType;
use crate::template_params::ReferencesParameter;
use crate::template_params::SectionType;
use crate::template_params::SortMode;
use crate::template_params::TemplateParams;
use crate::wiki::Wiki;
use anyhow::{Result, anyhow};
use chrono::DateTime;
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use wikimisc::mediawiki::api::Api;
use wikimisc::sparql_table::SparqlTable;
use wikimisc::wikibase::{Entity, EntityTrait, SnakDataType, Statement, StatementRank};

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

    pub fn profile(&mut self, msg: &str) {
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

    /// Main processing pipeline: parses template, runs SPARQL query, and generates results.
    pub async fn process(&mut self) -> Result<()> {
        self.profile("START list::process");
        self.process_template()?;
        self.profile("AFTER list::process process_template");
        self.run_query().await?;
        self.profile("AFTER list::process run_query");
        self.load_entities().await?;
        self.profile("AFTER list::process load_entities");
        ResultGenerator::generate_results(self).await?;
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

    pub const fn shadow_files_mut(&mut self) -> &mut HashSet<String> {
        &mut self.shadow_files
    }

    pub const fn local_page_cache_mut(&mut self) -> &mut HashMap<String, bool> {
        &mut self.local_page_cache
    }

    pub const fn section_id_to_name_mut(&mut self) -> &mut HashMap<usize, String> {
        &mut self.section_id_to_name
    }

    pub const fn ecw(&self) -> &EntityContainerWrapper {
        &self.ecw
    }

    pub const fn page_params(&self) -> &Arc<PageParams> {
        &self.page_params
    }

    pub const fn wb_api(&self) -> &Arc<Api> {
        &self.wb_api
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
        match Self::get_template_value(&self.template, "columns") {
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

        self.params = TemplateParams::new_from_params(&self.template, &self.page_params.config());
        if let Some(s) = Self::get_template_value(&self.template, "links") {
            self.params
                .set_links(LinksType::new_from_string(s.to_string()));
        }
        if let Some(l) = Self::get_template_value(&self.template, "language") {
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

    pub async fn cache_local_pages_exist(&self, pages: &[String]) -> Vec<(String, bool)> {
        let params: HashMap<String, String> = [
            ("action", "query"),
            ("prop", ""),
            ("titles", pages.join("|").as_str()),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let result = match self.page_params.mw_api().get_query_api_json(&params).await {
            Ok(r) => r,
            Err(_) => return vec![],
        };

        let mut normalized: HashMap<String, String> = pages
            .iter()
            .map(|page| (page.to_string(), page.to_string()))
            .collect();

        if let Some(query_normalized) = result["query"]["normalized"].as_array() {
            for n in query_normalized {
                if let (Some(from), Some(to)) = (n["from"].as_str(), n["to"].as_str()) {
                    normalized.insert(to.to_string(), from.to_string());
                }
            }
        }

        result["query"]["pages"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .filter_map(|(_k, v)| {
                        v["title"].as_str().and_then(|title| {
                            normalized.contains_key(title).then(|| {
                                let page_exists = v["missing"].as_str().is_none();
                                (title.to_string(), page_exists)
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
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

        let ids = ResultGenerator::get_ids_from_sparql_rows(self)?;
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

    pub async fn load_row_entities(&mut self) -> Result<()> {
        let mut items_to_load = Vec::with_capacity(self.results.len());
        for (_row_id, row) in self.results_iter() {
            items_to_load.push(row.entity_id().to_string());
        }
        self.ecw.load_entities(&self.wb_api, &items_to_load).await?;
        Ok(())
    }

    pub async fn process_results(&mut self) -> Result<()> {
        self.profile("START list::process_results");
        self.gather_and_load_items().await?;
        self.profile("AFTER list::process_results gather_and_load_items");
        ListProcessor::fill_autodesc(self).await?;
        self.profile("AFTER list::process_results fill_autodesc");
        ListProcessor::process_redlinks_only(self).await?;
        self.profile("AFTER list::process_results process_redlinks_only");
        ListProcessor::process_items_to_local_links(self).await?;
        self.profile("AFTER list::process_results process_items_to_local_links");
        ListProcessor::process_redlinks(self).await?;
        self.profile("AFTER list::process_results process_redlinks");
        ListProcessor::process_remove_shadow_files(self).await?;
        self.profile("AFTER list::process_results process_remove_shadow_files");
        ListProcessor::process_excess_files(self)?;
        self.profile("AFTER list::process_results process_excess_files");
        ListProcessor::process_reference_items(self).await?;
        self.profile("AFTER list::process_results process_reference_items");
        ListProcessor::process_sort_results(self).await?;
        self.profile("AFTER list::process_results process_sort_results");
        ListProcessor::process_assign_sections(self).await?;
        self.profile("AFTER list::process_results process_assign_sections");
        ListProcessor::process_regions(self).await?;
        self.profile("AFTER list::process_results process_regions");
        ListProcessor::fix_local_links(self).await?;
        self.profile("AFTER list::process_results fix_local_links");
        self.profile("END list::process_results");
        Ok(())
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
            SectionType::Property(p) => p.to_string(),
            SectionType::SparqlVariable(_v) => {
                return Err(anyhow!("SPARQL variable section type not supported yet"));
            }
            SectionType::None => return Ok(vec![]), // Nothing to do
        };
        self.gather_items_for_property(&prop).await
    }

    async fn gather_items_sort(&mut self) -> Result<Vec<String>> {
        let prop = match self.params.sort() {
            SortMode::Property(prop) => prop.to_string(),
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
        if entity_id.starts_with('p') || entity_id.starts_with('P') {
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
