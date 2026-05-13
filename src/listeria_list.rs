//! Core list processing logic.
//!
//! Handles the full pipeline from SPARQL query execution to rendered output.

use crate::column::Column;
use crate::column_type::ColumnType;
use crate::entity_container_wrapper::{EntityContainerWrapper, EntityEntry};
use crate::list_processor::ListProcessor;
use crate::listeria_error::ListeriaError;
use crate::my_entity::MyEntity;
use crate::page_params::PageParams;
use crate::profiling_service::ProfilingService;
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
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use wikimisc::mediawiki::api::Api;
use wikimisc::sparql_table_vec::SparqlTableVec;
use wikimisc::wikibase::{EntityTrait, SnakDataType, Statement, StatementRank, Value as WikibaseValue};

const AUTODESC_FALLBACK: &str = "FALLBACK";

/// All mutable per-page state that the processing pipeline reads and writes.
///
/// Keeping this in its own struct (rather than as loose `ListeriaList` fields)
/// makes the "what the pipeline mutates" set explicit and lets pipeline stages
/// be passed `&mut ProcessingState` in the future without dragging the whole
/// `ListeriaList` (config, API handles, profiler) along for the ride.
#[derive(Debug, Clone, Default)]
pub struct ProcessingState {
    /// Result rows for the current page, built up across pipeline stages.
    pub results: Vec<ResultRow>,
    /// Local file titles that shadow Commons uploads — populated by the
    /// shadow-files processor, consumed by the renderer.
    pub shadow_files: HashSet<String>,
    /// Per-page memoised "does this local page exist?" lookups, used by the
    /// redlink-aware link rendering paths.
    pub local_page_cache: HashMap<String, bool>,
    /// Section-id → display-name map populated during the sections stage.
    pub section_id_to_name: HashMap<usize, String>,
    /// Set of reference IDs already emitted on the page, used to deduplicate
    /// `<ref name="...">` definitions in the output wikitext.
    pub reference_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct ListeriaList {
    page_params: Arc<PageParams>,
    template: Template,
    columns: Vec<Column>,
    params: TemplateParams,
    sparql_table: Arc<SparqlTableVec>,
    ecw: EntityContainerWrapper,
    state: ProcessingState,
    wb_api: Arc<Api>,
    language: String,
    profiler: ProfilingService,
}

impl ListeriaList {
    pub async fn new(template: Template, page_params: Arc<PageParams>) -> Result<Self> {
        let wb_api = page_params.wb_api();
        let mut template = template;
        template.fix_values();
        let profiler = ProfilingService::new(
            page_params.config().clone(),
            page_params.wiki(),
            page_params.page(),
            page_params.config().profiling(),
        );
        Ok(Self {
            page_params: page_params.clone(),
            template,
            columns: Vec::new(),
            params: TemplateParams::new(),
            sparql_table: Arc::new(SparqlTableVec::new()),
            ecw: EntityContainerWrapper::new(
                page_params.config().max_concurrent_entry_queries(),
            )
            .await?
            .with_circuit_breaker(
                page_params
                    .config()
                    .mw_api_circuit_breaker(crate::configuration::MW_API_ENTITIES_KEY),
            ),
            state: ProcessingState::default(),
            wb_api,
            language: page_params.language().to_string(),
            profiler,
        })
    }

    pub fn do_get_regions(&self) -> bool {
        let wiki = self.wiki();
        self.page_params()
            .config()
            .location_regions()
            .iter()
            .any(|r| r == wiki)
    }

    pub fn process_regions_get_entity_ids(&self) -> HashSet<String> {
        let mut entity_ids = HashSet::new();
        for row in self.results().iter() {
            let has_location = row.cells().iter().any(|cell| {
                cell.parts()
                    .iter()
                    .any(|part| matches!(part.part(), ResultCellPart::Location(_)))
            });
            if has_location {
                entity_ids.insert(row.entity_id().to_string());
            }
        }
        entity_ids
    }

    pub async fn profile(&mut self, msg: &str) {
        self.profiler.profile(msg).await;
    }

    /// Main processing pipeline: parses template, runs SPARQL query, and generates results.
    pub async fn process(&mut self) -> Result<()> {
        self.profile("START list::process").await;
        self.process_template()?;
        self.profile("AFTER list::process process_template").await;
        self.run_query().await?;
        self.profile("AFTER list::process run_query").await;
        self.load_entities().await?;
        self.profile("AFTER list::process load_entities").await;
        ResultGenerator::generate_results(self).await?;
        self.profile("AFTER list::process generate_results").await;
        self.process_results().await?;
        self.profile("AFTER list::process process_results").await;
        self.profile("END list::process").await;
        Ok(())
    }

    pub async fn external_id_url(&self, prop: &str, id: &str) -> Option<String> {
        self.ecw.external_id_url(prop, id).await
    }

    pub const fn results(&self) -> &Vec<ResultRow> {
        &self.state.results
    }

    pub const fn results_mut(&mut self) -> &mut Vec<ResultRow> {
        &mut self.state.results
    }

    pub const fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub const fn shadow_files(&self) -> &HashSet<String> {
        &self.state.shadow_files
    }

    pub const fn shadow_files_mut(&mut self) -> &mut HashSet<String> {
        &mut self.state.shadow_files
    }

    pub const fn local_page_cache_mut(&mut self) -> &mut HashMap<String, bool> {
        &mut self.state.local_page_cache
    }

    pub const fn section_id_to_name_mut(&mut self) -> &mut HashMap<usize, String> {
        &mut self.state.section_id_to_name
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
        &self.state.reference_ids
    }

    pub fn sparql_table(&self) -> &SparqlTableVec {
        &self.sparql_table
    }

    pub const fn sparql_table_arc(&self) -> &Arc<SparqlTableVec> {
        &self.sparql_table
    }

    pub fn local_file_namespace_prefix(&self) -> &str {
        self.page_params.local_file_namespace_prefix()
    }

    pub fn get_wiki(&self) -> Option<Wiki> {
        let wiki = self.page_params.wiki();
        self.page_params.config().get_wiki(wiki).cloned()
    }

    pub fn section_name(&self, id: usize) -> Option<&str> {
        self.state.section_id_to_name.get(&id).map(|s| s.as_str())
    }

    pub fn process_template(&mut self) -> Result<()> {
        match self.template.get_value("columns") {
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

        self.params = TemplateParams::new_from_params(&self.template, self.page_params.config());
        if let Some(s) = self.template.get_value("links") {
            self.params
                .set_links(LinksType::new_from_string(s.to_string()));
        }
        if let Some(l) = self.template.get_value("language") {
            self.language = l.to_lowercase();
        }

        let wikibase = self.params.wikibase();
        self.wb_api = match self
            .page_params
            .config()
            .get_wbapi(&wikibase.to_lowercase())
        {
            Some(api) => api.clone(),
            None => return Err(ListeriaError::SparqlNoConfig(wikibase.to_string()).into()),
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
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let Ok(result) = self.page_params.mw_api().get_query_api_json(&params).await else {
            return Vec::new();
        };

        // Only the set of known titles is needed (the "from" side of any
        // normalization entry is never read), so store them in a HashSet.
        let mut known_titles: HashSet<&str> = pages.iter().map(String::as_str).collect();
        if let Some(query_normalized) = result["query"]["normalized"].as_array() {
            for n in query_normalized {
                if let Some(to) = n["to"].as_str() {
                    known_titles.insert(to);
                }
            }
        }

        result["query"]["pages"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .filter_map(|(_k, v)| {
                        v["title"].as_str().and_then(|title| {
                            known_titles.contains(title).then(|| {
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
        *self.state.local_page_cache.get(page).unwrap_or(&false)
    }

    /// Test-only helper that uses the first-letter (case-insensitive)
    /// normalisation path. Production code paths should call
    /// `render_context::normalize_page_title(s, list.is_case_sensitive_wiki())`
    /// directly so the per-wiki case setting is respected.
    pub fn normalize_page_title(s: &str) -> String {
        crate::render_context::normalize_page_title(s, false)
    }

    /// Formats a coordinate value to at most 6 decimal places, trimming trailing zeros.
    /// 6 decimals gives ~0.1 m precision, which is more than sufficient for geographic display.
    pub fn format_coordinate(val: f64) -> String {
        let s = format!("{:.6}", val);
        s.trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }

    pub fn get_location_template(
        &self,
        lat: f64,
        lon: f64,
        entity_id: Option<String>,
        region: Option<String>,
        label: Option<String>,
    ) -> String {
        let entity_id = entity_id.unwrap_or_default();
        let label = label.unwrap_or_else(|| entity_id.clone());
        self.page_params
            .config()
            .get_location_template(self.page_params.wiki())
            .replace("$LAT$", &Self::format_coordinate(lat))
            .replace("$LON$", &Self::format_coordinate(lon))
            .replace("$ITEM$", &entity_id)
            .replace("$REGION$", &region.unwrap_or_default())
            .replace("$LABEL$", &label)
    }

    pub fn thumbnail_size(&self) -> u64 {
        let default = self.page_params.config().default_thumbnail_size();
        self.template
            .get_value("thumb")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(default)
    }

    pub async fn run_query(&mut self) -> Result<()> {
        let wikibase_key = self.params.wikibase().to_lowercase();
        let sparql = match self.template.get_value("sparql") {
            Some(s) => s,
            None => return Err(ListeriaError::MissingSparqlParam.into()),
        };
        let mut sparql_results = SparqlResults::new(self.page_params.clone(), &wikibase_key);
        if self.page_params.config().is_single_wiki()
            && let Some(endpoint) = self.page_params.config().query_endpoint()
        {
            sparql_results = sparql_results.with_query_endpoint(endpoint);
        }
        let mut sparql_table = sparql_results.run_query(sparql).await?;
        sparql_table.set_main_variable(sparql_results.sparql_main_variable());
        self.sparql_table = Arc::new(sparql_table);
        Ok(())
    }

    /// Decides whether the entity cache must be populated before rendering.
    ///
    /// Returns `true` when **either** the column set needs entity data
    /// (anything beyond plain `Number` / `Item` / `Field` columns) **or** the
    /// link mode itself reads from the entity cache:
    ///
    /// - `LinksType::Local` filters rows via the entity's sitelinks
    ///   (`EntityContainerWrapper::use_local_links`); with no entities loaded
    ///   every row is silently dropped.
    /// - `LinksType::RedOnly` filters rows via the entity's sitelinks in
    ///   `ListProcessor::find_keep_flags`; same silent-empty failure mode.
    ///
    /// `Red`, `Text` and `Reasonator` only consult the entity cache when an
    /// `Entity` part has `try_localize=true`, which Item/Field/Number columns
    /// never produce. They therefore do not force a load on their own.
    fn needs_entity_loading(columns: &[Column], links: &LinksType) -> bool {
        let needs_for_columns = columns.iter().any(|c| {
            !matches!(
                c.obj(),
                ColumnType::Number | ColumnType::Item | ColumnType::Field(_)
            )
        });
        let needs_for_links = matches!(links, LinksType::Local | LinksType::RedOnly);
        needs_for_columns || needs_for_links
    }

    pub async fn load_entities(&mut self) -> Result<()> {
        if !Self::needs_entity_loading(&self.columns, self.params.links()) {
            return Ok(());
        }

        let ids = ResultGenerator::get_ids_from_sparql_rows(self)?;
        if ids.is_empty() {
            return Err(ListeriaError::NoItemsToShow.into());
        }
        self.ecw.load_entities(&self.wb_api, &ids).await?;

        self.label_columns().await;

        Ok(())
    }

    async fn label_columns(&mut self) {
        let mut columns = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            let mut c = c.clone();
            c.generate_label(self).await;
            columns.push(c);
        }
        self.columns = columns;
    }

    pub async fn get_autodesc_description(&self, e: &MyEntity) -> Result<String> {
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
        let items_to_load: Vec<String> = self
            .state
            .results
            .iter()
            .map(|row| row.entity_id().to_string())
            .collect();
        self.ecw.load_entities(&self.wb_api, &items_to_load).await?;
        Ok(())
    }

    pub async fn process_results(&mut self) -> Result<()> {
        self.profile("START list::process_results").await;
        self.gather_and_load_items().await?;
        self.profile("AFTER list::process_results gather_and_load_items")
            .await;
        let flags = *self.page_params.config().feature_flags();
        if flags.enable_autodesc {
            ListProcessor::fill_autodesc(self).await?;
            self.profile("AFTER list::process_results fill_autodesc")
                .await;
        }
        ListProcessor::process_redlinks_only(self).await?;
        self.profile("AFTER list::process_results process_redlinks_only")
            .await;
        ListProcessor::process_items_to_local_links(self).await?;
        self.profile("AFTER list::process_results process_items_to_local_links")
            .await;
        ListProcessor::process_redlinks(self).await?;
        self.profile("AFTER list::process_results process_redlinks")
            .await;
        if flags.enable_shadow_check {
            ListProcessor::process_remove_shadow_files(self).await?;
            self.profile("AFTER list::process_results process_remove_shadow_files")
                .await;
        }
        ListProcessor::process_excess_files(self)?;
        self.profile("AFTER list::process_results process_excess_files")
            .await;
        if flags.enable_references {
            ListProcessor::process_reference_items(self).await?;
            self.profile("AFTER list::process_results process_reference_items")
                .await;
        }
        ListProcessor::process_sort_results(self).await?;
        self.profile("AFTER list::process_results process_sort_results")
            .await;
        ListProcessor::process_assign_sections(self).await?;
        self.profile("AFTER list::process_results process_assign_sections")
            .await;
        if flags.enable_regions {
            ListProcessor::process_regions(self).await?;
        }
        ListProcessor::process_assign_location_names(self);
        self.profile("AFTER list::process_results process_regions+location_names")
            .await;
        ListProcessor::fix_local_links(self)?;
        self.profile("AFTER list::process_results fix_local_links")
            .await;
        self.profile("END list::process_results").await;
        Ok(())
    }

    pub const fn get_links_type(&self) -> &LinksType {
        self.params.links()
    }

    pub async fn get_entity(&self, entity_id: &str) -> Option<EntityEntry> {
        self.ecw.get_entity(entity_id).await
    }

    pub const fn get_row_template(&self) -> &Option<String> {
        self.params.row_template()
    }

    pub const fn get_reference_parameter(&self) -> &ReferencesParameter {
        self.params.references()
    }

    async fn gather_items_for_property(&mut self, prop: &str) -> Result<Vec<String>> {
        let mut entities_to_load = Vec::new();
        for row in self.state.results.iter() {
            let Some(entity) = self.ecw.get_entity(row.entity_id()).await else {
                continue;
            };
            for statement in self.get_filtered_claims(&entity, prop) {
                let snak = statement.main_snak();
                if *snak.datatype() != SnakDataType::WikibaseItem {
                    continue;
                }
                if let Some(dv) = snak.data_value()
                    && let wikimisc::wikibase::value::Value::Entity(v) = dv.value()
                {
                    entities_to_load.push(v.id().to_owned());
                }
            }
        }
        Ok(entities_to_load)
    }

    async fn gather_items_section(&mut self) -> Result<Vec<String>> {
        let prop = match self.params.section() {
            SectionType::Property(p) => p.to_string(),
            // SPARQL variable sections read their value straight from the
            // result row, so nothing extra needs loading from Wikidata.
            SectionType::SparqlVariable(_v) => return Ok(Vec::new()),
            SectionType::None => return Ok(Vec::new()),
        };
        self.gather_items_for_property(&prop).await
    }

    async fn gather_items_sort(&mut self) -> Result<Vec<String>> {
        let prop = match self.params.sort() {
            SortMode::Property(prop) => prop.to_string(),
            _ => return Ok(Vec::new()),
        };
        self.gather_items_for_property(&prop).await
    }

    async fn gather_and_load_items(&mut self) -> Result<()> {
        // Gather items to load
        let mut entities_to_load: Vec<String> = Vec::new();
        for row in self.state.results.iter() {
            for cell in row.cells() {
                entities_to_load.extend(
                    EntityContainerWrapper::gather_entities_and_external_properties(cell.parts()),
                );
            }
        }
        if let SortMode::Property(prop) = self.params.sort() {
            entities_to_load.push(prop.to_string());
        }

        match self.params.section() {
            SectionType::Property(prop) => {
                entities_to_load.push(prop.to_owned());
            }
            // SPARQL variable sections use the SPARQL-provided value directly
            // and therefore do not require loading any additional entities.
            SectionType::SparqlVariable(_) | SectionType::None => {}
        }
        // Deduplicate before loading: items like "depicts" (P180) can appear hundreds of
        // times across many rows, and without dedup the list sent to the API inflates to
        // tens-of-thousands of entries for large painting lists (issue #40).
        entities_to_load.sort_unstable();
        entities_to_load.dedup();
        // Non-fatal: sub-entity labels are best-effort. If loading fails (e.g. because
        // the list is very large), cells degrade to bare QID links rather than aborting
        // the entire page update.
        if let Err(e) = self.ecw.load_entities(&self.wb_api, &entities_to_load).await {
            log::warn!("Could not load sub-entities, some cells may show bare QIDs: {e}");
        }

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
        let mut ret: Vec<usize> = self.state.results.iter().map(|row| row.section()).collect();
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

    pub fn get_filtered_claims(&self, e: &MyEntity, property: &str) -> Vec<Statement> {
        let mut ret: Vec<Statement> = e
            .claims_with_property(property)
            .iter()
            .filter(|s| *s.rank() != StatementRank::Deprecated)
            .map(|&x| x.clone())
            .collect();

        if self.page_params.config().prefer_preferred() {
            let has_preferred = ret.iter().any(|x| *x.rank() == StatementRank::Preferred);
            if has_preferred {
                ret.retain(|x| *x.rank() == StatementRank::Preferred);
            }
        }

        // Stable-sort string-valued claims (external IDs, URLs, etc.) alphabetically
        // so the rendered output is deterministic regardless of API return order (#168).
        ret.sort_by_key(|s| {
            match s.main_snak().data_value() {
                Some(dv) => match dv.value() {
                    WikibaseValue::StringValue(v) => v.clone(),
                    _ => String::new(),
                },
                None => String::new(),
            }
        });

        ret
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
        Arc::clone(self.page_params.mw_api())
    }
}

impl crate::render_context::RenderContext for ListeriaList {
    fn language(&self) -> &str {
        ListeriaList::language(self)
    }

    fn default_language(&self) -> String {
        ListeriaList::default_language(self)
    }

    fn page_title(&self) -> &str {
        ListeriaList::page_title(self)
    }

    fn wiki(&self) -> &str {
        ListeriaList::wiki(self)
    }

    fn is_main_wikibase_wiki(&self) -> bool {
        ListeriaList::is_main_wikibase_wiki(self)
    }

    fn is_case_sensitive_wiki(&self) -> bool {
        self.page_params
            .config()
            .is_wiki_case_sensitive(self.wiki())
    }

    fn get_links_type(&self) -> &crate::template_params::LinksType {
        ListeriaList::get_links_type(self)
    }

    fn header_template(&self) -> &Option<String> {
        ListeriaList::header_template(self)
    }

    fn template_params(&self) -> &crate::template_params::TemplateParams {
        ListeriaList::template_params(self)
    }

    fn get_reference_parameter(&self) -> &crate::template_params::ReferencesParameter {
        ListeriaList::get_reference_parameter(self)
    }

    fn thumbnail_size(&self) -> u64 {
        ListeriaList::thumbnail_size(self)
    }

    fn local_file_namespace_prefix(&self) -> &str {
        ListeriaList::local_file_namespace_prefix(self)
    }

    fn column(&self, colnum: usize) -> Option<&crate::column::Column> {
        ListeriaList::column(self, colnum)
    }

    fn results(&self) -> &Vec<crate::result_row::ResultRow> {
        ListeriaList::results(self)
    }

    fn reference_ids(&self) -> &std::collections::HashSet<String> {
        ListeriaList::reference_ids(self)
    }

    fn get_wiki(&self) -> Option<crate::wiki::Wiki> {
        ListeriaList::get_wiki(self)
    }

    fn get_item_wiki_target(&self, entity_id: &str) -> String {
        ListeriaList::get_item_wiki_target(self, entity_id)
    }

    fn get_location_template(
        &self,
        lat: f64,
        lon: f64,
        entity_id: Option<String>,
        region: Option<String>,
        label: Option<String>,
    ) -> String {
        ListeriaList::get_location_template(self, lat, lon, entity_id, region, label)
    }

    fn ecw(&self) -> &crate::entity_container_wrapper::EntityContainerWrapper {
        ListeriaList::ecw(self)
    }

    fn get_filtered_claims(
        &self,
        entity: &crate::my_entity::MyEntity,
        property: &str,
    ) -> Vec<wikimisc::wikibase::Statement> {
        ListeriaList::get_filtered_claims(self, entity, property)
    }

    fn columns(&self) -> &Vec<crate::column::Column> {
        ListeriaList::columns(self)
    }

    fn get_section_ids(&self) -> Vec<usize> {
        ListeriaList::get_section_ids(self)
    }

    fn shadow_files(&self) -> &std::collections::HashSet<String> {
        ListeriaList::shadow_files(self)
    }

    fn summary(&self) -> &Option<String> {
        ListeriaList::summary(self)
    }

    fn skip_table(&self) -> bool {
        ListeriaList::skip_table(self)
    }

    fn get_row_template(&self) -> &Option<String> {
        ListeriaList::get_row_template(self)
    }

    fn section_name(&self, id: usize) -> Option<&str> {
        ListeriaList::section_name(self, id)
    }

    async fn get_entity(
        &self,
        entity_id: &str,
    ) -> Option<crate::entity_container_wrapper::EntityEntry> {
        ListeriaList::get_entity(self, entity_id).await
    }

    async fn get_item_link_with_fallback(&self, entity_id: &str) -> String {
        ListeriaList::get_item_link_with_fallback(self, entity_id).await
    }

    async fn get_label_with_fallback_lang(&self, entity_id: &str, language: &str) -> String {
        ListeriaList::get_label_with_fallback_lang(self, entity_id, language).await
    }

    async fn external_id_url(&self, prop: &str, id: &str) -> Option<String> {
        ListeriaList::external_id_url(self, prop, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- normalize_page_title ---

    #[test]
    fn test_normalize_page_title_basic() {
        assert_eq!(
            ListeriaList::normalize_page_title("hello world"),
            "Hello world"
        );
    }

    #[test]
    fn test_normalize_page_title_already_normalized() {
        assert_eq!(
            ListeriaList::normalize_page_title("Hello world"),
            "Hello world"
        );
    }

    #[test]
    fn test_normalize_page_title_single_char() {
        // Length < 2, returns as-is
        assert_eq!(ListeriaList::normalize_page_title("h"), "h");
    }

    #[test]
    fn test_normalize_page_title_empty() {
        assert_eq!(ListeriaList::normalize_page_title(""), "");
    }

    #[test]
    fn test_normalize_page_title_two_chars() {
        assert_eq!(ListeriaList::normalize_page_title("ab"), "Ab");
    }

    #[test]
    fn test_normalize_page_title_with_namespace() {
        assert_eq!(
            ListeriaList::normalize_page_title("category:test"),
            "Category:test"
        );
    }

    // --- format_coordinate (issue #32) ---

    #[test]
    fn test_format_coordinate_repeating_decimal() {
        // 50°55′27″ = 50 + 55/60 + 27/3600 = 50.924166... → rounded to 6 dp
        assert_eq!(ListeriaList::format_coordinate(50.924_166_666_666_665), "50.924167");
    }

    #[test]
    fn test_format_coordinate_repeating_decimal_2() {
        // 4°06′39″ = 4 + 6/60 + 39/3600 = 4.110833... → 6 dp, no rounding needed
        assert_eq!(ListeriaList::format_coordinate(4.110_833_333_333_334), "4.110833");
    }

    #[test]
    fn test_format_coordinate_trailing_zeros_trimmed() {
        assert_eq!(ListeriaList::format_coordinate(1.5), "1.5");
    }

    #[test]
    fn test_format_coordinate_whole_number() {
        assert_eq!(ListeriaList::format_coordinate(50.0), "50");
    }

    #[test]
    fn test_format_coordinate_negative() {
        assert_eq!(ListeriaList::format_coordinate(-33.868_820), "-33.86882");
    }

    #[test]
    fn test_format_coordinate_zero() {
        assert_eq!(ListeriaList::format_coordinate(0.0), "0");
    }

    // --- needs_entity_loading ---

    fn col(spec: &str) -> Column {
        Column::new(spec).expect("column spec must parse")
    }

    #[test]
    fn test_needs_entity_loading_item_only_with_all_links_is_false() {
        // Baseline: pure Item/Number/Field columns + default links → no load.
        let cols = vec![col("number"), col("item")];
        assert!(!ListeriaList::needs_entity_loading(&cols, &LinksType::All));
    }

    #[test]
    fn test_needs_entity_loading_item_only_with_local_links_forces_load() {
        // Bug fix: links=Local needs the entity cache to evaluate the
        // sitelink filter, even when no column reads entity data.
        let cols = vec![col("item")];
        assert!(ListeriaList::needs_entity_loading(
            &cols,
            &LinksType::Local
        ));
    }

    #[test]
    fn test_needs_entity_loading_item_only_with_red_only_links_forces_load() {
        // Bug fix: links=RedOnly filters rows by entity sitelinks too.
        let cols = vec![col("item")];
        assert!(ListeriaList::needs_entity_loading(
            &cols,
            &LinksType::RedOnly
        ));
    }

    #[test]
    fn test_needs_entity_loading_item_only_with_red_links_does_not_force() {
        // `Red` only colours existing Entity parts; with Item-only columns
        // there is nothing to colour, so no entity load is required.
        let cols = vec![col("item")];
        assert!(!ListeriaList::needs_entity_loading(&cols, &LinksType::Red));
    }

    #[test]
    fn test_needs_entity_loading_label_column_loads_regardless_of_links() {
        let cols = vec![col("label")];
        assert!(ListeriaList::needs_entity_loading(&cols, &LinksType::All));
        assert!(ListeriaList::needs_entity_loading(&cols, &LinksType::Text));
    }

    #[test]
    fn test_needs_entity_loading_property_column_loads_regardless_of_links() {
        let cols = vec![col("P31")];
        assert!(ListeriaList::needs_entity_loading(&cols, &LinksType::All));
    }

    #[test]
    fn test_needs_entity_loading_empty_columns_with_local_links_forces_load() {
        // Edge case: no columns at all but links=Local still needs the cache
        // to make a row-keep decision in use_local_links.
        let cols: Vec<Column> = Vec::new();
        assert!(ListeriaList::needs_entity_loading(
            &cols,
            &LinksType::Local
        ));
    }
}
