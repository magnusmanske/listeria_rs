//! Bot configuration management.
//!
//! Handles loading and parsing configuration from JSON files, including API endpoints,
//! template mappings, database settings, and operational parameters.

use crate::circuit_breaker::CircuitBreaker;
use crate::database_pool::DatabasePool;
use crate::wiki::Wiki;
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::{fs::File, io::BufReader, path::Path};
use tokio::sync::Semaphore;
use wikimisc::mediawiki::api::Api;
use wikimisc::wikibase::EntityTrait;
use wikimisc::wikibase::entity_container::EntityContainer;

/// Circuit-breaker key for the Wikidata entity-loading API (`wbgetentities`).
/// Used by [`EntityContainerWrapper`] so a flapping Wikidata API doesn't keep
/// hammering through every page's entity load.
pub const MW_API_ENTITIES_KEY: &str = "wikidata_entities";

#[derive(Debug, Clone)]
pub enum NamespaceGroup {
    All,            // All namespaces forbidden
    List(Vec<i64>), // List of forbidden namespaces
}

impl NamespaceGroup {
    #[must_use]
    pub fn can_edit_namespace(&self, nsid: i64) -> bool {
        match self {
            Self::All => false,
            Self::List(list) => nsid >= 0 && !list.contains(&nsid),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Configuration {
    wb_apis: HashMap<String, Arc<Api>>,
    namespace_blocks: HashMap<String, NamespaceGroup>,
    default_api: String,
    prefer_preferred: bool,
    default_language: String,
    template_start_sites: HashMap<String, String>,
    template_end_sites: HashMap<String, String>,
    location_templates: HashMap<String, String>,
    shadow_images_check: Vec<String>,
    default_thumbnail_size: Option<u64>,
    location_regions: Vec<String>,
    mysql: Option<Value>,
    oauth2_token: String,
    template_start_q: String,
    pattern_string_start: String,
    pattern_string_end: String,
    max_mw_apis_per_wiki: Option<usize>,
    max_mw_apis_total: Option<usize>,
    max_local_cached_entities: usize,
    max_concurrent_entry_queries: usize,
    api_timeout: u64,
    /// Wall-clock budget for processing a single page (seconds). Bounds the
    /// worst-case time a slow SPARQL endpoint or hung MediaWiki API can hold a
    /// dispatcher slot. On expiry the page is marked FAIL in the queue so it
    /// can be retried in a future pass rather than blocking forever.
    page_timeout_sec: u64,
    /// Per-DB-operation wall-clock budget (seconds). A wedged replica or a
    /// slow `pagestatus` UPDATE would otherwise hang the dispatcher hot path
    /// (`prepare_next_single_page`) indefinitely.
    db_query_timeout_sec: u64,
    ms_delay_after_edit: Option<u64>,
    max_threads: usize,
    pool: Option<Arc<DatabasePool>>,
    max_sparql_simultaneous: u64,
    profiling: bool,
    wikis: HashMap<String, Wiki>,
    is_single_wiki: bool, // Set single wiki mode
    quiet: bool,
    wiki_page_pattern: Option<String>, // For single wiki mode, the pattern for wiki pages
    delay_after_page_check_sec: Option<u64>, // For single wiki mode, the delay after checking a page
    query_endpoint: Option<String>,          // For single wiki mode, the SPARQL endpoint
    status_server_port: Option<u16>,         // For single wiki mode, the port for the status server
    sparql_prefix: Option<String>, // For single wiki mode, a prefix for all SPARQL queries
    main_item_prefix: String,      // For single wiki mode, the prefix for items
    /// Per-endpoint SPARQL semaphores. Each endpoint URL gets its own semaphore
    /// of `max_sparql_simultaneous` permits, so a slow Commons-Query-Service
    /// can't starve calls to the Wikidata-Query-Service of permits.
    /// Shared across all `Configuration` clones via the same `Arc`.
    sparql_semaphores: Arc<DashMap<String, Arc<Semaphore>>>,
    /// Per-endpoint circuit breakers. Shared across all `Configuration` clones
    /// so that failures recorded by one clone are visible to all others.
    sparql_circuit_breakers: Arc<DashMap<String, Arc<CircuitBreaker>>>,
    /// Per-key circuit breakers for MediaWiki API calls (page reads, edits,
    /// `wbgetentities`). Keyed by wiki name for per-wiki MW APIs; by an
    /// arbitrary identifier (e.g. `"wikidata_entities"`) for the Wikidata
    /// entity-loading API. Same `DashMap` lazy-creation pattern as
    /// `sparql_circuit_breakers`.
    mw_api_circuit_breakers: Arc<DashMap<String, Arc<CircuitBreaker>>>,
    /// Maps inbound wiki identifiers to their database/server names. Pre-seeded
    /// with the historical `be_x_oldwiki` aliases (see Phabricator T11216); JSON
    /// config can extend or override the map via the `wiki_name_aliases` key.
    /// Names not present in the map fall through `replace('-', '_')`.
    wiki_name_aliases: HashMap<String, String>,
    /// Explicit list of wikis whose `general.case` is `case-sensitive`.
    /// Wiktionary projects (`*wiktionary`) are detected by suffix and don't
    /// need to be listed; this set is for wikis that need the same treatment
    /// but don't match the wiktionary pattern (e.g. a custom MediaWiki
    /// installation in single-wiki mode).
    case_sensitive_wikis: HashSet<String>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            wb_apis: HashMap::new(),
            namespace_blocks: HashMap::new(),
            default_api: String::new(),
            prefer_preferred: false,
            default_language: String::new(),
            template_start_sites: HashMap::new(),
            template_end_sites: HashMap::new(),
            location_templates: HashMap::new(),
            shadow_images_check: Vec::new(),
            default_thumbnail_size: None,
            location_regions: Vec::new(),
            mysql: None,
            oauth2_token: String::new(),
            template_start_q: String::new(),
            pattern_string_start: String::new(),
            pattern_string_end: String::new(),
            max_mw_apis_per_wiki: None,
            max_mw_apis_total: None,
            max_local_cached_entities: 0,
            max_concurrent_entry_queries: 0,
            api_timeout: 0,
            page_timeout_sec: 0,
            db_query_timeout_sec: 0,
            ms_delay_after_edit: None,
            max_threads: 0,
            pool: None,
            max_sparql_simultaneous: 0,
            profiling: false,
            wikis: HashMap::new(),
            is_single_wiki: false,
            quiet: false,
            wiki_page_pattern: None,
            delay_after_page_check_sec: None,
            query_endpoint: None,
            status_server_port: None,
            sparql_prefix: None,
            main_item_prefix: String::new(),
            sparql_semaphores: Arc::new(DashMap::new()),
            sparql_circuit_breakers: Arc::new(DashMap::new()),
            mw_api_circuit_breakers: Arc::new(DashMap::new()),
            wiki_name_aliases: Self::default_wiki_name_aliases(),
            case_sensitive_wikis: HashSet::new(),
        }
    }
}

impl Configuration {
    /// Built-in wiki-name alias map.
    ///
    /// `be_x_oldwiki` has lived under four spellings ever since the Belarusian
    /// (Taraškievica) Wikipedia rename (Phabricator T11216); both the old and
    /// new identifiers — with `-` and `_` variants — must map to the same
    /// database schema name. JSON config can add more entries via the
    /// `wiki_name_aliases` key.
    fn default_wiki_name_aliases() -> HashMap<String, String> {
        ["be-taraskwiki", "be-x-oldwiki", "be_taraskwiki", "be_x_oldwiki"]
            .into_iter()
            .map(|name| (name.to_string(), "be_x_oldwiki".to_string()))
            .collect()
    }

    /// Normalises a wiki identifier into its database/server name.
    ///
    /// Returns the configured alias when one is registered for the input;
    /// otherwise replaces any hyphens with underscores (the Wikimedia
    /// convention for converting site IDs to DB schema names).
    #[must_use]
    pub fn fix_wiki_name(&self, wiki: &str) -> String {
        self.wiki_name_aliases
            .get(wiki)
            .cloned()
            .unwrap_or_else(|| wiki.replace('-', "_"))
    }

    /// Returns `true` when `wiki` treats page titles as case-sensitive
    /// (MediaWiki's `general.case = "case-sensitive"` setting).
    ///
    /// All Wiktionary projects are detected automatically by suffix; other
    /// case-sensitive wikis can be listed in the JSON config under
    /// `case_sensitive_wikis`. The default is `false` (first-letter
    /// case-folding, matching most Wikipedias and Wikidata).
    #[must_use]
    pub fn is_wiki_case_sensitive(&self, wiki: &str) -> bool {
        if self.case_sensitive_wikis.contains(wiki) {
            return true;
        }
        // Every Wiktionary project (`enwiktionary`, `dewiktionary`, …) is
        // case-sensitive in MediaWiki's siteinfo.
        wiki.ends_with("wiktionary")
    }

    /// Loads configuration from a JSON file.
    pub async fn new_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let j = tokio::task::spawn_blocking(move || -> Result<Value> {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            let j = serde_json::from_reader(reader)?;
            Ok(j)
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking join error: {e}"))??;
        Self::new_from_json(j).await
    }

    pub const fn set_max_local_cached_entities(&mut self, max_local_cached_entities: usize) {
        self.max_local_cached_entities = max_local_cached_entities;
    }

    #[must_use]
    pub const fn with_max_local_cached_entities(mut self, max_local_cached_entities: usize) -> Self {
        self.max_local_cached_entities = max_local_cached_entities;
        self
    }

    pub fn set_wikis(&mut self, wikis: HashMap<String, Wiki>) {
        self.wikis = wikis;
    }

    #[must_use]
    pub fn with_wikis(mut self, wikis: HashMap<String, Wiki>) -> Self {
        self.wikis = wikis;
        self
    }

    #[must_use]
    pub fn get_wiki(&self, wiki: &str) -> Option<&Wiki> {
        self.wikis.get(wiki)
    }

    /// Constructs a configuration from parsed JSON.
    /// Sets up APIs, database connections, and template mappings.
    pub async fn new_from_json(j: Value) -> Result<Self> {
        let mut ret: Self = Self {
            max_mw_apis_per_wiki: j["max_mw_apis_per_wiki"]
                .as_u64()
                .and_then(|u| u.try_into().ok()),
            is_single_wiki: j["template_start"].as_str().is_some()
                && j["template_end"].as_str().is_some()
                && j["apis"]["wiki"].as_str().is_some(),
            ..Default::default()
        };
        ret.new_from_json_misc(&j);
        ret.new_from_json_locations(&j);
        ret.new_from_json_wikibase_apis(&j).await?;
        ret.new_from_json_namespace_blocks(&j)?;
        ret.new_from_json_start_end_tempate_mappings(&j).await?;
        if j["mysql"].as_object().is_some() {
            ret.pool = Some(Arc::new(DatabasePool::new(&ret)?));
        }
        ret.validate()?;
        Ok(ret)
    }

    #[must_use]
    pub fn query_endpoint(&self) -> Option<String> {
        self.query_endpoint.to_owned()
    }

    pub fn sparql_prefix(&self) -> Option<&str> {
        self.sparql_prefix.as_deref()
    }

    fn new_from_json_namespace_blocks(&mut self, j: &Value) -> Result<()> {
        // Namespace blocks on wikis
        if let Some(o) = j["namespace_blocks"].as_object() {
            for (k, v) in o.iter() {
                // Check for string value ("*")
                if let Some(s) = v.as_str() {
                    if s == "*" {
                        // All namespaces
                        self.namespace_blocks
                            .insert(k.to_string(), NamespaceGroup::All);
                    } else {
                        return Err(anyhow!(
                            "Unrecognized string value for namespace_blocks[{k}]:{v}"
                        ));
                    }
                }

                // Check for array of integers
                if let Some(a) = v.as_array() {
                    let nsids: Vec<i64> = a
                        .iter()
                        .filter_map(|x| x.as_u64())
                        .filter_map(|x| x.try_into().ok())
                        .collect();
                    self.namespace_blocks
                        .insert(k.to_string(), NamespaceGroup::List(nsids));
                }
            }
        }
        Ok(())
    }

    pub const fn max_sparql_simultaneous(&self) -> u64 {
        self.max_sparql_simultaneous
    }

    /// Maximum number of `wbgetentities` outer batches that may be in flight
    /// concurrently. A value of `1` keeps loading sequential. Clamped to `1`
    /// at the call site (see [`EntityContainerWrapper::new`]) so a misconfigured
    /// `0` cannot deadlock the entity-loading semaphore.
    pub const fn max_concurrent_entry_queries(&self) -> usize {
        self.max_concurrent_entry_queries
    }

    /// Returns the semaphore that gates concurrent SPARQL requests for the
    /// given endpoint URL, creating one on first access with
    /// `max_sparql_simultaneous` permits. Per-endpoint isolation means a slow
    /// endpoint cannot starve calls to a healthy one.
    pub fn sparql_semaphore_for(&self, endpoint: &str) -> Arc<Semaphore> {
        self.sparql_semaphores
            .entry(endpoint.to_owned())
            .or_insert_with(|| Arc::new(Semaphore::new(self.max_sparql_simultaneous as usize)))
            .clone()
    }

    /// Returns the circuit breaker for the given SPARQL endpoint URL, creating
    /// one on first access. All `Configuration` clones share the same breaker map.
    pub fn sparql_circuit_breaker(&self, endpoint: &str) -> Arc<CircuitBreaker> {
        self.sparql_circuit_breakers
            .entry(endpoint.to_owned())
            .or_insert_with(|| Arc::new(CircuitBreaker::new()))
            .clone()
    }

    /// Returns the circuit breaker for a MediaWiki API key, creating one on
    /// first access. The `key` is the wiki name for per-wiki page reads/edits;
    /// for the Wikidata entity-loading API pass a stable identifier such as
    /// [`MW_API_ENTITIES_KEY`].
    pub fn mw_api_circuit_breaker(&self, key: &str) -> Arc<CircuitBreaker> {
        self.mw_api_circuit_breakers
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new(CircuitBreaker::new()))
            .clone()
    }

    pub const fn profiling(&self) -> bool {
        self.profiling
    }

    pub const fn quiet(&self) -> bool {
        self.quiet
    }

    pub const fn set_profiling(&mut self, profiling: bool) {
        self.profiling = profiling;
    }

    #[must_use]
    pub const fn with_profiling(mut self, profiling: bool) -> Self {
        self.profiling = profiling;
        self
    }

    /// Returns the database connection pool if configured.
    pub fn pool(&self) -> Result<&Arc<DatabasePool>> {
        self.pool
            .as_ref()
            .ok_or_else(|| anyhow!("Database pool not configured"))
    }

    pub const fn max_threads(&self) -> usize {
        self.max_threads
    }

    pub const fn ms_delay_after_edit(&self) -> Option<u64> {
        self.ms_delay_after_edit
    }

    pub const fn api_timeout(&self) -> Duration {
        Duration::from_secs(self.api_timeout)
    }

    /// Wall-clock budget for processing one page end-to-end.
    /// Used by the dispatch loop to abort and re-queue a stuck page.
    pub const fn page_timeout(&self) -> Duration {
        Duration::from_secs(self.page_timeout_sec)
    }

    /// Wall-clock budget for an individual DB operation
    /// (connection checkout + query execution combined).
    pub const fn db_query_timeout(&self) -> Duration {
        Duration::from_secs(self.db_query_timeout_sec)
    }

    pub fn oauth2_token(&self) -> &str {
        &self.oauth2_token
    }

    pub fn mysql(&self, key: &str) -> Value {
        match &self.mysql {
            Some(mysql) => mysql[key].clone(),
            None => Value::Null,
        }
    }

    pub const fn max_local_cached_entities(&self) -> usize {
        self.max_local_cached_entities
    }

    fn get_sitelink_mapping(
        entities: &EntityContainer,
        q: &str,
    ) -> Result<HashMap<String, String>> {
        let entity = entities
            .get_entity(q)
            .ok_or(anyhow!("Entity {q} not found"))?;
        match entity.sitelinks() {
            Some(sl) => Ok(sl
                .iter()
                .map(|s| (s.site().to_string(), s.title().to_string()))
                .collect()),
            None => Err(anyhow!("No sitelink in {q}")),
        }
    }

    pub fn check_for_shadow_images(&self, wiki: &str) -> bool {
        self.shadow_images_check.iter().any(|w| w == wiki)
    }

    /// Helper method to extract template title from a template map
    fn get_local_template_title(
        template_map: &HashMap<String, String>,
        wiki: &str,
        template_type: &str,
    ) -> Result<String> {
        let template = template_map
            .get(wiki)
            .ok_or_else(|| anyhow!("Cannot find local {template_type} template"))?;

        template
            .split(':')
            .next_back()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Invalid template format"))
    }

    pub fn get_local_template_title_start(&self, wiki: &str) -> Result<String> {
        Self::get_local_template_title(&self.template_start_sites, wiki, "start")
    }

    pub fn main_item_prefix(&self) -> String {
        self.main_item_prefix.to_owned()
    }

    pub const fn get_max_mw_apis_per_wiki(&self) -> &Option<usize> {
        &self.max_mw_apis_per_wiki
    }

    pub const fn get_max_mw_apis_total(&self) -> &Option<usize> {
        &self.max_mw_apis_total
    }

    pub fn get_local_template_title_end(&self, wiki: &str) -> Result<String> {
        Self::get_local_template_title(&self.template_end_sites, wiki, "end")
    }

    /// Checks if editing is allowed in the given namespace on this wiki.
    pub fn can_edit_namespace(&self, wiki: &str, nsid: i64) -> bool {
        self.namespace_blocks
            .get(wiki)
            .map_or_else(|| true, |nsg| nsg.can_edit_namespace(nsid))
    }

    pub fn get_location_template(&self, wiki: &str) -> String {
        self.location_templates
            .get(wiki)
            .or_else(|| self.location_templates.get("default"))
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_template_start_q(&self) -> String {
        self.template_start_q.clone()
    }

    pub const fn prefer_preferred(&self) -> bool {
        self.prefer_preferred
    }

    pub fn default_language(&self) -> &str {
        &self.default_language
    }

    pub fn default_thumbnail_size(&self) -> u64 {
        self.default_thumbnail_size.unwrap_or(128)
    }

    pub const fn location_regions(&self) -> &Vec<String> {
        &self.location_regions
    }

    pub async fn wbapi_login(&mut self, key: &str) -> bool {
        let oauth2_token = self.oauth2_token().to_owned();
        match self.wb_apis.get_mut(key) {
            Some(api) => {
                if let Some(api) = Arc::get_mut(api) {
                    api.set_oauth2(&oauth2_token);
                }
                true
            }
            None => false,
        }
    }

    pub fn get_wbapi(&self, key: &str) -> Option<&Arc<Api>> {
        self.wb_apis.get(key)
    }

    pub fn get_default_api(&self) -> &str {
        &self.default_api
    }

    /// Returns the default Wikibase API client.
    pub fn get_default_wbapi(&self) -> Result<&Arc<Api>> {
        self.wb_apis
            .get(&self.default_api)
            .ok_or_else(|| anyhow!("No default API set in config file"))
    }

    pub fn pattern_string_start(&self) -> &str {
        &self.pattern_string_start
    }

    pub fn pattern_string_end(&self) -> &str {
        &self.pattern_string_end
    }

    pub const fn is_single_wiki(&self) -> bool {
        self.is_single_wiki
    }

    /// Validates that required configuration fields are sensible.
    ///
    /// Returns an error if any constraint is violated so callers can
    /// fail fast with a clear message instead of crashing later.
    pub fn validate(&self) -> Result<()> {
        if !self.is_single_wiki && self.default_api.is_empty() {
            return Err(anyhow!("default_api must be set in multi-wiki mode"));
        }
        if self.max_sparql_simultaneous == 0 {
            return Err(anyhow!("max_sparql_simultaneous must be > 0"));
        }
        if self.max_threads == 0 {
            return Err(anyhow!("max_threads must be > 0"));
        }
        if self.api_timeout == 0 {
            return Err(anyhow!("api_timeout must be > 0"));
        }
        if self.max_concurrent_entry_queries == 0 {
            return Err(anyhow!("max_concurrent_entry_queries must be > 0"));
        }
        if self.page_timeout_sec == 0 {
            return Err(anyhow!("page_timeout_sec must be > 0"));
        }
        if self.db_query_timeout_sec == 0 {
            return Err(anyhow!("db_query_timeout_sec must be > 0"));
        }
        Ok(())
    }

    pub const fn delay_after_page_check_sec(&self) -> Option<u64> {
        self.delay_after_page_check_sec
    }

    pub const fn status_server_port(&self) -> Option<u16> {
        self.status_server_port
    }

    pub fn wiki_page_pattern(&self) -> Option<String> {
        self.wiki_page_pattern.clone()
    }

    async fn new_from_json_start_end_tempate_mappings(&mut self, j: &Value) -> Result<()> {
        if let (Some(template_start), Some(template_end)) =
            (j["template_start"].as_str(), j["template_end"].as_str())
        {
            self.template_start_sites
                .insert("wiki".to_string(), template_start.replace('_', " "));
            self.template_end_sites
                .insert("wiki".to_string(), template_end.replace('_', " "));
            return Ok(());
        }

        let api = self.get_default_wbapi()?;
        let q_start = j["template_start_q"]
            .as_str()
            .ok_or_else(|| anyhow!("No template_start_q in config"))?
            .to_string();
        let q_end = j["template_end_q"]
            .as_str()
            .ok_or_else(|| anyhow!("No template_end_q in config"))?
            .to_string();

        let to_load = vec![q_start.clone(), q_end.clone()];
        let entity_container = EntityContainer::new();
        entity_container
            .load_entities(api, &to_load)
            .await
            .map_err(|e| anyhow!("Error loading entities: {e}"))?;

        self.template_start_sites = Self::get_sitelink_mapping(&entity_container, &q_start)?;
        self.template_end_sites = Self::get_sitelink_mapping(&entity_container, &q_end)?;
        self.template_start_q = q_start;
        Ok(())
    }

    async fn new_from_json_wikibase_apis(&mut self, j: &Value) -> Result<()> {
        self.oauth2_token = j["wiki_login"]["token"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if j["mysql"].is_object() {
            self.mysql = Some(j["mysql"].to_owned());
        }

        let oauth2_token = self.oauth2_token.to_owned();
        if let Some(o) = j["apis"].as_object() {
            for (k, v) in o.iter() {
                if let (name, Some(url)) = (k.as_str(), v.as_str()) {
                    let mut api = Api::new(url).await?;
                    api.set_oauth2(&oauth2_token);
                    self.wb_apis.insert(name.to_string(), Arc::new(api));
                }
            }
        }
        Ok(())
    }

    fn new_from_json_locations(&mut self, j: &Value) {
        // Location regions
        if let Some(lr) = j["location_regions"].as_array() {
            self.location_regions = lr
                .iter()
                .map(|s| {
                    s.as_str()
                        .expect("location_regions needs to be a string")
                        .to_string()
                })
                .collect();
        }

        // Location template patterns
        if let Some(o) = j["location_templates"].as_object() {
            for (k, v) in o.iter() {
                if let (k, Some(v)) = (k.as_str(), v.as_str()) {
                    self.location_templates.insert(k.to_string(), v.to_string());
                }
            }
        }
    }

    fn new_from_json_misc(&mut self, j: &Value) {
        self.max_mw_apis_total = j["max_mw_apis_total"]
            .as_u64()
            .and_then(|u| u.try_into().ok());
        self.default_api = j["default_api"].as_str().unwrap_or_default().to_string();
        self.query_endpoint = j["query_endpoint"].as_str().map(|s| s.to_string());
        self.default_language = j["default_language"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.prefer_preferred = j["prefer_preferred"].as_bool().unwrap_or_default();
        self.max_sparql_simultaneous = j["max_sparql_simultaneous"].as_u64().unwrap_or(10);
        self.default_thumbnail_size = j["default_thumbnail_size"].as_u64();
        self.max_local_cached_entities = j["max_local_cached_entities"]
            .as_u64()
            .and_then(|u| u.try_into().ok())
            .unwrap_or(5000);
        self.max_concurrent_entry_queries = j["max_concurrent_entry_queries"]
            .as_u64()
            .and_then(|u| u.try_into().ok())
            .unwrap_or(5);
        self.api_timeout = j["api_timeout"].as_u64().unwrap_or(360);
        self.page_timeout_sec = j["page_timeout_sec"].as_u64().unwrap_or(600);
        self.db_query_timeout_sec = j["db_query_timeout_sec"].as_u64().unwrap_or(30);
        self.ms_delay_after_edit = j["ms_delay_after_edit"].as_u64();
        self.delay_after_page_check_sec = j["delay_after_page_check_sec"].as_u64();
        self.max_threads = j["max_threads"]
            .as_u64()
            .and_then(|u| u.try_into().ok())
            .unwrap_or(8);
        self.status_server_port = j["status_server_port"]
            .as_u64()
            .and_then(|u| u.try_into().ok());
        self.profiling = j["profiling"].as_bool().unwrap_or_default();
        self.quiet = j["quiet"].as_bool().unwrap_or_default();
        if let Some(obj) = j["wiki_name_aliases"].as_object() {
            // Merge over the built-in defaults so JSON entries can both
            // extend the map and override individual defaults.
            for (k, v) in obj {
                if let Some(target) = v.as_str() {
                    self.wiki_name_aliases
                        .insert(k.clone(), target.to_string());
                }
            }
        }
        if let Some(arr) = j["case_sensitive_wikis"].as_array() {
            for v in arr {
                if let Some(name) = v.as_str() {
                    self.case_sensitive_wikis.insert(name.to_string());
                }
            }
        }
        self.wiki_page_pattern = j["wiki_page_pattern"].as_str().map(|s| s.to_string());
        self.pattern_string_start = j["pattern_string_start"]
            .as_str()
            .unwrap_or(r#"\{\{(Wikidata[ _]list[^\|]*|"#)
            .to_string();
        self.pattern_string_end = j["pattern_string_end"]
            .as_str()
            .unwrap_or(r#"\{\{(Wikidata[ _]list[ _]end|"#)
            .to_string();
        self.main_item_prefix = j["main_item_prefix"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.sparql_prefix = j["sparql_prefix"].as_str().map(|s| s.to_string());
        if let Some(sic) = j["shadow_images_check"].as_array() {
            self.shadow_images_check = sic
                .iter()
                .map(|s| {
                    s.as_str()
                        .expect("shadow_images_check needs to be a string")
                        .to_string()
                })
                .collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_group_all_blocks_all() {
        let group = NamespaceGroup::All;
        assert!(!group.can_edit_namespace(0)); // Main namespace
        assert!(!group.can_edit_namespace(1)); // Talk
        assert!(!group.can_edit_namespace(10)); // Template
        assert!(!group.can_edit_namespace(-1)); // Special
        assert!(!group.can_edit_namespace(100)); // Any custom namespace
    }

    #[test]
    fn test_namespace_group_list_allows_unlisted() {
        let group = NamespaceGroup::List(vec![1, 3, 5]);
        assert!(group.can_edit_namespace(0)); // Not in list, positive
        assert!(group.can_edit_namespace(2)); // Not in list, positive
        assert!(group.can_edit_namespace(10)); // Not in list, positive
    }

    #[test]
    fn test_namespace_group_list_blocks_listed() {
        let group = NamespaceGroup::List(vec![1, 3, 5]);
        assert!(!group.can_edit_namespace(1)); // In list
        assert!(!group.can_edit_namespace(3)); // In list
        assert!(!group.can_edit_namespace(5)); // In list
    }

    #[test]
    fn test_namespace_group_list_blocks_negative() {
        let group = NamespaceGroup::List(Vec::new());
        assert!(!group.can_edit_namespace(-1)); // Negative always blocked
        assert!(!group.can_edit_namespace(-2)); // Negative always blocked
    }

    #[test]
    fn test_namespace_group_list_allows_zero() {
        let group = NamespaceGroup::List(vec![1, 2, 3]);
        assert!(group.can_edit_namespace(0)); // Main namespace, not in list
    }

    #[test]
    fn test_namespace_group_empty_list_allows_all_positive() {
        let group = NamespaceGroup::List(Vec::new());
        assert!(group.can_edit_namespace(0));
        assert!(group.can_edit_namespace(1));
        assert!(group.can_edit_namespace(100));
        assert!(!group.can_edit_namespace(-1)); // Still blocks negative
    }

    #[test]
    fn test_namespace_group_list_with_duplicates() {
        let group = NamespaceGroup::List(vec![1, 1, 2, 2, 3]);
        assert!(!group.can_edit_namespace(1));
        assert!(!group.can_edit_namespace(2));
        assert!(!group.can_edit_namespace(3));
        assert!(group.can_edit_namespace(4));
    }

    #[test]
    fn test_namespace_group_list_large_numbers() {
        let group = NamespaceGroup::List(vec![1000, 2000, 3000]);
        assert!(!group.can_edit_namespace(1000));
        assert!(!group.can_edit_namespace(2000));
        assert!(group.can_edit_namespace(999));
        assert!(group.can_edit_namespace(1001));
    }

    #[test]
    fn test_namespace_group_list_with_zero() {
        let group = NamespaceGroup::List(vec![0, 1, 2]);
        assert!(!group.can_edit_namespace(0)); // 0 is in the list
        assert!(!group.can_edit_namespace(1));
        assert!(group.can_edit_namespace(3));
    }

    #[test]
    fn test_namespace_group_list_negative_in_list() {
        // Edge case: what if someone adds negative to the list?
        let group = NamespaceGroup::List(vec![-1, 0, 1]);
        assert!(!group.can_edit_namespace(-1)); // Negative always blocked
        assert!(!group.can_edit_namespace(-2)); // Negative always blocked
        assert!(!group.can_edit_namespace(0)); // 0 in list
        assert!(!group.can_edit_namespace(1)); // 1 in list
    }

    // --- validate ---

    fn valid_config() -> Configuration {
        Configuration {
            default_api: "wikidata".to_string(),
            max_sparql_simultaneous: 5,
            max_threads: 4,
            api_timeout: 60,
            max_concurrent_entry_queries: 5,
            page_timeout_sec: 600,
            db_query_timeout_sec: 30,
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_valid_multi_wiki() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn test_validate_missing_default_api_in_multi_wiki_mode() {
        let mut config = valid_config();
        config.default_api = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_missing_default_api_ok_in_single_wiki_mode() {
        let mut config = valid_config();
        config.default_api = String::new();
        config.is_single_wiki = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_sparql_simultaneous() {
        let mut config = valid_config();
        config.max_sparql_simultaneous = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_zero_max_threads() {
        let mut config = valid_config();
        config.max_threads = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_zero_api_timeout() {
        let mut config = valid_config();
        config.api_timeout = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_zero_max_concurrent_entry_queries() {
        // A value of 0 would deadlock the entity-loading semaphore — reject it
        // at config validation time so the bot fails fast on startup.
        let mut config = valid_config();
        config.max_concurrent_entry_queries = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_zero_page_timeout_sec() {
        // A page_timeout of 0 would mean every dispatched page times out
        // instantly. Reject it at config validation time.
        let mut config = valid_config();
        config.page_timeout_sec = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_zero_db_query_timeout_sec() {
        // Similarly, a DB-query timeout of 0 would cancel every DB op
        // instantly. Reject it at config validation time.
        let mut config = valid_config();
        config.db_query_timeout_sec = 0;
        assert!(config.validate().is_err());
    }

    // ── check_for_shadow_images ────────────────────────────────────────────

    #[test]
    fn test_check_for_shadow_images_found() {
        let config = Configuration {
            shadow_images_check: vec!["enwiki".to_string(), "dewiki".to_string()],
            ..Default::default()
        };
        assert!(config.check_for_shadow_images("enwiki"));
        assert!(config.check_for_shadow_images("dewiki"));
    }

    #[test]
    fn test_check_for_shadow_images_not_found() {
        let config = Configuration {
            shadow_images_check: vec!["enwiki".to_string()],
            ..Default::default()
        };
        assert!(!config.check_for_shadow_images("frwiki"));
        assert!(!config.check_for_shadow_images(""));
    }

    #[test]
    fn test_check_for_shadow_images_empty_list() {
        let config = Configuration::default();
        assert!(!config.check_for_shadow_images("enwiki"));
    }

    // ── get_location_template ──────────────────────────────────────────────

    #[test]
    fn test_get_location_template_specific_wiki() {
        let mut config = Configuration::default();
        config
            .location_templates
            .insert("enwiki".to_string(), "{{Coord|$1|$2}}".to_string());
        assert_eq!(
            config.get_location_template("enwiki"),
            "{{Coord|$1|$2}}"
        );
    }

    #[test]
    fn test_get_location_template_falls_back_to_default() {
        let mut config = Configuration::default();
        config
            .location_templates
            .insert("default".to_string(), "{{Coord|$1|$2|default}}".to_string());
        assert_eq!(
            config.get_location_template("frwiki"),
            "{{Coord|$1|$2|default}}"
        );
    }

    #[test]
    fn test_get_location_template_specific_overrides_default() {
        let mut config = Configuration::default();
        config
            .location_templates
            .insert("default".to_string(), "default_tmpl".to_string());
        config
            .location_templates
            .insert("enwiki".to_string(), "enwiki_tmpl".to_string());
        assert_eq!(config.get_location_template("enwiki"), "enwiki_tmpl");
        assert_eq!(config.get_location_template("dewiki"), "default_tmpl");
    }

    #[test]
    fn test_get_location_template_none_returns_empty() {
        let config = Configuration::default();
        assert_eq!(config.get_location_template("enwiki"), "");
    }

    // ── can_edit_namespace ─────────────────────────────────────────────────

    #[test]
    fn test_can_edit_namespace_no_block_for_wiki() {
        let config = Configuration::default();
        // No entry for "enwiki" → editing is allowed in all namespaces
        assert!(config.can_edit_namespace("enwiki", 0));
        assert!(config.can_edit_namespace("enwiki", 10));
    }

    #[test]
    fn test_can_edit_namespace_wiki_with_all_block() {
        let mut config = Configuration::default();
        config
            .namespace_blocks
            .insert("enwiki".to_string(), NamespaceGroup::All);
        assert!(!config.can_edit_namespace("enwiki", 0));
        assert!(!config.can_edit_namespace("enwiki", 10));
        // Another wiki is still unrestricted
        assert!(config.can_edit_namespace("frwiki", 0));
    }

    #[test]
    fn test_can_edit_namespace_wiki_with_list_block() {
        let mut config = Configuration::default();
        config.namespace_blocks.insert(
            "enwiki".to_string(),
            NamespaceGroup::List(vec![1, 3]),
        );
        assert!(!config.can_edit_namespace("enwiki", 1));
        assert!(!config.can_edit_namespace("enwiki", 3));
        assert!(config.can_edit_namespace("enwiki", 0));
        assert!(config.can_edit_namespace("enwiki", 10));
    }

    // ── get_local_template_title_start / _end ─────────────────────────────

    #[test]
    fn test_get_local_template_title_start_with_namespace_prefix() {
        let mut config = Configuration::default();
        config
            .template_start_sites
            .insert("enwiki".to_string(), "Template:Wikidata list".to_string());
        assert_eq!(
            config.get_local_template_title_start("enwiki").unwrap(),
            "Wikidata list"
        );
    }

    #[test]
    fn test_get_local_template_title_start_missing_wiki_is_err() {
        let config = Configuration::default();
        assert!(config.get_local_template_title_start("enwiki").is_err());
    }

    #[test]
    fn test_get_local_template_title_end_with_namespace_prefix() {
        let mut config = Configuration::default();
        config
            .template_end_sites
            .insert("enwiki".to_string(), "Template:Wikidata list end".to_string());
        assert_eq!(
            config.get_local_template_title_end("enwiki").unwrap(),
            "Wikidata list end"
        );
    }

    #[test]
    fn test_get_local_template_title_end_missing_wiki_is_err() {
        let config = Configuration::default();
        assert!(config.get_local_template_title_end("enwiki").is_err());
    }

    #[test]
    fn test_get_local_template_title_no_colon_returns_full_name() {
        let mut config = Configuration::default();
        config
            .template_start_sites
            .insert("enwiki".to_string(), "Wikidata_list".to_string());
        // No colon → split_back gives the whole string
        assert_eq!(
            config.get_local_template_title_start("enwiki").unwrap(),
            "Wikidata_list"
        );
    }

    // ── mysql accessor ─────────────────────────────────────────────────────

    #[test]
    fn test_mysql_accessor_no_config_returns_null() {
        let config = Configuration::default();
        assert!(config.mysql("host").is_null());
    }

    #[test]
    fn test_mysql_accessor_with_config() {
        let config = Configuration {
            mysql: Some(serde_json::json!({"host": "localhost", "port": 3306})),
            ..Default::default()
        };
        assert_eq!(config.mysql("host").as_str(), Some("localhost"));
        assert_eq!(config.mysql("port").as_u64(), Some(3306));
        assert!(config.mysql("missing").is_null());
    }

    // ── pool accessor ──────────────────────────────────────────────────────

    #[test]
    fn test_pool_accessor_no_pool_is_err() {
        let config = Configuration::default();
        assert!(config.pool().is_err());
    }

    // ── default_thumbnail_size ─────────────────────────────────────────────

    #[test]
    fn test_default_thumbnail_size_uses_default_128() {
        let config = Configuration::default();
        assert_eq!(config.default_thumbnail_size(), 128);
    }

    #[test]
    fn test_default_thumbnail_size_custom() {
        let config = Configuration {
            default_thumbnail_size: Some(256),
            ..Default::default()
        };
        assert_eq!(config.default_thumbnail_size(), 256);
    }

    // ── api_timeout ────────────────────────────────────────────────────────

    #[test]
    fn test_api_timeout_as_duration() {
        let config = Configuration {
            api_timeout: 60,
            ..Default::default()
        };
        assert_eq!(config.api_timeout(), Duration::from_secs(60));
    }

    // ── get_default_wbapi ──────────────────────────────────────────────────

    #[test]
    fn test_get_default_wbapi_no_api_is_err() {
        let config = Configuration::default();
        assert!(config.get_default_wbapi().is_err());
    }

    // ── set_wikis / get_wiki ───────────────────────────────────────────────

    #[test]
    fn test_set_and_get_wiki() {
        let mut config = Configuration::default();
        let w = Wiki::from_row((
            1,
            "enwiki".to_string(),
            "active".to_string(),
            "20240101".to_string(),
            true,
            true,
        ))
        .unwrap();
        config.set_wikis([("enwiki".to_string(), w.clone())].into());
        assert!(config.get_wiki("enwiki").is_some());
        assert!(config.get_wiki("dewiki").is_none());
    }

    // ── is_single_wiki detection ───────────────────────────────────────────

    #[test]
    fn test_is_single_wiki_false_by_default() {
        let config = Configuration::default();
        assert!(!config.is_single_wiki());
    }

    // ── new_from_json_misc defaults ────────────────────────────────────────

    #[test]
    fn test_new_from_json_misc_fills_defaults() {
        let mut config = Configuration::default();
        config.new_from_json_misc(&serde_json::json!({}));
        // All optional fields absent → use hard-coded defaults
        assert_eq!(config.max_sparql_simultaneous, 10);
        assert_eq!(config.api_timeout, 360);
        assert_eq!(config.max_threads, 8);
        assert_eq!(config.max_local_cached_entities, 5000);
    }

    #[test]
    fn test_new_from_json_misc_reads_provided_values() {
        let mut config = Configuration::default();
        config.new_from_json_misc(&serde_json::json!({
            "max_sparql_simultaneous": 3,
            "api_timeout": 120,
            "max_threads": 16,
            "default_language": "fr",
            "quiet": true,
            "profiling": true,
        }));
        assert_eq!(config.max_sparql_simultaneous, 3);
        assert_eq!(config.api_timeout, 120);
        assert_eq!(config.max_threads, 16);
        assert_eq!(config.default_language, "fr");
        assert!(config.quiet);
        assert!(config.profiling);
    }

    // ── fix_wiki_name ──────────────────────────────────────────────────────

    #[test]
    fn test_fix_wiki_name_be_x_oldwiki_default_aliases() {
        let config = Configuration::default();
        // All four historical spellings collapse to be_x_oldwiki (T11216).
        assert_eq!(config.fix_wiki_name("be-taraskwiki"), "be_x_oldwiki");
        assert_eq!(config.fix_wiki_name("be_taraskwiki"), "be_x_oldwiki");
        assert_eq!(config.fix_wiki_name("be-x-oldwiki"), "be_x_oldwiki");
        assert_eq!(config.fix_wiki_name("be_x_oldwiki"), "be_x_oldwiki");
    }

    #[test]
    fn test_fix_wiki_name_no_alias_replaces_hyphens() {
        let config = Configuration::default();
        assert_eq!(config.fix_wiki_name("dewiki"), "dewiki");
        assert_eq!(config.fix_wiki_name("zh-yuewiki"), "zh_yuewiki");
        assert_eq!(config.fix_wiki_name("simple-en-wiki"), "simple_en_wiki");
    }

    #[test]
    fn test_fix_wiki_name_empty_string() {
        let config = Configuration::default();
        assert_eq!(config.fix_wiki_name(""), "");
    }

    #[test]
    fn test_fix_wiki_name_user_alias_extends_defaults() {
        let mut config = Configuration::default();
        config.new_from_json_misc(&serde_json::json!({
            "wiki_name_aliases": { "fake-old": "newname" }
        }));
        // The user-supplied alias resolves
        assert_eq!(config.fix_wiki_name("fake-old"), "newname");
        // The default be_x_oldwiki aliases are still in place
        assert_eq!(config.fix_wiki_name("be-taraskwiki"), "be_x_oldwiki");
        // Unrelated wikis still hyphen-replace
        assert_eq!(config.fix_wiki_name("dewiki"), "dewiki");
    }

    #[test]
    fn test_fix_wiki_name_user_alias_overrides_default() {
        let mut config = Configuration::default();
        config.new_from_json_misc(&serde_json::json!({
            "wiki_name_aliases": { "be_x_oldwiki": "be_overridden" }
        }));
        // User entry for an existing key wins
        assert_eq!(config.fix_wiki_name("be_x_oldwiki"), "be_overridden");
        // Sibling defaults that the user didn't override remain
        assert_eq!(config.fix_wiki_name("be-taraskwiki"), "be_x_oldwiki");
    }

    // ── is_wiki_case_sensitive ─────────────────────────────────────────────

    #[test]
    fn test_is_wiki_case_sensitive_wiktionary_by_suffix() {
        let config = Configuration::default();
        // Every wiktionary project is case-sensitive without needing explicit
        // config entries.
        assert!(config.is_wiki_case_sensitive("enwiktionary"));
        assert!(config.is_wiki_case_sensitive("dewiktionary"));
        assert!(config.is_wiki_case_sensitive("zh_min_nanwiktionary"));
    }

    #[test]
    fn test_is_wiki_case_sensitive_non_wiktionary_default_false() {
        let config = Configuration::default();
        // The vast majority of Wikimedia projects are first-letter; default
        // must not flag them as case-sensitive.
        assert!(!config.is_wiki_case_sensitive("enwiki"));
        assert!(!config.is_wiki_case_sensitive("wikidatawiki"));
        assert!(!config.is_wiki_case_sensitive("commonswiki"));
        assert!(!config.is_wiki_case_sensitive("dewiki"));
    }

    #[test]
    fn test_is_wiki_case_sensitive_reads_explicit_list_from_json() {
        let mut config = Configuration::default();
        config.new_from_json_misc(&serde_json::json!({
            "case_sensitive_wikis": ["customwiki", "another-wiki"]
        }));
        assert!(config.is_wiki_case_sensitive("customwiki"));
        assert!(config.is_wiki_case_sensitive("another-wiki"));
        // wiktionary heuristic still applies after explicit list parses
        assert!(config.is_wiki_case_sensitive("frwiktionary"));
        // Unrelated wiki stays false
        assert!(!config.is_wiki_case_sensitive("frwiki"));
    }

    // ── new_from_json_locations ────────────────────────────────────────────

    #[test]
    fn test_new_from_json_locations_reads_templates() {
        let mut config = Configuration::default();
        config.new_from_json_locations(&serde_json::json!({
            "location_templates": {
                "default": "{{Coord|$1|$2}}",
                "enwiki": "{{Coord|$1|$2|display=title}}"
            },
            "location_regions": ["US", "DE"]
        }));
        assert_eq!(
            config.location_templates.get("default").unwrap(),
            "{{Coord|$1|$2}}"
        );
        assert_eq!(
            config.location_templates.get("enwiki").unwrap(),
            "{{Coord|$1|$2|display=title}}"
        );
        assert_eq!(config.location_regions, vec!["US", "DE"]);
    }

    // ── new_from_json_namespace_blocks ─────────────────────────────────────

    #[test]
    fn test_new_from_json_namespace_blocks_star_means_all() {
        let mut config = Configuration::default();
        let j = serde_json::json!({ "namespace_blocks": { "enwiki": "*" } });
        config.new_from_json_namespace_blocks(&j).unwrap();
        assert!(matches!(
            config.namespace_blocks.get("enwiki").unwrap(),
            NamespaceGroup::All
        ));
    }

    #[test]
    fn test_new_from_json_namespace_blocks_array_means_list() {
        let mut config = Configuration::default();
        let j = serde_json::json!({ "namespace_blocks": { "enwiki": [1, 3] } });
        config.new_from_json_namespace_blocks(&j).unwrap();
        assert!(matches!(
            config.namespace_blocks.get("enwiki").unwrap(),
            NamespaceGroup::List(_)
        ));
    }

    #[test]
    fn test_new_from_json_namespace_blocks_unrecognised_string_is_err() {
        let mut config = Configuration::default();
        let j = serde_json::json!({ "namespace_blocks": { "enwiki": "bad_value" } });
        assert!(config.new_from_json_namespace_blocks(&j).is_err());
    }

    // ── consuming builder (with_*) ─────────────────────────────────────────

    #[test]
    fn test_with_profiling_returns_updated_config() {
        let config = Configuration::default().with_profiling(true);
        assert!(config.profiling());
    }

    #[test]
    fn test_with_profiling_false() {
        let config = Configuration {
            profiling: true,
            ..Default::default()
        };
        let config = config.with_profiling(false);
        assert!(!config.profiling());
    }

    #[test]
    fn test_with_max_local_cached_entities() {
        let config = Configuration::default().with_max_local_cached_entities(42);
        assert_eq!(config.max_local_cached_entities(), 42);
    }

    #[test]
    fn test_sparql_semaphore_for_returns_same_handle_per_endpoint() {
        // Two callers asking for the semaphore for the same endpoint must
        // get handles to the same underlying Semaphore — otherwise the
        // concurrency cap would be per-caller, not per-endpoint.
        let config = Configuration {
            max_sparql_simultaneous: 5,
            ..Default::default()
        };
        let s1 = config.sparql_semaphore_for("https://example.org/sparql");
        let s2 = config.sparql_semaphore_for("https://example.org/sparql");
        assert!(
            Arc::ptr_eq(&s1, &s2),
            "same endpoint must return the same Semaphore Arc"
        );
    }

    #[test]
    fn test_sparql_semaphore_for_isolates_distinct_endpoints() {
        // Distinct endpoints must get distinct Semaphores so a slow one
        // can't starve a healthy one.
        let config = Configuration {
            max_sparql_simultaneous: 5,
            ..Default::default()
        };
        let s1 = config.sparql_semaphore_for("https://wcqs.example.org/sparql");
        let s2 = config.sparql_semaphore_for("https://wdqs.example.org/sparql");
        assert!(
            !Arc::ptr_eq(&s1, &s2),
            "distinct endpoints must have isolated Semaphores"
        );
    }

    #[test]
    fn test_with_wikis_adds_wikis() {
        use crate::wiki::Wiki;
        let w = Wiki::from_row((
            1,
            "enwiki".to_string(),
            "active".to_string(),
            "20240101".to_string(),
            true,
            true,
        ))
        .unwrap();
        let config =
            Configuration::default().with_wikis([("enwiki".to_string(), w)].into());
        assert!(config.get_wiki("enwiki").is_some());
    }
}
