//! SPARQL query execution with retry logic and rate limiting.

use crate::listeria_error::ListeriaError;
use crate::page_params::PageParams;
use crate::telemetry;
use anyhow::Result;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use wikimisc::{
    sparql_client::{SparqlClient, SparqlError},
    sparql_endpoint::SparqlEndpoint,
    sparql_results::SparqlApiResult,
    sparql_retry::{Retry, RetryPolicy},
    sparql_rewrite::{
        LabelServiceRewrite, NamedSubqueryRewrite, rewrite_label_service, rewrite_named_subqueries,
    },
    sparql_table_vec::SparqlTableVec,
};

/// Max attempts for a single SPARQL query (1 initial + retries).
///
/// Wikidata Query Service routinely returns transient 5xx / connection-reset
/// failures during load spikes; one or two retries converts most of these
/// into successes without operator intervention.
const SPARQL_MAX_ATTEMPTS: u32 = 3;
/// Initial backoff between SPARQL retries; doubles each attempt.
/// 500 ms → 1 s → 2 s gives the endpoint room to recover from a transient
/// overload without monopolising the bot's wall-clock budget.
const SPARQL_INITIAL_BACKOFF_MS: u64 = 500;

#[derive(Debug, Clone)]
pub struct SparqlResults {
    page_params: Arc<PageParams>,
    sparql_main_variable: Option<String>,
    wikibase_key: String,
    /// Where to send queries; when `None`, see [`Self::resolve_endpoint`].
    endpoint: Option<SparqlEndpoint>,
    /// Upper-cased names of the result variables the caller reads, so a
    /// query rewrite knows which label outputs it must keep.
    used_variables: HashSet<String>,
    simulate: bool,
}

impl SparqlResults {
    #[must_use]
    pub fn new(page_params: Arc<PageParams>, wikibase_key: &str) -> Self {
        let simulate = page_params.simulate();
        Self {
            page_params,
            wikibase_key: wikibase_key.to_string(),
            sparql_main_variable: None,
            endpoint: None,
            used_variables: HashSet::new(),
            simulate,
        }
    }

    /// Sends queries to `endpoint` rather than the wikibase's default.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: SparqlEndpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// The result variables the caller reads, in any case.
    #[must_use]
    pub fn with_used_variables(mut self, variables: impl IntoIterator<Item = String>) -> Self {
        self.used_variables = variables.into_iter().map(|v| v.to_uppercase()).collect();
        self
    }

    pub const fn set_simulate(&mut self, simulate: bool) {
        self.simulate = simulate;
    }

    /// Executes a SPARQL query with template expansion and retry logic.
    pub async fn run_query(&mut self, mut sparql: String) -> Result<SparqlTableVec> {
        self.expand_sparql_templates(&mut sparql).await?;

        // Return simulated results early, skipping the real SPARQL endpoint.
        if self.simulate
            && let Some(table) = self.build_simulated_table()?
        {
            return Ok(table);
        }
        self.run_sparql_query(&sparql).await
    }

    /// Builds a `SparqlTableVec` from the simulated SPARQL results stored in
    /// `page_params`, returning `None` when no simulated results are configured.
    fn build_simulated_table(&mut self) -> Result<Option<SparqlTableVec>> {
        let Some(json_text) = self.page_params.simulated_sparql_results() else {
            return Ok(None);
        };
        let result: SparqlApiResult = serde_json::from_str(json_text)?;
        Ok(Some(self.table_from_result(result)?))
    }

    fn table_from_result(&mut self, result: SparqlApiResult) -> Result<SparqlTableVec> {
        self.set_main_variable(&result);
        let mut table = SparqlTableVec::from_api_result(result)?;
        table.set_main_variable(self.sparql_main_variable());
        Ok(table)
    }

    async fn run_sparql_query(&mut self, sparql: &str) -> Result<SparqlTableVec> {
        let endpoint = self.resolve_endpoint()?;
        let config = self.page_params.config();
        // Acquire the per-endpoint permit so a slow endpoint can't starve
        // concurrent calls to a healthy one.
        let semaphore = config.sparql_semaphore_for(endpoint.url());
        let _permit = semaphore.acquire().await?;

        let circuit_breaker = config.sparql_circuit_breaker(endpoint.url());
        if circuit_breaker.is_open() {
            tracing::warn!(
                endpoint = %endpoint.url(),
                "SPARQL circuit OPEN; short-circuiting query (audit F5.2)"
            );
            return Err(ListeriaError::SparqlCircuitOpen(endpoint.url().to_string()).into());
        }

        let sparql = self.adapted_to(endpoint.url(), sparql);
        let sparql = match config.sparql_prefix() {
            Some(prefix) => format!("{}\n{}", prefix, sparql),
            None => sparql.into_owned(),
        };
        // The client adds the endpoint's own prefixes, if it has any.
        //
        // It retries so a single transient flake (5xx, rate limiting, broken
        // connection) doesn't fail the whole page; a malformed query or a
        // timeout fails the same way every time, so it is not retried. The
        // circuit breaker is only updated by the *terminal* outcome of the
        // retry budget — intermediate retry failures don't ding the breaker,
        // so a flake recovered by retry leaves the breaker fully healthy.
        let http_client = config.sparql_http_client(endpoint.url())?;
        let client = SparqlClient::with_client(endpoint, http_client)
            .with_max_response_bytes(config.max_sparql_response_bytes())
            .with_retry_policy(
                RetryPolicy::new(SPARQL_MAX_ATTEMPTS - 1)
                    .with_initial_backoff(Duration::from_millis(SPARQL_INITIAL_BACKOFF_MS))
                    .on_retry(log_retry),
            );
        let result = client.query(&sparql).await;

        match result {
            Ok(result) => {
                circuit_breaker.record_success();
                self.table_from_result(result)
            }
            Err(e) => {
                if Self::is_query_error(&e) {
                    // The endpoint answered, so it is healthy.
                    circuit_breaker.record_success();
                } else {
                    circuit_breaker.record_failure();
                    telemetry::record_sparql_failure();
                }
                Err(e.into())
            }
        }
    }

    /// `sparql` as the endpoint at `endpoint_url` needs it: with
    /// Blazegraph-only features rewritten into standard SPARQL, if the
    /// endpoint is configured for that. A query the rewrite does not support
    /// is sent as it is, and the endpoint reports why it fails.
    fn adapted_to<'q>(&self, endpoint_url: &str, sparql: &'q str) -> Cow<'q, str> {
        if !self
            .page_params
            .config()
            .rewrites_blazegraph_queries(endpoint_url)
        {
            return Cow::Borrowed(sparql);
        }
        // Named subqueries first: the label service rewrite does not look into them.
        let sparql = match rewrite_named_subqueries(sparql) {
            NamedSubqueryRewrite::Rewritten(query) => Cow::Owned(query),
            NamedSubqueryRewrite::Unchanged => Cow::Borrowed(sparql),
            NamedSubqueryRewrite::Unsupported(reason) => {
                tracing::info!(
                    endpoint = endpoint_url,
                    %reason,
                    "named subqueries not rewritten; sending the query unchanged"
                );
                return Cow::Borrowed(sparql);
            }
        };
        let is_used = |variable: &str| self.used_variables.contains(&variable.to_uppercase());
        match rewrite_label_service(&sparql, is_used) {
            LabelServiceRewrite::Rewritten(query) => Cow::Owned(query),
            LabelServiceRewrite::Unchanged => sparql,
            LabelServiceRewrite::Unsupported(reason) => {
                tracing::info!(
                    endpoint = endpoint_url,
                    %reason,
                    "label service not rewritten; sending the query with it as written"
                );
                sparql
            }
        }
    }

    /// Whether the query itself is at fault, as opposed to the endpoint
    /// failing to answer it: the endpoint rejected it (HTTP 400, e.g. a
    /// syntax error or an unknown prefix), or its result is too large to
    /// read. Such an error recurs on every attempt and says nothing about
    /// the endpoint's health.
    const fn is_query_error(error: &SparqlError) -> bool {
        matches!(
            error,
            SparqlError::Endpoint { status: 400, .. } | SparqlError::ResponseTooLarge { .. }
        )
    }

    /// The endpoint queries go to, in order of precedence:
    /// 1. the one set with [`Self::with_endpoint`];
    /// 2. the one `default_sparql_endpoints` assigns to this wikibase;
    /// 3. the one the wikibase advertises in its siteinfo (`wikibase-sparql`).
    fn resolve_endpoint(&self) -> Result<SparqlEndpoint> {
        if let Some(endpoint) = &self.endpoint {
            return Ok(endpoint.clone());
        }
        let config = self.page_params.config();
        if let Some(endpoint) = config.default_sparql_endpoint(&self.wikibase_key) {
            return Ok(endpoint.clone());
        }
        let api = config
            .get_wbapi(&self.wikibase_key)
            .ok_or_else(|| ListeriaError::SparqlNoConfig(self.wikibase_key.clone()))?;
        let url = api
            .get_site_info_string("general", "wikibase-sparql")
            .map_err(|_| ListeriaError::SparqlNoEndpoint(self.wikibase_key.clone()))?;
        Ok(SparqlEndpoint::new(url))
    }

    fn set_main_variable(&mut self, result: &SparqlApiResult) {
        self.sparql_main_variable = None;
        if let Some(arr) = result.head().get("vars") {
            // Insist on ?item
            let required_variable_name = "item";
            for v in arr {
                if required_variable_name == v {
                    self.sparql_main_variable = Some(required_variable_name.to_string());
                    break;
                }
            }
        }
    }

    async fn expand_sparql_templates(&self, sparql: &mut String) -> Result<()> {
        if !sparql.contains("{{") {
            // No template
            return Ok(());
        }
        let api = self.page_params.mw_api();
        let params: HashMap<String, String> = vec![
            ("action", "expandtemplates"),
            ("title", self.page_params.page()),
            ("prop", "wikitext"),
            ("text", sparql),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        // The underlying reqwest client is already configured with
        // `api_timeout`, but a future refactor that swaps clients would
        // silently regress that bound. Wrap the call in a defensive outer
        // timeout so a hung MW API can never block the SPARQL pipeline
        // indefinitely.
        let timeout = self.page_params.config().api_timeout();
        let j = tokio::time::timeout(timeout, api.get_query_api_json(&params))
            .await
            .map_err(|_| {
                anyhow::anyhow!("expandtemplates timed out after {}s", timeout.as_secs())
            })??;
        if let Some(s) = j["expandtemplates"]["wikitext"].as_str() {
            *sparql = s.to_string();
        }
        Ok(())
    }

    #[must_use]
    pub fn sparql_main_variable(&self) -> Option<String> {
        self.sparql_main_variable.to_owned()
    }
}

/// Logs a SPARQL retry with the same fields as the MediaWiki API retries
/// (`page_operations`).
fn log_retry(retry: &Retry<'_>) {
    tracing::warn!(
        operation = "sparql_query",
        endpoint = retry.endpoint,
        attempt = retry.retry,
        backoff_ms = u64::try_from(retry.wait.as_millis()).unwrap_or(u64::MAX),
        error = %retry.error,
        "external call failed; retrying"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparql_result_parsing_with_item_variable() {
        // Test parsing SPARQL result with "item" variable
        let json = r#"{
            "head": {
                "vars": ["item", "itemLabel"]
            },
            "results": {
                "bindings": []
            }
        }"#;

        let result: SparqlApiResult = serde_json::from_str(json).unwrap();

        // Verify the result has the expected structure
        assert!(result.head().get("vars").is_some());
        if let Some(vars) = result.head().get("vars") {
            assert!(vars.contains(&"item".to_string()));
            assert_eq!(vars.len(), 2);
        }
    }

    #[test]
    fn test_sparql_result_parsing_without_item_variable() {
        // Test parsing SPARQL result without "item" variable
        let json = r#"{
            "head": {
                "vars": ["foo", "bar"]
            },
            "results": {
                "bindings": []
            }
        }"#;

        let result: SparqlApiResult = serde_json::from_str(json).unwrap();

        // Verify "item" is not present
        if let Some(vars) = result.head().get("vars") {
            assert!(!vars.contains(&"item".to_string()));
            assert_eq!(vars.len(), 2);
        }
    }

    #[test]
    fn test_sparql_result_item_not_first_position() {
        // Test SPARQL result with "item" not as first variable
        let json = r#"{
            "head": {
                "vars": ["foo", "item", "bar"]
            },
            "results": {
                "bindings": []
            }
        }"#;

        let result: SparqlApiResult = serde_json::from_str(json).unwrap();

        // Verify "item" is present even when not first
        if let Some(vars) = result.head().get("vars") {
            assert!(vars.contains(&"item".to_string()));
            assert_eq!(vars[1], "item");
        }
    }

    #[test]
    fn test_sparql_result_empty_vars() {
        // Test SPARQL result with empty vars array
        let json = r#"{
            "head": {
                "vars": []
            },
            "results": {
                "bindings": []
            }
        }"#;

        let result: SparqlApiResult = serde_json::from_str(json).unwrap();

        // Verify empty vars array
        if let Some(vars) = result.head().get("vars") {
            assert_eq!(vars.len(), 0);
        }
    }

    #[test]
    fn test_sparql_result_multiple_variables() {
        // Test SPARQL result with multiple variables
        let json = r#"{
            "head": {
                "vars": ["item", "prop", "value", "itemLabel"]
            },
            "results": {
                "bindings": []
            }
        }"#;

        let result: SparqlApiResult = serde_json::from_str(json).unwrap();

        // Verify multiple variables are parsed correctly
        if let Some(vars) = result.head().get("vars") {
            assert_eq!(vars.len(), 4);
            assert!(vars.contains(&"item".to_string()));
            assert!(vars.contains(&"prop".to_string()));
            assert!(vars.contains(&"value".to_string()));
            assert!(vars.contains(&"itemLabel".to_string()));
        }
    }

    #[test]
    fn test_sparql_table_creation() {
        // Test that SparqlTable can be created from a valid result
        let json = r#"{
            "head": {
                "vars": ["item"]
            },
            "results": {
                "bindings": []
            }
        }"#;

        let result: SparqlApiResult = serde_json::from_str(json).unwrap();
        let table = SparqlTableVec::from_api_result(result);

        assert!(table.is_ok());
    }

    #[test]
    fn test_sparql_table_creation_with_data() {
        // Test SparqlTable creation with actual data
        let json = r#"{
            "head": {
                "vars": ["item", "itemLabel"]
            },
            "results": {
                "bindings": [
                    {
                        "item": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q42"
                        },
                        "itemLabel": {
                            "type": "literal",
                            "value": "Douglas Adams"
                        }
                    }
                ]
            }
        }"#;

        let result: SparqlApiResult = serde_json::from_str(json).unwrap();
        let table = SparqlTableVec::from_api_result(result);

        assert!(table.is_ok());
    }

    #[test]
    fn test_clone_string_types() {
        // Test cloning of the string types used in SparqlResults
        let wikibase_key = "wikidata".to_string();
        let cloned = wikibase_key.clone();
        assert_eq!(wikibase_key, cloned);

        let endpoint = Some("https://example.com/sparql".to_string());
        let cloned_endpoint = endpoint.clone();
        assert_eq!(endpoint, cloned_endpoint);

        let main_var: Option<String> = Some("item".to_string());
        let cloned_var = main_var.clone();
        assert_eq!(main_var, cloned_var);
    }

    #[test]
    fn test_json_parsing_edge_cases() {
        // Test that we can parse various SPARQL result formats
        let json_no_bindings = r#"{"head": {"vars": []}, "results": {"bindings": []}}"#;
        let result1: Result<SparqlApiResult, _> = serde_json::from_str(json_no_bindings);
        assert!(result1.is_ok());

        // Test with multiple variables
        let json_multi = r#"{
            "head": {"vars": ["a", "b", "c"]},
            "results": {"bindings": []}
        }"#;
        let result2: Result<SparqlApiResult, _> = serde_json::from_str(json_multi);
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_build_simulated_table_with_valid_json() {
        use crate::page_params::PageParams;
        use std::sync::Arc;

        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
        let mut page_params = PageParams::new(config, api, "Test".to_string())
            .await
            .unwrap();

        let sparql_json = r#"{
            "head": { "vars": ["item"] },
            "results": {
                "bindings": [
                    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q42" } }
                ]
            }
        }"#;
        page_params.set_simulation(None, Some(sparql_json.to_string()), None);

        let page_params = Arc::new(page_params);
        let mut sparql_results = SparqlResults::new(page_params, "wikidata");
        sparql_results.set_simulate(true);

        let result = sparql_results.build_simulated_table().unwrap();
        assert!(
            result.is_some(),
            "Expected Some(table) when simulated SPARQL results are set"
        );
        let table = result.unwrap();
        assert_eq!(table.len(), 1, "Expected one row in the simulated table");
        assert_eq!(
            sparql_results.sparql_main_variable(),
            Some("item".to_string()),
            "Expected main variable to be 'item'"
        );
    }

    #[tokio::test]
    async fn test_build_simulated_table_returns_none_when_no_results_configured() {
        use crate::page_params::PageParams;
        use std::sync::Arc;

        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
        let page_params = PageParams::new(config, api, "Test".to_string())
            .await
            .unwrap();
        let page_params = Arc::new(page_params);

        // No simulated SPARQL results set
        let mut sparql_results = SparqlResults::new(page_params, "wikidata");
        sparql_results.set_simulate(true);

        let result = sparql_results.build_simulated_table().unwrap();
        assert!(
            result.is_none(),
            "Expected None when no simulated SPARQL results are configured"
        );
    }

    // ── endpoint selection and transport ──────────────────────────────────

    use crate::configuration::Configuration;
    use crate::test_utils::{cached_config_clone, sent_sparql_query};
    use wikimisc::sparql_value::SparqlValue;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const MOCK_PATH: &str = "/sparql";
    const QUERY: &str = "SELECT ?item ?coord WHERE { ?item wdt:P625 ?coord }";
    /// A QLever result, which spells coordinates `POINT(…)`.
    const QLEVER_RESULT: &str = r#"{"head":{"vars":["item","coord"]},"results":{"bindings":[{"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q64"},"coord":{"datatype":"http://www.opengis.net/ont/geosparql#wktLiteral","type":"literal","value":"POINT(13.383333 52.516667)"}}]},"meta":{"query-time-ms":9,"result-size-total":1}}"#;
    const QLEVER_SYNTAX_ERROR: &str = r#"{"exception":"Invalid SPARQL query: Prefix wdt was not registered using a PREFIX declaration","status":"ERROR"}"#;

    async fn page_params_with(config: Configuration) -> Arc<PageParams> {
        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        Arc::new(
            PageParams::new(Arc::new(config), api, "Test".to_string())
                .await
                .unwrap(),
        )
    }

    fn mock_endpoint(server: &MockServer) -> SparqlEndpoint {
        SparqlEndpoint::new(format!("{}{MOCK_PATH}", server.uri()))
    }

    async fn mount(server: &MockServer, response: ResponseTemplate, expected_calls: u64) {
        Mock::given(method("POST"))
            .and(path(MOCK_PATH))
            .respond_with(response)
            .expect(expected_calls)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn test_current_config_uses_the_wikibase_endpoint() {
        // With today's config (no `sparql_endpoints`), Wikidata lists must
        // keep going to WDQS, with nothing added to the query.
        let page_params = page_params_with(cached_config_clone().await).await;
        let endpoint = SparqlResults::new(page_params, "wikidata")
            .resolve_endpoint()
            .unwrap();
        assert_eq!(endpoint, SparqlEndpoint::wdqs());
    }

    #[tokio::test]
    async fn test_resolve_endpoint_precedence() {
        let a = SparqlEndpoint::new("https://a.example/sparql");
        let b = SparqlEndpoint::new("https://b.example/sparql");
        let config = cached_config_clone()
            .await
            .with_sparql_endpoint("a", a.clone())
            .with_sparql_endpoint("b", b.clone())
            .with_default_sparql_endpoint("wikidata", "a");
        let page_params = page_params_with(config).await;

        let configured_default = SparqlResults::new(page_params.clone(), "wikidata");
        assert_eq!(configured_default.resolve_endpoint().unwrap(), a);
        let explicit = configured_default.with_endpoint(b.clone());
        assert_eq!(explicit.resolve_endpoint().unwrap(), b);
    }

    #[tokio::test]
    async fn test_resolve_endpoint_errors() {
        let page_params = page_params_with(cached_config_clone().await).await;
        // Commons advertises no SPARQL endpoint in its siteinfo.
        let no_endpoint = SparqlResults::new(page_params.clone(), "commons")
            .resolve_endpoint()
            .unwrap_err();
        assert!(
            no_endpoint.to_string().contains("No SPARQL endpoint"),
            "{no_endpoint}"
        );
        let no_wikibase = SparqlResults::new(page_params, "nosuchwikibase")
            .resolve_endpoint()
            .unwrap_err();
        assert!(
            no_wikibase.to_string().contains("No wikibase setup"),
            "{no_wikibase}"
        );
    }

    #[tokio::test]
    async fn test_query_is_sent_with_headers_and_endpoint_prefixes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MOCK_PATH))
            .and(header("accept", "application/sparql-results+json"))
            .and(header("user-agent", crate::LISTERIA_USER_AGENT))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(QLEVER_RESULT, "application/sparql-results+json"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let endpoint =
            mock_endpoint(&server).with_prefix("wdt", "http://www.wikidata.org/prop/direct/");
        let page_params = page_params_with(cached_config_clone().await).await;
        let mut sparql_results =
            SparqlResults::new(page_params, "wikidata").with_endpoint(endpoint);

        let table = sparql_results.run_query(QUERY.to_string()).await.unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(
            sparql_results.sparql_main_variable(),
            Some("item".to_string())
        );
        let coord = table.get_var_index("coord").unwrap();
        assert!(matches!(
            table.get_row_col(0, coord),
            Some(SparqlValue::Location(_))
        ));
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            sent_sparql_query(&requests[0]),
            format!("PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n{QUERY}")
        );
    }

    #[tokio::test]
    async fn test_configured_user_agent_is_sent() {
        // WDQS v2 admits only allow-listed user agents during its pilot.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(MOCK_PATH))
            .and(header("user-agent", "ListeriaBot/v2pilot"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(QLEVER_RESULT, "application/sparql-results+json"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let endpoint = mock_endpoint(&server);
        let config = cached_config_clone()
            .await
            .with_sparql_user_agent(endpoint.url(), "ListeriaBot/v2pilot");
        let page_params = page_params_with(config).await;
        let mut sparql_results =
            SparqlResults::new(page_params, "wikidata").with_endpoint(endpoint);

        assert!(sparql_results.run_query(QUERY.to_string()).await.is_ok());
    }

    const LABEL_QUERY: &str = "SELECT ?item ?itemLabel WHERE { ?item wdt:P31 wd:Q5 . SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" } }";

    /// Runs `LABEL_QUERY` against a mock and returns the query it received.
    async fn sent_label_query(rewrite: bool, used: &[&str]) -> String {
        sent_query(LABEL_QUERY, rewrite, used).await
    }

    async fn sent_query(query: &str, rewrite: bool, used: &[&str]) -> String {
        let server = MockServer::start().await;
        mount(
            &server,
            ResponseTemplate::new(200)
                .set_body_raw(QLEVER_RESULT, "application/sparql-results+json"),
            1,
        )
        .await;
        let endpoint = mock_endpoint(&server);
        let mut config = cached_config_clone().await;
        if rewrite {
            config = config.with_blazegraph_rewrite(endpoint.url());
        }
        let page_params = page_params_with(config).await;
        let mut sparql_results = SparqlResults::new(page_params, "wikidata")
            .with_endpoint(endpoint)
            .with_used_variables(used.iter().map(|v| v.to_string()));
        sparql_results.run_query(query.to_string()).await.unwrap();
        sent_sparql_query(&server.received_requests().await.unwrap()[0])
    }

    #[tokio::test]
    async fn test_named_subqueries_are_inlined_for_rewriting_endpoint() {
        let query = "SELECT ?item ?itemLabel WITH { SELECT ?item WHERE { ?item wdt:P31 wd:Q5 } } AS %people \
                     WHERE { INCLUDE %people SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" } }";
        let sent = sent_query(query, true, &["ITEMLABEL"]).await;
        assert!(
            !sent.contains("INCLUDE") && !sent.contains("SERVICE"),
            "{sent}"
        );
        assert!(
            sent.contains("{ SELECT ?item WHERE { ?item wdt:P31 wd:Q5 } }"),
            "{sent}"
        );
        // Not rewritten for an endpoint that is not opted in.
        assert_eq!(sent_query(query, false, &["ITEMLABEL"]).await, query);
    }

    #[tokio::test]
    async fn test_queries_are_sent_unchanged_without_blazegraph_rewrite() {
        // WDQS v1 and any endpoint not opted in get the query verbatim.
        assert_eq!(sent_label_query(false, &["ITEMLABEL"]).await, LABEL_QUERY);
    }

    #[tokio::test]
    async fn test_unused_label_service_is_removed_for_rewriting_endpoint() {
        let sent = sent_label_query(true, &[]).await;
        assert_eq!(
            sent,
            "SELECT ?item ?itemLabel WHERE { ?item wdt:P31 wd:Q5 .  }"
        );
    }

    #[tokio::test]
    async fn test_used_label_is_rewritten_for_rewriting_endpoint() {
        // Column names are matched case-insensitively, as everywhere else.
        let sent = sent_label_query(true, &["ITEMLABEL"]).await;
        assert!(!sent.contains("SERVICE"), "{sent}");
        assert!(sent.contains("AS ?itemLabel)"), "{sent}");
    }

    #[tokio::test]
    async fn test_malformed_query_is_reported_and_not_retried() {
        let server = MockServer::start().await;
        let attempts = 5; // Enough to open the breaker if each counted.
        mount(
            &server,
            ResponseTemplate::new(400).set_body_string(QLEVER_SYNTAX_ERROR),
            attempts,
        )
        .await;
        let page_params = page_params_with(cached_config_clone().await).await;

        for _ in 0..attempts {
            let mut sparql_results = SparqlResults::new(page_params.clone(), "wikidata")
                .with_endpoint(mock_endpoint(&server));
            let err = sparql_results
                .run_query(QUERY.to_string())
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("Prefix wdt was not registered"),
                "{err}"
            );
        }
        let url = mock_endpoint(&server).url().to_string();
        assert!(!page_params.config().sparql_circuit_breaker(&url).is_open());
    }

    #[tokio::test]
    async fn test_oversized_result_is_refused_and_not_retried() {
        // A runaway query can make WDQS stream hundreds of megabytes; reading
        // stops at the limit, and asking again would only repeat that.
        let server = MockServer::start().await;
        let attempts = 5; // Enough to open the breaker if each counted.
        let padded = format!("{QLEVER_RESULT}{}", " ".repeat(1024 * 1024));
        mount(
            &server,
            ResponseTemplate::new(200).set_body_raw(padded, "application/sparql-results+json"),
            attempts,
        )
        .await;
        let config = cached_config_clone().await.with_max_sparql_response_mb(1);
        let page_params = page_params_with(config).await;

        for _ in 0..attempts {
            let mut sparql_results = SparqlResults::new(page_params.clone(), "wikidata")
                .with_endpoint(mock_endpoint(&server));
            let err = sparql_results
                .run_query(QUERY.to_string())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("larger than"), "{err}");
        }
        let url = mock_endpoint(&server).url().to_string();
        assert!(!page_params.config().sparql_circuit_breaker(&url).is_open());
    }

    #[tokio::test]
    async fn test_server_error_is_retried() {
        let server = MockServer::start().await;
        mount(
            &server,
            ResponseTemplate::new(503).set_body_string("Service Unavailable"),
            u64::from(SPARQL_MAX_ATTEMPTS),
        )
        .await;
        let page_params = page_params_with(cached_config_clone().await).await;
        let mut sparql_results =
            SparqlResults::new(page_params, "wikidata").with_endpoint(mock_endpoint(&server));

        assert!(sparql_results.run_query(QUERY.to_string()).await.is_err());
    }
}
