//! SPARQL query execution with retry logic and rate limiting.

use crate::page_params::PageParams;
use anyhow::{Result, anyhow};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tokio::sync::Semaphore;
use wikimisc::{
    mediawiki::api::Api, sparql_results::SparqlApiResult, sparql_table_vec::SparqlTableVec,
};

/// Limits the number of concurrent SPARQL requests.
/// Initialised on first use from `Configuration::max_sparql_simultaneous`.
static SPARQL_REQUEST_SEMAPHORE: OnceLock<Semaphore> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct SparqlResults {
    page_params: Arc<PageParams>,
    sparql_main_variable: Option<String>,
    wikibase_key: String,
    query_endpoint: Option<String>, // For single wiki mode, the SPARQL endpoint
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
            query_endpoint: None,
            simulate,
        }
    }

    #[must_use]
    pub fn with_query_endpoint(mut self, query_endpoint: String) -> Self {
        self.query_endpoint = Some(query_endpoint);
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
        self.set_main_variable(&result);
        let mut table = SparqlTableVec::from_api_result(result)?;
        table.set_main_variable(self.sparql_main_variable());
        Ok(Some(table))
    }

    async fn run_sparql_query(&mut self, sparql: &str) -> Result<SparqlTableVec> {
        let wikibase_key = &self.wikibase_key;
        let api = match self.page_params.config().get_wbapi(wikibase_key) {
            Some(api) => api.clone(),
            None => return Err(anyhow!("No wikibase setup configured for '{wikibase_key}'")),
        };
        let max = self.page_params.config().max_sparql_simultaneous() as usize;
        let semaphore = SPARQL_REQUEST_SEMAPHORE.get_or_init(|| Semaphore::new(max));
        let _permit = semaphore.acquire().await?;
        self.run_sparql_query_stream(&api, sparql).await
    }

    async fn run_sparql_query_stream(
        &mut self,
        wb_api_sparql: &Api,
        sparql: &str,
    ) -> Result<SparqlTableVec> {
        let query_api_url = self.get_sparql_endpoint(wb_api_sparql);
        let sparql = match self.page_params.config().sparql_prefix() {
            Some(prefix) => format!("{}\n{}", prefix, sparql),
            None => sparql.to_string(),
        };
        let params = [("query", sparql.as_str()), ("format", "json")];
        let timeout = self.page_params.config().api_timeout();
        let response = wb_api_sparql
            .client()
            .post(&query_api_url)
            .header(reqwest::header::USER_AGENT, crate::LISTERIA_USER_AGENT)
            .timeout(timeout)
            .form(&params)
            .send()
            .await?;
        let result = response.json::<SparqlApiResult>().await?;
        self.set_main_variable(&result);

        let mut ret = SparqlTableVec::from_api_result(result)?;
        ret.set_main_variable(self.sparql_main_variable());
        Ok(ret)
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
        let j = api.get_query_api_json(&params).await?;
        if let Some(s) = j["expandtemplates"]["wikitext"].as_str() {
            *sparql = s.to_string();
        }
        Ok(())
    }

    fn get_sparql_endpoint(&self, wb_api_sparql: &Api) -> String {
        if let Some(endpoint) = &self.query_endpoint {
            return endpoint.to_owned();
        }
        wb_api_sparql
            .get_site_info_string("general", "wikibase-sparql")
            .unwrap_or("https://wcqs-beta.wmflabs.org/sparql")
            .to_string()
    }

    #[must_use]
    pub fn sparql_main_variable(&self) -> Option<String> {
        self.sparql_main_variable.to_owned()
    }
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
        use crate::configuration::Configuration;
        use crate::page_params::PageParams;
        use std::sync::Arc;
        use wikimisc::mediawiki::api::Api;

        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        let api = Arc::new(api);
        let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());
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
        use crate::configuration::Configuration;
        use crate::page_params::PageParams;
        use std::sync::Arc;
        use wikimisc::mediawiki::api::Api;

        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        let api = Arc::new(api);
        let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());
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
}
