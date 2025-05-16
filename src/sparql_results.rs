use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Semaphore;
use wikimisc::{mediawiki::api::Api, sparql_results::SparqlApiResult, sparql_table::SparqlTable};

use crate::page_params::PageParams;

lazy_static! {
    static ref sparql_request_semaphore: Semaphore = Semaphore::new(3);
    // TODO set from self.page_params.config().max_sparql_simultaneous()
}

#[derive(Debug, Clone)]
pub struct SparqlResults {
    page_params: Arc<PageParams>,
    sparql_main_variable: Option<String>,
    wikibase_key: String,
    simulate: bool,
}

impl SparqlResults {
    pub fn new(page_params: Arc<PageParams>, wikibase_key: &str) -> Self {
        let simulate = page_params.simulate();
        Self {
            page_params,
            wikibase_key: wikibase_key.to_string(),
            sparql_main_variable: None,
            simulate,
        }
    }

    pub fn set_simulate(&mut self, simulate: bool) {
        self.simulate = simulate;
    }

    pub async fn run_query(&mut self, mut sparql: String) -> Result<SparqlTable> {
        self.expand_sparql_templates(&mut sparql)
            .await
            .map_err(|e| anyhow!("{e}"))?;

        // Return simulated results
        if self.simulate {
            if let Some(json_text) = self.page_params.simulated_sparql_results() {
                let result: SparqlApiResult = serde_json::from_str(json_text)?;
                self.set_main_variable(&result);

                let mut ret = SparqlTable::from_api_result(result)?;
                ret.set_main_variable(self.sparql_main_variable());
                return Ok(ret);
            }
        }
        self.run_sparql_query(&sparql).await
    }

    async fn run_sparql_query(&mut self, sparql: &str) -> Result<SparqlTable> {
        let wikibase_key = &self.wikibase_key;
        let api = match self.page_params.config().get_wbapi(wikibase_key) {
            Some(api) => api.clone(),
            None => return Err(anyhow!("No wikibase setup configured for '{wikibase_key}'")),
        };
        let _permit = sparql_request_semaphore.acquire().await?;
        self.run_sparql_query_stream(&api, sparql).await
    }

    async fn run_sparql_query_stream(
        &mut self,
        wb_api_sparql: &Api,
        sparql: &str,
    ) -> Result<SparqlTable> {
        let query_api_url = self.get_sparql_endpoint(wb_api_sparql);
        let params = [("query", sparql), ("format", "json")];
        let response = wb_api_sparql
            .client()
            .post(&query_api_url)
            .header(reqwest::header::USER_AGENT, crate::LISTERIA_USER_AGENT)
            .form(&params)
            .send()
            .await?;
        // TODO .timeout(self.config.api_timeout())

        let result = response.json::<SparqlApiResult>().await?;
        self.set_main_variable(&result);

        let mut ret = SparqlTable::from_api_result(result)?;
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
        match wb_api_sparql.get_site_info_string("general", "wikibase-sparql") {
            Ok(endpoint) => {
                // SPARQL service given by site
                endpoint
            }
            _ => {
                // Override SPARQL service (hardcoded for Commons)
                "https://wcqs-beta.wmflabs.org/sparql"
            }
        }
        .to_string()
    }

    pub fn sparql_main_variable(&self) -> Option<String> {
        self.sparql_main_variable.to_owned()
    }
}
