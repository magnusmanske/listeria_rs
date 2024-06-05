use anyhow::{anyhow, Result};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use wikimisc::{
    mediawiki::api::Api,
    sparql_results::{SparqlApiResult, SparqlResultRows},
};

use crate::page_params::PageParams;

lazy_static! {
    static ref SPARQL_REQUEST_COUNTER: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
}

#[derive(Debug, Clone)]
pub struct SparqlResults {
    page_params: Arc<PageParams>,
    sparql_main_variable: Option<String>,
    wikibase_key: String,
    pub simulate: bool,
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

    pub async fn run_query(&mut self, mut sparql: String) -> Result<SparqlResultRows> {
        self.expand_sparql_templates(&mut sparql)
            .await
            .map_err(|e| anyhow!("{e}"))?;

        // Return simulated results
        if self.simulate {
            match self.page_params.simulated_sparql_results() {
                Some(json_text) => {
                    let results: SparqlApiResult = serde_json::from_str(json_text)?;
                    self.set_main_variable(&results);
                    return Ok(results.bindings().to_owned());
                }
                None => {}
            }
        }
        self.run_sparql_query(&sparql).await
    }

    async fn run_sparql_query(&mut self, sparql: &str) -> Result<SparqlResultRows> {
        let wikibase_key = &self.wikibase_key;
        let api = match self.page_params.config().get_wbapi(wikibase_key) {
            Some(api) => api.clone(),
            None => return Err(anyhow!("No wikibase setup configured for '{wikibase_key}'")),
        };
        loop {
            if *SPARQL_REQUEST_COUNTER
                .lock()
                .expect("ListeriaList: Mutex is bad")
                < self.page_params.config().max_sparql_simultaneous()
            {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        *SPARQL_REQUEST_COUNTER
            .lock()
            .expect("ListeriaList: Mutex is bad") += 1;

        let result = self.run_sparql_query_stream(&api, sparql).await;
        *SPARQL_REQUEST_COUNTER
            .lock()
            .expect("ListeriaList: Mutex is bad") -= 1;
        result
    }

    async fn run_sparql_query_stream(
        &mut self,
        wb_api_sparql: &Api,
        sparql: &str,
    ) -> Result<SparqlResultRows> {
        // TODO:
        //     let max_sparql_attempts = self.page_params.config().max_sparql_attempts();
        let query_api_url = self.get_sparql_endpoint(wb_api_sparql);
        let params = [("query", sparql), ("format", "json")];
        let response = wb_api_sparql
            .client()
            .post(&query_api_url)
            .header(reqwest::header::USER_AGENT, "ListeriaBot/0.1.2 (https://listeria.toolforge.org/; magnusmanske@googlemail.com) reqwest/0.11.23")
            .form(&params)
            .send()
            .await?;
        let result = response.json::<SparqlApiResult>().await?;
        self.set_main_variable(&result);
        Ok(result.bindings().to_owned())
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
