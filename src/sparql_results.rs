use anyhow::{anyhow, Result};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use wikimisc::{
    mediawiki::{api::Api, media_wiki_error::MediaWikiError},
    sparql_value::SparqlValue,
};

use crate::page_params::PageParams;

pub type SparqlResultRows = Vec<HashMap<String, SparqlValue>>;

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
                    let j = serde_json::from_str(json_text)?;
                    return self.parse_sparql(j);
                }
                None => {}
            }
        }

        let j = self.run_sparql_query(&sparql).await?;
        self.parse_sparql(j)
    }

    async fn run_sparql_query(&self, sparql: &str) -> Result<Value> {
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
            // sleep(Duration::from_millis(100)).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        *SPARQL_REQUEST_COUNTER
            .lock()
            .expect("ListeriaList: Mutex is bad") += 1;
        let result = self.run_sparql_query_api(&api, sparql).await;
        *SPARQL_REQUEST_COUNTER
            .lock()
            .expect("ListeriaList: Mutex is bad") -= 1;
        result
    }

    fn parse_sparql(&mut self, j: Value) -> Result<SparqlResultRows> {
        let mut sparql_rows = vec![];
        sparql_rows.clear();
        self.sparql_main_variable = None;

        if let Some(arr) = j["head"]["vars"].as_array() {
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
                    match SparqlValue::new_from_json(v) {
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
            sparql_rows.push(row);
        }
        Ok(sparql_rows)
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

    async fn run_sparql_query_api(&self, wb_api_sparql: &Api, sparql: &str) -> Result<Value> {
        // SPARQL might need some retries sometimes, bad server or somesuch
        let mut sparql = sparql.to_string();
        let max_sparql_attempts = self.page_params.config().max_sparql_attempts();
        let mut attempts_left = max_sparql_attempts;
        let endpoint = self.get_sparql_endpoint(wb_api_sparql);
        loop {
            let ret = wb_api_sparql
                .sparql_query_endpoint(&sparql, &endpoint)
                .await;
            match ret {
                Ok(ret) => return Ok(ret),
                Err(e) => {
                    match &e {
                        MediaWikiError::String(s) => {
                            if s.contains("expected value at line 1 column 1: SPARQL-QUERY:") {
                                return Err(anyhow!("SPARQL is broken: {s}\n{sparql}"));
                            }
                            if attempts_left>0 && s.contains("error decoding response body: expected value at line 1 column 1") {
                                // sleep(Duration::from_millis(500*(max_sparql_attempts-attempts_left+1))).await;
                                tokio::time::sleep(tokio::time::Duration::from_millis(500*(max_sparql_attempts-attempts_left+1))).await;
                                sparql += &format!(" /* {attempts_left} */");
                                attempts_left -= 1;
                                continue;
                            }
                            if s.contains(
                                "error decoding response body: expected value at line 1 column 1",
                            ) {
                                return Err(anyhow!("SPARQL is probably broken: {s}\n{sparql}"));
                            }
                            return Err(anyhow!("{e}"));
                        }
                        e => return Err(anyhow!("{e}")),
                    }
                }
            }
        }
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
