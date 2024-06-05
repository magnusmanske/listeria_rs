use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use wikimisc::{mediawiki::api::Api, sparql_value::SparqlValue};

use crate::page_params::PageParams;

pub type SparqlResultRow = HashMap<String, SparqlValue>;
pub type SparqlResultRows = Vec<SparqlResultRow>;

lazy_static! {
    static ref SPARQL_REQUEST_COUNTER: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
}

#[derive(Debug, Clone, Deserialize)]
struct SparqlApiResults {
    bindings: SparqlResultRows,
}

#[derive(Debug, Clone, Deserialize)]
struct SparqlApiResult {
    head: HashMap<String, Vec<String>>,
    results: SparqlApiResults,
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
                    return Ok(results.results.bindings);
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
        Ok(result.results.bindings)
    }

    fn set_main_variable(&mut self, j: &SparqlApiResult) {
        self.sparql_main_variable = None;
        if let Some(arr) = j.head.get("vars") {
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

    // fn parse_sparql(&mut self, j: Value) -> Result<SparqlResultRows> {
    //     let mut sparql_rows = vec![];
    //     sparql_rows.clear();
    //     self.sparql_main_variable = None;

    //     if let Some(arr) = j["head"]["vars"].as_array() {
    //         // Insist on ?item
    //         let required_variable_name = "item";
    //         for v in arr {
    //             if Some(required_variable_name) == v.as_str() {
    //                 self.sparql_main_variable = Some(required_variable_name.to_string());
    //                 break;
    //             }
    //         }
    //     }

    //     let bindings = j["results"]["bindings"]
    //         .as_array()
    //         .ok_or(anyhow!("Broken SPARQL results.bindings"))?;
    //     for b in bindings.iter() {
    //         let mut row: HashMap<String, SparqlValue> = HashMap::new();
    //         if let Some(bo) = b.as_object() {
    //             for (k, v) in bo.iter() {
    //                 match SparqlValue::new_from_json(v) {
    //                     Some(v2) => row.insert(k.to_owned(), v2),
    //                     None => {
    //                         return Err(anyhow!("Can't parse SPARQL value: {} => {:?}", &k, &v))
    //                     }
    //                 };
    //             }
    //         }
    //         if row.is_empty() {
    //             continue;
    //         }
    //         sparql_rows.push(row);
    //     }
    //     Ok(sparql_rows)
    // }

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

    // async fn run_sparql_query_api(&self, wb_api_sparql: &Api, sparql: &str) -> Result<Value> {
    //     // SPARQL might need some retries sometimes, bad server or somesuch
    //     let mut sparql = sparql.to_string();
    //     let max_sparql_attempts = self.page_params.config().max_sparql_attempts();
    //     let mut attempts_left = max_sparql_attempts;
    //     let endpoint = self.get_sparql_endpoint(wb_api_sparql);
    //     loop {
    //         let ret = wb_api_sparql
    //             .sparql_query_endpoint(&sparql, &endpoint)
    //             .await;
    //         match ret {
    //             Ok(ret) => return Ok(ret),
    //             Err(e) => {
    //                 match &e {
    //                     MediaWikiError::String(s) => {
    //                         if s.contains("expected value at line 1 column 1: SPARQL-QUERY:") {
    //                             return Err(anyhow!("SPARQL is broken: {s}\n{sparql}"));
    //                         }
    //                         if attempts_left>0 && s.contains("error decoding response body: expected value at line 1 column 1") {
    //                             // sleep(Duration::from_millis(500*(max_sparql_attempts-attempts_left+1))).await;
    //                             tokio::time::sleep(tokio::time::Duration::from_millis(500*(max_sparql_attempts-attempts_left+1))).await;
    //                             sparql += &format!(" /* {attempts_left} */");
    //                             attempts_left -= 1;
    //                             continue;
    //                         }
    //                         if s.contains(
    //                             "error decoding response body: expected value at line 1 column 1",
    //                         ) {
    //                             return Err(anyhow!("SPARQL is probably broken: {s}\n{sparql}"));
    //                         }
    //                         return Err(anyhow!("{e}"));
    //                     }
    //                     e => return Err(anyhow!("{e}")),
    //                 }
    //             }
    //         }
    //     }
    // }

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sparql_results() {
        let j = json!({"head":{"vars":["q","x"]},"results":{"bindings":[{"q":{"type":"uri","value":"http://www.wikidata.org/entity/Q21"},"x":{"type":"literal","value":"GB-ENG"}},{"q":{"type":"uri","value":"http://www.wikidata.org/entity/Q145"},"x":{"type":"literal","value":"GB-UKM"}},{"q":{"type":"uri","value":"http://www.wikidata.org/entity/Q21272276"},"x":{"type":"literal","value":"GB-CAM"}}]}});
        let r = SparqlApiResult::deserialize(j).unwrap();
        assert_eq!(r.head["vars"], vec!["q", "x"]);
        assert_eq!(r.results.bindings.len(), 3);
        assert_eq!(
            r.results.bindings[0]["q"],
            SparqlValue::Entity("Q21".to_string())
        );
        assert_eq!(
            r.results.bindings[0]["x"],
            SparqlValue::Literal("GB-ENG".to_string())
        );
    }
}
