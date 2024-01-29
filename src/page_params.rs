use crate::{configuration::Configuration, wiki_apis::WikiApis, ApiLock};
use anyhow::Result;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PageParams {
    language: String,
    wiki: String,
    page: String,
    mw_api: ApiLock,
    wb_api: ApiLock,
    simulate: bool,
    simulated_text: Option<String>,
    simulated_sparql_results: Option<String>,
    simulated_autodesc: Option<Vec<String>>,
    wiki_apis: Arc<WikiApis>,
    local_file_namespace_prefix: String,
}

impl PageParams {
    pub async fn new(
        wiki_apis: Arc<WikiApis>,
        mw_api: ApiLock,
        page: String,
    ) -> Result<Self> {
        let api = mw_api.read().await;
        let wiki = api.get_site_info_string("general", "wikiid")?.to_string();
        let language = api.get_site_info_string("general", "lang")?.to_string();
        let local_file_namespace_prefix = api.get_local_namespace_name(6).unwrap_or("File").to_string();
        drop(api);

        let wb_api = wiki_apis.get_default_wbapi().await?;
        let ret = Self {
            wiki,
            page,
            language,
            mw_api: mw_api.clone(),
            wb_api,
            simulate: false,
            simulated_text: None,
            simulated_sparql_results: None,
            simulated_autodesc: None,
            wiki_apis,
            local_file_namespace_prefix,
        };
        Ok(ret)
    }

    pub fn local_file_namespace_prefix(&self) -> &String {
        &self.local_file_namespace_prefix
    }

    pub fn simulate(&self) -> bool {
        self.simulate
    }

    pub fn wiki(&self) -> &str {
        &self.wiki
    }

    pub fn page(&self) -> &str {
        &self.page
    }

    pub fn language(&self) -> &str {
        &self.language
    }

    pub fn config(&self) -> &Configuration {
        self.wiki_apis.config()
    }

    pub fn mw_api(&self) -> &ApiLock {
        &self.mw_api
    }

    pub fn wb_api(&self) -> ApiLock {
        self.wb_api.clone()
    }

    pub async fn get_wb_api(&self, wiki: &str) -> Result<ApiLock> {
        self.wiki_apis.get_or_create_wiki_api(wiki).await
    }

    pub fn simulated_text(&self) -> &Option<String> {
        &self.simulated_text
    }

    pub fn simulated_sparql_results(&self) -> &Option<String> {
        &self.simulated_sparql_results
    }

    pub fn simulated_autodesc(&self) -> &Option<Vec<String>> {
        &self.simulated_autodesc
    }

    pub fn set_simulation(&mut self, text: Option<String>, sparql_results: Option<String>, autodesc: Option<Vec<String>>) {
        self.simulate = true;
        self.simulated_text = text;
        self.simulated_sparql_results = sparql_results;
        self.simulated_autodesc = autodesc;
    }
}
