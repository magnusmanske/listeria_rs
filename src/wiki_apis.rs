use tokio::sync::Mutex;
use crate::configuration::Configuration;
use crate::site_matrix::SiteMatrix;
use crate::ApiLock;
use anyhow::{Result,anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use wikibase::mediawiki::api::Api;

const API_TIMEOUT: Duration = Duration::from_secs(360);
const MS_DELAY_AFTER_EDIT: u64 = 100;


#[derive(Debug, Clone)]
pub struct WikiApis {
    config: Arc<Configuration>,
    site_matrix: Arc<SiteMatrix>,
    apis: Arc<Mutex<HashMap<String, ApiLock>>>,
}

impl WikiApis {
    pub async fn new(config: Arc<Configuration>) -> Result<Self> {
        let site_matrix = Arc::new(SiteMatrix::new(&config).await?);
        Ok(Self {
            apis: Arc::new(Mutex::new(HashMap::new())),
            config,
            site_matrix,
        })
    }

    pub async fn get_or_create_wiki_api(&self, wiki: &str) -> Result<ApiLock> {
        let mut lock = self.apis.lock().await;
        if let Some(api) = &lock.get(wiki) {
            return Ok((*api).clone());
        }

        let mw_api = self.create_wiki_api(wiki).await?;
        lock.insert(wiki.to_owned(), mw_api);

        lock
            .get(wiki)
            .ok_or(anyhow!("Wiki not found: {wiki}"))
            .map(|api| api.clone())
    }

    async fn create_wiki_api(&self, wiki: &str) -> Result<ApiLock> {
        let api_url = format!("{}/w/api.php", self.site_matrix.get_server_url_for_wiki(wiki)?);
        let builder = wikibase::mediawiki::reqwest::Client::builder().timeout(API_TIMEOUT);
        let mut mw_api = Api::new_from_builder(&api_url, builder).await?;
        mw_api.set_oauth2(self.config.oauth2_token());
        mw_api.set_edit_delay(Some(MS_DELAY_AFTER_EDIT)); // Slow down editing a bit
        let mw_api = Arc::new(RwLock::new(mw_api));
        Ok(mw_api)
    }

}