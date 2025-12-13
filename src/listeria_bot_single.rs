use crate::configuration::Configuration;
use crate::listeria_bot::ListeriaBot;
use crate::listeria_bot_wiki::ListeriaBotWiki;
use crate::page_to_process::PageToProcess;
use crate::wiki_page_result::WikiPageResult;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ListeriaBotSingle {
    config: Arc<Configuration>,
    ticontinue: Arc<Mutex<Option<String>>>,
    page_cache: Arc<Mutex<Vec<PageToProcess>>>,
    template_inclusion_end_reached: Arc<Mutex<bool>>,
    the_bot: Arc<Mutex<Option<Arc<ListeriaBotWiki>>>>,
    running: usize,
}

impl ListeriaBot for ListeriaBotSingle {
    async fn new(config_file: &str) -> Result<Self> {
        let config = Arc::new(Configuration::new_from_file(config_file).await?);
        Self::new_from_config(config).await
    }

    async fn new_from_config(config: Arc<Configuration>) -> Result<Self> {
        Ok(Self {
            config,
            ticontinue: Arc::new(Mutex::new(None)),
            page_cache: Arc::new(Mutex::new(Vec::new())),
            template_inclusion_end_reached: Arc::new(Mutex::new(false)),
            the_bot: Arc::new(Mutex::new(None)),
            running: 0,
        })
    }
    fn config(&self) -> &Configuration {
        &self.config
    }
    async fn reset_running(&self) -> Result<()> {
        // No need
        Ok(())
    }
    async fn clear_deleted(&self) -> Result<()> {
        // No need
        Ok(())
    }
    async fn set_runtime(&self, _pagestatus_id: u64, _seconds: u64) -> Result<()> {
        // No need
        Ok(())
    }
    async fn run_single_bot(&self, page: PageToProcess) -> Result<WikiPageResult> {
        let bot = match self.create_bot_for_wiki(page.wiki()).await {
            Some(bot) => bot.to_owned(),
            None => {
                return Err(anyhow!(
                    "ListeriaBot::run_single_bot: No such wiki '{}'",
                    page.wiki()
                ));
            }
        };
        if !self.config().quiet() {
            println!("Running bot on {}", page.title());
        }
        let mut wpr = bot.process_page(page.title()).await;
        wpr.standardize_meassage();
        Ok(wpr)
    }

    /// Removed a pagestatus ID from the running list
    async fn release_running(&self, _pagestatus_id: u64) {
        // No need
    }

    /// Returns how many pages are running
    async fn get_running_count(&self) -> usize {
        self.running
    }

    /// Returns a page to be processed.
    async fn prepare_next_single_page(&self) -> Result<PageToProcess> {
        if self.page_cache_is_empty().await {
            if *self.template_inclusion_end_reached.lock().await {
                // Throw an error but only once, so the caller knows the list has been processed
                *self.template_inclusion_end_reached.lock().await = false;
                return Err(anyhow!("End of template inclusion reached"));
            }
            self.load_more_pages().await?;
        }
        let ret = match self.page_cache.lock().await.pop() {
            Some(page) => page,
            None => return Err(anyhow!("No pages available")),
        };
        Ok(ret)
    }
}

impl ListeriaBotSingle {
    async fn create_bot_for_wiki(&self, wiki: &str) -> Option<Arc<ListeriaBotWiki>> {
        let mut the_bot = self.the_bot.lock().await;
        if the_bot.is_some() {
            return the_bot.clone();
        }
        let mw_api = self.config.get_default_wbapi().ok()?;
        let bot = ListeriaBotWiki::new(wiki, mw_api.clone(), self.config.clone());
        let bot = Arc::new(bot);
        *the_bot = Some(bot.clone());
        Some(bot)
    }

    async fn page_cache_is_empty(&self) -> bool {
        self.page_cache.lock().await.is_empty()
    }

    fn get_start_template(&self) -> Result<String> {
        Ok(self
            .config
            .get_local_template_title_start("wiki")?
            .to_string())
    }
    async fn load_more_pages(&self) -> Result<()> {
        let api = self.config.get_default_wbapi()?;
        let mut params: HashMap<String, String> = [
            ("action", "query"),
            ("prop", "transcludedin"),
            ("tishow", "!redirect"),
            ("tilimit", "500"),
            (
                "titles",
                &format!("Template:{}", self.get_start_template()?),
            ),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let mut ticontinue_lock = self.ticontinue.lock().await;
        if let Some(ticontinue) = &*ticontinue_lock {
            params.insert("ticontinue".to_string(), ticontinue.to_string());
        }
        let result = api.get_query_api_json(&params).await?;
        let templates = result["query"]["pages"]
            .as_object()
            .ok_or(anyhow!("'query.pages' not an object in API response"))?;
        for (_template_id, template) in templates {
            let pages = template["transcludedin"]
                .as_array()
                .ok_or(anyhow!("'transcludedin' not an array in API response"))?;
            let pages: Vec<PageToProcess> = pages
                .iter()
                .filter_map(|page| {
                    let parts = (
                        page["pageid"].as_u64()?,
                        page["title"].as_str()?.to_string(),
                        "dummy".to_string(),
                        "wiki".to_string(),
                    );
                    Some(PageToProcess::from_parts(parts))
                })
                .collect();
            *(self.page_cache.lock().await) = pages;
        }
        if let Some(s) = result["continue"]["ticontinue"].as_str() {
            *ticontinue_lock = Some(s.to_string());
        } else {
            *ticontinue_lock = None;
            *self.template_inclusion_end_reached.lock().await = true;
        }
        Ok(())
    }
}
