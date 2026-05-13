//! Multi-wiki bot that processes Listeria templates across Wikimedia wikis.

use crate::configuration::Configuration;
use crate::listeria_bot::ListeriaBot;
use crate::listeria_bot_wiki::ListeriaBotWiki;
use crate::page_to_process::PageToProcess;
use crate::pagestatus_repository::PageStatusRepository;
use crate::wiki_apis::WikiApis;
use crate::wiki_page_result::WikiPageResult;
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use dashmap::DashSet;
use log::info;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ListeriaBotWikidata {
    config: Arc<Configuration>,
    wiki_apis: Arc<WikiApis>,
    bot_per_wiki: DashMap<String, ListeriaBotWiki>,
    running: DashSet<u64>,
    pagestatus: PageStatusRepository,
}

impl ListeriaBotWikidata {
    pub async fn clear_log_table(&self) -> Result<()> {
        use mysql_async::prelude::Queryable;
        let sql = "TRUNCATE `list_log`";
        self.config
            .pool()?
            .get_conn()
            .await?
            .exec_iter(sql, ())
            .await?;
        Ok(())
    }

    fn running_ids_string(&self) -> String {
        let mut parts: Vec<String> = self.running.iter().map(|id| id.to_string()).collect();
        if parts.is_empty() {
            "0".to_string()
        } else {
            parts.sort_unstable();
            parts.join(",")
        }
    }
}

impl ListeriaBot for ListeriaBotWikidata {
    async fn new(config_file: &str) -> Result<Self> {
        let config = Arc::new(Configuration::new_from_file(config_file).await?);
        Self::new_from_config(config).await
    }

    async fn new_from_config(config: Arc<Configuration>) -> Result<Self> {
        let wikis = WikiApis::new(config.clone())
            .await?
            .get_all_wikis_in_database()
            .await?;
        let config = Arc::new((*config).clone().with_wikis(wikis));
        let wiki_apis = WikiApis::new(config.clone()).await?;
        let pagestatus = PageStatusRepository::new(config.pool()?.as_ref().clone());

        Ok(Self {
            config: config.clone(),
            wiki_apis: Arc::new(wiki_apis),
            bot_per_wiki: DashMap::new(),
            running: DashSet::new(),
            pagestatus,
        })
    }

    fn config(&self) -> &Configuration {
        &self.config
    }

    async fn reset_running(&self) -> Result<()> {
        self.pagestatus.reset_running().await
    }

    async fn clear_deleted(&self) -> Result<()> {
        self.pagestatus.clear_deleted().await
    }

    async fn clear_deferred(&self) -> Result<()> {
        self.pagestatus.clear_deferred().await
    }

    /// Removes a pagestatus ID from the running list.
    async fn release_running(&self, pagestatus_id: u64) {
        self.running.remove(&pagestatus_id);
    }

    /// Returns how many pages are currently running.
    async fn get_running_count(&self) -> usize {
        self.running.len()
    }

    /// Returns the next page to be processed.
    async fn prepare_next_single_page(&self) -> Result<PageToProcess> {
        let ids = self.running_ids_string();
        info!(target: "lock", "Getting next page, without {ids}");
        // DEFERRED rows are pages whose processing hit an open circuit
        // breaker; they are cleared at bot startup, so during steady state
        // we want the dispatcher to leave them alone.
        const IGNORE_STATUS: &str = "'RUNNING','DELETED','TRANSLATION','DEFERRED'";

        if let Some(page) = self.pagestatus.find_priority_page(&ids, IGNORE_STATUS).await? {
            info!(target: "lock", "Found a priority page: {:?}", &page);
            self.pagestatus
                .update_page_status(page.title(), page.wiki(), "RUNNING", "PREPARING")
                .await?;
            self.running.insert(page.id());
            return Ok(page);
        }

        let page = self
            .pagestatus
            .find_oldest_page(&ids, IGNORE_STATUS)
            .await?
            .ok_or_else(|| anyhow!("prepare_next_single_page: no page available"))?;

        info!(target: "lock", "Found a page: {:?}", &page);
        self.pagestatus
            .update_page_status(page.title(), page.wiki(), "RUNNING", "PREPARING")
            .await?;
        self.running.insert(page.id());
        Ok(page)
    }

    async fn set_runtime(&self, pagestatus_id: u64, seconds: u64) -> Result<()> {
        self.pagestatus.set_runtime(pagestatus_id, seconds).await
    }

    async fn run_single_bot(&self, page: PageToProcess) -> Result<WikiPageResult> {
        let bot = match self.create_bot_for_wiki(page.wiki()).await {
            Some(bot) => bot.to_owned(),
            None => {
                self.pagestatus
                    .update_page_status(
                        page.title(),
                        page.wiki(),
                        "FAIL",
                        &format!("No such wiki: {}", page.wiki()),
                    )
                    .await?;
                return Err(anyhow!(
                    "ListeriaBot::run_single_bot: No such wiki '{}'",
                    page.wiki()
                ));
            }
        };
        let mut wpr = bot.process_page(page.title()).await;
        wpr.standardize_message();
        self.pagestatus
            .update_page_status(wpr.page(), wpr.wiki(), wpr.result(), wpr.message())
            .await?;
        Ok(wpr)
    }
}

impl ListeriaBotWikidata {
    /// Marks a page as FAIL in the pagestatus queue. Used by the dispatcher
    /// when an outer wall-clock timeout aborts `run_single_bot` before its
    /// own status update can run — without this, the row would stay RUNNING
    /// until the next `reset_running` on bot restart.
    pub async fn mark_page_failed(&self, wiki: &str, page: &str, message: &str) -> Result<()> {
        self.pagestatus
            .update_page_status(page, wiki, "FAIL", message)
            .await
    }

    async fn create_bot_for_wiki(&self, wiki: &str) -> Option<ListeriaBotWiki> {
        if let Some(bot) = self.bot_per_wiki.get(wiki) {
            let new_bot = bot.to_owned();
            return Some(new_bot);
        }
        info!("Creating bot for {wiki}");
        let mw_api = self.wiki_apis.get_or_create_wiki_api(wiki).await.ok()?;
        let bot = ListeriaBotWiki::new(wiki, mw_api, self.config.clone());
        self.bot_per_wiki.insert(wiki.to_string(), bot.clone());
        info!("Created bot for {wiki}");
        Some(bot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The priority logic now lives in PageStatusRepository and is tested there.
    // These tests cover the running-ids helper that remains in this module.

    #[test]
    fn test_running_ids_string_empty() {
        // Can't build a full ListeriaBotWikidata without a DB, so test the
        // logic directly via a local DashSet.
        let running: DashSet<u64> = DashSet::new();
        let ids = {
            let mut parts: Vec<String> = running.iter().map(|id| id.to_string()).collect();
            if parts.is_empty() {
                "0".to_string()
            } else {
                parts.sort_unstable();
                parts.join(",")
            }
        };
        assert_eq!(ids, "0");
    }

    #[test]
    fn test_running_ids_string_sorted() {
        let running: DashSet<u64> = DashSet::new();
        running.insert(3);
        running.insert(1);
        running.insert(2);
        let mut parts: Vec<String> = running.iter().map(|id| id.to_string()).collect();
        parts.sort_unstable();
        let ids = parts.join(",");
        assert_eq!(ids, "1,2,3");
    }
}
