use crate::configuration::Configuration;
use crate::listeria_bot::ListeriaBot;
use crate::listeria_bot_wiki::ListeriaBotWiki;
use crate::page_to_process::PageToProcess;
use crate::wiki_apis::WikiApis;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use dashmap::DashSet;
use log::info;
use mysql_async::from_row;
use mysql_async::prelude::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ListeriaBotWikidata {
    config: Arc<Configuration>,
    wiki_apis: Arc<WikiApis>,
    bot_per_wiki: DashMap<String, ListeriaBotWiki>,
    running: DashSet<u64>,
}

impl ListeriaBot for ListeriaBotWikidata {
    async fn new(config_file: &str) -> Result<Self> {
        let config = Arc::new(Configuration::new_from_file(config_file).await?);
        let wiki_apis = WikiApis::new(config.clone()).await?;
        let wikis = wiki_apis.get_all_wikis_in_database().await?;

        // HACKISH BUT WORKS
        let mut config = Configuration::new_from_file(config_file).await?;
        config.set_wikis(wikis);
        let config = Arc::new(config);
        let wiki_apis = WikiApis::new(config.clone()).await?;

        Ok(Self {
            config: config.clone(),
            wiki_apis: Arc::new(wiki_apis),
            bot_per_wiki: DashMap::new(),
            running: DashSet::new(),
        })
    }

    fn config(&self) -> &Configuration {
        &self.config
    }

    async fn reset_running(&self) -> Result<()> {
        let sql = "UPDATE pagestatus SET status='PAUSED' WHERE status='RUNNING'";
        self.run_sql(sql).await
    }

    async fn clear_deleted(&self) -> Result<()> {
        let sql = "DELETE FROM `pagestatus` WHERE `status`='DELETED'";
        self.run_sql(sql).await
    }

    /// Removed a pagestatus ID from the running list
    async fn release_running(&self, pagestatus_id: u64) {
        // println!("Releasing {pagestatus_id}");
        Self::print_sysinfo();
        self.running.remove(&pagestatus_id);
    }

    /// Returns how many pages are running
    async fn get_running_count(&self) -> usize {
        self.running.len()
    }

    /// Returns a page to be processed.
    async fn prepare_next_single_page(&self) -> Result<PageToProcess> {
        let ids: String = self
            .running
            .iter()
            .map(|id| format!("{}", *id))
            .collect::<Vec<String>>()
            .join(",");
        let ids = if ids.is_empty() { "0".to_string() } else { ids };
        info!(target: "lock","Getting next page, without {ids}");
        const IGNORE_STATUS: &str = "'RUNNING','DELETED','TRANSLATION'";

        // Tries to find a "priority" page
        let sql = format!(
            "SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki
            FROM pagestatus,wikis
            WHERE priority=1
            AND wikis.id=pagestatus.wiki
            AND wikis.status='ACTIVE'
            AND pagestatus.status NOT IN ({IGNORE_STATUS})
            AND pagestatus.id NOT IN ({ids})
            ORDER BY pagestatus.timestamp
            LIMIT 1"
        );
        if let Some(page) = self.get_page_for_sql(&sql).await {
            self.update_page_status(page.title(), page.wiki(), "RUNNING", "PREPARING")
                .await?;
            info!(target: "lock","Found a priority page: {:?}",&page);
            self.running.insert(page.id());
            return Ok(page);
        }

        // Get the oldest page
        let sql = format!(
            "SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki
            FROM pagestatus,wikis
            WHERE pagestatus.wiki=wikis.id
            AND wikis.status='ACTIVE'
            AND pagestatus.status NOT IN ({IGNORE_STATUS})
            AND pagestatus.id NOT IN ({ids})
            ORDER BY pagestatus.timestamp
            LIMIT 1"
        );
        let page = self
            .get_page_for_sql(&sql)
            .await
            .ok_or(anyhow!("prepare_next_single_page:: no pop\n{sql}\n{ids}"))?;
        info!(target: "lock","Found a page: {:?}",&page);
        self.update_page_status(page.title(), page.wiki(), "RUNNING", "PREPARING")
            .await?;
        self.running.insert(page.id());
        Ok(page)
    }

    async fn set_runtime(&self, pagestatus_id: u64, seconds: u64) -> Result<()> {
        let sql = "UPDATE `pagestatus` SET `last_runtime_sec`=:seconds WHERE `id`=:pagestatus_id";
        self.config
            .pool()
            .get_conn()
            .await?
            .exec_drop(sql, params! {seconds, pagestatus_id})
            .await?;
        Ok(())
    }

    async fn run_single_bot(&self, page: PageToProcess) -> Result<()> {
        let bot = match self.create_bot_for_wiki(page.wiki()).await {
            Some(bot) => bot.to_owned(),
            None => {
                self.update_page_status(
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
        wpr.standardize_meassage();
        self.update_page_status(wpr.page(), wpr.wiki(), wpr.result(), wpr.message())
            .await?;
        Ok(())
    }
}

impl ListeriaBotWikidata {
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

    async fn run_sql(&self, sql: &str) -> Result<()> {
        let _ = self
            .config
            .pool()
            .get_conn()
            .await?
            .exec_iter(sql, ())
            .await;
        Ok(())
    }

    async fn get_page_for_sql(&self, sql: &str) -> Option<PageToProcess> {
        self.config
            .pool()
            .get_conn()
            .await
            .ok()?
            .exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(PageToProcess::from_row)
            .await
            .ok()?
            .pop()
    }

    async fn update_page_status(
        &self,
        page: &str,
        wiki: &str,
        status: &str,
        message: &str,
    ) -> Result<()> {
        let now: DateTime<Utc> = Utc::now();
        let timestamp = now.format("%Y%m%d%H%M%S").to_string();
        // println!("{timestamp} {wiki}:{page} : {status}: {message}");
        let params = params! {
            "wiki" => wiki,
            "page" => page,
            "timestamp" => timestamp,
            "status" => status,
            "message" => message.chars().take(200).collect::<String>(),
        };
        let priority = match status {
            // Reset priority on OK or FAIL
            "OK" | "FAIL" => "0",
            _ => "`priority`",
        };
        let sql = format!(
            "UPDATE `pagestatus` SET
            `status`=:status,
            `message`=:message,
            `timestamp`=:timestamp,
            `bot_version`=2,
            `priority`={priority}
            WHERE `wiki`=(SELECT id FROM `wikis` WHERE `name`=:wiki) AND `page`=:page"
        );
        self.config
            .pool()
            .get_conn()
            .await?
            .exec_iter(sql.as_str(), params)
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(())
    }
}
