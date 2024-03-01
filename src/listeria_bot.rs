use tokio::sync::Mutex;
use chrono::{DateTime, Utc};
use crate::configuration::Configuration;
use crate::listeria_page::ListeriaPage;
use crate::page_to_process::PageToProcess;
use crate::wiki_apis::WikiApis;
use crate::wiki_page_result::WikiPageResult;
use crate::ApiLock;
use anyhow::{Result,anyhow};
use mysql_async::from_row;
use mysql_async::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use log::info;



#[derive(Debug, Clone)]
struct ListeriaBotWiki {
    wiki: String,
    api: ApiLock,
    config: Arc<Configuration>,
}

impl ListeriaBotWiki {
    pub fn new(wiki: &str, api: ApiLock, config: Arc<Configuration>) -> Self {
        println!("Creating bot for {}", wiki);
        Self {
            wiki: wiki.to_string(),
            api,
            config,
        }
    }

    pub async fn process_page(&self, page: &str) -> WikiPageResult {
        let mut listeria_page =
            match ListeriaPage::new(self.config.clone(), self.api.clone(), page.to_owned()).await {
                Ok(p) => p,
                Err(e) => {
                    return WikiPageResult::new(
                        &self.wiki,
                        page,
                        "FAIL",
                        format!("Could not open/parse page '{}': {}", page, e),
                    )
                }
            };
        if let Err(wpr) = listeria_page.run().await {
            return wpr
        }
        let _did_edit = match listeria_page.update_source_page().await {
            Ok(x) => x,
            Err(wpr) => return wpr,
        };
        WikiPageResult::new(&self.wiki, page, "OK", "".to_string())
    }
}



#[derive(Debug, Clone)]
pub struct ListeriaBot {
    config: Arc<Configuration>,
    wiki_apis: Arc<WikiApis>,
    bot_per_wiki: Arc<Mutex<HashMap<String, ListeriaBotWiki>>>,
    running: Arc<Mutex<HashSet<u64>>>,
}

impl ListeriaBot {
    pub async fn new(config_file: &str) -> Result<Self> {
        let config = Arc::new(Configuration::new_from_file(config_file).await?);
        Ok(Self {
            config: config.clone(),
            wiki_apis: Arc::new(WikiApis::new(config.clone()).await?),
            bot_per_wiki: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(HashSet::default())),
        })
    }

    pub fn config(&self) -> &Configuration {
        &self.config
    }

    async fn create_bot_for_wiki(&self, wiki: &str) -> Option<ListeriaBotWiki> {
        let mut lock = self.bot_per_wiki.lock().await;
        if let Some(bot) = lock.get(wiki) {
            return Some(bot.to_owned())
        }
        info!("Creating bot for {wiki}");
        let mw_api = match self.wiki_apis.get_or_create_wiki_api(&wiki).await {
            Ok(mw_api) => mw_api,
            Err(e) => {
                eprintln!("{e}");
                return None;
            }
        };

        let bot = ListeriaBotWiki::new(&wiki, mw_api, self.config.clone());
        lock.insert(wiki.to_string(), bot.clone());
        info!("Created bot for {wiki}");
        return Some(bot);
    }

    pub async fn reset_running(&self) -> Result<()> {
        let sql = "UPDATE pagestatus SET status='OK' WHERE status='RUNNING'";
        let _ = self.config.pool().get_conn().await?.exec_iter(sql, ()).await;
        Ok(())
    }

    async fn get_page_for_sql(&self, sql: &str) -> Option<PageToProcess> {
        self.config.pool().get_conn().await.ok()?
            .exec_iter(sql, ())
            .await.ok()?
            .map_and_drop(|row| PageToProcess::from_row(row))
            .await.ok()?
            .pop()
    }

    /// Removed a pagestatus ID from the running list
    pub async fn release_running(&self, pagestatus_id: u64) {
        println!("Releasing {pagestatus_id}");
        self.running.lock().await.remove(&pagestatus_id);
    }

    /// Returns how many pages are running
    pub async fn get_running_count(&self) -> usize {
        self.running.lock().await.len()
    }

    /// Returns a page to be processed. 
    pub async fn prepare_next_single_page(&self) -> Result<PageToProcess> {
        let mut running = self.running.lock().await;
        let ids: String = running.iter().map(|id|format!("{id}")).collect::<Vec<String>>().join(",");
        let ids = if ids.is_empty() { "0".to_string() } else { ids } ;
        info!(target: "lock","Getting next page, without {ids}");
        let ignore_status = "'RUNNING','DELETED','TRANSLATION'";
        
        // Tries to find a "priority" page
        let sql = format!("SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki 
            FROM pagestatus,wikis 
            WHERE priority=1
            AND wikis.id=pagestatus.wiki
            AND wikis.status='ACTIVE' 
            AND pagestatus.status NOT IN ({ignore_status})
            AND pagestatus.id NOT IN ({ids})
            ORDER BY rand()
            LIMIT 1");
        if let Some(page) = self.get_page_for_sql(&sql).await {
            self.update_page_status(&page.title,&page.wiki,"RUNNING","PREPARING").await?;
            info!(target: "lock","Found a priority page: {:?}",&page);
            running.insert(page.id);
            return Ok(page)
        }

        // Get the oldest page
        let sql = format!("
            SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki 
            FROM pagestatus,wikis 
            WHERE pagestatus.wiki=wikis.id
            AND wikis.status='ACTIVE' 
            AND pagestatus.status NOT IN ({ignore_status})
            AND pagestatus.id NOT IN ({ids})
            ORDER BY pagestatus.timestamp
            LIMIT 1");
        let page = self.get_page_for_sql(&sql).await
            .ok_or(anyhow!("prepare_next_single_page:: no pop\n{sql}\n{ids}"))?;
        info!(target: "lock","Found a page: {:?}",&page);
        self.update_page_status(&page.title,&page.wiki,"RUNNING","PREPARING").await?;
        running.insert(page.id);
        Ok(page)
    }

    pub async fn set_runtime(&self, pagestatus_id: u64,seconds: u64) -> Result<()> {
        let sql = "UPDATE `pagestatus` SET `last_runtime_sec`=? WHERE `id`=?";
        self.config.pool().get_conn().await?.exec_drop(sql,(seconds, pagestatus_id,)).await?;
        Ok(())
    }

    pub async fn run_single_bot(&self, page: PageToProcess ) -> Result<()> {
        let bot = match self.create_bot_for_wiki(&page.wiki).await {
            Some(bot) => bot.to_owned(),
            None => {
                self.update_page_status( &page.title, &page.wiki, "FAIL", &format!("No such wiki: {}",&page.wiki)).await?;
                return Err(anyhow!("ListeriaBot::run_single_bot: No such wiki '{}'",page.wiki))
            }
        };
        let mut wpr = bot.process_page(&page.title).await;
        if wpr.message.contains("This page is a translation of the page") {
            wpr.result = "TRANSLATION".into();
            wpr.message = "This page is a translation".into();
        }
        if wpr.message.contains("Connection reset by peer (os error 104)") {
            wpr.message = "104_RESET_BY_PEER".into();
        }
        if wpr.message.contains("api.php): operation timed out") {
            wpr.message = "WIKI_TIMEOUT".into();
        }
        if wpr.message.contains("expected value at line 1 column 1: SPARQL-QUERY:") {
            wpr.message = "SPARQL_ERROR".into();
        }
        self.update_page_status(&wpr.page, &wpr.wiki, &wpr.result, &wpr.message).await?;
        if wpr.message.contains("104_RESET_BY_PEER") {
            self.reset_wiki(&page.wiki).await;
        }
        Ok(())
    }

    async fn reset_wiki(&self, wiki: &str) {
        let _ = self.bot_per_wiki.lock().await.remove(wiki);
        // std::process::exit(0); // Seems that os error 104 is a system wide thing with Wikimedia, best to quit the app and restart
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
        println!("{timestamp} {wiki}:{page} : {status}: {message}");
        let params = params! {
            "wiki" => wiki,
            "page" => page,
            "timestamp" => timestamp,
            "status" => status,
            "message" => message.chars().take(200).collect::<String>(),
        };
        let priority = if status=="OK"||status=="FAILED" { "0" } else { "`priority`" }; // Reset priority on OK or FAIL
        let sql = format!("UPDATE `pagestatus` SET
            `status`=:status,
            `message`=:message,
            `timestamp`=:timestamp,
            `bot_version`=2,
            `priority`={priority}
            WHERE `wiki`=(SELECT id FROM `wikis` WHERE `name`=:wiki) AND `page`=:page") ;
        self.config.pool().get_conn().await?
            .exec_iter(sql.as_str(), params)
            .await?
            .map_and_drop(|row| from_row::<String>(row))
            .await?;
        Ok(())
    }
}
