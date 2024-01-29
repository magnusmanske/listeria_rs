use tokio::sync::Mutex;
use chrono::{DateTime, Utc};
use crate::configuration::Configuration;
use crate::database_pool::DatabasePool;
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



#[derive(Debug, Clone)]
struct ListeriaBotWiki {
    wiki: String,
    api: ApiLock,
    wiki_apis: Arc<WikiApis>,
}

impl ListeriaBotWiki {
    pub fn new(wiki: &str, api: ApiLock, wiki_apis: Arc<WikiApis>) -> Self {
        println!("Creating bot for {}", wiki);
        Self {
            wiki: wiki.to_string(),
            api,
            wiki_apis,
        }
    }

    pub async fn process_page(&self, page: &str) -> WikiPageResult {
        let mut listeria_page =
            match ListeriaPage::new(self.wiki_apis.clone(), self.api.clone(), page.to_owned()).await {
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
    wiki_apis: Arc<WikiApis>,
    pool: DatabasePool,
    bot_per_wiki: Arc<Mutex<HashMap<String, ListeriaBotWiki>>>,
    running: Arc<Mutex<HashSet<u64>>>,
}

impl ListeriaBot {
    pub async fn new(config_file: &str) -> Result<Self> {
        let config = Configuration::new_from_file(config_file).await?;
        let pool = DatabasePool::new(&config)?;
        Ok(Self {
            wiki_apis: Arc::new(WikiApis::new(config).await?),
            pool,
            bot_per_wiki: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(HashSet::default())),
        })
    }


    async fn create_bot_for_wiki(&self, wiki: &str) -> Option<ListeriaBotWiki> {
        let mut lock = self.bot_per_wiki.lock().await;
        if let Some(bot) = lock.get(wiki) {
            return Some(bot.to_owned())
        }
        let mw_api = match self.wiki_apis.get_or_create_wiki_api(&wiki).await {
            Ok(mw_api) => mw_api,
            Err(e) => {
                eprintln!("{e}");
                return None;
            }
        };

        let bot = ListeriaBotWiki::new(&wiki, mw_api, self.wiki_apis.clone());
        lock.insert(wiki.to_string(), bot.clone());
        return Some(bot);
    }

    pub async fn reset_running(&self) -> Result<()> {
        let sql = "UPDATE pagestatus SET status='OK' WHERE status='RUNNING'";
        let _ = self.pool.get_conn().await?.exec_iter(sql, ()).await;
        Ok(())
    }

    async fn get_page_for_sql(&self, sql: &str) -> Option<PageToProcess> {
        self.pool.get_conn().await.ok()?
            .exec_iter(sql, ())
            .await.ok()?
            .map_and_drop(|row| PageToProcess::from_row(row))
            .await.ok()?
            .pop()
    }

    /// Removed a pagestatus ID from the running list
    pub async fn release_running(&self, pagestatus_id: u64) {
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
        
        // Tries to find a "priority" page
        let sql = format!("SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki 
            FROM pagestatus,wikis 
            WHERE priority=1
            AND wikis.id=pagestatus.wiki
            AND pagestatus.id NOT IN ({ids})
            ORDER BY rand()
            LIMIT 1");
        if let Some(page) = self.get_page_for_sql(&sql).await {
            running.insert(page.id);
            self.update_page_status(&page.title,&page.wiki,"RUNNING","PREPARING").await?;
            return Ok(page)
        }

        // Get the oldest page
        let sql = format!("
            SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki 
            FROM pagestatus,wikis 
            WHERE pagestatus.wiki=wikis.id
            AND wikis.status='ACTIVE' 
            AND pagestatus.status NOT IN ('RUNNING','DELETED')
            AND pagestatus.id NOT IN ({ids})
            ORDER BY pagestatus.timestamp
            LIMIT 1");
        let page = self.get_page_for_sql(&sql).await.ok_or(anyhow!("prepare_next_single_page:: no pop"))?;
        running.insert(page.id);
        self.update_page_status(&page.title,&page.wiki,"RUNNING","PREPARING").await?;
        Ok(page)
    }

    pub async fn run_single_bot(&self, page: PageToProcess ) -> Result<()> {
        let bot = match self.create_bot_for_wiki(&page.wiki).await {
            Some(bot) => bot.to_owned(),
            None => {
                self.update_page_status( &page.title, &page.wiki, "FAIL", &format!("No such wiki: {}",&page.wiki)).await?;
                return Err(anyhow!("ListeriaBot::run_single_bot: No such wiki '{}'",page.wiki))
            }
        };
        let wpr = bot.process_page(&page.title).await;
        self.update_page_status(&wpr.page, &wpr.wiki, &wpr.result, &wpr.message).await?;
        if wpr.message.contains("Connection reset by peer (os error 104)") {
            self.reset_wiki(&page.wiki).await;
        }
        Ok(())
    }

    async fn reset_wiki(&self, wiki: &str) {
        let _ = self.bot_per_wiki.lock().await.remove(wiki);
        std::process::exit(0); // Seems that os error 104 is a system wide thing with Wikimedia, best to quit the app and restart
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
        self.pool.get_conn().await?
            .exec_iter(sql.as_str(), params)
            .await?
            .map_and_drop(|row| from_row::<String>(row))
            .await?;
        Ok(())
    }
}
