use my::OptsBuilder;
use tokio::sync::Mutex;
use chrono::{DateTime, Utc};
use crate::configuration::Configuration;
use crate::listeria_page::ListeriaPage;
use crate::ApiLock;
use anyhow::{Result,anyhow};
use mysql_async as my;
use mysql_async::from_row;
use mysql_async::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use wikibase::mediawiki::api::Api;

const API_TIMEOUT: Duration = Duration::from_secs(360);
const MS_DELAY_AFTER_EDIT: u64 = 100;

#[derive(Debug, Clone, Default)]
pub struct PageToProcess {
    pub id: u64,
    pub title: String,
    pub status: String,
    pub wiki: String,
}

impl PageToProcess {
    pub fn from_parts(parts: (u64,String,String,String)) -> Self {
        Self {
            id: parts.0,
            title: parts.1,
            status: parts.2,
            wiki: parts.3,
        }
    }

    pub fn from_row(row: mysql_async::Row) -> Self {
        let parts = from_row::<(u64, String, String, String)>(row);
        Self::from_parts(parts)
    }
}

#[derive(Debug, Clone)]
pub struct WikiPageResult {
    pub wiki: String,
    pub page: String,
    pub result: String,
    pub message: String,
}

unsafe impl Send for WikiPageResult {}

impl WikiPageResult {
    pub fn new(wiki: &str, page: &str, result: &str, message: String) -> Self {
        Self {
            wiki: wiki.to_string(),
            page: page.to_string(),
            result: result.to_string(),
            message,
        }
    }

    pub fn fail(wiki: &str, page: &str, message: &str) -> Self {
        Self::new(
            wiki,
            page,
            "FAIL",
            message.to_string()
        )
    }
}

#[derive(Debug, Clone)]
pub struct ListeriaBotWiki {
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
pub struct SiteMatrix {
    site_matrix: Value,
}

impl SiteMatrix {
    pub async fn new(config: &Configuration) -> Result<Self> {
        // Load site matrix
        let api = config.get_default_wbapi()?;
        let params: HashMap<String, String> = vec![("action", "sitematrix")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let site_matrix = api
            .get_query_api_json(&params)
            .await?;
        Ok(Self { site_matrix })
    }


    fn get_url_for_wiki_from_site(&self, wiki: &str, site: &Value) -> Option<String> {
        self.get_value_from_site_matrix_entry(wiki, site, "dbname", "url")
    }

    fn get_value_from_site_matrix_entry(
        &self,
        value: &str,
        site: &Value,
        key_match: &str,
        key_return: &str,
    ) -> Option<String> {
        if site["closed"].as_str().is_some() {
            return None;
        }
        if site["private"].as_str().is_some() {
            return None;
        }
        match site[key_match].as_str() {
            Some(site_url) => {
                if value == site_url {
                    match site[key_return].as_str() {
                        Some(url) => Some(url.to_string()),
                        None => None,
                    }
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn get_server_url_for_wiki(&self, wiki: &str) -> Result<String> {
        match wiki.replace("_", "-").as_str() {
            "be-taraskwiki" | "be-x-oldwiki" => {
                return Ok("https://be-tarask.wikipedia.org".to_string())
            }
            "metawiki" => {
                return Ok("https://meta.wikimedia.org".to_string())
            }
            _ => {}
        }
        self.site_matrix["sitematrix"]
            .as_object()
            .ok_or_else(|| anyhow!("ListeriaBot::get_server_url_for_wiki: sitematrix not an object"))?
            .iter()
            .filter_map(|(id, data)| match id.as_str() {
                "count" => None,
                "specials" => data
                    .as_array()?
                    .iter()
                    .filter_map(|site| self.get_url_for_wiki_from_site(wiki, site))
                    .next(),
                _other => match data["site"].as_array() {
                    Some(sites) => sites
                        .iter()
                        .filter_map(|site| self.get_url_for_wiki_from_site(wiki, site))
                        .next(),
                    None => None,
                },
            })
            .next()
            .ok_or(anyhow!("AppState::get_server_url_for_wiki: Cannot find server for wiki '{wiki}'"))
    }

}

#[derive(Debug, Clone)]
pub struct DatabasePool {
    pool: mysql_async::Pool,
}

impl DatabasePool {
    pub fn new(config: &Configuration) -> Result<Self> {
        let opts = Self::pool_opts_from_config(&config)?;
        Ok(Self { pool: mysql_async::Pool::new(opts) } )
    }

    pub async fn get_conn(&self) -> Result<my::Conn> {
        let ret = self.pool.get_conn().await?;
        Ok(ret)
    }

    fn pool_opts_from_config(config: &Configuration) -> Result<OptsBuilder> {
        let host = config
            .mysql("host")
            .as_str()
            .ok_or(anyhow!("No host in config"))?
            .to_string();
        let schema = config
            .mysql("schema")
            .as_str()
            .ok_or(anyhow!("No schema in config"))?
            .to_string();
        let port = config.mysql("port").as_u64().ok_or(anyhow!("No port in config"))? as u16;
        let user = config
            .mysql("user")
            .as_str()
            .ok_or(anyhow!("No user in config"))?
            .to_string();
        let password = config
            .mysql("password")
            .as_str()
            .ok_or(anyhow!("No password in config"))?
            .to_string();

        let opts = my::OptsBuilder::default()
            .ip_or_hostname(host.to_owned())
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(password))
            .tcp_port(port);

        Ok(opts)
    }
}

#[derive(Debug, Clone)]
pub struct ListeriaBot {
    config: Arc<Configuration>,
    wiki_apis: Arc<Mutex<HashMap<String, ApiLock>>>,
    pool: DatabasePool,
    site_matrix: SiteMatrix,
    bot_per_wiki: Arc<Mutex<HashMap<String, ListeriaBotWiki>>>,
}

impl ListeriaBot {
    pub async fn new(config_file: &str) -> Result<Self> {
        let config = Configuration::new_from_file(config_file).await?;
        let pool = DatabasePool::new(&config)?;
        let site_matrix = SiteMatrix::new(&config).await?;
        Ok(Self {
            config: Arc::new(config),
            wiki_apis: Arc::new(Mutex::new(HashMap::new())),
            pool,
            site_matrix,
            bot_per_wiki: Arc::new(Mutex::new(HashMap::new())),
        })
    }


    async fn create_bot_for_wiki(&self, wiki: &str) -> Option<ListeriaBotWiki> {
        let mut lock = self.bot_per_wiki.lock().await;
        if let Some(bot) = lock.get(wiki) {
            return Some(bot.to_owned())
        }
        let mw_api = match self.get_or_create_wiki_api(&wiki).await {
            Ok(mw_api) => mw_api,
            Err(e) => {
                eprintln!("{e}");
                return None;
            }
        };

        let bot = ListeriaBotWiki::new(&wiki, mw_api, self.config.clone());
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
  
    /// Returns a page to be processed. 
    pub async fn prepare_next_single_page(&self) -> Result<PageToProcess> {
        // Tries to find a "priority" page
        let sql = "SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki 
            FROM pagestatus,wikis 
            WHERE priority=1
            ORDER BY pagestatus.timestamp
            LIMIT 1";
        if let Some(page) = self.get_page_for_sql(sql).await {
            self.update_page_status(&page.title,&page.wiki,"RUNNING","PREPARING").await?;
            return Ok(page)
        }

        // Gets the first 100 pages (by timestamp), then randomly picks one
        let sql = r#"SELECT * FROM (
            SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki 
            FROM pagestatus,wikis 
            WHERE pagestatus.wiki=wikis.id AND wikis.status='ACTIVE' AND pagestatus.status NOT IN ('RUNNING','DELETED')
            ORDER BY pagestatus.timestamp
            LIMIT 100) ps
            ORDER BY rand()
            LIMIT 1"#;
        let page = self.get_page_for_sql(sql).await.ok_or(anyhow!("prepare_next_single_page:: no pop"))?;
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

    async fn create_wiki_api(&self, wiki: &str) -> Result<ApiLock> {
        let api_url = format!("{}/w/api.php", self.site_matrix.get_server_url_for_wiki(wiki)?);
        let builder = wikibase::mediawiki::reqwest::Client::builder().timeout(API_TIMEOUT);
        let mut mw_api = Api::new_from_builder(&api_url, builder).await?;
        mw_api.set_oauth2(self.config.oauth2_token());
        mw_api.set_edit_delay(Some(MS_DELAY_AFTER_EDIT)); // Slow down editing a bit
        let mw_api = Arc::new(RwLock::new(mw_api));
        Ok(mw_api)
    }

    async fn get_or_create_wiki_api(&self, wiki: &str) -> Result<ApiLock> {
        let mut lock = self.wiki_apis.lock().await;
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

    pub async fn destruct(&mut self) {
        //self.pool.disconnect().await; // TODO
    }
}
