use tokio::sync::Mutex;
use chrono::{DateTime, Utc};
use futures::future::*;
use crate::configuration::Configuration;
use crate::listeria_page::ListeriaPage;
use mysql_async as my;
use mysql_async::from_row;
use mysql_async::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use wikibase::mediawiki::api::Api;

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
}

#[derive(Debug, Clone)]
pub struct ListeriaBotWiki {
    wiki: String,
    api: Arc<RwLock<Api>>,
    config: Arc<Configuration>,
}

impl ListeriaBotWiki {
    pub fn new(wiki: &str, api: Arc<RwLock<Api>>, config: Arc<Configuration>) -> Self {
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
        match listeria_page.run().await {
            Ok(_) => {}
            Err(e) => return WikiPageResult::new(&self.wiki, page, "FAIL", e),
        }
        let _did_edit = match listeria_page.update_source_page().await {
            Ok(x) => x,
            Err(e) => return WikiPageResult::new(&self.wiki, page, "FAIL", e),
        };
        WikiPageResult::new(&self.wiki, page, "OK", "".to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ListeriaBot {
    config: Arc<Configuration>,
    wiki_apis: Arc<Mutex<HashMap<String, Arc<RwLock<Api>>>>>,
    pool: mysql_async::Pool,
    _next_page_cache: Vec<PageToProcess>,
    site_matrix: Value,
    bot_per_wiki: Arc<Mutex<HashMap<String, ListeriaBotWiki>>>,
    ignore_wikis: Vec<String>,
}

impl ListeriaBot {
    pub async fn new(config_file: &str) -> Result<Self, String> {
        let config = Configuration::new_from_file(config_file).await?;

        let host = config
            .mysql("host")
            .as_str()
            .ok_or("No host in config")?
            .to_string();
        let schema = config
            .mysql("schema")
            .as_str()
            .ok_or("No schema in config")?
            .to_string();
        let port = config.mysql("port").as_u64().ok_or("No port in config")? as u16;
        let user = config
            .mysql("user")
            .as_str()
            .ok_or("No user in config")?
            .to_string();
        let password = config
            .mysql("password")
            .as_str()
            .ok_or("No password in config")?
            .to_string();

        let opts = my::OptsBuilder::default()
            .ip_or_hostname(host.to_owned())
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(password))
            .tcp_port(port);

        // Load site matrix
        let api = config.get_default_wbapi()?;
        let params: HashMap<String, String> = vec![("action", "sitematrix")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let site_matrix = api
            .get_query_api_json(&params)
            .await
            .map_err(|e| e.to_string())?;
        let mut ret = Self {
            config: Arc::new(config),
            wiki_apis: Arc::new(Mutex::new(HashMap::new())),
            pool: mysql_async::Pool::new(opts),
            _next_page_cache: vec![],
            site_matrix,
            bot_per_wiki: Arc::new(Mutex::new(HashMap::new())),
            ignore_wikis: Vec::new(),
        };

        ret.update_bots().await?;
        Ok(ret)
    }

    async fn update_bots(&mut self) -> Result<(), String> {
        let mut conn = self.pool.get_conn().await.map_err(|e| e.to_string())?;
        let sql = "SELECT DISTINCT wikis.name AS wiki FROM wikis".to_string();
        let wikis = conn
            .exec_iter(sql.as_str(), ())
            .await
            .map_err(|e| format!("PageList::update_bots: SQL query error[1]: {:?}", e))?
            .map_and_drop(from_row::<String>)
            .await
            .map_err(|e| format!("PageList::update_bots: SQL query error[2]: {:?}", e))?;
        conn.disconnect().await.map_err(|e| format!("{:?}", e))?;

        let existing_wikis: Vec<String> = self.bot_per_wiki.lock().await.keys().cloned().collect();
        let new_wikis: Vec<String> = wikis
            .iter()
            .filter(|wiki| !existing_wikis.contains(*wiki))
            .filter(|wiki| !self.ignore_wikis.contains(*wiki))
            .cloned()
            .collect();
        //let new_wikis = vec!["enwiki".to_string(),"dewiki".to_string()]; // TESTING

        let login_in_parallel = false;
        if login_in_parallel {
            //This does not work, probably a MW issue
            let mut futures = Vec::new();
            for wiki in &new_wikis {
                let future = self.create_wiki_api(wiki);
                futures.push(future);
            }
            let results = join_all(futures).await;

            for num in 0..results.len() {
                let wiki = &new_wikis[num];
                match &results[num] {
                    Ok(mw_api) => {
                        self.wiki_apis.lock().await.insert(wiki.to_owned(), mw_api.clone());
                    }
                    Err(e) => {
                        println!("Can't login to {}: {}", wiki, e);
                        self.ignore_wikis.push(wiki.to_owned());
                    }
                }
            }
        }

        /*
        for wiki in &new_wikis {
            let _ = self.create_bot_for_wiki(wiki).await;
        } */
        Ok(())
    }

    async fn create_bot_for_wiki(&self, wiki: &str) -> Option<ListeriaBotWiki> {
        if let Some(bot) = self.bot_per_wiki.lock().await.get(wiki) {
            return Some(bot.to_owned())
        }
        let mw_api = match self.get_or_create_wiki_api(&wiki).await {
            Ok(mw_api) => mw_api,
            Err(e) => {
                eprintln!("{}",e);
                return None;
            }
        };

        let mut bpw = self.bot_per_wiki.lock().await; // Lock during creation, as to not create multiple

        // Check again
        if let Some(bot) = bpw.get(wiki) {
            return Some(bot.to_owned())
        }

        let bot = ListeriaBotWiki::new(&wiki, mw_api, self.config.clone());
        bpw.insert(wiki.to_string(), bot.clone());
        return Some(bot);
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

    fn get_server_url_for_wiki(&self, wiki: &str) -> Result<String, String> {
        match wiki.replace("_", "-").as_str() {
            "be-taraskwiki" | "be-x-oldwiki" => {
                return Ok("https://be-tarask.wikipedia.org".to_string())
            }
            _ => {}
        }
        self.site_matrix["sitematrix"]
            .as_object()
            .ok_or_else(|| {
                "ListeriaBot::get_server_url_for_wiki: sitematrix not an object".to_string()
            })?
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
            .ok_or(format!(
                "AppState::get_server_url_for_wiki: Cannot find server for wiki '{}'",
                &wiki
            ))
    }

    pub async fn reset_running(&self) -> Result<(),String> {
        let sql = "UPDATE pagestatus SET status='OK' WHERE status='RUNNING'";
        let _ = self.pool.get_conn().await.map_err(|e| e.to_string())?.exec_iter(sql, ()).await;
        Ok(())
    }
  
    /// Returns a page to be processed. 
    pub async fn prepare_next_single_page(&self) -> Result<PageToProcess, String> {
        // Gets the first 1000 pages (by timestamp), then randomly picks one
        let sql = r#"SELECT * FROM (
            SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki 
            FROM pagestatus,wikis 
            WHERE pagestatus.wiki=wikis.id AND wikis.status='ACTIVE' AND pagestatus.status!='RUNNING' 
            ORDER BY pagestatus.timestamp
            LIMIT 1000) ps
            ORDER BY rand()
            LIMIT 1"#;
        let mut conn = self.pool.get_conn().await.map_err(|e| e.to_string())?;
        let page = conn
            .exec_iter(sql, ())
            .await
            .map_err(|e| format!("ListeriaBot::prepare_next_single_page: SQL query error[1]: {:?}",e))?
            .map_and_drop(|row| PageToProcess::from_row(row))
            .await
            .map_err(|e| format!("ListeriaBot::prepare_next_single_page: SQL query error[2]: {:?}",e))?
            .pop()
            .ok_or(format!("!!"))?;
        self.update_page_status(&mut conn,&page.title,&page.wiki,"RUNNING","PREPARING").await?;
        Ok(page)
    }

    pub async fn run_single_bot(&self, page: PageToProcess ) -> Result<(), String> {
        let bot = match self.create_bot_for_wiki(&page.wiki).await {
            Some(bot) => bot.to_owned(),
            None => return Err(format!("ListeriaBot::run_single_bot: No such wiki '{}'",page.wiki))
        };
        let wpr = bot.process_page(&page.title).await;
        let mut conn = self.pool.get_conn().await.map_err(|e| e.to_string())?;
        self.update_page_status(&mut conn, &wpr.page, &wpr.wiki, &wpr.result, &wpr.message).await?;
        Ok(())
    }

    pub async fn process_next_page(&self) -> Result<(), String> {
        // Get next page to update, for all wikis
        let wikis: Vec<String> = self
            .bot_per_wiki
            .lock().await
            .iter()
            .map(|(wiki, _bot)| wiki.to_string())
            .collect();
        let mut wiki2page = HashMap::new();
        let mut conn = self.pool.get_conn().await.map_err(|e| e.to_string())?;
        for wiki in wikis {
            let sql = format!("SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki FROM pagestatus,wikis WHERE pagestatus.wiki=wikis.id AND wikis.status='ACTIVE' AND pagestatus.status!='RUNNING' AND wikis.name='{}' order by pagestatus.timestamp LIMIT 1",wiki);
            let pages = conn
                .exec_iter(sql.as_str(), ())
                .await
                .map_err(|e| {
                    format!(
                        "ListeriaBot::process_next_page: SQL query error[1]: {:?}",
                        e
                    )
                })?
                .map_and_drop(|row| {
                    let parts = from_row::<(u64, String, String, String)>(row);
                    PageToProcess {
                        id: parts.0,
                        title: parts.1,
                        status: parts.2,
                        wiki: parts.3,
                    }
                })
                .await
                .map_err(|e| {
                    format!(
                        "ListeriaBot::process_next_page: SQL query error[2]: {:?}",
                        e
                    )
                })?;
            match pages.get(0) {
                Some(page_to_process) => {
                    wiki2page.insert(wiki, page_to_process.title.to_owned());
                }
                None => {
                    continue;
                }
            }
        }

        // Update status to RUNNING
        let mut running = Vec::new();
        for wiki in self.bot_per_wiki.lock().await.keys() {
            let page = match wiki2page.get(wiki) {
                Some(page) => page,
                None => {
                    continue;
                }
            };
            running.push((wiki.to_owned(), page.to_owned()));
        }
        for (wiki, page) in running {
            self.update_page_status(
                &mut conn,
                &page,
                &wiki,
                "RUNNING",
                "",
            )
            .await?; // TODO
        }
        conn.disconnect().await.map_err(|e| format!("{:?}", e))?;
/*
        let mut futures = Vec::new();
        for (wiki, bot) in &self.bot_per_wiki {
            let page = match wiki2page.get(wiki) {
                Some(page) => page,
                None => {
                    continue;
                }
            };
            let future = bot.process_page(&page);
            //let bot = bot.clone();
            //let future = tokio::spawn(async move { bot.process_page(&page).await});
            futures.push(future);
        }

        let results = join_all(futures).await;
        let mut conn = self.pool.get_conn().await.map_err(|e| e.to_string())?;
        for wpr in &results {
            self.update_page_status(&mut conn, &wpr.page, &wpr.wiki, &wpr.result, &wpr.message)
                    .await?;
        }
        conn.disconnect().await.map_err(|e| format!("{:?}", e))?; */
        Ok(())
    }

    async fn update_page_status(
        &self,
        conn: &mut mysql_async::Conn,
        page: &str,
        wiki: &str,
        status: &str,
        message: &str,
    ) -> Result<(), String> {
        let now: DateTime<Utc> = Utc::now();
        let timestamp = now.format("%Y%m%d%H%M%S").to_string();
        println!("{wiki}:{page} : {status}");
        let params = params! {
            "wiki" => wiki,
            "page" => page,
            "timestamp" => timestamp,
            "status" => status,
            "message" => message, //format!("V2:{}",&message),
        };
        let sql = "UPDATE `pagestatus` SET `status`=:status,`message`=:message,`timestamp`=:timestamp,`bot_version`=2 WHERE `wiki`=(SELECT id FROM `wikis` WHERE `name`=:wiki) AND `page`=:page".to_string() ;
        conn.exec_iter(sql.as_str(), params)
            .await
            .map_err(|e| {
                format!(
                    "ListeriaBot::update_page_status: SQL query error[1]: {:?}",
                    e
                )
            })?
            .map_and_drop(|row| from_row::<String>(row))
            .await
            .map_err(|e| {
                format!(
                    "ListeriaBot::update_page_status: SQL query error[2]: {:?}",
                    e
                )
            })?;
        Ok(())
    }

    async fn create_wiki_api(&self, wiki: &str) -> Result<Arc<RwLock<Api>>, String> {
        let api_url = format!("{}/w/api.php", self.get_server_url_for_wiki(wiki)?);
        let mut mw_api = wikibase::mediawiki::api::Api::new(&api_url)
            .await
            .map_err(|e| e.to_string())?;
        mw_api
            .login(
                self.config.wiki_user().to_owned(),
                self.config.wiki_password().to_owned(),
            )
            .await
            .map_err(|e| format!("{}: {}",wiki,e))?;
        let mw_api = Arc::new(RwLock::new(mw_api));
        Ok(mw_api)
    }

    async fn get_or_create_wiki_api(&self, wiki: &str) -> Result<Arc<RwLock<Api>>, String> {
        match &self.wiki_apis.lock().await.get(wiki) {
            Some(api) => {
                return Ok((*api).clone());
            }
            None => {}
        }

        let mw_api = self.create_wiki_api(wiki).await?;
        self.wiki_apis.lock().await.insert(wiki.to_owned(), mw_api);

        self.wiki_apis
            .lock().await
            .get(wiki)
            .ok_or(format!("Wiki not found: {}", wiki))
            .map(|api| api.clone())
    }

    pub async fn destruct(&mut self) {
        //self.pool.disconnect().await.unwrap(); // TODO
    }
}
