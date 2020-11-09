extern crate config;
extern crate serde_json;

use std::collections::HashMap;
use tokio::sync::RwLock;
//use tokio_threadpool::ThreadPool;
//use futures::future::{Future, lazy};
use futures::future::*;
use std::sync::Arc;
use listeria::listeria_page::ListeriaPage;
use listeria::configuration::Configuration;
use wikibase::mediawiki::api::Api;
use mysql_async::prelude::*;
use mysql_async::from_row;
use mysql_async as my;
use serde_json::Value;

// ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N

#[derive(Debug, Clone, Default)]
struct PageToProcess {
    id: u64,
    title: String,
    status: String,
    wiki: String,
}

#[derive(Debug, Clone)]
pub struct ListeriaBotWiki {
    wiki: String,
    api: Arc<RwLock<Api>>,
    config: Arc<Configuration>,
}

impl ListeriaBotWiki {
    pub fn new(wiki:&str,api:Arc<RwLock<Api>>,config:Arc<Configuration>) -> Self {
        println!("Creating bot for {}",wiki);
        Self {
            wiki:wiki.to_string(),
            api,
            config
        }
    }

    pub async fn process_page(&self, page:&str) -> Result<String,String> {
        let mut listeria_page = match ListeriaPage::new(self.config.clone(), self.api.clone(), page.to_owned()).await {
            Ok(p) => p,
            Err(e) => return Err(format!("Could not open/parse page '{}': {}", page,e)),
        };
        match listeria_page.run().await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        //let renderer = RendererWikitext::new();
        //let old_wikitext = listeria_page.load_page_as("wikitext").await.expect("FAILED load page as wikitext");
        //let new_wikitext = renderer.get_new_wikitext(&old_wikitext,&listeria_page).unwrap().unwrap();
        //println!("{:?}",&new_wikitext);
        match listeria_page.update_source_page().await? {
            true => {
                println!("{} on {} edited",page,self.wiki);
                //panic!("TEST");
            }
            false => println!("{} on {} not edited",page,self.wiki),
        }
        Ok("OK".to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ListeriaBot {
    config: Arc<Configuration>,
    wiki_apis: HashMap<String,Arc<RwLock<Api>>>,
    pool: mysql_async::Pool,
    next_page_cache: Vec<PageToProcess>,
    site_matrix: Value,
    bot_per_wiki: HashMap<String,ListeriaBotWiki>,
    //thread_pool:Arc<ThreadPool>,
}

impl ListeriaBot {
    pub async fn new(config_file: &str) -> Result<Self,String> {
        let config = Configuration::new_from_file(config_file).await?;

        let host = config.mysql("host").as_str().ok_or("No host in config")?.to_string();
        let schema = config.mysql("schema").as_str().ok_or("No schema in config")?.to_string();
        let port = config.mysql("port").as_u64().ok_or("No port in config")? as u16;
        let user = config.mysql("user").as_str().ok_or("No user in config")?.to_string();
        let password = config.mysql("password").as_str().ok_or("No password in config")?.to_string();

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

        let site_matrix = api.get_query_api_json(&params).await.expect("Can't run action=sitematrix on Wikidata API");
        let mut ret = Self {
            config: Arc::new(config),
            wiki_apis: HashMap::new(),
            pool: mysql_async::Pool::new(opts),
            next_page_cache: vec![],
            site_matrix,
            bot_per_wiki: HashMap::new(),
            //thread_pool: Arc::new(ThreadPool::new()),
        };

        ret.update_bots().await?;
        Ok(ret)
    }

    async fn update_bots(&mut self) -> Result<(),String> {
        let mut conn = self.pool.get_conn().await.expect("Can't connect to database");
        let sql = "SELECT DISTINCT wikis.name AS wiki FROM wikis".to_string() ;
        let wikis = conn.exec_iter(
            sql.as_str(),
            ()
        ).await
        .map_err(|e|format!("PageList::update_bots: SQL query error[1]: {:?}",e))?
        .map_and_drop(|row| { from_row::<String>(row) } )
        .await
        .map_err(|e|format!("PageList::update_bots: SQL query error[2]: {:?}",e))?;
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;

        let _new_wikis : Vec<String> = wikis.iter().filter(|wiki|!self.bot_per_wiki.contains_key(*wiki)).cloned().collect();
        let new_wikis = vec!["dewiki".to_string(),"enwiki".to_string()]; // TESTING FIXME

        for wiki in new_wikis {
            let mw_api = self.get_or_create_wiki_api(&wiki).await?;
            let bot = ListeriaBotWiki::new(&wiki,mw_api,self.config.clone());
            self.bot_per_wiki.insert(wiki,bot);
        }
        Ok(())
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
            .expect("AppState::get_server_url_for_wiki: sitematrix not an object")
            .iter()
            .filter_map(|(id, data)| match id.as_str() {
                "count" => None,
                "specials" => data
                    .as_array()
                    .expect("AppState::get_server_url_for_wiki: 'specials' is not an array")
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

    pub async fn process_next_page(&mut self) -> Result<(),String> {
        // Get next page to update, for all wikis
        let wikis = self.bot_per_wiki.keys() ;
        let mut wiki2page = HashMap::new();
        let mut conn = self.pool.get_conn().await.expect("Can't connect to database");
        for wiki in wikis {
            let sql = format!("SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki FROM pagestatus,wikis WHERE pagestatus.wiki=wikis.id AND wikis.status='ACTIVE' AND pagestatus.status!='RUNNING' AND wikis.name='{}' order by pagestatus.timestamp DESC LIMIT 1",wiki);
            let pages = conn.exec_iter(
                sql.as_str(),
                ()
            ).await
            .map_err(|e|format!("PageList::run_batch_query: SQL query error[1]: {:?}",e))?
            .map_and_drop(|row| {
                let parts = from_row::<(u64,String,String,String)>(row);
                PageToProcess { id:parts.0, title:parts.1, status:parts.2, wiki:parts.3 }
            } )
            .await
            .map_err(|e|format!("PageList::run_batch_query: SQL query error[2]: {:?}",e))?;
            match pages.get(0) {
                Some(page_to_process) => {wiki2page.insert(wiki,page_to_process.title.to_owned());}
                None => {continue;}
            }
        }
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;
        println!("{:?}",wiki2page);

        let mut futures = Vec::new();
        for (wiki,bot) in self.bot_per_wiki.iter() {
            let page = match wiki2page.get(wiki) {
                Some(page) => {page},
                None => {continue;},
            };
            let future = bot.process_page(page);
            futures.push(future);
        }
        let results = join_all(futures).await;
        println!("{:?}",&results);
        /*
        let page = self.get_next_page_to_process().await?;
        println!("Processing {} : {}",&page.wiki,&page.title);

        let mw_api = self.get_or_create_wiki_api(&page.wiki).await?;
        let mut listeria_page = match ListeriaPage::new(self.config.clone(), mw_api, page.title.to_owned()).await {
            Ok(p) => p,
            Err(e) => return Err(format!("Could not open/parse page '{}': {}", &page.title,e)),
        };
        match listeria_page.run().await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        //let renderer = RendererWikitext::new();
        //let old_wikitext = listeria_page.load_page_as("wikitext").await.expect("FAILED load page as wikitext");
        //let new_wikitext = renderer.get_new_wikitext(&old_wikitext,&listeria_page).unwrap().unwrap();
        //println!("{:?}",&new_wikitext);
        match listeria_page.update_source_page().await? {
            true => {
                println!("{} edited",&page.title);
                //panic!("TEST");
            }
            false => println!("{} not edited",&page.title),
        }
        */
        Ok(())
    }

    async fn get_or_create_wiki_api(&mut self, wiki: &str) -> Result<Arc<RwLock<Api>>,String> {
        match &self.wiki_apis.get(wiki) {
            Some(api) => { return Ok((*api).clone()); }
            None => {}
        }

        let api_url = format!("{}/w/api.php",self.get_server_url_for_wiki(wiki)?);
        let mut mw_api = wikibase::mediawiki::api::Api::new(&api_url)
            .await
            .expect("Could not connect to MW API");
        mw_api
            .login(self.config.wiki_user().to_owned(), self.config.wiki_password().to_owned())
            .await
            .expect("Could not log in");
        let mw_api = Arc::new(RwLock::new(mw_api));
        self.wiki_apis.insert(wiki.to_owned(),mw_api);
        
        self.wiki_apis.get(wiki).ok_or(format!("Wiki not found: {}",wiki)).map(|api|api.clone())
    }

    async fn _get_next_page_to_process(&mut self, wiki: Option<String>) -> Result<PageToProcess,String> {
        if !self.next_page_cache.is_empty() {
            let page = self.next_page_cache.remove(0);
            return Ok(page);
        }

        let max_results : u64 = 100 ;
        
        let mut conn = self.pool.get_conn().await.expect("Can't connect to database");
        let sql = match wiki {
            Some(wiki) => format!("SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki FROM pagestatus,wikis WHERE pagestatus.wiki=wikis.id AND wikis.status='ACTIVE' AND pagestatus.status!='RUNNING' AND wiki='{}' order by pagestatus.timestamp DESC LIMIT 1",wiki),
            None => format!("SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki FROM pagestatus,wikis WHERE pagestatus.wiki=wikis.id AND wikis.status='ACTIVE' AND pagestatus.status!='RUNNING' order by pagestatus.timestamp DESC LIMIT {}",max_results),
        };
        println!("{}",&sql);
        self.next_page_cache = conn.exec_iter(
            sql.as_str(),
            ()
        ).await
        .map_err(|e|format!("PageList::run_batch_query: SQL query error[1]: {:?}",e))?
        .map_and_drop(|row| {
            let parts = from_row::<(u64,String,String,String)>(row);
            PageToProcess { id:parts.0, title:parts.1, status:parts.2, wiki:parts.3 }
        } )
        .await
        .map_err(|e|format!("PageList::run_batch_query: SQL query error[2]: {:?}",e))?;
        //println!("{:?}",&self.next_page_cache);
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;

        match self.next_page_cache.get(0) {
            Some(_) => {
                Ok(self.next_page_cache.remove(0))
            }
            None => Err("bot next_page_cache is empty in get_next_page_to_process".to_string())
        }
    }

    pub async fn destruct(&mut self) {
        //self.pool.disconnect().await.unwrap(); // TODO
    }

}

#[tokio::main]
async fn main() {
    let mut bot = ListeriaBot::new("config.json").await.unwrap();
    //loop {
        match bot.process_next_page().await {
            Ok(()) => {}
            Err(e) => { println!("{}",&e); }
        }
    //}
    /*
    let mut mw_api = wikibase::mediawiki::api::Api::new(api_url)
        .await
        .expect("Could not connect to MW API");
    let mw_api = Arc::new(RwLock::new(mw_api));
    */

}
