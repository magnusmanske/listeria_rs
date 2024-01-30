use mysql_async::{from_row, prelude::*, Conn};
use mysql_async::{Opts, OptsBuilder};
use tokio::sync::Mutex;
use wikibase::entity_container::EntityContainer;
use wikibase::mediawiki::title::Title;
use wikibase::EntityTrait;
use crate::configuration::Configuration;
use crate::site_matrix::SiteMatrix;
use crate::database_pool::DatabasePool;
use crate::ApiLock;
use anyhow::{Result,anyhow};
use std::collections::{HashMap, HashSet};
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
    pool: DatabasePool,
}

impl WikiApis {
    pub async fn new(config: Arc<Configuration>) -> Result<Self> {
        let pool = DatabasePool::new(&config)?;
        let site_matrix = Arc::new(SiteMatrix::new(&config).await?);
        Ok(Self {
            apis: Arc::new(Mutex::new(HashMap::new())),
            config,
            site_matrix,
            pool,
        })
    }

    /// Returns a MediaWiki API instance for the given wiki. Creates a new one and caches it, if required.
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

    /// Creates a MediaWiki API instance for the given wiki
    async fn create_wiki_api(&self, wiki: &str) -> Result<ApiLock> {
        let api_url = format!("{}/w/api.php", self.site_matrix.get_server_url_for_wiki(wiki)?);
        Self::create_wiki_api_from_api_url(&api_url, &self.config.oauth2_token()).await
    }

    pub async fn create_wiki_api_from_api_url(api_url: &str, oauth2_token: &str) -> Result<ApiLock> {
        let builder = wikibase::mediawiki::reqwest::Client::builder().timeout(API_TIMEOUT);
        let mut mw_api = Api::new_from_builder(&api_url, builder).await?;
        mw_api.set_oauth2(oauth2_token);
        mw_api.set_edit_delay(Some(MS_DELAY_AFTER_EDIT)); // Slow down editing a bit
        let mw_api = Arc::new(RwLock::new(mw_api));
        Ok(mw_api)
    }


    /// Updates the database to contain all wikis that have a Listeria start template
    pub async fn update_wiki_list_in_database(&self) -> Result<()> {
        let q = self.config.get_template_start_q(); // Wikidata item for {{Wikidata list}}
        let api = self.config.get_default_wbapi()?;
        let entities = EntityContainer::new();
        entities
            .load_entities(&api, &vec![q.to_owned()])
            .await
            .map_err(|e|anyhow!("{e}"))?;
        let entity = entities.get_entity(&q).ok_or_else(||anyhow!("{q} item not found on Wikidata"))?;
        let current_wikis: Vec<String> = entity.sitelinks().iter().flatten().map(|s|s.site().to_owned()).collect(); // All wikis with a start template
        let existing_wikis: HashSet<String> = self.get_wikis_in_database().await?.into_iter().collect();
        let new_wikis: Vec<String> = current_wikis
            .iter()
            .filter(|wiki| !existing_wikis.contains(*wiki))
            .cloned()
            .collect();
        if !new_wikis.is_empty() {
            let placeholders = self.placeholders(new_wikis.len(),"(?,'ACTIVE')");
            let sql = format!("INSERT IGNORE INTO `wikis` (`name`,`status`) VALUES {placeholders}");
            println!("Adding {new_wikis:?}");
            self.pool.get_conn().await?.exec_drop(sql,new_wikis).await?;
        }
        Ok(())
    }

    /// Updates the database to have all pages on a given wiki with both Listeria start an end template
    pub async fn update_pages_on_wiki(&self, wiki: &str) -> Result<()> {
        let api_url = self.site_matrix.get_server_url_for_wiki(wiki)? + "/w/api.php";
        let mw_api = Api::new(&api_url).await?;
        let template_start = self.config.get_local_template_title_start(wiki)?.replace(' ',"_");
        let template_end = self.config.get_local_template_title_end(wiki)?.replace(' ',"_");
        let sql = "SELECT page_namespace,page_title
            FROM page,templatelinks t1,templatelinks t2,linktarget l1,linktarget l2
            WHERE page_id=t1.tl_from AND t1.tl_target_id=l1.lt_id AND l1.lt_title=? AND l1.lt_namespace=10
            AND page_id=t2.tl_from AND t2.tl_target_id=l2.lt_id AND l2.lt_title=? AND l2.lt_namespace=10" ;
        let opts = self.get_mysql_opts_for_wiki(wiki)?;
        let current_pages: Vec<String> = Conn::new(opts).await?
            .exec_iter(sql, (template_start,template_end))
            .await?
            .map_and_drop(|row| from_row::<(i64,String)>(row))
            .await?
            .iter()
            .filter(|(nsid,_title)| self.config.can_edit_namespace(wiki, *nsid))
            .map(|(nsid,title)| Title::new(title, *nsid))
            .filter_map(|title| title.full_with_underscores(&mw_api))
            .collect();
        let existing_pages: HashSet<String> = self.get_pages_for_wiki_in_database(wiki).await?.into_iter().collect();
        let new_pages: Vec<String> = current_pages
            .iter()
            .filter(|page| !existing_pages.contains(*page))
            .cloned()
            .collect();
        if !new_pages.is_empty() {
            let wiki_id = self.get_wiki_id(wiki).await?;
            println!("Adding {} pages for {wiki}",new_pages.len());
            for chunk in new_pages.chunks(10000) {
                let chunk: Vec<String> = chunk.into();
                let placeholders = self.placeholders(chunk.len(),&format!("({wiki_id},?,'WAITING','')"));
                let sql = format!("INSERT IGNORE INTO `pagestatus` (`wiki`,`page`,`status`,`query_sparql`) VALUES {placeholders}");
                self.pool.get_conn().await?.exec_drop(sql,chunk).await?;
                }
        }

        Ok(())
    }

    /// Updates the pages on all wikis in the database
    pub async fn update_all_wikis(&self) -> Result<()> {
        let wikis = self.get_wikis_in_database().await?;
        for wiki in &wikis {
            if let Err(e) = self.update_pages_on_wiki(wiki).await {
                println!("Problem with {wiki}: {e}")
            }
        }
        Ok(())
    }

    // Generates a sequence of "ELEMENT," with given number of ELEMENTs
    fn placeholders(&self, num: usize, element: &str) -> String {
        let mut placeholders = Vec::with_capacity(num);
        placeholders.resize(num, element.to_string());
        placeholders.join(",")
    }

    // Returns all wikis in the database
    async fn get_wikis_in_database(&self) -> Result<Vec<String>> {
        Ok(self.pool.get_conn().await?
            .exec_iter("SELECT `name` FROM `wikis`", ())
            .await?
            .map_and_drop(|row| from_row::<String>(row))
            .await?)
    }

    // Returns all the pages for a given wiki in the database
    async fn get_pages_for_wiki_in_database(&self, wiki: &str) -> Result<Vec<String>> {
        let sql = "SELECT `page` FROM pagestatus,wikis WHERE wikis.id=pagestatus.wiki AND wikis.name=?";
        Ok(self.pool.get_conn().await?
            .exec_iter(sql, (wiki,))
            .await?
            .map_and_drop(|row| from_row::<String>(row))
            .await?)
    }

    /// Returns the numeric ID for a wiki in the database
    async fn get_wiki_id(&self, wiki: &str) -> Result<u64> {
        self.pool.get_conn().await?
            .exec_iter("SELECT `id` FROM `wikis` WHERE `name`=?", (wiki,))
            .await?
            .map_and_drop(|row| from_row::<u64>(row))
            .await?
            .iter()
            .cloned()
            .next()
            .ok_or_else(||anyhow!("Wiki {wiki} not known"))
        
    }

    /// Returns the database connection settings for a given wiki
    fn get_mysql_opts_for_wiki(&self,wiki:&str) -> Result<Opts> {
        let user = self.config.mysql("user").as_str().ok_or_else(||anyhow!("No MySQL user set"))?.to_string();
        let pass = self.config.mysql("password").as_str().ok_or_else(||anyhow!("No MySQL password set"))?.to_string();
        let ( host , schema ) = self.db_host_and_schema_for_wiki(&wiki)?;
        let port: u16 = if host=="127.0.0.1" { 3307 } else { self.config.mysql("port").as_u64().unwrap_or(3306) as u16} ;
        let opts = OptsBuilder::default()
            .ip_or_hostname(host)
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(pass))
            .tcp_port(port)
            .into();
        Ok(opts)
    }

    fn get_db_server_group(&self) -> &str {
        ".web.db.svc.eqiad.wmflabs"
    }

    /// Adjusts the name of some wikis to work as a DB server name
    pub fn fix_wiki_name(&self,wiki: &str) -> String {
        match wiki {
            "be-taraskwiki" | "be-x-oldwiki" | "be_taraskwiki" | "be_x_oldwiki" => "be_x_oldwiki",
            other => other,
        }.to_string().replace('-',"_")
    }

    /// Returns the server and database name for the wiki, as a tuple
    pub fn db_host_and_schema_for_wiki(&self, wiki: &str) -> Result<(String, String)> {
        let wiki = self.fix_wiki_name(wiki);
        let host = match self.config.mysql("host").as_str() {
            Some("127.0.0.1") => "127.0.0.1".to_string(),
            Some(_host) => wiki.to_owned() + self.get_db_server_group(),
            None => return Err(anyhow!("No host for MySQL")),
        };
        let schema = format!("{}_p",wiki);
        Ok((host, schema))
    }
}

/* TESTING
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3307:dewiki.web.db.svc.eqiad.wmflabs:3306 -N &
*/