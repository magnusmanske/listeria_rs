use crate::configuration::Configuration;
use crate::database_pool::DatabasePool;
use crate::site_matrix::SiteMatrix;
use crate::ApiLock;
use anyhow::{anyhow, Result};
use log::{info, warn};
use mysql_async::{from_row, prelude::*, Conn};
use mysql_async::{Opts, OptsBuilder};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::sleep;
use wikibase::mediawiki::api::Api;
use wikibase::mediawiki::title::Title;
use wikibase::EntityTrait;

const LISTERIA_USER_AGENT: &str = "User-Agent: ListeriaBot/0.1.2 (https://listeria.toolforge.org/; magnusmanske@googlemail.com) reqwest/0.11.23";

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
        self.wait_for_max_mw_apis_total().await;

        if let Some(api) = &self.apis.lock().await.get(wiki) {
            self.wait_for_wiki_apis(api).await;
            return Ok((*api).clone());
        }

        let mut lock = self.apis.lock().await;
        let mw_api = self.create_wiki_api(wiki).await?;
        lock.insert(wiki.to_owned(), mw_api);
        info!(target: "lock", "WikiApis::get_or_create_wiki_api: new wiki {wiki} created");

        lock.get(wiki)
            .ok_or(anyhow!("Wiki not found: {wiki}"))
            .map(|api| api.clone())
    }

    async fn wait_for_wiki_apis(&self, api: &&Arc<RwLock<Api>>) {
        // Prevent many APIs in use, to limit the number of concurrent requests, to avoid 104 errors.
        // See https://phabricator.wikimedia.org/T356160
        if let Some(max) = self.config.get_max_mw_apis_per_wiki() {
            while Arc::strong_count(api) >= *max {
                sleep(Duration::from_millis(100)).await;
                warn!(target: "lock", "WikiApis::get_or_create_wiki_api: sleeping because per-wiki limit {} was reached", max);
            }
        }
    }

    async fn wait_for_max_mw_apis_total(&self) {
        if let Some(max) = self.config.get_max_mw_apis_total() {
            loop {
                let current_strong_locks: usize = self
                    .apis
                    .lock()
                    .await
                    .iter()
                    .map(|(_wiki, api)| Arc::strong_count(api))
                    .sum();
                if current_strong_locks < *max {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
                warn!(target: "lock", "WikiApis::get_or_create_wiki_api: sleeping because total limit {} was reached", max);
            }
        }
    }

    /// Creates a MediaWiki API instance for the given wiki
    async fn create_wiki_api(&self, wiki: &str) -> Result<ApiLock> {
        let api_url = format!(
            "{}/w/api.php",
            self.site_matrix.get_server_url_for_wiki(wiki)?
        );
        self.create_wiki_api_from_api_url(&api_url, self.config.oauth2_token())
            .await
    }

    pub async fn create_wiki_api_from_api_url(
        &self,
        api_url: &str,
        oauth2_token: &str,
    ) -> Result<ApiLock> {
        let builder = wikibase::mediawiki::reqwest::Client::builder()
            .timeout(self.config.api_timeout())
            .user_agent(LISTERIA_USER_AGENT)
            .gzip(true)
            .deflate(true)
            .brotli(true);
        let mut mw_api = Api::new_from_builder(api_url, builder).await?;
        mw_api.set_oauth2(oauth2_token);
        mw_api.set_edit_delay(self.config.ms_delay_after_edit()); // Slow down editing a bit
        let mw_api = Arc::new(RwLock::new(mw_api));
        Ok(mw_api)
    }

    /// Updates the database to contain all wikis that have a Listeria start template
    pub async fn update_wiki_list_in_database(&self) -> Result<()> {
        let q = self.config.get_template_start_q(); // Wikidata item for {{Wikidata list}}
        let api = self.config.get_default_wbapi()?;
        let start_template_entity = self.load_entity_from_id(api, q).await?;
        let current_wikis: Vec<String> =
            Self::get_all_wikis_with_start_template(start_template_entity);
        let existing_wikis: HashSet<String> =
            self.get_wikis_in_database().await?.into_iter().collect();
        let new_wikis: Vec<String> = current_wikis
            .iter()
            .filter(|wiki| !existing_wikis.contains(*wiki))
            .cloned()
            .collect();
        self.add_new_wikis_to_database(new_wikis).await?;
        Ok(())
    }

    /// Adds new wikis to the database
    async fn add_new_wikis_to_database(&self, new_wikis: Vec<String>) -> Result<(), anyhow::Error> {
        if new_wikis.is_empty() {
            return Ok(());
        }
        let placeholders = self.placeholders(new_wikis.len(), "(?,'ACTIVE')");
        let sql = format!("INSERT IGNORE INTO `wikis` (`name`,`status`) VALUES {placeholders}");
        println!("Adding {new_wikis:?}");
        self.pool
            .get_conn()
            .await?
            .exec_drop(sql, new_wikis)
            .await?;
        Ok(())
    }

    /// Returns the Wikidata item for a given template
    async fn load_entity_from_id(
        &self,
        api: &Arc<Api>,
        q: String,
    ) -> Result<wikibase::Entity, anyhow::Error> {
        let entities = self.config.create_entity_container();
        entities
            .load_entities(api, &vec![q.to_owned()])
            .await
            .map_err(|e| anyhow!("{e}"))?;
        let entity = entities
            .get_entity(&q)
            .ok_or_else(|| anyhow!("{q} item not found on Wikidata"))?;
        Ok(entity)
    }

    /// Updates the database to have all pages on a given wiki with both Listeria start an end template
    pub async fn update_pages_on_wiki(&self, wiki: &str) -> Result<()> {
        let api_url = self.site_matrix.get_server_url_for_wiki(wiki)? + "/w/api.php";
        let mw_api = Api::new(&api_url).await?;
        let template_start = self
            .config
            .get_local_template_title_start(wiki)?
            .replace(' ', "_");
        let template_end = self
            .config
            .get_local_template_title_end(wiki)?
            .replace(' ', "_");
        let sql = "SELECT page_namespace,page_title
            FROM page,templatelinks t1,templatelinks t2,linktarget l1,linktarget l2
            WHERE page_id=t1.tl_from AND t1.tl_target_id=l1.lt_id AND l1.lt_title=? AND l1.lt_namespace=10
            AND page_id=t2.tl_from AND t2.tl_target_id=l2.lt_id AND l2.lt_title=? AND l2.lt_namespace=10" ;
        let opts = self.get_mysql_opts_for_wiki(wiki)?;
        let current_pages: Vec<String> = Conn::new(opts)
            .await?
            .exec_iter(sql, (template_start, template_end))
            .await?
            .map_and_drop(from_row::<(i64, String)>)
            .await?
            .iter()
            .filter(|(nsid, _title)| self.config.can_edit_namespace(wiki, *nsid))
            .map(|(nsid, title)| Title::new(title, *nsid))
            .filter_map(|title| title.full_with_underscores(&mw_api))
            .collect();
        let existing_pages: HashSet<String> = self
            .get_pages_for_wiki_in_database(wiki)
            .await?
            .into_iter()
            .collect();
        let new_pages: Vec<String> = current_pages
            .iter()
            .filter(|page| !existing_pages.contains(*page))
            .cloned()
            .collect();
        if !new_pages.is_empty() {
            let wiki_id = self.get_wiki_id(wiki).await?;
            println!("Adding {} pages for {wiki}", new_pages.len());
            for chunk in new_pages.chunks(10000) {
                let chunk: Vec<String> = chunk.into();
                let placeholders =
                    self.placeholders(chunk.len(), &format!("({wiki_id},?,'WAITING','')"));
                let sql = format!("INSERT IGNORE INTO `pagestatus` (`wiki`,`page`,`status`,`query_sparql`) VALUES {placeholders}");
                self.pool.get_conn().await?.exec_drop(sql, chunk).await?;
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

    /// Returns a string with the given number of placeholders, separated by commas
    fn placeholders(&self, num: usize, element: &str) -> String {
        let mut placeholders = Vec::with_capacity(num);
        placeholders.resize(num, element.to_string());
        placeholders.join(",")
    }

    /// Returns all the wikis in the database
    async fn get_wikis_in_database(&self) -> Result<Vec<String>> {
        Ok(self
            .pool
            .get_conn()
            .await?
            .exec_iter("SELECT `name` FROM `wikis`", ())
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    /// Returns all the pages for a given wiki in the database
    async fn get_pages_for_wiki_in_database(&self, wiki: &str) -> Result<Vec<String>> {
        let sql =
            "SELECT `page` FROM pagestatus,wikis WHERE wikis.id=pagestatus.wiki AND wikis.name=?";
        Ok(self
            .pool
            .get_conn()
            .await?
            .exec_iter(sql, (wiki,))
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    /// Returns the numeric ID for a wiki in the database
    async fn get_wiki_id(&self, wiki: &str) -> Result<u64> {
        self.pool
            .get_conn()
            .await?
            .exec_iter("SELECT `id` FROM `wikis` WHERE `name`=?", (wiki,))
            .await?
            .map_and_drop(from_row::<u64>)
            .await?
            .iter()
            .cloned()
            .next()
            .ok_or_else(|| anyhow!("Wiki {wiki} not known"))
    }

    /// Returns the database connection settings for a given wiki
    fn get_mysql_user(&self) -> Result<String> {
        self.config
            .mysql("user")
            .as_str()
            .ok_or_else(|| anyhow!("No MySQL user set"))
            .map(|s| s.to_string())
    }

    /// Returns the MySQL password from the configuration
    fn get_mysql_password(&self) -> Result<String> {
        self.config
            .mysql("password")
            .as_str()
            .ok_or_else(|| anyhow!("No MySQL password set"))
            .map(|s| s.to_string())
    }

    /// Returns the database connection settings for a given wiki
    fn get_mysql_opts_for_wiki(&self, wiki: &str) -> Result<Opts> {
        let user = self.get_mysql_user()?;
        let pass = self.get_mysql_password()?;
        let (host, schema) = self.db_host_and_schema_for_wiki(wiki)?;
        let port: u16 = if host == "127.0.0.1" {
            3307
        } else {
            self.config.mysql("port").as_u64().unwrap_or(3306) as u16
        };
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
    pub fn fix_wiki_name(&self, wiki: &str) -> String {
        match wiki {
            "be-taraskwiki" | "be-x-oldwiki" | "be_taraskwiki" | "be_x_oldwiki" => "be_x_oldwiki",
            other => other,
        }
        .to_string()
        .replace('-', "_")
    }

    /// Returns the server and database name for the wiki, as a tuple
    pub fn db_host_and_schema_for_wiki(&self, wiki: &str) -> Result<(String, String)> {
        let wiki = self.fix_wiki_name(wiki);
        let host = match self.config.mysql("host").as_str() {
            Some("127.0.0.1") => "127.0.0.1".to_string(),
            Some(_host) => wiki.to_owned() + self.get_db_server_group(),
            None => return Err(anyhow!("No host for MySQL")),
        };
        let schema = format!("{}_p", wiki);
        Ok((host, schema))
    }

    /// Returns the a list of all wikis with a start template
    fn get_all_wikis_with_start_template(entity: wikibase::Entity) -> Vec<String> {
        entity
            .sitelinks()
            .iter()
            .flatten()
            .map(|s| s.site().to_owned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /* TESTING
    ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
    ssh magnus@tools-login.wmflabs.org -L 3307:dewiki.web.db.svc.eqiad.wmflabs:3306 -N &
    */

    #[tokio::test]
    async fn test_fix_wiki_name() {
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let wa = WikiApis::new(Arc::new(config)).await.unwrap();
        assert_eq!(wa.fix_wiki_name("be-taraskwiki"), "be_x_oldwiki");
        assert_eq!(wa.fix_wiki_name("be_taraskwiki"), "be_x_oldwiki");
        assert_eq!(wa.fix_wiki_name("be-x-oldwiki"), "be_x_oldwiki");
        assert_eq!(wa.fix_wiki_name("be_x_oldwiki"), "be_x_oldwiki");
        assert_eq!(wa.fix_wiki_name("dewiki"), "dewiki");
    }

    #[tokio::test]
    async fn test_get_db_server_group() {
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let wa = WikiApis::new(Arc::new(config)).await.unwrap();
        assert_eq!(wa.get_db_server_group(), ".web.db.svc.eqiad.wmflabs");
    }

    #[tokio::test]
    async fn test_placeholders() {
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let wa = WikiApis::new(Arc::new(config)).await.unwrap();
        assert_eq!(wa.placeholders(3, "?"), "?,?,?");
    }
}
