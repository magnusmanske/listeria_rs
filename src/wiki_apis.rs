//! MediaWiki API management and connection pooling for multiple wikis.

use crate::{
    ApiArc, configuration::Configuration, database_pool::DatabasePool, wiki::Wiki,
    wiki_repository::WikiRepository,
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use log::info;
use mysql_async::{Conn, Opts, OptsBuilder, from_row, prelude::*};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use wikimisc::{
    mediawiki::{api::Api, title::Title},
    site_matrix::SiteMatrix,
    wikibase::{Entity, EntityTrait, entity_container::EntityContainer},
};

/// Cached per-wiki resources. The `api` is set up lazily on first use; the
/// `semaphore` caps in-flight operations against that wiki to
/// `max_mw_apis_per_wiki`. Both live in the same struct so the per-wiki
/// `OnceCell` produces a paired (api, semaphore) atomically.
struct WikiInner {
    api: ApiArc,
    /// Permits = `max_mw_apis_per_wiki` (or effectively unlimited when the
    /// config value is unset). Permits are acquired by callers via
    /// `acquire_wiki_api` and held for the duration of the operation.
    semaphore: Arc<Semaphore>,
}

/// RAII guard returned by [`WikiApis::acquire_wiki_api`]. Holds the API plus
/// the per-wiki and (optional) global permits; permits are released only
/// when this guard is dropped, providing real backpressure instead of the
/// strong-count poll the previous implementation relied on.
#[derive(Debug)]
pub struct WikiApiHandle {
    api: ApiArc,
    // Permits live here purely so Drop releases them. Their precise type isn't
    // observable to callers, but we keep them named so it's obvious why they
    // exist if someone reads this struct.
    _per_wiki_permit: OwnedSemaphorePermit,
    _total_permit: Option<OwnedSemaphorePermit>,
}

impl WikiApiHandle {
    /// Borrow the API for the lifetime of this guard.
    pub fn api(&self) -> &ApiArc {
        &self.api
    }
}

#[derive(Clone)]
pub struct WikiApis {
    config: Arc<Configuration>,
    site_matrix: SiteMatrix,
    /// Each entry is an `Arc<OnceCell<WikiInner>>` so that concurrent callers
    /// for the same new wiki wait on a single `get_or_try_init` rather than
    /// racing to each create their own TCP connection. The `DashMap` shard
    /// lock is held only for the map lookup/insert, not during the async API
    /// creation.
    apis: Arc<DashMap<String, Arc<OnceCell<WikiInner>>>>,
    /// Global cap across all wikis. `None` when the config doesn't set
    /// `max_mw_apis_total` — in that case no global gate is applied.
    total_semaphore: Option<Arc<Semaphore>>,
    /// Owns the SQL that talks to the bot's `wikis` and `pagestatus` tables.
    /// Kept as a separate type so the SQL strings live in one place and the
    /// rest of `WikiApis` stays focused on API pooling and wiki discovery.
    wiki_repo: WikiRepository,
}

impl std::fmt::Debug for WikiApis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Avoid descending into the cached APIs (they don't implement Debug
        // uniformly upstream); print just the bookkeeping fields that matter.
        f.debug_struct("WikiApis")
            .field("num_wikis_cached", &self.apis.len())
            .field("has_total_gate", &self.total_semaphore.is_some())
            .finish()
    }
}

impl WikiApis {
    pub async fn new(config: Arc<Configuration>) -> Result<Self> {
        let pool = DatabasePool::new(&config)?;
        let site_matrix = SiteMatrix::new(config.get_default_wbapi()?).await?;
        let total_semaphore = (*config.get_max_mw_apis_total())
            .map(|n| Arc::new(Semaphore::new(n)));
        Ok(Self {
            apis: Arc::new(DashMap::new()),
            config,
            site_matrix,
            total_semaphore,
            wiki_repo: WikiRepository::new(pool),
        })
    }

    /// Acquires a guarded handle to a wiki's MediaWiki API.
    ///
    /// Replaces the legacy `Arc::strong_count`-polled gate (see #T356160) with
    /// real semaphores:
    ///  1. Optionally acquires a global permit (`max_mw_apis_total`).
    ///  2. Lazily initialises the per-wiki API and a per-wiki semaphore sized
    ///     to `max_mw_apis_per_wiki` (defaulting to effectively unlimited).
    ///  3. Acquires a per-wiki permit.
    ///
    /// Both permits are stored on the returned [`WikiApiHandle`] and released
    /// on drop, giving correct, non-polling backpressure that doesn't depend
    /// on counting transient `Arc` clones.
    pub async fn acquire_wiki_api(&self, wiki: &str) -> Result<WikiApiHandle> {
        // Acquire the global permit first so we never hold a per-wiki permit
        // while waiting on the global one — that ordering preserves liveness
        // even when wikis fight for the same global budget.
        let total_permit = match &self.total_semaphore {
            Some(sem) => Some(
                Arc::clone(sem)
                    .acquire_owned()
                    .await
                    .map_err(|e| anyhow!("global mw_api semaphore closed: {e}"))?,
            ),
            None => None,
        };

        // Get-or-create the OnceCell for this wiki, holding the DashMap shard
        // lock only for the brief map operation, not during async init.
        let once = self
            .apis
            .entry(wiki.to_owned())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        // Initialise exactly once; concurrent callers wait on the first init.
        let inner = once
            .get_or_try_init(|| async {
                let api = self.create_wiki_api(wiki).await?;
                let permits = (*self.config.get_max_mw_apis_per_wiki())
                    .unwrap_or(Semaphore::MAX_PERMITS);
                info!(target: "lock", "WikiApis::acquire_wiki_api: new wiki {wiki} created");
                Ok::<WikiInner, anyhow::Error>(WikiInner {
                    api,
                    semaphore: Arc::new(Semaphore::new(permits)),
                })
            })
            .await?;

        let per_wiki_permit = Arc::clone(&inner.semaphore)
            .acquire_owned()
            .await
            .map_err(|e| anyhow!("per-wiki mw_api semaphore closed: {e}"))?;

        Ok(WikiApiHandle {
            api: inner.api.clone(),
            _per_wiki_permit: per_wiki_permit,
            _total_permit: total_permit,
        })
    }

    /// Creates a MediaWiki API instance for the given wiki
    async fn create_wiki_api(&self, wiki: &str) -> Result<ApiArc> {
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
    ) -> Result<ApiArc> {
        let builder = wikimisc::mediawiki::reqwest::Client::builder()
            .timeout(self.config.api_timeout())
            .user_agent(crate::LISTERIA_USER_AGENT)
            .gzip(true)
            .deflate(true)
            .brotli(true);
        let mut mw_api = Api::new_from_builder(api_url, builder).await?;
        mw_api.set_oauth2(oauth2_token);
        mw_api.set_edit_delay(self.config.ms_delay_after_edit()); // Slow down editing a bit
        let mw_api = Arc::new(mw_api);
        Ok(mw_api)
    }

    /// Updates the database to contain all wikis that have a Listeria start template
    pub async fn update_wiki_list_in_database(&self) -> Result<()> {
        let current_wikis = self.get_all_wikis_with_start_template().await?;
        let existing_wikis: HashSet<String> = self
            .get_all_wikis_in_database()
            .await?
            .keys()
            .cloned()
            .collect();
        let new_wikis: Vec<String> = current_wikis
            .iter()
            .filter(|wiki| !existing_wikis.contains(*wiki))
            .cloned()
            .collect();
        if !new_wikis.is_empty() {
            log::info!("Adding {new_wikis:?}");
        }
        self.wiki_repo.add_wikis(&new_wikis).await?;
        Ok(())
    }

    /// Returns a list of all wikis with a start template from Wikidata
    async fn get_all_wikis_with_start_template(&self) -> Result<Vec<String>> {
        let q = self.config.get_template_start_q();
        let api = self.config.get_default_wbapi()?;
        let start_template_entity = self.load_entity_from_id(api, q).await?;
        let current_wikis: Vec<String> = Self::get_all_wikis_with_template(start_template_entity);
        Ok(current_wikis)
    }

    /// Returns the Wikidata item for a given template
    async fn load_entity_from_id(&self, api: &Arc<Api>, q: String) -> Result<Entity> {
        let entity_container = EntityContainer::new();
        let to_load = vec![q.to_owned()];
        if let Err(e) = entity_container.load_entities(api, &to_load).await {
            return Err(anyhow!("{q} item not found on Wikidata: {e}"));
        }
        let entity = entity_container
            .get_entity(&q)
            .ok_or_else(|| anyhow!("{q} item not found on Wikidata"))?;
        Ok(entity)
    }

    /// Updates the database to have all pages on a given wiki with both Listeria start an end template
    pub async fn update_pages_on_wiki(&self, wiki: &str) -> Result<()> {
        let current_pages = self.get_current_pages_on_wiki(wiki).await?;
        let existing_pages: HashSet<String> = self
            .wiki_repo
            .get_pages_for_wiki(wiki)
            .await?
            .into_iter()
            .collect();
        let new_pages: Vec<String> = current_pages
            .iter()
            .filter(|page| !existing_pages.contains(*page))
            .cloned()
            .collect();
        if !new_pages.is_empty() {
            let wiki_id = self.wiki_repo.get_wiki_id(wiki).await?;
            log::info!("Adding {} pages for {wiki}", new_pages.len());
            self.wiki_repo.add_pages_for_wiki(wiki_id, &new_pages).await?;
        }
        Ok(())
    }

    async fn get_current_pages_on_wiki(&self, wiki: &str) -> Result<Vec<String>> {
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
        // Per-wiki replica is a direct (non-pooled) connection, so the
        // DatabasePool's with_timeout doesn't apply. Bound the whole
        // (connect + query) chain explicitly using the configured budget.
        let db_timeout = self.config.db_query_timeout();
        let rows: Vec<(i64, String)> = tokio::time::timeout(db_timeout, async {
            Conn::new(opts)
                .await?
                .exec_iter(sql, (template_start, template_end))
                .await?
                .map_and_drop(from_row::<(i64, String)>)
                .await
                .map_err(anyhow::Error::from)
        })
        .await
        .map_err(|_| {
            anyhow!(
                "DB operation 'get_current_pages_on_wiki' timed out after {}s",
                db_timeout.as_secs()
            )
        })??;
        let current_pages: Vec<String> = rows
            .iter()
            .filter(|(nsid, _title)| self.config.can_edit_namespace(wiki, *nsid))
            .map(|(nsid, title)| Title::new(title, *nsid))
            .filter_map(|title| title.full_with_underscores(&mw_api))
            .collect();
        Ok(current_pages)
    }

    /// Updates the pages on all wikis in the database
    pub async fn update_all_wikis(&self) -> Result<()> {
        let wikis = self.get_all_wikis_in_database().await?;
        for (name, _wiki) in wikis {
            if let Err(e) = self.update_pages_on_wiki(&name).await {
                log::warn!("Problem with {name}: {e}");
            }
        }
        Ok(())
    }

    /// Returns all the wikis in the database (delegates to `WikiRepository`
    /// so the public API stays stable; SQL lives in the repository).
    pub async fn get_all_wikis_in_database(&self) -> Result<HashMap<String, Wiki>> {
        self.wiki_repo.get_all_wikis().await
    }

    /// Helper method to extract a string value from MySQL configuration
    fn get_mysql_config_string(&self, key: &str) -> Result<String> {
        self.config
            .mysql(key)
            .as_str()
            .ok_or_else(|| anyhow!("No MySQL {key} set"))
            .map(|s| s.to_string())
    }

    /// Returns the database connection settings for a given wiki
    fn get_mysql_user(&self) -> Result<String> {
        self.get_mysql_config_string("user")
    }

    /// Returns the MySQL password from the configuration
    fn get_mysql_password(&self) -> Result<String> {
        self.get_mysql_config_string("password")
    }

    /// Returns the database connection settings for a given wiki
    fn get_mysql_opts_for_wiki(&self, wiki: &str) -> Result<Opts> {
        let user = self.get_mysql_user()?;
        let pass = self.get_mysql_password()?;
        let (host, schema) = self.db_host_and_schema_for_wiki(wiki)?;
        let port: u16 = if host == "127.0.0.1" {
            3307
        } else {
            self.config
                .mysql("port")
                .as_u64()
                .and_then(|u| u.try_into().ok())
                .unwrap_or(3306)
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

    /// Returns the server group for the database
    #[allow(clippy::unused_self)]
    const fn get_db_server_group(&self) -> &str {
        ".web.db.svc.eqiad.wmflabs"
    }

    /// Returns the server and database name for the wiki, as a tuple
    pub fn db_host_and_schema_for_wiki(&self, wiki: &str) -> Result<(String, String)> {
        let wiki = self.config.fix_wiki_name(wiki);
        let host = match self.config.mysql("host").as_str() {
            Some("127.0.0.1") => "127.0.0.1".to_string(),
            Some(_host) => wiki.to_owned() + self.get_db_server_group(),
            None => return Err(anyhow!("No host for MySQL")),
        };
        let schema = format!("{wiki}_p");
        Ok((host, schema))
    }

    /// Returns the a list of all wikis with a start template
    fn get_all_wikis_with_template(entity: Entity) -> Vec<String> {
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

    #[tokio::test]
    #[ignore = "requires Toolforge MySQL tunnel on port 3308"]
    async fn test_get_db_server_group() {
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let wa = WikiApis::new(Arc::new(config)).await.unwrap();
        assert_eq!(wa.get_db_server_group(), ".web.db.svc.eqiad.wmflabs");
    }

    #[test]
    fn test_placeholders() {
        let result = std::iter::repeat_n("?", 3).collect::<Vec<_>>().join(",");
        assert_eq!(result, "?,?,?");
    }

    /// `WikiApiHandle` is just a guard; the contract this test pins down is
    /// that dropping it releases the per-wiki permit, so a second call to
    /// `acquire_owned` on the same Semaphore can proceed. Mirrors the
    /// acquisition pattern used in `acquire_wiki_api` without needing the
    /// full `WikiApis` construction (which requires a live DB).
    #[tokio::test]
    async fn test_per_wiki_permit_releases_on_drop() {
        let sem = Arc::new(Semaphore::new(1));

        let p1 = Arc::clone(&sem).acquire_owned().await.unwrap();
        // While p1 is alive, a non-blocking try should fail.
        assert!(
            Arc::clone(&sem).try_acquire_owned().is_err(),
            "permit should be held"
        );
        drop(p1);
        // Permit released — a fresh try should now succeed.
        let _p2 = Arc::clone(&sem)
            .try_acquire_owned()
            .expect("permit should be available after drop");
    }
}
