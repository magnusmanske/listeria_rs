//! Wiki Replica access for the per-wiki MediaWiki databases.
//!
//! This is separate from [`crate::database_pool::DatabasePool`], which talks
//! to the bot's *own* ToolsDB database. Here we read the MediaWiki tables of
//! the wikis Listeria edits, to find out which of their pages carry a Listeria
//! template.
//!
//! ## Why a query has to name its tables
//!
//! A wiki's tables do not all live on one replica cluster. Some are split off
//! into an *extension database* reached under a prefixed host name, and in
//! September 2026 the Commons links tables — `templatelinks` and `linktarget`
//! among them — moved to such a cluster (`x4`, reached as
//! `links.commonswiki.web.db.svc.wikimedia.cloud`). See
//! <https://wikitech.wikimedia.org/wiki/News/2026_Commons_links_tables_database_split>.
//! Only Commons and its test instance are affected; every other wiki still
//! serves those tables from its core cluster.
//!
//! A caller therefore states which tables its SQL reads, and [`ReplicaDb`]
//! resolves the host from that via the `toolforge` crate's [`ExtensionDb`],
//! which holds the knowledge of the current splits, so the next one arrives
//! with a `toolforge` bump rather than an edit here. Tables spread over more
//! than one database are refused with
//! [`wikimisc::toolforge::Error::TablesSpanDatabases`] — no single connection
//! can join them, and such a query has to be taken apart and joined in code
//! instead.
//!
//! ## Connections, credentials and local development
//!
//! Connections come from one `toolforge` [`WikiPool`], which validates the
//! database name against `meta_p`, shares connections between the wikis on
//! one database section, and releases idle ones as the Toolforge
//! [connection handling policy](https://wikitech.wikimedia.org/wiki/Help:Wiki_Replicas#Connection_handling_policy)
//! asks. Host names and credentials come from the `toolforge` crate as well:
//! the `TOOL_REPLICA_USER` / `TOOL_REPLICA_PASSWORD` environment variables,
//! or else the tool's `replica.my.cnf`.
//!
//! To develop locally, copy `replica.my.cnf` to your machine, add
//! `local='true'` to its `[client]` section and open SSH tunnels with
//! `toolforge-tunnel`. The pool expects database section `sN` on the port
//! configured for it in the file's `[tunnels]` section (e.g. `s1 = 3401`), or
//! on 3306 + N, and an extension database on its own entry there (e.g.
//! `links.commonswiki = 3378`), or on 3306. Section `s7` is always needed, as
//! it holds `meta_p`.
//!
//! [`ExtensionDb`]: wikimisc::toolforge::db::ExtensionDb

use crate::configuration::Configuration;
use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::sync::OnceCell;
use wikimisc::mysql_async::{Params, from_row, prelude::*};
use wikimisc::toolforge::db::Cluster;
use wikimisc::toolforge::pool::WikiPool;

/// The replica service to connect to. `web` kills a query after five
/// minutes, which is ample for the one query Listeria runs against the
/// replicas, and leaves the slower `analytics` service to batch jobs.
const REPLICA_CLUSTER: Cluster = Cluster::Web;

/// Reads the MediaWiki tables of the wikis Listeria edits.
///
/// See the [module documentation](self) for why every query names the tables
/// it reads.
#[derive(Debug, Clone)]
pub struct ReplicaDb {
    config: Arc<Configuration>,
    /// Built on first use, so that work not touching the replicas does not
    /// depend on finding their credentials. Shared by all clones.
    pool: Arc<OnceCell<WikiPool>>,
}

impl ReplicaDb {
    pub fn new(config: Arc<Configuration>) -> Self {
        Self {
            config,
            pool: Arc::new(OnceCell::new()),
        }
    }

    /// Runs `sql` against the replica database that holds all of `tables`
    /// for `wiki`, and collects the rows.
    ///
    /// `op_name` only names the operation in the timeout error message.
    ///
    /// [`crate::database_pool::DatabasePool`]'s timeout does not cover this
    /// pool, so the whole connect-and-query chain is bounded here by the
    /// configured `db_query_timeout`. The pool also sets it as the server's
    /// `max_statement_time`, so a query given up on here does not keep
    /// running on the replica.
    ///
    /// # Errors
    ///
    /// Fails if the tables are spread over several databases, which no single
    /// connection can join, if `wiki` is not a known database, and if the
    /// tool's credentials cannot be found.
    pub async fn exec<P, T>(
        &self,
        wiki: &str,
        tables: &[&str],
        op_name: &str,
        sql: &str,
        params: P,
    ) -> Result<Vec<T>>
    where
        P: Into<Params> + Send,
        T: FromRow + Send + 'static,
    {
        let wiki = self.config.fix_wiki_name(wiki);
        let timeout = self.config.db_query_timeout();
        tokio::time::timeout(timeout, async move {
            self.pool()
                .await?
                .connect_for_tables(&wiki, tables)
                .await?
                .exec_iter(sql, params)
                .await?
                .map_and_drop(from_row::<T>)
                .await
                .map_err(anyhow::Error::from)
        })
        .await
        .map_err(|_| {
            anyhow!(
                "DB operation '{op_name}' timed out after {}s",
                timeout.as_secs()
            )
        })?
    }

    /// The connection pool, built on first use.
    async fn pool(&self) -> Result<&WikiPool> {
        let pool = self
            .pool
            .get_or_try_init(|| async {
                WikiPool::builder(REPLICA_CLUSTER)
                    .max_statement_time(self.config.db_query_timeout())
                    .build()
            })
            .await?;
        Ok(pool)
    }
}

/// Live tests against the Wiki Replicas. They need the tool's replica
/// credentials and, locally, `toolforge-tunnel` running for sections `s1`,
/// `s3` and `s7` and for `links.commonswiki` (see the module documentation).
#[cfg(test)]
mod tests {
    use super::*;

    /// The tables of `WikiApis::get_current_pages_on_wiki`.
    const TABLES: &[&str] = &["page", "templatelinks", "linktarget"];

    /// A few pages that transclude `Template:{template}`, over the same
    /// tables as `WikiApis::get_current_pages_on_wiki`.
    const SQL: &str = "SELECT page_namespace,page_title
        FROM page,templatelinks,linktarget
        WHERE page_id=tl_from AND tl_target_id=lt_id AND lt_namespace=10 AND lt_title=?
        LIMIT 3";

    fn replica_db() -> ReplicaDb {
        let config = Configuration::default().with_db_query_timeout_sec(30);
        ReplicaDb::new(Arc::new(config))
    }

    async fn pages_with_template(
        replica_db: &ReplicaDb,
        wiki: &str,
        template: &str,
    ) -> Result<Vec<(i64, String)>> {
        replica_db
            .exec(wiki, TABLES, "test", SQL, (template,))
            .await
    }

    #[tokio::test]
    #[ignore = "requires Wiki Replica access"]
    async fn test_live_core_database() {
        let pages = pages_with_template(&replica_db(), "enwiki", "Wikidata_list")
            .await
            .unwrap();
        assert_eq!(pages.len(), 3);
    }

    /// Commons' links tables are only up to date in its `links` extension
    /// database.
    #[tokio::test]
    #[ignore = "requires Wiki Replica access"]
    async fn test_live_links_extension_database() {
        let pages = pages_with_template(&replica_db(), "commonswiki", "Information")
            .await
            .unwrap();
        assert_eq!(pages.len(), 3);
    }

    /// Wiki names are normalised before the lookup: `be-taraskwiki` lives in
    /// the `be_x_oldwiki` database. One pool serves all wikis.
    #[tokio::test]
    #[ignore = "requires Wiki Replica access"]
    async fn test_live_wiki_name_is_normalised() {
        let replica_db = replica_db();
        let sql = "SELECT page_title FROM page WHERE page_namespace=0 ORDER BY page_id LIMIT 3";
        let mut results = Vec::new();
        for wiki in ["be-taraskwiki", "be_x_oldwiki"] {
            let titles: Vec<String> = replica_db
                .exec(wiki, &["page"], "test", sql, ())
                .await
                .unwrap();
            assert_eq!(titles.len(), 3, "{wiki}");
            results.push(titles);
        }
        assert_eq!(results[0], results[1]);
    }

    /// Names that are not a known database are refused before any query.
    #[tokio::test]
    #[ignore = "requires Wiki Replica access"]
    async fn test_live_unknown_wiki_is_rejected() {
        let replica_db = replica_db();
        for wiki in ["nosuchwiki", "enwiki.evil.example", "enwiki;DROP"] {
            let err = pages_with_template(&replica_db, wiki, "Wikidata_list")
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<wikimisc::toolforge::Error>(),
                    Some(wikimisc::toolforge::Error::UnknownDatabase(_))
                ),
                "{wiki}: {err}"
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires Wiki Replica access"]
    async fn test_live_tables_spanning_databases_are_rejected() {
        let result: Result<Vec<u64>> = replica_db()
            .exec(
                "commonswiki",
                &["actor", "templatelinks"],
                "test",
                "SELECT 1",
                (),
            )
            .await;
        let err = result.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<wikimisc::toolforge::Error>(),
                Some(wikimisc::toolforge::Error::TablesSpanDatabases { .. })
            ),
            "{err}"
        );
    }
}
