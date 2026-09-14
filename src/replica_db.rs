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
//! resolves the host from that: [`DbCluster`] holds wikimisc's knowledge of
//! the current splits, so the next one arrives with a wikimisc bump rather
//! than an edit here. Tables spread over more than one cluster are refused
//! with [`wikimisc::toolforge_db::DatabaseError::TablesSpanClusters`] — no
//! single connection can join them, and such a query has to be taken apart
//! and joined in code instead.
//!
//! ## Local development
//!
//! A `mysql.host` of `127.0.0.1` means the replicas are reached through an SSH
//! tunnel, and every wiki and cluster then shares that one host and port. A
//! single tunnel only reaches a single cluster, so querying the Commons links
//! tables locally needs the tunnel pointed at
//! `links.commonswiki.web.db.svc.wikimedia.cloud`.

use crate::configuration::Configuration;
use anyhow::{Result, anyhow};
use mysql_async::{Conn, Opts, OptsBuilder, Params, from_row, prelude::*};
use std::sync::Arc;
use wikimisc::toolforge_db::{DbCluster, DbServerGroup, HostSchema};

/// Domain shared by every Wiki Replica service name.
const REPLICA_DOMAIN: &str = "db.svc.wikimedia.cloud";

/// The replica service name to build host names from. `web` kills a query
/// after five minutes, which is ample for the one query Listeria runs against
/// the replicas, and leaves the slower `analytics` service to batch jobs.
const REPLICA_SERVICE: DbServerGroup = DbServerGroup::Web;

/// The `mysql.host` value that means "everything is reached through a local
/// SSH tunnel", i.e. a development machine rather than Toolforge.
const LOCAL_HOST: &str = "127.0.0.1";

/// Port the local SSH tunnel to the replicas is expected on.
const LOCAL_TUNNEL_PORT: u16 = 3307;

/// Port of every Toolforge database service.
const DEFAULT_PORT: u16 = 3306;

/// Reads the MediaWiki tables of the wikis Listeria edits.
///
/// See the [module documentation](self) for why every query names the tables
/// it reads.
#[derive(Debug, Clone)]
pub struct ReplicaDb {
    config: Arc<Configuration>,
}

impl ReplicaDb {
    pub const fn new(config: Arc<Configuration>) -> Self {
        Self { config }
    }

    /// Runs `sql` against the replica cluster that holds all of `tables` for
    /// `wiki`, and collects the rows.
    ///
    /// `op_name` only names the operation in the timeout error message.
    ///
    /// The connection is a direct one rather than out of a pool: the caller
    /// runs a single query per wiki, at most once per pass over the wiki list,
    /// so a pool per wiki would only hold connections the Toolforge
    /// [connection handling policy](https://wikitech.wikimedia.org/wiki/Help:Wiki_Replicas#Connection_handling_policy)
    /// wants released. That also means [`crate::database_pool::DatabasePool`]'s
    /// timeout does not cover it, so the whole connect-and-query chain is
    /// bounded here by the configured `db_query_timeout`.
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
        let opts = self.opts_for_tables(wiki, tables)?;
        let timeout = self.config.db_query_timeout();
        tokio::time::timeout(timeout, async move {
            Conn::new(opts)
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

    /// Host and schema of the wiki's core cluster.
    ///
    /// Tables split off into an extension database are *not* readable there;
    /// use [`Self::host_and_schema_for_tables`] to let the table list pick the
    /// cluster.
    pub fn host_and_schema(&self, wiki: &str) -> Result<HostSchema> {
        self.host_and_schema_for_cluster(wiki, DbCluster::Core)
    }

    /// Host and schema of one named cluster of the wiki.
    ///
    /// Asking for a cluster the wiki has no split for falls back to
    /// [`DbCluster::Core`], where those tables still live.
    pub fn host_and_schema_for_cluster(
        &self,
        wiki: &str,
        cluster: DbCluster,
    ) -> Result<HostSchema> {
        let wiki = self.config.fix_wiki_name(wiki);
        Ok(HostSchema::new(
            &Self::replica_host(self.local_host()?.as_deref(), &wiki, cluster),
            &format!("{wiki}_p"),
        ))
    }

    /// Host and schema of the cluster that can serve a query reading all of
    /// `tables`.
    ///
    /// # Errors
    ///
    /// Fails if the tables are spread over several clusters, which no single
    /// connection can join.
    pub fn host_and_schema_for_tables(&self, wiki: &str, tables: &[&str]) -> Result<HostSchema> {
        let wiki = self.config.fix_wiki_name(wiki);
        let cluster = DbCluster::for_tables(&wiki, tables)?;
        self.host_and_schema_for_cluster(&wiki, cluster)
    }

    /// Connection options for a query over `tables`.
    fn opts_for_tables(&self, wiki: &str, tables: &[&str]) -> Result<Opts> {
        let host_schema = self.host_and_schema_for_tables(wiki, tables)?;
        let opts = OptsBuilder::default()
            .ip_or_hostname(host_schema.host())
            .db_name(Some(host_schema.schema()))
            .user(Some(self.config_string("user")?))
            .pass(Some(self.config_string("password")?))
            .tcp_port(self.port(host_schema.host()))
            .into();
        Ok(opts)
    }

    /// Builds the replica host name for one cluster of a wiki, or returns the
    /// tunnel host when there is one. `wiki` must already be normalised to its
    /// database name.
    fn replica_host(local_host: Option<&str>, wiki: &str, cluster: DbCluster) -> String {
        if let Some(host) = local_host {
            // Off Toolforge every cluster is reached through the same tunnel.
            return host.to_string();
        }
        // A wiki without this split keeps serving those tables from its core
        // cluster, which has no host prefix.
        let prefix = match cluster.applies_to_wiki(wiki) {
            true => cluster.host_prefix(),
            false => "",
        };
        format!("{prefix}{wiki}.{REPLICA_SERVICE}.{REPLICA_DOMAIN}")
    }

    /// `Some(host)` when the configuration points at a local tunnel rather
    /// than at Toolforge, in which case that one host serves every wiki and
    /// cluster.
    fn local_host(&self) -> Result<Option<String>> {
        match self.config.mysql("host").as_str() {
            Some(LOCAL_HOST) => Ok(Some(LOCAL_HOST.to_string())),
            Some(_) => Ok(None),
            None => Err(anyhow!("No host for MySQL")),
        }
    }

    /// The port for `host`: the tunnel's when running locally, otherwise the
    /// configured one, which every Toolforge database service has at 3306.
    fn port(&self, host: &str) -> u16 {
        if host == LOCAL_HOST {
            return LOCAL_TUNNEL_PORT;
        }
        self.config
            .mysql("port")
            .as_u64()
            .and_then(|port| port.try_into().ok())
            .unwrap_or(DEFAULT_PORT)
    }

    /// A string value from the `mysql` config object.
    fn config_string(&self, key: &str) -> Result<String> {
        self.config
            .mysql(key)
            .as_str()
            .ok_or_else(|| anyhow!("No MySQL {key} set"))
            .map(ToString::to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tables of the query in `WikiApis::get_current_pages_on_wiki` — the one
    /// the Commons split actually moved.
    const PAGE_TEMPLATE_TABLES: &[&str] = &["page", "templatelinks", "linktarget"];

    #[test]
    fn test_replica_host_on_toolforge_uses_core_cluster_by_default() {
        assert_eq!(
            ReplicaDb::replica_host(None, "enwiki", DbCluster::Core),
            "enwiki.web.db.svc.wikimedia.cloud"
        );
    }

    #[test]
    fn test_replica_host_prefixes_the_links_cluster_for_commons() {
        assert_eq!(
            ReplicaDb::replica_host(None, "commonswiki", DbCluster::Links),
            "links.commonswiki.web.db.svc.wikimedia.cloud"
        );
        assert_eq!(
            ReplicaDb::replica_host(None, "testcommonswiki", DbCluster::Links),
            "links.testcommonswiki.web.db.svc.wikimedia.cloud"
        );
    }

    /// The split is Commons-only: asking for the links cluster of any other
    /// wiki must still resolve to that wiki's core host, so callers can name
    /// the cluster unconditionally.
    #[test]
    fn test_replica_host_ignores_links_cluster_for_unsplit_wiki() {
        assert_eq!(
            ReplicaDb::replica_host(None, "dewiki", DbCluster::Links),
            "dewiki.web.db.svc.wikimedia.cloud"
        );
    }

    #[test]
    fn test_replica_host_uses_the_tunnel_for_every_cluster() {
        for cluster in [DbCluster::Core, DbCluster::Links] {
            assert_eq!(
                ReplicaDb::replica_host(Some(LOCAL_HOST), "commonswiki", cluster),
                LOCAL_HOST
            );
        }
    }

    /// The page-finding query lands on the links cluster for Commons and on
    /// the core cluster everywhere else. `page` exists on both clusters, which
    /// is what keeps this a single-connection join after the split.
    #[test]
    fn test_page_template_query_cluster_per_wiki() {
        assert_eq!(
            DbCluster::for_tables("commonswiki", PAGE_TEMPLATE_TABLES).unwrap(),
            DbCluster::Links
        );
        assert_eq!(
            DbCluster::for_tables("enwiki", PAGE_TEMPLATE_TABLES).unwrap(),
            DbCluster::Core
        );
    }

    /// A join that reaches across the split cannot be served at all, and must
    /// surface as an error rather than as a silently missing table.
    #[test]
    fn test_query_spanning_clusters_is_rejected() {
        assert!(DbCluster::for_tables("commonswiki", &["actor", "templatelinks"]).is_err());
    }
}
