//! Data-access layer for the `wikis` and `pagestatus`-via-wikis tables.
//!
//! All SQL that the multi-wiki bot uses to discover and enroll wikis lives
//! here. Callers (currently `WikiApis`) deal in typed Rust values; the SQL
//! strings are invisible to the rest of the codebase.
//!
//! `PageStatusRepository` (see `pagestatus_repository.rs`) owns the SQL that
//! reads/writes individual rows in the per-page processing queue. The two
//! repositories share the same `DatabasePool` but are kept separate because
//! they answer different questions: "which wikis exist and what pages do they
//! list?" vs "what's the status of an individual queued page?".

use crate::database_pool::DatabasePool;
use crate::wiki::Wiki;
use anyhow::{Result, anyhow};
use mysql_async::{from_row, prelude::*};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct WikiRepository {
    pool: DatabasePool,
}

impl WikiRepository {
    #[must_use]
    pub const fn new(pool: DatabasePool) -> Self {
        Self { pool }
    }

    /// Returns every wiki currently registered in the bot's `wikis` table,
    /// keyed by name.
    pub async fn get_all_wikis(&self) -> Result<HashMap<String, Wiki>> {
        self.pool
            .with_timeout("get_all_wikis", || async {
                let rows = self
                    .pool
                    .get_conn()
                    .await?
                    .exec_iter(
                        "SELECT `id`,`name`,`status`,`timestamp`,`use_invoke`,`use_cite_web` FROM `wikis`",
                        (),
                    )
                    .await?
                    .map_and_drop(from_row::<(usize, String, String, String, bool, bool)>)
                    .await?;
                Ok(rows
                    .into_iter()
                    .map(Wiki::from_row)
                    .filter_map(|wiki| wiki.ok())
                    .map(|wiki| (wiki.name().to_string(), wiki))
                    .collect())
            })
            .await
    }

    /// Inserts wikis that are not already in the `wikis` table.
    ///
    /// Uses `INSERT IGNORE` so duplicate inserts are silently skipped — this
    /// keeps the call idempotent when the discovery loop sees a wiki that
    /// landed concurrently from another source.
    pub async fn add_wikis(&self, new_wikis: &[String]) -> Result<()> {
        if new_wikis.is_empty() {
            return Ok(());
        }
        self.pool
            .with_timeout("add_wikis", || async {
                let placeholders = std::iter::repeat_n("(?,'ACTIVE')", new_wikis.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql =
                    format!("INSERT IGNORE INTO `wikis` (`name`,`status`) VALUES {placeholders}");
                self.pool
                    .get_conn()
                    .await?
                    .exec_drop(sql, new_wikis.to_vec())
                    .await?;
                Ok(())
            })
            .await
    }

    /// Returns the numeric `wikis.id` for a wiki name, or an error if absent.
    pub async fn get_wiki_id(&self, wiki: &str) -> Result<u64> {
        self.pool
            .with_timeout("get_wiki_id", || async {
                self.pool
                    .get_conn()
                    .await?
                    .exec_iter("SELECT `id` FROM `wikis` WHERE `name`=?", (wiki,))
                    .await?
                    .map_and_drop(from_row::<u64>)
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("Wiki {wiki} not known"))
            })
            .await
    }

    /// Returns the full list of page titles currently enrolled in the queue
    /// for the given wiki.
    pub async fn get_pages_for_wiki(&self, wiki: &str) -> Result<Vec<String>> {
        self.pool
            .with_timeout("get_pages_for_wiki", || async {
                let sql = "SELECT `page` FROM pagestatus,wikis WHERE wikis.id=pagestatus.wiki AND wikis.name=?";
                Ok(self
                    .pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, (wiki,))
                    .await?
                    .map_and_drop(from_row::<String>)
                    .await?)
            })
            .await
    }

    /// Inserts new page rows into `pagestatus` in chunks of 10 000 to keep
    /// individual SQL statements within MySQL's max-packet limit.
    pub async fn add_pages_for_wiki(&self, wiki_id: u64, new_pages: &[String]) -> Result<()> {
        if new_pages.is_empty() {
            return Ok(());
        }
        // Each chunk gets its own timeout — a slow chunk shouldn't punish the
        // next chunk's budget, and a wedged chunk fails this call cleanly.
        for chunk in new_pages.chunks(10000) {
            let chunk: Vec<String> = chunk.to_vec();
            self.pool
                .with_timeout("add_pages_for_wiki_chunk", || async {
                    let element = format!("({wiki_id},?,'WAITING','','')");
                    let placeholders = std::iter::repeat_n(element.as_str(), chunk.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "INSERT IGNORE INTO `pagestatus` (`wiki`,`page`,`status`,`query_sparql`,`message`) VALUES {placeholders}"
                    );
                    self.pool
                        .get_conn()
                        .await?
                        .exec_drop(sql, chunk)
                        .await?;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // SQL methods need a live MySQL connection so they are exercised via the
    // ignored integration tests on `WikiApis`. The unit-level checks here
    // simply guard the construction signatures.

    use super::*;

    /// Ensures the repository type is `Clone + Debug` so it can be embedded
    /// in `WikiApis` (which is itself `Clone + Debug`) without breaking the
    /// auto-derives further up the stack.
    #[test]
    fn test_repository_is_clone_and_debug() {
        fn assert_clone_debug<T: Clone + std::fmt::Debug>() {}
        assert_clone_debug::<WikiRepository>();
    }
}
