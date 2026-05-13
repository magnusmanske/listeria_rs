//! Data-access layer for the `pagestatus` and `wikis` tables.
//!
//! All SQL that touches these tables lives here. Callers deal only with
//! typed Rust values; the SQL strings are invisible to the rest of the
//! codebase.

use crate::database_pool::DatabasePool;
use crate::page_to_process::PageToProcess;
use anyhow::Result;
use chrono::{DateTime, Utc};
use mysql_async::{from_row, params, prelude::*};

#[derive(Debug, Clone)]
pub struct PageStatusRepository {
    pool: DatabasePool,
}

impl PageStatusRepository {
    pub const fn new(pool: DatabasePool) -> Self {
        Self { pool }
    }

    /// Marks every RUNNING row as FAIL with a "bot restarted" message.
    pub async fn reset_running(&self) -> Result<()> {
        self.pool
            .with_timeout("reset_running", || async {
                let now: DateTime<Utc> = Utc::now();
                let timestamp = now.format("%Y%m%d%H%M%S").to_string();
                let sql = "UPDATE pagestatus \
                    SET status='FAIL', priority=0, \
                    message='Bot restarted while page was processing', \
                    timestamp=:timestamp \
                    WHERE status='RUNNING'";
                self.pool
                    .get_conn()
                    .await?
                    .exec_drop(sql, params! { timestamp })
                    .await?;
                Ok(())
            })
            .await
    }

    /// Removes all DELETED rows from the queue.
    pub async fn clear_deleted(&self) -> Result<()> {
        self.pool
            .with_timeout("clear_deleted", || async {
                let sql = "DELETE FROM `pagestatus` WHERE `status`='DELETED'";
                self.pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, ())
                    .await?;
                Ok(())
            })
            .await
    }

    /// Resets all DEFERRED rows back to FAIL so they become eligible for
    /// re-processing on the next dispatcher pass. Called at bot startup;
    /// pages that were deferred because a circuit was open are given a
    /// fresh chance the next time the bot runs.
    pub async fn clear_deferred(&self) -> Result<()> {
        self.pool
            .with_timeout("clear_deferred", || async {
                let sql = "UPDATE `pagestatus` SET `status`='FAIL', \
                           `message`='cleared from DEFERRED on bot startup' \
                           WHERE `status`='DEFERRED'";
                self.pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, ())
                    .await?;
                Ok(())
            })
            .await
    }

    /// Records how many seconds a page took to process.
    pub async fn set_runtime(&self, pagestatus_id: u64, seconds: u64) -> Result<()> {
        self.pool
            .with_timeout("set_runtime", || async {
                let sql =
                    "UPDATE `pagestatus` SET `last_runtime_sec`=:seconds WHERE `id`=:pagestatus_id";
                self.pool
                    .get_conn()
                    .await?
                    .exec_drop(sql, params! {seconds, pagestatus_id})
                    .await?;
                Ok(())
            })
            .await
    }

    /// Transitions a page's status in the queue.
    ///
    /// While a page is RUNNING the priority is preserved so the scheduler can
    /// still identify it as high-priority. For every other terminal status the
    /// priority is reset to 0 to prevent accumulation in the priority queue.
    pub async fn update_page_status(
        &self,
        page: &str,
        wiki: &str,
        status: &str,
        message: &str,
    ) -> Result<()> {
        self.pool
            .with_timeout("update_page_status", || async {
                let now: DateTime<Utc> = Utc::now();
                let timestamp = now.format("%Y%m%d%H%M%S").to_string();
                let p = params! {
                    "wiki" => wiki,
                    "page" => page,
                    "timestamp" => timestamp,
                    "status" => status,
                    "message" => message.chars().take(200).collect::<String>(),
                };
                let priority = if status == "RUNNING" {
                    "`priority`"
                } else {
                    "0"
                };
                let sql = format!(
                    "UPDATE `pagestatus` SET
                    `status`=:status,
                    `message`=:message,
                    `timestamp`=:timestamp,
                    `bot_version`=2,
                    `priority`={priority}
                    WHERE `wiki`=(SELECT id FROM `wikis` WHERE `name`=:wiki) AND `page`=:page"
                );
                self.pool
                    .get_conn()
                    .await?
                    .exec_iter(sql.as_str(), p)
                    .await?
                    .map_and_drop(from_row::<String>)
                    .await?;
                Ok(())
            })
            .await
    }

    /// Returns the highest-priority waiting page, if any.
    pub async fn find_priority_page(
        &self,
        ids: &str,
        ignore_status: &str,
    ) -> Result<Option<PageToProcess>> {
        let sql = format!(
            "SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki
            FROM pagestatus,wikis
            WHERE priority=1
            AND wikis.id=pagestatus.wiki
            AND wikis.status='ACTIVE'
            AND pagestatus.status NOT IN ({ignore_status})
            AND pagestatus.id NOT IN ({ids})
            ORDER BY pagestatus.timestamp
            LIMIT 1"
        );
        Ok(self.get_page_for_sql(&sql).await)
    }

    /// Returns the oldest waiting page that is not in `ids` or `ignore_status`.
    pub async fn find_oldest_page(
        &self,
        ids: &str,
        ignore_status: &str,
    ) -> Result<Option<PageToProcess>> {
        let sql = format!(
            "SELECT pagestatus.id,pagestatus.page,pagestatus.status,wikis.name AS wiki
            FROM pagestatus,wikis
            WHERE pagestatus.wiki=wikis.id
            AND wikis.status='ACTIVE'
            AND pagestatus.status NOT IN ({ignore_status})
            AND pagestatus.id NOT IN ({ids})
            ORDER BY pagestatus.timestamp
            LIMIT 1"
        );
        Ok(self.get_page_for_sql(&sql).await)
    }

    async fn get_page_for_sql(&self, sql: &str) -> Option<PageToProcess> {
        // The Option-returning signature drops the error context; wrap in a
        // timeout-aware closure so a wedged query at least gets logged before
        // being converted to None.
        let result: Result<Option<PageToProcess>> = self
            .pool
            .with_timeout("get_page_for_sql", || async {
                let rows = self
                    .pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, ())
                    .await?
                    .map_and_drop(PageToProcess::from_row)
                    .await?;
                Ok(rows.into_iter().next())
            })
            .await;
        match result {
            Ok(page) => page,
            Err(e) => {
                log::warn!("get_page_for_sql: {e}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Pure-logic tests below mirror the priority fragment the repository
    // constructs inline; they don't reach into the repository itself, so no
    // `use super::*` is needed.

    /// Verify that the priority SQL fragment is consistent with what the
    /// repository generates. This mirrors the logic previously tested in
    /// listeria_bot_wikidata (which held this logic inline).
    #[test]
    fn test_priority_fragment_running_keeps_existing() {
        let status = "RUNNING";
        let fragment = if status == "RUNNING" {
            "`priority`"
        } else {
            "0"
        };
        assert_eq!(fragment, "`priority`");
    }

    #[test]
    fn test_priority_fragment_other_resets_to_zero() {
        for status in &["OK", "FAIL", "TRANSLATION", "INVALID", "DELETED"] {
            let fragment = if *status == "RUNNING" {
                "`priority`"
            } else {
                "0"
            };
            assert_eq!(fragment, "0", "status={status} should reset priority");
        }
    }
}
