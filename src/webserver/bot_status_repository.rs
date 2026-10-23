//! What `botstatus.php` shows of the queue (`pagestatus` and `wikis`).

use crate::database_pool::DatabasePool;
use anyhow::Result;
use wikimisc::mysql_async::{from_row, prelude::*};

/// How many pages of a wiki have a status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StatusCount {
    pub(super) wiki: String,
    pub(super) status: String,
    pub(super) pages: u64,
}

/// A page in the list of a wiki's pages with one status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PageRow {
    pub(super) page: String,
    pub(super) message: String,
    pub(super) timestamp: String,
    pub(super) runtime_sec: Option<i64>,
}

/// Everything the queue knows about a page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PageDetails {
    pub(super) page: String,
    pub(super) wiki: String,
    pub(super) status: String,
    pub(super) message: String,
    pub(super) timestamp: String,
    pub(super) runtime_sec: Option<i64>,
    pub(super) sparql: String,
    pub(super) priority: Option<i64>,
    pub(super) bot_version: Option<i64>,
}

type PageDetailsRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
);

impl From<PageDetailsRow> for PageDetails {
    fn from(row: PageDetailsRow) -> Self {
        let (page, wiki, status, message, timestamp, runtime_sec, sparql, priority, bot_version) =
            row;
        Self {
            page,
            wiki,
            status,
            message: message.unwrap_or_default(),
            timestamp: timestamp.unwrap_or_default(),
            runtime_sec,
            sparql: sparql.unwrap_or_default(),
            priority,
            bot_version,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct BotStatusRepository {
    pool: DatabasePool,
}

impl BotStatusRepository {
    pub(super) const fn new(pool: DatabasePool) -> Self {
        Self { pool }
    }

    /// The number of pages of each wiki and status.
    pub(super) async fn status_counts(&self) -> Result<Vec<StatusCount>> {
        self.pool
            .with_timeout("status_counts", || async {
                let sql = "SELECT wikis.name,pagestatus.status,COUNT(*) \
                    FROM pagestatus JOIN wikis ON wikis.id=pagestatus.wiki \
                    GROUP BY wikis.name,pagestatus.status";
                let rows = self
                    .pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, ())
                    .await?
                    .map_and_drop(from_row::<(String, String, u64)>)
                    .await?;
                Ok(rows
                    .into_iter()
                    .map(|(wiki, status, pages)| StatusCount {
                        wiki,
                        status,
                        pages,
                    })
                    .collect())
            })
            .await
    }

    /// The first `limit` pages of a wiki with a status, by title.
    pub(super) async fn pages(&self, wiki: &str, status: &str, limit: u64) -> Result<Vec<PageRow>> {
        self.pool
            .with_timeout("pages_with_status", || async {
                // `CAST`, as the text of a timestamp is wanted whatever its column type
                let sql = "SELECT pagestatus.page,pagestatus.message,CAST(pagestatus.timestamp AS CHAR),pagestatus.last_runtime_sec \
                    FROM pagestatus JOIN wikis ON wikis.id=pagestatus.wiki \
                    WHERE wikis.name=? AND pagestatus.status=? \
                    ORDER BY pagestatus.page LIMIT ?";
                let rows = self
                    .pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, (wiki, status, limit))
                    .await?
                    .map_and_drop(
                        from_row::<(String, Option<String>, Option<String>, Option<i64>)>,
                    )
                    .await?;
                Ok(rows
                    .into_iter()
                    .map(|(page, message, timestamp, runtime_sec)| PageRow {
                        page,
                        message: message.unwrap_or_default(),
                        timestamp: timestamp.unwrap_or_default(),
                        runtime_sec,
                    })
                    .collect())
            })
            .await
    }

    /// A page of a wiki, if it is in the queue.
    pub(super) async fn page(&self, wiki: &str, page: &str) -> Result<Option<PageDetails>> {
        self.pool
            .with_timeout("page_details", || async {
                let sql = "SELECT pagestatus.page,wikis.name,pagestatus.status,pagestatus.message,\
                    CAST(pagestatus.timestamp AS CHAR),pagestatus.last_runtime_sec,pagestatus.query_sparql,\
                    pagestatus.priority,pagestatus.bot_version \
                    FROM pagestatus JOIN wikis ON wikis.id=pagestatus.wiki \
                    WHERE wikis.name=? AND pagestatus.page=?";
                let rows = self
                    .pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, (wiki, page))
                    .await?
                    .map_and_drop(from_row::<PageDetailsRow>)
                    .await?;
                Ok(rows.into_iter().next().map(PageDetails::from))
            })
            .await
    }
}
