//! The dynamic lists of `dynamic.html` (the `list` table of their own
//! database).

use crate::database_pool::DatabasePool;
use anyhow::Result;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::LazyLock;
use wikimisc::mysql_async::{self, from_row, prelude::*};

const COLUMNS: &str = "id,name,owner,wdq,config,items,number_of_items,\
    CAST(created AS CHAR),CAST(last_update AS CHAR),sparql";

static RE_ITEM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[qQ](\d+)$").expect("valid regex"));

/// A list, as `api.php` returns it.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub(super) struct DynamicList {
    id: u64,
    name: String,
    owner: String,
    /// The query of the list in WDQ, Wikidata's query language before SPARQL
    wdq: String,
    /// How the list is shown (columns, sections, …), set by `dynamic.html`
    config: Value,
    /// The numbers of the list's items, with a comma before and after each
    /// (`,42,1337,`), so `LIKE '%,42,%'` finds the lists of Q42
    items: String,
    number_of_items: u64,
    created: String,
    last_update: String,
    sparql: String,
}

impl DynamicList {
    pub(super) fn owner(&self) -> &str {
        &self.owner
    }

    pub(super) fn sparql(&self) -> &str {
        &self.sparql
    }
}

type Row = (
    u64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<u64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

impl From<Row> for DynamicList {
    fn from(row: Row) -> Self {
        let (id, name, owner, wdq, config, items, number_of_items, created, last_update, sparql) =
            row;
        Self {
            id,
            name: name.unwrap_or_default(),
            owner: owner.unwrap_or_default(),
            wdq: wdq.unwrap_or_default(),
            config: config
                .and_then(|config| serde_json::from_str(&config).ok())
                .unwrap_or_default(),
            items: items.unwrap_or_default(),
            number_of_items: number_of_items.unwrap_or_default(),
            created: created.unwrap_or_default(),
            last_update: last_update.unwrap_or_default(),
            sparql: sparql.unwrap_or_default(),
        }
    }
}

/// A list as `dynamic.html` saves it (`set_list`); its other fields, the
/// owner among them, are ignored.
#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct ListInput {
    /// Missing for a new list. A number, or a number in a string.
    #[serde(default)]
    id: Value,
    #[serde(default)]
    name: String,
    #[serde(default)]
    sparql: String,
    #[serde(default)]
    config: Value,
}

impl ListInput {
    pub(super) fn id(&self) -> Option<u64> {
        match &self.id {
            Value::Number(number) => number.as_u64(),
            Value::String(text) => text.trim().parse().ok(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DynamicListRepository {
    pool: DatabasePool,
}

impl DynamicListRepository {
    pub(super) const fn new(pool: DatabasePool) -> Self {
        Self { pool }
    }

    pub(super) async fn get(&self, id: u64) -> Result<Option<DynamicList>> {
        let lists = self
            .select("get_list", "WHERE id=? LIMIT 1", vec![id.into()])
            .await?;
        Ok(lists.into_iter().next())
    }

    /// The `limit` lists created (or saved) last.
    pub(super) async fn latest(&self, limit: u64) -> Result<Vec<DynamicList>> {
        self.select(
            "latest_lists",
            "ORDER BY created DESC LIMIT ?",
            vec![limit.into()],
        )
        .await
    }

    /// The lists of a user, the latest first.
    pub(super) async fn of_owner(&self, owner: &str) -> Result<Vec<DynamicList>> {
        self.select(
            "lists_of_owner",
            "WHERE owner=? ORDER BY created DESC",
            vec![owner.into()],
        )
        .await
    }

    /// Up to `limit` lists matching each word of `text`: in their name or
    /// owner, or as an item (`Q42`) in them.
    pub(super) async fn search(&self, text: &str, limit: u64) -> Result<Vec<DynamicList>> {
        let Some((condition, mut params)) = search_condition(text) else {
            return Ok(vec![]);
        };
        params.push(limit.into());
        self.select(
            "search_lists",
            &format!("WHERE {condition} LIMIT ?"),
            params,
        )
        .await
    }

    /// Saves a new list of `owner`; returns its ID.
    pub(super) async fn create(
        &self,
        list: &ListInput,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<u64> {
        self.pool
            .with_timeout("create_list", || async {
                let sql = "INSERT INTO list (name,sparql,config,owner,created,items,number_of_items) \
                    VALUES (?,?,?,?,?,'',0)";
                let params = (
                    &list.name,
                    &list.sparql,
                    list.config.to_string(),
                    owner,
                    db_timestamp(now),
                );
                let mut conn = self.pool.get_conn().await?;
                conn.exec_drop(sql, params).await?;
                conn.last_insert_id()
                    .ok_or_else(|| anyhow::anyhow!("No ID for the new list"))
            })
            .await
    }

    /// Saves list `id`.
    pub(super) async fn update(&self, id: u64, list: &ListInput, now: DateTime<Utc>) -> Result<()> {
        self.pool
            .with_timeout("update_list", || async {
                let sql = "UPDATE list SET name=?,sparql=?,config=?,created=? WHERE id=?";
                let params = (
                    &list.name,
                    &list.sparql,
                    list.config.to_string(),
                    db_timestamp(now),
                    id,
                );
                self.pool.get_conn().await?.exec_drop(sql, params).await?;
                Ok(())
            })
            .await
    }

    /// Sets the items of list `id`.
    pub(super) async fn set_items(&self, id: u64, items: &[u64], now: DateTime<Utc>) -> Result<()> {
        self.pool
            .with_timeout("set_list_items", || async {
                let sql = "UPDATE list SET items=?,number_of_items=?,last_update=? WHERE id=?";
                let params = (items_column(items), items.len(), db_timestamp(now), id);
                self.pool.get_conn().await?.exec_drop(sql, params).await?;
                Ok(())
            })
            .await
    }

    async fn select(
        &self,
        operation: &str,
        clauses: &str,
        params: Vec<mysql_async::Value>,
    ) -> Result<Vec<DynamicList>> {
        self.pool
            .with_timeout(operation, || async {
                let sql = format!("SELECT {COLUMNS} FROM list {clauses}");
                let rows = self
                    .pool
                    .get_conn()
                    .await?
                    .exec_iter(sql, params)
                    .await?
                    .map_and_drop(from_row::<Row>)
                    .await?;
                Ok(rows.into_iter().map(DynamicList::from).collect())
            })
            .await
    }
}

/// The SQL condition (and its parameters) of a search for lists matching
/// each word of `text`; `None` without words.
fn search_condition(text: &str) -> Option<(String, Vec<mysql_async::Value>)> {
    let mut conditions = vec![];
    let mut params: Vec<mysql_async::Value> = vec![];
    for word in text.split_whitespace() {
        match RE_ITEM.captures(word) {
            Some(captures) => {
                conditions.push("items LIKE ?");
                params.push(format!("%,{},%", &captures[1]).into());
            }
            None => {
                let pattern = format!("%{}%", escape_like(word));
                conditions.push("(name LIKE ? OR owner LIKE ?)");
                params.push(pattern.clone().into());
                params.push(pattern.into());
            }
        }
    }
    (!conditions.is_empty()).then(|| (conditions.join(" AND "), params))
}

/// Text matched literally by `LIKE`.
fn escape_like(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn items_column(items: &[u64]) -> String {
    if items.is_empty() {
        return String::new();
    }
    let items: Vec<String> = items.iter().map(u64::to_string).collect();
    format!(",{},", items.join(","))
}

/// A time as the list table has it: the 20 characters its columns keep of
/// PHP's `date('c')` (`2023-03-31T09:10:08+`), so lists still sort by it.
fn db_timestamp(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%S+").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn test_search_condition() {
        let (condition, params) = search_condition("  Q42 cats\t50%_ ").unwrap();
        assert_eq!(
            condition,
            "items LIKE ? AND (name LIKE ? OR owner LIKE ?) AND (name LIKE ? OR owner LIKE ?)"
        );
        let params: Vec<mysql_async::Value> = params;
        assert_eq!(
            params,
            vec![
                "%,42,%".into(),
                "%cats%".into(),
                "%cats%".into(),
                "%50\\%\\_%".into(),
                "%50\\%\\_%".into(),
            ]
        );
        assert!(search_condition(" \n ").is_none());
    }

    #[test]
    fn test_items_column() {
        assert_eq!(items_column(&[]), "");
        assert_eq!(items_column(&[42]), ",42,");
        assert_eq!(items_column(&[42, 1337]), ",42,1337,");
    }

    #[test]
    fn test_db_timestamp() {
        let time = Utc.with_ymd_and_hms(2023, 3, 31, 9, 10, 8).unwrap();
        assert_eq!(db_timestamp(time), "2023-03-31T09:10:08+");
    }

    #[test]
    fn test_list_input_id() {
        let numeric: ListInput = serde_json::from_value(json!({"id": 419})).unwrap();
        assert_eq!(numeric.id(), Some(419));
        let text: ListInput = serde_json::from_value(json!({"id": "419"})).unwrap();
        assert_eq!(text.id(), Some(419));
        let input: ListInput =
            serde_json::from_value(json!({"name": "x", "owner": "Someone", "items": ["Q1"]}))
                .unwrap();
        assert_eq!(input.id(), None);
        assert_eq!(input.name, "x");
    }

    #[test]
    fn test_list_from_row() {
        let row: Row = (
            419,
            Some("Science".to_string()),
            Some("Dnshitobu".to_string()),
            None,
            Some(r#"{"table":"tabbed"}"#.to_string()),
            Some(",1,2,".to_string()),
            Some(2),
            Some("2023-03-31T09:10:08+".to_string()),
            None,
            Some("SELECT ?item {}".to_string()),
        );
        let list = DynamicList::from(row);
        let json = serde_json::to_value(&list).unwrap();
        assert_eq!(json["id"], 419);
        assert_eq!(json["config"]["table"], "tabbed");
        assert_eq!(json["items"], ",1,2,");
        assert_eq!(json["wdq"], "");
        assert_eq!(json["last_update"], "");
        assert_eq!(list.owner(), "Dnshitobu");
    }
}
