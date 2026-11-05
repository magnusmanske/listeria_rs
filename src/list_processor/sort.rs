//! Result sorting.

use crate::coop::ITEMS_PER_YIELD;
use crate::listeria_list::ListeriaList;
use crate::result_row::ResultRow;
use crate::template_params::{SortMode, SortOrder};
use anyhow::{Result, anyhow};
use futures::future::join_all;
use std::future::Future;
use wikimisc::wikibase::SnakDataType;

impl super::ListProcessor {
    pub async fn process_sort_results(list: &mut ListeriaList) -> Result<()> {
        let mut datatype = SnakDataType::String;
        list.profile("BEFORE process_sort_results SORTKEYS").await;
        let sortkeys: Vec<String> = match list.template_params().sort() {
            SortMode::Label => {
                list.load_row_entities().await?;
                Self::collect_sortkeys(list, |row| row.get_sortkey_label(list)).await
            }
            SortMode::FamilyName => {
                Self::collect_sortkeys(list, |row| row.get_sortkey_family_name(list)).await
            }
            SortMode::Property(prop) => {
                datatype = list.ecw().get_datatype_for_property(prop).await;
                Self::collect_sortkeys(list, |row| row.get_sortkey_prop(prop, list, &datatype))
                    .await
            }
            SortMode::SparqlVariable(variable) => list
                .results()
                .iter()
                .map(|row| row.get_sortkey_sparql(variable, list))
                .collect(),
            SortMode::None => return Ok(()),
        };
        list.profile("AFTER process_sort_results SORTKEYS").await;

        let descending = *list.template_params().sort_order() == SortOrder::Descending;
        list.profile("BEFORE process_sort_results_finish sort of items")
            .await;
        Self::apply_sort(list.results_mut(), sortkeys, descending, datatype).await?;
        list.profile("AFTER process_sort_results_finish sort").await;

        Ok(())
    }

    /// The sort key of every row, built in chunks with a yield between them.
    ///
    /// Building a key reads the entity cache and formats strings, so it is
    /// CPU work with no real await point: one `join_all` over every row would
    /// hold a runtime worker (and the telemetry push) for the whole list.
    async fn collect_sortkeys<'a, F, Fut>(list: &'a ListeriaList, build: F) -> Vec<String>
    where
        F: Fn(&'a ResultRow) -> Fut,
        Fut: Future<Output = String> + 'a,
    {
        let mut sortkeys = Vec::with_capacity(list.results().len());
        for chunk in list.results().chunks(ITEMS_PER_YIELD) {
            sortkeys.extend(join_all(chunk.iter().map(&build)).await);
            tokio::task::yield_now().await;
        }
        sortkeys
    }

    /// Applies sort keys and order to a result set — no list dependency.
    ///
    /// `results` and `sortkeys` must have the same length.
    pub(crate) async fn apply_sort(
        results: &mut Vec<ResultRow>,
        sortkeys: Vec<String>,
        descending: bool,
        datatype: SnakDataType,
    ) -> Result<()> {
        if results.len() != sortkeys.len() {
            return Err(anyhow!("process_sort_results: sortkeys length mismatch"));
        }

        for (row, sk) in results.iter_mut().zip(sortkeys.iter()) {
            row.set_sortkey(sk.to_owned());
        }

        let mut owned = std::mem::take(results);
        owned = tokio::task::spawn_blocking(move || {
            owned.sort_by(|a, b| a.compare_to(b, &datatype));
            if descending {
                owned.reverse();
            }
            owned
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking join error: {e}"))?;
        *results = owned;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Tests reference items by full path (`super::super::ListProcessor::…`)
    // and the SnakDataType / ResultRow types imported below, so there's no
    // need to glob-import the enclosing module.
    use crate::result_row::ResultRow;
    use wikimisc::wikibase::SnakDataType;

    fn rows_with_keys(keys: &[&str]) -> Vec<ResultRow> {
        keys.iter()
            .enumerate()
            .map(|(i, k)| {
                let mut r = ResultRow::new(&format!("Q{i}"));
                r.set_sortkey(k.to_string());
                r
            })
            .collect()
    }

    #[tokio::test]
    async fn test_apply_sort_ascending_string() {
        let keys = vec![
            "banana".to_string(),
            "apple".to_string(),
            "cherry".to_string(),
        ];
        let mut results = rows_with_keys(&["banana", "apple", "cherry"]);
        super::super::ListProcessor::apply_sort(&mut results, keys, false, SnakDataType::String)
            .await
            .unwrap();
        assert_eq!(results[0].sortkey(), "apple");
        assert_eq!(results[1].sortkey(), "banana");
        assert_eq!(results[2].sortkey(), "cherry");
    }

    #[tokio::test]
    async fn test_apply_sort_descending_string() {
        let keys = vec![
            "banana".to_string(),
            "apple".to_string(),
            "cherry".to_string(),
        ];
        let mut results = rows_with_keys(&["banana", "apple", "cherry"]);
        super::super::ListProcessor::apply_sort(&mut results, keys, true, SnakDataType::String)
            .await
            .unwrap();
        assert_eq!(results[0].sortkey(), "cherry");
        assert_eq!(results[1].sortkey(), "banana");
        assert_eq!(results[2].sortkey(), "apple");
    }

    #[tokio::test]
    async fn test_apply_sort_length_mismatch_returns_err() {
        let keys = vec!["a".to_string(), "b".to_string()];
        let mut results = rows_with_keys(&["a"]);
        let err = super::super::ListProcessor::apply_sort(
            &mut results,
            keys,
            false,
            SnakDataType::String,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("sortkeys length mismatch"));
    }

    #[tokio::test]
    async fn test_apply_sort_empty() {
        let mut results: Vec<ResultRow> = vec![];
        super::super::ListProcessor::apply_sort(&mut results, vec![], false, SnakDataType::String)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_apply_sort_time_numeric() {
        let keys = vec!["1900".to_string(), "33".to_string()];
        let mut results = rows_with_keys(&["1900", "33"]);
        super::super::ListProcessor::apply_sort(&mut results, keys, false, SnakDataType::Time)
            .await
            .unwrap();
        assert_eq!(results[0].sortkey(), "33");
        assert_eq!(results[1].sortkey(), "1900");
    }
}
