//! Result sorting.

use crate::listeria_list::ListeriaList;
use crate::template_params::{SortMode, SortOrder};
use anyhow::{Result, anyhow};
use futures::future::join_all;
use wikimisc::wikibase::SnakDataType;

impl super::ListProcessor {
    pub async fn process_sort_results(list: &mut ListeriaList) -> Result<()> {
        let sortkeys: Vec<String>;
        let mut datatype = SnakDataType::String;
        list.profile("BEFORE process_sort_results SORTKEYS").await;
        match list.template_params().sort() {
            SortMode::Label => {
                list.load_row_entities().await?;
                let mut futures = Vec::with_capacity(list.results().len());
                for row in list.results().iter() {
                    futures.push(row.get_sortkey_label(list));
                }
                sortkeys = join_all(futures).await.to_vec();
            }
            SortMode::FamilyName => {
                let mut futures = Vec::with_capacity(list.results().len());
                for row in list.results().iter() {
                    futures.push(row.get_sortkey_family_name(list));
                }
                sortkeys = join_all(futures).await.to_vec();
            }
            SortMode::Property(prop) => {
                datatype = list.ecw().get_datatype_for_property(prop).await;
                let mut futures = Vec::with_capacity(list.results().len());
                for row in list.results().iter() {
                    futures.push(row.get_sortkey_prop(prop, list, &datatype));
                }
                sortkeys = join_all(futures).await.to_vec();
            }
            SortMode::SparqlVariable(variable) => {
                sortkeys = list
                    .results()
                    .iter()
                    .map(|row| row.get_sortkey_sparql(variable, list))
                    .collect();
            }
            SortMode::None => return Ok(()),
        }
        list.profile("AFTER process_sort_results SORTKEYS").await;

        let ret = Self::process_sort_results_finish(list, sortkeys, datatype).await;
        list.profile("AFTER process_sort_results_finish").await;
        ret
    }

    pub(crate) async fn process_sort_results_finish(
        list: &mut ListeriaList,
        sortkeys: Vec<String>,
        datatype: SnakDataType,
    ) -> Result<()> {
        if list.results().len() != sortkeys.len() {
            return Err(anyhow!("process_sort_results: sortkeys length mismatch"));
        }

        for row_id in 0..list.results().len() {
            if let Some(row) = list.results_mut().get_mut(row_id)
                && let Some(sk) = sortkeys.get(row_id)
            {
                row.set_sortkey(sk.to_owned());
            };
        }

        list.profile("BEFORE process_sort_results_finish sort of items")
            .await;
        let descending = *list.template_params().sort_order() == SortOrder::Descending;
        let mut results = std::mem::take(list.results_mut());
        results = tokio::task::spawn_blocking(move || {
            results.sort_by(|a, b| a.compare_to(b, &datatype));
            if descending {
                results.reverse();
            }
            results
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking join error: {e}"))?;
        *list.results_mut() = results;
        list.profile("AFTER process_sort_results_finish sort").await;

        Ok(())
    }
}
