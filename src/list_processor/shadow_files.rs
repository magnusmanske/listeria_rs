//! Shadow-file and excess-file processing.

use crate::listeria_list::ListeriaList;
use crate::result_cell_part::ResultCellPart;
use anyhow::Result;
use futures::future::join_all;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

impl super::ListProcessor {
    pub async fn process_remove_shadow_files(list: &mut ListeriaList) -> Result<()> {
        if !Self::check_this_wiki_for_shadow_images(list) {
            return Ok(());
        }
        let files_to_check = Self::get_files_to_check(list);
        list.shadow_files_mut().clear();
        let param_list = Self::get_param_list_for_files(list, &files_to_check);

        let api_results = Self::fetch_file_info(list, &param_list, files_to_check).await;
        let shadow_files = Self::identify_shadow_files(api_results);
        Self::remove_shadow_files_from_rows(list, &shadow_files);

        *list.shadow_files_mut() = shadow_files;

        Ok(())
    }

    pub fn process_excess_files(list: &mut ListeriaList) -> Result<()> {
        for row in list.results_mut().iter_mut() {
            row.remove_excess_files();
        }
        Ok(())
    }

    fn check_this_wiki_for_shadow_images(list: &ListeriaList) -> bool {
        list.page_params()
            .config()
            .check_for_shadow_images(list.wiki())
    }

    async fn fetch_file_info(
        list: &mut ListeriaList,
        param_list: &[HashMap<String, String>],
        files_to_check: Vec<String>,
    ) -> Vec<(String, Value)> {
        let page_params = list.page_params().clone();
        let api_read = page_params.mw_api();

        let mut futures = Vec::with_capacity(param_list.len());
        for params in param_list {
            futures.push(api_read.get_query_api_json(params));
        }
        list.profile(&format!(
            "ListProcessor::process_remove_shadow_files running {} futures",
            futures.len()
        ))
        .await;

        join_all(futures)
            .await
            .iter()
            .zip(files_to_check)
            .filter_map(|(result, filename)| match result {
                Ok(j) => Some((filename, j.to_owned())),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn identify_shadow_files(api_results: Vec<(String, Value)>) -> HashSet<String> {
        api_results
            .into_iter()
            .filter_map(|(filename, j)| {
                let could_be_local = j["query"]["pages"].as_object().map_or_else(
                    || true,
                    |results| {
                        results
                            .iter()
                            .filter_map(|(_k, o)| o["imagerepository"].as_str())
                            .any(|s| s != "shared")
                    },
                );
                could_be_local.then_some(filename)
            })
            .collect()
    }

    fn remove_shadow_files_from_rows(list: &mut ListeriaList, shadow_files: &HashSet<String>) {
        for row_id in 0..list.results().len() {
            let row = match list.results_mut().get_mut(row_id) {
                Some(row) => row,
                None => continue,
            };
            row.remove_shadow_files(shadow_files);
        }
    }

    fn get_files_to_check(list: &ListeriaList) -> Vec<String> {
        let mut files_to_check = Vec::new();
        for row in list.results().iter() {
            for cell in row.cells() {
                for part in cell.parts() {
                    if let ResultCellPart::File(file) = part.part() {
                        files_to_check.push(file.to_owned());
                    }
                }
            }
        }
        files_to_check.sort_unstable();
        files_to_check.dedup();
        files_to_check
    }

    fn get_param_list_for_files(
        list: &ListeriaList,
        files_to_check: &[String],
    ) -> Vec<HashMap<String, String>> {
        files_to_check
            .iter()
            .map(|filename| {
                let prefixed_filename = format!(
                    "{}:{}",
                    list.page_params().local_file_namespace_prefix(),
                    &filename
                );
                [
                    ("action", "query"),
                    ("titles", prefixed_filename.as_str()),
                    ("prop", "imageinfo"),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
            })
            .collect()
    }
}
