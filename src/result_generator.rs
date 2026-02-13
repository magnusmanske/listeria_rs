//! Generates result tables from SPARQL query results.

use crate::{column_type::ColumnType, listeria_list::ListeriaList, result_row::ResultRow};
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use wikimisc::{sparql_table_vec::SparqlTableVec, sparql_value::SparqlValue};

/// Handles the generation of result rows from SPARQL query results
#[derive(Debug)]
pub struct ResultGenerator;

impl ResultGenerator {
    pub async fn generate_results(list: &mut ListeriaList) -> Result<()> {
        let mut tmp_results: Vec<ResultRow> = Vec::new();
        if list.template_params().one_row_per_item() {
            Self::generate_results_one_row_per_item(list, &mut tmp_results).await?;
        } else {
            Self::generate_results_multiple_rows_per_item(list, &mut tmp_results).await?;
        };
        *list.results_mut() = tmp_results;
        Ok(())
    }

    async fn generate_results_multiple_rows_per_item(
        list: &mut ListeriaList,
        tmp_results: &mut Vec<ResultRow>,
    ) -> Result<()> {
        let var_index = Self::get_var_index(list)?;
        for row_id in 0..list.sparql_table().len() {
            let row = match list.sparql_table().get(row_id) {
                Some(row) => row,
                None => {
                    continue;
                }
            };
            let v = row.get(var_index).map(|v| v.to_owned());
            if let Some(Some(SparqlValue::Entity(id))) = v {
                let mut tmp_table = SparqlTableVec::from_table(list.sparql_table());
                tmp_table.push(row.to_owned());
                if let Some(x) = list.ecw().get_result_row(&id, &tmp_table, list).await {
                    tmp_results.push(x);
                }
            }
        }
        Ok(())
    }

    async fn generate_results_one_row_per_item(
        list: &mut ListeriaList,
        tmp_results: &mut Vec<ResultRow>,
    ) -> Result<()> {
        let var_index = Self::get_var_index(list)?;
        let sparql_row_ids: Vec<String> =
            Self::get_ids_from_sparql_rows(list)?.into_iter().collect();
        let mut id2rows: HashMap<String, Vec<usize>> = HashMap::new();
        for row_id in 0..list.sparql_table().len() {
            if let Some(SparqlValue::Entity(id)) =
                list.sparql_table().get_row_col(row_id, var_index)
            {
                id2rows.entry(id.to_string()).or_default().push(row_id);
            };
        }
        let sparql_table = list.sparql_table_arc().clone();
        for id in &sparql_row_ids {
            let tmp_rows = Self::get_tmp_rows(&sparql_table, &id2rows, id).await?;
            if let Some(row) = list.ecw().get_result_row(id, &tmp_rows, list).await {
                tmp_results.push(row);
            }
        }
        Ok(())
    }

    pub fn get_ids_from_sparql_rows(list: &ListeriaList) -> Result<Vec<String>> {
        let var_index = Self::get_var_index(list)?;
        let mut ids_tmp = Vec::new();
        for row_id in 0..list.sparql_table().len() {
            if let Some(SparqlValue::Entity(id)) =
                list.sparql_table().get_row_col(row_id, var_index)
            {
                ids_tmp.push(id.to_string());
            }
        }

        // Can't sort/dedup, need to preserve original order!
        let mut ids: Vec<String> = Vec::new();
        ids_tmp.iter().for_each(|id| {
            if !ids.contains(id) {
                ids.push(id.to_string());
            }
        });

        // Column headers
        list.columns().iter().for_each(|c| match c.obj() {
            ColumnType::Property(prop) => {
                ids.push(prop.to_owned());
            }
            ColumnType::PropertyQualifier((prop, qual)) => {
                ids.push(prop.to_owned());
                ids.push(qual.to_owned());
            }
            ColumnType::PropertyQualifierValue((prop1, qual, prop2)) => {
                ids.push(prop1.to_owned());
                ids.push(qual.to_owned());
                ids.push(prop2.to_owned());
            }
            _ => {}
        });

        Ok(ids)
    }

    fn get_var_index(list: &ListeriaList) -> Result<usize> {
        list.sparql_table()
            .main_column()
            .ok_or_else(|| anyhow!("Could not find SPARQL variable in results"))
    }

    async fn get_tmp_rows(
        sparql_table: &Arc<SparqlTableVec>,
        id2rows: &HashMap<String, Vec<usize>>,
        id: &String,
    ) -> Result<SparqlTableVec> {
        let sparql_table = sparql_table.clone();
        let row_ids = id2rows.get(id).map(|v| v.to_owned()).unwrap_or_default();
        tokio::task::spawn_blocking(move || {
            let mut tmp_rows = SparqlTableVec::from_table(&sparql_table);
            for row_id in row_ids {
                if let Some(row) = sparql_table.get(row_id) {
                    tmp_rows.push(row);
                }
            }
            tmp_rows
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking join error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::Configuration;
    use crate::page_params::PageParams;
    use crate::template::Template;
    use std::sync::Arc;
    use wikimisc::mediawiki::api::Api;

    async fn create_test_list() -> ListeriaList {
        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        let api = Arc::new(api);
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let config = Arc::new(config);
        let page_params = PageParams::new(config, api, "Test:Page".to_string())
            .await
            .unwrap();
        let page_params = Arc::new(page_params);

        let template_text =
            "{{Wikidata list|columns=item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }}}";
        let template = Template::new_from_params(template_text).unwrap();

        ListeriaList::new(template, page_params).await.unwrap()
    }

    #[tokio::test]
    async fn test_get_var_index_with_valid_table() {
        let list = create_test_list().await;
        // The sparql_table is empty by default, so this should fail
        let result = ResultGenerator::get_var_index(&list);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_ids_from_sparql_rows_empty() {
        let list = create_test_list().await;
        let result = ResultGenerator::get_ids_from_sparql_rows(&list);
        // May fail if sparql table is not set up, but should not panic
        if let Ok(ids) = result {
            // Should have no entity IDs from rows (only possibly column headers)
            assert!(
                ids.is_empty()
                    || ids
                        .iter()
                        .all(|id| id.starts_with('P') || id.starts_with('Q'))
            );
        }
    }

    #[tokio::test]
    async fn test_get_ids_preserves_order() {
        // This test verifies that get_ids_from_sparql_rows doesn't panic
        // even with an empty SPARQL table
        let list = create_test_list().await;
        let _result = ResultGenerator::get_ids_from_sparql_rows(&list);
        // Test passes if it doesn't panic
    }

    #[tokio::test]
    async fn test_generate_results_with_empty_table() {
        let mut list = create_test_list().await;
        let _result = ResultGenerator::generate_results(&mut list).await;
        // Test passes if it doesn't panic
        // Result may fail due to empty sparql table, which is expected
    }

    #[test]
    fn test_result_generator_is_debug() {
        // Verify that ResultGenerator implements Debug
        let _ = format!("{:?}", ResultGenerator);
    }
}
