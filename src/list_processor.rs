//! Result processing and entity resolution.
//!
//! Transforms SPARQL results into structured data with resolved entities and references.
//! Processing stages are organised into sub-modules by concern:
//!
//! - `shadow_files` — shadow-image and excess-file removal
//! - `links`        — redlinks, local links, link target fixing
//! - `sort`         — result sorting
//! - `sections`     — section assignment
//! - `autodesc`     — autodesc description gathering
//! - `regions`      — geographic region detection and location naming
//! - `references`   — stated-in reference loading

mod autodesc;
mod links;
mod references;
mod regions;
mod sections;
mod shadow_files;
mod sort;

/// Handles the processing of result data for ListeriaList.
///
/// This is a zero-field marker struct used as a namespace for the processing
/// pipeline stages.  Each stage is a free `async fn` that takes `&mut ListeriaList`,
/// performs its transformation, and returns `Result<()>`.
#[derive(Debug, Clone, Copy)]
pub struct ListProcessor;

#[cfg(test)]
mod tests {
    use super::ListProcessor;
    use crate::configuration::Configuration;
    use crate::listeria_list::ListeriaList;
    use crate::page_params::PageParams;
    use crate::template::Template;
    use std::sync::Arc;

    async fn create_test_list() -> ListeriaList {
        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
        let page_params = PageParams::new(config, api, "Test:Page".to_string())
            .await
            .unwrap();
        let page_params = Arc::new(page_params);

        let template_text =
            "{{Wikidata list|columns=item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }}}";
        let template = Template::new_from_params(template_text).unwrap();

        ListeriaList::new(template, page_params).await.unwrap()
    }

    #[test]
    fn test_list_processor_is_debug() {
        let _ = format!("{:?}", ListProcessor);
    }

    #[tokio::test]
    async fn test_process_excess_files_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_excess_files(&mut list);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_items_to_local_links_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_items_to_local_links(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_redlinks_only_not_redonly() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_redlinks_only(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_sort_results_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_sort_results(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_reference_items_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_reference_items(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fix_local_links_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::fix_local_links(&mut list);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fill_autodesc_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::fill_autodesc(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_assign_sections_none() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_assign_sections(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_regions_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_regions(&mut list).await;
        assert!(result.is_ok());
    }

    // ── build_section_count ──────────────────────────────────────────────────

    #[test]
    fn test_build_section_count_empty() {
        let names: Vec<String> = vec![];
        let count = ListProcessor::build_section_count(&names);
        assert!(count.is_empty());
    }

    #[test]
    fn test_build_section_count_single() {
        let names = vec!["human".to_string()];
        let count = ListProcessor::build_section_count(&names);
        assert_eq!(count.len(), 1);
        assert_eq!(count[&"human".to_string()], 1);
    }

    #[test]
    fn test_build_section_count_multiple_same() {
        let names = vec![
            "human".to_string(),
            "human".to_string(),
            "human".to_string(),
        ];
        let count = ListProcessor::build_section_count(&names);
        assert_eq!(count.len(), 1);
        assert_eq!(count[&"human".to_string()], 3);
    }

    #[test]
    fn test_build_section_count_multiple_different() {
        let names = vec![
            "human".to_string(),
            "sovereign state".to_string(),
            "human".to_string(),
            "city".to_string(),
            "sovereign state".to_string(),
            "sovereign state".to_string(),
        ];
        let count = ListProcessor::build_section_count(&names);
        assert_eq!(count.len(), 3);
        assert_eq!(count[&"human".to_string()], 2);
        assert_eq!(count[&"sovereign state".to_string()], 3);
        assert_eq!(count[&"city".to_string()], 1);
    }

    // ── build_valid_section_names ────────────────────────────────────────────

    #[test]
    fn test_build_valid_section_names_all_qualify() {
        let names = vec![
            "human".to_string(),
            "human".to_string(),
            "state".to_string(),
            "state".to_string(),
        ];
        let count = ListProcessor::build_section_count(&names);
        let valid = ListProcessor::build_valid_section_names(count, 1);
        assert_eq!(valid, vec!["human".to_string(), "state".to_string()]);
    }

    #[test]
    fn test_build_valid_section_names_filters_below_min() {
        let names = vec![
            "human".to_string(),
            "human".to_string(),
            "human".to_string(),
            "rare".to_string(),
        ];
        let count = ListProcessor::build_section_count(&names);
        let valid = ListProcessor::build_valid_section_names(count, 2);
        assert_eq!(valid, vec!["human".to_string()]);
        assert!(!valid.contains(&"rare".to_string()));
    }

    #[test]
    fn test_build_valid_section_names_none_qualify() {
        let names = vec!["human".to_string(), "state".to_string()];
        let count = ListProcessor::build_section_count(&names);
        let valid = ListProcessor::build_valid_section_names(count, 5);
        assert!(valid.is_empty());
    }

    #[test]
    fn test_build_valid_section_names_output_is_sorted() {
        let names = vec![
            "zebra".to_string(),
            "zebra".to_string(),
            "apple".to_string(),
            "apple".to_string(),
            "mango".to_string(),
            "mango".to_string(),
        ];
        let count = ListProcessor::build_section_count(&names);
        let valid = ListProcessor::build_valid_section_names(count, 1);
        assert_eq!(
            valid,
            vec![
                "apple".to_string(),
                "mango".to_string(),
                "zebra".to_string()
            ]
        );
    }

    #[test]
    fn test_build_valid_section_names_excludes_empty_strings() {
        let names = vec![
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "human".to_string(),
            "human".to_string(),
        ];
        let count = ListProcessor::build_section_count(&names);
        let valid = ListProcessor::build_valid_section_names(count, 1);
        assert_eq!(valid, vec!["human".to_string()]);
        assert!(!valid.contains(&"".to_string()));
    }

    #[test]
    fn test_build_valid_section_names_excludes_whitespace_only() {
        let names = vec![
            "  ".to_string(),
            "  ".to_string(),
            "human".to_string(),
            "human".to_string(),
        ];
        let count = ListProcessor::build_section_count(&names);
        let valid = ListProcessor::build_valid_section_names(count, 1);
        assert_eq!(valid, vec!["human".to_string()]);
    }

    // ── create_section_mappings ──────────────────────────────────────────────

    #[test]
    fn test_create_section_mappings_misc_always_appended() {
        let valid = vec!["human".to_string(), "state".to_string()];
        let (name2id, id2name, misc_id) = ListProcessor::create_section_mappings(valid);
        assert!(name2id.contains_key("Misc"));
        assert!(id2name.values().any(|v| v == "Misc"));
        assert_eq!(misc_id, 2);
        assert_eq!(id2name[&misc_id], "Misc");
    }

    #[test]
    fn test_create_section_mappings_empty_input() {
        let (name2id, id2name, misc_id) = ListProcessor::create_section_mappings(vec![]);
        assert_eq!(name2id.len(), 1);
        assert_eq!(id2name.len(), 1);
        assert_eq!(misc_id, 0);
        assert_eq!(name2id["Misc"], 0);
    }

    #[test]
    fn test_create_section_mappings_bidirectional_consistency() {
        let valid = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        let (name2id, id2name, _misc_id) = ListProcessor::create_section_mappings(valid);
        for (name, id) in &name2id {
            assert_eq!(&id2name[id], name);
        }
        for (id, name) in &id2name {
            assert_eq!(&name2id[name], id);
        }
    }

    #[test]
    fn test_create_section_mappings_ids_are_unique() {
        let valid = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (name2id, _id2name, _misc_id) = ListProcessor::create_section_mappings(valid);
        let mut ids: Vec<usize> = name2id.values().cloned().collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), name2id.len());
    }

    // ── assign_row_section_ids ───────────────────────────────────────────────

    #[test]
    fn test_assign_row_section_ids_maps_correctly() {
        use crate::result_row::ResultRow;

        let valid = vec!["alpha".to_string(), "beta".to_string()];
        let (name2id, id2name, misc_id) = ListProcessor::create_section_mappings(valid);

        let mut rows = [
            ResultRow::new("Q1"),
            ResultRow::new("Q2"),
            ResultRow::new("Q3"),
        ];

        let section_names = ["alpha".to_string(), "beta".to_string(), "alpha".to_string()];

        for (row_id, row) in rows.iter_mut().enumerate() {
            let section_name = &section_names[row_id];
            let section_id = name2id.get(section_name).copied().unwrap_or(misc_id);
            row.set_section(section_id);
        }

        let alpha_id = name2id["alpha"];
        let beta_id = name2id["beta"];
        assert_eq!(rows[0].section(), alpha_id);
        assert_eq!(rows[1].section(), beta_id);
        assert_eq!(rows[2].section(), alpha_id);
        assert_eq!(id2name[&alpha_id], "alpha");
        assert_eq!(id2name[&beta_id], "beta");
    }

    #[test]
    fn test_assign_row_section_ids_unknown_name_maps_to_misc() {
        use crate::result_row::ResultRow;

        let valid = vec!["human".to_string()];
        let (name2id, _id2name, misc_id) = ListProcessor::create_section_mappings(valid);

        let mut row = ResultRow::new("Q99");
        let section_id = name2id.get("unknown").copied().unwrap_or(misc_id);
        row.set_section(section_id);
        assert_eq!(row.section(), misc_id);
    }

    #[test]
    fn test_assign_row_section_ids_empty_rows_noop() {
        use crate::result_row::ResultRow;

        let valid = vec!["human".to_string()];
        let (name2id, _id2name, misc_id) = ListProcessor::create_section_mappings(valid);
        let section_names: Vec<String> = vec![];
        let mut rows: Vec<ResultRow> = vec![];

        for (row_id, row) in rows.iter_mut().enumerate() {
            let section_name = &section_names[row_id];
            let section_id = name2id.get(section_name).copied().unwrap_or(misc_id);
            row.set_section(section_id);
        }
        assert!(rows.is_empty());
    }

    // ── identify_shadow_files ────────────────────────────────────────────────

    #[test]
    fn test_identify_shadow_files_shared_file_excluded() {
        let api_results = vec![(
            "Example.jpg".to_string(),
            serde_json::json!({
                "query": {
                    "pages": {
                        "1": { "imagerepository": "shared" }
                    }
                }
            }),
        )];
        let shadows = ListProcessor::identify_shadow_files(api_results);
        assert!(!shadows.contains("Example.jpg"));
    }

    #[test]
    fn test_identify_shadow_files_local_file_included() {
        let api_results = vec![(
            "LocalOverride.jpg".to_string(),
            serde_json::json!({
                "query": {
                    "pages": {
                        "2": { "imagerepository": "local" }
                    }
                }
            }),
        )];
        let shadows = ListProcessor::identify_shadow_files(api_results);
        assert!(shadows.contains("LocalOverride.jpg"));
    }

    #[test]
    fn test_identify_shadow_files_missing_query_pages_assumed_local() {
        let api_results = vec![(
            "Unknown.jpg".to_string(),
            serde_json::json!({ "query": {} }),
        )];
        let shadows = ListProcessor::identify_shadow_files(api_results);
        assert!(shadows.contains("Unknown.jpg"));
    }

    #[test]
    fn test_identify_shadow_files_empty_input() {
        let shadows = ListProcessor::identify_shadow_files(vec![]);
        assert!(shadows.is_empty());
    }

    #[test]
    fn test_identify_shadow_files_mixed() {
        let api_results = vec![
            (
                "Shadow.jpg".to_string(),
                serde_json::json!({
                    "query": { "pages": { "1": { "imagerepository": "local" } } }
                }),
            ),
            (
                "Fine.jpg".to_string(),
                serde_json::json!({
                    "query": { "pages": { "2": { "imagerepository": "shared" } } }
                }),
            ),
        ];
        let shadows = ListProcessor::identify_shadow_files(api_results);
        assert!(shadows.contains("Shadow.jpg"));
        assert!(!shadows.contains("Fine.jpg"));
    }

    // ── collect_stated_in_from_part ──────────────────────────────────────────

    #[test]
    fn test_collect_stated_in_from_part_with_stated_in() {
        use crate::reference::Reference;
        use crate::result_cell_part::{PartWithReference, ResultCellPart};
        use wikimisc::wikibase::Snak;

        let snaks = vec![Snak::new_item("P248", "Q36578")];
        let reference = Reference::new_from_snaks(&snaks, "en").unwrap();
        let part = PartWithReference::new(ResultCellPart::Text("x".into()), Some(vec![reference]));
        let mut items: Vec<String> = vec![];
        ListProcessor::collect_stated_in_from_part(&part, &mut items);
        assert_eq!(items, vec!["Q36578".to_string()]);
    }

    #[test]
    fn test_collect_stated_in_from_part_no_stated_in() {
        use crate::reference::Reference;
        use crate::result_cell_part::{PartWithReference, ResultCellPart};
        use wikimisc::wikibase::Snak;

        let snaks = vec![Snak::new_string("P854", "https://example.com")];
        let reference = Reference::new_from_snaks(&snaks, "en").unwrap();
        let part = PartWithReference::new(ResultCellPart::Text("x".into()), Some(vec![reference]));
        let mut items: Vec<String> = vec![];
        ListProcessor::collect_stated_in_from_part(&part, &mut items);
        assert!(items.is_empty());
    }

    #[test]
    fn test_collect_stated_in_from_part_no_references() {
        use crate::result_cell_part::{PartWithReference, ResultCellPart};

        let part = PartWithReference::new(ResultCellPart::Text("x".into()), None);
        let mut items: Vec<String> = vec![];
        ListProcessor::collect_stated_in_from_part(&part, &mut items);
        assert!(items.is_empty());
    }

    #[test]
    fn test_collect_stated_in_from_part_multiple_references() {
        use crate::reference::Reference;
        use crate::result_cell_part::{PartWithReference, ResultCellPart};
        use wikimisc::wikibase::Snak;

        let refs = vec![
            Reference::new_from_snaks(&[Snak::new_item("P248", "Q1")], "en").unwrap(),
            Reference::new_from_snaks(&[Snak::new_item("P248", "Q2")], "en").unwrap(),
            Reference::new_from_snaks(&[Snak::new_string("P854", "https://example.com")], "en")
                .unwrap(),
        ];
        let part = PartWithReference::new(ResultCellPart::Text("x".into()), Some(refs));
        let mut items: Vec<String> = vec![];
        ListProcessor::collect_stated_in_from_part(&part, &mut items);
        assert_eq!(items.len(), 2);
        assert!(items.contains(&"Q1".to_string()));
        assert!(items.contains(&"Q2".to_string()));
    }

    // ── process_sort_results_finish ──────────────────────────────────────────

    #[tokio::test]
    async fn test_process_sort_results_finish_sorts_ascending() {
        use crate::result_row::ResultRow;
        use wikimisc::wikibase::SnakDataType;

        let mut list = create_test_list().await;

        let mut r1 = ResultRow::new("Q3");
        r1.set_sortkey("charlie".to_string());
        let mut r2 = ResultRow::new("Q1");
        r2.set_sortkey("alpha".to_string());
        let mut r3 = ResultRow::new("Q2");
        r3.set_sortkey("bravo".to_string());
        *list.results_mut() = vec![r1, r2, r3];

        ListProcessor::process_sort_results_finish(
            &mut list,
            vec![
                "charlie".to_string(),
                "alpha".to_string(),
                "bravo".to_string(),
            ],
            SnakDataType::String,
        )
        .await
        .unwrap();

        let ids: Vec<&str> = list.results().iter().map(|r| r.entity_id()).collect();
        assert_eq!(ids, vec!["Q1", "Q2", "Q3"]);
    }

    #[tokio::test]
    async fn test_process_sort_results_finish_length_mismatch_errors() {
        use crate::result_row::ResultRow;
        use wikimisc::wikibase::SnakDataType;

        let mut list = create_test_list().await;
        *list.results_mut() = vec![ResultRow::new("Q1"), ResultRow::new("Q2")];

        let result = ListProcessor::process_sort_results_finish(
            &mut list,
            vec!["only_one".to_string()],
            SnakDataType::String,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_sort_results_finish_sorts_descending_via_sort_order() {
        use crate::page_params::PageParams;
        use crate::result_row::ResultRow;
        use crate::template::Template;
        use std::sync::Arc;
        use wikimisc::wikibase::SnakDataType;

        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
        let page_params = Arc::new(
            PageParams::new(config, api, "Test:Page".to_string())
                .await
                .unwrap(),
        );
        let template = Template::new_from_params(
            "sort_order=desc|columns=item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }}",
        )
        .unwrap();
        let mut list = ListeriaList::new(template, page_params).await.unwrap();
        list.process_template().unwrap();

        let mut r1 = ResultRow::new("Q1");
        r1.set_sortkey("alpha".to_string());
        let mut r2 = ResultRow::new("Q2");
        r2.set_sortkey("bravo".to_string());
        let mut r3 = ResultRow::new("Q3");
        r3.set_sortkey("charlie".to_string());
        *list.results_mut() = vec![r1, r2, r3];

        ListProcessor::process_sort_results_finish(
            &mut list,
            vec![
                "alpha".to_string(),
                "bravo".to_string(),
                "charlie".to_string(),
            ],
            SnakDataType::String,
        )
        .await
        .unwrap();

        let ids: Vec<&str> = list.results().iter().map(|r| r.entity_id()).collect();
        assert_eq!(ids, vec!["Q3", "Q2", "Q1"]);
    }

    // ── collect_entity_ids_from_results ─────────────────────────────────────

    #[test]
    fn test_collect_entity_ids_from_results_empty() {
        use crate::result_row::ResultRow;

        let rows: Vec<ResultRow> = vec![];
        let mut ids: Vec<String> = Vec::new();
        for row in rows.iter() {
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let crate::result_cell_part::ResultCellPart::Entity(entity_info) =
                        part.part()
                        && entity_info.try_localize
                    {
                        ids.push(entity_info.id.to_owned());
                    }
                });
            });
        }
        assert!(ids.is_empty());
    }

    // ── set_keep_flags ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_set_keep_flags_applies_to_all_rows() {
        use crate::result_row::ResultRow;

        let mut list = create_test_list().await;
        *list.results_mut() = vec![
            ResultRow::new("Q1"),
            ResultRow::new("Q2"),
            ResultRow::new("Q3"),
        ];

        ListProcessor::set_keep_flags(&mut list, vec![true, false, true]);

        assert!(list.results()[0].keep());
        assert!(!list.results()[1].keep());
        assert!(list.results()[2].keep());
    }

    // ── assign_row_section_ids regression tests (issue #166) ────────────────

    #[tokio::test]
    async fn test_assign_row_section_ids_all_rows_assigned() {
        use crate::result_row::ResultRow;

        let mut list = create_test_list().await;
        *list.results_mut() = vec![
            ResultRow::new("Q1"),
            ResultRow::new("Q2"),
            ResultRow::new("Q3"),
            ResultRow::new("Q4"),
            ResultRow::new("Q5"),
        ];

        let valid = vec!["alpha".to_string(), "beta".to_string()];
        let (name2id, id2name, misc_id) = ListProcessor::create_section_mappings(valid);
        *list.section_id_to_name_mut() = id2name;

        let section_names = vec![
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
        ];

        ListProcessor::assign_row_section_ids(&mut list, section_names, name2id, misc_id).unwrap();

        for row in list.results().iter() {
            assert!(
                list.section_name(row.section()).is_some(),
                "row {} has section {} with no name",
                row.entity_id(),
                row.section()
            );
        }
    }

    #[tokio::test]
    async fn test_assign_row_section_ids_length_mismatch_errors() {
        use crate::result_row::ResultRow;

        let mut list = create_test_list().await;
        *list.results_mut() = vec![
            ResultRow::new("Q1"),
            ResultRow::new("Q2"),
            ResultRow::new("Q3"),
        ];

        let valid = vec!["alpha".to_string()];
        let (name2id, _id2name, misc_id) = ListProcessor::create_section_mappings(valid);

        let section_names = vec!["alpha".to_string()];
        let result =
            ListProcessor::assign_row_section_ids(&mut list, section_names, name2id, misc_id);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_assign_row_section_ids_unknown_names_go_to_misc() {
        use crate::result_row::ResultRow;

        let mut list = create_test_list().await;
        *list.results_mut() = vec![
            ResultRow::new("Q1"),
            ResultRow::new("Q2"),
            ResultRow::new("Q3"),
        ];

        let valid = vec!["alpha".to_string()];
        let (name2id, id2name, misc_id) = ListProcessor::create_section_mappings(valid);
        *list.section_id_to_name_mut() = id2name;

        let section_names = vec![
            "alpha".to_string(),
            "unknown_section".to_string(),
            "another_unknown".to_string(),
        ];

        ListProcessor::assign_row_section_ids(&mut list, section_names, name2id, misc_id).unwrap();

        assert_eq!(list.results()[0].section(), 0);
        assert_eq!(list.results()[1].section(), misc_id);
        assert_eq!(list.results()[2].section(), misc_id);

        for row in list.results().iter() {
            assert!(
                list.section_name(row.section()).is_some(),
                "row {} has section {} with no name",
                row.entity_id(),
                row.section()
            );
        }
    }

    #[tokio::test]
    async fn test_section_names_must_match_row_count() {
        use crate::result_row::ResultRow;

        let mut list = create_test_list().await;
        let num_rows = 30;
        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            rows.push(ResultRow::new(&format!("Q{i}")));
        }
        *list.results_mut() = rows;

        let categories = ["cat_a", "cat_b", "cat_c"];
        let section_names: Vec<String> = (0..num_rows)
            .map(|i| categories[i % categories.len()].to_string())
            .collect();

        assert_eq!(section_names.len(), list.results().len());

        let section_count = ListProcessor::build_section_count(&section_names);
        let valid_section_names = ListProcessor::build_valid_section_names(section_count, 1);
        let (name2id, id2name, misc_id) =
            ListProcessor::create_section_mappings(valid_section_names);
        *list.section_id_to_name_mut() = id2name;

        ListProcessor::assign_row_section_ids(&mut list, section_names, name2id, misc_id).unwrap();

        for (i, row) in list.results().iter().enumerate() {
            let section_name = list.section_name(row.section());
            assert!(
                section_name.is_some(),
                "row {} (Q{}) was assigned section {} which has no name",
                i,
                i,
                row.section()
            );
        }
    }

    #[tokio::test]
    async fn test_empty_section_names_go_to_misc() {
        use crate::result_row::ResultRow;

        let mut list = create_test_list().await;
        let num_rows = 30;
        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            rows.push(ResultRow::new(&format!("Q{i}")));
        }
        *list.results_mut() = rows;

        let mut section_names: Vec<String> = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            if i < 22 {
                section_names.push("".to_string());
            } else {
                section_names.push("Atari".to_string());
            }
        }

        let section_count = ListProcessor::build_section_count(&section_names);
        let valid_section_names = ListProcessor::build_valid_section_names(section_count, 2);

        assert!(
            !valid_section_names.contains(&"".to_string()),
            "empty string should not be a valid section name"
        );
        assert!(valid_section_names.contains(&"Atari".to_string()));

        let (name2id, id2name, misc_id) =
            ListProcessor::create_section_mappings(valid_section_names);
        *list.section_id_to_name_mut() = id2name;

        ListProcessor::assign_row_section_ids(&mut list, section_names, name2id, misc_id).unwrap();

        for i in 0..22 {
            assert_eq!(
                list.results()[i].section(),
                misc_id,
                "row {} should be in Misc",
                i
            );
        }

        for i in 22..num_rows {
            let section_name = list.section_name(list.results()[i].section());
            assert_eq!(
                section_name,
                Some("Atari"),
                "row {} should be in section 'Atari'",
                i
            );
        }

        for (i, row) in list.results().iter().enumerate() {
            assert!(
                list.section_name(row.section()).is_some(),
                "row {} has section {} with no name",
                i,
                row.section()
            );
        }
    }

    // ── process_assign_location_names (issue #136) ───────────────────────────

    fn make_location_part(lat: f64, lon: f64) -> crate::result_cell_part::PartWithReference {
        use crate::result_cell_part::{LocationInfo, PartWithReference, ResultCellPart};
        PartWithReference::new(
            ResultCellPart::Location(LocationInfo::new(lat, lon, None)),
            None,
        )
    }

    fn location_name_of(part: &crate::result_cell_part::PartWithReference) -> Option<String> {
        use crate::result_cell_part::ResultCellPart;
        match part.part() {
            ResultCellPart::Location(loc) => loc.name.clone(),
            _ => None,
        }
    }

    fn make_empty_cell() -> crate::result_cell::ResultCell {
        serde_json::from_value(serde_json::json!({
            "parts": [],
            "wdedit_class": null,
            "deduplicate_parts": true
        }))
        .unwrap()
    }

    async fn build_list_with_location_rows(rows: Vec<(&str, Vec<usize>)>) -> ListeriaList {
        use crate::result_row::ResultRow;
        let mut list = create_test_list().await;
        let mut result_rows = Vec::new();
        for (entity_id, cell_specs) in rows {
            let mut row = ResultRow::new(entity_id);
            let cells: Vec<_> = cell_specs
                .into_iter()
                .map(|n_locations| {
                    let mut cell = make_empty_cell();
                    let parts: Vec<_> = (0..n_locations)
                        .map(|i| make_location_part(i as f64, i as f64))
                        .collect();
                    cell.set_parts(parts);
                    cell
                })
                .collect();
            *row.cells_mut() = cells;
            result_rows.push(row);
        }
        *list.results_mut() = result_rows;
        list
    }

    #[tokio::test]
    async fn test_assign_location_names_single_row_single_location() {
        let mut list = build_list_with_location_rows(vec![("Q42", vec![1])]).await;
        ListProcessor::process_assign_location_names(&mut list);
        let row = &list.results()[0];
        assert_eq!(
            location_name_of(&row.cells()[0].parts()[0]),
            Some("Q42".to_string())
        );
    }

    #[tokio::test]
    async fn test_assign_location_names_multiple_locations_same_row() {
        let mut list = build_list_with_location_rows(vec![("Q42", vec![3])]).await;
        ListProcessor::process_assign_location_names(&mut list);
        let cell = &list.results()[0].cells()[0];
        assert_eq!(location_name_of(&cell.parts()[0]), Some("Q42".to_string()));
        assert_eq!(
            location_name_of(&cell.parts()[1]),
            Some("Q42_2".to_string())
        );
        assert_eq!(
            location_name_of(&cell.parts()[2]),
            Some("Q42_3".to_string())
        );
    }

    #[tokio::test]
    async fn test_assign_location_names_multiple_rows_distinct_items() {
        let mut list =
            build_list_with_location_rows(vec![("Q1", vec![1]), ("Q2", vec![1]), ("Q3", vec![1])])
                .await;
        ListProcessor::process_assign_location_names(&mut list);
        assert_eq!(
            location_name_of(&list.results()[0].cells()[0].parts()[0]),
            Some("Q1".to_string())
        );
        assert_eq!(
            location_name_of(&list.results()[1].cells()[0].parts()[0]),
            Some("Q2".to_string())
        );
        assert_eq!(
            location_name_of(&list.results()[2].cells()[0].parts()[0]),
            Some("Q3".to_string())
        );
    }

    #[tokio::test]
    async fn test_assign_location_names_same_item_in_multiple_rows() {
        let mut list =
            build_list_with_location_rows(vec![("Q42", vec![1]), ("Q42", vec![1])]).await;
        ListProcessor::process_assign_location_names(&mut list);
        assert_eq!(
            location_name_of(&list.results()[0].cells()[0].parts()[0]),
            Some("Q42".to_string())
        );
        assert_eq!(
            location_name_of(&list.results()[1].cells()[0].parts()[0]),
            Some("Q42_2".to_string())
        );
    }

    #[tokio::test]
    async fn test_assign_location_names_recurses_into_snaklist() {
        use crate::result_cell_part::{LocationInfo, PartWithReference, ResultCellPart};
        use crate::result_row::ResultRow;

        let mut list = create_test_list().await;
        let mut row = ResultRow::new("Q42");
        let mut snaklist_cell = make_empty_cell();

        let nested_parts = vec![
            PartWithReference::new(
                ResultCellPart::Location(LocationInfo::new(1.0, 1.0, None)),
                None,
            ),
            PartWithReference::new(
                ResultCellPart::Location(LocationInfo::new(2.0, 2.0, None)),
                None,
            ),
        ];
        let snaklist = PartWithReference::new(ResultCellPart::SnakList(nested_parts), None);
        snaklist_cell.set_parts(vec![snaklist]);
        *row.cells_mut() = vec![snaklist_cell];
        *list.results_mut() = vec![row];

        ListProcessor::process_assign_location_names(&mut list);

        let rendered_cell = &list.results()[0].cells()[0];
        match rendered_cell.parts()[0].part() {
            ResultCellPart::SnakList(rendered_nested) => {
                assert_eq!(
                    location_name_of(&rendered_nested[0]),
                    Some("Q42".to_string())
                );
                assert_eq!(
                    location_name_of(&rendered_nested[1]),
                    Some("Q42_2".to_string())
                );
            }
            other => panic!("Expected SnakList, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_assign_location_names_idempotent_within_call() {
        let mut list = build_list_with_location_rows(vec![("Q42", vec![2])]).await;
        ListProcessor::process_assign_location_names(&mut list);
        let first_run: Vec<_> = list.results()[0].cells()[0]
            .parts()
            .iter()
            .map(location_name_of)
            .collect();
        ListProcessor::process_assign_location_names(&mut list);
        let second_run: Vec<_> = list.results()[0].cells()[0]
            .parts()
            .iter()
            .map(location_name_of)
            .collect();
        assert_eq!(first_run, second_run);
    }

    // Suppress unused import warning for Configuration — it is used in
    // test_process_sort_results_finish_sorts_descending_via_sort_order
    // through the re-exported cached_config helper, but not directly here.
    #[allow(unused_imports)]
    use Configuration as _;
}
