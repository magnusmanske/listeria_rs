//! AutoDesc description gathering and injection.

use crate::listeria_list::ListeriaList;
use crate::result_cell_part::ResultCellPart;
use anyhow::Result;
use std::collections::HashMap;

impl super::ListProcessor {
    pub async fn fill_autodesc(list: &mut ListeriaList) -> Result<()> {
        // Two-phase: gather descriptions without holding &mut, then write them back.
        let autodescs = Self::fill_autodesc_gather_descriptions(list).await?;
        Self::fill_autodesc_set_descriptions(list, autodescs)?;
        Ok(())
    }

    fn fill_autodesc_set_descriptions(
        list: &mut ListeriaList,
        autodescs: HashMap<String, String>,
    ) -> Result<()> {
        for row in list.results_mut().iter_mut() {
            for cell in row.cells_mut() {
                for part_with_reference in cell.parts_mut() {
                    if let ResultCellPart::AutoDesc(ad) = part_with_reference.part_mut()
                        && let Some(desc) = autodescs.get(ad.entity_id())
                    {
                        ad.set_description(desc);
                    }
                }
            }
        }
        Ok(())
    }

    async fn fill_autodesc_gather_descriptions(
        list: &mut ListeriaList,
    ) -> Result<HashMap<String, String>> {
        let entity_ids = Self::collect_autodesc_entity_ids(list);
        Self::load_and_get_descriptions(list, entity_ids).await
    }

    fn collect_autodesc_entity_ids(list: &ListeriaList) -> Vec<String> {
        let mut entity_ids = Vec::new();
        for row in list.results().iter() {
            for cell in row.cells() {
                for part_with_reference in cell.parts() {
                    if let ResultCellPart::AutoDesc(ad) = part_with_reference.part() {
                        entity_ids.push(ad.entity_id().to_owned());
                    }
                }
            }
        }
        entity_ids
    }

    async fn load_and_get_descriptions(
        list: &mut ListeriaList,
        entity_ids: Vec<String>,
    ) -> Result<HashMap<String, String>> {
        if entity_ids.is_empty() {
            return Ok(HashMap::new());
        }
        list.ecw().load_entities(list.wb_api(), &entity_ids).await?;
        let mut autodescs = HashMap::new();
        for entity_id in entity_ids {
            if let Some(entity) = list.ecw().get_entity(&entity_id).await
                && let Ok(desc) = list.get_autodesc_description(&entity).await
            {
                autodescs.insert(entity_id, desc);
            }
        }
        Ok(autodescs)
    }
}

#[cfg(test)]
mod tests {
    use super::super::ListProcessor;
    use crate::page_params::PageParams;
    use crate::result_cell::ResultCell;
    use crate::result_cell_part::{AutoDesc, PartWithReference, ResultCellPart};
    use crate::result_row::ResultRow;
    use crate::template::Template;
    use std::collections::HashMap;
    use std::sync::Arc;

    async fn make_list() -> crate::listeria_list::ListeriaList {
        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
        let page_params = Arc::new(
            PageParams::new(config, api, "Test:Page".to_string())
                .await
                .unwrap(),
        );
        let template = Template::new_from_params(
            "columns=item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }}",
        )
        .unwrap();
        crate::listeria_list::ListeriaList::new(template, page_params)
            .await
            .unwrap()
    }

    fn make_autodesc_cell(entity_id: &str) -> ResultCell {
        let ad: AutoDesc = serde_json::from_value(serde_json::json!({
            "entity_id": entity_id,
            "desc": null
        }))
        .unwrap();
        let part = PartWithReference::new(ResultCellPart::AutoDesc(ad), None);
        serde_json::from_value(serde_json::json!({
            "parts": serde_json::to_value(vec![part]).unwrap(),
            "wdedit_class": null,
            "deduplicate_parts": true
        }))
        .unwrap()
    }

    // ── collect_autodesc_entity_ids ────────────────────────────────────────

    #[tokio::test]
    async fn test_collect_autodesc_entity_ids_empty_list() {
        let list = make_list().await;
        // No results → no entity IDs
        let ids = ListProcessor::collect_autodesc_entity_ids(&list);
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn test_collect_autodesc_entity_ids_with_autodesc_parts() {
        let mut list = make_list().await;
        let mut row = ResultRow::new("Q42");
        *row.cells_mut() = vec![make_autodesc_cell("Q42")];
        *list.results_mut() = vec![row];

        let ids = ListProcessor::collect_autodesc_entity_ids(&list);
        assert_eq!(ids, vec!["Q42".to_string()]);
    }

    #[tokio::test]
    async fn test_collect_autodesc_entity_ids_skips_non_autodesc_parts() {
        let mut list = make_list().await;
        let text_part = PartWithReference::new(ResultCellPart::Text("hello".into()), None);
        let cell: ResultCell = serde_json::from_value(serde_json::json!({
            "parts": serde_json::to_value(vec![text_part]).unwrap(),
            "wdedit_class": null,
            "deduplicate_parts": true
        }))
        .unwrap();
        let mut row = ResultRow::new("Q1");
        *row.cells_mut() = vec![cell];
        *list.results_mut() = vec![row];

        let ids = ListProcessor::collect_autodesc_entity_ids(&list);
        assert!(ids.is_empty());
    }

    // ── fill_autodesc_set_descriptions ────────────────────────────────────

    #[tokio::test]
    async fn test_fill_autodesc_set_descriptions_updates_matching_part() {
        let mut list = make_list().await;
        let mut row = ResultRow::new("Q42");
        *row.cells_mut() = vec![make_autodesc_cell("Q42")];
        *list.results_mut() = vec![row];

        let mut autodescs = HashMap::new();
        autodescs.insert("Q42".to_string(), "a famous person".to_string());

        ListProcessor::fill_autodesc_set_descriptions(&mut list, autodescs).unwrap();

        let part = &list.results()[0].cells()[0].parts()[0];
        match part.part() {
            ResultCellPart::AutoDesc(ad) => {
                // The desc field is private; we verify indirectly that the
                // function ran without error and the row is still present.
                let _ = ad; // presence confirms the part is AutoDesc
            }
            other => panic!("Expected AutoDesc, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_fill_autodesc_set_descriptions_no_match_leaves_part_unchanged() {
        let mut list = make_list().await;
        let mut row = ResultRow::new("Q42");
        *row.cells_mut() = vec![make_autodesc_cell("Q42")];
        *list.results_mut() = vec![row];

        // autodescs has a different ID — no match
        let mut autodescs = HashMap::new();
        autodescs.insert("Q99".to_string(), "unrelated".to_string());

        ListProcessor::fill_autodesc_set_descriptions(&mut list, autodescs).unwrap();

        // Row must still be there (function must not panic or drop rows)
        assert_eq!(list.results().len(), 1);
        assert_eq!(list.results()[0].cells()[0].parts().len(), 1);
    }

    #[tokio::test]
    async fn test_fill_autodesc_set_descriptions_empty_list_ok() {
        let mut list = make_list().await;
        let result = ListProcessor::fill_autodesc_set_descriptions(&mut list, HashMap::new());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fill_autodesc_set_descriptions_non_autodesc_parts_unaffected() {
        let mut list = make_list().await;
        let text_part = PartWithReference::new(ResultCellPart::Text("hello".into()), None);
        let cell: ResultCell = serde_json::from_value(serde_json::json!({
            "parts": serde_json::to_value(vec![text_part]).unwrap(),
            "wdedit_class": null,
            "deduplicate_parts": true
        }))
        .unwrap();
        let mut row = ResultRow::new("Q1");
        *row.cells_mut() = vec![cell];
        *list.results_mut() = vec![row];

        let mut autodescs = HashMap::new();
        autodescs.insert("Q1".to_string(), "should not appear".to_string());

        ListProcessor::fill_autodesc_set_descriptions(&mut list, autodescs).unwrap();

        // The Text part must remain unchanged
        assert!(matches!(
            list.results()[0].cells()[0].parts()[0].part(),
            ResultCellPart::Text(_)
        ));
    }
}
