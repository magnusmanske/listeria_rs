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
