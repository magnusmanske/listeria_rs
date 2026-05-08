//! Reference item loading (stated-in sources).

use crate::listeria_list::ListeriaList;
use crate::result_cell_part::PartWithReference;
use anyhow::Result;

impl super::ListProcessor {
    pub async fn process_reference_items(list: &mut ListeriaList) -> Result<()> {
        let items_to_load = Self::collect_stated_in_items_from_references(list);
        if !items_to_load.is_empty() {
            list.ecw()
                .load_entities(list.wb_api(), &items_to_load)
                .await?;
        }
        Ok(())
    }

    fn collect_stated_in_items_from_references(list: &mut ListeriaList) -> Vec<String> {
        let mut items_to_load: Vec<String> = Vec::new();
        for row in list.results_mut().iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                for part_with_reference in cell.parts_mut().iter_mut() {
                    Self::collect_stated_in_from_part(part_with_reference, &mut items_to_load);
                }
            }
        }
        items_to_load.sort_unstable();
        items_to_load.dedup();
        items_to_load
    }

    pub(crate) fn collect_stated_in_from_part(
        part: &PartWithReference,
        items_to_load: &mut Vec<String>,
    ) {
        if let Some(references) = part.references() {
            for reference in references.iter() {
                if let Some(stated_in) = reference.stated_in() {
                    items_to_load.push(stated_in.to_string());
                }
            }
        }
    }
}
