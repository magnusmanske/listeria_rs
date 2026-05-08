//! Geographic region detection and location name assignment.

use crate::listeria_list::ListeriaList;
use crate::result_cell_part::ResultCellPart;
use crate::sparql_results::SparqlResults;
use anyhow::Result;
use futures::future::join_all;
use std::collections::HashMap;
use wikimisc::sparql_value::SparqlValue;

impl super::ListProcessor {
    pub async fn process_regions(list: &mut ListeriaList) -> Result<()> {
        if !list.do_get_regions() {
            return Ok(());
        }

        let entity_ids = list.process_regions_get_entity_ids();
        let entity_id2region = Self::process_regions_get_entity_id2region(list, entity_ids).await;

        for row in list.results_mut().iter_mut() {
            let the_region = match entity_id2region.get(row.entity_id()) {
                Some(r) => r,
                None => continue,
            };
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    if let ResultCellPart::Location(loc_info) = part.part_mut() {
                        loc_info.region = Some(the_region.clone());
                    }
                }
            }
        }

        Ok(())
    }

    /// Walks every result row and assigns a page-unique anchor name to each
    /// `ResultCellPart::Location`.
    ///
    /// The first occurrence of an item's id (e.g. `Q123`) keeps that id as
    /// its name; subsequent occurrences receive a numeric suffix
    /// (`Q123_2`, `Q123_3`, …). This guarantees that the rendered
    /// `{{Coordinate|...|name=...}}` (or equivalent per-wiki) templates do
    /// not collide as HTML anchor IDs (GitHub issue #136).
    pub fn process_assign_location_names(list: &mut ListeriaList) {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for row in list.results_mut().iter_mut() {
            let entity_id = row.entity_id().to_string();
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    Self::assign_location_name_in_part(part.part_mut(), &entity_id, &mut counts);
                }
            }
        }
    }

    fn assign_location_name_in_part(
        part: &mut ResultCellPart,
        entity_id: &str,
        counts: &mut HashMap<String, usize>,
    ) {
        match part {
            ResultCellPart::Location(loc_info) => {
                let count = counts.entry(entity_id.to_string()).or_insert(0);
                *count += 1;
                loc_info.name = Some(if *count == 1 {
                    entity_id.to_string()
                } else {
                    format!("{entity_id}_{}", *count)
                });
            }
            ResultCellPart::SnakList(parts) => {
                for nested in parts.iter_mut() {
                    Self::assign_location_name_in_part(nested.part_mut(), entity_id, counts);
                }
            }
            _ => {}
        }
    }

    async fn get_region_for_entity_id(list: &ListeriaList, entity_id: &str) -> Option<String> {
        let wikibase_key = list.template_params().wikibase().to_lowercase();
        let sparql = format!("SELECT ?q ?x {{ wd:{entity_id} wdt:P131* ?q . ?q wdt:P300 ?x }}");
        let mut sparql_results = SparqlResults::new(list.page_params().clone(), &wikibase_key);
        sparql_results.set_simulate(false);
        let mut region = String::new();
        let sparql_table = sparql_results.run_query(sparql).await.ok()?;
        let x_idx = sparql_table.get_var_index("x")?;
        for row_id in 0..sparql_table.len() {
            match sparql_table.get_row_col(row_id, x_idx) {
                Some(SparqlValue::Literal(r)) => {
                    if r.len() > region.len() {
                        region = r.to_string();
                    }
                }
                _ => continue,
            }
        }
        if region.is_empty() { None } else { Some(region) }
    }

    async fn process_regions_get_entity_id2region(
        list: &mut ListeriaList,
        entity_ids: std::collections::HashSet<String>,
    ) -> HashMap<String, String> {
        let entity_ids: Vec<String> = entity_ids.into_iter().collect();
        let futures: Vec<_> = entity_ids
            .iter()
            .map(|entity_id| Self::get_region_for_entity_id(list, entity_id))
            .collect();
        join_all(futures)
            .await
            .into_iter()
            .zip(entity_ids)
            .filter_map(|(region, entity_id)| region.map(|r| (entity_id, r)))
            .collect()
    }
}
