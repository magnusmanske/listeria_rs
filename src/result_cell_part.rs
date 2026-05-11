//! Individual parts that make up table cells, with rendering logic for different data types.

mod from_snak;
mod render;
mod types;

pub use types::{AutoDesc, EntityInfo, ExternalIdInfo, LinkTarget, LocalLinkInfo, LocationInfo};

use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::reference::Reference;
use crate::render_context::RenderContext;
use futures::future::join_all;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartWithReference {
    part: ResultCellPart,
    references: Option<Vec<Reference>>,
}

impl PartWithReference {
    #[must_use]
    pub const fn new(part: ResultCellPart, references: Option<Vec<Reference>>) -> Self {
        Self { part, references }
    }

    #[must_use]
    pub const fn references(&self) -> &Option<Vec<Reference>> {
        &self.references
    }

    #[must_use]
    pub const fn part(&self) -> &ResultCellPart {
        &self.part
    }

    pub const fn part_mut(&mut self) -> &mut ResultCellPart {
        &mut self.part
    }

    pub async fn as_wikitext(
        &self,
        list: &impl RenderContext,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let wikitext_part = self.part.as_wikitext(list, rownum, colnum).await;
        let wikitext_reference = if let Some(references) = &self.references {
            let futures: Vec<_> = references
                .iter()
                .map(|reference| reference.as_reference(list))
                .collect();
            join_all(futures).await.join("")
        } else {
            String::new()
        };
        wikitext_part + &wikitext_reference
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResultCellPart {
    Number,
    Entity(EntityInfo),
    EntitySchema(String),
    LocalLink(LocalLinkInfo),
    Time(String, i32), // (display, sort_year)
    Location(LocationInfo),
    File(String),
    Uri(String),
    ExternalId(ExternalIdInfo),
    Text(String),
    SnakList(Vec<PartWithReference>), // PP and PQP
    AutoDesc(AutoDesc),
    Quantity(f64, Option<String>), // (amount, unit_entity_id)
}

impl ResultCellPart {
    async fn localize_snak_list(
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
        v: &mut [PartWithReference],
    ) {
        for part_with_reference in v.iter_mut() {
            let result_cell_part = &mut part_with_reference.part;
            if let ResultCellPart::Entity(entity_info) = result_cell_part
                && entity_info.try_localize
                && let Some(ll) = ecw
                    .entity_to_local_link(&entity_info.id, wiki, language)
                    .await
            {
                *result_cell_part = ll;
            }
        }
    }

    pub async fn localize_item_links(
        &mut self,
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
    ) {
        match self {
            ResultCellPart::Entity(entity_info) if entity_info.try_localize => {
                if let Some(ll) = ecw
                    .entity_to_local_link(&entity_info.id, wiki, language)
                    .await
                {
                    *self = ll;
                };
            }
            ResultCellPart::SnakList(v) => {
                Self::localize_snak_list(ecw, wiki, language, v).await;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_part_with_reference_new() {
        let part = ResultCellPart::Text("test".to_string());
        let references = Some(vec![Reference::default()]);
        let pwr = PartWithReference::new(part.clone(), references.clone());
        assert_eq!(pwr.part(), &part);
        assert_eq!(pwr.references().as_ref().unwrap().len(), 1);
    }

    // SparqlValue::Location test removed - external struct instantiation not straightforward

    #[test]
    fn test_part_with_reference_no_references() {
        let part = ResultCellPart::Text("test".to_string());
        let pwr = PartWithReference::new(part.clone(), None);
        assert_eq!(pwr.part(), &part);
        assert!(pwr.references().is_none());
    }

    // --- PartWithReference::part_mut ---

    #[test]
    fn test_part_with_reference_part_mut() {
        let original = ResultCellPart::Text("before".to_string());
        let mut pwr = PartWithReference::new(original, None);
        // Mutate the inner part through part_mut()
        *pwr.part_mut() = ResultCellPart::Text("after".to_string());
        assert_eq!(pwr.part(), &ResultCellPart::Text("after".to_string()));
    }
}
