//! Table cells composed of parts with optional references.

use crate::{
    column::Column,
    entity_container_wrapper::EntityContainerWrapper,
    render_context::RenderContext,
    result_cell_part::{PartWithReference, ResultCellPart},
};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wikimisc::sparql_table_vec::SparqlTableVec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultCell {
    parts: Vec<PartWithReference>,
    wdedit_class: Option<String>,
    deduplicate_parts: bool,
}

impl ResultCell {
    pub async fn new(
        list: &impl RenderContext,
        entity_id: &str,
        sparql_table: &SparqlTableVec,
        col: &Column,
    ) -> Self {
        let (parts, wdedit_class) = col
            .obj()
            .render_cell_parts(list, entity_id, sparql_table)
            .await;
        Self {
            parts,
            wdedit_class,
            deduplicate_parts: true,
        }
    }

    #[must_use]
    pub fn get_sortkey(&self) -> String {
        match self.parts.first() {
            Some(part_with_reference) => match part_with_reference.part() {
                ResultCellPart::Entity(entity_info) => entity_info.id.clone(),
                ResultCellPart::LocalLink(link_info) => link_info.page.clone(),
                ResultCellPart::Time(_display, year) => year.to_string(),
                ResultCellPart::File(s) | ResultCellPart::Uri(s) | ResultCellPart::Text(s) => {
                    s.clone()
                }
                ResultCellPart::ExternalId(ext_id_info) => ext_id_info.id.clone(),
                _ => String::new(),
            },
            None => String::new(),
        }
    }

    #[must_use]
    pub const fn parts(&self) -> &Vec<PartWithReference> {
        &self.parts
    }

    pub const fn parts_mut(&mut self) -> &mut Vec<PartWithReference> {
        &mut self.parts
    }

    pub fn set_parts(&mut self, parts: Vec<PartWithReference>) {
        self.parts = parts;
    }

    pub async fn localize_item_links_in_parts(
        parts: &mut [PartWithReference],
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
    ) {
        let futures: Vec<_> = parts
            .iter_mut()
            .map(|part_with_reference| {
                part_with_reference
                    .part_mut()
                    .localize_item_links(ecw, wiki, language)
            })
            .collect();
        futures::future::join_all(futures).await;
    }

    pub async fn as_tabbed_data(&self, list: &impl RenderContext, rownum: usize, colnum: usize) -> Value {
        let mut ret = Vec::with_capacity(self.parts.len());
        for part_with_reference in self.parts.iter() {
            ret.push(
                part_with_reference
                    .part()
                    .as_tabbed_data(list, rownum, colnum)
                    .await,
            );
        }
        json!(ret.join("<br/>"))
    }

    pub async fn as_wikitext(&self, list: &impl RenderContext, rownum: usize, colnum: usize) -> String {
        let futures: Vec<_> = self
            .parts
            .iter()
            .map(|part| part.as_wikitext(list, rownum, colnum))
            .collect();
        let mut parts = join_all(futures).await;
        if self.deduplicate_parts {
            parts = Self::do_deduplicate_parts(&parts);
        }
        self.get_cell_prefix(list) + &parts.join("<br/>")
    }

    fn get_cell_prefix(&self, list: &impl RenderContext) -> String {
        let time_sort_year = self.parts.first().and_then(|p| match p.part() {
            ResultCellPart::Time(_, year) => Some(*year),
            _ => None,
        });

        let wdedit_class = if list.template_params().wdedit() && list.header_template().is_none() {
            self.wdedit_class.as_deref()
        } else {
            None
        };

        match (wdedit_class, time_sort_year) {
            (Some(class), Some(year)) => {
                format!("class='{class}' data-sort-value=\"{year}\" | ")
            }
            (Some(class), None) => format!("class='{class}'| "),
            (None, Some(year)) => format!(" data-sort-value=\"{year}\" | "),
            (None, None) => " ".to_string(),
        }
    }

    fn do_deduplicate_parts(parts: &[String]) -> Vec<String> {
        let mut seen = std::collections::HashSet::with_capacity(parts.len());
        let mut result = Vec::with_capacity(parts.len());
        for part in parts {
            if seen.insert(part) {
                result.push(part.to_owned());
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_cell_part::{EntityInfo, ExternalIdInfo, LinkTarget, LocalLinkInfo};

    fn make_cell(parts: Vec<ResultCellPart>) -> ResultCell {
        let pwrs: Vec<PartWithReference> = parts
            .into_iter()
            .map(|p| PartWithReference::new(p, None))
            .collect();
        serde_json::from_value(serde_json::json!({
            "parts": serde_json::to_value(&pwrs).unwrap(),
            "wdedit_class": null,
            "deduplicate_parts": true
        }))
        .unwrap()
    }

    // --- get_sortkey ---

    #[test]
    fn test_get_sortkey_entity() {
        let cell = make_cell(vec![ResultCellPart::Entity(EntityInfo::new(
            "Q42".to_string(),
            true,
        ))]);
        assert_eq!(cell.get_sortkey(), "Q42");
    }

    #[test]
    fn test_get_sortkey_local_link() {
        let cell = make_cell(vec![ResultCellPart::LocalLink(LocalLinkInfo::new(
            "Main Page".to_string(),
            "Main".to_string(),
            LinkTarget::Page,
        ))]);
        assert_eq!(cell.get_sortkey(), "Main Page");
    }

    #[test]
    fn test_get_sortkey_time() {
        let cell = make_cell(vec![ResultCellPart::Time("2024-01-15".to_string(), 2024)]);
        assert_eq!(cell.get_sortkey(), "2024");
    }

    #[test]
    fn test_get_sortkey_text() {
        let cell = make_cell(vec![ResultCellPart::Text("hello world".to_string())]);
        assert_eq!(cell.get_sortkey(), "hello world");
    }

    #[test]
    fn test_get_sortkey_file() {
        let cell = make_cell(vec![ResultCellPart::File("photo.jpg".to_string())]);
        assert_eq!(cell.get_sortkey(), "photo.jpg");
    }

    #[test]
    fn test_get_sortkey_uri() {
        let cell = make_cell(vec![ResultCellPart::Uri("https://example.com".to_string())]);
        assert_eq!(cell.get_sortkey(), "https://example.com");
    }

    #[test]
    fn test_get_sortkey_external_id() {
        let cell = make_cell(vec![ResultCellPart::ExternalId(ExternalIdInfo::new(
            "P213".to_string(),
            "12345".to_string(),
        ))]);
        assert_eq!(cell.get_sortkey(), "12345");
    }

    #[test]
    fn test_get_sortkey_number_returns_empty() {
        let cell = make_cell(vec![ResultCellPart::Number]);
        assert_eq!(cell.get_sortkey(), "");
    }

    #[test]
    fn test_get_sortkey_empty_parts() {
        let cell = make_cell(vec![]);
        assert_eq!(cell.get_sortkey(), "");
    }

    #[test]
    fn test_get_sortkey_uses_first_part() {
        let cell = make_cell(vec![
            ResultCellPart::Text("first".to_string()),
            ResultCellPart::Text("second".to_string()),
        ]);
        assert_eq!(cell.get_sortkey(), "first");
    }

    // --- do_deduplicate_parts ---

    #[test]
    fn test_deduplicate_parts_removes_duplicates() {
        let parts = vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
        ];
        let result = ResultCell::do_deduplicate_parts(&parts);
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_deduplicate_parts_preserves_order() {
        let parts = vec!["z".to_string(), "a".to_string(), "z".to_string()];
        let result = ResultCell::do_deduplicate_parts(&parts);
        assert_eq!(result, vec!["z", "a"]);
    }

    #[test]
    fn test_deduplicate_parts_empty() {
        let parts: Vec<String> = vec![];
        let result = ResultCell::do_deduplicate_parts(&parts);
        assert!(result.is_empty());
    }

    #[test]
    fn test_deduplicate_parts_single() {
        let parts = vec!["only".to_string()];
        let result = ResultCell::do_deduplicate_parts(&parts);
        assert_eq!(result, vec!["only"]);
    }

    #[test]
    fn test_deduplicate_parts_all_same() {
        let parts = vec!["x".to_string(), "x".to_string(), "x".to_string()];
        let result = ResultCell::do_deduplicate_parts(&parts);
        assert_eq!(result, vec!["x"]);
    }

    // --- set_parts / parts ---

    #[test]
    fn test_set_and_get_parts() {
        let mut cell = make_cell(vec![]);
        assert!(cell.parts().is_empty());
        let new_parts = vec![
            PartWithReference::new(ResultCellPart::Text("hello".to_string()), None),
            PartWithReference::new(ResultCellPart::Number, None),
        ];
        cell.set_parts(new_parts);
        assert_eq!(cell.parts().len(), 2);
    }
}
