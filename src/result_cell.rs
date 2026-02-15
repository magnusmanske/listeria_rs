//! Table cells composed of parts with optional references.

use crate::{
    column::Column,
    column_type::ColumnType,
    entity_container_wrapper::EntityContainerWrapper,
    listeria_list::ListeriaList,
    reference::Reference,
    result_cell_part::{
        AutoDesc, EntityInfo, LinkTarget, LocalLinkInfo, PartWithReference, ResultCellPart,
    },
    template_params::ReferencesParameter,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wikimisc::{
    sparql_table_vec::SparqlTableVec,
    wikibase::{Statement, entity::EntityTrait},
};

// Wikitext escape sequences
const WIKITEXT_APOSTROPHE_ESCAPE: &str = "&#39;";
const WIKITEXT_LT_ESCAPE: &str = "&lt;";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultCell {
    parts: Vec<PartWithReference>,
    wdedit_class: Option<String>,
    deduplicate_parts: bool,
}

impl ResultCell {
    pub async fn new(
        list: &ListeriaList,
        entity_id: &str,
        sparql_table: &SparqlTableVec,
        col: &Column,
    ) -> Self {
        let mut ret = Self {
            parts: Vec::new(),
            wdedit_class: None,
            deduplicate_parts: true,
        };

        let entity = list.get_entity(entity_id).await;
        match col.obj() {
            ColumnType::Qid => Self::ct_qid(&mut ret, entity_id),
            ColumnType::Item => Self::ct_item(&mut ret, entity_id),
            ColumnType::Description(langs) => Self::ct_description(&entity, list, &mut ret, langs),
            ColumnType::Field(varname) => Self::ct_field(varname, sparql_table, &mut ret),
            ColumnType::Property(property) => Self::ct_property(&entity, &mut ret, list, property),
            ColumnType::PropertyQualifier((p1, p2)) => Self::ct_pq(&entity, list, p1, &mut ret, p2),
            ColumnType::PropertyQualifierValue((p1, q1, p2)) => {
                Self::ct_pqv(&entity, list, p1, &mut ret, q1, p2);
            }
            ColumnType::LabelLang(language) => {
                Self::ct_label_lang(&entity, language, &mut ret, list);
            }
            ColumnType::AliasLang(language) => Self::ct_alias_lang(&entity, language, &mut ret),
            ColumnType::Label => Self::ct_label(entity, &mut ret, list, entity_id),
            ColumnType::Number => Self::ct_number(&mut ret),
            ColumnType::Unknown => {} // Ignore
        }

        ret
    }

    fn fix_wikitext_for_output(s: &str) -> String {
        s.replace('\'', WIKITEXT_APOSTROPHE_ESCAPE)
            .replace('<', WIKITEXT_LT_ESCAPE)
    }

    fn get_parts_p_p(statement: &Statement, property: &str) -> Vec<ResultCellPart> {
        statement
            .qualifiers()
            .iter()
            .filter(|snak| *snak.property() == *property)
            .map(|snak| {
                ResultCellPart::SnakList(vec![
                    PartWithReference::new(ResultCellPart::from_snak(statement.main_snak()), None),
                    PartWithReference::new(ResultCellPart::from_snak(snak), None),
                ])
            })
            .collect()
    }

    fn get_references_for_statement(
        statement: &Statement,
        language: &str,
    ) -> Option<Vec<Reference>> {
        let references = statement.references();
        let mut ret: Vec<Reference> = Vec::with_capacity(references.len());
        for reference in references.iter() {
            if let Some(r) = Reference::new_from_snaks(reference.snaks(), language) {
                ret.push(r);
            }
        }
        if ret.is_empty() { None } else { Some(ret) }
    }

    fn get_parts_p_q_p(
        statement: &Statement,
        target_item: &str,
        property: &str,
    ) -> Vec<ResultCellPart> {
        let links_to_target = match statement.main_snak().data_value() {
            Some(dv) => match dv.value() {
                wikimisc::wikibase::value::Value::Entity(e) => e.id() == target_item,
                _ => false,
            },
            None => false,
        };
        if !links_to_target {
            return Vec::new();
        }
        //self.get_parts_p_p(statement,property)
        statement
            .qualifiers()
            .iter()
            .filter(|snak| *snak.property() == *property)
            .map(|snak| {
                ResultCellPart::SnakList(vec![PartWithReference::new(
                    ResultCellPart::from_snak(snak),
                    None,
                )])
            })
            .collect()
    }

    #[must_use]
    pub fn get_sortkey(&self) -> String {
        match self.parts.first() {
            Some(part_with_reference) => match part_with_reference.part() {
                ResultCellPart::Entity(entity_info) => entity_info.id.clone(),
                ResultCellPart::LocalLink(link_info) => link_info.page.clone(),
                ResultCellPart::Time(time) => time.clone(),
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
        for part_with_reference in parts.iter_mut() {
            part_with_reference
                .part_mut()
                .localize_item_links(ecw, wiki, language)
                .await;
        }
    }

    pub async fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> Value {
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

    pub async fn as_wikitext(
        &mut self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let mut parts = Vec::with_capacity(self.parts.len());
        for part_with_reference in &mut self.parts {
            parts.push(part_with_reference.as_wikitext(list, rownum, colnum).await);
        }
        if self.deduplicate_parts {
            parts = Self::do_deduplicate_parts(&parts);
        }
        self.get_cell_class(list) + &parts.join("<br/>")
    }

    fn get_cell_class(&mut self, list: &ListeriaList) -> String {
        if list.template_params().wdedit()
            && list.header_template().is_none()
            && let Some(class) = &self.wdedit_class
        {
            format!("class='{class}'| ")
        } else {
            " ".to_string()
        }
    }

    fn ct_number(ret: &mut ResultCell) {
        ret.parts
            .push(PartWithReference::new(ResultCellPart::Number, None));
    }

    fn ct_label(
        entity: Option<wikimisc::wikibase::Entity>,
        ret: &mut ResultCell,
        list: &ListeriaList,
        entity_id: &str,
    ) {
        if let Some(e) = entity {
            ret.wdedit_class = match &list.header_template() {
                Some(_) => None,
                None => Some("wd_label".to_string()),
            };
            let label = match e.label_in_locale(list.language()) {
                Some(s) => s.to_string(),
                None => entity_id.to_string(),
            };
            let local_page = match e.sitelinks() {
                Some(sl) => sl
                    .iter()
                    .filter(|s| *s.site() == *list.wiki())
                    .map(|s| s.title().to_string())
                    .next(),
                None => None,
            };
            match local_page {
                Some(page) => {
                    ret.parts.push(PartWithReference::new(
                        ResultCellPart::LocalLink(LocalLinkInfo::new(
                            page,
                            label,
                            LinkTarget::Page,
                        )),
                        None,
                    ));
                }
                None => {
                    ret.parts.push(PartWithReference::new(
                        ResultCellPart::Entity(EntityInfo::new(entity_id.to_string(), true)),
                        None,
                    ));
                }
            }
        }
    }

    fn ct_alias_lang(
        entity: &Option<wikimisc::wikibase::Entity>,
        language: &String,
        ret: &mut ResultCell,
    ) {
        if let Some(e) = entity {
            let mut aliases: Vec<String> = e
                .aliases()
                .iter()
                .filter(|alias| alias.language() == language)
                .map(|alias| alias.value().to_string())
                .collect();
            aliases.sort();
            aliases.iter().for_each(|alias| {
                ret.parts.push(PartWithReference::new(
                    ResultCellPart::Text(alias.to_owned()),
                    None,
                ));
            });
        }
    }

    fn ct_label_lang(
        entity: &Option<wikimisc::wikibase::Entity>,
        language: &str,
        ret: &mut ResultCell,
        list: &ListeriaList,
    ) {
        if let Some(e) = entity {
            match e.label_in_locale(language) {
                Some(s) => {
                    ret.parts.push(PartWithReference::new(
                        ResultCellPart::Text(s.to_string()),
                        None,
                    ));
                }
                None => {
                    if let Some(s) = e.label_in_locale(list.language()) {
                        ret.parts.push(PartWithReference::new(
                            ResultCellPart::Text(s.to_string()),
                            None,
                        ));
                    }
                }
            }
        }
    }

    fn ct_pqv(
        entity: &Option<wikimisc::wikibase::Entity>,
        list: &ListeriaList,
        p1: &str,
        ret: &mut ResultCell,
        q1: &str,
        p2: &str,
    ) {
        if let Some(e) = entity {
            list.get_filtered_claims(e, p1)
                .iter()
                .for_each(|statement| {
                    ResultCell::get_parts_p_q_p(statement, q1, p2)
                        .iter()
                        .for_each(|part| {
                            ret.parts
                                .push(PartWithReference::new(part.to_owned(), None));
                        });
                });
        }
    }

    fn ct_pq(
        entity: &Option<wikimisc::wikibase::Entity>,
        list: &ListeriaList,
        p1: &str,
        ret: &mut ResultCell,
        p2: &str,
    ) {
        if let Some(e) = entity {
            list.get_filtered_claims(e, p1)
                .iter()
                .for_each(|statement| {
                    ResultCell::get_parts_p_p(statement, p2)
                        .iter()
                        .for_each(|part| {
                            ret.parts
                                .push(PartWithReference::new(part.to_owned(), None));
                        });
                });
        }
    }

    fn ct_property(
        entity: &Option<wikimisc::wikibase::Entity>,
        ret: &mut ResultCell,
        list: &ListeriaList,
        property: &str,
    ) {
        if let Some(e) = entity {
            ret.wdedit_class = match &list.header_template() {
                Some(_) => None,
                None => Some(format!("wd_{}", property.to_lowercase())),
            };
            list.get_filtered_claims(e, property)
                .iter()
                .for_each(|statement| {
                    let references = match list.get_reference_parameter() {
                        ReferencesParameter::All => {
                            Self::get_references_for_statement(statement, list.language())
                        }
                        _ => None,
                    };
                    ret.parts.push(PartWithReference::new(
                        ResultCellPart::from_snak(statement.main_snak()),
                        references,
                    ));
                });
        }
    }

    fn ct_field(varname: &str, sparql_table: &SparqlTableVec, ret: &mut ResultCell) {
        let var_index = match sparql_table.get_var_index(varname) {
            Some(i) => i,
            None => return, // Nothing to do
        };
        for row_id in 0..sparql_table.len() {
            if let Some(x) = sparql_table.get_row_col(row_id, var_index) {
                ret.parts.push(PartWithReference::new(
                    ResultCellPart::from_sparql_value(&x),
                    None,
                ));
            }
        }
    }

    fn ct_description(
        entity: &Option<wikimisc::wikibase::Entity>,
        list: &ListeriaList,
        ret: &mut ResultCell,
        langs: &[String],
    ) {
        if let Some(e) = entity {
            let description = if langs.is_empty() {
                // Default behavior: use list language
                e.description_in_locale(list.language())
            } else {
                // Try each fallback language in order
                langs.iter().find_map(|lang| e.description_in_locale(lang))
            };

            match description {
                Some(s) => {
                    ret.wdedit_class = match &list.header_template() {
                        Some(_) => None,
                        None => Some("wd_desc".to_string()),
                    };
                    let s = Self::fix_wikitext_for_output(s);
                    ret.parts
                        .push(PartWithReference::new(ResultCellPart::Text(s), None));
                }
                None => {
                    ret.parts.push(PartWithReference::new(
                        ResultCellPart::AutoDesc(AutoDesc::new(e)),
                        None,
                    ));
                }
            }
        }
    }

    fn ct_item(ret: &mut ResultCell, entity_id: &str) {
        ret.parts.push(PartWithReference::new(
            ResultCellPart::Entity(EntityInfo::new(entity_id.to_owned(), false)),
            None,
        ));
    }

    fn ct_qid(ret: &mut ResultCell, entity_id: &str) {
        ret.parts.push(PartWithReference::new(
            ResultCellPart::Text(entity_id.to_string()),
            None,
        ));
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
    use crate::result_cell_part::ExternalIdInfo;

    #[test]
    fn test_fix_wikitext_for_output() {
        assert_eq!(ResultCell::fix_wikitext_for_output("a'b<c"), "a&#39;b&lt;c");
    }

    #[test]
    fn test_fix_wikitext_for_output_no_special_chars() {
        assert_eq!(
            ResultCell::fix_wikitext_for_output("normal text"),
            "normal text"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_only_apostrophe() {
        assert_eq!(ResultCell::fix_wikitext_for_output("it's"), "it&#39;s");
    }

    #[test]
    fn test_fix_wikitext_for_output_only_less_than() {
        assert_eq!(ResultCell::fix_wikitext_for_output("a<b"), "a&lt;b");
    }

    #[test]
    fn test_fix_wikitext_for_output_multiple_apostrophes() {
        assert_eq!(
            ResultCell::fix_wikitext_for_output("it's John's"),
            "it&#39;s John&#39;s"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_multiple_less_than() {
        assert_eq!(ResultCell::fix_wikitext_for_output("a<b<c"), "a&lt;b&lt;c");
    }

    #[test]
    fn test_fix_wikitext_for_output_empty_string() {
        assert_eq!(ResultCell::fix_wikitext_for_output(""), "");
    }

    #[test]
    fn test_fix_wikitext_for_output_html_tag() {
        assert_eq!(
            ResultCell::fix_wikitext_for_output("<script>alert('hi')</script>"),
            "&lt;script>alert(&#39;hi&#39;)&lt;/script>"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_wikitext_bold() {
        // Bold markup should remain unchanged
        assert_eq!(
            ResultCell::fix_wikitext_for_output("'''bold'''"),
            "&#39;&#39;&#39;bold&#39;&#39;&#39;"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_comparison() {
        assert_eq!(ResultCell::fix_wikitext_for_output("1<2"), "1&lt;2");
    }

    #[test]
    fn test_fix_wikitext_for_output_unicode_with_special() {
        assert_eq!(
            ResultCell::fix_wikitext_for_output("日本's data<test"),
            "日本&#39;s data&lt;test"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_consecutive_special() {
        assert_eq!(
            ResultCell::fix_wikitext_for_output("'<'<"),
            "&#39;&lt;&#39;&lt;"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_greater_than_unchanged() {
        // Greater than should NOT be escaped
        assert_eq!(ResultCell::fix_wikitext_for_output("a>b"), "a>b");
    }

    #[test]
    fn test_fix_wikitext_for_output_double_quote_unchanged() {
        // Double quotes should NOT be escaped
        assert_eq!(
            ResultCell::fix_wikitext_for_output("\"quoted\""),
            "\"quoted\""
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_ampersand_unchanged() {
        // Ampersand should NOT be escaped
        assert_eq!(ResultCell::fix_wikitext_for_output("a&b"), "a&b");
    }

    // --- get_sortkey ---

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
        let cell = make_cell(vec![ResultCellPart::Time(
            "+2024-01-15T00:00:00Z".to_string(),
        )]);
        assert_eq!(cell.get_sortkey(), "+2024-01-15T00:00:00Z");
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
