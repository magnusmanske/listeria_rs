//! Column types for result tables.
//!
//! `ColumnType` is both a parser (via `new`) and the single dispatch point for
//! per-column cell rendering (`render_cell_parts`).  Adding a new column type
//! only requires extending the `ColumnType` enum and the two match expressions
//! that live inside this file — callers (`ResultCell`) need no changes.

use crate::entity_container_wrapper::{EntityContainerWrapper, EntityEntry};
use crate::reference::Reference;
use crate::render_context::RenderContext;
use crate::result_cell_part::{AutoDesc, EntityInfo, LinkTarget, LocalLinkInfo, PartWithReference, ResultCellPart};
use crate::template_params::ReferencesParameter;
use wikimisc::sparql_table_vec::SparqlTableVec;
use wikimisc::wikibase::{Statement, entity::EntityTrait};

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Number,
    Label,
    LabelLang(String),
    AliasLang(String),
    Description(Vec<String>),
    Item,
    Qid,
    Property(String),
    PropertyQualifier((String, String)),
    PropertyQualifierValue((String, String, String)),
    Field(String),
    Sitelink(String),
    Unknown,
}

impl ColumnType {
    /// Check if a string matches `[PpQq]\d+` pattern and return the uppercase form.
    fn parse_pq_id(s: &str, prefix: u8) -> Option<String> {
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            return None;
        }
        let first = bytes[0];
        if first != prefix && first != (prefix ^ 0x20) {
            return None;
        }
        if bytes.len() < 2 {
            return None;
        }
        if bytes[1..].iter().all(|b| b.is_ascii_digit()) {
            Some(s.to_uppercase())
        } else {
            None
        }
    }

    /// Try to parse a slash-separated compound like "P31/P580" or "P39/Q41582/P580"
    /// from already-trimmed parts.
    fn parse_slash_compound(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        // Split on '/' and trim each part
        let parts: Vec<&str> = trimmed.split('/').map(|p| p.trim()).collect();
        match parts.len() {
            2 => {
                let p1 = Self::parse_pq_id(parts[0], b'P')?;
                let p2 = Self::parse_pq_id(parts[1], b'P')?;
                Some(ColumnType::PropertyQualifier((p1, p2)))
            }
            3 => {
                let p1 = Self::parse_pq_id(parts[0], b'P')?;
                let q1 = Self::parse_pq_id(parts[1], b'Q')?;
                let p2 = Self::parse_pq_id(parts[2], b'P')?;
                Some(ColumnType::PropertyQualifierValue((p1, q1, p2)))
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn new(s: &str) -> Self {
        let lower = s.to_lowercase();
        let lower_trimmed = lower.trim();

        // Fast path: exact keyword matches
        match lower_trimmed {
            "number" => return ColumnType::Number,
            "label" => return ColumnType::Label,
            "description" => return ColumnType::Description(Vec::new()),
            "item" => return ColumnType::Item,
            "qid" => return ColumnType::Qid,
            _ => {}
        }

        if let Some(ct) = Self::parse_from_lowercase_prefix(lower_trimmed) {
            return ct;
        }

        // From here on, work with the original string (preserving case for P/Q ids)
        let trimmed = s.trim();

        // Check for simple property: P\d+
        if let Some(p) = Self::parse_pq_id(trimmed, b'P') {
            return ColumnType::Property(p);
        }

        // Check for compound (contains '/'):  P/P or P/Q/P
        if trimmed.contains('/')
            && let Some(ct) = Self::parse_slash_compound(trimmed)
        {
            return ct;
        }

        // Check for field: ?...
        if let Some(rest) = trimmed.strip_prefix('?')
            && !rest.is_empty()
        {
            return ColumnType::Field(rest.to_uppercase());
        }

        ColumnType::Unknown
    }

    fn parse_from_lowercase_prefix(lower_trimmed: &str) -> Option<Self> {
        if let Some(rest) = lower_trimmed.strip_prefix("description/") {
            let langs = rest
                .split('/')
                .map(|lang| lang.trim().to_string())
                .filter(|lang| !lang.is_empty())
                .collect();
            return Some(ColumnType::Description(langs));
        }
        for (prefix, ctor) in [
            ("label/", ColumnType::LabelLang as fn(String) -> Self),
            ("alias/", ColumnType::AliasLang),
        ] {
            if let Some(rest) = lower_trimmed.strip_prefix(prefix) {
                return Some(ctor(rest.to_string()));
            }
        }
        lower_trimmed
            .strip_prefix("sitelink/")
            .map(str::trim)
            .filter(|wiki| !wiki.is_empty())
            .map(|wiki| ColumnType::Sitelink(wiki.to_string()))
    }

    #[must_use]
    pub fn as_key(&self) -> String {
        match self {
            Self::Number => "number".to_string(),
            Self::Label => "label".to_string(),
            Self::Description(_) => "desc".to_string(),
            Self::Item => "item".to_string(),
            Self::Qid => "qid".to_string(),
            Self::LabelLang(l) => format!("language:{l}"),
            Self::AliasLang(l) => format!("alias:{l}"),
            Self::Property(p) => p.to_lowercase(),
            Self::PropertyQualifier((p, q)) => {
                let mut key = p.to_lowercase();
                key.push('_');
                key.push_str(&q.to_lowercase());
                key
            }
            Self::PropertyQualifierValue((p, q, v)) => {
                let mut key = p.to_lowercase();
                key.push('_');
                key.push_str(&q.to_lowercase());
                key.push('_');
                key.push_str(&v.to_lowercase());
                key
            }
            Self::Field(f) => f.to_lowercase(),
            Self::Sitelink(wiki) => format!("sitelink/{wiki}"),
            Self::Unknown => "unknown".to_string(),
        }
    }

    /// Renders the cell parts for a single entity row, returning
    /// `(parts, wdedit_class)`.  This is the only place that needs to change
    /// when a new `ColumnType` variant is added.
    pub async fn render_cell_parts(
        &self,
        list: &impl RenderContext,
        entity_id: &str,
        sparql_table: &SparqlTableVec,
    ) -> (Vec<PartWithReference>, Option<String>) {
        let entity = list.get_entity(entity_id).await;
        let mut parts: Vec<PartWithReference> = Vec::new();
        let mut wdedit_class: Option<String> = None;

        match self {
            Self::Qid => {
                parts.push(PartWithReference::new(
                    ResultCellPart::Text(entity_id.to_string()),
                    None,
                ));
            }
            Self::Item => {
                parts.push(PartWithReference::new(
                    ResultCellPart::Entity(EntityInfo::new(entity_id.to_owned(), false)),
                    None,
                ));
            }
            Self::Number => {
                parts.push(PartWithReference::new(ResultCellPart::Number, None));
            }
            Self::Description(langs) => {
                Self::render_description(&entity, list, langs, &mut parts, &mut wdedit_class);
            }
            Self::Field(varname) => {
                let Some(var_index) = sparql_table.get_var_index(varname) else {
                    return (parts, wdedit_class);
                };
                Self::render_field(var_index, sparql_table, &mut parts);
            }
            Self::Property(property) => {
                Self::render_property(&entity, list, property, &mut parts, &mut wdedit_class);
            }
            Self::PropertyQualifier((p1, p2)) => {
                Self::render_property_qualifier(&entity, list, p1, p2, &mut parts);
            }
            Self::PropertyQualifierValue((p1, q1, p2)) => {
                Self::render_property_qualifier_value(&entity, list, p1, q1, p2, &mut parts);
            }
            Self::LabelLang(language) => {
                Self::render_label_lang(&entity, list, language, &mut parts);
            }
            Self::AliasLang(language) => {
                Self::render_alias_lang(&entity, language, &mut parts);
            }
            Self::Label => {
                Self::render_label(entity, list, entity_id, &mut parts, &mut wdedit_class);
            }
            Self::Sitelink(wiki) => {
                Self::render_sitelink(&entity, wiki, list, &mut parts);
            }
            Self::Unknown => {} // nothing to render
        }

        (parts, wdedit_class)
    }

    fn render_description(
        entity: &Option<EntityEntry>,
        list: &impl RenderContext,
        langs: &[String],
        parts: &mut Vec<PartWithReference>,
        wdedit_class: &mut Option<String>,
    ) {
        let Some(e) = entity else { return };
        let description = if langs.is_empty() {
            e.description_in_locale(list.language())
        } else {
            langs.iter().find_map(|lang| e.description_in_locale(lang))
        };
        match description {
            Some(s) => {
                *wdedit_class = list
                    .header_template()
                    .is_none()
                    .then(|| "wd_desc".to_string());
                let s = Self::fix_wikitext_for_output(s);
                parts.push(PartWithReference::new(ResultCellPart::Text(s), None));
            }
            None => {
                parts.push(PartWithReference::new(
                    ResultCellPart::AutoDesc(AutoDesc::new(e)),
                    None,
                ));
            }
        }
    }

    fn render_property(
        entity: &Option<EntityEntry>,
        list: &impl RenderContext,
        property: &str,
        parts: &mut Vec<PartWithReference>,
        wdedit_class: &mut Option<String>,
    ) {
        let Some(e) = entity else { return };
        *wdedit_class = list
            .header_template()
            .is_none()
            .then(|| format!("wd_{}", property.to_lowercase()));
        for statement in list.get_filtered_claims(e, property) {
            let references = match list.get_reference_parameter() {
                ReferencesParameter::All => {
                    Self::get_references_for_statement(&statement, list.language())
                }
                _ => None,
            };
            parts.push(PartWithReference::new(
                ResultCellPart::from_snak(statement.main_snak()),
                references,
            ));
        }
    }

    fn render_label(
        entity: Option<EntityEntry>,
        list: &impl RenderContext,
        entity_id: &str,
        parts: &mut Vec<PartWithReference>,
        wdedit_class: &mut Option<String>,
    ) {
        let Some(e) = entity else { return };
        *wdedit_class = list
            .header_template()
            .is_none()
            .then(|| "wd_label".to_string());
        let label =
            EntityContainerWrapper::label_with_fallback_from_entity(&e, list.language(), entity_id);
        let local_page = e.sitelinks().as_ref().and_then(|sl| {
            sl.iter()
                .find(|s| *s.site() == *list.wiki())
                .map(|s| s.title().to_string())
        });
        let part = match local_page {
            Some(page) => {
                ResultCellPart::LocalLink(LocalLinkInfo::new(page, label, LinkTarget::Page))
            }
            None => ResultCellPart::Entity(EntityInfo::new(entity_id.to_string(), true)),
        };
        parts.push(PartWithReference::new(part, None));
    }

    fn render_sitelink(
        entity: &Option<EntityEntry>,
        wiki: &str,
        list: &impl RenderContext,
        parts: &mut Vec<PartWithReference>,
    ) {
        let Some(e) = entity else { return };
        let Some(sitelinks) = e.sitelinks().as_ref() else { return };
        let Some(sl) = sitelinks.iter().find(|s| *s.site() == *wiki) else { return };
        let title = sl.title().to_string();
        let part = if wiki == list.wiki() {
            ResultCellPart::LocalLink(LocalLinkInfo::new(
                title.clone(),
                title,
                LinkTarget::Page,
            ))
        } else {
            let prefix = Self::wiki_id_to_interwiki_prefix(wiki);
            let display = title.replace('_', " ");
            ResultCellPart::Text(format!("[[:{}:{}|{}]]", prefix, title, display))
        };
        parts.push(PartWithReference::new(part, None));
    }

    fn render_field(
        var_index: usize,
        sparql_table: &SparqlTableVec,
        parts: &mut Vec<PartWithReference>,
    ) {
        for row_id in 0..sparql_table.len() {
            if let Some(x) = sparql_table.get_row_col(row_id, var_index) {
                parts.push(PartWithReference::new(
                    ResultCellPart::from_sparql_value(&x),
                    None,
                ));
            }
        }
    }

    fn render_property_qualifier(
        entity: &Option<EntityEntry>,
        list: &impl RenderContext,
        p1: &str,
        p2: &str,
        parts: &mut Vec<PartWithReference>,
    ) {
        let Some(e) = entity else { return };
        for statement in list.get_filtered_claims(e, p1) {
            for part in Self::get_parts_p_p(&statement, p2) {
                parts.push(PartWithReference::new(part, None));
            }
        }
    }

    fn render_property_qualifier_value(
        entity: &Option<EntityEntry>,
        list: &impl RenderContext,
        p1: &str,
        q1: &str,
        p2: &str,
        parts: &mut Vec<PartWithReference>,
    ) {
        let Some(e) = entity else { return };
        for statement in list.get_filtered_claims(e, p1) {
            for part in Self::get_parts_p_q_p(&statement, q1, p2) {
                parts.push(PartWithReference::new(part, None));
            }
        }
    }

    fn render_label_lang(
        entity: &Option<EntityEntry>,
        list: &impl RenderContext,
        language: &str,
        parts: &mut Vec<PartWithReference>,
    ) {
        let Some(e) = entity else { return };
        let label = e
            .label_in_locale(language)
            .or_else(|| e.label_in_locale(list.language()))
            .map(|s| s.to_string());
        if let Some(s) = label {
            parts.push(PartWithReference::new(ResultCellPart::Text(s), None));
        }
    }

    fn render_alias_lang(
        entity: &Option<EntityEntry>,
        language: &str,
        parts: &mut Vec<PartWithReference>,
    ) {
        let Some(e) = entity else { return };
        let mut aliases: Vec<String> = e
            .aliases()
            .iter()
            .filter(|alias| alias.language() == language)
            .map(|alias| alias.value().to_string())
            .collect();
        aliases.sort();
        for alias in aliases {
            parts.push(PartWithReference::new(ResultCellPart::Text(alias), None));
        }
    }

    fn fix_wikitext_for_output(s: &str) -> String {
        s.replace('\'', "&#39;").replace('<', "&lt;")
    }

    fn wiki_id_to_interwiki_prefix(wiki: &str) -> String {
        match wiki {
            "commonswiki" => "commons".to_string(),
            "wikidatawiki" => "d".to_string(),
            w if w.ends_with("wiki") => w[..w.len() - 4].to_string(),
            w => w.to_string(),
        }
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

    fn get_references_for_statement(
        statement: &Statement,
        language: &str,
    ) -> Option<Vec<Reference>> {
        let ret: Vec<Reference> = statement
            .references()
            .iter()
            .filter_map(|r| Reference::new_from_snaks(r.snaks(), language))
            .collect();
        if ret.is_empty() { None } else { Some(ret) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- fix_wikitext_for_output ---

    #[test]
    fn test_fix_wikitext_for_output() {
        assert_eq!(ColumnType::fix_wikitext_for_output("a'b<c"), "a&#39;b&lt;c");
    }

    #[test]
    fn test_fix_wikitext_for_output_no_special_chars() {
        assert_eq!(ColumnType::fix_wikitext_for_output("normal text"), "normal text");
    }

    #[test]
    fn test_fix_wikitext_for_output_only_apostrophe() {
        assert_eq!(ColumnType::fix_wikitext_for_output("it's"), "it&#39;s");
    }

    #[test]
    fn test_fix_wikitext_for_output_only_less_than() {
        assert_eq!(ColumnType::fix_wikitext_for_output("a<b"), "a&lt;b");
    }

    #[test]
    fn test_fix_wikitext_for_output_multiple_apostrophes() {
        assert_eq!(ColumnType::fix_wikitext_for_output("it's John's"), "it&#39;s John&#39;s");
    }

    #[test]
    fn test_fix_wikitext_for_output_multiple_less_than() {
        assert_eq!(ColumnType::fix_wikitext_for_output("a<b<c"), "a&lt;b&lt;c");
    }

    #[test]
    fn test_fix_wikitext_for_output_empty_string() {
        assert_eq!(ColumnType::fix_wikitext_for_output(""), "");
    }

    #[test]
    fn test_fix_wikitext_for_output_html_tag() {
        assert_eq!(
            ColumnType::fix_wikitext_for_output("<script>alert('hi')</script>"),
            "&lt;script>alert(&#39;hi&#39;)&lt;/script>"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_wikitext_bold() {
        assert_eq!(
            ColumnType::fix_wikitext_for_output("'''bold'''"),
            "&#39;&#39;&#39;bold&#39;&#39;&#39;"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_comparison() {
        assert_eq!(ColumnType::fix_wikitext_for_output("1<2"), "1&lt;2");
    }

    #[test]
    fn test_fix_wikitext_for_output_unicode_with_special() {
        assert_eq!(
            ColumnType::fix_wikitext_for_output("日本's data<test"),
            "日本&#39;s data&lt;test"
        );
    }

    #[test]
    fn test_fix_wikitext_for_output_consecutive_special() {
        assert_eq!(ColumnType::fix_wikitext_for_output("'<'<"), "&#39;&lt;&#39;&lt;");
    }

    #[test]
    fn test_fix_wikitext_for_output_greater_than_unchanged() {
        assert_eq!(ColumnType::fix_wikitext_for_output("a>b"), "a>b");
    }

    #[test]
    fn test_fix_wikitext_for_output_double_quote_unchanged() {
        assert_eq!(ColumnType::fix_wikitext_for_output("\"quoted\""), "\"quoted\"");
    }

    #[test]
    fn test_fix_wikitext_for_output_ampersand_unchanged() {
        assert_eq!(ColumnType::fix_wikitext_for_output("a&b"), "a&b");
    }

    // --- wiki_id_to_interwiki_prefix ---

    #[test]
    fn test_wiki_id_to_interwiki_prefix_standard() {
        assert_eq!(ColumnType::wiki_id_to_interwiki_prefix("enwiki"), "en");
        assert_eq!(ColumnType::wiki_id_to_interwiki_prefix("dewiki"), "de");
        assert_eq!(ColumnType::wiki_id_to_interwiki_prefix("frwiki"), "fr");
    }

    #[test]
    fn test_wiki_id_to_interwiki_prefix_special() {
        assert_eq!(ColumnType::wiki_id_to_interwiki_prefix("commonswiki"), "commons");
        assert_eq!(ColumnType::wiki_id_to_interwiki_prefix("wikidatawiki"), "d");
    }
}
