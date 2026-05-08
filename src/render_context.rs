//! Narrow rendering context trait used by cell-part and cell rendering.
//!
//! Decouples the rendering pipeline (`ResultCellPart`, `ResultCell`,
//! `Reference`) from the concrete `ListeriaList` type, making each
//! renderer independently testable and easier to extend.

use crate::column::Column;
use crate::entity_container_wrapper::{EntityContainerWrapper, EntityEntry};
use crate::my_entity::MyEntity;
use crate::result_row::ResultRow;
use crate::template_params::{LinksType, ReferencesParameter, TemplateParams};
use crate::wiki::Wiki;
use std::collections::HashSet;
use wikimisc::wikibase::Statement;

/// Methods from `ListeriaList` that the rendering layer needs.
///
/// Implement this trait on any type you want to pass to `ResultCellPart`,
/// `ResultCell`, or `Reference` rendering methods. `ListeriaList` is the
/// only production implementation; tests can provide a lightweight mock.
#[allow(async_fn_in_trait)]
pub trait RenderContext {
    // ── locale & identity ────────────────────────────────────────────────
    fn language(&self) -> &str;
    fn default_language(&self) -> String;
    fn page_title(&self) -> &str;
    fn wiki(&self) -> &str;
    fn is_main_wikibase_wiki(&self) -> bool;

    // ── link / rendering preferences ─────────────────────────────────────
    fn get_links_type(&self) -> &LinksType;
    fn header_template(&self) -> &Option<String>;
    fn template_params(&self) -> &TemplateParams;
    fn get_reference_parameter(&self) -> &ReferencesParameter;
    fn thumbnail_size(&self) -> u64;
    fn local_file_namespace_prefix(&self) -> &str;

    // ── column / result access ────────────────────────────────────────────
    fn column(&self, colnum: usize) -> Option<&Column>;
    fn results(&self) -> &Vec<ResultRow>;
    fn reference_ids(&self) -> &std::collections::HashSet<String>;

    // ── wiki info ─────────────────────────────────────────────────────────
    fn get_wiki(&self) -> Option<Wiki>;
    fn get_item_wiki_target(&self, entity_id: &str) -> String;
    fn get_location_template(
        &self,
        lat: f64,
        lon: f64,
        entity_id: Option<String>,
        region: Option<String>,
        label: Option<String>,
    ) -> String;

    // ── entity access (sync) ──────────────────────────────────────────────
    fn ecw(&self) -> &EntityContainerWrapper;
    fn get_filtered_claims(&self, entity: &MyEntity, property: &str) -> Vec<Statement>;

    // ── section / layout access ───────────────────────────────────────────
    fn columns(&self) -> &Vec<Column>;
    fn get_section_ids(&self) -> Vec<usize>;
    fn shadow_files(&self) -> &HashSet<String>;
    fn summary(&self) -> &Option<String>;
    fn skip_table(&self) -> bool;
    fn get_row_template(&self) -> &Option<String>;
    fn section_name(&self, id: usize) -> Option<&str>;

    // ── entity access (async) ─────────────────────────────────────────────
    async fn get_entity(&self, entity_id: &str) -> Option<EntityEntry>;
    async fn get_item_link_with_fallback(&self, entity_id: &str) -> String;
    async fn get_label_with_fallback_lang(&self, entity_id: &str, language: &str) -> String;
    async fn external_id_url(&self, prop: &str, id: &str) -> Option<String>;
}

/// Capitalises the first letter of a page title for link normalisation.
///
/// Returns the string unchanged when it has fewer than 2 bytes (avoids
/// uppercasing lone ASCII punctuation or single combining code points).
/// TODO: consult per-wiki capitalisation config instead of always uppercasing.
pub(crate) fn normalize_page_title(s: &str) -> String {
    if s.len() < 2 {
        return s.to_string();
    }
    let mut c = s.chars();
    c.next()
        .map(|f| f.to_uppercase().collect::<String>() + c.as_str())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::normalize_page_title;

    #[test]
    fn test_empty_string() {
        assert_eq!(normalize_page_title(""), "");
    }

    #[test]
    fn test_single_ascii_char_unchanged() {
        assert_eq!(normalize_page_title("h"), "h");
    }

    #[test]
    fn test_two_chars_lowercase_uppercased() {
        assert_eq!(normalize_page_title("ab"), "Ab");
    }

    #[test]
    fn test_already_uppercase_unchanged() {
        assert_eq!(normalize_page_title("Hello world"), "Hello world");
    }

    #[test]
    fn test_lowercase_first_char_uppercased() {
        assert_eq!(normalize_page_title("hello world"), "Hello world");
    }

    #[test]
    fn test_namespace_prefix_uppercased() {
        assert_eq!(normalize_page_title("category:test"), "Category:test");
    }

    #[test]
    fn test_unicode_multibyte_first_char() {
        assert_eq!(normalize_page_title("über alles"), "Über alles");
    }

    #[test]
    fn test_digit_first_char_unchanged() {
        // digits have no uppercase form, remain unchanged
        assert_eq!(normalize_page_title("123 abc"), "123 abc");
    }

    #[test]
    fn test_already_uppercase_unicode() {
        assert_eq!(normalize_page_title("Über etwas"), "Über etwas");
    }

    #[test]
    fn test_rest_of_string_preserved() {
        // Only the very first character changes; the rest is untouched
        assert_eq!(normalize_page_title("aBcDe"), "ABcDe");
    }
}
