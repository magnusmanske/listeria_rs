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

    // ── entity access (async) ─────────────────────────────────────────────
    async fn get_entity(&self, entity_id: &str) -> Option<EntityEntry>;
    async fn get_item_link_with_fallback(&self, entity_id: &str) -> String;
    async fn get_label_with_fallback_lang(&self, entity_id: &str, language: &str) -> String;
    async fn external_id_url(&self, prop: &str, id: &str) -> Option<String>;
}
