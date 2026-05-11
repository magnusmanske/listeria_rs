//! Wikitext and tabbed-data rendering for `ResultCellPart`.
//!
//! These methods turn a typed cell part into output strings using a
//! `RenderContext`. Pure data transforms (Snak/SPARQL conversion) live in
//! `from_snak.rs`; the data carriers themselves live in `types.rs`.

use super::{LinkTarget, LocationInfo, PartWithReference, ResultCellPart};
use crate::column_type::ColumnType;
use crate::render_context::{normalize_page_title, RenderContext};
use crate::template_params::LinksType;
use futures::future::join_all;
use wikimisc::wikibase::entity::EntityTrait;

impl ResultCellPart {
    fn tabbed_string_safe(s: String) -> String {
        let mut ret = s.replace(['\n', '\t'], " ");

        // limit string to ~400 bytes max. Use the largest char boundary at or
        // below 380 so we never slice inside a multi-byte UTF-8 code point
        // (which would panic in String::truncate).
        if ret.len() > 380 {
            let mut cut = 380;
            while cut > 0 && !ret.is_char_boundary(cut) {
                cut -= 1;
            }
            ret.truncate(cut);
        }
        ret
    }

    async fn as_wikitext_entity(
        &self,
        list: &impl RenderContext,
        id: &str,
        try_localize: bool,
        colnum: usize,
    ) -> String {
        if !try_localize {
            let is_item_column = list
                .column(colnum)
                .is_some_and(|col| *col.obj() == ColumnType::Item);

            let target = list.get_item_wiki_target(id);
            return if list.is_main_wikibase_wiki() || is_item_column {
                format!("[[{target}|{id}]]")
            } else {
                format!("''[[{target}|{id}]]''")
            };
        }

        let entity_id_link = list.get_item_link_with_fallback(id).await;
        let Some(entity) = list.get_entity(id).await else {
            return entity_id_link;
        };

        let use_language = entity
            .label_in_locale(list.language())
            .map_or_else(|| list.default_language(), |_| list.language().to_string());

        let use_label = list.get_label_with_fallback_lang(id, &use_language).await;
        let target = list.get_item_wiki_target(id);
        let labeled_entity_link = if list.is_main_wikibase_wiki() {
            format!("[[{target}|{use_label}]]")
        } else {
            format!("''[[{target}|{use_label}]]''")
        };

        Self::render_entity_link(list, use_label, id, labeled_entity_link)
    }

    fn as_wikitext_local_link(
        list: &impl RenderContext,
        title: &str,
        label: &str,
        link_target: &LinkTarget,
    ) -> String {
        let start = if matches!(link_target, LinkTarget::Category) {
            "[[:"
        } else {
            "[["
        };

        let norm_title = normalize_page_title(title);
        let norm_page = normalize_page_title(list.page_title()).replace(' ', "_");

        if norm_page == norm_title.replace(' ', "_") {
            label.to_string()
        } else if norm_title == normalize_page_title(label) {
            format!("{start}{label}]]")
        } else {
            format!("{start}{title}|{label}]]")
        }
    }

    async fn as_wikitext_location(
        list: &impl RenderContext,
        loc_info: &LocationInfo,
        rownum: usize,
    ) -> String {
        // Prefer the explicitly-assigned, page-unique anchor name when
        // available (see ListProcessor::process_assign_location_names). Fall
        // back to the row's entity_id for safety if no name has been
        // assigned (e.g. in tests that bypass the processing pipeline).
        let entity_id = list
            .results()
            .get(rownum)
            .map(|e| e.entity_id().to_string());
        let name = loc_info.name.clone().or_else(|| entity_id.clone());
        let label = match &entity_id {
            Some(id) => {
                let l = list
                    .ecw()
                    .get_entity_label_with_fallback(id, list.language())
                    .await;
                if l == *id { None } else { Some(l) }
            }
            None => None,
        };
        list.get_location_template(
            loc_info.latitude,
            loc_info.longitude,
            name,
            loc_info.region.clone(),
            label,
        )
    }

    fn as_wikitext_file(list: &impl RenderContext, file: &str) -> String {
        let thumb = list.thumbnail_size();
        format!(
            "[[{}:{}|center|{}px]]",
            list.local_file_namespace_prefix(),
            file,
            thumb
        )
    }

    async fn as_wikitext_external_id(
        list: &impl RenderContext,
        property: &str,
        id: &str,
    ) -> String {
        match list.external_id_url(property, id).await {
            Some(url) => format!("[{url} {id}]"),
            None => id.to_string(),
        }
    }

    /// Converts a URI to wikitext. Wikipedia article URLs become interwiki
    /// links (`[[:en:Title|Title]]`); all other URIs are rendered as-is.
    fn uri_to_wikitext(url: &str) -> String {
        Self::wikipedia_url_to_wikilink(url).unwrap_or_else(|| url.to_string())
    }

    /// Parses `https://{lang}.wikipedia.org/wiki/{title}` and returns a
    /// MediaWiki interwiki link string, or `None` if the URL doesn't match.
    fn wikipedia_url_to_wikilink(url: &str) -> Option<String> {
        let without_scheme = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))?;
        let (host, path) = without_scheme.split_once('/')?;
        let lang = host.strip_suffix(".wikipedia.org")?;
        if lang.is_empty() {
            return None;
        }
        let title = path.strip_prefix("wiki/")?;
        // Drop any fragment (#Section) from the title
        let title = title.split('#').next().unwrap_or(title);
        if title.is_empty() {
            return None;
        }
        let display = title.replace('_', " ");
        Some(format!("[[:{}:{}|{}]]", lang, title, display))
    }

    fn as_wikitext_text(list: &impl RenderContext, text: &str, colnum: usize) -> String {
        // Newlines in cell values break wiki table structure: MediaWiki ends the
        // cell at the first bare newline, and lines starting with a space are
        // rendered as pre-formatted code blocks. Replace \n with <br/> to keep
        // multi-line values readable without corrupting the table (#98).
        let text = &text.replace('\n', "<br/>");
        list.column(colnum)
            .and_then(|col| match col.obj() {
                ColumnType::Property(p) if p == "P373" => {
                    Some(format!("[[:commons:Category:{text}|{text}]]"))
                }
                _ => None,
            })
            .unwrap_or_else(|| text.to_string())
    }

    async fn as_wikitext_snak_list(
        v: &[PartWithReference],
        list: &impl RenderContext,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let futures: Vec<_> = v
            .iter()
            .map(|rcp| rcp.part().as_wikitext(list, rownum, colnum))
            .collect();
        join_all(futures).await.join(" — ")
    }

    pub async fn as_wikitext(
        &self,
        list: &impl RenderContext,
        rownum: usize,
        colnum: usize,
    ) -> String {
        match self {
            ResultCellPart::Number => format!("style='text-align:right'| {}", rownum + 1),
            ResultCellPart::Entity(entity_info) => {
                self.as_wikitext_entity(list, &entity_info.id, entity_info.try_localize, colnum)
                    .await
            }
            ResultCellPart::EntitySchema(id) => {
                // `get_entity_label_with_fallback` returns the id itself when
                // no label is available, so callers who never load the schema
                // entity (the common case) see the same output as before.
                let label = list
                    .ecw()
                    .get_entity_label_with_fallback(id, list.language())
                    .await;
                format!("[[EntitySchema:{id}|{label}]]")
            }
            ResultCellPart::LocalLink(link_info) => Self::as_wikitext_local_link(
                list,
                &link_info.page,
                &link_info.label,
                &link_info.target,
            ),
            ResultCellPart::Time(time, _year) => time.clone(),
            ResultCellPart::Location(loc_info) => {
                Self::as_wikitext_location(list, loc_info, rownum).await
            }
            ResultCellPart::File(file) => Self::as_wikitext_file(list, file),
            ResultCellPart::Uri(url) => Self::uri_to_wikitext(url),
            ResultCellPart::ExternalId(ext_id_info) => {
                Self::as_wikitext_external_id(list, &ext_id_info.property, &ext_id_info.id).await
            }
            ResultCellPart::Text(text) => Self::as_wikitext_text(list, text, colnum),
            ResultCellPart::SnakList(v) => Self::as_wikitext_snak_list(v, list, rownum, colnum).await,
            ResultCellPart::AutoDesc(ad) => ad.desc().unwrap_or_default().to_string(),
            ResultCellPart::Quantity(amount, unit_id) => {
                self.as_wikitext_quantity(list, *amount, unit_id.as_deref()).await
            }
        }
    }

    async fn as_wikitext_quantity(
        &self,
        list: &impl RenderContext,
        amount: f64,
        unit_id: Option<&str>,
    ) -> String {
        let amount_str = amount.to_string();
        match unit_id {
            Some(uid) => {
                let label = list
                    .ecw()
                    .get_entity_label_with_fallback(uid, list.language())
                    .await;
                format!("{amount_str} {label}")
            }
            None => amount_str,
        }
    }

    pub async fn as_tabbed_data(
        &self,
        list: &impl RenderContext,
        rownum: usize,
        colnum: usize,
    ) -> String {
        Self::tabbed_string_safe(self.as_wikitext(list, rownum, colnum).await)
    }

    fn render_entity_link(
        list: &impl RenderContext,
        use_label: String,
        id: &str,
        labeled_entity_link: String,
    ) -> String {
        match list.get_links_type() {
            LinksType::Text => use_label,
            LinksType::Red | LinksType::RedOnly => {
                // For categories/namespaced labels use the colon prefix to avoid
                // a category inclusion; for everything else use a plain link.
                // MediaWiki determines blue vs. red based on page existence;
                // we no longer synthesise a "(Q-id)" page that rarely exists (#137).
                if use_label.contains(':') {
                    format!("[[:{}|]]", &use_label)
                } else {
                    format!("[[{}]]", &use_label)
                }
            }
            LinksType::Reasonator => {
                format!("[https://reasonator.toolforge.org/?q={id} {use_label}]")
            }
            _ => labeled_entity_link,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- tabbed_string_safe ---

    #[test]
    fn test_tabbed_string_safe_removes_newlines() {
        let input = "line1\nline2\nline3".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "line1 line2 line3");
    }

    #[test]
    fn test_tabbed_string_safe_removes_tabs() {
        let input = "col1\tcol2\tcol3".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "col1 col2 col3");
    }

    #[test]
    fn test_tabbed_string_safe_removes_both() {
        let input = "line1\tcol1\nline2\tcol2".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "line1 col1 line2 col2");
    }

    #[test]
    fn test_tabbed_string_safe_short_string() {
        let input = "short string".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "short string");
    }

    #[test]
    fn test_tabbed_string_safe_long_string_truncated() {
        let input = "a".repeat(500);
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result.len(), 380);
    }

    #[test]
    fn test_tabbed_string_safe_exactly_380() {
        let input = "b".repeat(380);
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result.len(), 380);
    }

    #[test]
    fn test_tabbed_string_safe_empty() {
        let result = ResultCellPart::tabbed_string_safe(String::new());
        assert_eq!(result, "");
    }

    #[test]
    fn test_tabbed_string_safe_multibyte_truncation_boundary() {
        // '€' is 3 bytes in UTF-8. 130 copies = 390 bytes. Byte 380 lies in
        // the middle of a multibyte code point, so a naive truncate(380) would
        // panic. Verify that tabbed_string_safe keeps the truncation on a char
        // boundary and leaves a valid UTF-8 string <= 380 bytes.
        let input = "€".repeat(130);
        assert_eq!(input.len(), 390);
        let result = ResultCellPart::tabbed_string_safe(input);
        assert!(result.len() <= 380);
        // Confirm the result is still valid UTF-8 (it is, because it's a
        // String, but also make sure none of the '€' characters were cut off
        // mid-way — the length must be divisible by 3).
        assert_eq!(result.len() % 3, 0);
    }

    // --- wikipedia_url_to_wikilink (#138) ---

    #[test]
    fn test_wikipedia_url_to_wikilink_basic() {
        let result = ResultCellPart::wikipedia_url_to_wikilink(
            "https://en.wikipedia.org/wiki/Obelisk_(biology)",
        );
        assert_eq!(
            result,
            Some("[[:en:Obelisk_(biology)|Obelisk (biology)]]".to_string())
        );
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_other_lang() {
        let result =
            ResultCellPart::wikipedia_url_to_wikilink("https://de.wikipedia.org/wiki/Berlin");
        assert_eq!(result, Some("[[:de:Berlin|Berlin]]".to_string()));
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_with_fragment() {
        let result =
            ResultCellPart::wikipedia_url_to_wikilink("https://en.wikipedia.org/wiki/Foo#Section");
        assert_eq!(result, Some("[[:en:Foo|Foo]]".to_string()));
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_non_wikipedia_url() {
        let result =
            ResultCellPart::wikipedia_url_to_wikilink("https://example.com/wiki/Foo");
        assert_eq!(result, None);
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_empty_title() {
        let result = ResultCellPart::wikipedia_url_to_wikilink("https://en.wikipedia.org/wiki/");
        assert_eq!(result, None);
    }
}
