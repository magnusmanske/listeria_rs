//! Renders results as MediaWiki wikitext tables.

use crate::{
    coop::ITEMS_PER_YIELD, listeria_page::ListeriaPage, render_context::RenderContext,
    renderer::Renderer,
};
use anyhow::Result;
use futures::future::join_all;

#[derive(Debug, Clone, Copy)]
pub struct RendererWikitext;

impl RendererWikitext {
    pub const fn new() -> Self {
        Self
    }
}

impl Default for RendererWikitext {
    fn default() -> Self {
        Self::new()
    }
}

impl Renderer for RendererWikitext {
    async fn render<C: RenderContext>(&mut self, list: &C) -> Result<String> {
        let mut wt = String::new();
        let section_ids = list.get_section_ids();
        if section_ids.is_empty() {
            // Codeberg #15: an empty SPARQL result set must still render an
            // empty table (header + `|}`) so the on-wiki page shows that the
            // list is intentionally empty, rather than collapsing to two bare
            // `{{Wikidata list…}}` markers with nothing between them. The
            // section_id passed here is sentinel `0`; no row references it,
            // so `process_rows` is a no-op.
            //
            // `skip_table` and the `header_template+row_template` mode opt out
            // entirely (they manage their own framing), so skip the fallback
            // for them too.
            let uses_external_template =
                list.get_row_template().is_some() && list.header_template().is_some();
            if !list.skip_table() && !uses_external_template {
                wt += &self.as_wikitext_section(list, 0).await;
            }
        } else {
            for section_id in section_ids {
                wt += &self.as_wikitext_section(list, section_id).await;
            }
        }
        if !list.shadow_files().is_empty() {
            wt += "\n----\nThe following local image(s) are not shown in the above list, because they shadow a Commons image of the same name, and might be non-free:";
            let mut shadow_files: Vec<String> = list.shadow_files().iter().cloned().collect();
            shadow_files.sort(); // For prettier, consistent display
            for file in shadow_files {
                wt += format!("\n# [[:{}:{}|]]", list.local_file_namespace_prefix(), file).as_str();
            }
        }

        if let Some("ITEMNUMBER") = list.summary().as_deref() {
            let label = list.template_params().summary_label();
            wt += format!("\n----\n&sum; {} {label}.", list.results().len()).as_str();
        }

        // Optional user-supplied class on a wrapper around the complete list
        // output. Opt-in only, so existing pages are byte-identical.
        if let Some(classes) = list
            .template_params()
            .list_class()
            .and_then(Self::sanitize_classes)
        {
            wt = format!("<div class=\"{classes}\">\n{wt}\n</div>");
        }

        Ok(wt)
    }

    async fn get_new_wikitext(
        &self,
        _wikitext: &str,
        page: &ListeriaPage,
    ) -> Result<Option<String>> {
        let mut new_wikitext = String::new();
        for element in page.elements() {
            let mut element = element.clone();
            if let Ok(s) = element.as_wikitext().await {
                new_wikitext += &s;
            }
        }
        Ok(Some(new_wikitext))
    }
}

impl RendererWikitext {
    async fn as_wikitext_section<C: RenderContext>(&self, list: &C, section_id: usize) -> String {
        let mut wt = String::new();

        if let Some(name) = list.section_name(section_id) {
            wt += &Self::render_header(name);
        }

        wt += &Self::as_wikitext_table_header(list);

        if !(list.skip_table()
            || list.results().is_empty()
            || list.template_params().wdedit()
            || list.get_row_template().is_some() && list.header_template().is_some())
        {
            wt += "|-\n";
        }

        Self::process_rows(list, section_id, &mut wt).await;

        // End
        if !list.skip_table() {
            wt += "\n|}";
        }

        if let Some(footer) = list.footer_template() {
            wt += "\n";
            wt += &Self::render_template_value(footer);
        }

        wt
    }

    fn as_wikitext_table_header<C: RenderContext>(list: &C) -> String {
        let mut wt = String::new();
        match &list.header_template() {
            Some(t) => {
                wt += &Self::render_template_value(t);
                wt += "\n";
            }
            None => {
                if !list.skip_table() {
                    wt += "{| class='wikitable sortable";
                    if list.template_params().wdedit() {
                        wt += " wd_can_edit";
                    }
                    // Optional extra classes (`tableclass=`), filtered to safe
                    // class-name tokens so the attribute cannot be broken out of.
                    if let Some(classes) = list
                        .template_params()
                        .table_class()
                        .and_then(Self::sanitize_classes)
                    {
                        wt += " ";
                        wt += &classes;
                    }
                    wt += "'";
                    // Optional `width=` attribute, set via `tablewidth=` template
                    // parameter (codeberg #32). Historical default has no
                    // explicit width attribute, so `None` produces no addition.
                    if let Some(w) = list.template_params().table_width() {
                        wt += &format!(" width='{w}'");
                    }
                    wt += "\n";
                    list.columns().iter().for_each(|col| {
                        wt += "! ";
                        wt += col.label();
                        wt += "\n";
                    });
                }
            }
        }
        wt
    }

    /// Renders a `header_template`/`footer_template` value as a template
    /// invocation.
    ///
    /// A bare template name (`My template`) is wrapped as `{{My template}}`,
    /// preserving the historic behaviour. A value that already looks like a
    /// complete invocation (`{{My template|class=x}}`) is emitted verbatim, so
    /// callers can pass parameters — including CSS classes — without having to
    /// create a wrapper template.
    fn render_template_value(value: &str) -> String {
        let value = value.trim();
        if value.starts_with("{{") && value.ends_with("}}") {
            value.to_string()
        } else {
            format!("{{{{{value}}}}}")
        }
    }

    /// Keeps only whitespace-separated class-name tokens made of ASCII
    /// alphanumerics, `-` and `_`, so a user-supplied `tableclass`/`listclass`
    /// cannot break out of the HTML attribute it is placed in.
    fn sanitize_classes(value: &str) -> Option<String> {
        let classes: Vec<&str> = value
            .split_whitespace()
            .filter(|c| {
                !c.is_empty()
                    && c.chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
            })
            .collect();
        if classes.is_empty() {
            None
        } else {
            Some(classes.join(" "))
        }
    }

    fn render_header(name: &str) -> String {
        if name.trim().is_empty() {
            "\n\n\n".to_string()
        } else {
            format!("\n\n\n== {name} ==\n")
        }
    }

    async fn process_rows<C: RenderContext>(list: &C, section_id: usize, wt: &mut String) {
        // Collect (global_index, row) pairs for this section so that each row
        // is rendered with its global position in list.results(). This is
        // required by as_wikitext_location, which looks up the entity_id from
        // list.results()[rownum]; using a section-local sub-index would return
        // the wrong entity and produce incorrect coordinate template names
        // (see GitHub issue #136).
        let section_rows: Vec<(usize, &crate::result_row::ResultRow)> = list
            .results()
            .iter()
            .enumerate()
            .filter(|(_, row)| row.section() == section_id)
            .collect();

        // Render in chunks, yielding between them. Rendering a row is pure
        // CPU work with no await point, so one `join_all` over the whole
        // section would hold the runtime worker for the entire table: on a
        // small CPU quota (often a single worker thread) that starves timers
        // and the telemetry push for minutes, and a page shows as "stale".
        // Chunking also keeps only one chunk's strings alive at a time.
        let mut rows = Vec::with_capacity(section_rows.len());
        for chunk in section_rows.chunks(ITEMS_PER_YIELD) {
            let futures: Vec<_> = chunk
                .iter()
                .map(|(global_idx, row)| row.as_wikitext(list, *global_idx))
                .collect();
            rows.extend(join_all(futures).await);
            tokio::task::yield_now().await;
        }

        if list.skip_table() {
            *wt += &rows.join("\n");
        } else if list.template_params().wdedit() {
            let x: Vec<String> = section_rows
                .iter()
                .map(|(_, row)| row)
                .zip(rows.iter())
                .map(|(row, rendered)| match &list.header_template() {
                    Some(_) => rendered.to_string(),
                    None => format!(
                        "\n|- class='wd_{}'\n{}",
                        row.entity_id().to_lowercase(),
                        rendered
                    ),
                })
                .collect();
            *wt += x.join("").trim();
        } else {
            *wt += &rows.join("\n|-\n");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listeria_list::ListeriaList;
    use crate::page_params::PageParams;
    use crate::template::Template;
    use std::sync::Arc;

    async fn create_test_list_with_template(template_text: &str) -> ListeriaList {
        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
        let page_params = Arc::new(
            PageParams::new(config, api, "Test:Page".to_string())
                .await
                .unwrap(),
        );
        let template = Template::new_from_params(template_text).unwrap();
        let mut list = ListeriaList::new(template, page_params).await.unwrap();
        // ListeriaList::new uses default TemplateParams; the template's actual
        // params are only applied by process_template (called inside process()).
        // Tests that inspect template-derived behaviour must call it explicitly.
        list.process_template().expect("process_template succeeds");
        list
    }

    #[tokio::test]
    async fn test_empty_results_render_empty_table_with_header() {
        // Codeberg #15: an empty SPARQL result set must still produce a
        // wikitable (with column headers only) — otherwise the on-wiki page
        // collapses to bare `{{Wikidata list…}}` markers and the reader
        // can't tell whether the bot ran at all.
        //
        // Note: Template::new_from_params expects the body of the template
        // (no surrounding `{{...}}`); pipes inside un-balanced braces are
        // treated as literal text by the parser. PageElement strips the
        // wrapping in production; we mirror that here.
        let list = create_test_list_with_template(
            "Wikidata list|columns=label,item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(
            wt.contains("{| class='wikitable sortable'"),
            "empty result must still open a wikitable, got: {wt:?}"
        );
        assert!(
            wt.trim_end().ends_with("|}"),
            "empty result must close the wikitable, got: {wt:?}"
        );
        // No data row separators should appear; only headers.
        assert!(
            !wt.contains("|-\n|"),
            "no row separators should appear in an empty table, got: {wt:?}"
        );
    }

    #[tokio::test]
    async fn test_tablewidth_parameter_emits_width_attribute() {
        // Codeberg #32: `tablewidth=` must surface as a `width='...'`
        // attribute on the rendered `{|` wikitable opener.
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|tablewidth=80%|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(
            wt.contains("width='80%'"),
            "tablewidth parameter must produce width attribute, got: {wt:?}"
        );
    }

    #[tokio::test]
    async fn test_no_tablewidth_means_no_width_attribute() {
        // Default behaviour (no `tablewidth=`) preserves historical output —
        // no `width=` attribute on the wikitable. Codeberg #32 added the
        // opt-in; the historic default must not change.
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(
            !wt.contains("width="),
            "no tablewidth parameter must not emit width attribute, got: {wt:?}"
        );
    }

    #[tokio::test]
    async fn test_empty_results_with_skip_table_renders_nothing() {
        // `skip_table=` opts the whole list out of table framing; an empty
        // result in that mode must stay empty (don't inject a table the
        // user explicitly disabled).
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|skip_table=1|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(
            !wt.contains("{| class='wikitable"),
            "skip_table must not emit a wikitable even on empty results, got: {wt:?}"
        );
    }

    #[tokio::test]
    async fn test_footer_template_appended_after_table() {
        // The closing counterpart must land after the automatic table close,
        // i.e. outside the table / full width.
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|footer_template=My footer|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(
            wt.trim_end().ends_with("|}\n{{My footer}}"),
            "footer must follow the table close, got: {wt:?}"
        );
    }

    #[tokio::test]
    async fn test_footer_template_brackets_rows_with_skip_table() {
        // `header_template` + rows + `footer_template` + `skip_table` is the
        // generic envelope mode (e.g. card lists).
        let mut list = create_test_list_with_template(
            "Wikidata list|columns=item|header_template=Open|footer_template=Close|skip_table=yes|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        list.results_mut()
            .push(crate::result_row::ResultRow::new("Q42"));
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert_eq!(wt.trim(), "{{Open}}\n|\n{{Close}}");
    }

    #[tokio::test]
    async fn test_header_and_footer_accept_full_invocations() {
        // A value that already looks like a full invocation is emitted as-is,
        // so users can pass template parameters (e.g. CSS classes).
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|header_template={{Open|class=top}}|footer_template={{Close|class=bottom}}|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(wt.contains("{{Open|class=top}}"), "got: {wt:?}");
        assert!(wt.contains("{{Close|class=bottom}}"), "got: {wt:?}");
        assert!(!wt.contains("{{{{"), "must not double-wrap, got: {wt:?}");
    }

    #[tokio::test]
    async fn test_bare_template_names_are_still_wrapped() {
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|header_template=My header|footer_template=My footer|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(wt.contains("{{My header}}"), "got: {wt:?}");
        assert!(wt.contains("{{My footer}}"), "got: {wt:?}");
    }

    #[tokio::test]
    async fn test_table_class_is_added_to_opener() {
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|tableclass=glam striped|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(
            wt.contains("{| class='wikitable sortable glam striped'"),
            "got: {wt:?}"
        );
    }

    #[tokio::test]
    async fn test_table_class_cannot_break_out_of_attribute() {
        // Quotes are balanced in the template (the parser rejects unclosed
        // ones), but a quoted token would still break the HTML attribute.
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|tableclass=ok'x'|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(!wt.contains("ok'x'"), "got: {wt:?}");
        assert!(wt.contains("{| class='wikitable sortable'"), "got: {wt:?}");
    }

    #[tokio::test]
    async fn test_list_class_wraps_whole_output() {
        let list = create_test_list_with_template(
            "Wikidata list|columns=item|listclass=glam-block|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }",
        )
        .await;
        let mut renderer = RendererWikitext::new();
        let wt = renderer.render(&list).await.expect("render succeeds");
        assert!(
            wt.starts_with("<div class=\"glam-block\">\n"),
            "got: {wt:?}"
        );
        assert!(wt.trim_end().ends_with("</div>"), "got: {wt:?}");
    }

    #[test]
    fn test_render_template_value() {
        assert_eq!(
            RendererWikitext::render_template_value("My template"),
            "{{My template}}"
        );
        assert_eq!(
            RendererWikitext::render_template_value("{{My template|class=x}}"),
            "{{My template|class=x}}"
        );
        assert_eq!(
            RendererWikitext::render_template_value("  My template  "),
            "{{My template}}"
        );
    }

    #[test]
    fn test_sanitize_classes() {
        assert_eq!(
            RendererWikitext::sanitize_classes("glam  striped").as_deref(),
            Some("glam striped")
        );
        assert_eq!(
            RendererWikitext::sanitize_classes("ok'onmouseover=evil"),
            None
        );
        assert_eq!(RendererWikitext::sanitize_classes("   "), None);
    }

    #[test]
    fn test_render_header() {
        assert_eq!(RendererWikitext::render_header("foo"), "\n\n\n== foo ==\n");
        assert_eq!(RendererWikitext::render_header(""), "\n\n\n");
        assert_eq!(RendererWikitext::render_header("  "), "\n\n\n");
    }

    #[test]
    fn test_render_header_with_special_chars() {
        assert_eq!(
            RendererWikitext::render_header("Test & Section"),
            "\n\n\n== Test & Section ==\n"
        );
    }

    #[test]
    fn test_render_header_with_unicode() {
        assert_eq!(
            RendererWikitext::render_header("日本語"),
            "\n\n\n== 日本語 ==\n"
        );
    }

    #[test]
    fn test_render_header_with_wikitext() {
        assert_eq!(
            RendererWikitext::render_header("Section [[link]]"),
            "\n\n\n== Section [[link]] ==\n"
        );
    }

    #[test]
    fn test_render_header_with_tabs() {
        assert_eq!(RendererWikitext::render_header("\t\t"), "\n\n\n");
    }

    #[test]
    fn test_render_header_with_newlines() {
        assert_eq!(
            RendererWikitext::render_header("Multi\nLine"),
            "\n\n\n== Multi\nLine ==\n"
        );
    }

    #[test]
    fn test_render_header_long_title() {
        let long_title = "A".repeat(100);
        assert_eq!(
            RendererWikitext::render_header(&long_title),
            format!("\n\n\n== {} ==\n", long_title)
        );
    }

    #[test]
    fn test_render_header_with_leading_trailing_spaces() {
        assert_eq!(
            RendererWikitext::render_header("  Section  "),
            "\n\n\n==   Section   ==\n"
        );
    }

    #[test]
    fn test_render_header_single_space() {
        assert_eq!(RendererWikitext::render_header(" "), "\n\n\n");
    }

    #[test]
    fn test_render_header_with_equals() {
        assert_eq!(
            RendererWikitext::render_header("A = B"),
            "\n\n\n== A = B ==\n"
        );
    }

    #[test]
    fn test_render_header_with_brackets() {
        assert_eq!(
            RendererWikitext::render_header("Section {test}"),
            "\n\n\n== Section {test} ==\n"
        );
    }

    #[test]
    fn test_render_header_with_pipes() {
        assert_eq!(
            RendererWikitext::render_header("A | B"),
            "\n\n\n== A | B ==\n"
        );
    }
}
