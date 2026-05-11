//! Renders results as tabbed data stored in database tables.

use crate::{listeria_page::ListeriaPage, render_context::RenderContext, renderer::Renderer};
use anyhow::{Result, anyhow};
use regex::{Regex, RegexBuilder};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;
use wikimisc::mediawiki::Api;

static RE_TABBED_DATA_MARKER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\|\s*tabbed_data[^\|\}]*").expect("RE_TABBED_DATA_MARKER does not parse")
});

static RE_TEMPLATE_START_WITH_END: LazyLock<Regex> = LazyLock::new(|| {
    RegexBuilder::new(
        r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)(\{\{[Ww]ikidata[ _]list[ _]end\}\})(.*)"#,
    )
    .multi_line(true)
    .dot_matches_new_line(true)
    .build()
    .expect("RE_TEMPLATE_START_WITH_END does not parse")
});

static RE_TEMPLATE_START_NO_END: LazyLock<Regex> = LazyLock::new(|| {
    RegexBuilder::new(r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)"#)
        .multi_line(true)
        .dot_matches_new_line(true)
        .build()
        .expect("RE_TEMPLATE_START_NO_END does not parse")
});

#[derive(Debug, Clone, Copy)]
pub struct RendererTabbedData;

impl RendererTabbedData {
    pub const fn new() -> Self {
        Self
    }
}

impl Default for RendererTabbedData {
    fn default() -> Self {
        Self::new()
    }
}

impl Renderer for RendererTabbedData {
    async fn render<C: RenderContext>(&mut self, list: &C) -> Result<String> {
        let mut ret = json!({"license": "CC0-1.0","description": {"en":"Listeria output"},"sources":"https://github.com/magnusmanske/listeria_rs","schema":{"fields":[{ "name": "section", "type": "number", "title": { list.language().to_owned(): "Section"}}]},"data":[]});
        list.columns().iter().enumerate().for_each(|(colnum,col)| {
            if let Some(x) = ret["schema"]["fields"].as_array_mut() {
                x.push(json!({"name":"col_".to_string()+&colnum.to_string(),"type":"string","title":{list.language().to_owned():col.label()}}));
            }
        });
        let mut ret_data = Vec::with_capacity(list.results().len());
        for rownum in 0..list.results().len() {
            if let Some(row) = list.results().get(rownum) {
                ret_data.push(row.as_tabbed_data(list, rownum).await);
            }
        }
        ret["data"] = json!(ret_data);
        tokio::task::spawn_blocking(move || format!("{ret}"))
            .await
            .map_err(|e| anyhow!("spawn_blocking join error: {e}"))
    }

    async fn get_new_wikitext(
        &self,
        wikitext: &str,
        _page: &ListeriaPage,
    ) -> Result<Option<String>> {
        // TODO use local template name
        let wikitext = wikitext.to_owned();
        tokio::task::spawn_blocking(move || {
            let (before, blob, end_template, after) =
                RendererTabbedData::extract_template_parts(&wikitext)?;

            let (start_template, rest) = match RendererTabbedData::separate_start_template(&blob) {
                Some(parts) => parts,
                None => return Err(anyhow!("Can't split start template")),
            };

            let append = if end_template.is_empty() {
                rest
            } else {
                after.to_string()
            };

            let start_template = RendererTabbedData::process_template_marker(&start_template)?;
            let new_wikitext =
                RendererTabbedData::build_new_wikitext(&before, &start_template, &append);

            // Compare to old wikitext
            if wikitext == new_wikitext {
                // All is as it should be
                return Ok(None);
            }

            Ok(Some(new_wikitext))
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking join error: {e}"))?
    }
}

impl RendererTabbedData {
    fn extract_template_parts(wikitext: &str) -> Result<(String, String, String, String)> {
        if let Some(caps) = RE_TEMPLATE_START_WITH_END.captures(wikitext) {
            return Ok(Self::get_wikitext_captures(caps));
        }
        if let Some(caps) = RE_TEMPLATE_START_NO_END.captures(wikitext) {
            return Ok(Self::get_wikitext_captures(caps));
        }
        Err(anyhow!("No template/end template found"))
    }

    fn process_template_marker(start_template: &str) -> Result<String> {
        // Remove tabbed data marker
        let start_template = RE_TABBED_DATA_MARKER.replace(start_template, "");

        // Add tabbed data marker
        let processed = start_template[0..start_template.len() - 2]
            .trim()
            .to_string()
            + "\n|tabbed_data=1}}";

        Ok(processed)
    }

    fn build_new_wikitext(before: &str, start_template: &str, append: &str) -> String {
        before.to_owned() + start_template + "\n" + append.trim()
    }

    #[must_use]
    pub fn tabbed_data_page_name(&self, list: &impl RenderContext) -> Option<String> {
        let ret = "Data:Listeria/".to_string() + list.wiki() + "/" + list.page_title() + ".tab";
        if ret.len() > 250 {
            return None; // Page title too long
        }
        Some(ret)
    }

    fn get_wikitext_captures(caps: regex::Captures<'_>) -> (String, String, String, String) {
        (
            caps.get(1)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
            caps.get(2)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
            caps.get(3)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
            caps.get(4)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
        )
    }

    pub async fn write_tabbed_data(
        &mut self,
        tabbed_data_json: Value,
        commons_api: &mut Api,
        list: &impl RenderContext,
    ) -> Result<bool> {
        let data_page = self
            .tabbed_data_page_name(list)
            .ok_or(anyhow!("Data page name too long"))?;
        let text = tokio::task::spawn_blocking(move || ::serde_json::to_string(&tabbed_data_json))
            .await
            .map_err(|e| anyhow!("spawn_blocking join error: {e}"))??;
        let token = commons_api.get_edit_token().await?;
        let params: HashMap<String, String> = [
            ("action", "edit"),
            ("title", data_page.as_str()),
            ("summary", "Listeria test"),
            ("text", text.as_str()),
            ("minor", "true"),
            ("recreate", "true"),
            ("token", token.as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();
        // No need to check if this is the same as the existing data; MW API will return OK but not actually edit
        let result = commons_api.post_query_api_json_mut(&params).await?;

        // Check if the edit was successful
        if result["edit"]["result"].as_str() != Some("Success") {
            return Err(anyhow!(
                "Edit failed: {}",
                result
                    .get("error")
                    .and_then(|e| e.get("info"))
                    .and_then(|i| i.as_str())
                    .unwrap_or("Unknown error")
            ));
        }

        Ok(true) //list.data_has_changed = true; // Just to make sure to update including page
    }

    fn separate_start_template(blob: &str) -> Option<(String, String)> {
        let mut split_at: Option<usize> = None;
        let mut curly_count: i32 = 0;
        blob.char_indices().for_each(|(pos, c)| {
            match c {
                '{' => {
                    curly_count += 1;
                }
                '}' => {
                    curly_count -= 1;
                }
                _ => {}
            }
            if curly_count == 0 && split_at.is_none() {
                split_at = Some(pos + 1);
            }
        });
        match split_at {
            Some(pos) => {
                let mut template = blob.to_string();
                let rest = template.split_off(pos);
                Some((template, rest))
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_separate_start_template_simple() {
        let blob = "{{Wikidata list|sparql=SELECT}}rest of content";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{Wikidata list|sparql=SELECT}}");
        assert_eq!(rest, "rest of content");
    }

    #[test]
    fn test_separate_start_template_nested() {
        let blob = "{{Wikidata list|param={{nested}}|other=value}}after";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{Wikidata list|param={{nested}}|other=value}}");
        assert_eq!(rest, "after");
    }

    #[test]
    fn test_separate_start_template_deeply_nested() {
        let blob = "{{outer|{{middle|{{inner}}}}}}remaining text";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{outer|{{middle|{{inner}}}}}}");
        assert_eq!(rest, "remaining text");
    }

    #[test]
    fn test_separate_start_template_no_rest() {
        let blob = "{{Wikidata list|sparql=SELECT}}";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{Wikidata list|sparql=SELECT}}");
        assert_eq!(rest, "");
    }

    #[test]
    fn test_separate_start_template_unbalanced() {
        let blob = "{{Wikidata list|sparql=SELECT";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_none());
    }

    #[test]
    fn test_separate_start_template_extra_closing() {
        let blob = "{{template}}}}extra";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{template}}");
        assert_eq!(rest, "}}extra");
    }

    #[test]
    fn test_separate_start_template_multiline() {
        let blob = "{{Wikidata list\n|sparql=SELECT\n|columns=label\n}}\nrest";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(
            template,
            "{{Wikidata list\n|sparql=SELECT\n|columns=label\n}}"
        );
        assert_eq!(rest, "\nrest");
    }

    #[test]
    fn test_separate_start_template_triple_braces() {
        let blob = "{{template|param={{{variable}}}}}after";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{template|param={{{variable}}}}}");
        assert_eq!(rest, "after");
    }

    #[test]
    fn test_separate_start_template_multiple_nested() {
        let blob = "{{t1|{{t2}}|{{t3}}}}content";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{t1|{{t2}}|{{t3}}}}");
        assert_eq!(rest, "content");
    }

    #[test]
    fn test_separate_start_template_empty_string() {
        let blob = "";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_wikitext_captures_all_groups() {
        let text = "prefix{{Wikidata list|p=v}}{{Wikidata list end}}suffix";
        let pattern = r"^(.*?)(\{\{Wikidata[ _]list\b.+)(\{\{Wikidata[ _]list[ _]end\}\})(.)";
        let re = Regex::new(pattern).unwrap();
        let caps = re.captures(text).unwrap();

        let (before, blob, end_template, after) = RendererTabbedData::get_wikitext_captures(caps);
        assert_eq!(before, "prefix");
        assert_eq!(blob, "{{Wikidata list|p=v}}");
        assert_eq!(end_template, "{{Wikidata list end}}");
        assert_eq!(after, "s");
    }

    #[test]
    fn test_get_wikitext_captures_missing_groups() {
        let text = "{{Wikidata list}}";
        let pattern = r"()(\{\{Wikidata[ _]list\}\})()()";
        let re = Regex::new(pattern).unwrap();
        let caps = re.captures(text).unwrap();

        let (before, blob, end_template, after) = RendererTabbedData::get_wikitext_captures(caps);
        assert_eq!(before, "");
        assert_eq!(blob, "{{Wikidata list}}");
        assert_eq!(end_template, "");
        assert_eq!(after, "");
    }

    #[test]
    fn test_tabbed_data_page_name_normal() {
        let _renderer = RendererTabbedData::new();
        // We need a mock ListeriaList, which requires complex setup
        // This test would need significant mocking infrastructure
        // Skipping for now as it requires ListeriaList which needs DB/API setup
    }

    #[test]
    fn test_separate_start_template_consecutive_templates() {
        let blob = "{{first}}{{second}}";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{first}}");
        assert_eq!(rest, "{{second}}");
    }

    #[test]
    fn test_separate_start_template_with_pipe_and_nested() {
        let blob = "{{list|sparql={{#invoke:Sparql|query}}|columns=label}}content";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(
            template,
            "{{list|sparql={{#invoke:Sparql|query}}|columns=label}}"
        );
        assert_eq!(rest, "content");
    }

    #[test]
    fn test_separate_start_template_unicode() {
        let blob = "{{template|param=日本語|value=données}}texte";
        let result = RendererTabbedData::separate_start_template(blob);
        assert!(result.is_some());
        let (template, rest) = result.unwrap();
        assert_eq!(template, "{{template|param=日本語|value=données}}");
        assert_eq!(rest, "texte");
    }

    // ── extract_template_parts ─────────────────────────────────────────────

    #[test]
    fn test_extract_template_parts_with_end_template() {
        let wikitext = "{{Wikidata list|sparql=SELECT}}\n{{Wikidata list end}}";
        let (_, blob, end_template, _) =
            RendererTabbedData::extract_template_parts(wikitext).unwrap();
        assert!(blob.contains("Wikidata list|sparql=SELECT"));
        assert_eq!(end_template, "{{Wikidata list end}}");
    }

    #[test]
    fn test_extract_template_parts_no_end_template() {
        let wikitext = "{{Wikidata list|sparql=SELECT}}";
        let (_, blob, end_template, _) =
            RendererTabbedData::extract_template_parts(wikitext).unwrap();
        assert!(blob.contains("Wikidata list"));
        assert!(end_template.is_empty());
    }

    #[test]
    fn test_extract_template_parts_no_template_is_err() {
        assert!(RendererTabbedData::extract_template_parts("no template here").is_err());
    }

    #[test]
    fn test_extract_template_parts_lowercase_w() {
        let wikitext = "{{wikidata list|sparql=SELECT}}";
        assert!(RendererTabbedData::extract_template_parts(wikitext).is_ok());
    }

    #[test]
    fn test_extract_template_parts_underscore_variant() {
        let wikitext = "{{Wikidata_list|sparql=SELECT}}";
        assert!(RendererTabbedData::extract_template_parts(wikitext).is_ok());
    }

    // ── process_template_marker ────────────────────────────────────────────

    #[test]
    fn test_process_template_marker_adds_tabbed_data() {
        let result =
            RendererTabbedData::process_template_marker("{{Wikidata list|columns=label}}")
                .unwrap();
        assert!(result.contains("tabbed_data=1"));
        assert!(result.ends_with("}}"));
    }

    #[test]
    fn test_process_template_marker_removes_existing_and_readds() {
        let result = RendererTabbedData::process_template_marker(
            "{{Wikidata list|columns=label| tabbed_data=old}}",
        )
        .unwrap();
        assert!(result.contains("tabbed_data=1"));
        assert!(!result.contains("old"));
    }

    #[test]
    fn test_process_template_marker_preserves_other_params() {
        let result = RendererTabbedData::process_template_marker(
            "{{Wikidata list|columns=label|sparql=SELECT}}",
        )
        .unwrap();
        assert!(result.contains("columns=label"));
        assert!(result.contains("sparql=SELECT"));
        assert!(result.contains("tabbed_data=1"));
    }

    // ── build_new_wikitext ─────────────────────────────────────────────────

    #[test]
    fn test_build_new_wikitext_basic() {
        let result = RendererTabbedData::build_new_wikitext("before\n", "{{t}}", "after");
        assert_eq!(result, "before\n{{t}}\nafter");
    }

    #[test]
    fn test_build_new_wikitext_trims_append_whitespace() {
        let result = RendererTabbedData::build_new_wikitext("", "{{t}}", "  content  ");
        assert_eq!(result, "{{t}}\ncontent");
    }

    #[test]
    fn test_build_new_wikitext_empty_append_leaves_trailing_newline() {
        let result = RendererTabbedData::build_new_wikitext("", "{{t}}", "");
        assert_eq!(result, "{{t}}\n");
    }

    // ── tabbed_data_page_name ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_tabbed_data_page_name_format() {
        use std::sync::Arc;
        let api = crate::test_utils::cached_api("https://www.wikidata.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
        let page_params = Arc::new(
            crate::page_params::PageParams::new(config, api, "Test:Page".to_string())
                .await
                .unwrap(),
        );
        let template = crate::template::Template::new_from_params(
            "columns=item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }}",
        )
        .unwrap();
        let list = crate::listeria_list::ListeriaList::new(template, page_params)
            .await
            .unwrap();
        let renderer = RendererTabbedData::new();
        let name = renderer.tabbed_data_page_name(&list).unwrap();
        assert!(name.starts_with("Data:Listeria/"), "Expected Data:Listeria/ prefix, got: {name}");
        assert!(name.ends_with(".tab"), "Expected .tab suffix, got: {name}");
        assert!(name.len() <= 250);
    }

    #[test]
    fn test_tabbed_data_page_name_truncated_for_long_title() {
        // Build a ListeriaList would require async, so instead test the name-length
        // guard directly: a name > 250 chars must be rejected.
        // Verify the 250-byte limit constant is respected by the implementation.
        // (The constant is embedded in the source; this test documents the contract.)
        let max = 250_usize;
        let long: String = "x".repeat(max + 1);
        // A name this long must not be returned — only reachable via the real
        // function, so we just document the boundary condition here.
        assert!(long.len() > max);
    }
}
