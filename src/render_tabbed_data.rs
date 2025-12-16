//! Renders results as tabbed data stored in database tables.

use crate::{listeria_list::ListeriaList, listeria_page::ListeriaPage, renderer::Renderer};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use regex::{Regex, RegexBuilder};
use serde_json::Value;
use std::collections::HashMap;
use wikimisc::mediawiki::Api;

#[derive(Debug)]
pub struct RendererTabbedData;

#[async_trait]
impl Renderer for RendererTabbedData {
    fn new() -> Self {
        Self {}
    }

    async fn render(&mut self, list: &mut ListeriaList) -> Result<String> {
        let mut ret = json!({"license": "CC0-1.0","description": {"en":"Listeria output"},"sources":"https://github.com/magnusmanske/listeria_rs","schema":{"fields":[{ "name": "section", "type": "number", "title": { list.language().to_owned(): "Section"}}]},"data":[]});
        list.columns().iter().enumerate().for_each(|(colnum,col)| {
            if let Some(x) = ret["schema"]["fields"].as_array_mut() {
                x.push(json!({"name":"col_".to_string()+&colnum.to_string(),"type":"string","title":{list.language().to_owned():col.label()}}));
            }
        });
        let mut ret_data = vec![];
        for rownum in 0..list.results().len() {
            if let Some(row) = list.results().get(rownum) {
                ret_data.push(row.as_tabbed_data(list, rownum).await);
            }
        }
        ret["data"] = json!(ret_data);
        Ok(format!("{ret}"))
    }

    async fn get_new_wikitext(
        &self,
        wikitext: &str,
        _page: &ListeriaPage,
    ) -> Result<Option<String>> {
        // TODO use local template name

        // Start/end template
        let pattern1 =
            r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)(\{\{[Ww]ikidata[ _]list[ _]end\}\})(.*)"#;

        // No end template
        let pattern2 = r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)"#;

        let re_wikitext1: Regex = RegexBuilder::new(pattern1)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()?;
        let re_wikitext2: Regex = RegexBuilder::new(pattern2)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()?;

        let (before, blob, end_template, after) = match re_wikitext1.captures(wikitext) {
            Some(caps) => Self::get_wikitext_captures(caps),
            None => match re_wikitext2.captures(wikitext) {
                Some(caps) => Self::get_wikitext_captures(caps),
                None => return Err(anyhow!("No template/end template found")),
            },
        };

        let (start_template, rest) = match Self::separate_start_template(&blob) {
            Some(parts) => parts,
            None => return Err(anyhow!("Can't split start template")),
        };

        let append = if end_template.is_empty() {
            rest
        } else {
            after.to_string()
        };

        // Remove tabbed data marker
        let start_template = Regex::new(r"\|\s*tabbed_data[^\|\}]*")?.replace(&start_template, "");

        // Add tabbed data marker
        let start_template = start_template[0..start_template.len() - 2]
            .trim()
            .to_string()
            + "\n|tabbed_data=1}}";

        // Create new wikitext
        let new_wikitext = before.to_owned() + &start_template + "\n" + append.trim();

        // Compare to old wikitext
        if wikitext == new_wikitext {
            // All is as it should be
            return Ok(None);
        }

        Ok(Some(new_wikitext))
    }
}

impl RendererTabbedData {
    #[must_use]
    pub fn tabbed_data_page_name(&self, list: &ListeriaList) -> Option<String> {
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
        list: &ListeriaList,
    ) -> Result<bool> {
        let data_page = self
            .tabbed_data_page_name(list)
            .ok_or(anyhow!("Data page name too long"))?;
        let text = ::serde_json::to_string(&tabbed_data_json)?;
        let token = commons_api.get_edit_token().await?;
        let params: HashMap<String, String> = vec![
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
        let _result = commons_api.post_query_api_json_mut(&params).await?;
        // TODO check ["edit"]["result"] == "Success"
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
}
