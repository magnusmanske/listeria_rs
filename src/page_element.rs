//! Wiki page parsing to extract Listeria template blocks.

use crate::listeria_list::ListeriaList;
use crate::listeria_page::ListeriaPage;
use crate::render_wikitext::RendererWikitext;
use crate::renderer::Renderer;
use crate::template::Template;
use anyhow::Result;
use regex::Regex;
use regex::RegexBuilder;

#[derive(Debug, Clone)]
pub struct PageElement {
    before: String,
    template_start: String,
    _inside: String,
    template_end: String,
    after: String,
    list: ListeriaList,
    is_just_text: bool,
}

impl PageElement {
    fn extract_text_segment(text: &str, start: usize, end: usize) -> Option<String> {
        String::from_utf8(text.as_bytes()[start..end].to_vec()).ok()
    }

    fn extract_template_text(
        text: &str,
        match_start: &regex::Match<'_>,
        template_start_end_bytes: usize,
    ) -> Option<String> {
        Self::extract_text_segment(text, match_start.end(), template_start_end_bytes - 2)
    }

    fn create_template_from_text(template_text: &str) -> Option<Template> {
        Template::new_from_params(template_text).ok()
    }

    fn extract_inside_text(
        text: &str,
        single_template: bool,
        template_start_end_bytes: usize,
        match_end: &regex::Match<'_>,
    ) -> Option<String> {
        if single_template {
            Some(String::new())
        } else {
            Self::extract_text_segment(text, template_start_end_bytes, match_end.start())
        }
    }

    fn extract_template_end_text(
        text: &str,
        single_template: bool,
        match_end: &regex::Match<'_>,
    ) -> Option<String> {
        if single_template {
            Some(String::new())
        } else {
            Self::extract_text_segment(text, match_end.start(), match_end.end())
        }
    }

    async fn build_page_element(
        text: &str,
        match_start: regex::Match<'_>,
        match_end: regex::Match<'_>,
        template_start_end_bytes: usize,
        single_template: bool,
        template: Template,
        page: &ListeriaPage,
    ) -> Option<Self> {
        Some(Self {
            before: Self::extract_text_segment(text, 0, match_start.start())?,
            template_start: Self::extract_text_segment(
                text,
                match_start.start(),
                template_start_end_bytes,
            )?,
            _inside: Self::extract_inside_text(
                text,
                single_template,
                template_start_end_bytes,
                &match_end,
            )?,
            template_end: Self::extract_template_end_text(text, single_template, &match_end)?,
            after: if single_template {
                Self::extract_text_segment(text, template_start_end_bytes, text.len())?
            } else {
                Self::extract_text_segment(text, match_end.end(), text.len())?
            },
            list: ListeriaList::new(template, page.page_params()).await.ok()?,
            is_just_text: false,
        })
    }

    pub async fn new_from_text(text: &str, page: &ListeriaPage) -> Option<Self> {
        let (seperator_start, seperator_end) = Self::get_start_stop_separators(page)?;

        let (match_start, match_end, single_template) =
            match Self::matches_from_separators(seperator_start, text, seperator_end) {
                Ok(value) => value,
                Err(value) => return value,
            };

        let remaining =
            Self::new_from_text_remaining(single_template, text, match_start, match_end)?;
        let template_start_end_bytes = Self::get_template_end(remaining)? + match_start.end();

        let template_text =
            Self::extract_template_text(text, &match_start, template_start_end_bytes)?;
        let template = Self::create_template_from_text(&template_text)?;

        Self::build_page_element(
            text,
            match_start,
            match_end,
            template_start_end_bytes,
            single_template,
            template,
            page,
        )
        .await
    }

    pub async fn new_just_text(text: &str, page: &ListeriaPage) -> Result<Self> {
        let template = Template::default();
        Ok(Self {
            before: text.to_string(),
            template_start: String::new(),
            _inside: String::new(),
            template_end: String::new(),
            after: String::new(),
            list: ListeriaList::new(template, page.page_params()).await?,
            is_just_text: true,
        })
    }

    pub fn get_and_clean_after(&mut self) -> String {
        std::mem::take(&mut self.after)
    }

    pub async fn new_inside(&mut self) -> Result<String> {
        if self.is_just_text {
            return Ok(String::new());
        }
        let mut renderer = RendererWikitext::new();
        renderer.render(&mut self.list).await
    }

    pub async fn as_wikitext(&mut self) -> Result<String> {
        if self.is_just_text {
            return Ok(self.before.clone());
        }
        let new_inside = self.new_inside().await?;
        Ok(format!(
            "{}{}\n{}\n{}{}",
            &self.before, &self.template_start, &new_inside, &self.template_end, &self.after
        ))
    }

    pub async fn process(&mut self) -> Result<()> {
        if self.is_just_text {
            return Ok(());
        }
        self.list.process().await
    }

    #[must_use]
    pub const fn is_just_text(&self) -> bool {
        self.is_just_text
    }

    fn get_template_end(text: String) -> Option<usize> {
        let mut pos: usize = 0;
        let mut curly_braces_open: usize = 2;
        let tv = text.as_bytes();
        while pos < tv.len() && curly_braces_open > 0 {
            match tv[pos] as char {
                '{' => curly_braces_open += 1,
                '}' => curly_braces_open -= 1,
                _ => {}
            }
            pos += 1;
        }
        if curly_braces_open == 0 {
            Some(pos)
        } else {
            None
        }
    }

    fn get_start_stop_separators(page: &ListeriaPage) -> Option<(Regex, Regex)> {
        let start_template = page
            .config()
            .get_local_template_title_start(page.wiki())
            .ok()?;
        let end_template = page
            .config()
            .get_local_template_title_end(page.wiki())
            .ok()?;
        let pattern_string_start = page.config().pattern_string_start().to_string()
        + &start_template.replace(' ', "[ _]")
        //+ r#")\s*\|"#; // New version
        + r#"[^\|]*)"#;
        let pattern_string_end = page.config().pattern_string_end().to_string()
            + &end_template.replace(' ', "[ _]")
            + r#")(\s*\}\})"#;
        let seperator_start: Regex = RegexBuilder::new(&pattern_string_start)
            .multi_line(true)
            .dot_matches_new_line(true)
            .case_insensitive(true)
            .build()
            .ok()?;
        let seperator_end: Regex = RegexBuilder::new(&pattern_string_end)
            .multi_line(true)
            .dot_matches_new_line(true)
            .case_insensitive(true)
            .build()
            .ok()?;
        Some((seperator_start, seperator_end))
    }

    #[allow(clippy::result_large_err)]
    fn matches_from_separators(
        seperator_start: Regex,
        text: &str,
        seperator_end: Regex,
    ) -> Result<(regex::Match<'_>, regex::Match<'_>, bool), Option<PageElement>> {
        let match_start = seperator_start.find(text).ok_or(None)?;
        let (match_end, single_template) = seperator_end
            .find_at(text, match_start.start())
            .map_or((match_start, true), |m| (m, false));
        Ok((match_start, match_end, single_template))
    }

    fn new_from_text_remaining(
        single_template: bool,
        text: &str,
        match_start: regex::Match<'_>,
        match_end: regex::Match<'_>,
    ) -> Option<String> {
        let remaining = if single_template {
            String::from_utf8(text.as_bytes()[match_start.end()..].to_vec()).ok()?
        } else {
            if match_end.start() < match_start.end() {
                return None;
            }
            String::from_utf8(text.as_bytes()[match_start.end()..match_end.start()].to_vec())
                .ok()?
        };
        Some(remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_template_end_simple() {
        let text = "param1|param2}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(15));
    }

    #[test]
    fn test_get_template_end_nested_single() {
        let text = "param1|{{nested}}|param2}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(26));
    }

    #[test]
    fn test_get_template_end_nested_multiple() {
        let text = "param1|{{nested1|{{nested2}}}}|param2}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(39));
    }

    #[test]
    fn test_get_template_end_deep_nesting() {
        let text = "a|{{b|{{c|{{d}}}}}}|e}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(23));
    }

    #[test]
    fn test_get_template_end_unbalanced_missing_close() {
        let text = "param1|{{nested|param2".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_template_end_unbalanced_extra_open() {
        let text = "param1|{{{extra}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_template_end_empty() {
        let text = "}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_get_template_end_complex_with_pipes() {
        let text =
            "sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }|columns=label,P31}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(69));
    }

    #[test]
    fn test_get_template_end_with_triple_braces() {
        let text = "param|{{{variable}}}}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(22));
    }

    #[test]
    fn test_get_template_end_consecutive_opens_and_closes() {
        let text = "a|{{{{b}}}}|c}}".to_string();
        let result = PageElement::get_template_end(text);
        assert_eq!(result, Some(15));
    }

    #[test]
    fn test_new_from_text_remaining_single_template() {
        let text = "Some text {{template|param1|param2}} more text";
        let match_start = regex::Regex::new(r"\{\{template")
            .unwrap()
            .find(text)
            .unwrap();
        let match_end = match_start; // Single template uses same match

        let result = PageElement::new_from_text_remaining(true, text, match_start, match_end);
        assert_eq!(result, Some("|param1|param2}} more text".to_string()));
    }

    #[test]
    fn test_new_from_text_remaining_paired_template() {
        let text = "{{start|params}}content here{{end}}";
        let match_start = regex::Regex::new(r"\{\{start").unwrap().find(text).unwrap();
        let match_end = regex::Regex::new(r"\{\{end").unwrap().find(text).unwrap();

        let result = PageElement::new_from_text_remaining(false, text, match_start, match_end);
        assert_eq!(result, Some("|params}}content here".to_string()));
    }

    #[test]
    fn test_new_from_text_remaining_invalid_order() {
        let text = "{{end}}content{{start}}";
        let match_start = regex::Regex::new(r"\{\{start").unwrap().find(text).unwrap();
        let match_end = regex::Regex::new(r"\{\{end").unwrap().find(text).unwrap();

        let result = PageElement::new_from_text_remaining(false, text, match_start, match_end);
        assert_eq!(result, None); // End comes before start
    }

    #[test]
    fn test_new_from_text_remaining_empty_between() {
        let text = "{{start}}{{end}}";
        let match_start = regex::Regex::new(r"\{\{start").unwrap().find(text).unwrap();
        let match_end = regex::Regex::new(r"\{\{end").unwrap().find(text).unwrap();

        let result = PageElement::new_from_text_remaining(false, text, match_start, match_end);
        assert_eq!(result, Some("}}".to_string()));
    }

    #[test]
    fn test_new_from_text_remaining_unicode() {
        let text = "{{template|param=日本語}}{{end}}";
        let match_start = regex::Regex::new(r"\{\{template")
            .unwrap()
            .find(text)
            .unwrap();
        let match_end = regex::Regex::new(r"\{\{end").unwrap().find(text).unwrap();

        let result = PageElement::new_from_text_remaining(false, text, match_start, match_end);
        assert!(result.is_some());
        assert!(result.unwrap().contains("日本語"));
    }

    #[test]
    fn test_new_from_text_remaining_multiline() {
        let text = "{{start|p1=value1\n|p2=value2}}\ncontent\n{{end}}";
        let match_start = regex::Regex::new(r"\{\{start").unwrap().find(text).unwrap();
        let match_end = regex::Regex::new(r"\{\{end").unwrap().find(text).unwrap();

        let result = PageElement::new_from_text_remaining(false, text, match_start, match_end);
        assert!(result.is_some());
        assert!(result.unwrap().contains("content"));
    }

    #[test]
    fn test_matches_from_separators_both_found() {
        let text = "prefix {{Wikidata list|sparql=SELECT}} content {{Wikidata list end}} suffix";
        let sep_start = RegexBuilder::new(r"\{\{Wikidata[ _]list")
            .case_insensitive(true)
            .build()
            .unwrap();
        let sep_end = RegexBuilder::new(r"\{\{Wikidata[ _]list[ _]end")
            .case_insensitive(true)
            .build()
            .unwrap();

        let result = PageElement::matches_from_separators(sep_start, text, sep_end);
        assert!(result.is_ok());
        let (match_start, match_end, single) = result.unwrap();
        assert_eq!(match_start.start(), 7);
        assert_eq!(match_end.start(), 47);
        assert!(!single);
    }

    #[test]
    fn test_matches_from_separators_no_start() {
        let text = "just some text without templates";
        let sep_start = RegexBuilder::new(r"\{\{Wikidata[ _]list")
            .case_insensitive(true)
            .build()
            .unwrap();
        let sep_end = RegexBuilder::new(r"\{\{Wikidata[ _]list[ _]end")
            .case_insensitive(true)
            .build()
            .unwrap();

        let result = PageElement::matches_from_separators(sep_start, text, sep_end);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_none());
    }

    #[test]
    fn test_matches_from_separators_single_template() {
        let text = "prefix {{Wikidata list|sparql=SELECT}} suffix";
        let sep_start = RegexBuilder::new(r"\{\{Wikidata[ _]list")
            .case_insensitive(true)
            .build()
            .unwrap();
        let sep_end = RegexBuilder::new(r"\{\{Wikidata[ _]list[ _]end")
            .case_insensitive(true)
            .build()
            .unwrap();

        let result = PageElement::matches_from_separators(sep_start, text, sep_end);
        assert!(result.is_ok());
        let (match_start, match_end, single) = result.unwrap();
        assert_eq!(match_start.start(), match_end.start());
        assert!(single);
    }

    #[test]
    fn test_matches_from_separators_case_insensitive() {
        let text = "{{WIKIDATA_LIST|params}}content{{wikidata_list_end}}";
        let sep_start = RegexBuilder::new(r"\{\{Wikidata[ _]list")
            .case_insensitive(true)
            .build()
            .unwrap();
        let sep_end = RegexBuilder::new(r"\{\{Wikidata[ _]list[ _]end")
            .case_insensitive(true)
            .build()
            .unwrap();

        let result = PageElement::matches_from_separators(sep_start, text, sep_end);
        assert!(result.is_ok());
        let (_, _, single) = result.unwrap();
        assert!(!single);
    }

    #[test]
    fn test_matches_from_separators_space_vs_underscore() {
        let text = "{{Wikidata_list|params}}content{{Wikidata list end}}";
        let sep_start = RegexBuilder::new(r"\{\{Wikidata[ _]list")
            .case_insensitive(true)
            .build()
            .unwrap();
        let sep_end = RegexBuilder::new(r"\{\{Wikidata[ _]list[ _]end")
            .case_insensitive(true)
            .build()
            .unwrap();

        let result = PageElement::matches_from_separators(sep_start, text, sep_end);
        assert!(result.is_ok());
    }
}
