use crate::*;
use regex::RegexBuilder;

pub struct RendererWikitext {}

impl Renderer for RendererWikitext {
    fn new() -> Self {
        Self {}
    }

    fn render(&mut self, list: &ListeriaList) -> Result<String, String> {
        let mut wt: String = list
            .get_section_ids()
            .iter()
            .map(|section_id| self.as_wikitext_section(list, *section_id))
            .collect();

        if !list.shadow_files().is_empty() {
            wt += "\n----\nThe following local image(s) are not shown in the above list, because they shadow a Commons image of the same name, and might be non-free:";
            for file in list.shadow_files() {
                wt += format!("\n# [[:{}:{}|]]", list.local_file_namespace_prefix(), file).as_str();
            }
        }

        if let Some("ITEMNUMBER") = list.summary().as_deref() {
            wt += format!("\n----\n&sum; {} items.", list.results().len()).as_str();
        }

        Ok(wt)
    }

    fn get_new_wikitext(
        &self,
        wikitext: &str,
        page: &ListeriaPage,
    ) -> Result<Option<String>, String> {
        let start_template = page
            .page_params
            .config
            .get_local_template_title_start(&page.page_params.wiki)?;
        let end_template = page
            .page_params
            .config
            .get_local_template_title_end(&page.page_params.wiki)?;
        let pattern_string_start = r#"\{\{([Ww]ikidata[ _]list|"#.to_string()
            + &start_template.replace(" ", "[ _]")
            + r#")\s*(\|.*?\}\}|\}\})"#;
        let pattern_string_end = r#"^(.*?)\{\{([Ww]ikidata[ _]list[ _]end|"#.to_string()
            + &end_template.replace(" ", "[ _]")
            + r#")(\s*\}\}.*)$"#;
        let seperator_start: Regex = RegexBuilder::new(&pattern_string_start)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();
        let seperator_end: Regex = RegexBuilder::new(&pattern_string_end)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();

        // TODO cover non-closed templates for data

        let result_start = Self::split_keep(&seperator_start, wikitext);
        //println!("START:\n{:#?}",&result_start);
        let mut new_wikitext = String::new();
        let mut last_was_template_open = false;
        let mut template_counter = 0;
        for part in result_start {
            if seperator_start.is_match_at(part, 0) {
                last_was_template_open = true;
                new_wikitext += part; // TODO modify?
                new_wikitext += "\n";
                continue;
            }
            if !last_was_template_open {
                new_wikitext += part;
                continue;
            }
            last_was_template_open = false;
            let result_end = seperator_end.captures(&part);
            //println!("END:\n{:#?}",&result_end);
            match result_end {
                Some(caps) => {
                    let (_before, template_name, template_end_after) = (
                        caps.get(1).unwrap().as_str(),
                        caps.get(2).unwrap().as_str(),
                        caps.get(3).unwrap().as_str(),
                    );
                    if page.lists().len() <= template_counter {
                        return Err("More lists than templates".to_string());
                    }
                    let mut renderer = RendererWikitext::new();
                    new_wikitext += &renderer.render(&page.lists()[template_counter])?;
                    new_wikitext += "\n{{";
                    new_wikitext += template_name;
                    new_wikitext += template_end_after;
                    template_counter += 1;
                }
                None => {
                    new_wikitext += part;
                }
            }
        }

        if template_counter != page.lists().len() {
            return Err(format!(
                "Replaced {} lists but there are {}",
                &template_counter,
                page.lists().len()
            ));
        }

        Ok(Some(new_wikitext))
    }
}

impl RendererWikitext {
    fn as_wikitext_section(&self, list: &ListeriaList, section_id: usize) -> String {
        let mut wt = String::new();

        if let Some(name) = list.section_name(section_id) {
            let header = format!("\n\n\n== {} ==\n", name);
            wt += &header;
        }

        wt += &self.as_wikitext_table_header(list);

        if list.get_row_template().is_none() && !list.skip_table() && !list.results().is_empty() {
            wt += "|-\n";
        }

        // Rows
        let rows = list
            .results()
            .iter()
            .filter(|row| row.section() == section_id)
            .enumerate()
            .map(|(rownum, row)| row.as_wikitext(list, rownum))
            .collect::<Vec<String>>();
        if list.skip_table() {
            wt += &rows.join("\n");
        } else {
            wt += &rows.join("\n|-\n");
        }

        // End
        if !list.skip_table() {
            wt += "\n|}";
        }

        wt
    }

    fn as_wikitext_table_header(&self, list: &ListeriaList) -> String {
        let mut wt = String::new();
        match &list.header_template() {
            Some(t) => {
                wt += "{{";
                wt += &t;
                wt += "}}\n";
            }
            None => {
                if !list.skip_table() {
                    wt += "{| class='wikitable sortable' style='width:100%'\n";
                    list.columns()
                        .iter()
                        .enumerate()
                        .for_each(|(_colnum, col)| {
                            wt += "! ";
                            wt += &col.label;
                            wt += "\n";
                        });
                }
            }
        }
        wt
    }

    fn split_keep<'a>(r: &Regex, text: &'a str) -> Vec<&'a str> {
        let mut result = Vec::new();
        let mut last = 0;
        for (index, matched) in text.match_indices(r) {
            if last != index {
                result.push(&text[last..index]);
            }
            result.push(matched);
            last = index + matched.len();
        }
        if last < text.len() {
            result.push(&text[last..]);
        }
        result
    }
}
