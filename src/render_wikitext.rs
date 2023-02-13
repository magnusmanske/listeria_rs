use crate::{ListeriaList, ListeriaPage, Renderer};

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
        _wikitext: &str,
        page: &ListeriaPage,
    ) -> Result<Option<String>, String> {
        let new_wikitext = page
            .elements()
            .iter()
            .filter_map(|element| element.as_wikitext().ok())
            .collect();
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

        if list.get_row_template().is_none()
            && !list.skip_table()
            && !list.results().is_empty()
            && !list.template_params().wdedit
        {
            wt += "|-\n";
        }

        let row_entity_ids: Vec<String> = list
            .results()
            .iter()
            .filter(|row| row.section() == section_id)
            .map(|row| row.entity_id())
            .cloned()
            .collect();

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
        } else if list.template_params().wdedit {
            let x: Vec<String> = row_entity_ids
                .iter()
                .zip(rows.iter())
                .map(|(entity_id, row)| {
                    match &list.header_template() {
                        Some(_) => row.to_string(),
                        None => format!("\n|- class='wd_{}'\n{}", &entity_id.to_lowercase(), &row)
                    }
                })
                .collect();
            wt += &x.join("").trim();
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
                    wt += "{| class='wikitable sortable";
                    if list.template_params().wdedit {
                        wt += " wd_can_edit";
                    }
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
}
