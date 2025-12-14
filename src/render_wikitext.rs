use crate::{listeria_list::ListeriaList, listeria_page::ListeriaPage, renderer::Renderer};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug)]
pub struct RendererWikitext;

#[async_trait]
impl Renderer for RendererWikitext {
    fn new() -> Self {
        Self {}
    }

    async fn render(&mut self, list: &mut ListeriaList) -> Result<String> {
        let mut wt = String::new();
        for section_id in list.get_section_ids() {
            wt += &self.as_wikitext_section(list, section_id).await;
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
            wt += format!("\n----\n&sum; {} items.", list.results().len()).as_str();
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
    async fn as_wikitext_section(&self, list: &mut ListeriaList, section_id: usize) -> String {
        let mut wt = String::new();

        if let Some(name) = list.section_name(section_id) {
            wt += &Self::render_header(name);
        }

        wt += &Self::as_wikitext_table_header(list);

        if list.get_row_template().is_none()
            && !list.skip_table()
            && !list.results().is_empty()
            && !list.template_params().wdedit()
        {
            wt += "|-\n";
        }

        Self::process_rows(list, section_id, &mut wt).await;

        // End
        if !list.skip_table() {
            wt += "\n|}";
        }

        wt
    }

    fn as_wikitext_table_header(list: &ListeriaList) -> String {
        let mut wt = String::new();
        match &list.header_template() {
            Some(t) => {
                wt += "{{";
                wt += t;
                wt += "}}\n";
            }
            None => {
                if !list.skip_table() {
                    wt += "{| class='wikitable sortable";
                    if list.template_params().wdedit() {
                        wt += " wd_can_edit";
                    }
                    wt += "'\n";
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

    fn render_header(name: &str) -> String {
        if name.trim().is_empty() {
            "\n\n\n".to_string()
        } else {
            format!("\n\n\n== {name} ==\n")
        }
    }

    async fn process_rows(list: &mut ListeriaList, section_id: usize, wt: &mut String) {
        let mut row_entity_ids = vec![];
        for rownum in 0..list.results().len() {
            if let Some(row) = list.results().get(rownum)
                && row.section() == section_id
            {
                row_entity_ids.push(row.entity_id().to_string());
            }
        }

        // Rows
        let mut current_sub_row = 0;
        let mut rows = vec![];
        for rownum in 0..list.results().len() {
            if let Some(row) = list.results().get(rownum) {
                let mut row = row.clone();
                if row.section() == section_id {
                    let wt_sub_row = row.as_wikitext(list, current_sub_row).await;
                    rows.push(wt_sub_row);
                    current_sub_row += 1;
                }
            }
        }

        if list.skip_table() {
            *wt += &rows.join("\n");
        } else if list.template_params().wdedit() {
            let x: Vec<String> = row_entity_ids
                .iter()
                .zip(rows.iter())
                .map(|(entity_id, row)| match &list.header_template() {
                    Some(_) => row.to_string(),
                    None => format!("\n|- class='wd_{}'\n{}", &entity_id.to_lowercase(), &row),
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

    #[test]
    fn test_render_header() {
        assert_eq!(RendererWikitext::render_header("foo"), "\n\n\n== foo ==\n");
        assert_eq!(RendererWikitext::render_header(""), "\n\n\n");
        assert_eq!(RendererWikitext::render_header("  "), "\n\n\n");
    }
}
