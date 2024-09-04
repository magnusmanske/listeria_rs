use crate::renderer::Renderer;
use crate::*;
use regex::{Regex, RegexBuilder};
use serde_json::Value;

pub struct RendererTabbedData {}

impl Renderer for RendererTabbedData {
    fn new() -> Self {
        Self {}
    }

    fn render(&mut self, list: &mut ListeriaList) -> Result<String> {
        let mut ret = json!({"license": "CC0-1.0","description": {"en":"Listeria output"},"sources":"https://github.com/magnusmanske/listeria_rs","schema":{"fields":[{ "name": "section", "type": "number", "title": { list.language().to_owned(): "Section"}}]},"data":[]});
        list.columns().iter().enumerate().for_each(|(colnum,col)| {
            if let Some(x) = ret["schema"]["fields"].as_array_mut() {
                x.push(json!({"name":"col_".to_string()+&colnum.to_string(),"type":"string","title":{list.language().to_owned():col.label()}}));
            }
        });
        let mut ret_data = vec![];
        for rownum in 0..list.results().len() {
            if let Some(row) = list.results().get(rownum) {
                ret_data.push(row.as_tabbed_data(list, rownum));
            }
        }
        ret["data"] = json!(ret_data);
        Ok(format!("{}", ret))
    }

    fn get_new_wikitext(&self, wikitext: &str, _page: &ListeriaPage) -> Result<Option<String>> {
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

        let (start_template, rest) = match self.separate_start_template(&blob) {
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

    fn separate_start_template(&self, blob: &str) -> Option<(String, String)> {
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
