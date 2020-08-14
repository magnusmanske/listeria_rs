use crate::*;

pub struct RendererTabbedData {
}

impl Renderer for RendererTabbedData {
    fn new() -> Self {
        Self{}
    }

    fn render(&mut self,list:&ListeriaList) -> Result<String,String> {
        let mut ret = json!({"license": "CC0-1.0","description": {"en":"Listeria output"},"sources":"https://github.com/magnusmanske/listeria_rs","schema":{"fields":[{ "name": "section", "type": "number", "title": { list.language().to_owned(): "Section"}}]},"data":[]});
        list.columns().iter().enumerate().for_each(|(colnum,col)| {
            ret["schema"]["fields"]
                .as_array_mut()
                .unwrap() // OK, this must exist
                .push(json!({"name":"col_".to_string()+&colnum.to_string(),"type":"string","title":{list.language().to_owned():col.label}}))
        });
        ret["data"] = list
            .results()
            .iter()
            .enumerate()
            .map(|(rownum, row)| row.as_tabbed_data(&list, rownum))
            .collect();
        Ok(format!("{}",ret))
        }
}

impl RendererTabbedData {
    pub fn tabbed_data_page_name(&self,list:&ListeriaList) -> Option<String> {
        let ret = "Data:Listeria/".to_string() + &list.wiki() + "/" + &list.page_title() + ".tab";
        if ret.len() > 250 {
            return None; // Page title too long
        }
        Some(ret)
    }

    pub async fn write_tabbed_data(
        &mut self,
        tabbed_data_json: Value,
        commons_api: &mut Api,
        list:&ListeriaList
    ) -> Result<bool, String> {
        let data_page = self
            .tabbed_data_page_name(list)
            .ok_or("Data page name too long")?;
        let text = ::serde_json::to_string(&tabbed_data_json).unwrap();
        let params: HashMap<String, String> = vec![
            ("action", "edit"),
            ("title", data_page.as_str()),
            ("summary", "Listeria test"),
            ("text", text.as_str()),
            ("minor", "true"),
            ("recreate", "true"),
            ("token", commons_api.get_edit_token().await.unwrap().as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();
        // No need to check if this is the same as the existing data; MW API will return OK but not actually edit
        let _result = match commons_api.post_query_api_json_mut(&params).await {
            Ok(r) => r,
            Err(e) => return Err(format!("{:?}", e)),
        };
        // TODO check ["edit"]["result"] == "Success"
        Ok(true) //list.data_has_changed = true; // Just to make sure to update including page
    }
}