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