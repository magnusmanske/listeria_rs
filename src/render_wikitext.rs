use crate::*;

pub struct RendererWikitext {
}

impl Renderer for RendererWikitext {
    fn new() -> Self {
        Self{}
    }

    fn render(&mut self,list:&ListeriaList) -> Result<String,String> {
        let section_ids = list.get_section_ids() ;
        // TODO section headers
        let mut wt : String = section_ids
            .iter()
            .map(|section_id|self.as_wikitext_section(list,*section_id))
            .collect() ;

        if !list.shadow_files().is_empty() {
            wt += "\n----\nThe following local image(s) are not shown in the above list, because they shadow a Commons image of the same name, and might be non-free:";
            for file in list.shadow_files() {
                wt += format!("\n# [[:{}:{}|]]",list.local_file_namespace_prefix(),file).as_str();
            }
        }

        match list.params.summary.as_ref().map(|s|s.as_str()) {
            Some("ITEMNUMBER") => {
                wt += format!("\n----\n&sum; {} items.",list.results.len()).as_str();
            }
            _ => {}
        }

        Ok(wt)
    }
}

impl RendererWikitext {
    fn as_wikitext_section(&self,list:&ListeriaList,section_id:usize) -> String {
        let mut wt = String::new() ;

        // TODO: section header

        wt += &self.as_wikitext_table_header(list) ;

        if list.get_row_template().is_none() && !list.skip_table() {
            if !list.results.is_empty() {
                wt += "|-\n";
            }
        }

        // Rows
        let rows = list
            .results
            .iter()
            .filter(|row|row.section==section_id)
            .enumerate()
            .map(|(rownum, row)| row.as_wikitext(list, rownum))
            .collect::<Vec<String>>() ;
        if list.skip_table() {
            wt += &rows.join("\n");
        } else {
            wt += &rows.join("\n|-\n");
        }

        // End
        if !list.skip_table() {
            wt += "\n|}" ;
        }

        wt
    }

    fn as_wikitext_table_header(&self,list:&ListeriaList) -> String {
        let mut wt = String::new() ;
        match &list.params.header_template {
            Some(t) => {
                wt += "{{" ;
                wt +=  &t ;
                wt += "}}\n" ;
            }
            None => {
                if !list.params.skip_table {
                    wt += "{| class='wikitable sortable' style='width:100%'\n" ;
                    list.columns.iter().enumerate().for_each(|(_colnum,col)| {
                        wt += "! " ;
                        wt += &col.label ;
                        wt += "\n" ;
                    });
                }
            }
        }
        wt
    }

}