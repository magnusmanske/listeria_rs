use crate::*;
use std::sync::Arc;
use regex::{Regex, RegexBuilder};
use roxmltree;
use serde_json::Value;
use std::collections::HashMap;
use wikibase::mediawiki::api::Api;

/* TODO
- Sort by P/P, P/Q/P DOES NOT WORK IN LISTERIA-PHP
- Show only preffered values (eg P41 in Q43175)
- Main namespace block
- coords commonswiki CHECK
- coords dewiki IMPLEMENT region
- actually edit the page

TEMPLATE PARAMETERS
autolist IMPLEMENT
links IMPLEMENT fully
wdedit IMPLEMENT
references IMPLEMENT
freq IGNORED

min_section DONE
section DONE
sparql DONE
columns DONE
sort DONE
language done?
thumb DONE via thumbnail_size()
row_template DONE
header_template DONE
skip_table DONE
summary DONE
*/

#[derive(Debug, Clone)]
pub struct ListeriaPage {
    pub page_params: PageParams,
    template_title_start: String,
    template: Option<Template>,
    results: Vec<ResultRow>,
    data_has_changed: bool,
    lists:Vec<ListeriaList>,
}

impl ListeriaPage {
    pub async fn new(mw_api: Arc<Api>, page: String, wb_api:Arc<Api>) -> Option<Self> {
        let page_params = PageParams {
            wiki: mw_api
                .get_site_info_string("general", "wikiid")
                .expect("No wikiid in site info")
                .to_string(),
            page: page,
            language: mw_api
                .get_site_info_string("general", "lang")
                .ok()?
                .to_string(),
            mw_api: mw_api.clone(),
            wb_api: wb_api.clone(),
            simulate: false,
            simulated_text: None,
            simulated_sparql_results: None,
            } ;
        Some(Self {
            page_params: page_params,
            template_title_start: "Wikidata list".to_string(),
            template: None,
            results: vec![],
            data_has_changed: false,
            lists:vec![],
        })
    }

    pub fn do_simulate(&mut self,text: Option<String>, sparql_results:Option<String>) {
        self.page_params.simulate = true ;
        self.page_params.simulated_text = text ;
        self.page_params.simulated_sparql_results = sparql_results ;
    }

    pub fn language(&self) -> &String {
        &self.page_params.language
    }

    pub async fn run(&mut self) -> Result<(), String> {
        self.lists.clear();
        let templates = self.load_page().await?;
        for template in templates {
            let mut list = ListeriaList::new(template.clone(),self.page_params.clone()) ;
            self.template = Some(template.clone());
            list.process_template()?;
            list.run_query().await?;
            list.load_entities().await?;
            list.generate_results()?;
            list.patch_results().await?;
            self.lists.push(list);
        }
        Ok(())
    }


    async fn load_page(&mut self) -> Result<Vec<Template>, String> {
        let text = self.load_page_as("parsetree").await?.to_owned();
        let doc = roxmltree::Document::parse(&text).unwrap();
        let ret = doc.root()
            .descendants()
            .filter(|n| n.is_element() && n.tag_name().name() == "template")
            .filter_map(|node| {
                match Template::new_from_xml(&node) {
                    Some(t) => {
                        if t.title == self.template_title_start {
                            Some(t)
                        } else {
                            None
                        }
                    }
                    None => None
                }
            })
            .collect::<Vec<Template>>();
        Ok(ret)
    }



    pub fn tabbed_data_page_name(&self) -> Option<String> {
        let ret = "Data:Listeria/".to_string() + &self.page_params.wiki + "/" + &self.page_params.page + ".tab";
        if ret.len() > 250 {
            return None; // Page title too long
        }
        Some(ret)
    }

    pub async fn write_tabbed_data(
        &mut self,
        tabbed_data_json: Value,
        commons_api: &mut Api,
    ) -> Result<(), String> {
        let data_page = self
            .tabbed_data_page_name()
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
        // TODO set data_has_changed is result is not "same as before"
        self.data_has_changed = true; // Just to make sure to update including page
        Ok(())
    }

    async fn load_page_as(&self, mode: &str) -> Result<String, String> {
        let mut params: HashMap<String, String> = vec![
            ("action", "parse"),
            ("prop", mode),
//            ("page", self.page.as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();

        match &self.page_params.simulated_text {
            Some(t) => {
                params.insert("title".to_string(),self.page_params.page.clone());
                params.insert("text".to_string(),t.to_string());
            }
            None => {
                params.insert("page".to_string(),self.page_params.page.clone());
            }
        }

        let result = self
            .page_params
            .mw_api
            .get_query_api_json(&params)
            .await
            .expect("Loading page failed");
        match result["parse"][mode]["*"].as_str() {
            Some(ret) => Ok(ret.to_string()),
            None => return Err(format!("No parse tree for {}", &self.page_params.page)),
        }
    }

    fn separate_start_template(&self, blob: &String) -> Option<(String, String)> {
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
                let mut template = blob.clone();
                let rest = template.split_off(pos);
                Some((template, rest))
            }
            None => None,
        }
    }

    pub fn as_wikitext(&self) -> Result<String,String> {
        let mut ret = String::new();
        for list in &self.lists {
            let mut renderer = RendererWikitext::new();
            ret += &renderer.render(&list)?;
        }
        Ok(ret)
    }

    pub async fn update_source_page(&self) -> Result<(), String> {
        let wikitext = self.load_page_as("wikitext").await?;

        // TODO use local template name

        // Start/end template
        let pattern1 =
            r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)(\{\{[Ww]ikidata[ _]list[ _]end\}\})(.*)"#;

        // No end template
        let pattern2 = r#"^(.*?)(\{\{[Ww]ikidata[ _]list\b.+)"#;

        let re_wikitext1: Regex = RegexBuilder::new(pattern1)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();
        let re_wikitext2: Regex = RegexBuilder::new(pattern2)
            .multi_line(true)
            .dot_matches_new_line(true)
            .build()
            .unwrap();

        let (before, blob, end_template, after) = match re_wikitext1.captures(&wikitext) {
            Some(caps) => (
                caps.get(1).unwrap().as_str(),
                caps.get(2).unwrap().as_str(),
                caps.get(3).unwrap().as_str(),
                caps.get(4).unwrap().as_str(),
            ),
            None => match re_wikitext2.captures(&wikitext) {
                Some(caps) => (
                    caps.get(1).unwrap().as_str(),
                    caps.get(2).unwrap().as_str(),
                    "",
                    "",
                ),
                None => return Err(format!("No template/end template found")),
            },
        };

        let (start_template, rest) = match self.separate_start_template(&blob.to_string()) {
            Some(parts) => parts,
            None => return Err(format!("Can't split start template")),
        };

        let append = if end_template.is_empty() {
            rest.to_string()
        } else {
            after.to_string()
        };

        // Remove tabbed data marker
        let start_template = Regex::new(r"\|\s*tabbed_data[^\|\}]*")
            .unwrap()
            .replace(&start_template, "");

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
            if self.data_has_changed {
                self.purge_page().await?;
            }
            return Ok(());
        }

        // TODO edit page

        Ok(())
    }

    async fn purge_page(&self) -> Result<(), String> {
        if self.page_params.simulate {
            println!("SIMULATING: purging [[{}]] on {}", &self.page_params.page,self.page_params.wiki);
            return Ok(())
        }
        let params: HashMap<String, String> =
            vec![("action", "purge"), ("titles", self.page_params.page.as_str())]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();

        match self.page_params.mw_api.get_query_api_json(&params).await {
            Ok(_r) => Ok(()),
            Err(e) => return Err(format!("{:?}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs ;
    use std::path::PathBuf;
    use crate::* ;

    fn read_fixture_from_file(path:PathBuf) -> HashMap<String,String> {
        let text = fs::read_to_string(path).unwrap();
        let rows = text.split("\n");
        let mut key = String::new();
        let mut value = String::new();
        let mut data : HashMap<String,String> = HashMap::new() ;
        for row in rows {
            if row.starts_with("$$$$") {
                if !key.is_empty() {
                    data.insert(key,value.trim().to_string()) ;
                }
                value.clear() ;
                key = row.strip_prefix("$$$$").unwrap().trim().to_string().to_uppercase();
            } else {
                value += "\n";
                value += row ;
            }
        }
        if !key.is_empty() {
            data.insert(key,value.trim().to_string());
        }
        data
    }

    async fn check_fixture_file(path:PathBuf) {
        //println!("{:?}",&path);
        let data = read_fixture_from_file ( path ) ;
        let mw_api = wikibase::mediawiki::api::Api::new(&data["API"]).await.unwrap(); // TODO reuse existing one?
        let wb_api = Api::new("https://www.wikidata.org/w/api.php").await.unwrap();
        let mw_api = Arc::new(mw_api);
        let wb_api = Arc::new(wb_api);
        let mut page = ListeriaPage::new(mw_api, data["PAGETITLE"].clone(),wb_api).await.unwrap();
        page.do_simulate(data.get("WIKITEXT").map(|s|s.to_string()),data.get("SPARQL_RESULTS").map(|s|s.to_string()));
        page.run().await.unwrap();
        let wt = page.as_wikitext().unwrap().trim().to_string();
        if data.contains_key("EXPECTED") {
            //println!("Checking EXPECTED");
            //println!("{}",&wt);
            assert_eq!(wt,data["EXPECTED"]);
        }
        if data.contains_key("EXPECTED_PART") {
            //println!("Checking EXPECTED_PART");
            assert!(wt.contains(&data["EXPECTED_PART"]));
        }
    }

    #[tokio::test]
    async fn shadow_images() {
        check_fixture_file(PathBuf::from("test_data/shadow_images.fixture")).await;
    }

    #[tokio::test]
    async fn summary_itemnumber() {
        check_fixture_file(PathBuf::from("test_data/summary_itemnumber.fixture")).await;
    }

    #[tokio::test]
    async fn header_template() {
        check_fixture_file(PathBuf::from("test_data/header_template.fixture")).await;
    }

    #[tokio::test]
    async fn header_row_template() {
        check_fixture_file(PathBuf::from("test_data/header_row_template.fixture")).await;
    }

    #[tokio::test]
    async fn links_all() {
        check_fixture_file(PathBuf::from("test_data/links_all.fixture")).await;
    }

    #[tokio::test]
    async fn links_red() {
        check_fixture_file(PathBuf::from("test_data/links_red.fixture")).await;
    }

    #[tokio::test]
    async fn links_red_only() {
        check_fixture_file(PathBuf::from("test_data/links_red_only.fixture")).await;
    }

    #[tokio::test]
    async fn links_text() {
        check_fixture_file(PathBuf::from("test_data/links_text.fixture")).await;
    }

    #[tokio::test]
    async fn links_local() {
        check_fixture_file(PathBuf::from("test_data/links_local.fixture")).await;
    }

    #[tokio::test]
    async fn links_reasonator() {
        check_fixture_file(PathBuf::from("test_data/links_reasonator.fixture")).await;
    }

    #[tokio::test]
    async fn date_extid_quantity() {
        check_fixture_file(PathBuf::from("test_data/date_extid_quantity.fixture")).await;
    }

    #[tokio::test]
    async fn coordinates() {
        check_fixture_file(PathBuf::from("test_data/coordinates.fixture")).await;
    }

    #[tokio::test]
    async fn sort_label() {
        check_fixture_file(PathBuf::from("test_data/sort_label.fixture")).await;
    }

    #[tokio::test]
    async fn sort_prop_item() {
        check_fixture_file(PathBuf::from("test_data/sort_prop_item.fixture")).await;
    }

    #[tokio::test]
    async fn sort_prop_time() {
        check_fixture_file(PathBuf::from("test_data/sort_prop_time.fixture")).await;
    }

    #[tokio::test]
    async fn sort_prop_string() {
        check_fixture_file(PathBuf::from("test_data/sort_prop_string.fixture")).await;
    }

    #[tokio::test]
    async fn sort_prop_quantity() {
        check_fixture_file(PathBuf::from("test_data/sort_prop_quantity.fixture")).await;
    }

    #[tokio::test]
    async fn sort_prop_monolingual() {
        check_fixture_file(PathBuf::from("test_data/sort_prop_monolingual.fixture")).await;
    }

    #[tokio::test]
    async fn sort_reverse() {
        check_fixture_file(PathBuf::from("test_data/sort_reverse.fixture")).await;
    }

    #[tokio::test]
    async fn sort_family_name() {
        check_fixture_file(PathBuf::from("test_data/sort_family_name.fixture")).await;
    }

    #[tokio::test]
    async fn columns() {
        check_fixture_file(PathBuf::from("test_data/columns.fixture")).await;
    }

    #[tokio::test]
    async fn p_p() {
        check_fixture_file(PathBuf::from("test_data/p_p.fixture")).await;
    }

    #[tokio::test]
    async fn p_q_p() {
        check_fixture_file(PathBuf::from("test_data/p_q_p.fixture")).await;
    }

    #[tokio::test]
    async fn sections() {
        check_fixture_file(PathBuf::from("test_data/sections.fixture")).await;
    }

    /*
    // I want all of it, Molari, ALL OF IT!
    #[tokio::test]
    async fn fixtures() {
        let paths = fs::read_dir("./test_data").unwrap();
        for path in paths {
            let path = path.unwrap();
            println!("Fixture {}",path.path().display());
            check_fixture_file(path.path()).await;
        }
    }
    */
}
