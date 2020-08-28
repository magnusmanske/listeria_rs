use crate::*;
use futures::future::try_join_all;
use std::sync::Arc;
use std::collections::HashMap;
use wikibase::mediawiki::api::Api;
use crate::column::*;

/* TODO
- Sort by P/P, P/Q/P DOES NOT WORK IN LISTERIA-PHP

TEMPLATE PARAMETERS
links IMPLEMENT fully
references IMPLEMENT
freq IGNORED => bot manager

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
    page_params: Arc<PageParams>,
    results: Vec<ResultRow>,
    data_has_changed: bool,
    elements:Vec<PageElement>,
}

impl ListeriaPage {
    pub async fn new(config: Arc<Configuration>, mw_api: Arc<RwLock<Api>>, page: String) -> Result<Self,String> {
        let page_params = PageParams::new(config, mw_api, page).await? ;
        let page_params = Arc::new(page_params);
        Ok(Self {
            page_params,
            results: vec![],
            data_has_changed: false,
            elements:vec![],
        })
    }

    pub fn config(&self) -> &Configuration {
        &self.page_params.config
    }

    pub fn wiki(&self) -> &String {
        &self.page_params.wiki
    }

    pub fn do_simulate(&mut self,text: Option<String>, sparql_results:Option<String>) {
        match Arc::get_mut(&mut self.page_params) {
            Some(pp) => {
                pp.simulate = true ;
                pp.simulated_text = text ;
                pp.simulated_sparql_results = sparql_results ;        
            }
            None => {
                panic!("Cannot simulate")
            }
        }
    }

    pub fn page_params(&self) -> Arc<PageParams> {
        self.page_params.clone()
    }

    pub fn language(&self) -> &String {
        &self.page_params.language
    }

    pub async fn check_namespace(&self) -> Result<(),String> {
        let api = self.page_params.mw_api.read().await;
        let title = wikibase::mediawiki::title::Title::new_from_full(&self.page_params.page,&api);
        drop(api);
        if self.page_params.config.can_edit_namespace(&self.page_params.wiki,title.namespace_id()) {
            Ok(())
        } else {
            Err(format!("Namespace {} not allowed for edit on {}",title.namespace_id(),&self.page_params.wiki))
        }
    }

    pub async fn run(&mut self) -> Result<(), String> {
        self.check_namespace().await?;
        self.elements = self.load_page().await?;

        let mut promises = Vec::new();
        for element in &mut self.elements {
            promises.push(element.process());
        }
        try_join_all(promises).await?;
        Ok(())
    }

    async fn load_page(&mut self) -> Result<Vec<PageElement>, String> {
        let mut text = self.load_page_as("wikitext").await?;
        let mut ret = vec![] ;
        let mut again : bool = true ;
        while again {
            let mut element = match PageElement::new_from_text(&text,&self) {
                Some(pe) => pe,
                None => {
                    again = false ;
                    PageElement::new_just_text(&text,self)
                }
            };
            if again { text = element.get_and_clean_after() ; }
            ret.push(element);
        }
        Ok(ret)
    }

    /*
    async fn _load_page(&mut self) -> Result<Vec<Template>, String> {
        let text = self.load_page_as("parsetree").await?;
        let doc = roxmltree::Document::parse(&text).unwrap();
        let template_start = self.page_params.config.get_local_template_title_start(&self.page_params.wiki)? ;
        let ret = doc.root()
            .descendants()
            .filter(|n| n.is_element() && n.tag_name().name() == "template")
            .filter_map(|mut node| {
                match Template::new_from_xml(&mut node) {
                    Some(t) => {
                        // HARDCODED EN AS FALLBACK
                        if t.title == template_start || t.title == "Wikidata list" {
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
    */

    pub async fn load_page_as(&self, mode: &str) -> Result<String, String> {
        let mut params: HashMap<String, String> = vec![
            ("action", "parse"),
            ("prop", mode),
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
            .read()
            .await
            .post_query_api_json(&params)
            .await
            .map_err(|e|format!("Loading page failed: {}",e))?;
        match result["parse"][mode]["*"].as_str() {
            Some(ret) => Ok(ret.to_string()),
            None => Err(format!("No parse tree for {}", &self.page_params.page)),
        }
    }

    pub fn as_wikitext(&self) -> Result<Vec<String>,String> {
        let mut ret : Vec<String> = vec!();
        for element in &self.elements {
            if !element.is_just_text() {
                ret.push(element.new_inside()?);
            }
        }
        Ok(ret)
    }

    pub fn elements(&self) -> &Vec<PageElement> {
        &self.elements
    }

    async fn save_wikitext_to_page(&self,title:&str,wikitext:&str) -> Result<(),String> {
        let mut api = self.page_params.mw_api.write().await ;
        let token = api.get_edit_token().await.map_err(|e|e.to_string())?;
        let params: HashMap<String, String> = vec![
            ("action", "edit"),
            ("title", title),
            ("text", wikitext),
            ("summary", "Wikidata list updated [V2]"),
            ("token", &token),
        ]
        .into_iter()
        .map(|(k,v)|(k.to_string(),v.to_string()))
        .collect();
        api.post_query_api_json(&params).await.map_err(|e|e.to_string())?;
        Ok(())
    }


    pub async fn update_source_page(&mut self) -> Result<bool, String> {
        let renderer = RendererWikitext::new();
        let mut edited = false ;
        let old_wikitext = self.load_page_as("wikitext").await?;
        let new_wikitext = renderer.get_new_wikitext(&old_wikitext,self)? ; // Safe
        match new_wikitext {
            Some(new_wikitext) => {
                if old_wikitext != new_wikitext {
                    self.save_wikitext_to_page(&self.page_params.page,&new_wikitext).await?;
                    edited = true ;
                }
            }
            None => {
                if self.data_has_changed {
                    self.purge_page().await?;
                }    
            }
        }

        Ok(edited)
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

        match self
            .page_params
            .mw_api
            .write()
            .await
            .get_query_api_json(&params)
            .await {
            Ok(_r) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs ;
    use std::path::PathBuf;
    use crate::* ;
    use crate::render_wikitext::RendererWikitext;
    use crate::listeria_page::ListeriaPage;

    fn read_fixture_from_file(path:PathBuf) -> HashMap<String,String> {
        let text = fs::read_to_string(path).unwrap();
        let rows = text.split('\n');
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
                value += &format!("\n{}",row);
            }
        }
        if !key.is_empty() {
            data.insert(key,value.trim().to_string());
        }
        data
    }

    async fn check_fixture_file(path:PathBuf) {
        let data = read_fixture_from_file ( path.clone() ) ;
        let mw_api = wikibase::mediawiki::api::Api::new(&data["API"]).await.unwrap();
        let mw_api = Arc::new(RwLock::new(mw_api));

        let file = std::fs::File::open("config.json").unwrap();
        let reader = BufReader::new(file);
        let mut j : Value = serde_json::from_reader(reader).unwrap();
        j["namespace_blocks"] = json!({}); // Allow all namespaces, everywhere
        if path.to_str().unwrap() == "test_data/shadow_images.fixture" { // HACKISH
            j["prefer_preferred"] = json!(false) ;
        }
        let mut config = Configuration::new_from_json(j).await.unwrap();
        if path.to_str().unwrap() == "test_data/commons_sparql.fixture" { // HACKISH
            let result = config.wbapi_login("commons").await;
            println!("LOGIN TO COMMONS: {}",result);
        }
        let config = Arc::new(config);

        let mut page = ListeriaPage::new(config,mw_api, data["PAGETITLE"].clone()).await.unwrap();
        page.do_simulate(data.get("WIKITEXT").map(|s|s.to_string()),data.get("SPARQL_RESULTS").map(|s|s.to_string()));
        page.run().await.unwrap();
        let wt = page.as_wikitext().unwrap();
        let wt = wt.join("\n\n----\n\n");
        let wt = wt.trim().to_string();
        if data.contains_key("EXPECTED") {
            assert_eq!(wt,data["EXPECTED"]);
        }
        if data.contains_key("EXPECTED_PART") {
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

    #[tokio::test]
    async fn preferred_rank() {
        check_fixture_file(PathBuf::from("test_data/preferred_rank.fixture")).await;
    }

    #[tokio::test]
    async fn multiple_lists() {
        check_fixture_file(PathBuf::from("test_data/multiple_lists.fixture")).await;
    }

    #[tokio::test]
    async fn autodesc() {
        check_fixture_file(PathBuf::from("test_data/autodesc.fixture")).await;
    }

    #[tokio::test]
    async fn dewiki() {
        check_fixture_file(PathBuf::from("test_data/dewiki.fixture")).await;
    }

    #[tokio::test]
    async fn dewiki_coordinates() {
        check_fixture_file(PathBuf::from("test_data/dewiki_coordinates.fixture")).await;
    }

    #[tokio::test]
    async fn commons() {
        check_fixture_file(PathBuf::from("test_data/commons.fixture")).await;
    }

    #[tokio::test]
    async fn commons_sparql() {
        //check_fixture_file(PathBuf::from("test_data/commons_sparql.fixture")).await; // TODO
    }

    #[tokio::test]
    async fn references() {
        //check_fixture_file(PathBuf::from("test_data/references.fixture")).await;
    }

    #[tokio::test]
    async fn wdedit() {
        check_fixture_file(PathBuf::from("test_data/wdedit.fixture")).await;
    }

    #[tokio::test]
    async fn curly_braces() {
        check_fixture_file(PathBuf::from("test_data/curly_braces.fixture")).await;
    }

    #[tokio::test]
    async fn edit_wikitext() {
        let data = read_fixture_from_file ( PathBuf::from("test_data/edit_wikitext.fixture") ) ;
        let mw_api = wikibase::mediawiki::api::Api::new("https://en.wikipedia.org/w/api.php").await.unwrap();
        let mw_api = Arc::new(RwLock::new(mw_api));
        let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());
        let mut page = ListeriaPage::new(config,mw_api, "User:Magnus Manske/listeria test5".to_string()).await.unwrap();
        page.do_simulate(data.get("WIKITEXT").map(|s|s.to_string()),data.get("SPARQL_RESULTS").map(|s|s.to_string()));
        page.run().await.unwrap();
        let wikitext = page.load_page_as("wikitext").await.expect("FAILED load page as wikitext");
        let renderer = RendererWikitext::new();
        let wt = renderer.get_new_wikitext(&wikitext,&page).expect("FAILED get_new_wikitext").expect("new_wikitext not Some()");
        let wt = wt.trim().to_string();
        assert_eq!(wt,data["EXPECTED"]);
    }

}
