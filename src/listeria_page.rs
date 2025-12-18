//! Represents a wiki page containing one or more Listeria lists.

use anyhow::{Result, anyhow};
use futures::future::try_join_all;
use std::sync::Arc;

use crate::{
    ApiArc, configuration::Configuration, page_element::PageElement,
    page_operations::PageOperations, page_params::PageParams, render_wikitext::RendererWikitext,
    renderer::Renderer, wiki_page_result::WikiPageResult,
};

/* TODO
- Sort by P/P, P/Q/P DOES NOT WORK IN LISTERIA-PHP

TESTS:
- template resolution in SPARQL

TEMPLATE PARAMETERS
links IMPLEMENT fully?
*/

#[derive(Debug, Clone)]
pub struct ListeriaPage {
    page_params: Arc<PageParams>,
    data_has_changed: bool,
    elements: Vec<PageElement>,
}

impl ListeriaPage {
    pub async fn new(config: Arc<Configuration>, mw_api: ApiArc, page: String) -> Result<Self> {
        let page_params = PageParams::new(config, mw_api, page).await?;
        let page_params = Arc::new(page_params);
        Ok(Self {
            page_params,
            data_has_changed: false,
            elements: Vec::new(),
        })
    }

    pub fn config(&self) -> Arc<Configuration> {
        Arc::clone(&self.page_params.config())
    }

    pub fn wiki(&self) -> &str {
        self.page_params.wiki()
    }

    pub fn do_simulate(
        &mut self,
        text: Option<String>,
        sparql_results: Option<String>,
        autodesc: Option<Vec<String>>,
    ) -> Result<()> {
        Arc::get_mut(&mut self.page_params)
            .ok_or(anyhow!("Cannot simulate"))?
            .set_simulation(text, sparql_results, autodesc);
        Ok(())
    }

    pub fn page_params(&self) -> Arc<PageParams> {
        Arc::clone(&self.page_params)
    }

    pub fn language(&self) -> &str {
        self.page_params.language()
    }

    /// Verifies that the page is in an editable namespace.
    pub async fn check_namespace(&self) -> Result<()> {
        let api = self.page_params.mw_api();
        let title = wikimisc::mediawiki::title::Title::new_from_full(self.page_params.page(), api);
        if self
            .page_params
            .config()
            .can_edit_namespace(self.page_params.wiki(), title.namespace_id())
        {
            Ok(())
        } else {
            Err(anyhow!(
                "Namespace {} not allowed for edit on {}",
                title.namespace_id(),
                self.page_params.wiki()
            ))
        }
    }

    pub async fn run(&mut self) -> Result<(), WikiPageResult> {
        self.check_namespace()
            .await
            .map_err(|e| self.fail(&e.to_string()))?;
        self.elements = PageOperations::load_page(self).await?;

        let mut promises = Vec::new();
        for element in &mut self.elements {
            promises.push(element.process());
        }
        let _ = try_join_all(promises)
            .await
            .map_err(|e| self.fail(&e.to_string()))?;
        Ok(())
    }

    fn fail(&self, message: &str) -> WikiPageResult {
        WikiPageResult::fail(self.wiki(), self.page_params.page(), message)
    }

    pub async fn load_page_as(&self, mode: &str) -> Result<String, WikiPageResult> {
        PageOperations::load_page_as(self, mode).await
    }

    pub async fn as_wikitext(&mut self) -> Result<Vec<String>> {
        let mut ret: Vec<String> = Vec::with_capacity(self.elements.len());
        for element in &mut self.elements {
            if !element.is_just_text() {
                ret.push(element.new_inside().await?);
            }
        }
        Ok(ret)
    }

    pub const fn elements(&self) -> &Vec<PageElement> {
        &self.elements
    }

    pub async fn update_source_page(&mut self) -> Result<bool, WikiPageResult> {
        let renderer = RendererWikitext::new();
        let mut edited = false;
        let old_wikitext = self.load_page_as("wikitext").await?;
        let new_wikitext = renderer
            .get_new_wikitext(&old_wikitext, self)
            .await
            .map_err(|e| self.fail(&e.to_string()))?; // Safe
        match new_wikitext {
            Some(new_wikitext) => {
                if old_wikitext != new_wikitext {
                    PageOperations::save_wikitext_to_page(
                        self,
                        self.page_params.page(),
                        &new_wikitext,
                    )
                    .await
                    .map_err(|e| self.fail(&e.to_string()))?;
                    edited = true;
                }
            }
            None => {
                if self.data_has_changed {
                    PageOperations::purge_page(self)
                        .await
                        .map_err(|e| self.fail(&e.to_string()))?;
                }
            }
        }

        Ok(edited)
    }
}

#[cfg(test)]
mod tests {
    use self::configuration::Configuration;
    use crate::listeria_page::ListeriaPage;
    use crate::render_wikitext::RendererWikitext;
    use crate::renderer::Renderer;
    use crate::*;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::fs;
    use std::io::BufReader;
    use std::path::PathBuf;

    fn read_fixture_from_file(path: PathBuf) -> HashMap<String, String> {
        let text = fs::read_to_string(path).unwrap();
        let rows = text.split('\n');
        let mut key = String::new();
        let mut value = String::new();
        let mut data: HashMap<String, String> = HashMap::new();
        for row in rows {
            if row.starts_with("$$$$") {
                if !key.is_empty() {
                    data.insert(key, value.trim().to_string());
                }
                value.clear();
                key = row
                    .strip_prefix("$$$$")
                    .unwrap()
                    .trim()
                    .to_string()
                    .to_uppercase();
            } else {
                value += &format!("\n{row}");
            }
        }
        if !key.is_empty() {
            data.insert(key, value.trim().to_string());
        }
        data
    }

    async fn check_fixture_file(path: PathBuf) {
        let data = read_fixture_from_file(path.clone());
        let mw_api = wikimisc::mediawiki::api::Api::new(&data["API"])
            .await
            .unwrap();
        let mw_api = Arc::new(mw_api);

        let file = std::fs::File::open("config.json").unwrap();
        let reader = BufReader::new(file);
        let mut j: Value = serde_json::from_reader(reader).unwrap();
        j["namespace_blocks"] = json!({}); // Allow all namespaces, everywhere
        if path.to_str().unwrap() == "test_data/shadow_images.fixture" {
            // HACKISH
            j["prefer_preferred"] = json!(false);
        }
        let mut config = Configuration::new_from_json(j).await.unwrap();
        if path.to_str().unwrap() == "test_data/commons_sparql.fixture" {
            // HACKISH TODO FIXME
            let _ = config.wbapi_login("commons").await;
        }
        let config = Arc::new(config);
        let mut page = ListeriaPage::new(config, mw_api, data["PAGETITLE"].clone())
            .await
            .unwrap();
        page.do_simulate(
            data.get("WIKITEXT").map(|s| s.to_string()),
            data.get("SPARQL_RESULTS").map(|s| s.to_string()),
            data.get("AUTODESC")
                .map(|s| s.split('\n').map(|s| s.to_string()).collect()),
        )
        .unwrap();
        page.run().await.unwrap();
        let wt = page.as_wikitext().await.unwrap();
        let wt = wt.join("\n\n----\n\n");
        let wt = wt.trim().to_string();
        if data.contains_key("EXPECTED") {
            assert_eq!(wt, data["EXPECTED"]);
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

    // TODO update references.fixture
    // #[tokio::test]
    // async fn references() {
    //     check_fixture_file(PathBuf::from("test_data/references.fixture")).await;
    // }

    #[tokio::test]
    async fn wdedit() {
        check_fixture_file(PathBuf::from("test_data/wdedit.fixture")).await;
    }

    #[tokio::test]
    async fn curly_braces() {
        check_fixture_file(PathBuf::from("test_data/curly_braces.fixture")).await;
    }

    #[tokio::test]
    async fn item() {
        check_fixture_file(PathBuf::from("test_data/item.fixture")).await;
    }

    #[tokio::test]
    async fn column_header() {
        check_fixture_file(PathBuf::from("test_data/column_header.fixture")).await;
    }

    #[tokio::test]
    async fn item_column() {
        check_fixture_file(PathBuf::from("test_data/item_column.fixture")).await;
    }

    #[tokio::test]
    async fn template_empty_keys() {
        check_fixture_file(PathBuf::from("test_data/template_empty_keys.fixture")).await;
    }

    #[tokio::test]
    async fn qid() {
        check_fixture_file(PathBuf::from("test_data/qid.fixture")).await;
    }

    #[tokio::test]
    async fn alias_lang() {
        check_fixture_file(PathBuf::from("test_data/alias_lang.fixture")).await;
    }

    #[tokio::test]
    async fn image_pipe() {
        check_fixture_file(PathBuf::from("test_data/image_pipe.fixture")).await;
    }

    #[tokio::test]
    async fn edit_wikitext() {
        let data = read_fixture_from_file(PathBuf::from("test_data/edit_wikitext.fixture"));
        let mw_api = wikimisc::mediawiki::api::Api::new("https://en.wikipedia.org/w/api.php")
            .await
            .unwrap();
        let mw_api = Arc::new(mw_api);
        let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());
        let mut page = ListeriaPage::new(
            config,
            mw_api,
            "User:Magnus Manske/listeria test5".to_string(),
        )
        .await
        .unwrap();
        page.do_simulate(
            data.get("WIKITEXT").map(|s| s.to_string()),
            data.get("SPARQL_RESULTS").map(|s| s.to_string()),
            None,
        )
        .unwrap();
        page.run().await.unwrap();
        let wikitext = page
            .load_page_as("wikitext")
            .await
            .expect("FAILED load page as wikitext");
        let renderer = RendererWikitext::new();
        let wt = renderer
            .get_new_wikitext(&wikitext, &page)
            .await
            .expect("FAILED get_new_wikitext")
            .expect("new_wikitext not Some()");
        let wt = wt.trim().to_string();
        assert_eq!(wt, data["EXPECTED"]);
    }
}
