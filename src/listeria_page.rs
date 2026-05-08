//! Represents a wiki page containing one or more Listeria lists.

use anyhow::{Result, anyhow};
use chrono::Utc;
use futures::future::try_join_all;
use std::collections::HashMap;
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
    elements: Vec<PageElement>,
}

impl ListeriaPage {
    pub async fn new(config: Arc<Configuration>, mw_api: ApiArc, page: String) -> Result<Self> {
        let page_params = PageParams::new(config, mw_api, page).await?;
        let page_params = Arc::new(page_params);
        Ok(Self {
            page_params,
            elements: Vec::new(),
        })
    }

    pub fn config(&self) -> Arc<Configuration> {
        Arc::clone(self.page_params.config())
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
    pub fn check_namespace(&self) -> Result<()> {
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

    fn max_freq(&self) -> u64 {
        self.elements
            .iter()
            .filter(|e| !e.is_just_text())
            .map(|e| e.freq())
            .max()
            .unwrap_or(0)
    }

    async fn get_last_edit_days_ago(&self) -> Option<u64> {
        let params: HashMap<String, String> = [
            ("action", "query"),
            ("prop", "revisions"),
            ("titles", self.page_params.page()),
            ("rvlimit", "1"),
            ("rvprop", "timestamp"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let result = self
            .page_params
            .mw_api()
            .get_query_api_json(&params)
            .await
            .ok()?;

        let timestamp = result["query"]["pages"]
            .as_object()?
            .values()
            .next()?["revisions"][0]["timestamp"]
            .as_str()?;

        let last_edit = chrono::DateTime::parse_from_rfc3339(timestamp).ok()?;
        let days = Utc::now()
            .signed_duration_since(last_edit)
            .num_days()
            .max(0) as u64;
        Some(days)
    }

    pub async fn update_source_page(&mut self) -> Result<bool, WikiPageResult> {
        let max_freq = self.max_freq();
        if max_freq > 0
            && !self.page_params.simulate()
            && let Some(days_ago) = self.get_last_edit_days_ago().await
            && days_ago < max_freq
        {
            return Ok(false);
        }

        let renderer = RendererWikitext::new();
        let mut edited = false;
        let old_wikitext = self.load_page_as("wikitext").await?;
        let new_wikitext = renderer
            .get_new_wikitext(&old_wikitext, self)
            .await
            .map_err(|e| self.fail(&e.to_string()))?; // Safe
        if let Some(new_wikitext) = new_wikitext
            && old_wikitext != new_wikitext
        {
            PageOperations::save_wikitext_to_page(
                self,
                self.page_params.page(),
                &new_wikitext,
            )
            .await
            .map_err(|e| self.fail(&e.to_string()))?;
            edited = true;
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

    /// Standard fixture test config: namespace_blocks cleared, prefer_preferred from config.json.
    static TEST_CONFIG_BASE: tokio::sync::OnceCell<Arc<Configuration>> =
        tokio::sync::OnceCell::const_new();

    /// shadow_images fixture config: same as base but prefer_preferred=false.
    static TEST_CONFIG_SHADOW: tokio::sync::OnceCell<Arc<Configuration>> =
        tokio::sync::OnceCell::const_new();

    async fn cached_config(prefer_preferred_false: bool) -> Arc<Configuration> {
        let cell = if prefer_preferred_false {
            &TEST_CONFIG_SHADOW
        } else {
            &TEST_CONFIG_BASE
        };
        cell.get_or_init(|| async move {
            let file = std::fs::File::open("config.json").unwrap();
            let reader = BufReader::new(file);
            let mut j: Value = serde_json::from_reader(reader).unwrap();
            j["namespace_blocks"] = json!({});
            if prefer_preferred_false {
                j["prefer_preferred"] = json!(false);
            }
            Arc::new(Configuration::new_from_json(j).await.unwrap())
        })
        .await
        .clone()
    }

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
        let mw_api = crate::test_utils::cached_api(&data["API"]).await;

        let is_shadow = path.ends_with("shadow_images.fixture");
        let config = cached_config(is_shadow).await;
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
    async fn summary_itemnumber_label() {
        check_fixture_file(PathBuf::from("test_data/summary_itemnumber_label.fixture")).await;
    }

    #[tokio::test]
    async fn sitelink_column() {
        check_fixture_file(PathBuf::from("test_data/sitelink_column.fixture")).await;
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
    async fn row_template_table() {
        check_fixture_file(PathBuf::from("test_data/row_template_table.fixture")).await;
    }

    #[tokio::test]
    async fn dewiki_sections_coordinates() {
        check_fixture_file(PathBuf::from("test_data/dewiki_sections_coordinates.fixture")).await;
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
    async fn sort_label_case_insensitive() {
        check_fixture_file(PathBuf::from("test_data/sort_label_case_insensitive.fixture")).await;
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

    // Flaky: depends on live SPARQL queries to Wikidata WDQS (no SPARQL_RESULTS in fixture).
    // When WDQS throttles or returns an HTML error page, page.run() errors with
    // "error decoding response body". Run with `cargo test -- --ignored` for live smoke tests.
    #[tokio::test]
    #[ignore = "live SPARQL endpoint dependency, intermittently throttled by WDQS"]
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

    // Flaky: asserts on `region=` values (e.g. GB-ENG, DE-NW, GR-I) that
    // ListProcessor::get_region_for_entity_id fetches via a live SPARQL query
    // (P131*/P300). The fixture cannot inject these results because that code
    // path forces set_simulate(false). When WDQS throttles, errors are
    // silently swallowed by `.ok()?` (list_processor.rs ~line 483) and the
    // resulting empty `region=` breaks the exact-match EXPECTED assertion.
    // Run with `cargo test -- --ignored` for a live smoke test.
    #[tokio::test]
    #[ignore = "live SPARQL region lookup is intermittently throttled by WDQS"]
    async fn dewiki_coordinates() {
        check_fixture_file(PathBuf::from("test_data/dewiki_coordinates.fixture")).await;
    }

    #[tokio::test]
    async fn trwiki_coordinates() {
        check_fixture_file(PathBuf::from("test_data/trwiki_coordinates.fixture")).await;
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
