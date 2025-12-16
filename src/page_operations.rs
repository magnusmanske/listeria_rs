use crate::listeria_page::ListeriaPage;
use crate::page_element::PageElement;
use crate::wiki_page_result::WikiPageResult;
use anyhow::{Result, anyhow};
use std::collections::HashMap;

/// Handles page loading, saving, and update operations for ListeriaPage
#[derive(Debug)]
pub struct PageOperations;

impl PageOperations {
    /// Loads a page and extracts all Listeria template blocks.
    pub async fn load_page(page: &mut ListeriaPage) -> Result<Vec<PageElement>, WikiPageResult> {
        let mut text = Self::load_page_as(page, "wikitext").await?;
        let mut ret = vec![];
        let mut again: bool = true;
        while again {
            let mut element = match PageElement::new_from_text(&text, page).await {
                Some(pe) => pe,
                None => {
                    again = false;
                    PageElement::new_just_text(&text, page)
                        .await
                        .map_err(|e| Self::fail(page, &e.to_string()))?
                }
            };
            if again {
                text = element.get_and_clean_after();
            }
            ret.push(element);
        }
        Ok(ret)
    }

    /// Loads page content via the MediaWiki API in the specified format.
    pub async fn load_page_as(page: &ListeriaPage, mode: &str) -> Result<String, WikiPageResult> {
        let mut params: HashMap<String, String> = [("action", "parse"), ("prop", mode)]
            .iter()
            .map(|x| (x.0.to_string(), x.1.to_string()))
            .collect();

        match page.page_params().simulated_text() {
            Some(t) => {
                params.insert("title".to_string(), page.page_params().page().to_string());
                params.insert("text".to_string(), t.to_string());
            }
            None => {
                params.insert("page".to_string(), page.page_params().page().to_string());
            }
        }
        let result = page
            .page_params()
            .mw_api()
            .post_query_api_json(&params)
            .await
            .map_err(|e| Self::fail(page, &e.to_string()))?;
        if let Some(error) = result["error"]["code"].as_str() {
            match error {
                "missingtitle" => {
                    return Err(WikiPageResult::new(
                        page.page_params().wiki(),
                        page.page_params().page(),
                        "DELETED",
                        "Wiki says this page is missing".to_string(),
                    ));
                }
                "invalid" => {
                    return Err(WikiPageResult::new(
                        page.page_params().wiki(),
                        page.page_params().page(),
                        "INVALID",
                        "Wiki says this page has an invalid title".to_string(),
                    ));
                }
                other => {
                    return Err(WikiPageResult::new(
                        page.page_params().wiki(),
                        page.page_params().page(),
                        "FAIL",
                        other.to_string(),
                    ));
                }
            }
        };
        match result["parse"][mode]["*"].as_str() {
            Some(ret) => Ok(ret.to_string()),
            None => Err(Self::fail(page, &format!("No parse tree for {mode}"))),
        }
    }

    pub async fn save_wikitext_to_page(
        page: &ListeriaPage,
        title: &str,
        wikitext: &str,
    ) -> Result<()> {
        let page_params = page.page_params();
        let api_arc = page_params.mw_api();
        let mut api = (**api_arc).clone();
        let token = api.get_edit_token().await?;
        let params: HashMap<String, String> = vec![
            ("action", "edit"),
            ("title", title),
            ("text", wikitext),
            ("summary", "Wikidata list updated [V2]"),
            ("token", &token),
            ("bot", "1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let j = api.post_query_api_json(&params).await?;
        match j["error"].as_object() {
            Some(o) => {
                let msg = o["info"].as_str().unwrap_or("Error while saving");
                Err(anyhow!("{msg}"))
            }
            None => Ok(()),
        }
    }

    pub async fn purge_page(page: &ListeriaPage) -> Result<()> {
        if page.page_params().simulate() {
            println!(
                "SIMULATING: purging [[{}]] on {}",
                page.page_params().page(),
                page.page_params().wiki()
            );
            return Ok(());
        }
        let params: HashMap<String, String> =
            [("action", "purge"), ("titles", page.page_params().page())]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();

        let _ = page
            .page_params()
            .mw_api()
            .get_query_api_json(&params)
            .await?;
        Ok(())
    }

    fn fail(page: &ListeriaPage, message: &str) -> WikiPageResult {
        WikiPageResult::fail(page.wiki(), page.page_params().page(), message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::Configuration;
    use std::sync::Arc;
    use wikimisc::mediawiki::api::Api;

    async fn create_test_page() -> ListeriaPage {
        let api = Api::new("https://en.wikipedia.org/w/api.php")
            .await
            .unwrap();
        let api = Arc::new(api);
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let config = Arc::new(config);

        ListeriaPage::new(config, api, "Test:Page".to_string())
            .await
            .unwrap()
    }

    #[test]
    fn test_page_operations_is_debug() {
        // Verify that PageOperations implements Debug
        let _ = format!("{:?}", PageOperations);
    }

    #[tokio::test]
    async fn test_load_page_as_wikitext_with_simulation() {
        let mut page = create_test_page().await;
        // Set up simulation with some test text
        page.do_simulate(Some("Test wikitext content".to_string()), None, None);

        // This should work with simulated text
        let result = PageOperations::load_page_as(&page, "wikitext").await;
        // The result might fail due to API issues, but the function should handle it
        // We're mainly testing that it doesn't panic
        let _ = result;
    }

    #[tokio::test]
    async fn test_purge_page_with_simulation() {
        let mut page = create_test_page().await;
        page.do_simulate(Some("Test".to_string()), None, None);

        // Purge should succeed in simulation mode without making actual API calls
        let result = PageOperations::purge_page(&page).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fail_creates_wiki_page_result() {
        let page = create_test_page().await;
        let result = PageOperations::fail(&page, "Test error message");

        assert_eq!(result.result(), "FAIL");
        assert!(result.message().contains("Test error message"));
    }

    #[tokio::test]
    async fn test_load_page_with_simulation() {
        let mut page = create_test_page().await;
        page.do_simulate(Some("{{Wikidata list}}".to_string()), None, None);

        // This should attempt to load and parse the page
        let result = PageOperations::load_page(&mut page).await;
        // The result may vary, but we're testing it doesn't panic
        let _ = result;
    }
}
