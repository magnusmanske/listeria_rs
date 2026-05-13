use crate::circuit_breaker::with_breaker;
use crate::listeria_page::ListeriaPage;
use crate::page_element::PageElement;
use crate::retry::retry_with_backoff;
use crate::wiki_page_result::WikiPageResult;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::time::Duration;

/// Max MediaWiki API attempts per request (1 initial + retries).
///
/// Used only for transport-level errors (network / timeout / 5xx) at the
/// `post_query_api_json` boundary. API-level errors that arrive as `Ok(json)`
/// with a populated `error` object are not retried — those are semantic
/// failures (e.g. `missingtitle`) where retrying changes nothing.
const MW_API_MAX_ATTEMPTS: u32 = 3;
/// Initial backoff between MediaWiki API retries; doubles each attempt.
const MW_API_INITIAL_BACKOFF_MS: u64 = 250;

/// Handles page loading, saving, and update operations for ListeriaPage
#[derive(Debug, Clone, Copy)]
pub struct PageOperations;

impl PageOperations {
    /// Loads a page and extracts all Listeria template blocks.
    pub async fn load_page(page: &mut ListeriaPage) -> Result<Vec<PageElement>, WikiPageResult> {
        let mut text = Self::load_page_as(page, "wikitext").await?;
        let mut ret = Vec::new();
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
        if ret.iter().any(|e| e.is_missing_end_template()) {
            return Err(WikiPageResult::new(
                page.page_params().wiki(),
                page.page_params().page(),
                "FAIL",
                "{{Wikidata list end}} is missing; refusing to update to prevent list duplication (#108)".to_string(),
            ));
        }

        Ok(ret)
    }

    /// Loads page content via the MediaWiki API in the specified format.
    pub async fn load_page_as(page: &ListeriaPage, mode: &str) -> Result<String, WikiPageResult> {
        // When wikitext is already available locally (simulate mode), skip the
        // action=parse round-trip — the API would return the same text unchanged.
        if mode == "wikitext"
            && let Some(t) = page.page_params().simulated_text()
        {
            return Ok(t.to_string());
        }

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
        let wiki = page.page_params().wiki().to_string();
        let breaker = page.page_params().config().mw_api_circuit_breaker(&wiki);
        let result = with_breaker(
            &breaker,
            || anyhow!("MW API circuit open for {wiki}"),
            || async {
                retry_with_backoff(
                    "load_page_as",
                    MW_API_MAX_ATTEMPTS,
                    Duration::from_millis(MW_API_INITIAL_BACKOFF_MS),
                    || async {
                        page.page_params()
                            .mw_api()
                            .post_query_api_json(&params)
                            .await
                    },
                )
                .await
                .map_err(anyhow::Error::from)
            },
        )
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
        // Token fetch is itself a network call — retry it on transient failure
        // so an edit isn't lost just because the CSRF endpoint flaked.
        // Inline retry loop (rather than reusing retry_with_backoff) because
        // `get_edit_token` borrows `&mut api`, which an `FnMut`-returning-async
        // closure cannot express without escape-of-captured-variable errors.
        let wiki = page_params.wiki().to_string();
        let breaker = page_params.config().mw_api_circuit_breaker(&wiki);
        // Token fetch is inside the breaker — a flapping CSRF endpoint should
        // count against the same wiki budget that the edit POST uses.
        let token = with_breaker(
            &breaker,
            || anyhow!("MW API circuit open for {wiki}"),
            || async { Self::get_edit_token_with_retries(&mut api).await },
        )
        .await?;
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
        let j = with_breaker(
            &breaker,
            || anyhow!("MW API circuit open for {wiki}"),
            || async {
                retry_with_backoff(
                    "save_wikitext_to_page",
                    MW_API_MAX_ATTEMPTS,
                    Duration::from_millis(MW_API_INITIAL_BACKOFF_MS),
                    || async { api.post_query_api_json(&params).await },
                )
                .await
                .map_err(anyhow::Error::from)
            },
        )
        .await?;
        match j["error"].as_object() {
            Some(o) => {
                let msg = o["info"].as_str().unwrap_or("Error while saving");
                Err(anyhow!("{msg}"))
            }
            None => Ok(()),
        }
    }

    /// Manual exponential-backoff retry for `Api::get_edit_token`.
    ///
    /// `get_edit_token` borrows `&mut Api`, so it cannot be expressed as an
    /// `FnMut`-returning-async closure for the generic [`retry_with_backoff`]
    /// helper without hitting "captured variable cannot escape FnMut closure
    /// body". Mirrors the same policy: `MW_API_MAX_ATTEMPTS` total tries with
    /// `MW_API_INITIAL_BACKOFF_MS` doubling each retry.
    async fn get_edit_token_with_retries(api: &mut wikimisc::mediawiki::api::Api) -> Result<String> {
        let mut backoff = Duration::from_millis(MW_API_INITIAL_BACKOFF_MS);
        let mut attempt: u32 = 1;
        loop {
            match api.get_edit_token().await {
                Ok(token) => return Ok(token),
                Err(e) if attempt < MW_API_MAX_ATTEMPTS => {
                    tracing::warn!(
                        operation = "get_edit_token",
                        attempt = attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "MediaWiki API call failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn purge_page(page: &ListeriaPage) -> Result<()> {
        if page.page_params().simulate() {
            log::info!(
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

        let wiki = page.page_params().wiki().to_string();
        let breaker = page.page_params().config().mw_api_circuit_breaker(&wiki);
        let _ = with_breaker(
            &breaker,
            || anyhow!("MW API circuit open for {wiki}"),
            || async {
                retry_with_backoff(
                    "purge_page",
                    MW_API_MAX_ATTEMPTS,
                    Duration::from_millis(MW_API_INITIAL_BACKOFF_MS),
                    || async {
                        page.page_params()
                            .mw_api()
                            .get_query_api_json(&params)
                            .await
                    },
                )
                .await
                .map_err(anyhow::Error::from)
            },
        )
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

    async fn create_test_page() -> ListeriaPage {
        let api = crate::test_utils::cached_api("https://en.wikipedia.org/w/api.php").await;
        let config = crate::test_utils::cached_config().await;
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
        page.do_simulate(Some("Test wikitext content".to_string()), None, None)
            .unwrap();

        // This should work with simulated text
        let result = PageOperations::load_page_as(&page, "wikitext").await;
        // The result might fail due to API issues, but the function should handle it
        // We're mainly testing that it doesn't panic
        let _ = result;
    }

    #[tokio::test]
    async fn test_purge_page_with_simulation() {
        let mut page = create_test_page().await;
        page.do_simulate(Some("Test".to_string()), None, None)
            .unwrap();

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
        page.do_simulate(Some("{{Wikidata list}}".to_string()), None, None)
            .unwrap();

        // This should attempt to load and parse the page
        let result = PageOperations::load_page(&mut page).await;
        // The result may vary, but we're testing it doesn't panic
        let _ = result;
    }
}
