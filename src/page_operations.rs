use crate::listeria_page::ListeriaPage;
use crate::page_element::PageElement;
use crate::wiki_page_result::WikiPageResult;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

/// Max MediaWiki API attempts per request (1 initial + retries).
const MW_API_MAX_ATTEMPTS: u32 = 3;
/// Initial backoff between MediaWiki API retries; doubles each attempt.
const MW_API_INITIAL_BACKOFF_MS: u64 = 250;

/// Retries an async fallible operation with exponential backoff.
///
/// Logs every failed-and-retried attempt via `tracing::warn!` carrying the
/// `operation`, `attempt`, `backoff_ms`, and `error` fields, so retry behaviour
/// is observable in production. The final failure is returned to the caller
/// unchanged — callers decide how to surface it.
///
/// Used only for transport-level errors (network / timeout / 5xx) at the
/// `post_query_api_json` boundary. API-level errors that arrive as `Ok(json)`
/// with a populated `error` object are not retried — those are semantic
/// failures (e.g. `missingtitle`) where retrying changes nothing.
async fn retry_with_backoff<T, F, Fut, E>(
    operation: &'static str,
    max_attempts: u32,
    initial_backoff: Duration,
    mut f: F,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut backoff = initial_backoff;
    let mut attempt: u32 = 1;
    loop {
        match f().await {
            Ok(value) => return Ok(value),
            Err(e) if attempt < max_attempts => {
                tracing::warn!(
                    operation = operation,
                    attempt = attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "MediaWiki API call failed; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

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
        let result = retry_with_backoff(
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
        let j = retry_with_backoff(
            "save_wikitext_to_page",
            MW_API_MAX_ATTEMPTS,
            Duration::from_millis(MW_API_INITIAL_BACKOFF_MS),
            || async { api.post_query_api_json(&params).await },
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

        let _ = retry_with_backoff(
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
    use std::sync::atomic::{AtomicU32, Ordering};

    // ── retry_with_backoff ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_retry_with_backoff_returns_immediately_on_success() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<&'static str, &'static str> = retry_with_backoff(
            "test",
            3,
            Duration::from_millis(1),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok("done")
            },
        )
        .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_retries_until_success() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<&'static str, &'static str> = retry_with_backoff(
            "test",
            3,
            Duration::from_millis(1),
            || async {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 { Err("transient") } else { Ok("done") }
            },
        )
        .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "must call exactly until the first success"
        );
    }

    #[tokio::test]
    async fn test_retry_with_backoff_returns_last_error_after_max_attempts() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<(), &'static str> = retry_with_backoff(
            "test",
            3,
            Duration::from_millis(1),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("nope")
            },
        )
        .await;
        assert_eq!(result.unwrap_err(), "nope");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "must call exactly max_attempts times"
        );
    }

    #[tokio::test]
    async fn test_retry_with_backoff_max_attempts_one_does_not_retry() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<(), &'static str> = retry_with_backoff(
            "test",
            1,
            Duration::from_millis(1),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("first")
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "max_attempts=1 must mean a single try with no retries"
        );
    }

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
