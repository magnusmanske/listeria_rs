//! CLI command implementations for the bot's operation modes.

use crate::status_server::{AppState, StatusServer};
use crate::wiki_page_result::WikiPageResult;
use crate::{
    configuration::Configuration, entity_container_wrapper::EntityContainerWrapper,
    listeria_bot::ListeriaBot, listeria_bot_single::ListeriaBotSingle,
    listeria_bot_wikidata::ListeriaBotWikidata, listeria_page::ListeriaPage, wiki_apis::WikiApis,
};
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::fs::read_to_string;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore};
use wikimisc::{seppuku::Seppuku, wikibase::EntityTrait};

const MAX_INACTIVITY_BEFORE_SEPPUKU_SEC: u64 = 300;

#[derive(Debug, Clone)]
pub struct MainCommands {
    pub config: Arc<Configuration>,
    pub config_file: String,
}

impl MainCommands {
    /// Processes a single page and updates it with generated list content.
    async fn update_page(&self, page_title: &str, api_url: &str) -> Result<String> {
        let mut mw_api = wikimisc::mediawiki::api::Api::new(api_url).await?;
        mw_api.set_oauth2(self.config.oauth2_token());

        let mw_api = Arc::new(mw_api);
        let mut page = ListeriaPage::new(self.config.clone(), mw_api, page_title.into()).await?;
        page.run().await.map_err(|e| anyhow!("{e:?}"))?;

        Ok(
            match page
                .update_source_page()
                .await
                .map_err(|e| anyhow!("{e:?}"))?
            {
                true => format!("{page_title} edited"),
                false => format!("{page_title} not edited"),
            },
        )
    }

    /// Updates the wiki list in the database and processes all queued pages.
    pub async fn update_wikis(&self) -> Result<()> {
        let wiki_list = WikiApis::new(self.config.clone()).await?;
        wiki_list.update_wiki_list_in_database().await?;
        wiki_list.update_all_wikis().await?;
        Ok(())
    }

    pub async fn load_test_entities(&mut self) -> Result<()> {
        let mut items = tokio::task::spawn_blocking(|| -> Result<Vec<String>> {
            let content = read_to_string("test_data/entities.tab")?;
            Ok(content.lines().map(|l| l.to_string()).collect())
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking join error: {e}"))??;
        // These two can be missing for some reason?
        items.push("Q3".to_string());
        items.push("Q4".to_string());

        let config = Arc::get_mut(&mut self.config)
            .ok_or(anyhow!("Failed to get mutable reference to config"))?;
        config.set_max_local_cached_entities(1000000); // A lot
        let ecw = EntityContainerWrapper::new().await?;
        let api = wikimisc::mediawiki::api::Api::new("https://www.wikidata.org/w/api.php").await?;
        ecw.load_entities(&api, &items).await?;

        let mut first = true;
        for item in items {
            let entity = match ecw.get_entity(&item).await {
                Some(e) => e,
                None => continue,
            };
            if first {
                println!("{{");
                first = false;
            } else {
                println!(",");
            }
            print!("\"{item}\":{}", serde_json::to_string(&entity.to_json())?);
        }
        println!("\n}}");
        Ok(())
    }

    pub async fn process_page(&self, server: &str, page: &str) -> Result<()> {
        let wiki_api = format!("https://{}/w/api.php", &server);
        let message = match self.update_page(page, &wiki_api).await {
            Ok(m) => format!("OK: {m}"),
            Err(e) => format!("ERROR: {e}"),
        };
        log::info!("{message}");
        Ok(())
    }

    pub async fn run_wikidata_bot(&self) -> Result<()> {
        let config = Arc::new((*self.config).clone());
        let bot = ListeriaBotWikidata::new_from_config(config).await?;
        let max_threads = bot.config().max_threads();
        log::info!("Starting {max_threads} bots");
        let _ = bot.reset_running().await;
        let _ = bot.clear_deleted().await;
        let _ = bot.clear_log_table().await;

        let bot = Arc::new(bot);
        static THREADS_SEMAPHORE: Semaphore = Semaphore::const_new(0);
        THREADS_SEMAPHORE.add_permits(max_threads);
        let seppuku = Seppuku::new(MAX_INACTIVITY_BEFORE_SEPPUKU_SEC);
        seppuku.arm();
        loop {
            let page = match bot.prepare_next_single_page().await {
                Ok(page) => page,
                Err(e) => {
                    log::warn!("Trying to get next page to process: {e}");
                    continue;
                }
            };

            let permit = THREADS_SEMAPHORE.acquire().await?;
            log::info!(
                "Starting new bot, {} running, {} available",
                max_threads - THREADS_SEMAPHORE.available_permits(),
                THREADS_SEMAPHORE.available_permits()
            );
            let bot = bot.clone();
            seppuku.alive();
            tokio::spawn(async move {
                let pagestatus_id = page.id();
                let start_time = Instant::now();
                if let Err(e) = bot.run_single_bot(page).await {
                    log::error!("Bot run failed: {e}");
                }
                let end_time = Instant::now();
                let diff = (end_time - start_time).as_secs();
                let _ = bot.set_runtime(pagestatus_id, diff).await;
                bot.release_running(pagestatus_id).await;
                drop(permit);
            });
        }
    }

    pub async fn run_single_wiki_bot(&self, once: bool) -> Result<()> {
        let state = AppState {
            pages: Arc::new(Mutex::new(HashMap::new())),
            started: Instant::now(),
            wiki_page_pattern: self.config.wiki_page_pattern(),
        };
        if let Some(port) = self.config.status_server_port() {
            let state_clone = state.clone();
            tokio::spawn(async move {
                if let Err(e) = StatusServer::run(port, state_clone).await {
                    log::error!("Status server error: {e}");
                }
            });
        }
        let config = Arc::new((*self.config).clone());
        let bot = ListeriaBotSingle::new_from_config(config).await?;
        let seppuku = Seppuku::new(MAX_INACTIVITY_BEFORE_SEPPUKU_SEC);
        seppuku.arm();
        loop {
            let page = match bot.prepare_next_single_page().await {
                Ok(page) => page,
                Err(_error) => {
                    if once {
                        if !bot.config().quiet() {
                            log::info!("All pages processed");
                        }
                        return Ok(());
                    }
                    if !bot.config().quiet() {
                        log::info!("All pages processed, restarting from beginning");
                    }
                    continue;
                }
            };

            seppuku.alive();
            let start_time = Instant::now();
            let mut result = match bot.run_single_bot(page.clone()).await {
                Ok(result) => result,
                Err(e) => WikiPageResult::new("wiki", page.title(), "Error", e.to_string()),
            };
            let end_time = Instant::now();
            let diff = end_time - start_time;
            result.set_runtime(diff);
            result.set_completed(Instant::now());
            state
                .pages
                .lock()
                .await
                .insert(page.title().to_string(), result);
            if let Some(seconds) = bot.config().delay_after_page_check_sec() {
                seppuku.disarm();
                tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
                seppuku.arm();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use wiremock::matchers::{body_string_contains, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn create_test_config() -> Arc<Configuration> {
        let config_content =
            fs::read_to_string("config.json").expect("config.json file should exist for tests");
        let j: serde_json::Value =
            serde_json::from_str(&config_content).expect("config.json should be valid JSON");
        let config = Configuration::new_from_json(j)
            .await
            .expect("Configuration should be created from JSON");
        Arc::new(config)
    }

    fn create_main_commands(config: Arc<Configuration>) -> MainCommands {
        MainCommands {
            config,
            config_file: "config.json".to_string(),
        }
    }

    /// Helper to create a mock API response for tokens
    fn mock_token_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "query": {
                "tokens": {
                    "csrftoken": "test_csrf_token+\\"
                }
            }
        }))
    }

    /// Helper to create a mock API response for page info
    fn mock_page_info_response(page_title: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "query": {
                "pages": {
                    "12345": {
                        "pageid": 12345,
                        "ns": 0,
                        "title": page_title,
                        "revisions": [{
                            "contentformat": "text/x-wiki",
                            "contentmodel": "wikitext",
                            "slots": {
                                "main": {
                                    "*": "Test page content\n{{Wikidata list}}"
                                }
                            }
                        }]
                    }
                }
            }
        }))
    }

    /// Helper to create a mock API response for SPARQL query (empty result)
    fn mock_sparql_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "head": {
                "vars": ["item"]
            },
            "results": {
                "bindings": []
            }
        }))
    }

    /// Helper to create a mock API response for page edit success
    fn mock_edit_success_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "edit": {
                "result": "Success",
                "pageid": 12345,
                "title": "Test Page",
                "contentmodel": "wikitext",
                "oldrevid": 100,
                "newrevid": 101
            }
        }))
    }

    #[tokio::test]
    async fn test_update_page_with_mock_server() {
        // Setup mock server
        let mock_server = MockServer::start().await;
        let api_url = format!("{}/w/api.php", mock_server.uri());

        // Mock the token request
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .and(query_param("meta", "tokens"))
            .respond_with(mock_token_response())
            .mount(&mock_server)
            .await;

        // Mock the page content request
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .and(query_param("prop", "revisions"))
            .respond_with(mock_page_info_response("Test Page"))
            .mount(&mock_server)
            .await;

        // Mock the SPARQL endpoint if needed
        Mock::given(method("POST"))
            .and(path("/sparql"))
            .respond_with(mock_sparql_response())
            .mount(&mock_server)
            .await;

        // Mock the edit request (if page gets edited)
        Mock::given(method("POST"))
            .and(path("/w/api.php"))
            .and(body_string_contains("action=edit"))
            .respond_with(mock_edit_success_response())
            .mount(&mock_server)
            .await;

        // Create test configuration
        let config = create_test_config().await;
        let main_commands = create_main_commands(config);

        // Test update_page - Note: This will likely fail because update_page
        // requires a full ListeriaPage setup which involves database and other dependencies
        // This test demonstrates the wiremock setup, but may need mocking of other components
        let result = main_commands.update_page("Test Page", &api_url).await;

        // The result depends on the full implementation, but we've mocked the HTTP layer
        // In a real scenario, you'd need to mock database and other external dependencies too
        match result {
            Ok(msg) => println!("Update succeeded: {}", msg),
            Err(e) => println!("Update failed (expected due to dependencies): {}", e),
        }
    }

    #[tokio::test]
    async fn test_update_page_token_request() {
        let mock_server = MockServer::start().await;
        let api_url = format!("{}/w/api.php", mock_server.uri());

        // Mock the initial siteinfo request that Api::new makes
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .and(query_param("meta", "siteinfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "query": {
                    "general": {
                        "sitename": "Test Wiki"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        // Mock the token request
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .and(query_param("meta", "tokens"))
            .respond_with(mock_token_response())
            .mount(&mock_server)
            .await;

        // This will test that the API creation works with the mock server
        let api = wikimisc::mediawiki::api::Api::new(&api_url).await;
        assert!(api.is_ok(), "API should be created successfully");
    }

    #[tokio::test]
    async fn test_update_page_error_handling() {
        let mock_server = MockServer::start().await;
        let api_url = format!("{}/w/api.php", mock_server.uri());

        // Mock a server error response
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let config = create_test_config().await;
        let main_commands = create_main_commands(config);

        // This should handle the error gracefully
        let result = main_commands.update_page("Test Page", &api_url).await;

        // We expect an error due to the 500 response
        assert!(result.is_err(), "Should return error for server failure");
    }

    #[tokio::test]
    async fn test_update_page_with_invalid_api_url() {
        let config = create_test_config().await;
        let main_commands = create_main_commands(config);

        // Test with an invalid URL
        let result = main_commands
            .update_page(
                "Test Page",
                "http://invalid-url-that-does-not-exist.local/api.php",
            )
            .await;

        assert!(result.is_err(), "Should return error for invalid URL");
    }

    #[tokio::test]
    async fn test_mock_api_page_content() {
        let mock_server = MockServer::start().await;
        let api_url = format!("{}/w/api.php", mock_server.uri());

        // Mock the initial siteinfo request that Api::new makes
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .and(query_param("meta", "siteinfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "query": {
                    "general": {
                        "sitename": "Test Wiki"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        // Mock token request
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .and(query_param("meta", "tokens"))
            .respond_with(mock_token_response())
            .mount(&mock_server)
            .await;

        // Mock page content request
        let page_title = "Sample Page";
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .and(query_param("prop", "revisions"))
            .respond_with(mock_page_info_response(page_title))
            .mount(&mock_server)
            .await;

        // Create API and verify it can fetch page info
        let api = wikimisc::mediawiki::api::Api::new(&api_url)
            .await
            .expect("API should be created");

        // This demonstrates that the mock server is working correctly
        // The actual page operations would require more complex mocking
        assert_eq!(
            api.get_site_info_string("general", "sitename")
                .unwrap_or_default(),
            "Test Wiki"
        );
    }

    #[tokio::test]
    async fn test_process_page_with_mock() {
        let mock_server = MockServer::start().await;
        let server_url = mock_server.uri().replace("http://", "");

        // Setup all necessary mocks
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .respond_with(mock_token_response())
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("action", "query"))
            .respond_with(mock_page_info_response("Test"))
            .mount(&mock_server)
            .await;

        let config = create_test_config().await;
        let main_commands = create_main_commands(config);

        // This will likely fail due to other dependencies, but demonstrates the setup
        let result = main_commands.process_page(&server_url, "Test").await;

        // We're mainly testing that the function can be called and doesn't panic
        match result {
            Ok(_) => println!("Process page completed"),
            Err(e) => println!("Expected error due to mocked environment: {}", e),
        }
    }

    #[test]
    fn test_main_commands_creation() {
        // Test that MainCommands can be created with basic config
        let config = Arc::new(Configuration::default());
        let main_commands = MainCommands {
            config: config.clone(),
            config_file: "test_config.json".to_string(),
        };

        assert_eq!(main_commands.config_file, "test_config.json");
        assert!(Arc::ptr_eq(&main_commands.config, &config));
    }

    #[tokio::test]
    async fn test_mock_server_setup() {
        // Basic test to ensure wiremock is working
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(200).set_body_string("Hello, World!"))
            .mount(&mock_server)
            .await;

        let response = reqwest::get(format!("{}/test", mock_server.uri()))
            .await
            .expect("Request should succeed");

        assert_eq!(response.status(), 200);
        assert_eq!(
            response.text().await.expect("Should have body"),
            "Hello, World!"
        );
    }
}
