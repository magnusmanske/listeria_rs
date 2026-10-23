//! CLI command implementations for the bot's operation modes.

use crate::status_server::{AppState, StatusServer};
use crate::telemetry;
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
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tracing::Instrument;
use wikimisc::{seppuku::Seppuku, wikibase::EntityTrait};

/// How much longer than `page_timeout` the watchdog waits; see [`watchdog_window`].
const WATCHDOG_MARGIN: Duration = Duration::from_secs(120);
/// Pause before asking the queue again after it failed to hand out a page, so
/// a database problem does not turn into a tight loop hammering the database.
const DISPATCH_RETRY_DELAY: Duration = Duration::from_secs(5);
/// How often the Wikidata bot runs [`MainCommands::update_wikis`], which adds
/// new wikis and newly tagged pages to the queue.
const UPDATE_WIKIS_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// How long the inactivity watchdog waits for a page to start or finish.
///
/// A page may legitimately run for up to `page_timeout`, so the window has to
/// be longer than that: with a shorter one, a bot whose every worker is busy
/// with slow pages (e.g. while WDQS is slow) starts no page for a while and
/// is killed although it is working. Every page ends within `page_timeout`,
/// so a working bot always shows activity within the window; if it does not,
/// it is wedged, and exiting lets the job be restarted.
fn watchdog_window(page_timeout: Duration) -> Duration {
    page_timeout + WATCHDOG_MARGIN
}

/// The inactivity watchdog, armed; see [`watchdog_window`].
fn watchdog(page_timeout: Duration) -> Seppuku {
    let seppuku = Seppuku::new(watchdog_window(page_timeout).as_secs());
    seppuku.arm();
    seppuku
}

/// How processing one page ended; see [`run_page`].
#[derive(Debug)]
enum PageOutcome {
    /// Processing returned a result, after recording the page's status.
    Done(WikiPageResult),
    /// Processing returned an error.
    Failed(anyhow::Error),
    /// Processing was cut short, before it could record the page's status.
    Aborted(PageAbort),
}

#[derive(Debug)]
enum PageAbort {
    TimedOut(Duration),
    /// The panic message.
    Panicked(String),
}

impl std::fmt::Display for PageAbort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut(budget) => write!(
                f,
                "Page processing exceeded {}s wall-clock budget",
                budget.as_secs()
            ),
            Self::Panicked(message) => write!(f, "Page processing panicked: {message}"),
        }
    }
}

impl PageOutcome {
    fn record_telemetry(&self, runtime: Duration) {
        match self {
            Self::Done(result) => telemetry::record_page(result.result(), runtime),
            Self::Failed(_) => telemetry::record_page(telemetry::STATUS_ERROR, runtime),
            Self::Aborted(PageAbort::TimedOut(_)) => telemetry::record_page_timeout(runtime),
            Self::Aborted(PageAbort::Panicked(_)) => telemetry::record_page_panic(runtime),
        }
    }
}

/// Processes one page (`processing`) within `page_timeout`, in a task of its
/// own, so that a panic fails only this page: tokio catches it at the task
/// boundary and it comes back as [`PageAbort::Panicked`].
///
/// The task also keeps the panic off the main thread, which matters even
/// where the caller runs on it (single-wiki mode): wikimisc's telemetry takes
/// a panic on the main thread for the end of the process. Spawned tasks run
/// on the multi-thread runtime's worker threads, never on the main thread.
async fn run_page<F>(page_timeout: Duration, processing: F) -> PageOutcome
where
    F: Future<Output = Result<WikiPageResult>> + Send + 'static,
{
    let task = tokio::spawn(tokio::time::timeout(page_timeout, processing).in_current_span());
    match task.await {
        Ok(Ok(Ok(result))) => PageOutcome::Done(result),
        Ok(Ok(Err(e))) => PageOutcome::Failed(e),
        Ok(Err(_elapsed)) => PageOutcome::Aborted(PageAbort::TimedOut(page_timeout)),
        Err(e) if e.is_panic() => {
            PageOutcome::Aborted(PageAbort::Panicked(panic_message(&*e.into_panic())))
        }
        // The task is never aborted, so this happens at runtime shutdown at most.
        Err(e) => PageOutcome::Failed(anyhow!("page task ended: {e}")),
    }
}

/// Runs `job` every `period`, forever; the first time one period from now,
/// so a bot that keeps restarting does not keep starting the job.
///
/// Each run is a task of its own, bounded by `period`: a run that fails,
/// panics or hangs is logged, and the schedule carries on. Runs never
/// overlap; one that takes longer than expected delays the next.
async fn run_periodically<F, Fut>(name: &str, period: Duration, job: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        log::info!("Starting {name}");
        match tokio::spawn(tokio::time::timeout(period, job())).await {
            Ok(Ok(Ok(()))) => log::info!("Finished {name}"),
            Ok(Ok(Err(e))) => log::error!("{name} failed: {e}"),
            Ok(Err(_elapsed)) => log::error!("{name} exceeded {}s", period.as_secs()),
            Err(e) if e.is_panic() => {
                log::error!("{name} panicked: {}", panic_message(&*e.into_panic()));
            }
            Err(e) => log::error!("{name} task ended: {e}"),
        }
    }
}

/// The message of a panic, from its payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "(non-string panic payload)".to_string())
}

#[derive(Debug, Clone)]
pub struct MainCommands {
    pub config: Arc<Configuration>,
    pub config_file: String,
}

impl MainCommands {
    /// Processes a single page and updates it with generated list content.
    async fn update_page(&self, page_title: &str, api_url: &str) -> Result<String> {
        let mut mw_api = wikimisc::mediawiki::api::Api::new(api_url).await?;
        mw_api.set_user_agent(crate::LISTERIA_USER_AGENT);
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

        self.config = Arc::new(
            (*self.config)
                .clone()
                .with_max_local_cached_entities(1_000_000),
        );
        let ecw = EntityContainerWrapper::new(self.config.max_concurrent_entry_queries())
            .await?
            .with_circuit_breaker(
                self.config
                    .mw_api_circuit_breaker(crate::configuration::MW_API_ENTITIES_KEY),
            );
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
        let _ = bot.clear_deferred().await;
        let _ = bot.clear_log_table().await;

        let main = self.clone();
        tokio::spawn(run_periodically(
            "update_wikis",
            UPDATE_WIKIS_INTERVAL,
            move || {
                let main = main.clone();
                async move { main.update_wikis().await }
            },
        ));

        let bot = Arc::new(bot);
        static THREADS_SEMAPHORE: Semaphore = Semaphore::const_new(0);
        THREADS_SEMAPHORE.add_permits(max_threads);
        telemetry::set_max_threads(max_threads);
        let seppuku = watchdog(bot.config().page_timeout());
        loop {
            let page = match bot.prepare_next_single_page().await {
                Ok(page) => page,
                Err(e) => {
                    log::warn!("Trying to get next page to process: {e}");
                    tokio::time::sleep(DISPATCH_RETRY_DELAY).await;
                    continue;
                }
            };

            let permit = THREADS_SEMAPHORE.acquire().await?;
            let running = telemetry::RunningBot::start();
            log::info!(
                "Starting new bot, {} running, {} available",
                max_threads - THREADS_SEMAPHORE.available_permits(),
                THREADS_SEMAPHORE.available_permits()
            );
            let bot = bot.clone();
            seppuku.alive();
            let seppuku = seppuku.clone();
            // Per-page span: every log line and trace event emitted inside
            // run_single_bot inherits {wiki, page} fields, making concurrent
            // bot output easy to attribute when many pages run in parallel.
            let span = tracing::info_span!(
                "page_process",
                wiki = %page.wiki(),
                page = %page.title(),
            );
            let page_timeout = bot.config().page_timeout();
            // Snapshot identifiers before moving `page` into run_single_bot,
            // so we can mark the row FAIL on timeout or panic without owning
            // the page.
            let pagestatus_id = page.id();
            let page_title = page.title().to_string();
            let page_wiki = page.wiki().to_string();
            tokio::spawn(
                async move {
                    let start_time = Instant::now();
                    let page_bot = Arc::clone(&bot);
                    let processing = async move { page_bot.run_single_bot(page).await };
                    let outcome = run_page(page_timeout, processing).await;
                    seppuku.alive();
                    let runtime = start_time.elapsed();
                    outcome.record_telemetry(runtime);
                    match outcome {
                        PageOutcome::Done(_) => {}
                        PageOutcome::Failed(e) => log::error!("Bot run failed: {e}"),
                        PageOutcome::Aborted(abort) => {
                            let msg = abort.to_string();
                            log::error!("{msg}: {page_wiki}/{page_title}");
                            if let Err(e) =
                                bot.mark_page_failed(&page_wiki, &page_title, &msg).await
                            {
                                log::error!("Failed to mark aborted page FAIL: {e}");
                            }
                        }
                    }
                    let _ = bot.set_runtime(pagestatus_id, runtime.as_secs()).await;
                    bot.release_running(pagestatus_id).await;
                    drop(running);
                    drop(permit);
                }
                .instrument(span),
            );
        }
    }

    pub async fn run_single_wiki_bot(&self, once: bool) -> Result<()> {
        let state = AppState {
            pages: Arc::new(RwLock::new(HashMap::new())),
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
        let bot = Arc::new(ListeriaBotSingle::new_from_config(config).await?);
        let seppuku = watchdog(bot.config().page_timeout());
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
            // Same span treatment as the multi-wiki bot — keeps log output
            // attributable even though pages are processed sequentially here.
            let span = tracing::info_span!(
                "page_process",
                wiki = %page.wiki(),
                page = %page.title(),
            );
            let page_timeout = bot.config().page_timeout();
            let (page_bot, page_to_run) = (Arc::clone(&bot), page.clone());
            let processing = async move { page_bot.run_single_bot(page_to_run).await };
            let outcome = run_page(page_timeout, processing).instrument(span).await;
            seppuku.alive();
            let runtime = start_time.elapsed();
            outcome.record_telemetry(runtime);
            let mut result = match outcome {
                PageOutcome::Done(result) => result,
                PageOutcome::Failed(e) => {
                    WikiPageResult::new("wiki", page.title(), "Error", e.to_string())
                }
                PageOutcome::Aborted(abort) => {
                    WikiPageResult::new("wiki", page.title(), "Error", abort.to_string())
                }
            };
            result.set_runtime(runtime);
            result.set_completed(Instant::now());
            state
                .pages
                .write()
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_watchdog_window_outlasts_the_longest_page() {
        // A page may run for the whole page_timeout without any other page
        // starting or finishing; the watchdog must not fire before that.
        for secs in [30, 600, 3600] {
            let page_timeout = Duration::from_secs(secs);
            assert!(watchdog_window(page_timeout) > page_timeout, "{secs}");
        }
    }

    const NO_TIMEOUT: Duration = Duration::from_secs(60);

    async fn panicking_page(message: &'static str) -> Result<WikiPageResult> {
        panic!("{message}")
    }

    async fn panicking_page_with_str() -> Result<WikiPageResult> {
        panic!("boom")
    }

    #[tokio::test]
    async fn test_run_page_turns_a_panic_into_an_abort() {
        // A formatted panic carries a String payload, a literal one a &str.
        for outcome in [
            run_page(NO_TIMEOUT, panicking_page("boom")).await,
            run_page(NO_TIMEOUT, panicking_page_with_str()).await,
        ] {
            match outcome {
                PageOutcome::Aborted(abort @ PageAbort::Panicked(_)) => {
                    assert_eq!(abort.to_string(), "Page processing panicked: boom");
                }
                other => panic!("Expected a panic, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_run_page_times_out() {
        let outcome = run_page(Duration::from_millis(10), std::future::pending()).await;
        assert!(matches!(
            outcome,
            PageOutcome::Aborted(PageAbort::TimedOut(_))
        ));
    }

    #[tokio::test]
    async fn test_run_page_passes_results_and_errors_through() {
        let done = run_page(NO_TIMEOUT, async {
            Ok(WikiPageResult::new("wiki", "Page", "OK", String::new()))
        })
        .await;
        assert!(matches!(done, PageOutcome::Done(result) if result.result() == "OK"));
        let failed = run_page(NO_TIMEOUT, async { Err(anyhow!("nope")) }).await;
        assert!(matches!(failed, PageOutcome::Failed(e) if e.to_string() == "nope"));
    }
    /// Starts `run_periodically` with an hourly `job` that records each run
    /// and then does what `outcome` says for that run (0-based).
    fn start_hourly(
        outcome: fn(usize) -> Result<()>,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = runs.clone();
        let schedule = tokio::spawn(run_periodically(
            "test job",
            Duration::from_secs(3600),
            move || {
                let run = counter.fetch_add(1, Ordering::SeqCst);
                async move { outcome(run) }
            },
        ));
        (schedule, runs)
    }

    fn runs_after(runs: &AtomicUsize) -> usize {
        runs.load(Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_periodically_starts_after_one_period() {
        let (schedule, runs) = start_hourly(|_| Ok(()));
        tokio::time::sleep(Duration::from_secs(3599)).await;
        assert_eq!(runs_after(&runs), 0);
        tokio::time::sleep(Duration::from_secs(2 * 3600)).await;
        assert_eq!(runs_after(&runs), 2);
        schedule.abort();
    }

    /// A run that fails or panics must not end the schedule.
    #[tokio::test(start_paused = true)]
    async fn test_run_periodically_survives_failures_and_panics() {
        let (schedule, runs) = start_hourly(|run| match run {
            0 => Err(anyhow!("nope")),
            1 => panic!("boom"),
            _ => Ok(()),
        });
        tokio::time::sleep(Duration::from_secs(3 * 3600 + 1)).await;
        assert_eq!(runs_after(&runs), 3);
        assert!(!schedule.is_finished());
        schedule.abort();
    }

    /// A hung run is given up on after one period, and runs never overlap.
    #[tokio::test(start_paused = true)]
    async fn test_run_periodically_times_out_hung_runs() {
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = runs.clone();
        let schedule = tokio::spawn(run_periodically(
            "test job",
            Duration::from_secs(3600),
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                std::future::pending()
            },
        ));
        // Run 1 starts at 1h and is given up on at 2h; run 2 starts then.
        tokio::time::sleep(Duration::from_secs(3600 + 1)).await;
        assert_eq!(runs_after(&runs), 1);
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert_eq!(runs_after(&runs), 2);
        schedule.abort();
    }

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
