use crate::wiki_page_result::WikiPageResult;
use crate::{
    configuration::Configuration, entity_container_wrapper::EntityContainerWrapper,
    listeria_bot::ListeriaBot, listeria_bot_single::ListeriaBotSingle,
    listeria_bot_wikidata::ListeriaBotWikidata, listeria_page::ListeriaPage, wiki_apis::WikiApis,
};
use anyhow::{Result, anyhow};
use axum::extract::State;
use axum::{Router, response::Html, routing::get};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use std::{fs::read_to_string, net::SocketAddr};
use tokio::sync::{Mutex, Semaphore};
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;
use wikimisc::{seppuku::Seppuku, wikibase::EntityTrait};

const MAX_INACTIVITY_BEFORE_SEPPUKU_SEC: u64 = 300;

#[derive(Debug, Clone)]
struct AppState {
    pages: Arc<Mutex<HashMap<String, WikiPageResult>>>,
    started: Instant,
    wiki_page_pattern: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MainCommands {
    pub config: Arc<Configuration>,
    pub config_file: String,
}

impl MainCommands {
    async fn update_page(&self, page_title: &str, api_url: &str) -> Result<String> {
        let mut mw_api = wikimisc::mediawiki::api::Api::new(api_url).await?;
        mw_api.set_oauth2(self.config.oauth2_token());

        let mw_api = Arc::new(mw_api);
        let mut page = ListeriaPage::new(self.config.clone(), mw_api, page_title.into()).await?;
        page.run().await.map_err(|e| anyhow!("{e:?}"))?;

        if false {
            // FOR TESTING
            println!("{}", page.as_wikitext().await?[0]);
            Ok("OK".to_string())
        } else {
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
    }

    pub async fn update_wikis(&self) -> Result<()> {
        let wiki_list = WikiApis::new(self.config.clone()).await?;
        wiki_list.update_wiki_list_in_database().await?;
        wiki_list.update_all_wikis().await?;
        Ok(())
    }

    pub async fn load_test_entities(&mut self) -> Result<()> {
        let mut items = vec![];
        for line in read_to_string("test_data/entities.tab")?.lines() {
            items.push(line.to_string());
        }
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
        println!("{message}");
        Ok(())
    }

    pub async fn run_wikidata_bot(&self) -> Result<()> {
        let config = Arc::new((*self.config).clone());
        let bot = ListeriaBotWikidata::new_from_config(config).await?;
        let max_threads = bot.config().max_threads();
        println!("Starting {max_threads} bots");
        let _ = bot.reset_running().await;
        let _ = bot.clear_deleted().await;
        let bot = Arc::new(bot);
        static THREADS_SEMAPHORE: Semaphore = Semaphore::const_new(0);
        THREADS_SEMAPHORE.add_permits(max_threads);
        let seppuku = Seppuku::new(MAX_INACTIVITY_BEFORE_SEPPUKU_SEC);
        seppuku.arm();
        loop {
            let page = match bot.prepare_next_single_page().await {
                Ok(page) => page,
                Err(e) => {
                    eprintln!("Trying to get next page to process: {e}");
                    continue;
                }
            };

            let permit = THREADS_SEMAPHORE.acquire().await?;
            println!(
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
                    eprintln!("Bot run failed: {e}");
                }
                let end_time = Instant::now();
                let diff = (end_time - start_time).as_secs();
                let _ = bot.set_runtime(pagestatus_id, diff).await;
                bot.release_running(pagestatus_id).await;
                drop(permit);
            });
        }
    }

    async fn status_server_root(State(state): State<AppState>) -> Html<String> {
        let now = Instant::now();
        let server_uptime = now.duration_since(state.started);
        let uptime_days = server_uptime.as_secs() / 86400;
        let uptime_hours = (server_uptime.as_secs() % 86400) / 3600;
        let uptime_minutes = (server_uptime.as_secs() % 3600) / 60;
        let uptime_seconds = server_uptime.as_secs() % 60;

        let mut statistics: HashMap<String, u64> = HashMap::new();
        for (_page, result) in state.pages.lock().await.iter() {
            *statistics.entry(result.result().to_string()).or_insert(0) += 1;
        }

        let last_event = state
            .pages
            .lock()
            .await
            .iter()
            .filter_map(|(_page, result)| result.completed())
            .map(|l| now.duration_since(l))
            .min();

        let problems: Vec<_> = state
            .pages
            .lock()
            .await
            .iter()
            // .filter(|(_page, result)| result.result() != "OK")
            .map(|(page, result)| (page.clone(), result.clone()))
            .collect();

        let mut html: String = "<html><head>".to_string();
        html += r#"<meta charset="UTF-8">
        <link href="html/bootstrap.min.css" rel="stylesheet">
        <script src="html/bootstrap.bundle.min.js" ></script>"#;
        html += "</head><body>";
        html += r#"<div class="card"><div class="card-body"><h5 class="card-title">Listeria status</h5>"#;
        html += &format!(
            "<p class='card-text'>Uptime: {} days, {} hours, {} minutes, {} seconds</p>",
            uptime_days, uptime_hours, uptime_minutes, uptime_seconds
        );
        if let Some(event) = last_event {
            html += &format!(
                "<p class='card-text'>Last page check: {} seconds ago</p>",
                event.as_secs()
            );
        }
        html += &format!(
            "<p class='card-text'>Total pages: {}</p>",
            state.pages.lock().await.len()
        );
        html += "</div></div>";

        html += r#"<div class="card"><div class="card-body"><h5 class="card-title">Page statistics</h5>"#;
        html += "<p class='card-text'><table class='table table-striped'>";
        html += "<thead><tr><th>Status</th><th>Count</th></tr></thead><tbody>";
        for (status, count) in statistics.iter() {
            html += &format!("<tr><td>{status}</td><td>{count}</td></tr>");
        }
        html += "</tbody></table></p></div></div>";

        if !problems.is_empty() {
            html +=
                r#"<div class="card"><div class="card-body"><h5 class="card-title">Issues</h5>"#;
            html += "<p class='card-text'><table class='table table-striped'>";
            html += "<thead><tr><th>Page</th><th>Status</th><th>Message</th></tr></thead><tbody>";
            for (page, result) in problems {
                let link = match &state.wiki_page_pattern {
                    Some(wiki_page_pattern) => {
                        format!(
                            "<a target=\"_blank\" href=\"{}\">{}</a>",
                            wiki_page_pattern
                                .replace("$1", &urlencoding::encode(&page.replace(' ', "_"))),
                            html_escape::encode_text(&page)
                        )
                    }
                    None => html_escape::encode_text(&page).to_string(),
                };
                html += &format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                    link,
                    result.result(),
                    result.message()
                );
            }
            html += "</tbody></table></p></div></div>";
        }

        html += "</body></html>";
        Html(html)
    }

    async fn run_status_server(port: u16, state: AppState) {
        // tracing_subscriber::fmt::init();

        // let cors = CorsLayer::new().allow_origin(Any);
        let app = Router::new()
            .route("/", get(Self::status_server_root))
            .nest_service("/html", ServeDir::new("html"))
            // .layer(cors),
            // .layer(TraceLayer::new_for_http())
            .layer(CompressionLayer::new())
            .with_state(state);

        let address = [0, 0, 0, 0]; // TODOO env::var("AC2WD_ADDRESS")

        let addr = SocketAddr::from((address, port));
        tracing::debug!("listening on {}", addr);
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("Could not create listener");
        axum::serve(listener, app)
            .await
            .expect("Could not start server");
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
                Self::run_status_server(port, state_clone).await;
            });
        }
        let config = Arc::new((*self.config).clone());
        let bot = ListeriaBotSingle::new_from_config(config).await?;
        let mut seppuku = Seppuku::new(MAX_INACTIVITY_BEFORE_SEPPUKU_SEC);
        seppuku.arm();
        loop {
            let page = match bot.prepare_next_single_page().await {
                Ok(page) => page,
                Err(_error) => {
                    if once {
                        if !bot.config().quiet() {
                            println!("All pages processed");
                        }
                        return Ok(());
                    }
                    if !bot.config().quiet() {
                        println!("All pages processed, restarting from beginning");
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
