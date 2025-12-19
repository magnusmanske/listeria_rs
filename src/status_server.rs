//! Status server for displaying bot statistics and health information.

use crate::wiki_page_result::WikiPageResult;
use anyhow::Result;
use axum::{Router, extract::State, response::Html, routing::get};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;

#[derive(Debug, Clone)]
pub struct AppState {
    pub pages: Arc<Mutex<HashMap<String, WikiPageResult>>>,
    pub started: Instant,
    pub wiki_page_pattern: Option<String>,
}

#[derive(Debug)]
struct ServerStatistics {
    uptime_days: u64,
    uptime_hours: u64,
    uptime_minutes: u64,
    uptime_seconds: u64,
    last_event: Option<Duration>,
    total_pages: usize,
    status_counts: HashMap<String, u64>,
}

impl ServerStatistics {
    fn from_state(state: &AppState, now: Instant) -> Self {
        let server_uptime = now.duration_since(state.started);
        Self {
            uptime_days: server_uptime.as_secs() / 86400,
            uptime_hours: (server_uptime.as_secs() % 86400) / 3600,
            uptime_minutes: (server_uptime.as_secs() % 3600) / 60,
            uptime_seconds: server_uptime.as_secs() % 60,
            last_event: None,
            total_pages: 0,
            status_counts: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct StatusServer;

impl StatusServer {
    fn build_html_header() -> String {
        let mut html = "<html><head>".to_string();
        html += r#"<meta charset="UTF-8">
        <title>Listeria</title>
        <link href="html/bootstrap.min.css" rel="stylesheet">"#;
        html += "</head><body>";
        html
    }

    fn build_status_card(stats: &ServerStatistics) -> String {
        let mut html = String::new();
        html += r#"<div class="card"><div class="card-body"><h5 class="card-title">Listeria status</h5>"#;
        html += &format!(
            "<p class='card-text'>Uptime: {} days, {} hours, {} minutes, {} seconds</p>",
            stats.uptime_days, stats.uptime_hours, stats.uptime_minutes, stats.uptime_seconds
        );
        if let Some(event) = stats.last_event {
            html += &format!(
                "<p class='card-text'>Last page check: {} seconds ago</p>",
                event.as_secs()
            );
        }
        html += &format!(
            "<p class='card-text'>Total pages: {}</p>",
            stats.total_pages
        );
        html += "</div></div>";
        html
    }

    fn build_statistics_table(statistics: &HashMap<String, u64>) -> String {
        let mut html = String::new();
        html += r#"<div class="card"><div class="card-body"><h5 class="card-title">Page statistics</h5>"#;
        html += "<p class='card-text'><table class='table table-striped'>";
        html += "<thead><tr><th>Status</th><th>Count</th></tr></thead><tbody>";
        for (status, count) in statistics.iter() {
            html += &format!("<tr><td>{status}</td><td>{count}</td></tr>");
        }
        html += "</tbody></table></p></div></div>";
        html
    }

    fn build_problems_table(
        problems: &[(String, WikiPageResult)],
        wiki_page_pattern: &Option<String>,
    ) -> String {
        let mut html = String::new();
        if !problems.is_empty() {
            html +=
                r#"<div class="card"><div class="card-body"><h5 class="card-title">Issues</h5>"#;
            html += "<p class='card-text'><table class='table table-striped'>";
            html += "<thead><tr><th>Page</th><th>Status</th><th>Message</th></tr></thead><tbody>";
            for (page, result) in problems {
                let link = match wiki_page_pattern {
                    Some(pattern) => {
                        format!(
                            "<a target=\"_blank\" href=\"{}\">{}</a>",
                            pattern.replace("$1", &urlencoding::encode(&page.replace(' ', "_"))),
                            html_escape::encode_text(page)
                        )
                    }
                    None => html_escape::encode_text(page).to_string(),
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
        html
    }

    async fn status_server_root(State(state): State<AppState>) -> Html<String> {
        let now = Instant::now();
        let mut statistics = ServerStatistics::from_state(&state, now);

        for (_page, result) in state.pages.lock().await.iter() {
            *statistics
                .status_counts
                .entry(result.result().to_string())
                .or_insert(0) += 1;
        }

        statistics.last_event = state
            .pages
            .lock()
            .await
            .iter()
            .filter_map(|(_page, result)| result.completed())
            .map(|l| now.duration_since(l))
            .min();

        statistics.total_pages = state.pages.lock().await.len();

        let problems: Vec<_> = state
            .pages
            .lock()
            .await
            .iter()
            .filter(|(_page, result)| result.result() != "OK")
            .map(|(page, result)| (page.clone(), result.clone()))
            .collect();

        let mut html = Self::build_html_header();
        html += &Self::build_status_card(&statistics);
        html += &Self::build_statistics_table(&statistics.status_counts);
        html += &Self::build_problems_table(&problems, &state.wiki_page_pattern);
        html += "</body></html>";
        Html(html)
    }

    pub async fn run(port: u16, state: AppState) -> Result<()> {
        let app = Router::new()
            .route("/", get(Self::status_server_root))
            .nest_service("/html", ServeDir::new("html"))
            .layer(CompressionLayer::new())
            .with_state(state);

        let address = [0, 0, 0, 0];
        let addr = SocketAddr::from((address, port));
        println!("listening on http://{}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}
