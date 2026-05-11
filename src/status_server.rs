//! Status server for displaying bot statistics and health information.

use crate::wiki_page_result::WikiPageResult;
use anyhow::Result;
use axum::{Router, extract::State, response::Html, routing::get};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;

/// Escapes `&`, `<`, `>`, `"`, and `'` so text is safe to embed in HTML.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Shared state for the HTTP status server.
///
/// `pages` is behind an `RwLock` so the read-heavy status endpoint can hold
/// a read guard without blocking concurrent bot-task writes. A single lock
/// acquisition per request (rather than four separate ones) also guarantees a
/// consistent snapshot.
#[derive(Debug, Clone)]
pub struct AppState {
    pub pages: Arc<RwLock<HashMap<String, WikiPageResult>>>,
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

#[derive(Debug, Clone, Copy)]
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
                            pattern.replace(
                                "$1",
                                &utf8_percent_encode(&page.replace(' ', "_"), NON_ALPHANUMERIC)
                                    .to_string()
                            ),
                            escape_html(page)
                        )
                    }
                    None => escape_html(page),
                };
                html += &format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                    link,
                    escape_html(result.result()),
                    escape_html(result.message())
                );
            }
            html += "</tbody></table></p></div></div>";
        }
        html
    }

    async fn status_server_root(State(state): State<AppState>) -> Html<String> {
        let now = Instant::now();
        let mut statistics = ServerStatistics::from_state(&state, now);

        // Take a single consistent snapshot; all metrics below derive from it.
        let snapshot = state.pages.read().await;

        for result in snapshot.values() {
            *statistics
                .status_counts
                .entry(result.result().to_string())
                .or_insert(0) += 1;
        }

        statistics.last_event = snapshot
            .values()
            .filter_map(|result| result.completed())
            .map(|l| now.duration_since(l))
            .min();

        statistics.total_pages = snapshot.len();

        let problems: Vec<_> = snapshot
            .iter()
            .filter(|(_page, result)| result.result() != "OK")
            .map(|(page, result)| (page.clone(), result.clone()))
            .collect();

        drop(snapshot);

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
        log::info!("listening on http://{}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── escape_html ────────────────────────────────────────────────────────

    #[test]
    fn test_escape_html_no_special_chars() {
        assert_eq!(escape_html("hello world"), "hello world");
    }

    #[test]
    fn test_escape_html_empty() {
        assert_eq!(escape_html(""), "");
    }

    #[test]
    fn test_escape_html_ampersand() {
        assert_eq!(escape_html("foo & bar"), "foo &amp; bar");
    }

    #[test]
    fn test_escape_html_less_than_greater_than() {
        assert_eq!(escape_html("<script>"), "&lt;script&gt;");
    }

    #[test]
    fn test_escape_html_double_quote() {
        assert_eq!(escape_html(r#"say "hi""#), "say &quot;hi&quot;");
    }

    #[test]
    fn test_escape_html_apostrophe() {
        assert_eq!(escape_html("it's"), "it&#39;s");
    }

    #[test]
    fn test_escape_html_all_special_chars() {
        assert_eq!(escape_html("<>&\"'"), "&lt;&gt;&amp;&quot;&#39;");
    }

    #[test]
    fn test_escape_html_xss_attempt() {
        let input = r#"<img src=x onerror="alert('xss')">"#;
        let escaped = escape_html(input);
        assert!(!escaped.contains('<'));
        assert!(!escaped.contains('>'));
        assert!(!escaped.contains('"'));
        assert!(!escaped.contains('\''));
    }

    // ── build_html_header ──────────────────────────────────────────────────

    #[test]
    fn test_build_html_header_contains_required_structure() {
        let html = StatusServer::build_html_header();
        assert!(html.contains("<html>"));
        assert!(html.contains("<head>"));
        assert!(html.contains("Listeria"));
        assert!(html.contains("<body>"));
        assert!(html.contains("bootstrap.min.css"));
    }

    // ── build_status_card ──────────────────────────────────────────────────

    #[test]
    fn test_build_status_card_shows_uptime_values() {
        let stats = ServerStatistics {
            uptime_days: 2,
            uptime_hours: 3,
            uptime_minutes: 15,
            uptime_seconds: 42,
            last_event: None,
            total_pages: 100,
            status_counts: HashMap::new(),
        };
        let html = StatusServer::build_status_card(&stats);
        assert!(html.contains("2 days"), "missing days");
        assert!(html.contains("3 hours"), "missing hours");
        assert!(html.contains("15 minutes"), "missing minutes");
        assert!(html.contains("42 seconds"), "missing seconds");
        assert!(html.contains("100"), "missing total pages");
    }

    #[test]
    fn test_build_status_card_shows_last_event_when_present() {
        let stats = ServerStatistics {
            uptime_days: 0,
            uptime_hours: 0,
            uptime_minutes: 0,
            uptime_seconds: 0,
            last_event: Some(Duration::from_secs(30)),
            total_pages: 0,
            status_counts: HashMap::new(),
        };
        let html = StatusServer::build_status_card(&stats);
        assert!(html.contains("30 seconds ago"));
    }

    #[test]
    fn test_build_status_card_no_last_event_when_none() {
        let stats = ServerStatistics {
            uptime_days: 0,
            uptime_hours: 0,
            uptime_minutes: 0,
            uptime_seconds: 0,
            last_event: None,
            total_pages: 0,
            status_counts: HashMap::new(),
        };
        let html = StatusServer::build_status_card(&stats);
        assert!(!html.contains("seconds ago"));
    }

    // ── build_statistics_table ─────────────────────────────────────────────

    #[test]
    fn test_build_statistics_table_empty() {
        let html = StatusServer::build_statistics_table(&HashMap::new());
        assert!(html.contains("Page statistics"));
        assert!(html.contains("<table"));
    }

    #[test]
    fn test_build_statistics_table_with_entries() {
        let mut stats = HashMap::new();
        stats.insert("OK".to_string(), 42_u64);
        stats.insert("FAIL".to_string(), 3_u64);
        let html = StatusServer::build_statistics_table(&stats);
        assert!(html.contains("OK"));
        assert!(html.contains("42"));
        assert!(html.contains("FAIL"));
        assert!(html.contains("3"));
    }

    // ── build_problems_table ───────────────────────────────────────────────

    #[test]
    fn test_build_problems_table_empty_returns_empty_string() {
        let html = StatusServer::build_problems_table(&[], &None);
        assert!(html.is_empty());
    }

    #[test]
    fn test_build_problems_table_shows_result_and_message() {
        let problems = vec![(
            "My page".to_string(),
            WikiPageResult::new("enwiki", "My page", "FAIL", "Something broke".to_string()),
        )];
        let html = StatusServer::build_problems_table(&problems, &None);
        assert!(html.contains("My page"));
        assert!(html.contains("FAIL"));
        assert!(html.contains("Something broke"));
    }

    #[test]
    fn test_build_problems_table_with_pattern_creates_link() {
        let problems = vec![(
            "My page".to_string(),
            WikiPageResult::new("enwiki", "My page", "FAIL", "error".to_string()),
        )];
        let html = StatusServer::build_problems_table(
            &problems,
            &Some("https://en.wikipedia.org/wiki/$1".to_string()),
        );
        assert!(html.contains("<a "), "expected hyperlink");
        assert!(html.contains("href="), "expected href attribute");
    }

    #[test]
    fn test_build_problems_table_escapes_html_in_page_name() {
        let problems = vec![(
            "<b>bold & special</b>".to_string(),
            WikiPageResult::new("enwiki", "page", "FAIL", "error".to_string()),
        )];
        let html = StatusServer::build_problems_table(&problems, &None);
        assert!(
            !html.contains("<b>bold"),
            "raw HTML must not appear unescaped"
        );
        assert!(html.contains("&lt;b&gt;"));
        assert!(html.contains("&amp;"));
    }

    #[test]
    fn test_build_problems_table_escapes_html_in_result_and_message() {
        // Error messages bubble up from MediaWiki / anyhow strings and can
        // contain arbitrary text. They must not be injected into the page raw.
        let problems = vec![(
            "page".to_string(),
            WikiPageResult::new(
                "enwiki",
                "page",
                "FAIL<script>",
                "<img src=x onerror=\"alert('xss')\">".to_string(),
            ),
        )];
        let html = StatusServer::build_problems_table(&problems, &None);
        assert!(
            !html.contains("FAIL<script>"),
            "raw <script> tag must not appear in the result cell"
        );
        assert!(
            !html.contains("<img src=x"),
            "raw <img must not appear in the message cell"
        );
        assert!(
            !html.contains("onerror=\"alert"),
            "raw onerror handler must not appear unescaped"
        );
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("&lt;img"));
    }

    // ── ServerStatistics::from_state ──────────────────────────────────────

    #[test]
    fn test_server_statistics_from_state_zero_uptime() {
        let started = Instant::now();
        let app_state = AppState {
            pages: Arc::new(RwLock::new(HashMap::new())),
            started,
            wiki_page_pattern: None,
        };
        let stats = ServerStatistics::from_state(&app_state, started);
        assert_eq!(stats.uptime_days, 0);
        assert_eq!(stats.uptime_hours, 0);
        assert_eq!(stats.uptime_minutes, 0);
        assert_eq!(stats.uptime_seconds, 0);
        assert_eq!(stats.total_pages, 0);
        assert!(stats.last_event.is_none());
        assert!(stats.status_counts.is_empty());
    }

    // ── Axum route handler ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_status_server_root_returns_ok_html() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = AppState {
            pages: Arc::new(RwLock::new(HashMap::new())),
            started: Instant::now(),
            wiki_page_pattern: None,
        };

        let app = Router::new()
            .route("/", get(StatusServer::status_server_root))
            .with_state(state);

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body.contains("Listeria"));
        assert!(body.contains("<html>"));
    }

    #[tokio::test]
    async fn test_status_server_root_with_failed_pages() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let mut pages = HashMap::new();
        pages.insert(
            "enwiki:Test".to_string(),
            WikiPageResult::new("enwiki", "Test", "FAIL", "timeout".to_string()),
        );

        let state = AppState {
            pages: Arc::new(RwLock::new(pages)),
            started: Instant::now(),
            wiki_page_pattern: None,
        };

        let app = Router::new()
            .route("/", get(StatusServer::status_server_root))
            .with_state(state);

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body.contains("FAIL"));
        assert!(body.contains("timeout"));
    }
}
