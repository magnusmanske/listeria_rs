//! The tool's website, for the multi-wiki (Wikidata) bot. It serves what the
//! PHP tool's `public_html` did, at the same URLs, as the lists on the wikis
//! link there:
//!
//! - `/`, `/index.php`: the about page; `?action=update&lang=…&page=…` moves
//!   a page to the front of the queue (the "update" link of each list)
//! - `/botstatus.php`: the queue, by group of wikis, wiki, status and page
//! - `/api.php`: the JSON API of the dynamic lists, and their login
//! - `/dynamic.html`: the dynamic lists, a page that runs in the browser
//!
//! It runs as the tool's Toolforge web service (`web` in the `Procfile`),
//! apart from the bot.

mod api;
mod bot_status;
mod bot_status_repository;
mod dynamic_lists;
mod html;
mod index;
mod oauth;
mod params;
mod session;

use crate::configuration::Configuration;
use crate::database_pool::DatabasePool;
use crate::pagestatus_repository::PageStatusRepository;
use crate::wiki_repository::WikiRepository;
use anyhow::{Result, anyhow};
use axum::Router;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use bot_status_repository::BotStatusRepository;
use dynamic_lists::DynamicListRepository;
use oauth::{OAuthClient, OAuthConsumer};
use serde::Deserialize;
use session::SessionKey;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use wikimisc::escape;
use wikimisc::server::{self, health};
use wikimisc::site_matrix::SiteMatrix;

/// The port without a `PORT` environment variable; Toolforge web services
/// get `PORT=8000`.
const DEFAULT_PORT: u16 = 8000;
/// Connections per database pool without `webserver.max_connections`. The
/// tool's database account has few to spare next to the bot's.
const DEFAULT_MAX_CONNECTIONS: usize = 2;
/// Requests taking longer are answered with a 504. Updating a dynamic list
/// runs its SPARQL query, which WDQS allows a minute.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// The `webserver` block of the config.
#[derive(Debug, Clone, Deserialize)]
pub struct WebserverConfig {
    /// The database of the dynamic lists, on the `mysql` server. Without it,
    /// `api.php` reports the dynamic lists as unavailable.
    #[serde(default)]
    dynamic_lists_schema: Option<String>,
    /// Connections per database pool (the bot's, and the dynamic lists').
    #[serde(default = "default_max_connections")]
    max_connections: usize,
    /// The OAuth 1.0a consumer users log in with on `dynamic.html`. Without
    /// it, nobody can log in.
    #[serde(default)]
    oauth: Option<OAuthConsumer>,
}

const fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

/// What the request handlers share.
#[derive(Debug, Clone)]
struct WebState {
    config: Arc<Configuration>,
    site_matrix: SiteMatrix,
    wikis: WikiRepository,
    queue: PageStatusRepository,
    bot_status: BotStatusRepository,
    dynamic_lists: Option<DynamicListRepository>,
    oauth: Option<Arc<OAuthClient>>,
    session: Option<SessionKey>,
}

impl WebState {
    async fn new(config: Arc<Configuration>) -> Result<Self> {
        let settings = config
            .webserver()
            .ok_or_else(|| anyhow!("No webserver block in config"))?;
        let bot_schema = config
            .mysql("schema")
            .as_str()
            .ok_or_else(|| anyhow!("No schema in config"))?
            .to_string();
        let pool = DatabasePool::for_schema(&config, &bot_schema, settings.max_connections)?;
        let dynamic_lists = match &settings.dynamic_lists_schema {
            Some(schema) => Some(DynamicListRepository::new(DatabasePool::for_schema(
                &config,
                schema,
                settings.max_connections,
            )?)),
            None => None,
        };
        let oauth = match &settings.oauth {
            Some(consumer) => Some(Arc::new(OAuthClient::new(consumer.clone())?)),
            None => None,
        };
        let session = settings
            .oauth
            .as_ref()
            .map(|consumer| SessionKey::derive(consumer.secret()));
        Ok(Self {
            site_matrix: SiteMatrix::new(config.get_default_wbapi()?).await?,
            wikis: WikiRepository::new(pool.clone()),
            queue: PageStatusRepository::new(pool.clone()),
            bot_status: BotStatusRepository::new(pool),
            dynamic_lists,
            oauth,
            session,
            config,
        })
    }

    /// The server URL of a wiki (`https://de.wikipedia.org` for `dewiki`).
    fn server_for_wiki(&self, wiki: &str) -> Option<String> {
        self.site_matrix.get_server_url_for_wiki(wiki).ok()
    }
}

/// Serves the website on `PORT` (default 8000) until SIGTERM or SIGINT.
pub async fn run(config: Arc<Configuration>) -> Result<()> {
    let state = WebState::new(config).await?;
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port_from_env());
    let listener = TcpListener::bind(address).await?;
    log::info!("Website listening on {address}");
    server::serve(listener, router(state)).await?;
    Ok(())
}

fn port_from_env() -> u16 {
    std::env::var("PORT")
        .ok()
        .and_then(|port| port.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT)
}

fn router(state: WebState) -> Router {
    let router = Router::new()
        .route("/", get(index::index))
        .route("/index.php", get(index::index))
        .route("/botstatus.php", get(bot_status::bot_status))
        .route("/api.php", get(api::api).post(api::api))
        .route("/dynamic.html", get(dynamic_lists_page))
        .route("/healthz", get(health::ok))
        .with_state(state);
    let router = server::timeout(router, REQUEST_TIMEOUT, StatusCode::GATEWAY_TIMEOUT);
    let router = server::standard_layers(router);
    server::request_metrics(router, &["/healthz"])
}

async fn dynamic_lists_page() -> Html<&'static str> {
    Html(include_str!("../html/dynamic.html"))
}

/// The error of an HTML page handler: logged, and shown on a page with
/// status 500.
#[derive(Debug)]
struct WebError(anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for WebError {
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let message = format!("{:#}", self.0);
        log::error!("Website: {message}");
        let body = format!(
            "<div class='alert alert-danger'>{}</div>",
            escape::html(&message)
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            html::page("Listeria", "/", &body),
        )
            .into_response()
    }
}

type PageResult = std::result::Result<Html<String>, WebError>;
