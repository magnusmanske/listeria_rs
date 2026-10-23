//! `api.php`: the JSON API of the dynamic lists (`query=…`), and their login
//! (`action=…`, and MediaWiki's OAuth callback), as `dynamic.html` uses them.
//!
//! Unlike the PHP tool's, it saves a list only for the user who is logged
//! in, and only lists they own: `dynamic.html` sends an owner, but it is not
//! trusted.

use super::dynamic_lists::{DynamicListRepository, ListInput};
use super::params::Params;
use super::session::{self, SessionKey};
use super::{WebState, html};
use crate::configuration::Configuration;
use anyhow::{Result, anyhow};
use axum::Json;
use axum::extract::State;
use axum::http::header::SET_COOKIE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{AppendHeaders, IntoResponse, Redirect, Response};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{Map, Value};
use std::time::Duration;
use wikimisc::entity_id::EntityId;
use wikimisc::escape;
use wikimisc::sparql_client::SparqlClient;
use wikimisc::sparql_endpoint::{SparqlEndpoint, WDQS_URL};
use wikimisc::sparql_results::SparqlApiResult;
use wikimisc::title::percent_encode;

/// Where users go after logging in or out.
const DYNAMIC_LISTS_PAGE: &str = "/dynamic.html";
/// The cookie with the request token of a login in progress, so the login
/// can only be completed in the browser that started it.
const LOGIN_COOKIE: &str = "listeria_login";
/// How long a user has to allow the login.
const LOGIN_COOKIE_LIFETIME: Duration = Duration::from_secs(60 * 60);
const DEFAULT_LIMIT: u64 = 10;
const MAX_LIMIT: u64 = 500;

pub(super) async fn api(
    State(state): State<WebState>,
    headers: HeaderMap,
    params: Params,
) -> Response {
    match params.get_or("query", "") {
        "" => account(&state, &headers, &params).await,
        query => {
            let user = state.session.as_ref().and_then(|key| key.user(&headers));
            Json(lists_response(&state, query, &params, user.as_deref()).await).into_response()
        }
    }
}

/// `{"status": "OK", "ts": …}` with the answer to `query`, or with the error
/// as the status.
async fn lists_response(
    state: &WebState,
    query: &str,
    params: &Params,
    user: Option<&str>,
) -> Value {
    let now = Utc::now();
    let mut ret = Map::new();
    ret.insert("status".into(), "OK".into());
    ret.insert(
        "ts".into(),
        now.to_rfc3339_opts(SecondsFormat::Secs, false).into(),
    );
    match lists_query(state, query, params, user, now).await {
        Ok(answer) => ret.extend(answer),
        Err(e) => {
            log::warn!("api.php?query={query}: {e:#}");
            ret.insert("status".into(), format!("{e:#}").into());
        }
    }
    Value::Object(ret)
}

async fn lists_query(
    state: &WebState,
    query: &str,
    params: &Params,
    user: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Map<String, Value>> {
    let lists = state
        .dynamic_lists
        .as_ref()
        .ok_or_else(|| anyhow!("Dynamic lists are not available"))?;
    let limit = params.number("limit", DEFAULT_LIMIT).min(MAX_LIMIT);
    let list_id = params.number("list", 0);
    match query {
        "get_list" => match lists.get(list_id).await? {
            Some(list) => data(list),
            None => Ok(Map::new()),
        },
        "update_list" => {
            update_items(&state.config, lists, list_id, now).await?;
            Ok(Map::new())
        }
        "get_last_lists" => data(lists.latest(limit).await?),
        "search" => data(lists.search(params.get_or("text", ""), limit).await?),
        "get_user_lists" => data(lists.of_owner(params.get_or("user", "")).await?),
        "set_list" => {
            let user = user.ok_or_else(|| anyhow!("Not logged in"))?;
            let input: ListInput = serde_json::from_str(params.get_or("list", "{}"))?;
            let id = save_list(&state.config, lists, &input, user, now).await?;
            Ok(Map::from_iter([("id".to_string(), id.into())]))
        }
        _ => Ok(Map::new()),
    }
}

fn data(data: impl Serialize) -> Result<Map<String, Value>> {
    Ok(Map::from_iter([(
        "data".to_string(),
        serde_json::to_value(data)?,
    )]))
}

/// Saves a list of `user`, new or their own; returns its ID.
async fn save_list(
    config: &Configuration,
    lists: &DynamicListRepository,
    input: &ListInput,
    user: &str,
    now: DateTime<Utc>,
) -> Result<u64> {
    if let Some(id) = input.id() {
        let list = lists
            .get(id)
            .await?
            .ok_or_else(|| anyhow!("Wrong list ID: {id}"))?;
        if list.owner() != user {
            return Err(anyhow!("List {id} belongs to {}", list.owner()));
        }
        lists.update(id, input, now).await?;
        return Ok(id);
    }
    let id = lists.create(input, user, now).await?;
    // The list is saved even if its query fails; its items can be updated later
    if let Err(e) = update_items(config, lists, id, now).await {
        log::warn!("Initial update of dynamic list {id}: {e:#}");
    }
    Ok(id)
}

/// Sets the items of list `id` to those its query returns now.
async fn update_items(
    config: &Configuration,
    lists: &DynamicListRepository,
    id: u64,
    now: DateTime<Utc>,
) -> Result<()> {
    let list = lists
        .get(id)
        .await?
        .ok_or_else(|| anyhow!("Wrong list ID: {id}"))?;
    let items = match list.sparql().trim() {
        "" => vec![],
        sparql => query_items(config, sparql).await?,
    };
    lists.set_items(id, &items, now).await
}

/// The items of a query's first variable, by number. Runs on WDQS, as
/// `dynamic.html` runs the queries itself.
async fn query_items(config: &Configuration, sparql: &str) -> Result<Vec<u64>> {
    let client =
        SparqlClient::with_client(SparqlEndpoint::wdqs(), config.sparql_http_client(WDQS_URL)?)
            .with_max_response_bytes(config.max_sparql_response_bytes());
    Ok(items_of_first_variable(&client.query(sparql).await?))
}

fn items_of_first_variable(result: &SparqlApiResult) -> Vec<u64> {
    let Some(variable) = result.head().get("vars").and_then(|vars| vars.first()) else {
        return vec![];
    };
    result
        .bindings()
        .iter()
        .filter_map(|row| match row.get(variable)?.entity_id()? {
            EntityId::Item(item) => Some(item.get()),
            _ => None,
        })
        .collect()
}

/// The login: `action=authorize` starts it, MediaWiki's callback completes
/// it; `action=get_rights` tells `dynamic.html` who is logged in.
async fn account(state: &WebState, headers: &HeaderMap, params: &Params) -> Response {
    if let (Some(token), Some(verifier)) = (params.get("oauth_token"), params.get("oauth_verifier"))
    {
        return finish_login(state, headers, token, verifier).await;
    }
    match params.get_or("action", "") {
        "authorize" => start_login(state).await,
        "get_rights" => {
            let user = state.session.as_ref().and_then(|key| key.user(headers));
            Json(rights(user.as_deref())).into_response()
        }
        "logout" => (
            AppendHeaders([(SET_COOKIE, session::logout_cookie())]),
            Redirect::to(DYNAMIC_LISTS_PAGE),
        )
            .into_response(),
        _ => Json(serde_json::json!({
            "status": "OK",
            "ts": Utc::now().to_rfc3339_opts(SecondsFormat::Secs, false),
        }))
        .into_response(),
    }
}

/// What the PHP tool's Widar answered to `get_rights`, as far as
/// `dynamic.html` reads it.
fn rights(user: Option<&str>) -> Value {
    let result = match user {
        Some(name) => serde_json::json!({"query": {"userinfo": {"name": name}}}),
        None => serde_json::json!({"error": {
            "code": "mwoauth-invalid-authorization",
            "info": "Not logged in",
        }}),
    };
    serde_json::json!({"error": "OK", "status": "OK", "data": [], "result": result})
}

async fn start_login(state: &WebState) -> Response {
    let Some(oauth) = &state.oauth else {
        return login_failed(StatusCode::NOT_FOUND, "Login is not available.");
    };
    match oauth.start_login().await {
        Ok((token, url)) => {
            let cookie =
                session::set_cookie(LOGIN_COOKIE, &percent_encode(&token), LOGIN_COOKIE_LIFETIME);
            (AppendHeaders([(SET_COOKIE, cookie)]), Redirect::to(&url)).into_response()
        }
        Err(e) => login_failed(StatusCode::BAD_GATEWAY, &format!("{e:#}")),
    }
}

async fn finish_login(
    state: &WebState,
    headers: &HeaderMap,
    token: &str,
    verifier: &str,
) -> Response {
    let (Some(oauth), Some(key)) = (&state.oauth, &state.session) else {
        return login_failed(StatusCode::NOT_FOUND, "Login is not available.");
    };
    if session::cookie(headers, LOGIN_COOKIE) != Some(percent_encode(token).as_str()) {
        return login_failed(
            StatusCode::BAD_REQUEST,
            "This login was not started in this browser, or has expired.",
        );
    }
    match oauth.finish_login(token, verifier).await {
        Ok(username) => logged_in(key, &username),
        Err(e) => login_failed(StatusCode::BAD_GATEWAY, &format!("{e:#}")),
    }
}

fn logged_in(key: &SessionKey, username: &str) -> Response {
    let cookies = [
        (SET_COOKIE, key.login_cookie(username)),
        (
            SET_COOKIE,
            session::set_cookie(LOGIN_COOKIE, "", Duration::ZERO),
        ),
    ];
    (AppendHeaders(cookies), Redirect::to(DYNAMIC_LISTS_PAGE)).into_response()
}

fn login_failed(status: StatusCode, message: &str) -> Response {
    log::warn!("Login failed: {message}");
    let body = format!(
        "<h3>Login failed</h3><p>{}</p><p>{} - please try logging in again.</p>",
        escape::html(message),
        html::link(DYNAMIC_LISTS_PAGE, "Back to the tool")
    );
    (status, html::page("Listeria", DYNAMIC_LISTS_PAGE, &body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_rights() {
        let logged_in = rights(Some("Magnus Manske"));
        assert_eq!(
            logged_in["result"]["query"]["userinfo"]["name"],
            "Magnus Manske"
        );
        assert_eq!(logged_in["status"], "OK");
        let anonymous = rights(None);
        assert!(anonymous["result"]["query"]["userinfo"]["name"].is_null());
        assert_eq!(
            anonymous["result"]["error"]["code"],
            "mwoauth-invalid-authorization"
        );
    }

    #[test]
    fn test_items_of_first_variable() {
        let result: SparqlApiResult = serde_json::from_value(json!({
            "head": {"vars": ["item", "other"]},
            "results": {"bindings": [
                {"item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q42"},
                 "other": {"type": "uri", "value": "http://www.wikidata.org/entity/Q1"}},
                {"item": {"type": "literal", "value": "not an item"}},
                {"other": {"type": "uri", "value": "http://www.wikidata.org/entity/Q2"}},
                {"item": {"type": "uri", "value": "http://www.wikidata.org/entity/P31"}},
                {"item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q1337"}},
            ]}
        }))
        .unwrap();
        assert_eq!(items_of_first_variable(&result), vec![42, 1337]);
        assert!(items_of_first_variable(&SparqlApiResult::default()).is_empty());
    }

    #[test]
    fn test_data() {
        let answer = data(vec![1, 2]).unwrap();
        assert_eq!(Value::Object(answer), json!({"data": [1, 2]}));
    }
}
