//! Request parameters the way PHP's `$_REQUEST` has them, as the website's
//! pages and `dynamic.html` expect: from the query string and, for a POST,
//! the form in the body, which wins.

use axum::extract::{Form, FromRequest, Query, Request};
use axum::http::Method;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub(super) struct Params(HashMap<String, String>);

impl Params {
    pub(super) fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// The parameter, or `default` when it is missing or empty.
    pub(super) fn get_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.get(key)
            .filter(|value| !value.is_empty())
            .unwrap_or(default)
    }

    /// The parameter as a number, or `default` when it is missing or not a
    /// number.
    pub(super) fn number(&self, key: &str, default: u64) -> u64 {
        self.get(key)
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(default)
    }
}

impl<S: Send + Sync> FromRequest<S> for Params {
    type Rejection = Response;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Query(mut params) = Query::<HashMap<String, String>>::try_from_uri(request.uri())
            .map_err(IntoResponse::into_response)?;
        if request.method() == Method::POST {
            let Form(form) = Form::<HashMap<String, String>>::from_request(request, state)
                .await
                .map_err(IntoResponse::into_response)?;
            params.extend(form);
        }
        Ok(Self(params))
    }
}

#[cfg(test)]
impl<const N: usize> From<[(&str, &str); N]> for Params {
    fn from(pairs: [(&str, &str); N]) -> Self {
        Self(
            pairs
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    async fn params_of(request: Request) -> Params {
        Params::from_request(request, &()).await.unwrap()
    }

    #[tokio::test]
    async fn test_query_string() {
        let request = Request::get("/api.php?query=search&text=a%20b")
            .body(Body::empty())
            .unwrap();
        let params = params_of(request).await;
        assert_eq!(params.get("query"), Some("search"));
        assert_eq!(params.get("text"), Some("a b"));
        assert_eq!(params.get("missing"), None);
    }

    #[tokio::test]
    async fn test_form_body_wins_over_query_string() {
        let request = Request::post("/api.php?query=get_list&list=1")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("query=set_list&list=%7B%7D"))
            .unwrap();
        let params = params_of(request).await;
        assert_eq!(params.get("query"), Some("set_list"));
        assert_eq!(params.get("list"), Some("{}"));
    }

    #[test]
    fn test_get_or_and_number() {
        let params = Params::from([("lang", ""), ("limit", "20"), ("bad", "x")]);
        assert_eq!(params.get_or("lang", "wikipedia"), "wikipedia");
        assert_eq!(params.get_or("project", "wikipedia"), "wikipedia");
        assert_eq!(params.number("limit", 10), 20);
        assert_eq!(params.number("bad", 10), 10);
        assert_eq!(params.number("missing", 10), 10);
    }
}
