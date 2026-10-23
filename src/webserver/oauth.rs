//! Logging in with a Wikimedia account through OAuth 1.0a, like the PHP
//! tool's Widar did, with the tool's consumer (its callback is `/api.php`):
//!
//! 1. [`OAuthClient::start_login`] gets a request token, and the URL where
//!    the user allows it.
//! 2. MediaWiki sends the user back to
//!    `/api.php?oauth_verifier=…&oauth_token=…`, and
//!    [`OAuthClient::finish_login`] trades the request token for an access
//!    token, with which it asks Wikidata who the user is.
//!
//! Only the user name is kept (in a signed cookie, see `session`); the
//! website edits nothing, so the access token is not needed afterwards.

use crate::LISTERIA_USER_AGENT;
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use serde_json::Value;
use sha1::Sha1;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wikimisc::title::percent_encode;

/// MediaWiki's OAuth endpoints for Wikimedia sites.
const OAUTH_URL: &str = "https://www.mediawiki.org/wiki/Special:OAuth";
/// The API asked who a user is.
const USERINFO_API: &str = "https://www.wikidata.org/w/api.php";
/// How long a user has to allow the login.
const PENDING_TTL: Duration = Duration::from_secs(60 * 60);
/// Most logins in progress at once, so a flood of them cannot fill the memory.
const MAX_PENDING: usize = 10_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TIMEOUT: Duration = Duration::from_secs(25);

/// The tool's OAuth consumer (`webserver.oauth` in the config).
#[derive(Clone, Deserialize)]
pub(super) struct OAuthConsumer {
    consumer_key: String,
    consumer_secret: String,
}

impl OAuthConsumer {
    pub(super) fn secret(&self) -> &str {
        &self.consumer_secret
    }
}

impl std::fmt::Debug for OAuthConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConsumer")
            .field("consumer_key", &self.consumer_key)
            .finish_non_exhaustive()
    }
}

/// A request or access token.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    key: String,
    secret: String,
}

#[derive(Debug)]
pub(super) struct OAuthClient {
    consumer: OAuthConsumer,
    http: reqwest::Client,
    /// The request tokens of logins in progress: key → (secret, when issued).
    pending: Mutex<HashMap<String, (String, Instant)>>,
}

impl OAuthClient {
    pub(super) fn new(consumer: OAuthConsumer) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(LISTERIA_USER_AGENT)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(TIMEOUT)
            .build()?;
        Ok(Self {
            consumer,
            http,
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// Starts a login: returns the request token's key, and the URL where the
    /// user allows the login.
    pub(super) async fn start_login(&self) -> Result<(String, String)> {
        // "oob": MediaWiki sends the user to the consumer's registered callback
        let token = self
            .fetch_token("initiate", &[("oauth_callback", "oob")], None)
            .await?;
        self.remember(&token)?;
        let url = format!(
            "{OAUTH_URL}/authorize?oauth_token={}&oauth_consumer_key={}",
            percent_encode(&token.key),
            percent_encode(&self.consumer.consumer_key)
        );
        Ok((token.key, url))
    }

    /// Completes the login of request token `token_key`, which the user
    /// allowed with `verifier`: returns the user name.
    pub(super) async fn finish_login(&self, token_key: &str, verifier: &str) -> Result<String> {
        let request_token = self
            .take_pending(token_key)
            .ok_or_else(|| anyhow!("This login attempt has expired. Please log in again."))?;
        let access_token = self
            .fetch_token(
                "token",
                &[("oauth_verifier", verifier)],
                Some(&request_token),
            )
            .await?;
        self.username(&access_token).await
    }

    /// Gets a token from `Special:OAuth/{step}`, signing with `token` if any.
    async fn fetch_token(
        &self,
        step: &str,
        extra_params: &[(&str, &str)],
        token: Option<&Token>,
    ) -> Result<Token> {
        let url = format!("{OAUTH_URL}/{step}");
        let mut params = self.oauth_params(token);
        params.push(("format".to_string(), "json".to_string()));
        params.extend(
            extra_params
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        let token_secret = token.map_or("", |token| token.secret.as_str());
        let signature = signature(
            "GET",
            &url,
            &params,
            &self.consumer.consumer_secret,
            token_secret,
        );
        params.push(("oauth_signature".to_string(), signature));
        let response = self
            .http
            .get(format!("{url}?{}", query_string(&params)))
            .send()
            .await?
            .text()
            .await?;
        parse_token(&response)
    }

    /// Asks Wikidata who the owner of `access_token` is.
    async fn username(&self, access_token: &Token) -> Result<String> {
        let query: Vec<(String, String)> = [
            ("action", "query"),
            ("meta", "userinfo"),
            ("format", "json"),
        ]
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        let mut oauth = self.oauth_params(Some(access_token));
        let signed: Vec<_> = query.iter().chain(oauth.iter()).cloned().collect();
        let signature = signature(
            "GET",
            USERINFO_API,
            &signed,
            &self.consumer.consumer_secret,
            &access_token.secret,
        );
        oauth.push(("oauth_signature".to_string(), signature));
        let header = oauth
            .iter()
            .map(|(key, value)| format!("{}=\"{}\"", percent_encode(key), percent_encode(value)))
            .collect::<Vec<_>>()
            .join(", ");
        let response: Value = self
            .http
            .get(format!("{USERINFO_API}?{}", query_string(&query)))
            .header(AUTHORIZATION, format!("OAuth {header}"))
            .send()
            .await?
            .json()
            .await?;
        username_from_userinfo(&response)
    }

    /// The `oauth_` parameters of a request, without the signature.
    fn oauth_params(&self, token: Option<&Token>) -> Vec<(String, String)> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut ret = vec![
            (
                "oauth_consumer_key".to_string(),
                self.consumer.consumer_key.clone(),
            ),
            (
                "oauth_nonce".to_string(),
                format!("{:032x}", rand::random::<u128>()),
            ),
            (
                "oauth_signature_method".to_string(),
                "HMAC-SHA1".to_string(),
            ),
            ("oauth_timestamp".to_string(), timestamp.to_string()),
            ("oauth_version".to_string(), "1.0".to_string()),
        ];
        if let Some(token) = token {
            ret.push(("oauth_token".to_string(), token.key.clone()));
        }
        ret
    }

    fn remember(&self, token: &Token) -> Result<()> {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        pending.retain(|_, (_, issued)| now.duration_since(*issued) < PENDING_TTL);
        if pending.len() >= MAX_PENDING {
            return Err(anyhow!(
                "Too many logins in progress, please try again later."
            ));
        }
        pending.insert(token.key.clone(), (token.secret.clone(), now));
        Ok(())
    }

    fn take_pending(&self, key: &str) -> Option<Token> {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        let (secret, issued) = pending.remove(key)?;
        (issued.elapsed() < PENDING_TTL).then(|| Token {
            key: key.to_string(),
            secret,
        })
    }
}

/// The OAuth 1.0a HMAC-SHA1 signature (RFC 5849, section 3.4) of a request
/// to `url` (without its query string) with `params` (those of the query
/// string and the `oauth_` ones).
fn signature(
    method: &str,
    url: &str,
    params: &[(String, String)],
    consumer_secret: &str,
    token_secret: &str,
) -> String {
    let mut encoded: Vec<(String, String)> = params
        .iter()
        .map(|(key, value)| (percent_encode(key), percent_encode(value)))
        .collect();
    encoded.sort();
    let normalized = encoded
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let base = format!(
        "{}&{}&{}",
        method.to_uppercase(),
        percent_encode(url),
        percent_encode(&normalized)
    );
    let key = format!(
        "{}&{}",
        percent_encode(consumer_secret),
        percent_encode(token_secret)
    );
    let mut mac =
        Hmac::<Sha1>::new_from_slice(key.as_bytes()).expect("HMAC takes keys of any length");
    mac.update(base.as_bytes());
    STANDARD.encode(mac.finalize().into_bytes())
}

fn query_string(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{}={}", percent_encode(key), percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The token in MediaWiki's answer: `oauth_token` and `oauth_token_secret`,
/// or the older `key` and `secret`.
fn parse_token(response: &str) -> Result<Token> {
    let invalid = || anyhow!("Unexpected answer from MediaWiki's OAuth: {response}");
    let json: Value = serde_json::from_str(response).map_err(|_| invalid())?;
    if json.get("error").is_some() {
        return Err(anyhow!("MediaWiki's OAuth refused: {json}"));
    }
    let field = |names: [&str; 2]| {
        names
            .iter()
            .find_map(|name| json[name].as_str())
            .map(str::to_string)
    };
    Ok(Token {
        key: field(["oauth_token", "key"]).ok_or_else(invalid)?,
        secret: field(["oauth_token_secret", "secret"]).ok_or_else(invalid)?,
    })
}

fn username_from_userinfo(response: &Value) -> Result<String> {
    let userinfo = &response["query"]["userinfo"];
    match userinfo["name"].as_str() {
        Some(name) if userinfo.get("anon").is_none() => Ok(name.to_string()),
        _ => Err(anyhow!(
            "Wikidata did not confirm the login: {}",
            response["error"]["info"]
                .as_str()
                .unwrap_or("not logged in")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn client() -> OAuthClient {
        OAuthClient::new(OAuthConsumer {
            consumer_key: "key".to_string(),
            consumer_secret: "secret".to_string(),
        })
        .unwrap()
    }

    #[test]
    fn test_signature_reference_example() {
        // The example of the OAuth Core 1.0 specification, appendix A.5
        let params = pairs(&[
            ("file", "vacation.jpg"),
            ("size", "original"),
            ("oauth_consumer_key", "dpf43f3p2l4k3l03"),
            ("oauth_token", "nnch734d00sl2jdk"),
            ("oauth_signature_method", "HMAC-SHA1"),
            ("oauth_timestamp", "1191242096"),
            ("oauth_nonce", "kllo9940pd9333jh"),
            ("oauth_version", "1.0"),
        ]);
        assert_eq!(
            signature(
                "GET",
                "http://photos.example.net/photos",
                &params,
                "kd94hf93k423kf44",
                "pfkkdhi9sl3r4s00"
            ),
            "tR3+Ty81lMeYAr/Fid0kMTYa/WM="
        );
    }

    #[test]
    fn test_query_string_encodes() {
        let params = pairs(&[("a", "b c"), ("oauth_callback", "oob"), ("x", "1/2+3")]);
        assert_eq!(
            query_string(&params),
            "a=b%20c&oauth_callback=oob&x=1%2F2%2B3"
        );
    }

    #[test]
    fn test_parse_token() {
        let expected = Token {
            key: "k".into(),
            secret: "s".into(),
        };
        let current = r#"{"oauth_token":"k","oauth_token_secret":"s"}"#;
        assert_eq!(parse_token(current).unwrap(), expected);
        let older = r#"{"key":"k","secret":"s","oauth_callback_confirmed":"true"}"#;
        assert_eq!(parse_token(older).unwrap(), expected);
        assert!(parse_token(r#"{"error":"mwoauth-invalid-authorization"}"#).is_err());
        assert!(parse_token("<html>Error</html>").is_err());
        assert!(parse_token(r#"{"key":"k"}"#).is_err());
    }

    #[test]
    fn test_username_from_userinfo() {
        let response = json!({"query": {"userinfo": {"id": 1, "name": "Magnus Manske"}}});
        assert_eq!(username_from_userinfo(&response).unwrap(), "Magnus Manske");
        let anonymous = json!({"query": {"userinfo": {"id": 0, "name": "127.0.0.1", "anon": ""}}});
        assert!(username_from_userinfo(&anonymous).is_err());
        let error = json!({"error": {"code": "mwoauth-invalid-authorization", "info": "No grant"}});
        assert!(
            username_from_userinfo(&error)
                .unwrap_err()
                .to_string()
                .contains("No grant")
        );
    }

    #[test]
    fn test_pending_tokens_are_taken_once() {
        let client = client();
        let token = Token {
            key: "k".into(),
            secret: "s".into(),
        };
        client.remember(&token).unwrap();
        assert_eq!(client.take_pending("other"), None);
        assert_eq!(client.take_pending("k"), Some(token));
        assert_eq!(client.take_pending("k"), None);
    }

    #[test]
    fn test_oauth_params() {
        let client = client();
        let params = client.oauth_params(Some(&Token {
            key: "t".into(),
            secret: "s".into(),
        }));
        let get = |name: &str| {
            params
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("oauth_consumer_key"), Some("key"));
        assert_eq!(get("oauth_token"), Some("t"));
        assert_eq!(get("oauth_nonce").map(str::len), Some(32));
        assert!(
            client
                .oauth_params(None)
                .iter()
                .all(|(key, _)| key != "oauth_token")
        );
    }

    #[test]
    fn test_consumer_debug_hides_secret() {
        let consumer = OAuthConsumer {
            consumer_key: "key".to_string(),
            consumer_secret: "very-secret".to_string(),
        };
        assert!(!format!("{consumer:?}").contains("very-secret"));
    }
}
