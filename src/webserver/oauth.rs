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
//!
//! The protocol itself lives in [`wikimisc::oauth1`].

use crate::LISTERIA_USER_AGENT;
use anyhow::{Result, anyhow};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use wikimisc::oauth1::{Consumer, OAuth, PendingTokens};

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

#[derive(Debug)]
pub(super) struct OAuthClient {
    oauth: OAuth,
    /// The request tokens of logins in progress.
    pending: PendingTokens,
}

impl OAuthClient {
    pub(super) fn new(consumer: OAuthConsumer) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(LISTERIA_USER_AGENT)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(TIMEOUT)
            .build()?;
        let consumer = Consumer::new(
            consumer.consumer_key,
            consumer.consumer_secret,
            LISTERIA_USER_AGENT,
        );
        Ok(Self {
            oauth: OAuth::new(consumer, http),
            pending: PendingTokens::new(PENDING_TTL, MAX_PENDING),
        })
    }

    /// Starts a login: returns the request token's key, and the URL where the
    /// user allows the login.
    pub(super) async fn start_login(&self) -> Result<(String, String)> {
        // "oob": MediaWiki sends the user to the consumer's registered callback
        let (token, url) = self.oauth.initiate("oob").await?;
        self.pending.remember(&token)?;
        Ok((token.key, url))
    }

    /// Completes the login of request token `token_key`, which the user
    /// allowed with `verifier`: returns the user name.
    pub(super) async fn finish_login(&self, token_key: &str, verifier: &str) -> Result<String> {
        let request_token = self
            .pending
            .take(token_key)
            .ok_or_else(|| anyhow!("This login attempt has expired. Please log in again."))?;
        let access_token = self.oauth.access_token(&request_token, verifier).await?;
        let response = self
            .oauth
            .api(
                USERINFO_API,
                Some(&access_token),
                &[("action", "query"), ("meta", "userinfo")],
            )
            .await?;
        username_from_userinfo(&response)
    }
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
    use wikimisc::oauth1::Token;

    fn client() -> OAuthClient {
        OAuthClient::new(OAuthConsumer {
            consumer_key: "key".to_string(),
            consumer_secret: "secret".to_string(),
        })
        .unwrap()
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
        client.pending.remember(&token).unwrap();
        assert_eq!(client.pending.take("other").map(|t| t.key), None);
        assert_eq!(
            client.pending.take("k").map(|t| (t.key, t.secret)),
            Some(("k".into(), "s".into()))
        );
        assert_eq!(client.pending.take("k").map(|t| t.key), None);
    }

    #[test]
    fn test_consumer_debug_hides_secret() {
        let consumer = Consumer::new("key", "very-secret", "agent");
        assert!(!format!("{consumer:?}").contains("very-secret"));
    }
}
