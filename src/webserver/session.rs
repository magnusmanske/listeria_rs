//! Who is logged in: the user name, in a cookie the website signs, so it can
//! be trusted without asking Wikidata on every request.
//!
//! The HMAC, cookie attributes and parsing live in
//! [`wikimisc::web::session`]; this module only names Listeria's cookie and
//! picks the context that keeps existing logins valid.

use axum::http::HeaderMap;
use std::time::Duration;
pub(super) use wikimisc::web::session::cookie;
use wikimisc::web::session::{
    SessionKey as SharedKey, clear_cookie, set_cookie as shared_set_cookie, unix_time,
};

/// The cookie with the signed user name.
const USER_COOKIE: &str = "listeria_user";
/// How long a login lasts.
const LOGIN_LIFETIME: Duration = Duration::from_secs(90 * 24 * 60 * 60);

/// Signs and checks the user cookie.
#[derive(Clone)]
pub(super) struct SessionKey(SharedKey);

impl std::fmt::Debug for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionKey(…)")
    }
}

impl SessionKey {
    /// The key derived from `secret`, which must not be known outside the
    /// website (the OAuth consumer secret).
    pub(super) fn derive(secret: &str) -> Self {
        Self(SharedKey::derive_with_context(
            secret,
            "listeria website session",
        ))
    }

    /// The logged-in user of a request.
    pub(super) fn user(&self, headers: &HeaderMap) -> Option<String> {
        self.verify(cookie(headers, USER_COOKIE)?, unix_time())
    }

    /// The `Set-Cookie` value that logs `username` in.
    pub(super) fn login_cookie(&self, username: &str) -> String {
        let expires = unix_time() + LOGIN_LIFETIME.as_secs();
        set_cookie(USER_COOKIE, &self.sign(username, expires), LOGIN_LIFETIME)
    }

    fn sign(&self, username: &str, expires: u64) -> String {
        self.0.sign(username, expires)
    }

    fn verify(&self, value: &str, now: u64) -> Option<String> {
        self.0.verify(value, now)
    }
}

/// The `Set-Cookie` value that logs the user out.
pub(super) fn logout_cookie() -> String {
    clear_cookie(USER_COOKIE, true)
}

/// A `Set-Cookie` value for a cookie only sent back to this site, over
/// HTTPS, and hidden from scripts.
pub(super) fn set_cookie(name: &str, value: &str, max_age: Duration) -> String {
    shared_set_cookie(name, value, max_age, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn key() -> SessionKey {
        SessionKey::derive("consumer secret")
    }

    #[test]
    fn test_sign_and_verify() {
        let value = key().sign("Magnus Manske", 2000);
        assert_eq!(key().verify(&value, 1000).as_deref(), Some("Magnus Manske"));
    }

    #[test]
    fn test_expired() {
        let value = key().sign("Magnus Manske", 2000);
        assert_eq!(key().verify(&value, 2000), None);
    }

    #[test]
    fn test_tampered_or_foreign() {
        let value = key().sign("Magnus Manske", 2000);
        let forged = value.replacen(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("Magnus Manske"),
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("Someone Else"),
            1,
        );
        assert_eq!(key().verify(&forged, 1000), None);
        let later = value.replacen("2000", "9999", 1);
        assert_eq!(
            SessionKey::derive("other secret").verify(&value, 1000),
            None
        );
        let _ = later;
    }

    #[test]
    fn test_login_cookie_and_logout() {
        let set = key().login_cookie("Magnus Manske");
        assert!(set.contains("HttpOnly"));
        assert!(set.contains("Secure"));
        assert!(set.contains("SameSite=Lax"));
        assert!(logout_cookie().contains("Max-Age=0"));
    }

    #[test]
    fn test_set_cookie_attributes() {
        let value = set_cookie("name", "value", Duration::from_secs(60));
        assert_eq!(
            value,
            "name=value; Max-Age=60; Path=/; HttpOnly; SameSite=Lax; Secure"
        );
    }
}
