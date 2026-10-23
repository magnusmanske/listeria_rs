//! Who is logged in: the user name, in a cookie the website signs, so it can
//! be trusted without asking Wikidata on every request.

use axum::http::HeaderMap;
use axum::http::header::COOKIE;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The cookie with the signed user name.
const USER_COOKIE: &str = "listeria_user";
/// How long a login lasts.
const LOGIN_LIFETIME: Duration = Duration::from_secs(90 * 24 * 60 * 60);

/// Signs and checks the user cookie.
#[derive(Clone)]
pub(super) struct SessionKey([u8; 32]);

impl std::fmt::Debug for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionKey(…)")
    }
}

impl SessionKey {
    /// The key derived from `secret`, which must not be known outside the
    /// website (the OAuth consumer secret).
    pub(super) fn derive(secret: &str) -> Self {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .expect("HMAC takes keys of any length");
        mac.update(b"listeria website session");
        Self(mac.finalize().into_bytes().into())
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

    /// `{expires}.{username}.{signature}`, the latter two in base64.
    fn sign(&self, username: &str, expires: u64) -> String {
        let payload = format!("{expires}.{}", URL_SAFE_NO_PAD.encode(username));
        let signature = URL_SAFE_NO_PAD.encode(self.mac(&payload).finalize().into_bytes());
        format!("{payload}.{signature}")
    }

    /// The user name in a cookie value, if the signature is right and the
    /// login has not expired by `now` (Unix time).
    fn verify(&self, value: &str, now: u64) -> Option<String> {
        let (payload, signature) = value.rsplit_once('.')?;
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        self.mac(payload).verify_slice(&signature).ok()?;
        let (expires, username) = payload.split_once('.')?;
        if expires.parse::<u64>().ok()? <= now {
            return None;
        }
        String::from_utf8(URL_SAFE_NO_PAD.decode(username).ok()?).ok()
    }

    fn mac(&self, payload: &str) -> Hmac<Sha256> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC takes keys of any length");
        mac.update(payload.as_bytes());
        mac
    }
}

/// The `Set-Cookie` value that logs the user out.
pub(super) fn logout_cookie() -> String {
    set_cookie(USER_COOKIE, "", Duration::ZERO)
}

/// The value of cookie `name` in a request.
pub(super) fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value)
}

/// A `Set-Cookie` value for a cookie only sent back to this site, over
/// HTTPS, and hidden from scripts. Cross-site requests other than following
/// a link (e.g. a POST from another site) do not get it.
pub(super) fn set_cookie(name: &str, value: &str, max_age: Duration) -> String {
    format!(
        "{name}={value}; Max-Age={}; Path=/; HttpOnly; Secure; SameSite=Lax",
        max_age.as_secs()
    )
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

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
            &URL_SAFE_NO_PAD.encode("Magnus Manske"),
            &URL_SAFE_NO_PAD.encode("Someone Else"),
            1,
        );
        assert_eq!(key().verify(&forged, 1000), None);
        let later = value.replacen("2000", "9999", 1);
        assert_eq!(key().verify(&later, 1000), None);
        assert_eq!(
            SessionKey::derive("other secret").verify(&value, 1000),
            None
        );
        assert_eq!(key().verify("garbage", 1000), None);
        assert_eq!(key().verify("", 1000), None);
    }

    #[test]
    fn test_user_from_headers() {
        let set_cookie = key().login_cookie("Magnus Manske");
        let value = set_cookie.split(';').next().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("other=1; {value}")).unwrap(),
        );
        assert_eq!(key().user(&headers).as_deref(), Some("Magnus Manske"));
        assert_eq!(key().user(&HeaderMap::new()), None);
    }

    #[test]
    fn test_cookie() {
        let mut headers = HeaderMap::new();
        headers.append(COOKIE, HeaderValue::from_static("a=1; b=2"));
        headers.append(COOKIE, HeaderValue::from_static("c=3"));
        assert_eq!(cookie(&headers, "b"), Some("2"));
        assert_eq!(cookie(&headers, "c"), Some("3"));
        assert_eq!(cookie(&headers, "d"), None);
    }

    #[test]
    fn test_set_cookie_attributes() {
        let value = set_cookie("name", "value", Duration::from_secs(60));
        assert_eq!(
            value,
            "name=value; Max-Age=60; Path=/; HttpOnly; Secure; SameSite=Lax"
        );
        assert!(logout_cookie().contains("Max-Age=0"));
    }
}
