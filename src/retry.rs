//! Generic exponential-backoff retry helper for transient I/O failures.
//!
//! Used at the boundaries that talk to external services (MediaWiki API,
//! SPARQL endpoints) so a single transient hiccup doesn't fail a whole page.
//! Each failed attempt is logged via `tracing::warn!` carrying `operation`,
//! `attempt`, `backoff_ms`, and `error` fields so the retry behaviour is
//! observable in production.

use std::future::Future;
use std::time::Duration;

/// Retries an async fallible operation with exponential backoff.
///
/// `max_attempts` is the total number of attempts (1 initial + `max_attempts - 1`
/// retries). `initial_backoff` is the wait before the first retry; the delay
/// doubles after each retry. The final failure is returned to the caller
/// unchanged — callers decide how to surface it.
pub(crate) async fn retry_with_backoff<T, F, Fut, E>(
    operation: &'static str,
    max_attempts: u32,
    initial_backoff: Duration,
    mut f: F,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut backoff = initial_backoff;
    let mut attempt: u32 = 1;
    loop {
        match f().await {
            Ok(value) => return Ok(value),
            Err(e) if attempt < max_attempts => {
                tracing::warn!(
                    operation = operation,
                    attempt = attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "external call failed; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn test_retry_with_backoff_returns_immediately_on_success() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<&'static str, &'static str> = retry_with_backoff(
            "test",
            3,
            Duration::from_millis(1),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok("done")
            },
        )
        .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_retries_until_success() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<&'static str, &'static str> = retry_with_backoff(
            "test",
            3,
            Duration::from_millis(1),
            || async {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 { Err("transient") } else { Ok("done") }
            },
        )
        .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "must call exactly until the first success"
        );
    }

    #[tokio::test]
    async fn test_retry_with_backoff_returns_last_error_after_max_attempts() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<(), &'static str> = retry_with_backoff(
            "test",
            3,
            Duration::from_millis(1),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("nope")
            },
        )
        .await;
        assert_eq!(result.unwrap_err(), "nope");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "must call exactly max_attempts times"
        );
    }

    #[tokio::test]
    async fn test_retry_with_backoff_max_attempts_one_does_not_retry() {
        let calls = AtomicU32::new(0);
        let result: std::result::Result<(), &'static str> = retry_with_backoff(
            "test",
            1,
            Duration::from_millis(1),
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("first")
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "max_attempts=1 must mean a single try with no retries"
        );
    }
}
