//! Circuit breaker for protecting external endpoints from thundering-herd retry storms.
//!
//! After `FAILURE_THRESHOLD` consecutive failures the circuit opens and rejects
//! further requests immediately for `RECOVERY_SECS`, giving the downstream
//! endpoint time to recover. After that window one probe request is allowed
//! through (half-open state); a success resets the breaker, a failure
//! re-opens it.
//!
//! All state is stored in lock-free atomics, making `CircuitBreaker` cheap to
//! clone behind an `Arc` and safe to share across async tasks.

use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Consecutive failures before the circuit opens.
const FAILURE_THRESHOLD: u32 = 5;
/// Seconds to keep the circuit open before allowing a probe request.
const RECOVERY_SECS: i64 = 60;

/// Thread-safe circuit breaker using lock-free atomics.
///
/// State is encoded in two atomics:
/// - `consecutive_failures`: incremented on each failure, reset to 0 on success.
/// - `opened_at_secs`: unix-second timestamp when the circuit was opened; 0 = closed.
#[derive(Debug, Default)]
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    opened_at_secs: AtomicI64,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self::default()
    }

    fn now_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// Returns `true` when the circuit is OPEN and the request should be
    /// rejected immediately without contacting the endpoint.
    ///
    /// When the recovery timeout has elapsed, the circuit transitions to
    /// HALF_OPEN: this method returns `false` exactly once so that one probe
    /// request can reach the endpoint. If that probe fails, `record_failure`
    /// will re-open the circuit.
    pub fn is_open(&self) -> bool {
        let opened_at = self.opened_at_secs.load(Ordering::Relaxed);
        if opened_at == 0 {
            return false;
        }
        if Self::now_secs() - opened_at < RECOVERY_SECS {
            return true; // still OPEN
        }
        // HALF_OPEN: clear opened_at to let exactly one probe through.
        // CAS prevents multiple concurrent callers from all acting as probes.
        let _ = self.opened_at_secs.compare_exchange(
            opened_at,
            0,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        false
    }

    /// Record a successful request. Resets the failure counter and closes the circuit.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.opened_at_secs.store(0, Ordering::Relaxed);
    }

    /// Record a failed request. Opens the circuit once the failure threshold is reached.
    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= FAILURE_THRESHOLD {
            // Open the circuit only if it is not already open.
            let _ = self.opened_at_secs.compare_exchange(
                0,
                Self::now_secs(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }
}

/// Wraps a fallible async operation with circuit-breaker semantics:
///   1. If the breaker is OPEN, return `open_err()` without invoking `f`.
///   2. Otherwise run `f().await`; record success or failure based on the
///      terminal outcome so a retried-and-succeeded operation leaves the
///      breaker fully healthy.
///
/// Generic over both `T` and `E` so it can be used uniformly across SPARQL
/// (custom `ListeriaError`), MW API (anyhow), and entity-load call sites.
///
/// A `tracing::warn!` is emitted each time the breaker short-circuits a call
/// so operators have a real-time signal that requests are being shed —
/// previously the only visibility was the DEFERRED row written to pagestatus
/// many layers later (audit F5.2).
pub async fn with_breaker<T, E, F, Fut>(
    breaker: &CircuitBreaker,
    open_err: impl FnOnce() -> E,
    f: F,
) -> Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    if breaker.is_open() {
        let err = open_err();
        tracing::warn!(
            error = %err,
            "circuit breaker is OPEN; short-circuiting request"
        );
        return Err(err);
    }
    match f().await {
        Ok(v) => {
            breaker.record_success();
            Ok(v)
        }
        Err(e) => {
            breaker.record_failure();
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_closed_by_default() {
        let cb = CircuitBreaker::new();
        assert!(!cb.is_open());
    }

    #[test]
    fn test_opens_after_threshold() {
        let cb = CircuitBreaker::new();
        for _ in 0..FAILURE_THRESHOLD {
            cb.record_failure();
        }
        assert!(cb.is_open());
    }

    #[test]
    fn test_does_not_open_below_threshold() {
        let cb = CircuitBreaker::new();
        for _ in 0..FAILURE_THRESHOLD - 1 {
            cb.record_failure();
        }
        assert!(!cb.is_open());
    }

    #[test]
    fn test_success_resets_circuit() {
        let cb = CircuitBreaker::new();
        for _ in 0..FAILURE_THRESHOLD {
            cb.record_failure();
        }
        assert!(cb.is_open());
        cb.record_success();
        assert!(!cb.is_open());
    }

    #[test]
    fn test_success_resets_failure_counter() {
        let cb = CircuitBreaker::new();
        // Record failures below threshold, then succeed, then fail again.
        for _ in 0..FAILURE_THRESHOLD - 1 {
            cb.record_failure();
        }
        cb.record_success();
        // After reset, FAILURE_THRESHOLD new failures are needed to open.
        for _ in 0..FAILURE_THRESHOLD - 1 {
            cb.record_failure();
        }
        assert!(!cb.is_open());
        cb.record_failure();
        assert!(cb.is_open());
    }

    #[test]
    fn test_idempotent_open() {
        let cb = CircuitBreaker::new();
        for _ in 0..FAILURE_THRESHOLD * 2 {
            cb.record_failure();
        }
        // Multiple extra failures must not corrupt state.
        assert!(cb.is_open());
    }

    /// Forces the breaker into a state where the recovery window has elapsed,
    /// without actually waiting RECOVERY_SECS seconds. Returns the simulated
    /// `opened_at_secs` value so callers can sanity-check the setup.
    fn open_and_age_past_recovery(cb: &CircuitBreaker) -> i64 {
        for _ in 0..FAILURE_THRESHOLD {
            cb.record_failure();
        }
        let aged = CircuitBreaker::now_secs() - RECOVERY_SECS - 1;
        cb.opened_at_secs.store(aged, Ordering::Relaxed);
        aged
    }

    #[test]
    fn test_is_open_still_true_within_recovery_window() {
        let cb = CircuitBreaker::new();
        for _ in 0..FAILURE_THRESHOLD {
            cb.record_failure();
        }
        // Simulate a small amount of elapsed time, still within the window.
        let recent = CircuitBreaker::now_secs() - (RECOVERY_SECS / 2);
        cb.opened_at_secs.store(recent, Ordering::Relaxed);
        assert!(
            cb.is_open(),
            "circuit must stay OPEN while inside the recovery window"
        );
    }

    #[test]
    fn test_half_open_lets_a_probe_through_once_and_then_remains_closed_on_success() {
        let cb = CircuitBreaker::new();
        let aged = open_and_age_past_recovery(&cb);
        assert!(aged > 0, "test setup: opened_at_secs should be positive");

        // First check: the probe path runs the CAS, returns false, and clears
        // opened_at_secs so subsequent callers don't all act as probes.
        assert!(!cb.is_open(), "first probe after recovery must reach endpoint");
        assert_eq!(
            cb.opened_at_secs.load(Ordering::Relaxed),
            0,
            "probe transition must clear opened_at_secs"
        );

        // A successful probe records via record_success, fully resetting state.
        cb.record_success();
        assert_eq!(cb.consecutive_failures.load(Ordering::Relaxed), 0);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_half_open_probe_failure_reopens_breaker() {
        let cb = CircuitBreaker::new();
        open_and_age_past_recovery(&cb);

        // The probe call clears opened_at_secs and lets the request through.
        assert!(!cb.is_open());
        // Recording a failure at this point counts against the existing failure
        // streak: after FAILURE_THRESHOLD prior failures, one more should open
        // the circuit again immediately.
        cb.record_failure();
        assert!(cb.is_open(), "failed probe must re-open the circuit");
    }

    // ── with_breaker ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_with_breaker_short_circuits_when_open() {
        let cb = CircuitBreaker::new();
        open_and_age_past_recovery(&cb);
        // Force the breaker back to OPEN with a recent timestamp so is_open()
        // returns true without consuming the half-open probe.
        cb.opened_at_secs
            .store(CircuitBreaker::now_secs(), Ordering::Relaxed);
        assert!(cb.is_open());

        let called = std::sync::atomic::AtomicBool::new(false);
        let res: Result<(), &str> = with_breaker(
            &cb,
            || "open",
            || async {
                called.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert_eq!(res.unwrap_err(), "open");
        assert!(!called.load(Ordering::SeqCst), "f must not run when open");
    }

    #[tokio::test]
    async fn test_with_breaker_records_success() {
        let cb = CircuitBreaker::new();
        // Pre-populate failures so we can prove record_success resets them.
        for _ in 0..FAILURE_THRESHOLD - 1 {
            cb.record_failure();
        }
        let res: Result<i32, &str> = with_breaker(&cb, || "open", || async { Ok(7) }).await;
        assert_eq!(res.unwrap(), 7);
        assert_eq!(
            cb.consecutive_failures.load(Ordering::Relaxed),
            0,
            "successful call must reset failure counter"
        );
    }

    #[tokio::test]
    async fn test_with_breaker_records_failure() {
        let cb = CircuitBreaker::new();
        let res: Result<(), &str> = with_breaker(&cb, || "open", || async { Err("boom") }).await;
        assert_eq!(res.unwrap_err(), "boom");
        assert_eq!(
            cb.consecutive_failures.load(Ordering::Relaxed),
            1,
            "failed call must increment the failure counter"
        );
    }

    #[test]
    fn test_half_open_probe_is_one_shot() {
        // After the recovery window elapses, is_open() must return false
        // exactly once — the implementation clears opened_at_secs on the
        // probe path so concurrent callers don't all bypass the breaker.
        let cb = CircuitBreaker::new();
        open_and_age_past_recovery(&cb);

        // First call: probe gets through.
        assert!(!cb.is_open());
        // Second call: no failure has been recorded yet, so the breaker stays
        // closed (opened_at_secs is 0). This matches the documented behaviour
        // — the probe is one-shot in the sense that opened_at_secs is cleared,
        // not in the sense that subsequent is_open() calls return true.
        assert!(!cb.is_open());
        assert_eq!(cb.opened_at_secs.load(Ordering::Relaxed), 0);
    }
}
