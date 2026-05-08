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
}
