//! Service-dashboard telemetry: setup and the bot's own metrics.
//!
//! [`install`] starts pushing to the dashboard (see `wikimisc::telemetry`);
//! process readings, panics and watchdog kills are reported by wikimisc.
//! The functions here record what the bot does. Without an installed
//! telemetry they are no-ops, so library code and tests can call them freely.
//!
//! Labels must stay bounded (the dashboard keeps at most 500 series): never
//! label by page or wiki.

use crate::configuration::Configuration;
use std::time::Duration;
use wikimisc::metrics::{counter, gauge, histogram};
use wikimisc::telemetry::{ExitReason, Telemetry, TelemetryError};

/// Status of a page whose processing returned an error instead of a result.
pub const STATUS_ERROR: &str = "ERROR";
/// Status of a page that timed out or panicked; the queue marks it FAIL.
const STATUS_FAIL: &str = "FAIL";
/// Statuses counted as they are; anything else is counted as `OTHER`.
const PAGE_STATUSES: &[&str] = &[
    "OK",
    "FAIL",
    "DELETED",
    "INVALID",
    "TRANSLATION",
    "DEFERRED",
    STATUS_ERROR,
];

/// Circuit-breaker kinds, the `kind` label of `listeria_circuit_breaker_trips_total`.
pub const BREAKER_SPARQL: &str = "sparql";
pub const BREAKER_MW_API: &str = "mw_api";

/// Starts pushing to the service dashboard as `component`. Telemetry never
/// stops the bot: without a token, or on any other error, it is logged and
/// the bot runs without it.
pub fn install(component: &str, config: &Configuration) {
    let result = Telemetry::builder(component)
        .config(config.telemetry())
        .version(env!("CARGO_PKG_VERSION"))
        .install();
    match result {
        Ok(_) => log::info!("telemetry enabled as '{component}'"),
        Err(TelemetryError::NoToken) => log::info!("telemetry disabled: no token configured"),
        Err(e) => log::warn!("telemetry disabled: {e}"),
    }
}

/// Sends the final push, reporting how the run ended; a no-op without
/// telemetry. Signals and watchdog kills are reported by wikimisc itself.
/// The error itself is only logged: its text can name pages or users, and the
/// dashboard shows exit details publicly.
pub async fn shutdown(result: &anyhow::Result<()>) {
    let reason = match result {
        Ok(()) => ExitReason::Exit,
        Err(_) => ExitReason::Other("stopped with an error (see the log)".to_string()),
    };
    wikimisc::telemetry::shutdown(reason).await;
}

/// Records a processed page: `listeria_pages_total{status}` and
/// `listeria_page_duration_seconds`.
pub fn record_page(status: &str, duration: Duration) {
    counter!("listeria_pages_total", "status" => page_status_label(status)).increment(1);
    histogram!("listeria_page_duration_seconds").record(duration.as_secs_f64());
}

/// Records a page that exceeded its wall-clock budget: counted as a failed
/// page, and in `listeria_page_timeouts_total`.
pub fn record_page_timeout(duration: Duration) {
    counter!("listeria_page_timeouts_total").increment(1);
    record_page(STATUS_FAIL, duration);
}

/// Records a page whose processing panicked as a failed page. The panic itself
/// is reported by wikimisc, by source location only: page titles stay out of
/// telemetry, as lists often live in user space (`User:Name/…`).
pub fn record_page_panic(duration: Duration) {
    record_page(STATUS_FAIL, duration);
}

/// Records a saved edit (`listeria_edits_total`).
pub fn record_edit() {
    counter!("listeria_edits_total").increment(1);
}

/// Records a SPARQL query that failed after all retries because the endpoint
/// did not answer (`listeria_sparql_failures_total`). Malformed queries are
/// the query's fault, not the endpoint's, and are not counted.
pub fn record_sparql_failure() {
    counter!("listeria_sparql_failures_total").increment(1);
}

/// Records a circuit breaker opening (`listeria_circuit_breaker_trips_total{kind}`).
pub fn record_circuit_breaker_trip(kind: &'static str) {
    counter!("listeria_circuit_breaker_trips_total", "kind" => kind).increment(1);
}

/// Sets `listeria_max_threads`, the number of pages processed in parallel at most.
pub fn set_max_threads(max_threads: usize) {
    gauge!("listeria_max_threads").set(max_threads as f64);
}

/// Counts a page being processed in `listeria_bots_running` for as long as it lives.
#[derive(Debug)]
pub struct RunningBot(());

impl RunningBot {
    #[must_use]
    pub fn start() -> Self {
        gauge!("listeria_bots_running").increment(1.0);
        Self(())
    }
}

impl Drop for RunningBot {
    fn drop(&mut self) {
        gauge!("listeria_bots_running").decrement(1.0);
    }
}

/// `status` as one of a fixed set of label values, whatever its case.
fn page_status_label(status: &str) -> &'static str {
    PAGE_STATUSES
        .iter()
        .find(|known| known.eq_ignore_ascii_case(status))
        .copied()
        .unwrap_or("OTHER")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_status_label_known() {
        assert_eq!(page_status_label("OK"), "OK");
        assert_eq!(page_status_label("DEFERRED"), "DEFERRED");
    }

    #[test]
    fn test_page_status_label_is_case_insensitive() {
        // Single-wiki mode reports errors as "Error".
        assert_eq!(page_status_label("Error"), STATUS_ERROR);
    }

    #[test]
    fn test_page_status_label_unknown_is_bounded() {
        assert_eq!(page_status_label("Something new"), "OTHER");
        assert_eq!(page_status_label(""), "OTHER");
    }

    #[test]
    fn test_recording_without_telemetry_is_a_noop() {
        record_page("OK", Duration::from_secs(1));
        record_page_timeout(Duration::from_secs(600));
        record_page_panic(Duration::from_secs(1));
        record_edit();
        record_sparql_failure();
        record_circuit_breaker_trip(BREAKER_SPARQL);
        set_max_threads(8);
        drop(RunningBot::start());
    }
}
