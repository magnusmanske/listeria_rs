//! Per-page profiling: optional timing data written to the `list_log` table.
//!
//! Separating this concern from `ListeriaList` keeps the core pipeline free
//! of direct database I/O and makes the profiling lifecycle independently
//! testable.

use crate::configuration::Configuration;
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// Writes per-stage timing rows to `list_log` when profiling is enabled.
///
/// Holds an `Arc<Configuration>` (for DB pool access), the wiki/page identity,
/// and the wall-clock timestamp of the previous `profile()` call so it can
/// compute inter-stage deltas.
#[derive(Debug, Clone)]
pub struct ProfilingService {
    config: Arc<Configuration>,
    wiki: String,
    page: String,
    enabled: bool,
    last_timestamp: DateTime<Utc>,
}

impl ProfilingService {
    pub fn new(config: Arc<Configuration>, wiki: &str, page: &str, enabled: bool) -> Self {
        Self {
            config,
            wiki: wiki.to_string(),
            page: page.to_string(),
            enabled,
            last_timestamp: Utc::now(),
        }
    }

    /// Records a profiling checkpoint. No-op when profiling is disabled.
    pub async fn profile(&mut self, msg: &str) {
        if !self.enabled {
            return;
        }
        let now = Utc::now();
        let last = self.last_timestamp;
        self.last_timestamp = now;
        let diff = now - last;
        let timestamp = now.format("%Y%m%d%H%M%S").to_string();
        let time_diff = diff.num_milliseconds();

        let _ = self.log2db(time_diff, &timestamp, msg).await;

        let section = format!("{}:{}", self.wiki, self.page);
        log::debug!("{timestamp} {section}: {msg} [{time_diff}ms]");
    }

    async fn log2db(&self, ms: i64, timestamp: &str, msg: &str) -> anyhow::Result<()> {
        use mysql_async::prelude::Queryable;
        use mysql_async::params;
        let sql = "REPLACE INTO list_log (wiki, page, timestamp, diff_ms, message) VALUES (:wiki, :page, :timestamp, :ms, :msg)";
        let wiki = self.wiki.as_str();
        let page = self.page.as_str();
        self.config
            .pool()?
            .get_conn()
            .await?
            .exec_drop(sql, params! {wiki, page, timestamp, ms, msg})
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_profiling_service_new() {
        let config = Arc::new(crate::configuration::Configuration::default());
        let svc = ProfilingService::new(config, "testwiki", "Test Page", false);
        assert!(!svc.enabled);
        assert_eq!(svc.wiki, "testwiki");
        assert_eq!(svc.page, "Test Page");
    }

    #[tokio::test]
    async fn test_profile_noop_when_disabled() {
        let config = Arc::new(crate::configuration::Configuration::default());
        let mut svc = ProfilingService::new(config, "testwiki", "Test Page", false);
        let before = svc.last_timestamp;
        svc.profile("some stage").await;
        // last_timestamp must not advance when disabled
        assert_eq!(svc.last_timestamp, before);
    }

    #[tokio::test]
    async fn test_profile_advances_timestamp_when_enabled_but_no_pool() {
        // With no DB pool the log2db call fails silently; the timestamp still
        // advances and the function does not panic.
        let config = Arc::new(crate::configuration::Configuration::default());
        let mut svc = ProfilingService::new(config, "testwiki", "Test Page", true);
        let before = svc.last_timestamp;
        svc.profile("test stage").await;
        assert!(svc.last_timestamp >= before);
    }
}
