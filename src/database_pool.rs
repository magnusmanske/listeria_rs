//! MySQL database connection pooling for the bot's own database.

use crate::configuration::Configuration;
use anyhow::{Result, anyhow};
use std::future::Future;
use std::time::Duration;
use wikimisc::mysql_async::Conn;
use wikimisc::toolforge::pool::CustomPool;

/// Pool size when the config sets no `max_connections`.
const DEFAULT_MAX_CONNECTIONS: usize = 8;

#[derive(Debug, Clone)]
pub struct DatabasePool {
    pool: CustomPool,
    query_timeout: Duration,
}

impl DatabasePool {
    /// Creates a new connection pool from configuration settings.
    pub fn new(config: &Configuration) -> Result<Self> {
        let schema = config
            .mysql("schema")
            .as_str()
            .ok_or(anyhow!("No schema in config"))?
            .to_string();
        let max_connections = config
            .mysql("max_connections")
            .as_u64()
            .and_then(|u| u.try_into().ok())
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);
        Self::for_schema(config, &schema, max_connections)
    }

    /// A pool on another database of the `mysql` server, with the same
    /// account (e.g. the website's dynamic lists).
    pub fn for_schema(
        config: &Configuration,
        schema: &str,
        max_connections: usize,
    ) -> Result<Self> {
        Ok(Self {
            pool: Self::pool_from_config(config, schema, max_connections)?,
            query_timeout: config.db_query_timeout(),
        })
    }

    /// Gets a database connection from the pool.
    pub async fn get_conn(&self) -> Result<Conn> {
        let ret = self.pool.connect().await?;
        Ok(ret)
    }

    /// Wraps a DB operation with the configured query timeout.
    ///
    /// Use this to bound the wall-clock cost of any (get_conn + query) chain:
    /// a wedged replica or a slow UPDATE would otherwise hang the caller
    /// indefinitely. The timeout covers both pool checkout and query
    /// execution because the future is built (and awaited) inside the wrapper.
    pub async fn with_timeout<F, Fut, T>(&self, op_name: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        tokio::time::timeout(self.query_timeout, f())
            .await
            .map_err(|_| {
                anyhow!(
                    "DB operation '{op_name}' timed out after {}s",
                    self.query_timeout.as_secs()
                )
            })?
    }

    /// Builds the pool from the `mysql` config object.
    ///
    /// The server and credentials come from the config rather than from
    /// Toolforge's own lookup, so the same config works on Toolforge and
    /// through a local SSH tunnel (`host` `127.0.0.1` with the tunnel's
    /// `port`).
    fn pool_from_config(
        config: &Configuration,
        schema: &str,
        max_connections: usize,
    ) -> Result<CustomPool> {
        let host = config
            .mysql("host")
            .as_str()
            .ok_or(anyhow!("No host in config"))?
            .to_string();
        let port = config
            .mysql("port")
            .as_u64()
            .ok_or(anyhow!("No port in config"))?
            .try_into()
            .map_err(|_| anyhow!("Port value out of range"))?;
        let user = config
            .mysql("user")
            .as_str()
            .ok_or(anyhow!("No user in config"))?
            .to_string();
        let password = config
            .mysql("password")
            .as_str()
            .ok_or(anyhow!("No password in config"))?
            .to_string();

        let pool = CustomPool::builder(host, user, password)
            .port(port)
            .database(schema)
            .pool_max(max_connections)
            .build()?;
        Ok(pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a DatabasePool that has the requested query_timeout but a Pool
    /// pointed at an unreachable host — the `with_timeout` tests must not
    /// require an actual MySQL server.
    fn pool_with_query_timeout(timeout: Duration) -> DatabasePool {
        DatabasePool {
            pool: CustomPool::builder("127.0.0.1", "_test_", "_test_")
                .port(1) // unreachable
                .database("_test_")
                .build()
                .unwrap(),
            query_timeout: timeout,
        }
    }

    #[tokio::test]
    async fn test_with_timeout_returns_success_when_under_budget() {
        let pool = pool_with_query_timeout(Duration::from_secs(5));
        let result: Result<i32> = pool.with_timeout("noop", || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_timeout_returns_error_when_over_budget() {
        let pool = pool_with_query_timeout(Duration::from_millis(20));
        let result: Result<()> = pool
            .with_timeout("slow_op", || async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(())
            })
            .await;
        let err = result.expect_err("must time out");
        assert!(
            err.to_string().contains("slow_op"),
            "error message should name the operation: {err}"
        );
        assert!(
            err.to_string().contains("timed out"),
            "error message should say it timed out: {err}"
        );
    }

    #[tokio::test]
    async fn test_with_timeout_propagates_inner_error() {
        let pool = pool_with_query_timeout(Duration::from_secs(5));
        let result: Result<()> = pool
            .with_timeout("erroring_op", || async { Err(anyhow!("inner failure")) })
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("inner failure"));
    }
}
