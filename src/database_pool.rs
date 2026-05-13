//! MySQL database connection pooling.

use crate::configuration::Configuration;
use anyhow::{Result, anyhow};
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DatabasePool {
    pool: mysql_async::Pool,
    query_timeout: Duration,
}

impl DatabasePool {
    /// Creates a new connection pool from configuration settings.
    pub fn new(config: &Configuration) -> Result<Self> {
        Ok(Self {
            pool: Pool::new(Self::pool_opts_from_config(config)?),
            query_timeout: config.db_query_timeout(),
        })
    }

    /// Gets a database connection from the pool.
    pub async fn get_conn(&self) -> Result<Conn> {
        let ret = self.pool.get_conn().await?;
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

    // Use toolforge crate, does not work right now
    // fn pool_opts_from_toolforge(config: &Configuration) -> Result<String> {
    //     let schema = config
    //         .mysql("schema")
    //         .as_str()
    //         .ok_or(anyhow!("No schema in config"))?
    //         .to_string();
    //     Ok(toolforge::db::toolsdb(schema)?.to_string())
    // }

    fn pool_opts_from_config(config: &Configuration) -> Result<OptsBuilder> {
        let host = config
            .mysql("host")
            .as_str()
            .ok_or(anyhow!("No host in config"))?
            .to_string();
        let schema = config
            .mysql("schema")
            .as_str()
            .ok_or(anyhow!("No schema in config"))?
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
        let max_connections = config
            .mysql("max_connections")
            .as_u64()
            .and_then(|u| u.try_into().ok())
            .unwrap_or(8);
        let constraints = PoolConstraints::new(0, max_connections)
            .ok_or(anyhow!("Could not get pool constraints"))?;
        let pool_opts = PoolOpts::default().with_constraints(constraints);
        // .with_inactive_connection_ttl(Duration::from_secs(60));

        let opts = OptsBuilder::default()
            .ip_or_hostname(host.to_owned())
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(password))
            .tcp_port(port)
            .pool_opts(pool_opts);

        Ok(opts)
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
            pool: Pool::new(
                OptsBuilder::default()
                    .ip_or_hostname("127.0.0.1")
                    .tcp_port(1) // unreachable
                    .db_name(Some("_test_"))
                    .user(Some("_test_"))
                    .pass(Some("_test_")),
            ),
            query_timeout: timeout,
        }
    }

    #[tokio::test]
    async fn test_with_timeout_returns_success_when_under_budget() {
        let pool = pool_with_query_timeout(Duration::from_secs(5));
        let result: Result<i32> = pool
            .with_timeout("noop", || async { Ok(42) })
            .await;
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
            .with_timeout("erroring_op", || async {
                Err(anyhow!("inner failure"))
            })
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("inner failure"));
    }
}
