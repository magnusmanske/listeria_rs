use crate::configuration::Configuration;
use anyhow::{anyhow, Result};
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts};

#[derive(Debug, Clone)]
pub struct DatabasePool {
    pool: mysql_async::Pool,
}

impl DatabasePool {
    pub fn new(config: &Configuration) -> Result<Self> {
        let opts = Self::pool_opts_from_config(&config)?;
        Ok(Self {
            pool: Pool::new(opts),
        })
    }

    pub async fn get_conn(&self) -> Result<Conn> {
        let ret = self.pool.get_conn().await?;
        Ok(ret)
    }

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
            .ok_or(anyhow!("No port in config"))? as u16;
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
        let max_connections = config.mysql("max_connections").as_u64().unwrap_or(8) as usize;
        let constraints = PoolConstraints::new(0, max_connections).unwrap();
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

    pub async fn destruct(&mut self) {
        //self.pool.disconnect().await; // TODO
    }
}
