use crate::configuration::Configuration;
use anyhow::{Result,anyhow};
use mysql_async::{Conn, OptsBuilder, Pool};

#[derive(Debug, Clone)]
pub struct DatabasePool {
    pool: mysql_async::Pool,
}

impl DatabasePool {
    pub fn new(config: &Configuration) -> Result<Self> {
        let opts = Self::pool_opts_from_config(&config)?;
        Ok(Self { pool: Pool::new(opts) } )
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
        let port = config.mysql("port").as_u64().ok_or(anyhow!("No port in config"))? as u16;
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

        let opts = OptsBuilder::default()
            .ip_or_hostname(host.to_owned())
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(password))
            .tcp_port(port);

        Ok(opts)
    }

    pub async fn destruct(&mut self) {
        //self.pool.disconnect().await; // TODO
    }
}