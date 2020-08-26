extern crate config;
extern crate serde_json;

use std::collections::HashMap;
use listeria::Renderer;
use crate::listeria::render_wikitext::RendererWikitext;
use tokio::sync::RwLock;
use std::sync::Arc;
use config::{Config, File};
use listeria;
use crate::listeria::listeria_page::ListeriaPage;
use crate::listeria::configuration::Configuration;
use wikibase::mediawiki::api::Api;
use mysql_async::prelude::*;
use mysql_async as my;

// ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N

pub struct ListeriaBot {
    settings: Arc<Config>,
    config: Arc<Configuration>,
    wikis: HashMap<String,Arc<RwLock<Api>>>,
    pool: mysql_async::Pool,
}
impl ListeriaBot {
    pub async fn new(ini_file: &str, config_file: &str) -> Result<Self,String> {
        let mut settings = Config::default();
        settings
            .merge(File::with_name(ini_file))
            .unwrap_or_else(|_| panic!("INI file '{}' can't be opened", ini_file));
        let config = Configuration::new_from_file(config_file).await?;

        let host = config.mysql("host").as_str().ok_or("No host in config")?.to_string();
        let schema = config.mysql("schema").as_str().ok_or("No schema in config")?.to_string();
        let port = config.mysql("port").as_u64().ok_or("No port in config")? as u16;
        let user = config.mysql("user").as_str().ok_or("No user in config")?.to_string();
        let password = config.mysql("password").as_str().ok_or("No password in config")?.to_string();

        let opts = my::OptsBuilder::default()
            .ip_or_hostname(host.to_owned())
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(password))
            .tcp_port(port);

        let ret = Self {
            settings: Arc::new(settings),
            config: Arc::new(config),
            wikis: HashMap::new(),
            pool: mysql_async::Pool::new(opts),
        };

        let mut conn = ret.pool.get_conn().await.expect("Can't connect to database");

        let test1 : Vec<my::Row> = conn.exec_iter(
            "SELECT `id`,`wiki`,`page`,`status` from pagestatus WHERE status!='RUNNING' order by `timestamp` DESC LIMIT 1",
            ()
        ).await
        .map_err(|e|format!("PageList::run_batch_query: SQL query error[1]: {:?}",e))?
        .collect_and_drop()
        .await
        .map_err(|e|format!("PageList::run_batch_query: SQL query error[2]: {:?}",e))?;
        
        conn.disconnect().await.map_err(|e|format!("{:?}",e))?;

        println!("{:?}",test1);

        Ok(ret)
    }

    pub async fn destruct(&mut self) {
        //self.pool.disconnect().await.unwrap(); // TODO
    }

}

#[tokio::main]
async fn main() {
    let bot = ListeriaBot::new("listeria.ini","config.json").await.unwrap();

    /*
    let mut mw_api = wikibase::mediawiki::api::Api::new(api_url)
        .await
        .expect("Could not connect to MW API");
    let mw_api = Arc::new(RwLock::new(mw_api));
    */

}
