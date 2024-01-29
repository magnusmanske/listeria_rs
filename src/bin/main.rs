extern crate config;
extern crate serde_json;

use anyhow::{Result,anyhow};
use listeria::configuration::Configuration;
use listeria::listeria_page::ListeriaPage;
use listeria::wiki_apis::WikiApis;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn update_page(wiki_apis: Arc<WikiApis>, page_title: &str, api_url: &str) -> Result<String> {
    let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());

    let mut mw_api = wikibase::mediawiki::api::Api::new(api_url).await?;
    mw_api.set_oauth2(config.oauth2_token());
    let mw_api = Arc::new(RwLock::new(mw_api));
    let mut page = ListeriaPage::new(wiki_apis, mw_api, page_title.into()).await?;
    page.run().await.map_err(|e|anyhow!("{e:?}"))?;

    Ok(match page.update_source_page().await.map_err(|e|anyhow!("{e:?}"))? {
        true => format!("{page_title} edited"),
        false => format!("{page_title} not edited"),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let first_arg = args
        .get(1)
        .ok_or_else(|| anyhow!("No wiki server argument"))?;

    if first_arg=="update_wikis" {
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let wiki_apis = WikiApis::new(config.clone()).await?;
        wiki_apis.update_wiki_list_in_database().await?;
        wiki_apis.update_all_wikis().await?;
        return Ok(())
    }

    let wiki_server = first_arg;
    let page = args.get(2).ok_or_else(|| anyhow!("No page argument"))?;

    let wiki_api = format!("https://{}/w/api.php", &wiki_server);
    let config = Configuration::new_from_file("config.json").await.unwrap();
    let wiki_apis = Arc::new(WikiApis::new(config.to_owned()).await?);
    let message = match update_page(wiki_apis, &page, &wiki_api).await {
        Ok(m) => format!("OK: {}", m),
        Err(e) => format!("ERROR: {}", e),
    };
    println!("{}", message);
    Ok(())
}

/*
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
*/
