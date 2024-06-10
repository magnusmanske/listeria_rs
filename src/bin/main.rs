extern crate config;
extern crate serde_json;

use anyhow::{anyhow, Result};
use listeria::configuration::Configuration;
use listeria::listeria_page::ListeriaPage;
use listeria::wiki_apis::WikiApis;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn update_page(
    config: Arc<Configuration>,
    page_title: &str,
    api_url: &str,
) -> Result<String> {
    let mut mw_api = wikimisc::mediawiki::api::Api::new(api_url).await?;
    mw_api.set_oauth2(config.oauth2_token());

    let mw_api = Arc::new(RwLock::new(mw_api));
    let mut page = ListeriaPage::new(config, mw_api, page_title.into()).await?;
    page.run().await.map_err(|e| anyhow!("{e:?}"))?;

    println!("{:?}", page.as_wikitext());

    Ok(
        match page
            .update_source_page()
            .await
            .map_err(|e| anyhow!("{e:?}"))?
        {
            true => format!("{page_title} edited"),
            false => format!("{page_title} not edited"),
        },
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_file = args
        .get(3)
        .map(|s| s.to_owned())
        .unwrap_or("config.json".to_string());

    let mut config = Configuration::new_from_file(&config_file).await.unwrap();
    config.set_profiling(true); // Force profiling on for manual single job

    let first_arg = args
        .get(1)
        .ok_or_else(|| anyhow!("No wiki server argument"))?;

    if first_arg == "update_wikis" {
        let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());
        let wiki_list = WikiApis::new(config.clone()).await?;
        wiki_list.update_wiki_list_in_database().await?;
        wiki_list.update_all_wikis().await?;
        return Ok(());
    }

    let wiki_server = first_arg;
    let page = args.get(2).ok_or_else(|| anyhow!("No page argument"))?;

    let wiki_api = format!("https://{}/w/api.php", &wiki_server);
    let message = match update_page(Arc::new(config), page, &wiki_api).await {
        Ok(m) => format!("OK: {}", m),
        Err(e) => format!("ERROR: {}", e),
    };
    println!("{}", message);
    Ok(())
}

/*
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
*/
