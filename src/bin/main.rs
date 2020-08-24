extern crate config;
extern crate serde_json;

use listeria::Renderer;
use listeria::RendererWikitext;
use tokio::sync::Mutex;
use std::sync::Arc;
use config::{Config, File};
use listeria;
use crate::listeria::listeria_page::ListeriaPage;
use crate::listeria::Configuration;

async fn update_page(settings:&Config,page_title:&str,api_url:&str) {
    let user = settings.get_str("user.user").expect("No user name");
    let pass = settings.get_str("user.pass").expect("No user pass");

    let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());

    let mut mw_api = wikibase::mediawiki::api::Api::new(api_url)
        .await
        .expect("Could not connect to MW API");
    mw_api
        .login(user.to_owned(), pass.to_owned())
        .await
        .expect("Could not log in");
    let mw_api = Arc::new(Mutex::new(mw_api));
    let mut page = match ListeriaPage::new(config, mw_api, page_title.into()).await {
        Ok(p) => p,
        Err(e) => panic!("Could not open/parse page '{}': {}", &page_title,e),
    };
    match page.run().await {
        Ok(_) => {}
        Err(e) => panic!("{}", e),
    }
    let renderer = RendererWikitext::new();
    let old_wikitext = page.load_page_as("wikitext").await.expect("FAILED load page as wikitext");
    let new_wikitext = renderer.get_new_wikitext(&old_wikitext,&page).unwrap().unwrap();
    println!("{:?}",&new_wikitext);
    /*
    match page.update_source_page().await.expect("update failed") {
        true => println!("{} edited",&page_title),
        false => println!("{} not edited",&page_title),
    }
    */


    //let j = page.as_tabbed_data().unwrap();
    //page.write_tabbed_data(j, &mut commons_api).unwrap();
    //page.update_source_page().await.unwrap();
}

#[tokio::main]
async fn main() {
    let ini_file = "listeria.ini";

    let mut settings = Config::default();
    settings
        .merge(File::with_name(ini_file))
        .unwrap_or_else(|_| panic!("INI file '{}' can't be opened", ini_file));

    update_page(&settings,
        "User:Magnus Manske/listeria test6",
        "https://en.wikipedia.org/w/api.php"
        ).await;
}
