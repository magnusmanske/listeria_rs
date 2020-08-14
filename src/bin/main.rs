extern crate config;
extern crate serde_json;

use std::sync::Arc;
use config::{Config, File};
use listeria;
use crate::listeria::listeria_page::ListeriaPage;
use crate::listeria::Configuration;

async fn update_page(settings:&Config,page_title:&str,api_url:&str) {
    let user = settings.get_str("user.user").expect("No user name");
    let pass = settings.get_str("user.pass").expect("No user pass");

    let config = Arc::new(Configuration::new_from_file("config.json").unwrap());

    let mut mw_api = wikibase::mediawiki::api::Api::new(api_url)
        .await
        .expect("Could not connect to MW API");
    mw_api
        .login(user.to_owned(), pass.to_owned())
        .await
        .expect("Could not log in");
    let mw_api = Arc::new(mw_api);
    
    /*
    let wb_api = Arc::new(wikibase::mediawiki::api::Api::new("https://www.wikidata.org/w/api.php")
            .await
            .expect("Could not connect to MW API"));
    */

    /*
    let mut commons_api =
        wikibase::mediawiki::api::Api::new("https://commons.wikimedia.org/w/api.php")
            .await
            .expect("Could not connect to Commons API");
    commons_api
        .login(user.to_owned(), pass.to_owned())
        .await
        .expect("Could not log in");
    */

    let mut page = match ListeriaPage::new(config, mw_api, page_title.into()).await {
        Ok(p) => p,
        Err(e) => panic!("Could not open/parse page '{}': {}", &page_title,e),
    };
    page.do_simulate(Some("
{{Wikidata list
|sparql=SELECT ?item { VALUES ?item { wd:Q17 } }
|columns=label:name,P41
|summary=itemnumber
}}
{{Wikidata list end}}".to_string()),None);

    match page.run().await {
        Ok(_) => {}
        Err(e) => panic!("{}", e),
    }
    let wt = page.as_wikitext().unwrap();
    println!("{:?}",wt);
    //let j = page.as_tabbed_data().unwrap();
    //page.write_tabbed_data(j, &mut commons_api).unwrap();

    //page.update_source_page().await.unwrap();
    // TODO update source wiki text (if necessary), or action=purge to update
}

#[tokio::main]
async fn main() {
    let ini_file = "listeria.ini";

    let mut settings = Config::default();
    settings
        .merge(File::with_name(ini_file))
        .expect(format!("INI file '{}' can't be opened", ini_file).as_str());

    update_page(&settings,
        "User:Magnus Manske/listeria test5",
        "https://en.wikipedia.org/w/api.php"
        ).await;
}
