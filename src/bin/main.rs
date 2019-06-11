extern crate config;
extern crate mediawiki;
//#[macro_use]
extern crate serde_json;

use config::{Config, File};
use roxmltree;
use std::collections::HashMap;

pub struct ListeriaPage {
    mw_api: mediawiki::api::Api,
    page: String,
}

impl ListeriaPage {
    pub fn new(mw_api: &mediawiki::api::Api, page: String) -> Self {
        Self {
            mw_api: mw_api.clone(),
            page: page,
        }
    }

    pub fn load_page(&self) -> Result<(), String> {
        let params: HashMap<String, String> = vec![
            ("action", "parse"),
            ("prop", "parsetree"),
            ("page", self.page.as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();

        let result = self
            .mw_api
            .get_query_api_json(&params)
            .expect("Loading page failed");
        let doc = match result["parse"]["parsetree"]["*"].as_str() {
            Some(text) => roxmltree::Document::parse(&text).unwrap(),
            None => return Err(format!("No parse tree for {}", &self.page)),
        };
        println!("{:?}", &doc);
        Ok(())
    }
}

fn main() {
    let ini_file = "bot.ini";
    let mut settings = Config::default();
    settings
        .merge(File::with_name(ini_file))
        .expect(format!("Replica file '{}' can't be opened", ini_file).as_str());
    let user = settings.get_str("user.user").expect("No user name");
    let pass = settings.get_str("user.pass").expect("No user pass");

    let mut mw_api = mediawiki::api::Api::new("https://de.wikipedia.org/w/api.php")
        .expect("Could not connect to MW API");
    mw_api.login(user, pass).expect("Could not log in");

    //println!("{:?}", mw_api.get_site_info());
    let page = ListeriaPage::new(&mw_api, "Benutzer:Magnus_Manske/listeria_test2".into());
    page.load_page().expect("Page load failed");
}
