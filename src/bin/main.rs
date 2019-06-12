extern crate config;
extern crate mediawiki;
//#[macro_use]
extern crate serde_json;

use config::{Config, File};
use listeria::*;

fn main() {
    let ini_file = "bot.ini";
    let page_title = "User:Magnus Manske/listeria test4"; //"Benutzer:Magnus_Manske/listeria_test2";
    let api_url = "https://en.wikipedia.org/w/api.php";

    let mut settings = Config::default();
    settings
        .merge(File::with_name(ini_file))
        .expect(format!("Replica file '{}' can't be opened", ini_file).as_str());
    let user = settings.get_str("user.user").expect("No user name");
    let pass = settings.get_str("user.pass").expect("No user pass");

    let mut mw_api = mediawiki::api::Api::new(api_url).expect("Could not connect to MW API");
    mw_api.login(user, pass).expect("Could not log in");

    //println!("{:?}", mw_api.get_site_info());
    let _page = match ListeriaPage::new(&mw_api, page_title.into()) {
        Some(p) => p,
        None => panic!("Could not open/parse page '{}'", &page_title),
    };
    //page.run_query().unwrap();
}
