extern crate config;
extern crate mediawiki;
//#[macro_use]
extern crate serde_json;

use config::{Config, File};
use listeria::*;

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
    let mut page = match ListeriaPage::new(&mw_api, "Benutzer:Magnus_Manske/listeria_test2".into())
    {
        Some(p) => p,
        None => panic!("Could not open/parse page"),
    };
    page.run_query().unwrap();
}
