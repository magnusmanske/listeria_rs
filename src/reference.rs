use crate::listeria_list::ListeriaList;
use serde::{Deserialize, Serialize};
use wikimisc::wikibase::Snak;
use wikimisc::wikibase::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Reference {
    url: Option<String>,
    title: Option<String>,
    date: Option<String>,
    stated_in: Option<String>, // Item
    md5: String,
    wikitext_cache: Option<String>,
}

impl PartialEq for Reference {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
            && self.title == other.title
            && self.date == other.date
            && self.stated_in == other.stated_in
    }
}

impl Reference {
    /// Creates a new reference from a snak array
    pub fn new_from_snaks(snaks: &[Snak], language: &str) -> Option<Self> {
        let mut ret = Self {
            ..Default::default()
        };

        for snak in snaks.iter() {
            match snak.property() {
                "P854" => Self::extract_reference_url(snak, &mut ret),
                "P1476" => Self::extract_title(snak, language, &mut ret),
                "P813" => Self::extract_timestamp(snak, &mut ret),
                "P248" => Self::extract_stated_in(snak, &mut ret),
                _ => {}
            }
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }

    pub fn stated_in(&self) -> &Option<String> {
        &self.stated_in
    }

    /// Returns true if the reference is empty
    fn is_empty(&self) -> bool {
        self.url.is_none() && self.stated_in.is_none()
    }

    /// Returns the reference as a wikitext string
    pub fn as_reference(&mut self, list: &ListeriaList) -> String {
        let wikitext = self.as_wikitext(list);
        let has_md5 = list.reference_ids().get(&self.md5).is_some();

        if has_md5 {
            format!("<ref name=\"ref_{}\" />", &self.md5)
        } else {
            format!("<ref name=\"ref_{}\">{}</ref>", &self.md5, &wikitext)
        }
    }

    /// Returns the wikitext representation of the reference
    fn as_wikitext(&mut self, list: &ListeriaList) -> String {
        let mut iterations_left: usize = 100; // Paranoia
        while iterations_left > 0 {
            iterations_left -= 1;
            if let Some(s) = &self.wikitext_cache {
                return s.to_string();
            }
            let mut s = String::new();

            let (use_invoke, use_cite_web) = match list.get_wiki() {
                Some(wiki) => (wiki.use_invoke(), wiki.use_cite_web()),
                None => (true, true), // Fallback for unknown wiki, should be fixed manually in DB
            };

            if use_cite_web && self.title.is_some() && self.url.is_some() {
                s = self.render_cite_web(use_invoke, list);
            } else if self.url.is_some() {
                if let Some(x) = self.url.as_ref() {
                    s += x;
                }
            } else if self.stated_in.is_some() {
                match &self.stated_in {
                    Some(q) => {
                        s += &list.get_item_link_with_fallback(q);
                    }
                    None => {}
                }
            }

            self.md5 = format!("{:x}", md5::compute(s.clone()));
            self.wikitext_cache = Some(s);
        }
        "Error: Could not generate reference wikitext, too many iterations".to_string()
    }

    fn render_cite_web(&self, use_invoke: bool, list: &ListeriaList) -> String {
        let template = if use_invoke { "{{#invoke:" } else { "{{" };
        let mut ret = format!(
            "{template}cite web|url={}|title={}",
            self.url.as_ref().unwrap_or(&String::new()),
            self.title.as_ref().unwrap_or(&String::new())
        );
        if let Some(stated_in) = &self.stated_in {
            ret += &format!("|website={}", list.get_item_link_with_fallback(stated_in));
        }
        if let Some(date) = &self.date {
            ret += &format!("|access-date={}", &date);
        }
        ret += "}}";
        ret
    }

    /// Extracts the stated_in info from a snak
    fn extract_stated_in(snak: &Snak, ret: &mut Reference) {
        // Stated in
        if let Some(dv) = snak.data_value() {
            if let Value::Entity(item) = dv.value() {
                ret.stated_in = Some(item.id().to_owned());
            }
        }
    }

    /// Extracts the timestamp from a snak
    fn extract_timestamp(snak: &Snak, ret: &mut Reference) {
        // Timestamp/last access
        if let Some(dv) = snak.data_value() {
            if let Value::Time(tv) = dv.value() {
                if let Some(pos) = tv.time().find('T') {
                    let (date, _) = tv.time().split_at(pos);
                    let mut date = date.replace('+', "").to_string();
                    if *tv.precision() >= 11 { // Day
                         // Keep
                    } else if *tv.precision() == 10 {
                        // Month
                        if let Some(pos) = date.rfind('-') {
                            date = date.split_at(pos).0.to_string();
                        }
                    } else if *tv.precision() <= 9 {
                        // Year etc TODO century etc
                        if let Some(pos) = date.find('-') {
                            date = date.split_at(pos).0.to_string();
                        }
                    }
                    ret.date = Some(date);
                }
            }
        }
    }

    /// Extracts the title from a snak
    fn extract_title(snak: &Snak, language: &str, ret: &mut Reference) {
        // Title
        if let Some(dv) = snak.data_value() {
            if let Value::MonoLingual(mlt) = dv.value() {
                if mlt.language() == language {
                    ret.title = Some(mlt.text().to_owned());
                }
            }
        }
    }

    /// Extracts the reference URL from a snak
    fn extract_reference_url(snak: &Snak, ret: &mut Reference) {
        // Reference URL
        if let Some(dv) = snak.data_value() {
            if let Value::StringValue(url) = dv.value() {
                ret.url = Some(url.to_owned());
            }
        }
    }
}
