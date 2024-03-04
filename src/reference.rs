use crate::listeria_list::ListeriaList;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug, Clone, Default)]
pub struct Reference {
    pub url: Option<String>,
    pub title: Option<String>,
    pub date: Option<String>,
    pub stated_in: Option<String>, // Item
    md5: Arc<RwLock<String>>,
    wikitext_cache: Arc<RwLock<Option<String>>>,
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
    pub fn new_from_snaks(snaks: &[wikibase::snak::Snak], language: &str) -> Option<Self> {
        let mut ret = Self {
            ..Default::default()
        };

        for snak in snaks.iter() {
            match snak.property() {
                "P854" => {
                    // Reference URL
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::StringValue(url) = dv.value() {
                            ret.url = Some(url.to_owned());
                        }
                    }
                }
                "P1476" => {
                    // Title
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::MonoLingual(mlt) = dv.value() {
                            if mlt.language() == language {
                                ret.title = Some(mlt.text().to_owned());
                            }
                        }
                    }
                }
                "P813" => {
                    // Timestamp/last access
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::Time(tv) = dv.value() {
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
                "P248" => {
                    // Stated in
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::Entity(item) = dv.value() {
                            ret.stated_in = Some(item.id().to_owned());
                        }
                    }
                }
                _ => {}
            }
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }

    fn is_empty(&self) -> bool {
        self.url.is_none() && self.stated_in.is_none()
    }

    pub fn as_reference(&self, list: &ListeriaList) -> String {
        let wikitext = self.as_wikitext(list);
        let md5 = match self.md5.read() {
            Ok(s) => s.to_string(),
            _ => return String::new(),
        };
        let has_md5 = list.reference_ids().get(&md5).is_some();

        if has_md5 {
            format!("<ref name='ref_{}' />", &md5)
        } else {
            format!("<ref name='ref_{}'>{}</ref>", &md5, &wikitext)
        }
    }

    fn as_wikitext(&self, list: &ListeriaList) -> String {
        loop { // TODO FIXME check that this loop does not run forever
            match self.wikitext_cache.read() {
                Ok(cache) => {
                    if let Some(s) = &*cache {
                        return s.to_string();
                    }
                }
                _ => return String::new(), // No error
            }
            let mut s = String::new();

            if self.title.is_some() && self.url.is_some() {
                s += &format!(
                    "{{{{cite web|url={}|title={}",
                    self.url.as_ref().unwrap_or(&String::new()),
                    self.title.as_ref().unwrap_or(&String::new())
                );
                if let Some(stated_in) = &self.stated_in {
                    s += &format!("|website={}", list.get_item_link_with_fallback(stated_in));
                }
                if let Some(date) = &self.date {
                    s += &format!("|access-date={}", &date);
                }
                s += "}}";
            } else if self.url.is_some() {
                if let Some(x) = self.url.as_ref() {
                    s += x;
                }
            } else if self.stated_in.is_some() {
                match &self.stated_in {
                    Some(q) => {
                        s += &list.get_item_link_with_fallback(&q);
                    }
                    None => {}
                }
            }

            match self.md5.write() {
                Ok(mut md5) => {
                    *md5 = format!("{:x}", md5::compute(s.clone()));
                }
                _ => return String::new(), // No error
            }

            match self.wikitext_cache.write() {
                Ok(mut cache) => {
                    *cache = Some(s);
                }
                _ => return String::new(), // No error
            }
        }
    }
}
