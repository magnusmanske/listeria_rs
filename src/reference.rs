use crate::listeria_list::ListeriaList;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;
use wikimisc::wikibase::Snak;
use wikimisc::wikibase::Value;

mod arc_rwlock_serde {
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, RwLock};

    pub fn serialize<S, T>(val: &Arc<RwLock<T>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        if let Ok(val) = val.read() {
            T::serialize(&*val, s)
        } else {
            Err(serde::ser::Error::custom("Could not read from RwLock"))
        }
    }

    pub fn deserialize<'de, D, T>(d: D) -> Result<Arc<RwLock<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Arc::new(RwLock::new(T::deserialize(d)?)))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Reference {
    pub url: Option<String>,
    pub title: Option<String>,
    pub date: Option<String>,
    pub stated_in: Option<String>, // Item
    #[serde(with = "arc_rwlock_serde")]
    md5: Arc<RwLock<String>>,
    #[serde(with = "arc_rwlock_serde")]
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

    /// Returns true if the reference is empty
    fn is_empty(&self) -> bool {
        self.url.is_none() && self.stated_in.is_none()
    }

    /// Returns the reference as a wikitext string
    pub fn as_reference(&self, list: &ListeriaList) -> String {
        let wikitext = self.as_wikitext(list);
        let md5 = match self.md5.read() {
            Ok(s) => s.to_string(),
            _ => return String::new(),
        };
        let has_md5 = list.reference_ids().get(&md5).is_some();

        if has_md5 {
            format!("<ref name=\"ref_{}\" />", &md5)
        } else {
            format!("<ref name=\"ref_{}\">{}</ref>", &md5, &wikitext)
        }
    }

    /// Returns the wikitext representation of the reference
    fn as_wikitext(&self, list: &ListeriaList) -> String {
        loop {
            // TODO FIXME check that this loop does not run forever
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
                    "{{{{#invoke:cite web|url={}|title={}",
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
                        s += &list.get_item_link_with_fallback(q);
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
