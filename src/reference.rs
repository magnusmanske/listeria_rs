//! Wikidata reference handling and formatting.

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
    #[must_use]
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

        if ret.is_empty() { None } else { Some(ret) }
    }

    #[must_use]
    pub const fn stated_in(&self) -> &Option<String> {
        &self.stated_in
    }

    /// Returns true if the reference is empty
    const fn is_empty(&self) -> bool {
        self.url.is_none() && self.stated_in.is_none()
    }

    /// Returns the reference as a wikitext string
    pub async fn as_reference(&mut self, list: &ListeriaList) -> String {
        let wikitext = self.as_wikitext(list).await;
        let has_md5 = list.reference_ids().get(&self.md5).is_some();

        if has_md5 {
            format!("<ref name=\"ref_{}\" />", &self.md5)
        } else {
            format!("<ref name=\"ref_{}\">{}</ref>", &self.md5, &wikitext)
        }
    }

    /// Returns the wikitext representation of the reference
    async fn as_wikitext(&mut self, list: &ListeriaList) -> String {
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
                s = self.render_cite_web(use_invoke, list).await;
            } else if self.url.is_some() {
                if let Some(x) = self.url.as_ref() {
                    s += x;
                }
            } else if let Some(q) = &self.stated_in {
                s += &list.get_item_link_with_fallback(q).await;
            }

            self.md5 = format!("{:x}", md5::compute(&s));
            self.wikitext_cache = Some(s);
        }
        "Error: Could not generate reference wikitext, too many iterations".to_string()
    }

    async fn render_cite_web(&self, use_invoke: bool, list: &ListeriaList) -> String {
        let template = if use_invoke {
            "{{#invoke:cite|web"
        } else {
            "{{cite web"
        };
        let mut ret = format!(
            "{template}|url={}|title={}",
            self.url.as_ref().unwrap_or(&String::new()),
            self.title.as_ref().unwrap_or(&String::new())
        );
        if let Some(stated_in) = &self.stated_in {
            ret += &format!(
                "|website={}",
                list.get_item_link_with_fallback(stated_in).await
            );
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
        if let Some(dv) = snak.data_value()
            && let Value::Entity(item) = dv.value()
        {
            ret.stated_in = Some(item.id().to_owned());
        }
    }

    /// Extracts the timestamp from a snak
    fn extract_timestamp(snak: &Snak, ret: &mut Reference) {
        // Timestamp/last access
        if let Some(dv) = snak.data_value()
            && let Value::Time(tv) = dv.value()
            && let Some(pos_t) = tv.time().find('T')
        {
            let (date, _) = tv.time().split_at(pos_t);
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

    /// Extracts the title from a snak
    fn extract_title(snak: &Snak, language: &str, ret: &mut Reference) {
        // Title
        if let Some(dv) = snak.data_value()
            && let Value::MonoLingual(mlt) = dv.value()
            && mlt.language() == language
        {
            ret.title = Some(mlt.text().to_owned());
        }
    }

    /// Extracts the reference URL from a snak
    fn extract_reference_url(snak: &Snak, ret: &mut Reference) {
        // Reference URL
        if let Some(dv) = snak.data_value()
            && let Value::StringValue(url) = dv.value()
        {
            ret.url = Some(url.to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reference_default_is_empty() {
        let reference = Reference::default();
        assert!(reference.is_empty());
    }

    #[test]
    fn test_reference_with_url_not_empty() {
        let reference = Reference {
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        assert!(!reference.is_empty());
    }

    #[test]
    fn test_reference_with_stated_in_not_empty() {
        let reference = Reference {
            stated_in: Some("Q123".to_string()),
            ..Default::default()
        };
        assert!(!reference.is_empty());
    }

    #[test]
    fn test_reference_with_only_title_is_empty() {
        let reference = Reference {
            title: Some("Test Title".to_string()),
            ..Default::default()
        };
        assert!(reference.is_empty());
    }

    #[test]
    fn test_reference_with_only_date_is_empty() {
        let reference = Reference {
            date: Some("2025-01-01".to_string()),
            ..Default::default()
        };
        assert!(reference.is_empty());
    }

    #[test]
    fn test_reference_equality_same_references() {
        let ref1 = Reference {
            url: Some("https://example.com".to_string()),
            title: Some("Example".to_string()),
            date: Some("2025-01-01".to_string()),
            stated_in: Some("Q123".to_string()),
            md5: "abc123".to_string(),
            wikitext_cache: Some("cached".to_string()),
        };
        let ref2 = Reference {
            url: Some("https://example.com".to_string()),
            title: Some("Example".to_string()),
            date: Some("2025-01-01".to_string()),
            stated_in: Some("Q123".to_string()),
            md5: "different".to_string(), // MD5 ignored in equality
            wikitext_cache: Some("also_different".to_string()), // Cache ignored in equality
        };
        assert_eq!(ref1, ref2);
    }

    #[test]
    fn test_reference_equality_different_url() {
        let ref1 = Reference {
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        let ref2 = Reference {
            url: Some("https://different.com".to_string()),
            ..Default::default()
        };
        assert_ne!(ref1, ref2);
    }

    #[test]
    fn test_reference_equality_different_title() {
        let ref1 = Reference {
            title: Some("Title 1".to_string()),
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        let ref2 = Reference {
            title: Some("Title 2".to_string()),
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        assert_ne!(ref1, ref2);
    }

    #[test]
    fn test_reference_equality_different_date() {
        let ref1 = Reference {
            date: Some("2025-01-01".to_string()),
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        let ref2 = Reference {
            date: Some("2025-01-02".to_string()),
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        assert_ne!(ref1, ref2);
    }

    #[test]
    fn test_reference_equality_different_stated_in() {
        let ref1 = Reference {
            stated_in: Some("Q123".to_string()),
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        let ref2 = Reference {
            stated_in: Some("Q456".to_string()),
            url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        assert_ne!(ref1, ref2);
    }

    #[test]
    fn test_stated_in_getter() {
        let reference = Reference {
            stated_in: Some("Q42".to_string()),
            ..Default::default()
        };
        assert_eq!(reference.stated_in(), &Some("Q42".to_string()));
    }

    #[test]
    fn test_stated_in_getter_none() {
        let reference = Reference::default();
        assert_eq!(reference.stated_in(), &None);
    }
}
