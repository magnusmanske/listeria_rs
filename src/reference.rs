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

    /// Returns the reference as a wikitext string.
    pub async fn as_reference(&self, list: &ListeriaList) -> String {
        let (wikitext, md5) = self.compute_wikitext(list).await;
        let has_md5 = list.reference_ids().get(&md5).is_some();
        if has_md5 {
            format!("<ref name=\"ref_{md5}\" />")
        } else {
            format!("<ref name=\"ref_{md5}\">{wikitext}</ref>")
        }
    }

    /// Computes the wikitext and a blake3 hash for this reference.
    /// Returns `(wikitext, hash)`. Results are not cached; callers run this once per render.
    async fn compute_wikitext(&self, list: &ListeriaList) -> (String, String) {
        let mut s = String::new();

        let (use_invoke, use_cite_web) = match list.get_wiki() {
            Some(wiki) => (wiki.use_invoke(), wiki.use_cite_web()),
            None => (true, true), // Fallback for unknown wiki, should be fixed manually in DB
        };

        if use_cite_web && self.title.is_some() && self.url.is_some() {
            s = self.render_cite_web(use_invoke, list).await;
        } else if let Some(url) = &self.url {
            s += url;
        } else if let Some(q) = &self.stated_in {
            s += &list.get_item_link_with_fallback(q).await;
        }

        let hash = blake3::hash(s.as_bytes()).to_hex().to_string();
        (s, hash)
    }

    async fn render_cite_web(&self, use_invoke: bool, list: &ListeriaList) -> String {
        let template = if use_invoke {
            "{{#invoke:cite|web"
        } else {
            "{{cite web"
        };
        let mut ret = format!(
            "{template}|url={}|title={}",
            self.url.as_deref().unwrap_or(""),
            self.title.as_deref().unwrap_or("")
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

            // Extract year for further processing.
            // Handle negative years (e.g. "-0099-01-01") by skipping the leading '-'.
            let year = if let Some(rest) = date.strip_prefix('-') {
                if let Some(pos) = rest.find('-') {
                    format!("-{}", &rest[..pos]).parse::<i32>().ok()
                } else {
                    format!("-{rest}").parse::<i32>().ok()
                }
            } else if let Some(pos) = date.find('-') {
                date.split_at(pos).0.parse::<i32>().ok()
            } else {
                date.parse::<i32>().ok()
            };

            if *tv.precision() >= 11 {
                // Day precision - keep full date
            } else if *tv.precision() == 10 {
                // Month precision - remove day
                if let Some(pos) = date.rfind('-') {
                    date = date.split_at(pos).0.to_string();
                }
            } else if *tv.precision() == 9 {
                // Year precision - keep only year (handle negative years)
                if date.starts_with('-') {
                    let date_rest = &date[1..];
                    if let Some(pos) = date_rest.find('-') {
                        date = format!("-{}", &date_rest[..pos]);
                    }
                } else if let Some(pos) = date.find('-') {
                    date = date.split_at(pos).0.to_string();
                }
            } else if *tv.precision() == 8 {
                // Decade precision (e.g., "1990s")
                if let Some(y) = year {
                    date = format!("{}0s", y / 10);
                }
            } else if *tv.precision() == 7 {
                // Century precision (e.g., "20th century")
                if let Some(y) = year {
                    let century = if y > 0 {
                        (y - 1) / 100 + 1
                    } else {
                        y / 100 - 1
                    };
                    date = format!("{} century", Self::ordinal_suffix(century));
                }
            } else if *tv.precision() == 6 {
                // Millennium precision (e.g., "3rd millennium")
                if let Some(y) = year {
                    let millennium = if y > 0 {
                        (y - 1) / 1000 + 1
                    } else {
                        y / 1000 - 1
                    };
                    date = format!("{} millennium", Self::ordinal_suffix(millennium));
                }
            } else {
                // Lower precision - just use year (handle negative years)
                if date.starts_with('-') {
                    let date_rest = &date[1..];
                    if let Some(pos) = date_rest.find('-') {
                        date = format!("-{}", &date_rest[..pos]);
                    }
                } else if let Some(pos) = date.find('-') {
                    date = date.split_at(pos).0.to_string();
                }
            }
            ret.date = Some(date);
        }
    }

    /// Converts a number to its ordinal form (e.g., 1 -> "1st", 21 -> "21st")
    fn ordinal_suffix(n: i32) -> String {
        let abs_n = n.abs();
        let suffix = if (abs_n % 100) >= 11 && (abs_n % 100) <= 13 {
            "th"
        } else {
            match abs_n % 10 {
                1 => "st",
                2 => "nd",
                3 => "rd",
                _ => "th",
            }
        };
        format!("{}{}", n, suffix)
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
        };
        let ref2 = Reference {
            url: Some("https://example.com".to_string()),
            title: Some("Example".to_string()),
            date: Some("2025-01-01".to_string()),
            stated_in: Some("Q123".to_string()),
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

    #[test]
    fn test_ordinal_suffix_basic() {
        assert_eq!(Reference::ordinal_suffix(1), "1st");
        assert_eq!(Reference::ordinal_suffix(2), "2nd");
        assert_eq!(Reference::ordinal_suffix(3), "3rd");
        assert_eq!(Reference::ordinal_suffix(4), "4th");
        assert_eq!(Reference::ordinal_suffix(5), "5th");
    }

    #[test]
    fn test_ordinal_suffix_teens() {
        assert_eq!(Reference::ordinal_suffix(11), "11th");
        assert_eq!(Reference::ordinal_suffix(12), "12th");
        assert_eq!(Reference::ordinal_suffix(13), "13th");
    }

    #[test]
    fn test_ordinal_suffix_twenties() {
        assert_eq!(Reference::ordinal_suffix(21), "21st");
        assert_eq!(Reference::ordinal_suffix(22), "22nd");
        assert_eq!(Reference::ordinal_suffix(23), "23rd");
        assert_eq!(Reference::ordinal_suffix(24), "24th");
    }

    #[test]
    fn test_ordinal_suffix_hundreds() {
        assert_eq!(Reference::ordinal_suffix(101), "101st");
        assert_eq!(Reference::ordinal_suffix(111), "111th");
        assert_eq!(Reference::ordinal_suffix(121), "121st");
    }

    #[test]
    fn test_ordinal_suffix_negative() {
        assert_eq!(Reference::ordinal_suffix(-1), "-1st");
        assert_eq!(Reference::ordinal_suffix(-2), "-2nd");
        assert_eq!(Reference::ordinal_suffix(-11), "-11th");
    }

    #[test]
    fn test_ordinal_suffix_zero() {
        assert_eq!(Reference::ordinal_suffix(0), "0th");
    }

    // --- extract_reference_url ---

    #[test]
    fn test_extract_reference_url() {
        let snak = Snak::new_string("P854", "https://example.com");
        let mut reference = Reference::default();
        Reference::extract_reference_url(&snak, &mut reference);
        assert_eq!(reference.url, Some("https://example.com".to_string()));
    }

    #[test]
    fn test_extract_reference_url_wrong_type() {
        // new_item creates an entity value, not a string — should not set URL
        let snak = Snak::new_item("P854", "Q42");
        let mut reference = Reference::default();
        Reference::extract_reference_url(&snak, &mut reference);
        assert_eq!(reference.url, None);
    }

    // --- extract_stated_in ---

    #[test]
    fn test_extract_stated_in() {
        let snak = Snak::new_item("P248", "Q36578");
        let mut reference = Reference::default();
        Reference::extract_stated_in(&snak, &mut reference);
        assert_eq!(reference.stated_in, Some("Q36578".to_string()));
    }

    #[test]
    fn test_extract_stated_in_wrong_type() {
        let snak = Snak::new_string("P248", "not an item");
        let mut reference = Reference::default();
        Reference::extract_stated_in(&snak, &mut reference);
        assert_eq!(reference.stated_in, None);
    }

    // --- extract_title ---

    #[test]
    fn test_extract_title_matching_language() {
        // Note: Snak::new_monolingual_text passes (language, value) to
        // MonoLingualText::new(text, language), so args are swapped internally.
        // We pass ("Test Title", "en") so that text="Test Title", language="en".
        let snak = Snak::new_monolingual_text("P1476", "Test Title", "en");
        let mut reference = Reference::default();
        Reference::extract_title(&snak, "en", &mut reference);
        assert_eq!(reference.title, Some("Test Title".to_string()));
    }

    #[test]
    fn test_extract_title_non_matching_language() {
        let snak = Snak::new_monolingual_text("P1476", "Testtitel", "de");
        let mut reference = Reference::default();
        Reference::extract_title(&snak, "en", &mut reference);
        assert_eq!(reference.title, None);
    }

    // --- extract_timestamp ---

    #[test]
    fn test_extract_timestamp_day_precision() {
        let snak = Snak::new_time("P813", "+2025-06-15T00:00:00Z", 11);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("2025-06-15".to_string()));
    }

    #[test]
    fn test_extract_timestamp_month_precision() {
        let snak = Snak::new_time("P813", "+2025-06-15T00:00:00Z", 10);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("2025-06".to_string()));
    }

    #[test]
    fn test_extract_timestamp_year_precision() {
        let snak = Snak::new_time("P813", "+2025-06-15T00:00:00Z", 9);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("2025".to_string()));
    }

    #[test]
    fn test_extract_timestamp_decade_precision() {
        let snak = Snak::new_time("P813", "+1990-01-01T00:00:00Z", 8);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("1990s".to_string()));
    }

    #[test]
    fn test_extract_timestamp_century_precision() {
        let snak = Snak::new_time("P813", "+1900-01-01T00:00:00Z", 7);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("19th century".to_string()));
    }

    #[test]
    fn test_extract_timestamp_millennium_precision() {
        let snak = Snak::new_time("P813", "+2000-01-01T00:00:00Z", 6);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("2nd millennium".to_string()));
    }

    // --- new_from_snaks ---

    #[test]
    fn test_new_from_snaks_with_url() {
        let snaks = vec![Snak::new_string("P854", "https://example.com")];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_some());
        let reference = reference.unwrap();
        assert_eq!(reference.url, Some("https://example.com".to_string()));
    }

    #[test]
    fn test_new_from_snaks_with_stated_in() {
        let snaks = vec![Snak::new_item("P248", "Q36578")];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_some());
        let reference = reference.unwrap();
        assert_eq!(reference.stated_in, Some("Q36578".to_string()));
    }

    #[test]
    fn test_new_from_snaks_empty() {
        let snaks: Vec<Snak> = vec![];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_none());
    }

    #[test]
    fn test_new_from_snaks_irrelevant_properties() {
        // P999 is not handled, so it should produce an empty (None) reference
        let snaks = vec![Snak::new_string("P999", "irrelevant")];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_none());
    }

    #[test]
    fn test_new_from_snaks_multiple() {
        let snaks = vec![
            Snak::new_string("P854", "https://example.com"),
            Snak::new_item("P248", "Q36578"),
            Snak::new_monolingual_text("P1476", "Test Title", "en"),
        ];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_some());
        let reference = reference.unwrap();
        assert_eq!(reference.url, Some("https://example.com".to_string()));
        assert_eq!(reference.stated_in, Some("Q36578".to_string()));
        assert_eq!(reference.title, Some("Test Title".to_string()));
    }

    // --- extract_timestamp edge cases ---

    #[test]
    fn test_extract_timestamp_precision_below_six_extracts_year() {
        // Precision 5 (or lower) hits the final else branch → year only
        let snak = Snak::new_time("P813", "+1234-06-15T00:00:00Z", 5);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("1234".to_string()));
    }

    #[test]
    fn test_extract_timestamp_precision_zero_extracts_year() {
        let snak = Snak::new_time("P813", "+2000-01-01T00:00:00Z", 0);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("2000".to_string()));
    }

    #[test]
    fn test_extract_timestamp_negative_year_century() {
        // Year -99 → century = -99/100 - 1 = -1, ordinal_suffix(-1) = "-1st"
        let snak = Snak::new_time("P813", "-0099-01-01T00:00:00Z", 7);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("-1st century".to_string()));
    }

    #[test]
    fn test_extract_timestamp_negative_year_millennium() {
        // Year -999 → millennium = -999/1000 - 1 = -1, ordinal_suffix(-1) = "-1st"
        let snak = Snak::new_time("P813", "-0999-01-01T00:00:00Z", 6);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("-1st millennium".to_string()));
    }

    #[test]
    fn test_extract_timestamp_decade_boundary() {
        // Year 1980 → 1980s (1980/10 = 198, 198*10 = 1980 → "1980s")
        let snak = Snak::new_time("P813", "+1980-06-01T00:00:00Z", 8);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("1980s".to_string()));
    }

    // --- new_from_snaks edge cases ---

    #[test]
    fn test_new_from_snaks_with_only_title_returns_none() {
        // Title alone (no URL, no stated_in) → is_empty() = true → None
        let snaks = vec![Snak::new_monolingual_text("P1476", "Some Title", "en")];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_none());
    }

    #[test]
    fn test_new_from_snaks_with_only_date_returns_none() {
        // Date alone (no URL, no stated_in) → is_empty() = true → None
        let snaks = vec![Snak::new_time("P813", "+2024-01-01T00:00:00Z", 11)];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_none());
    }

    #[test]
    fn test_new_from_snaks_url_and_date_returns_some() {
        // URL + date → is_empty() = false (URL present) → Some
        let snaks = vec![
            Snak::new_string("P854", "https://example.com"),
            Snak::new_time("P813", "+2024-06-01T00:00:00Z", 11),
        ];
        let reference = Reference::new_from_snaks(&snaks, "en");
        assert!(reference.is_some());
        let reference = reference.unwrap();
        assert_eq!(reference.url, Some("https://example.com".to_string()));
        assert_eq!(reference.date, Some("2024-06-01".to_string()));
    }

    // --- serialization ---

    #[test]
    fn test_reference_serialize_deserialize() {
        let reference = Reference {
            url: Some("https://example.com".to_string()),
            title: Some("Test".to_string()),
            date: Some("2025-01-01".to_string()),
            stated_in: Some("Q42".to_string()),
        };
        let json = serde_json::to_string(&reference).unwrap();
        let deserialized: Reference = serde_json::from_str(&json).unwrap();
        assert_eq!(reference, deserialized);
    }
}
