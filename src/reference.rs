//! Wikidata reference handling and formatting.

use crate::render_context::RenderContext;
use serde::{Deserialize, Serialize};
use wikimisc::date::Date;
use wikimisc::wikibase::Snak;
use wikimisc::wikibase::TimeValue;
use wikimisc::wikibase::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Reference {
    url: Option<String>,
    title: Option<String>,
    date: Option<String>,
    stated_in: Option<String>, // Item
    /// Archive URL (P1065). Codeberg #22.
    #[serde(default)]
    archive_url: Option<String>,
    /// Archive date (P2960). Codeberg #22.
    #[serde(default)]
    archive_date: Option<String>,
}

impl PartialEq for Reference {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
            && self.title == other.title
            && self.date == other.date
            && self.stated_in == other.stated_in
            && self.archive_url == other.archive_url
            && self.archive_date == other.archive_date
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
                // Codeberg #22: archive URL (P1065) and archive date (P2960)
                // produce `|archive-url=` / `|archive-date=` in cite-web output.
                "P1065" => Self::extract_archive_url(snak, &mut ret),
                "P2960" => Self::extract_archive_date(snak, &mut ret),
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
    pub async fn as_reference(&self, list: &impl RenderContext) -> String {
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
    async fn compute_wikitext(&self, list: &impl RenderContext) -> (String, String) {
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

    async fn render_cite_web(&self, use_invoke: bool, list: &impl RenderContext) -> String {
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
            ret += &format!("|access-date={}", date);
        }
        if let Some(archive_url) = &self.archive_url {
            ret += &format!("|archive-url={archive_url}");
        }
        if let Some(archive_date) = &self.archive_date {
            ret += &format!("|archive-date={archive_date}");
        }
        ret += "}}";
        ret
    }

    /// Extracts the archive URL (P1065) from a snak. Codeberg #22.
    fn extract_archive_url(snak: &Snak, ret: &mut Reference) {
        if let Some(dv) = snak.data_value()
            && let Value::StringValue(url) = dv.value()
        {
            ret.archive_url = Some(url.to_owned());
        }
    }

    /// Extracts the archive date (P2960) from a snak. Codeberg #22.
    /// Uses the same precision-aware formatting as access-date (P813).
    fn extract_archive_date(snak: &Snak, ret: &mut Reference) {
        if let Some(dv) = snak.data_value()
            && let Value::Time(tv) = dv.value()
        {
            ret.archive_date = Some(Self::describe_time(tv));
        }
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
        if let Some(dv) = snak.data_value()
            && let Value::Time(tv) = dv.value()
        {
            ret.date = Some(Self::describe_time(tv));
        }
    }

    /// A time value as precise as it is, in English: `2025-06-15`, `2025-06`,
    /// `2025`, `1990s`, `19th century`, `2nd millennium`, with a minus sign
    /// before the common era. A value Wikibase would not store (such as
    /// day 0 at day precision) is shown as it is, without the time of day.
    fn describe_time(tv: &TimeValue) -> String {
        let time = tv.time();
        Date::from_wikibase(time, *tv.precision()).map_or_else(
            |_| {
                let date = time.split_once('T').map_or(time, |(date, _)| date);
                date.strip_prefix('+').unwrap_or(date).to_string()
            },
            |date| date.describe(),
        )
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
            ..Default::default()
        };
        let ref2 = Reference {
            url: Some("https://example.com".to_string()),
            title: Some("Example".to_string()),
            date: Some("2025-01-01".to_string()),
            stated_in: Some("Q123".to_string()),
            ..Default::default()
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
        let snak = Snak::new_monolingual_text("P1476", "en", "Test Title");
        let mut reference = Reference::default();
        Reference::extract_title(&snak, "en", &mut reference);
        assert_eq!(reference.title, Some("Test Title".to_string()));
    }

    #[test]
    fn test_extract_title_non_matching_language() {
        let snak = Snak::new_monolingual_text("P1476", "de", "Testtitel");
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
            Snak::new_monolingual_text("P1476", "en", "Test Title"),
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
        // Year -99 → century -99/100 - 1 = -1 → "-1st"
        let snak = Snak::new_time("P813", "-0099-01-01T00:00:00Z", 7);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("-1st century".to_string()));
    }

    #[test]
    fn test_extract_timestamp_negative_year_millennium() {
        // Year -999 → millennium -999/1000 - 1 = -1 → "-1st"
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

    #[test]
    fn test_extract_timestamp_negative_year_precision() {
        let snak = Snak::new_time("P813", "-0500-00-00T00:00:00Z", 9);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("-0500".to_string()));
    }

    #[test]
    fn test_extract_timestamp_invalid_date_is_shown_as_it_is() {
        // Day 0 at day precision is not a date; show what Wikidata has.
        let snak = Snak::new_time("P813", "+2025-06-00T00:00:00Z", 11);
        let mut reference = Reference::default();
        Reference::extract_timestamp(&snak, &mut reference);
        assert_eq!(reference.date, Some("2025-06-00".to_string()));
    }

    // --- new_from_snaks edge cases ---

    #[test]
    fn test_new_from_snaks_with_only_title_returns_none() {
        // Title alone (no URL, no stated_in) → is_empty() = true → None
        let snaks = vec![Snak::new_monolingual_text("P1476", "en", "Some Title")];
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
            archive_url: None,
            archive_date: None,
        };
        let json = serde_json::to_string(&reference).unwrap();
        let deserialized: Reference = serde_json::from_str(&json).unwrap();
        assert_eq!(reference, deserialized);
    }

    // --- archive URL / date (codeberg #22) ---

    #[test]
    fn test_extract_archive_url() {
        let snak = Snak::new_string(
            "P1065",
            "https://web.archive.org/web/2020/https://x.example",
        );
        let mut reference = Reference::default();
        Reference::extract_archive_url(&snak, &mut reference);
        assert_eq!(
            reference.archive_url,
            Some("https://web.archive.org/web/2020/https://x.example".to_string())
        );
    }

    #[test]
    fn test_extract_archive_url_wrong_type_ignored() {
        // A non-string snak (e.g. accidental item value) must not populate
        // archive_url — same defensive behaviour as the URL extractor.
        let snak = Snak::new_item("P1065", "Q42");
        let mut reference = Reference::default();
        Reference::extract_archive_url(&snak, &mut reference);
        assert_eq!(reference.archive_url, None);
    }

    #[test]
    fn test_extract_archive_date_day_precision() {
        let snak = Snak::new_time("P2960", "+2024-03-15T00:00:00Z", 11);
        let mut reference = Reference::default();
        Reference::extract_archive_date(&snak, &mut reference);
        assert_eq!(reference.archive_date, Some("2024-03-15".to_string()));
    }

    #[test]
    fn test_extract_archive_date_year_precision() {
        // Lower-precision archive dates collapse the same way access-date does
        // — no point pretending we have day precision when Wikidata says we
        // don't.
        let snak = Snak::new_time("P2960", "+2024-03-15T00:00:00Z", 9);
        let mut reference = Reference::default();
        Reference::extract_archive_date(&snak, &mut reference);
        assert_eq!(reference.archive_date, Some("2024".to_string()));
    }

    #[test]
    fn test_new_from_snaks_with_archive_url_and_date() {
        let snaks = vec![
            Snak::new_string("P854", "https://example.com"),
            Snak::new_monolingual_text("P1476", "en", "Test Title"),
            Snak::new_string("P1065", "https://web.archive.org/web/2020/x"),
            Snak::new_time("P2960", "+2024-03-15T00:00:00Z", 11),
        ];
        let reference = Reference::new_from_snaks(&snaks, "en")
            .expect("archive-url-bearing reference is not empty");
        assert_eq!(
            reference.archive_url,
            Some("https://web.archive.org/web/2020/x".to_string())
        );
        assert_eq!(reference.archive_date, Some("2024-03-15".to_string()));
    }

    #[test]
    fn test_reference_equality_different_archive_url() {
        // Archive metadata must participate in equality so duplicate
        // references with different archives aren't collapsed by the
        // cell-level deduplication pass.
        let ref1 = Reference {
            url: Some("https://example.com".to_string()),
            archive_url: Some("https://archive.example/a".to_string()),
            ..Default::default()
        };
        let ref2 = Reference {
            url: Some("https://example.com".to_string()),
            archive_url: Some("https://archive.example/b".to_string()),
            ..Default::default()
        };
        assert_ne!(ref1, ref2);
    }
}
