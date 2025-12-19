//! Result tracking for processed wiki pages.

use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct WikiPageResult {
    wiki: String,
    page: String,
    result: String,
    message: String,
    duration: Option<Duration>,
    completed: Option<Instant>,
}

impl WikiPageResult {
    #[must_use]
    pub fn new(wiki: &str, page: &str, result: &str, message: String) -> Self {
        Self {
            wiki: wiki.to_string(),
            page: page.to_string(),
            result: result.to_string(),
            message,
            duration: None,
            completed: None,
        }
    }

    #[must_use]
    pub fn wiki(&self) -> &str {
        &self.wiki
    }

    #[must_use]
    pub fn page(&self) -> &str {
        &self.page
    }

    #[must_use]
    pub fn result(&self) -> &str {
        &self.result
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn fail(wiki: &str, page: &str, message: &str) -> Self {
        Self::new(wiki, page, "FAIL", message.to_string())
    }

    pub const fn runtime(&self) -> Option<Duration> {
        self.duration
    }

    pub const fn set_runtime(&mut self, runtime: Duration) {
        self.duration = Some(runtime);
    }

    pub const fn completed(&self) -> Option<Instant> {
        self.completed
    }

    pub const fn set_completed(&mut self, completed: Instant) {
        self.completed = Some(completed);
    }

    pub fn standardize_meassage(&mut self) {
        if self
            .message
            .contains("This page is a translation of the page")
        {
            self.result = "TRANSLATION".into();
            self.message = "This page is a translation".into();
        }
        if self
            .message
            .contains("Connection reset by peer (os error 104)")
        {
            self.message = "104_RESET_BY_PEER".into();
        }
        if self.message.contains("api.php): operation timed out") {
            self.message = "WIKI_TIMEOUT".into();
        }
        if self.message.contains("/sparql): operation timed out") {
            self.message = "SPARQL_TIMEOUT".into();
        }
        if self
            .message
            .contains("expected value at line 1 column 1: SPARQL-QUERY:")
        {
            self.message = "SPARQL_ERROR".into();
        }
        if self.message.contains("No 'sparql' parameter in Template") {
            self.message = format!("SPARQL_ERROR {}", self.message);
        }
        if self
            .message
            .contains("Could not determine SPARQL variable for item")
        {
            self.message = format!("SPARQL_ERROR {}", self.message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let result = WikiPageResult::new("enwiki", "Main_Page", "OK", "Success".to_string());

        assert_eq!(result.wiki(), "enwiki");
        assert_eq!(result.page(), "Main_Page");
        assert_eq!(result.result(), "OK");
        assert_eq!(result.message(), "Success");
        assert!(result.runtime().is_none());
        assert!(result.completed().is_none());
    }

    #[test]
    fn test_fail() {
        let result = WikiPageResult::fail("dewiki", "Test_Page", "Something went wrong");

        assert_eq!(result.wiki(), "dewiki");
        assert_eq!(result.page(), "Test_Page");
        assert_eq!(result.result(), "FAIL");
        assert_eq!(result.message(), "Something went wrong");
    }

    #[test]
    fn test_set_runtime() {
        let mut result = WikiPageResult::new("enwiki", "Test", "OK", "Done".to_string());

        assert!(result.runtime().is_none());

        let duration = Duration::from_secs(5);
        result.set_runtime(duration);

        assert_eq!(result.runtime(), Some(duration));
    }

    #[test]
    fn test_set_completed() {
        let mut result = WikiPageResult::new("enwiki", "Test", "OK", "Done".to_string());

        assert!(result.completed().is_none());

        let now = Instant::now();
        result.set_completed(now);

        assert_eq!(result.completed(), Some(now));
    }

    #[test]
    fn test_clone() {
        let result1 = WikiPageResult::new("enwiki", "Test", "OK", "Success".to_string());

        let result2 = result1.clone();

        assert_eq!(result1.wiki(), result2.wiki());
        assert_eq!(result1.page(), result2.page());
        assert_eq!(result1.result(), result2.result());
        assert_eq!(result1.message(), result2.message());
    }

    #[test]
    fn test_standardize_message_translation() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "OK",
            "This page is a translation of the page Foo".to_string(),
        );

        result.standardize_meassage();

        assert_eq!(result.result(), "TRANSLATION");
        assert_eq!(result.message(), "This page is a translation");
    }

    #[test]
    fn test_standardize_message_connection_reset() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "Connection reset by peer (os error 104)".to_string(),
        );

        result.standardize_meassage();

        assert_eq!(result.message(), "104_RESET_BY_PEER");
    }

    #[test]
    fn test_standardize_message_wiki_timeout() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "Error calling api.php): operation timed out".to_string(),
        );

        result.standardize_meassage();

        assert_eq!(result.message(), "WIKI_TIMEOUT");
    }

    #[test]
    fn test_standardize_message_sparql_timeout() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "Error calling /sparql): operation timed out".to_string(),
        );

        result.standardize_meassage();

        assert_eq!(result.message(), "SPARQL_TIMEOUT");
    }

    #[test]
    fn test_standardize_message_sparql_error_json() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "expected value at line 1 column 1: SPARQL-QUERY: SELECT ...".to_string(),
        );

        result.standardize_meassage();

        assert_eq!(result.message(), "SPARQL_ERROR");
    }

    #[test]
    fn test_standardize_message_no_sparql_parameter() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "No 'sparql' parameter in Template {{Foo}}".to_string(),
        );

        result.standardize_meassage();

        assert!(result.message().starts_with("SPARQL_ERROR"));
        assert!(
            result
                .message()
                .contains("No 'sparql' parameter in Template")
        );
    }

    #[test]
    fn test_standardize_message_sparql_variable_error() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "Could not determine SPARQL variable for item Q123".to_string(),
        );

        result.standardize_meassage();

        assert!(result.message().starts_with("SPARQL_ERROR"));
        assert!(
            result
                .message()
                .contains("Could not determine SPARQL variable for item")
        );
    }

    #[test]
    fn test_standardize_message_multiple_conditions() {
        // Test that multiple conditions can be checked (even though only first match applies)
        let mut result1 = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "This page is a translation of the page Foo and Connection reset by peer (os error 104)".to_string(),
        );

        result1.standardize_meassage();

        // Translation should be detected first
        assert_eq!(result1.result(), "TRANSLATION");
        assert_eq!(result1.message(), "This page is a translation");
    }

    #[test]
    fn test_standardize_message_no_match() {
        let mut result = WikiPageResult::new(
            "enwiki",
            "Test",
            "FAIL",
            "Some random error message".to_string(),
        );

        let original_message = result.message().to_string();
        result.standardize_meassage();

        // Message should remain unchanged
        assert_eq!(result.message(), original_message);
    }

    #[test]
    fn test_getters_with_different_values() {
        let result = WikiPageResult::new(
            "wikidatawiki",
            "Wikidata:Main_Page",
            "SUCCESS",
            "All good".to_string(),
        );

        assert_eq!(result.wiki(), "wikidatawiki");
        assert_eq!(result.page(), "Wikidata:Main_Page");
        assert_eq!(result.result(), "SUCCESS");
        assert_eq!(result.message(), "All good");
    }

    #[test]
    fn test_empty_strings() {
        let result = WikiPageResult::new("", "", "", String::new());

        assert_eq!(result.wiki(), "");
        assert_eq!(result.page(), "");
        assert_eq!(result.result(), "");
        assert_eq!(result.message(), "");
    }

    #[test]
    fn test_with_runtime_and_completed() {
        let mut result = WikiPageResult::new("enwiki", "Test", "OK", "Success".to_string());

        let duration = Duration::from_millis(1234);
        let now = Instant::now();

        result.set_runtime(duration);
        result.set_completed(now);

        assert_eq!(result.runtime(), Some(duration));
        assert_eq!(result.completed(), Some(now));
    }
}
