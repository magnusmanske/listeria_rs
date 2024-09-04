#[derive(Debug, Clone)]
pub struct WikiPageResult {
    wiki: String,
    page: String,
    result: String,
    message: String,
}

unsafe impl Send for WikiPageResult {}

impl WikiPageResult {
    pub fn new(wiki: &str, page: &str, result: &str, message: String) -> Self {
        Self {
            wiki: wiki.to_string(),
            page: page.to_string(),
            result: result.to_string(),
            message,
        }
    }

    pub fn wiki(&self) -> &str {
        &self.wiki
    }

    pub fn page(&self) -> &str {
        &self.page
    }

    pub fn result(&self) -> &str {
        &self.result
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn fail(wiki: &str, page: &str, message: &str) -> Self {
        Self::new(wiki, page, "FAIL", message.to_string())
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
