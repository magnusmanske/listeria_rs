
#[derive(Debug, Clone)]
pub struct WikiPageResult {
    pub wiki: String,
    pub page: String,
    pub result: String,
    pub message: String,
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

    pub fn fail(wiki: &str, page: &str, message: &str) -> Self {
        Self::new(
            wiki,
            page,
            "FAIL",
            message.to_string()
        )
    }
}