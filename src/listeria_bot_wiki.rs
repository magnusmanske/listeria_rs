use crate::ApiArc;
use crate::configuration::Configuration;
use crate::listeria_page::ListeriaPage;
use crate::wiki_page_result::WikiPageResult;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ListeriaBotWiki {
    wiki: String,
    api: ApiArc,
    config: Arc<Configuration>,
}

impl ListeriaBotWiki {
    pub fn new(wiki: &str, api: ApiArc, config: Arc<Configuration>) -> Self {
        Self {
            wiki: wiki.to_string(),
            api,
            config,
        }
    }

    pub async fn process_page(&self, page: &str) -> WikiPageResult {
        let mut listeria_page =
            match ListeriaPage::new(self.config.clone(), self.api.clone(), page.to_owned()).await {
                Ok(p) => p,
                Err(e) => {
                    return WikiPageResult::new(
                        &self.wiki,
                        page,
                        "FAIL",
                        format!("Could not open/parse page '{page}': {e}"),
                    );
                }
            };
        if let Err(wpr) = listeria_page.run().await {
            return wpr;
        }
        let _ = match listeria_page.update_source_page().await {
            Ok(x) => x,
            Err(wpr) => return wpr,
        };
        WikiPageResult::new(&self.wiki, page, "OK", "".to_string())
    }
}
