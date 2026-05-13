//! Per-wiki bot wrapper handling page processing for a single wiki.

use crate::ApiArc;
use crate::configuration::Configuration;
use crate::listeria_page::ListeriaPage;
use crate::wiki_apis::WikiApis;
use crate::wiki_page_result::WikiPageResult;
use std::sync::Arc;

/// Where the per-wiki MediaWiki API comes from.
///
/// Multi-wiki mode goes through [`WikiApis`] so each `process_page` call holds
/// a real per-wiki semaphore permit for the duration of the work (audit F4.1).
/// Single-wiki mode shares a single pre-built `ApiArc` with no gating — there
/// is only one upstream to talk to and there's no `WikiApis` to gate against.
#[derive(Debug, Clone)]
enum ApiSource {
    Gated(Arc<WikiApis>),
    Direct(ApiArc),
}

#[derive(Debug, Clone)]
pub struct ListeriaBotWiki {
    wiki: String,
    api_source: ApiSource,
    config: Arc<Configuration>,
}

impl ListeriaBotWiki {
    /// Multi-wiki constructor: API handles are acquired per-call from
    /// [`WikiApis`], whose semaphores cap per-wiki and global concurrency.
    #[must_use]
    pub fn new(wiki: &str, wiki_apis: Arc<WikiApis>, config: Arc<Configuration>) -> Self {
        Self {
            wiki: wiki.to_string(),
            api_source: ApiSource::Gated(wiki_apis),
            config,
        }
    }

    /// Single-wiki constructor: shares a single already-constructed API
    /// handle. No semaphore gating because single-wiki mode has only one
    /// upstream and there's no `WikiApis` to consult.
    #[must_use]
    pub fn new_with_direct_api(
        wiki: &str,
        api: ApiArc,
        config: Arc<Configuration>,
    ) -> Self {
        Self {
            wiki: wiki.to_string(),
            api_source: ApiSource::Direct(api),
            config,
        }
    }

    pub async fn process_page(&self, page: &str) -> WikiPageResult {
        // Held for the whole call so its Drop releases the permits exactly
        // when we return; for the Direct variant the guard is a no-op.
        let (_handle, api) = match &self.api_source {
            ApiSource::Gated(wiki_apis) => match wiki_apis.acquire_wiki_api(&self.wiki).await {
                Ok(h) => {
                    let api = h.api().clone();
                    (Some(h), api)
                }
                Err(e) => {
                    return WikiPageResult::new(
                        &self.wiki,
                        page,
                        "FAIL",
                        format!("Could not acquire MW API handle: {e}"),
                    );
                }
            },
            ApiSource::Direct(api) => (None, api.clone()),
        };
        let mut listeria_page = match ListeriaPage::new(self.config.clone(), api, page.to_owned())
            .await
        {
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
