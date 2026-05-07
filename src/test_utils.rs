//! Shared test utilities — only compiled in test builds.
//!
//! Provides process-global caches for expensive-to-construct objects
//! (`Api`, `Configuration`) so each object is initialised at most once
//! per test run regardless of how many test modules request it.

use crate::configuration::Configuration;
use dashmap::DashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::OnceCell;
use wikimisc::mediawiki::api::Api;

/// Caches `Api` objects by URL.  The first call for a URL performs the live
/// siteinfo fetch; all subsequent calls return the cached `Arc`.
static API_CACHE: LazyLock<DashMap<String, Arc<Api>>> = LazyLock::new(DashMap::new);

/// Returns a cached `Arc<Api>` for `url`, initialising it on first access.
pub async fn cached_api(url: &str) -> Arc<Api> {
    if let Some(api) = API_CACHE.get(url) {
        return api.clone();
    }
    let api = Arc::new(Api::new(url).await.expect("Api::new failed in test"));
    API_CACHE.insert(url.to_string(), api.clone());
    api
}

/// Cached raw `Configuration` loaded from `config.json` with no modifications.
static RAW_CONFIG: OnceCell<Arc<Configuration>> = OnceCell::const_new();

/// Returns a cached `Arc<Configuration>` built from `config.json`.
pub async fn cached_config() -> Arc<Configuration> {
    RAW_CONFIG
        .get_or_init(|| async {
            Arc::new(
                Configuration::new_from_file("config.json")
                    .await
                    .expect("Configuration::new_from_file failed in test"),
            )
        })
        .await
        .clone()
}
