//! Bot trait defining the interface for Listeria bot implementations.

use std::sync::Arc;

use crate::{
    configuration::Configuration, page_to_process::PageToProcess, wiki_page_result::WikiPageResult,
};
use anyhow::Result;

#[allow(async_fn_in_trait)]
pub trait ListeriaBot {
    async fn new(config_file: &str) -> Result<Self>
    where
        Self: Sized;
    async fn new_from_config(config: Arc<Configuration>) -> Result<Self>
    where
        Self: Sized;
    fn config(&self) -> &Configuration;
    async fn reset_running(&self) -> Result<()>;
    async fn clear_deleted(&self) -> Result<()>;
    async fn set_runtime(&self, pagestatus_id: u64, seconds: u64) -> Result<()>;
    async fn run_single_bot(&self, page: PageToProcess) -> Result<WikiPageResult>;

    /// Removed a pagestatus ID from the running list
    async fn release_running(&self, pagestatus_id: u64);

    /// Returns how many pages are running
    async fn get_running_count(&self) -> usize;

    /// Returns a page to be processed.
    async fn prepare_next_single_page(&self) -> Result<PageToProcess>;
}
