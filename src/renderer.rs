//! Renderer trait for different output formats.

use crate::listeria_list::ListeriaList;
use crate::listeria_page::ListeriaPage;
use anyhow::Result;

pub(crate) trait Renderer {
    fn new() -> Self;
    async fn render(&mut self, page: &mut ListeriaList) -> Result<String>;
    async fn get_new_wikitext(&self, wikitext: &str, page: &ListeriaPage)
    -> Result<Option<String>>;
}
