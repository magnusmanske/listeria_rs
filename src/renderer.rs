//! Renderer trait for different output formats.

use crate::listeria_page::ListeriaPage;
use crate::render_context::RenderContext;
use anyhow::Result;

#[allow(async_fn_in_trait)]
pub(crate) trait Renderer {
    async fn render<C: RenderContext>(&mut self, list: &C) -> Result<String>;
    async fn get_new_wikitext(&self, wikitext: &str, page: &ListeriaPage)
    -> Result<Option<String>>;
}
