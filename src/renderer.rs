use crate::listeria_list::ListeriaList;
use crate::listeria_page::ListeriaPage;
use anyhow::Result;

pub trait Renderer {
    fn new() -> Self;
    fn render(&mut self, page: &ListeriaList) -> impl std::future::Future<Output = Result<String>> + Send;
    fn get_new_wikitext(
        &self,
        wikitext: &str,
        page: &ListeriaPage,
    ) -> impl std::future::Future<Output = Result<Option<String>>> + Send;
}
