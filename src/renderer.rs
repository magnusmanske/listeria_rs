use crate::listeria_list::ListeriaList;
use crate::listeria_page::ListeriaPage;
use anyhow::Result;

pub trait Renderer {
    fn new() -> Self;
    fn render(&mut self, page: &mut ListeriaList) -> Result<String>;
    fn get_new_wikitext(&self, wikitext: &str, page: &ListeriaPage) -> Result<Option<String>>;
}
