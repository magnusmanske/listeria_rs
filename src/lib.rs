#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod column;
pub mod configuration;
pub mod database_pool;
pub mod entity_container_wrapper;
pub mod listeria_bot;
pub mod listeria_list;
pub mod listeria_page;
pub mod page_element;
pub mod page_params;
pub mod page_to_process;
pub mod reference;
pub mod render_tabbed_data;
pub mod render_wikitext;
pub mod renderer;
pub mod result_cell;
pub mod result_cell_part;
pub mod result_row;
pub mod sparql_results;
pub mod template;
pub mod template_params;
pub mod wiki;
pub mod wiki_apis;
pub mod wiki_page_result;

use crate::listeria_list::ListeriaList;
use crate::listeria_page::ListeriaPage;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use wikimisc::mediawiki::api::Api;

type ApiArc = Arc<Api>;

pub const LISTERIA_USER_AGENT: &str = "User-Agent: ListeriaBot/0.1.2 (https://listeria.toolforge.org/; magnusmanske@googlemail.com) reqwest/0.11.23";
