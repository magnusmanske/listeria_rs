#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod column;
pub mod configuration;
pub mod database_pool;
pub mod page_to_process;
pub mod page_element;
pub mod page_params;
pub mod wiki_page_result;
pub mod site_matrix;
pub mod wiki_apis;
pub mod entity_container_wrapper;
pub mod listeria_list;
pub mod listeria_page;
pub mod listeria_bot;
pub mod reference;
pub mod sparql_value;
pub mod template;
pub mod template_params;
pub mod render_tabbed_data;
pub mod render_wikitext;
pub mod result_cell;
pub mod result_cell_part;
pub mod result_row;
pub mod renderer;

use crate::listeria_list::ListeriaList;
use crate::listeria_page::ListeriaPage;
use anyhow::{Result,anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use wikibase::mediawiki::api::Api;

type ApiLock = Arc<RwLock<Api>>;
