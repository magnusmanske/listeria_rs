#![forbid(unsafe_code)]
#![warn(
    clippy::cognitive_complexity,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::doc_link_with_quotes,
    // clippy::doc_markdown,
    clippy::empty_line_after_outer_attr,
    clippy::empty_structs_with_brackets,
    clippy::float_cmp,
    clippy::float_cmp_const,
    clippy::float_equality_without_abs,
    keyword_idents,
    clippy::missing_const_for_fn,
    // missing_copy_implementations,
    missing_debug_implementations,
    // clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::mod_module_files,
    non_ascii_idents,
    noop_method_call,
    // clippy::option_if_let_else,
    clippy::print_stderr,
    // clippy::print_stdout,
    clippy::semicolon_if_nothing_returned,
    clippy::unseparated_literal_suffix,
    clippy::shadow_unrelated,
    clippy::similar_names,
    clippy::suspicious_operation_groupings,
    // unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    clippy::unused_self,
    // clippy::use_debug,
    clippy::used_underscore_binding,
    // clippy::useless_let_if_seq,
    // clippy::wildcard_dependencies,
    // clippy::wildcard_imports
)]
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_json;

pub mod column;
pub mod configuration;
pub mod database_pool;
pub mod entity_container_wrapper;
pub mod listeria_bot;
pub mod listeria_bot_single;
pub mod listeria_bot_wiki;
pub mod listeria_bot_wikidata;
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
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use wikimisc::mediawiki::api::Api;

type ApiArc = Arc<Api>;

pub const LISTERIA_USER_AGENT: &str = "User-Agent: ListeriaBot/0.1.2 (https://listeria.toolforge.org/; magnusmanske@googlemail.com) reqwest/0.11.23";
