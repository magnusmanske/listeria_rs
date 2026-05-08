//! Typed error variants for the Listeria pipeline.
//!
//! These errors appear in the SPARQL and entity-loading layers.
//! They implement `std::error::Error` via `thiserror` and convert
//! into `anyhow::Error` automatically, so existing `?` call-sites
//! keep working unchanged.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ListeriaError {
    #[error("No wikibase setup configured for '{0}'")]
    SparqlNoConfig(String),

    #[error("SPARQL query must include the ?item variable — do not rename it")]
    SparqlNoItemVariable,

    #[error("No 'sparql' parameter found in template")]
    MissingSparqlParam,

    #[error("No items to show")]
    NoItemsToShow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparql_no_config_message() {
        let e = ListeriaError::SparqlNoConfig("enwiki".to_string());
        assert_eq!(e.to_string(), "No wikibase setup configured for 'enwiki'");
    }

    #[test]
    fn test_sparql_no_item_variable_message_contains_item() {
        let e = ListeriaError::SparqlNoItemVariable;
        assert!(e.to_string().contains("?item"));
    }

    #[test]
    fn test_missing_sparql_param_message() {
        let e = ListeriaError::MissingSparqlParam;
        assert!(e.to_string().contains("sparql"));
    }

    #[test]
    fn test_no_items_to_show_message() {
        let e = ListeriaError::NoItemsToShow;
        assert_eq!(e.to_string(), "No items to show");
    }

    #[test]
    fn test_error_converts_to_anyhow() {
        let e: anyhow::Error = ListeriaError::SparqlNoItemVariable.into();
        assert!(e.to_string().contains("?item"));
    }
}
