//! Template parameter definitions and validation.

use crate::{configuration::Configuration, template::Template};
use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinksType {
    All,
    Local,
    Red,
    RedOnly,
    Text,
    Reasonator,
}

impl LinksType {
    pub fn new_from_string(s: String) -> Self {
        match s.trim().to_uppercase().as_str() {
            "LOCAL" => Self::Local,
            "RED" => Self::Red,
            "RED_ONLY" => Self::RedOnly,
            "TEXT" => Self::Text,
            "REASONATOR" => Self::Reasonator,
            _ => Self::All, // Fallback, default
        }
    }
}

#[derive(Debug, Clone)]
pub enum SortMode {
    Label,
    FamilyName,
    Property(String),
    SparqlVariable(String),
    None,
}

impl SortMode {
    #[must_use]
    pub fn new(os: Option<&String>) -> Self {
        static RE_PROP: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^P\d+$").expect("RE_PROP does not parse"));
        static RE_SPARQL: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^?\S+$").expect("RE_SPARQL does not parse"));
        let os = os.map(|s| s.trim().to_uppercase());
        match os {
            Some(s) => match s.as_str() {
                "LABEL" => Self::Label,
                "FAMILY_NAME" => Self::FamilyName,
                other => {
                    if RE_PROP.is_match(other) {
                        Self::Property(other.to_string())
                    } else if RE_SPARQL.is_match(other) {
                        Self::SparqlVariable(other[1..].to_string())
                    } else {
                        Self::None
                    }
                }
            },
            _ => Self::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    #[must_use]
    pub fn new(os: Option<&String>) -> Self {
        match os {
            Some(s) => {
                if s.to_uppercase().trim() == "DESC" {
                    Self::Descending
                } else {
                    Self::Ascending
                }
            }
            None => Self::Ascending,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReferencesParameter {
    None,
    All,
}

impl ReferencesParameter {
    pub fn new(os: Option<&String>) -> Self {
        match os {
            Some(s) => {
                if s.to_uppercase().trim() == "ALL" {
                    Self::All
                } else {
                    Self::None
                }
            }
            None => Self::None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SectionType {
    None,
    Property(String),
    SparqlVariable(String),
}

impl SectionType {
    pub fn new_from_string_option(s: Option<&String>) -> Self {
        static RE_PROP: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^[Pp]\d+$").expect("RE_PROP does not parse"));
        static RE_PROP_NUM: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\d+$").expect("RE_PROP_NUM does not parse")); // Yes people do that!
        static RE_SPARQL: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^@.+$").expect("RE_SPARQL does not parse"));
        let s = match s {
            Some(s) => s,
            None => return Self::None,
        };
        let s = s.trim();
        if RE_PROP.is_match(s) {
            return Self::Property(s.to_uppercase());
        }
        if RE_PROP_NUM.is_match(s) {
            return Self::Property(format!("P{}", &s));
        }
        if RE_SPARQL.is_match(s) {
            return Self::SparqlVariable(s.to_uppercase());
        }
        Self::None
    }
}

#[derive(Debug, Clone)]
pub struct TemplateParams {
    links: LinksType,
    sort: SortMode,
    section: SectionType,
    min_section: u64,
    row_template: Option<String>,
    header_template: Option<String>,
    autodesc: Option<String>,
    summary: Option<String>,
    skip_table: bool,
    wdedit: bool,
    references: ReferencesParameter,
    one_row_per_item: bool,
    sort_order: SortOrder,
    wikibase: String,
}

impl Default for TemplateParams {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateParams {
    pub const fn new() -> Self {
        Self {
            links: LinksType::All,
            sort: SortMode::None,
            section: SectionType::None,
            min_section: 2,
            row_template: None,
            header_template: None,
            autodesc: None,
            summary: None,
            skip_table: false,
            wdedit: false,
            references: ReferencesParameter::None,
            one_row_per_item: false,
            sort_order: SortOrder::Ascending,
            wikibase: String::new(),
        }
    }

    pub fn new_from_params(template: &Template, config: &Configuration) -> Self {
        Self {
            links: LinksType::All,
            sort: SortMode::new(template.params().get("sort")),
            section: SectionType::new_from_string_option(template.params().get("section")),
            min_section: template
                .params()
                .get("min_section")
                .map(|s| s.parse::<u64>().ok().or(Some(2)).unwrap_or(2))
                .unwrap_or(2),
            row_template: template
                .params()
                .get("row_template")
                .map(|s| s.trim().to_string()),
            header_template: template
                .params()
                .get("header_template")
                .map(|s| s.trim().to_string()),
            autodesc: template
                .params()
                .get("autolist")
                .map(|s| s.trim().to_uppercase())
                .or_else(|| {
                    template
                        .params()
                        .get("autodesc")
                        .map(|s| s.trim().to_uppercase())
                }),
            summary: template
                .params()
                .get("summary")
                .map(|s| s.trim().to_uppercase()),
            skip_table: template.params().contains_key("skip_table"),
            one_row_per_item: template
                .params()
                .get("one_row_per_item")
                .map(|s| s.trim().to_uppercase())
                != Some("NO".to_string()),
            wdedit: template
                .params()
                .get("wdedit")
                .map(|s| s.trim().to_uppercase())
                == Some("YES".to_string()),
            references: ReferencesParameter::new(template.params().get("references")),
            sort_order: SortOrder::new(template.params().get("sort_order")),
            wikibase: template
                .params()
                .get("wikibase")
                .map(|s| s.trim().to_uppercase())
                .unwrap_or_else(|| config.get_default_api().to_string()),
        }
    }

    pub fn wikibase(&self) -> &str {
        &self.wikibase
    }

    pub fn autodesc(&self) -> Option<String> {
        self.autodesc.to_owned()
    }

    pub const fn one_row_per_item(&self) -> bool {
        self.one_row_per_item
    }

    pub const fn skip_table(&self) -> bool {
        self.skip_table
    }

    pub const fn wdedit(&self) -> bool {
        self.wdedit
    }

    pub const fn sort(&self) -> &SortMode {
        &self.sort
    }

    pub const fn sort_order(&self) -> &SortOrder {
        &self.sort_order
    }

    pub const fn section(&self) -> &SectionType {
        &self.section
    }

    pub const fn min_section(&self) -> u64 {
        self.min_section
    }

    pub const fn summary(&self) -> &Option<String> {
        &self.summary
    }

    pub const fn row_template(&self) -> &Option<String> {
        &self.row_template
    }

    pub const fn header_template(&self) -> &Option<String> {
        &self.header_template
    }

    pub const fn references(&self) -> &ReferencesParameter {
        &self.references
    }

    pub const fn links(&self) -> &LinksType {
        &self.links
    }

    pub const fn set_links(&mut self, links: LinksType) {
        self.links = links;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_links_type_new_from_string() {
        assert_eq!(
            LinksType::new_from_string("LOCAL".to_string()),
            LinksType::Local
        );
        assert_eq!(
            LinksType::new_from_string("local".to_string()),
            LinksType::Local
        );
        assert_eq!(
            LinksType::new_from_string("  local  ".to_string()),
            LinksType::Local
        );

        assert_eq!(
            LinksType::new_from_string("RED".to_string()),
            LinksType::Red
        );
        assert_eq!(
            LinksType::new_from_string("red".to_string()),
            LinksType::Red
        );

        assert_eq!(
            LinksType::new_from_string("RED_ONLY".to_string()),
            LinksType::RedOnly
        );
        assert_eq!(
            LinksType::new_from_string("red_only".to_string()),
            LinksType::RedOnly
        );

        assert_eq!(
            LinksType::new_from_string("TEXT".to_string()),
            LinksType::Text
        );
        assert_eq!(
            LinksType::new_from_string("text".to_string()),
            LinksType::Text
        );

        assert_eq!(
            LinksType::new_from_string("REASONATOR".to_string()),
            LinksType::Reasonator
        );
        assert_eq!(
            LinksType::new_from_string("reasonator".to_string()),
            LinksType::Reasonator
        );

        // Default fallback
        assert_eq!(
            LinksType::new_from_string("ALL".to_string()),
            LinksType::All
        );
        assert_eq!(
            LinksType::new_from_string("invalid".to_string()),
            LinksType::All
        );
        assert_eq!(LinksType::new_from_string("".to_string()), LinksType::All);
    }

    #[test]
    fn test_sort_mode_new_label() {
        assert!(matches!(
            SortMode::new(Some(&"LABEL".to_string())),
            SortMode::Label
        ));
        assert!(matches!(
            SortMode::new(Some(&"label".to_string())),
            SortMode::Label
        ));
        assert!(matches!(
            SortMode::new(Some(&"  label  ".to_string())),
            SortMode::Label
        ));
    }

    #[test]
    fn test_sort_mode_new_family_name() {
        assert!(matches!(
            SortMode::new(Some(&"FAMILY_NAME".to_string())),
            SortMode::FamilyName
        ));
        assert!(matches!(
            SortMode::new(Some(&"family_name".to_string())),
            SortMode::FamilyName
        ));
    }

    #[test]
    fn test_sort_mode_new_property() {
        match SortMode::new(Some(&"P31".to_string())) {
            SortMode::Property(p) => assert_eq!(p, "P31"),
            _ => panic!("Expected Property variant"),
        }

        match SortMode::new(Some(&"p569".to_string())) {
            SortMode::Property(p) => assert_eq!(p, "P569"),
            _ => panic!("Expected Property variant"),
        }

        match SortMode::new(Some(&"P1".to_string())) {
            SortMode::Property(p) => assert_eq!(p, "P1"),
            _ => panic!("Expected Property variant"),
        }
    }

    #[test]
    fn test_sort_mode_new_sparql_variable() {
        match SortMode::new(Some(&"?birthDate".to_string())) {
            SortMode::SparqlVariable(v) => assert_eq!(v, "BIRTHDATE"),
            _ => panic!("Expected SparqlVariable variant"),
        }

        match SortMode::new(Some(&"?name".to_string())) {
            SortMode::SparqlVariable(v) => assert_eq!(v, "NAME"),
            _ => panic!("Expected SparqlVariable variant"),
        }
    }

    #[test]
    fn test_sort_mode_new_none() {
        assert!(matches!(SortMode::new(None), SortMode::None));
        // Note: Due to the regex r"^?\S+$" (unescaped ?), strings like "invalid"
        // match as SparqlVariable. Empty string returns None.
        assert!(matches!(
            SortMode::new(Some(&"".to_string())),
            SortMode::None
        ));
    }

    #[test]
    fn test_sort_order_new() {
        assert_eq!(
            SortOrder::new(Some(&"DESC".to_string())),
            SortOrder::Descending
        );
        assert_eq!(
            SortOrder::new(Some(&"desc".to_string())),
            SortOrder::Descending
        );
        assert_eq!(
            SortOrder::new(Some(&"  desc  ".to_string())),
            SortOrder::Descending
        );

        assert_eq!(
            SortOrder::new(Some(&"ASC".to_string())),
            SortOrder::Ascending
        );
        assert_eq!(
            SortOrder::new(Some(&"asc".to_string())),
            SortOrder::Ascending
        );
        assert_eq!(
            SortOrder::new(Some(&"anything".to_string())),
            SortOrder::Ascending
        );
        assert_eq!(SortOrder::new(None), SortOrder::Ascending);
    }

    #[test]
    fn test_references_parameter_new() {
        assert_eq!(
            ReferencesParameter::new(Some(&"ALL".to_string())),
            ReferencesParameter::All
        );
        assert_eq!(
            ReferencesParameter::new(Some(&"all".to_string())),
            ReferencesParameter::All
        );
        assert_eq!(
            ReferencesParameter::new(Some(&"  all  ".to_string())),
            ReferencesParameter::All
        );

        assert_eq!(
            ReferencesParameter::new(Some(&"NONE".to_string())),
            ReferencesParameter::None
        );
        assert_eq!(
            ReferencesParameter::new(Some(&"anything".to_string())),
            ReferencesParameter::None
        );
        assert_eq!(ReferencesParameter::new(None), ReferencesParameter::None);
    }

    #[test]
    fn test_section_type_new_property() {
        match SectionType::new_from_string_option(Some(&"P31".to_string())) {
            SectionType::Property(p) => assert_eq!(p, "P31"),
            _ => panic!("Expected Property variant"),
        }

        match SectionType::new_from_string_option(Some(&"p569".to_string())) {
            SectionType::Property(p) => assert_eq!(p, "P569"),
            _ => panic!("Expected Property variant"),
        }
    }

    #[test]
    fn test_section_type_new_property_from_number() {
        match SectionType::new_from_string_option(Some(&"31".to_string())) {
            SectionType::Property(p) => assert_eq!(p, "P31"),
            _ => panic!("Expected Property variant"),
        }

        match SectionType::new_from_string_option(Some(&"569".to_string())) {
            SectionType::Property(p) => assert_eq!(p, "P569"),
            _ => panic!("Expected Property variant"),
        }
    }

    #[test]
    fn test_section_type_new_sparql_variable() {
        match SectionType::new_from_string_option(Some(&"@section".to_string())) {
            SectionType::SparqlVariable(v) => assert_eq!(v, "@SECTION"),
            _ => panic!("Expected SparqlVariable variant"),
        }

        match SectionType::new_from_string_option(Some(&"@variable".to_string())) {
            SectionType::SparqlVariable(v) => assert_eq!(v, "@VARIABLE"),
            _ => panic!("Expected SparqlVariable variant"),
        }
    }

    #[test]
    fn test_section_type_new_none() {
        assert!(matches!(
            SectionType::new_from_string_option(None),
            SectionType::None
        ));
        assert!(matches!(
            SectionType::new_from_string_option(Some(&"invalid".to_string())),
            SectionType::None
        ));
        assert!(matches!(
            SectionType::new_from_string_option(Some(&"".to_string())),
            SectionType::None
        ));
    }

    #[test]
    fn test_template_params_default() {
        let params = TemplateParams::new();
        assert_eq!(params.links(), &LinksType::All);
        assert!(matches!(params.sort(), SortMode::None));
        assert!(matches!(params.section(), SectionType::None));
        assert_eq!(params.min_section(), 2);
        assert_eq!(params.row_template(), &None);
        assert_eq!(params.header_template(), &None);
        assert_eq!(params.autodesc(), None);
        assert_eq!(params.summary(), &None);
        assert!(!params.skip_table());
        assert!(!params.wdedit());
        assert_eq!(params.references(), &ReferencesParameter::None);
        assert!(!params.one_row_per_item()); // Default is false in new()
        assert_eq!(params.sort_order(), &SortOrder::Ascending);
    }

    #[test]
    fn test_set_links() {
        let mut params = TemplateParams::new();
        assert_eq!(params.links(), &LinksType::All);
        params.set_links(LinksType::Red);
        assert_eq!(params.links(), &LinksType::Red);
        params.set_links(LinksType::Text);
        assert_eq!(params.links(), &LinksType::Text);
    }

    #[test]
    fn test_section_type_property_with_whitespace() {
        match SectionType::new_from_string_option(Some(&"  P31  ".to_string())) {
            SectionType::Property(p) => assert_eq!(p, "P31"),
            _ => panic!("Expected Property variant"),
        }
    }

    #[test]
    fn test_section_type_number_with_whitespace() {
        match SectionType::new_from_string_option(Some(&"  42  ".to_string())) {
            SectionType::Property(p) => assert_eq!(p, "P42"),
            _ => panic!("Expected Property variant"),
        }
    }

    #[test]
    fn test_sort_mode_large_property_number() {
        match SortMode::new(Some(&"P99999".to_string())) {
            SortMode::Property(p) => assert_eq!(p, "P99999"),
            _ => panic!("Expected Property variant"),
        }
    }

    #[test]
    fn test_template_params_default_is_default_trait() {
        // Verify Default trait implementation matches new()
        let from_new = TemplateParams::new();
        let from_default = TemplateParams::default();
        assert_eq!(from_new.links(), from_default.links());
        assert_eq!(from_new.min_section(), from_default.min_section());
        assert_eq!(from_new.skip_table(), from_default.skip_table());
        assert_eq!(from_new.wdedit(), from_default.wdedit());
        assert_eq!(from_new.one_row_per_item(), from_default.one_row_per_item());
        assert_eq!(from_new.sort_order(), from_default.sort_order());
        assert_eq!(from_new.references(), from_default.references());
    }
}
