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
            LazyLock::new(|| Regex::new(r"^\?\S+$").expect("RE_SPARQL does not parse"));
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
    summary_label: Option<String>,
    skip_table: bool,
    wdedit: bool,
    references: ReferencesParameter,
    one_row_per_item: bool,
    sort_order: SortOrder,
    wikibase: String,
    freq: u64,
    /// Custom label for the catch-all "Misc" section produced by `min_section=`
    /// grouping. `None` keeps the historic literal "Misc"; users localize via
    /// the template parameter `misc=…` (codeberg #73).
    misc_section_name: Option<String>,
    /// Override for the table `width=` attribute (codeberg #32). `None` keeps
    /// the historic `wikitable sortable` default (no explicit width — the
    /// browser/Vector skin decides). An empty string is treated as `None`
    /// so users can disable width by passing `tablewidth=`.
    table_width: Option<String>,
    /// Opt-in to rendering a list whose query legitimately returns no rows
    /// (issue #55). By default an empty result aborts the list — and with it
    /// every other list on the page — because a transient SPARQL failure is
    /// far more common than an intentionally empty report, and silently
    /// blanking a list is destructive.
    allow_empty: bool,
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
            summary_label: None,
            skip_table: false,
            wdedit: false,
            references: ReferencesParameter::None,
            one_row_per_item: false,
            sort_order: SortOrder::Ascending,
            wikibase: String::new(),
            freq: 0,
            misc_section_name: None,
            table_width: None,
            allow_empty: false,
        }
    }

    pub fn new_from_params(template: &Template, config: &Configuration) -> Self {
        Self {
            links: LinksType::All,
            sort: SortMode::new(template.params().get("sort")),
            section: SectionType::new_from_string_option(template.params().get("section")),
            min_section: Self::parse_min_section(template),
            row_template: template
                .params()
                .get("row_template")
                .map(|s| s.trim().to_string()),
            header_template: template
                .params()
                .get("header_template")
                .map(|s| s.trim().to_string()),
            autodesc: Self::parse_autodesc(template),
            summary: template
                .params()
                .get("summary")
                .map(|s| s.trim().to_uppercase()),
            summary_label: template
                .params()
                .get("summary_label")
                .map(|s| s.trim().to_string()),
            skip_table: template.params().contains_key("skip_table"),
            one_row_per_item: Self::parse_one_row_per_item(template),
            wdedit: Self::parse_flag_yes(template, "wdedit"),
            references: ReferencesParameter::new(template.params().get("references")),
            sort_order: SortOrder::new(template.params().get("sort_order")),
            wikibase: Self::parse_wikibase(template, config),
            freq: template
                .params()
                .get("freq")
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0),
            misc_section_name: Self::parse_optional_string(template, "misc"),
            table_width: Self::parse_optional_string(template, "tablewidth"),
            allow_empty: Self::parse_flag_yes(template, "allow_empty"),
        }
    }

    /// Trims whitespace and returns `None` for empty input so callers can use
    /// `.unwrap_or` to substitute a default without re-checking for emptiness.
    fn parse_optional_string(template: &Template, key: &str) -> Option<String> {
        let value = template.params().get(key)?.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    }

    fn parse_min_section(template: &Template) -> u64 {
        template
            .params()
            .get("min_section")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(2)
    }

    fn parse_autodesc(template: &Template) -> Option<String> {
        template
            .params()
            .get("autolist")
            .or_else(|| template.params().get("autodesc"))
            .map(|s| s.trim().to_uppercase())
    }

    fn parse_one_row_per_item(template: &Template) -> bool {
        template
            .params()
            .get("one_row_per_item")
            .map(|s| s.trim().to_uppercase())
            != Some("NO".to_string())
    }

    fn parse_flag_yes(template: &Template, key: &str) -> bool {
        template.params().get(key).map(|s| s.trim().to_uppercase()) == Some("YES".to_string())
    }

    fn parse_wikibase(template: &Template, config: &Configuration) -> String {
        template
            .params()
            .get("wikibase")
            .map(|s| s.trim().to_uppercase())
            .unwrap_or_else(|| config.get_default_api().to_string())
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

    pub const fn allow_empty(&self) -> bool {
        self.allow_empty
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

    pub fn summary_label(&self) -> &str {
        self.summary_label.as_deref().unwrap_or("items")
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

    pub const fn freq(&self) -> u64 {
        self.freq
    }

    /// Section name to use for items that would otherwise fall into the
    /// catch-all "Misc" bucket. Default `"Misc"` preserves historical behaviour.
    pub fn misc_section_name(&self) -> &str {
        self.misc_section_name.as_deref().unwrap_or("Misc")
    }

    /// Optional `width=` attribute for the rendered wikitable. `None` keeps
    /// the historic default of no explicit width.
    pub fn table_width(&self) -> Option<&str> {
        self.table_width.as_deref()
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
        assert!(matches!(
            SortMode::new(Some(&String::new())),
            SortMode::None
        ));
        // Non-SPARQL tokens (no leading `?`) and non-property strings fall through to None.
        assert!(matches!(
            SortMode::new(Some(&"invalid".to_string())),
            SortMode::None
        ));
        assert!(matches!(
            SortMode::new(Some(&"Q42".to_string())),
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
        assert_eq!(params.freq(), 0);
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
    fn test_summary_label_default() {
        let params = TemplateParams::new();
        assert_eq!(params.summary_label(), "items");
    }

    #[test]
    fn test_misc_section_name_default() {
        let params = TemplateParams::new();
        assert_eq!(params.misc_section_name(), "Misc");
    }

    #[test]
    fn test_misc_section_name_custom() {
        // codeberg #73: `misc=` allows localizing the catch-all section label.
        let template = crate::template::Template::new_from_params("foo|misc=Sonstiges")
            .expect("template parses");
        let config = crate::configuration::Configuration::default();
        let params = TemplateParams::new_from_params(&template, &config);
        assert_eq!(params.misc_section_name(), "Sonstiges");
    }

    #[test]
    fn test_misc_section_name_empty_falls_back_to_default() {
        // Empty values are equivalent to omitting the parameter, so the
        // historic "Misc" default is preserved.
        let template =
            crate::template::Template::new_from_params("foo|misc=  ").expect("template parses");
        let config = crate::configuration::Configuration::default();
        let params = TemplateParams::new_from_params(&template, &config);
        assert_eq!(params.misc_section_name(), "Misc");
    }

    #[test]
    fn test_table_width_default_is_none() {
        let params = TemplateParams::new();
        assert!(params.table_width().is_none());
    }

    #[test]
    fn test_table_width_custom() {
        // codeberg #32: `tablewidth=` allows users to opt back in to width
        // attributes (or pick a non-100% value) on the rendered table.
        let template = crate::template::Template::new_from_params("foo|tablewidth=80%")
            .expect("template parses");
        let config = crate::configuration::Configuration::default();
        let params = TemplateParams::new_from_params(&template, &config);
        assert_eq!(params.table_width(), Some("80%"));
    }

    #[test]
    fn test_table_width_empty_is_none() {
        // `tablewidth=` (empty) lets users explicitly disable width, falling
        // back to no width attribute on the wikitable.
        let template =
            crate::template::Template::new_from_params("foo|tablewidth=").expect("template parses");
        let config = crate::configuration::Configuration::default();
        let params = TemplateParams::new_from_params(&template, &config);
        assert!(params.table_width().is_none());
    }

    #[test]
    fn test_allow_empty_defaults_to_false() {
        // Issue #55: opting in is required, so a transient empty SPARQL
        // result still aborts rather than blanking the list.
        assert!(!TemplateParams::new().allow_empty());
        let template = crate::template::Template::new_from_params("foo").expect("template parses");
        let config = crate::configuration::Configuration::default();
        assert!(!TemplateParams::new_from_params(&template, &config).allow_empty());
    }

    #[test]
    fn test_allow_empty_yes() {
        let template = crate::template::Template::new_from_params("foo|allow_empty=yes")
            .expect("template parses");
        let config = crate::configuration::Configuration::default();
        assert!(TemplateParams::new_from_params(&template, &config).allow_empty());
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
