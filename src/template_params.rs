//use crate::{LinksType, ReferencesParameter, SectionType, SortMode, SortOrder, Template};

use regex::Regex;

use crate::template::Template;

#[derive(Debug, Clone, PartialEq)]
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
    pub fn new(os: Option<&String>) -> Self {
        lazy_static! {
            static ref RE_PROP: Regex = Regex::new(r"^P\d+$").expect("RE_PROP does not parse");
            static ref RE_SPARQL: Regex = Regex::new(r"^?\S+$").expect("RE_SPARQL does not parse");
        }
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

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
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

#[derive(Debug, Clone, PartialEq)]
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
        lazy_static! {
            static ref RE_PROP : Regex = Regex::new(r"^[Pp]\d+$").expect("RE_PROP does not parse");
            static ref RE_PROP_NUM : Regex = Regex::new(r"^\d+$").expect("RE_PROP_NUM does not parse"); // Yes people do that!
            static ref RE_SPARQL : Regex = Regex::new(r"^@.+$").expect("RE_SPARQL does not parse");
        }
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
    pub fn new() -> Self {
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

    pub fn new_from_params(template: &Template) -> Self {
        Self {
            links: LinksType::All,
            sort: SortMode::new(template.params.get("sort")),
            section: SectionType::new_from_string_option(template.params.get("section")),
            min_section: template
                .params
                .get("min_section")
                .map(|s| s.parse::<u64>().ok().or(Some(2)).unwrap_or(2))
                .unwrap_or(2),
            row_template: template
                .params
                .get("row_template")
                .map(|s| s.trim().to_string()),
            header_template: template
                .params
                .get("header_template")
                .map(|s| s.trim().to_string()),
            autodesc: template
                .params
                .get("autolist")
                .map(|s| s.trim().to_uppercase())
                .or_else(|| {
                    template
                        .params
                        .get("autodesc")
                        .map(|s| s.trim().to_uppercase())
                }),
            summary: template
                .params
                .get("summary")
                .map(|s| s.trim().to_uppercase()),
            skip_table: template.params.get("skip_table").is_some(),
            one_row_per_item: template
                .params
                .get("one_row_per_item")
                .map(|s| s.trim().to_uppercase())
                != Some("NO".to_string()),
            wdedit: template
                .params
                .get("wdedit")
                .map(|s| s.trim().to_uppercase())
                == Some("YES".to_string()),
            references: ReferencesParameter::new(template.params.get("references")),
            sort_order: SortOrder::new(template.params.get("sort_order")),
            wikibase: template
                .params
                .get("wikibase")
                .map(|s| s.trim().to_uppercase())
                .unwrap_or_else(|| "wikidatawiki".to_string()), // TODO config
        }
    }

    pub fn wikibase(&self) -> &str {
        &self.wikibase
    }

    pub fn autodesc(&self) -> Option<String> {
        self.autodesc.to_owned()
    }

    pub fn one_row_per_item(&self) -> bool {
        self.one_row_per_item
    }

    pub fn skip_table(&self) -> bool {
        self.skip_table
    }

    pub fn wdedit(&self) -> bool {
        self.wdedit
    }

    pub fn sort(&self) -> &SortMode {
        &self.sort
    }

    pub fn sort_order(&self) -> &SortOrder {
        &self.sort_order
    }

    pub fn section(&self) -> &SectionType {
        &self.section
    }

    pub fn min_section(&self) -> u64 {
        self.min_section
    }

    pub fn summary(&self) -> &Option<String> {
        &self.summary
    }

    pub fn row_template(&self) -> &Option<String> {
        &self.row_template
    }

    pub fn header_template(&self) -> &Option<String> {
        &self.header_template
    }

    pub fn references(&self) -> &ReferencesParameter {
        &self.references
    }

    pub fn links(&self) -> &LinksType {
        &self.links
    }

    pub fn set_links(&mut self, links: LinksType) {
        self.links = links;
    }
}
