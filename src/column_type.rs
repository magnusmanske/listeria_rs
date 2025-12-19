//! Column types for result tables.

use regex::{Regex, RegexBuilder};
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Number,
    Label,
    LabelLang(String),
    AliasLang(String),
    Description(Vec<String>),
    Item,
    Qid,
    Property(String),
    PropertyQualifier((String, String)),
    PropertyQualifierValue((String, String, String)),
    Field(String),
    Unknown,
}

impl ColumnType {
    /// Helper method to extract and transform a capture group
    fn extract_capture<F>(caps: &regex::Captures, index: usize, transform: F) -> String
    where
        F: Fn(&str) -> String,
    {
        caps.get(index)
            .map(|m| transform(m.as_str()))
            .unwrap_or_default()
    }

    #[must_use]
    pub fn new(s: &str) -> Self {
        static RE_LABEL_LANG: LazyLock<Regex> = LazyLock::new(|| {
            RegexBuilder::new(r#"^label/(.+)$"#)
                .case_insensitive(true)
                .build()
                .expect("RE_LABEL_LANG does not parse")
        });
        static RE_ALIAS_LANG: LazyLock<Regex> = LazyLock::new(|| {
            RegexBuilder::new(r#"^alias/(.+)$"#)
                .case_insensitive(true)
                .build()
                .expect("RE_ALIAS_LANG does not parse")
        });
        static RE_DESCRIPTION_LANG: LazyLock<Regex> = LazyLock::new(|| {
            RegexBuilder::new(r#"^description/(.+)$"#)
                .case_insensitive(true)
                .build()
                .expect("RE_DESCRIPTION_LANG does not parse")
        });
        static RE_PROPERTY: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r#"^([Pp]\d+)$"#).expect("RE_PROPERTY does not parse"));
        static RE_PROP_QUAL: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Pp]\d+)\s*$"#).expect("RE_PROP_QUAL does not parse")
        });
        static RE_PROP_QUAL_VAL: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Qq]\d+)\s*/\s*([Pp]\d+)\s*$"#)
                .expect("RE_PROP_QUAL_VAL does not parse")
        });
        static RE_FIELD: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r#"^\?(.+)$"#).expect("RE_FIELD does not parse"));
        match s.to_lowercase().as_str() {
            "number" => return ColumnType::Number,
            "label" => return ColumnType::Label,
            "description" => return ColumnType::Description(Vec::new()),
            "item" => return ColumnType::Item,
            "qid" => return ColumnType::Qid,
            _ => {}
        }
        if let Some(caps) = RE_DESCRIPTION_LANG.captures(s) {
            let langs_str = Self::extract_capture(&caps, 1, |t| t.to_lowercase());
            let langs: Vec<String> = langs_str
                .split(',')
                .map(|lang| lang.trim().to_string())
                .filter(|lang| !lang.is_empty())
                .collect();
            return ColumnType::Description(langs);
        }
        if let Some(caps) = RE_LABEL_LANG.captures(s) {
            return ColumnType::LabelLang(Self::extract_capture(&caps, 1, |t| t.to_lowercase()));
        }
        if let Some(caps) = RE_ALIAS_LANG.captures(s) {
            return ColumnType::AliasLang(Self::extract_capture(&caps, 1, |t| t.to_lowercase()));
        }
        if let Some(caps) = RE_PROPERTY.captures(s) {
            return ColumnType::Property(Self::extract_capture(&caps, 1, |t| t.to_uppercase()));
        }
        if let Some(caps) = RE_PROP_QUAL.captures(s) {
            return ColumnType::PropertyQualifier((
                Self::extract_capture(&caps, 1, |t| t.to_uppercase()),
                Self::extract_capture(&caps, 2, |t| t.to_uppercase()),
            ));
        }
        if let Some(caps) = RE_PROP_QUAL_VAL.captures(s) {
            return ColumnType::PropertyQualifierValue((
                Self::extract_capture(&caps, 1, |t| t.to_uppercase()),
                Self::extract_capture(&caps, 2, |t| t.to_uppercase()),
                Self::extract_capture(&caps, 3, |t| t.to_uppercase()),
            ));
        }
        if let Some(caps) = RE_FIELD.captures(s) {
            return ColumnType::Field(Self::extract_capture(&caps, 1, |t| t.to_uppercase()));
        }
        ColumnType::Unknown
    }

    #[must_use]
    pub fn as_key(&self) -> String {
        match self {
            Self::Number => "number".to_string(),
            Self::Label => "label".to_string(),
            Self::Description(_) => "desc".to_string(),
            Self::Item => "item".to_string(),
            Self::Qid => "qid".to_string(),
            Self::LabelLang(l) => format!("language:{l}"),
            Self::AliasLang(l) => format!("alias:{l}"),
            Self::Property(p) => p.to_lowercase(),
            Self::PropertyQualifier((p, q)) => p.to_lowercase() + "_" + &q.to_lowercase(),
            Self::PropertyQualifierValue((p, q, v)) => {
                p.to_lowercase() + "_" + &q.to_lowercase() + "_" + &v.to_lowercase()
            }
            Self::Field(f) => f.to_lowercase(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
