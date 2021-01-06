use crate::listeria_list::ListeriaList;

use regex::{Regex, RegexBuilder};

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Number,
    Label,
    LabelLang(String),
    AliasLang(String),
    Description,
    Item,
    Qid,
    Property(String),
    PropertyQualifier((String, String)),
    PropertyQualifierValue((String, String, String)),
    Field(String),
    Unknown,
}

impl ColumnType {
    pub fn new(s: &str) -> Self {
        lazy_static! {
            static ref RE_LABEL_LANG: Regex = RegexBuilder::new(r#"^label/(.+)$"#)
                .case_insensitive(true)
                .build()
                .unwrap();
            static ref RE_ALIAS_LANG: Regex = RegexBuilder::new(r#"^alias/(.+)$"#)
                .case_insensitive(true)
                .build()
                .unwrap();
            static ref RE_PROPERTY: Regex = Regex::new(r#"^([Pp]\d+)$"#).unwrap();
            static ref RE_PROP_QUAL: Regex =
                Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Pp]\d+)\s*$"#).unwrap();
            static ref RE_PROP_QUAL_VAL: Regex =
                Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Qq]\d+)\s*/\s*([Pp]\d+)\s*$"#).unwrap();
            static ref RE_FIELD: Regex = Regex::new(r#"^\?(.+)$"#).unwrap();
        }
        match s.to_lowercase().as_str() {
            "number" => return ColumnType::Number,
            "label" => return ColumnType::Label,
            "description" => return ColumnType::Description,
            "item" => return ColumnType::Item,
            "qid" => return ColumnType::Qid,
            _ => {}
        }
        if let Some(caps) = RE_LABEL_LANG.captures(&s) {
            return ColumnType::LabelLang(match caps.get(1) {
                Some(x) => x.as_str().to_lowercase(),
                None => String::new(),
            });
        }
        if let Some(caps) = RE_ALIAS_LANG.captures(&s) {
            return ColumnType::AliasLang(match caps.get(1) {
                Some(x) => x.as_str().to_lowercase(),
                None => String::new(),
            });
        }
        if let Some(caps) = RE_PROPERTY.captures(&s) {
            return ColumnType::Property(match caps.get(1) {
                Some(x) => x.as_str().to_uppercase(),
                None => String::new(),
            });
        }
        if let Some(caps) = RE_PROP_QUAL.captures(&s) {
            return ColumnType::PropertyQualifier((
                match caps.get(1) {
                    Some(x) => x.as_str().to_uppercase(),
                    None => String::new(),
                },
                match caps.get(2) {
                    Some(x) => x.as_str().to_uppercase(),
                    None => String::new(),
                },
            ));
        }
        if let Some(caps) = RE_PROP_QUAL_VAL.captures(&s) {
            return ColumnType::PropertyQualifierValue((
                match caps.get(1) {
                    Some(x) => x.as_str().to_uppercase(),
                    None => String::new(),
                },
                match caps.get(2) {
                    Some(x) => x.as_str().to_uppercase(),
                    None => String::new(),
                },
                match caps.get(3) {
                    Some(x) => x.as_str().to_uppercase(),
                    None => String::new(),
                },
            ));
        }
        if let Some(caps) = RE_FIELD.captures(&s) {
            let ret = match caps.get(1) {
                Some(x) => x.as_str().to_lowercase(),
                None => String::new(),
            };
            return ColumnType::Field(ret);
        }
        ColumnType::Unknown
    }

    pub fn as_key(&self) -> String {
        match self {
            Self::Number => "number".to_string(),
            Self::Label => "label".to_string(),
            Self::Description => "desc".to_string(),
            Self::Item => "item".to_string(),
            Self::Qid => "qid".to_string(),
            Self::LabelLang(l) => format!("language:{}", l),
            Self::AliasLang(l) => format!("alias:{}", l),
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

#[derive(Debug, Clone)]
pub struct Column {
    pub obj: ColumnType,
    pub label: String,
    has_label: bool,
}

impl Column {
    pub fn new(s: &str) -> Self {
        lazy_static! {
            static ref RE_COLUMN_LABEL: Regex = Regex::new(r#"^\s*(.+?)\s*:\s*(.+?)\s*$"#).unwrap();
        }
        match RE_COLUMN_LABEL.captures(&s) {
            Some(caps) => Self {
                obj: ColumnType::new(&caps.get(1).unwrap().as_str().to_string()),
                label: caps.get(2).unwrap().as_str().to_string(),
                has_label: !caps.get(2).unwrap().as_str().is_empty(),
            },
            None => Self {
                obj: ColumnType::new(&s.trim().to_string()),
                label: s.trim().to_string(),
                has_label: false,
            },
        }
    }

    pub fn generate_label(&mut self, list: &ListeriaList) {
        if self.has_label {
            return;
        }
        self.label = match &self.obj {
            ColumnType::Property(prop) => list.get_label_with_fallback(prop, None),
            ColumnType::PropertyQualifier((prop, qual)) => {
                list.get_label_with_fallback(&prop, None)
                    + "/"
                    + &list.get_label_with_fallback(&qual, None)
            }
            ColumnType::PropertyQualifierValue((prop1, _qual, prop2)) => {
                list.get_label_with_fallback(&prop1, None)
                    + "/"
                    + &list.get_label_with_fallback(&prop2, None)
            }
            _ => self.label.to_owned(), // Fallback
        };
    }
}
