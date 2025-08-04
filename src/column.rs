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
                .expect("RE_LABEL_LANG does not parse");
            static ref RE_ALIAS_LANG: Regex = RegexBuilder::new(r#"^alias/(.+)$"#)
                .case_insensitive(true)
                .build()
                .expect("RE_ALIAS_LANG does not parse");
            static ref RE_PROPERTY: Regex =
                Regex::new(r#"^([Pp]\d+)$"#).expect("RE_PROPERTY does not parse");
            static ref RE_PROP_QUAL: Regex = Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Pp]\d+)\s*$"#)
                .expect("RE_PROP_QUAL does not parse");
            static ref RE_PROP_QUAL_VAL: Regex =
                Regex::new(r#"^\s*([Pp]\d+)\s*/\s*([Qq]\d+)\s*/\s*([Pp]\d+)\s*$"#)
                    .expect("RE_PROP_QUAL_VAL does not parse");
            static ref RE_FIELD: Regex =
                Regex::new(r#"^\?(.+)$"#).expect("RE_FIELD does not parse");
        }
        match s.to_lowercase().as_str() {
            "number" => return ColumnType::Number,
            "label" => return ColumnType::Label,
            "description" => return ColumnType::Description,
            "item" => return ColumnType::Item,
            "qid" => return ColumnType::Qid,
            _ => {}
        }
        if let Some(caps) = RE_LABEL_LANG.captures(s) {
            let ret = caps
                .get(1)
                .map(|s| s.as_str().to_lowercase())
                .unwrap_or_default();
            return ColumnType::LabelLang(ret);
        }
        if let Some(caps) = RE_ALIAS_LANG.captures(s) {
            let ret = caps
                .get(1)
                .map(|s| s.as_str().to_lowercase())
                .unwrap_or_default();
            return ColumnType::AliasLang(ret);
        }
        if let Some(caps) = RE_PROPERTY.captures(s) {
            let ret = caps
                .get(1)
                .map(|s| s.as_str().to_uppercase())
                .unwrap_or_default();
            return ColumnType::Property(ret);
        }
        if let Some(caps) = RE_PROP_QUAL.captures(s) {
            return ColumnType::PropertyQualifier((
                caps.get(1)
                    .map(|s| s.as_str().to_uppercase())
                    .unwrap_or_default(),
                caps.get(2)
                    .map(|s| s.as_str().to_uppercase())
                    .unwrap_or_default(),
            ));
        }
        if let Some(caps) = RE_PROP_QUAL_VAL.captures(s) {
            return ColumnType::PropertyQualifierValue((
                caps.get(1)
                    .map(|s| s.as_str().to_uppercase())
                    .unwrap_or_default(),
                caps.get(2)
                    .map(|s| s.as_str().to_uppercase())
                    .unwrap_or_default(),
                caps.get(3)
                    .map(|s| s.as_str().to_uppercase())
                    .unwrap_or_default(),
            ));
        }
        if let Some(caps) = RE_FIELD.captures(s) {
            let ret = caps
                .get(1)
                .map(|s| s.as_str().to_uppercase())
                .unwrap_or_default();
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

#[derive(Debug, Clone)]
pub struct Column {
    obj: ColumnType,
    label: String,
    has_label: bool,
}

impl Column {
    pub fn new(s: &str) -> Option<Self> {
        lazy_static! {
            static ref RE_COLUMN_LABEL: Regex =
                Regex::new(r#"^\s*(.+?)\s*:\s*(.+?)\s*$"#).expect("RE_COLUMN_LABEL does not parse");
        }
        match RE_COLUMN_LABEL.captures(s) {
            Some(caps) => Some(Self {
                obj: ColumnType::new(caps.get(1)?.as_str()),
                label: caps.get(2)?.as_str().to_string(),
                has_label: !caps.get(2)?.as_str().is_empty(),
            }),
            None => Some(Self {
                obj: ColumnType::new(s.trim()),
                label: s.trim().to_string(),
                has_label: false,
            }),
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn obj(&self) -> &ColumnType {
        &self.obj
    }

    pub fn generate_label(&mut self, list: &ListeriaList) {
        if self.has_label {
            return;
        }
        self.label = match &self.obj {
            ColumnType::Property(prop) => list.get_label_with_fallback(prop),
            ColumnType::PropertyQualifier((prop, qual)) => {
                list.get_label_with_fallback(prop) + "/" + &list.get_label_with_fallback(qual)
            }
            ColumnType::PropertyQualifierValue((prop1, _qual, prop2)) => {
                list.get_label_with_fallback(prop1) + "/" + &list.get_label_with_fallback(prop2)
            }
            _ => self.label.to_owned(), // Fallback
        };
    }
}
