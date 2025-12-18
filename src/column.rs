//! Column definitions for result tables.

use crate::{column_type::ColumnType, listeria_list::ListeriaList};
use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone)]
pub struct Column {
    obj: ColumnType,
    label: String,
    has_label: bool,
}

impl Column {
    #[must_use]
    pub fn new(s: &str) -> Option<Self> {
        static RE_COLUMN_LABEL: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"^\s*(.+?)\s*:\s*(.+?)\s*$"#).expect("RE_COLUMN_LABEL does not parse")
        });
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

    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub const fn obj(&self) -> &ColumnType {
        &self.obj
    }

    pub async fn generate_label(&mut self, list: &ListeriaList) {
        if self.has_label {
            return;
        }
        self.label = match &self.obj {
            ColumnType::Property(prop) => list.get_label_with_fallback(prop).await,
            ColumnType::PropertyQualifier((prop, qual)) => {
                list.get_label_with_fallback(prop).await
                    + "/"
                    + &list.get_label_with_fallback(qual).await
            }
            ColumnType::PropertyQualifierValue((prop1, _qual, prop2)) => {
                list.get_label_with_fallback(prop1).await
                    + "/"
                    + &list.get_label_with_fallback(prop2).await
            }
            ColumnType::Number => self.label.to_owned(),
            ColumnType::Label => self.label.to_owned(),
            ColumnType::LabelLang(_) => self.label.to_owned(),
            ColumnType::AliasLang(_) => self.label.to_owned(),
            ColumnType::Description => self.label.to_owned(),
            ColumnType::Item => self.label.to_owned(),
            ColumnType::Qid => self.label.to_owned(),
            ColumnType::Field(_) => self.label.to_owned(),
            ColumnType::Unknown => self.label.to_owned(),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_type_new_number() {
        assert_eq!(ColumnType::new("number"), ColumnType::Number);
        assert_eq!(ColumnType::new("NUMBER"), ColumnType::Number);
        assert_eq!(ColumnType::new("Number"), ColumnType::Number);
    }

    #[test]
    fn test_column_type_new_label() {
        assert_eq!(ColumnType::new("label"), ColumnType::Label);
        assert_eq!(ColumnType::new("LABEL"), ColumnType::Label);
    }

    #[test]
    fn test_column_type_new_description() {
        assert_eq!(ColumnType::new("description"), ColumnType::Description);
        assert_eq!(ColumnType::new("DESCRIPTION"), ColumnType::Description);
    }

    #[test]
    fn test_column_type_new_item() {
        assert_eq!(ColumnType::new("item"), ColumnType::Item);
        assert_eq!(ColumnType::new("ITEM"), ColumnType::Item);
    }

    #[test]
    fn test_column_type_new_qid() {
        assert_eq!(ColumnType::new("qid"), ColumnType::Qid);
        assert_eq!(ColumnType::new("QID"), ColumnType::Qid);
    }

    #[test]
    fn test_column_type_new_label_lang() {
        assert_eq!(
            ColumnType::new("label/de"),
            ColumnType::LabelLang("de".to_string())
        );
        assert_eq!(
            ColumnType::new("LABEL/FR"),
            ColumnType::LabelLang("fr".to_string())
        );
        assert_eq!(
            ColumnType::new("Label/en-GB"),
            ColumnType::LabelLang("en-gb".to_string())
        );
    }

    #[test]
    fn test_column_type_new_alias_lang() {
        assert_eq!(
            ColumnType::new("alias/es"),
            ColumnType::AliasLang("es".to_string())
        );
        assert_eq!(
            ColumnType::new("ALIAS/IT"),
            ColumnType::AliasLang("it".to_string())
        );
        assert_eq!(
            ColumnType::new("Alias/zh-Hans"),
            ColumnType::AliasLang("zh-hans".to_string())
        );
    }

    #[test]
    fn test_column_type_new_property() {
        assert_eq!(
            ColumnType::new("P31"),
            ColumnType::Property("P31".to_string())
        );
        assert_eq!(
            ColumnType::new("p123"),
            ColumnType::Property("P123".to_string())
        );
        assert_eq!(
            ColumnType::new("P1"),
            ColumnType::Property("P1".to_string())
        );
    }

    #[test]
    fn test_column_type_new_property_qualifier() {
        assert_eq!(
            ColumnType::new("P31/P580"),
            ColumnType::PropertyQualifier(("P31".to_string(), "P580".to_string()))
        );
        assert_eq!(
            ColumnType::new("p569/p1319"),
            ColumnType::PropertyQualifier(("P569".to_string(), "P1319".to_string()))
        );
        assert_eq!(
            ColumnType::new("P123 / P456"),
            ColumnType::PropertyQualifier(("P123".to_string(), "P456".to_string()))
        );
    }

    #[test]
    fn test_column_type_new_property_qualifier_value() {
        assert_eq!(
            ColumnType::new("P39/Q41582/P580"),
            ColumnType::PropertyQualifierValue((
                "P39".to_string(),
                "Q41582".to_string(),
                "P580".to_string()
            ))
        );
        assert_eq!(
            ColumnType::new("p108/q95/p580"),
            ColumnType::PropertyQualifierValue((
                "P108".to_string(),
                "Q95".to_string(),
                "P580".to_string()
            ))
        );
        assert_eq!(
            ColumnType::new("P1 / Q2 / P3"),
            ColumnType::PropertyQualifierValue((
                "P1".to_string(),
                "Q2".to_string(),
                "P3".to_string()
            ))
        );
    }

    #[test]
    fn test_column_type_new_field() {
        assert_eq!(
            ColumnType::new("?birthDate"),
            ColumnType::Field("BIRTHDATE".to_string())
        );
        assert_eq!(
            ColumnType::new("?name"),
            ColumnType::Field("NAME".to_string())
        );
    }

    #[test]
    fn test_column_type_new_unknown() {
        assert_eq!(ColumnType::new("invalid"), ColumnType::Unknown);
        assert_eq!(ColumnType::new("Q123"), ColumnType::Unknown);
        assert_eq!(ColumnType::new(""), ColumnType::Unknown);
    }

    #[test]
    fn test_column_type_as_key() {
        assert_eq!(ColumnType::Number.as_key(), "number");
        assert_eq!(ColumnType::Label.as_key(), "label");
        assert_eq!(ColumnType::Description.as_key(), "desc");
        assert_eq!(ColumnType::Item.as_key(), "item");
        assert_eq!(ColumnType::Qid.as_key(), "qid");
        assert_eq!(
            ColumnType::LabelLang("de".to_string()).as_key(),
            "language:de"
        );
        assert_eq!(ColumnType::AliasLang("fr".to_string()).as_key(), "alias:fr");
        assert_eq!(ColumnType::Property("P31".to_string()).as_key(), "p31");
        assert_eq!(
            ColumnType::PropertyQualifier(("P31".to_string(), "P580".to_string())).as_key(),
            "p31_p580"
        );
        assert_eq!(
            ColumnType::PropertyQualifierValue((
                "P39".to_string(),
                "Q41582".to_string(),
                "P580".to_string()
            ))
            .as_key(),
            "p39_q41582_p580"
        );
        assert_eq!(ColumnType::Field("NAME".to_string()).as_key(), "name");
        assert_eq!(ColumnType::Unknown.as_key(), "unknown");
    }

    #[test]
    fn test_column_new_without_label() {
        let col = Column::new("P31").unwrap();
        assert_eq!(col.obj(), &ColumnType::Property("P31".to_string()));
        assert_eq!(col.label(), "P31");
        assert!(!col.has_label);
    }

    #[test]
    fn test_column_new_with_label() {
        let col = Column::new("P31:instance of").unwrap();
        assert_eq!(col.obj(), &ColumnType::Property("P31".to_string()));
        assert_eq!(col.label(), "instance of");
        assert!(col.has_label);
    }

    #[test]
    fn test_column_new_with_whitespace() {
        let col = Column::new("  P569  :  date of birth  ").unwrap();
        assert_eq!(col.obj(), &ColumnType::Property("P569".to_string()));
        assert_eq!(col.label(), "date of birth");
        assert!(col.has_label);
    }

    #[test]
    fn test_column_new_label_lang_with_custom_label() {
        let col = Column::new("label/de:Beschreibung").unwrap();
        assert_eq!(col.obj(), &ColumnType::LabelLang("de".to_string()));
        assert_eq!(col.label(), "Beschreibung");
        assert!(col.has_label);
    }

    #[test]
    fn test_column_new_number_without_label() {
        let col = Column::new("number").unwrap();
        assert_eq!(col.obj(), &ColumnType::Number);
        assert_eq!(col.label(), "number");
        assert!(!col.has_label);
    }

    #[test]
    fn test_column_new_description_with_label() {
        let col = Column::new("description:Info").unwrap();
        assert_eq!(col.obj(), &ColumnType::Description);
        assert_eq!(col.label(), "Info");
        assert!(col.has_label);
    }
}
