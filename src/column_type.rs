//! Column types for result tables.

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
    /// Check if a string matches `[PpQq]\d+` pattern and return the uppercase form.
    fn parse_pq_id(s: &str, prefix: u8) -> Option<String> {
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            return None;
        }
        let first = bytes[0];
        if first != prefix && first != (prefix ^ 0x20) {
            return None;
        }
        if bytes.len() < 2 {
            return None;
        }
        if bytes[1..].iter().all(|b| b.is_ascii_digit()) {
            Some(s.to_uppercase())
        } else {
            None
        }
    }

    /// Try to parse a slash-separated compound like "P31/P580" or "P39/Q41582/P580"
    /// from already-trimmed parts.
    fn parse_slash_compound(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        // Split on '/' and trim each part
        let parts: Vec<&str> = trimmed.split('/').map(|p| p.trim()).collect();
        match parts.len() {
            2 => {
                let p1 = Self::parse_pq_id(parts[0], b'P')?;
                let p2 = Self::parse_pq_id(parts[1], b'P')?;
                Some(ColumnType::PropertyQualifier((p1, p2)))
            }
            3 => {
                let p1 = Self::parse_pq_id(parts[0], b'P')?;
                let q1 = Self::parse_pq_id(parts[1], b'Q')?;
                let p2 = Self::parse_pq_id(parts[2], b'P')?;
                Some(ColumnType::PropertyQualifierValue((p1, q1, p2)))
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn new(s: &str) -> Self {
        let lower = s.to_lowercase();
        let lower_trimmed = lower.trim();

        // Fast path: exact keyword matches
        match lower_trimmed {
            "number" => return ColumnType::Number,
            "label" => return ColumnType::Label,
            "description" => return ColumnType::Description(Vec::new()),
            "item" => return ColumnType::Item,
            "qid" => return ColumnType::Qid,
            _ => {}
        }

        // Check for "description/..." (case-insensitive, already lowered)
        if let Some(rest) = lower_trimmed.strip_prefix("description/") {
            let langs: Vec<String> = rest
                .split('/')
                .map(|lang| lang.trim().to_string())
                .filter(|lang| !lang.is_empty())
                .collect();
            return ColumnType::Description(langs);
        }

        // Check for "label/..." (case-insensitive)
        if let Some(rest) = lower_trimmed.strip_prefix("label/") {
            return ColumnType::LabelLang(rest.to_string());
        }

        // Check for "alias/..." (case-insensitive)
        if let Some(rest) = lower_trimmed.strip_prefix("alias/") {
            return ColumnType::AliasLang(rest.to_string());
        }

        // From here on, work with the original string (preserving case for P/Q ids)
        let trimmed = s.trim();

        // Check for simple property: P\d+
        if let Some(p) = Self::parse_pq_id(trimmed, b'P') {
            return ColumnType::Property(p);
        }

        // Check for compound (contains '/'):  P/P or P/Q/P
        if trimmed.contains('/')
            && let Some(ct) = Self::parse_slash_compound(trimmed)
        {
            return ct;
        }

        // Check for field: ?...
        if let Some(rest) = trimmed.strip_prefix('?')
            && !rest.is_empty()
        {
            return ColumnType::Field(rest.to_uppercase());
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
            Self::PropertyQualifier((p, q)) => {
                let mut key = p.to_lowercase();
                key.push('_');
                key.push_str(&q.to_lowercase());
                key
            }
            Self::PropertyQualifierValue((p, q, v)) => {
                let mut key = p.to_lowercase();
                key.push('_');
                key.push_str(&q.to_lowercase());
                key.push('_');
                key.push_str(&v.to_lowercase());
                key
            }
            Self::Field(f) => f.to_lowercase(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
