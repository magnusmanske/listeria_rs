//! Template parsing and parameter extraction.

use anyhow::{Result, anyhow};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct Template {
    params: HashMap<String, String>,
}

impl Template {
    pub fn new_from_params(_title: String, text: String) -> Result<Self> {
        let mut curly_braces = 0;
        let mut parts: Vec<String> = vec![];
        let mut part: Vec<char> = vec![];
        let mut quoted = false;
        let mut quote_char: char = ' ';
        text.chars().for_each(|c| match c {
            '\'' | '"' => {
                if quoted {
                    if quote_char == c {
                        quoted = false;
                    }
                } else {
                    quoted = true;
                    quote_char = c;
                }
                part.push(c);
            }
            '{' => {
                curly_braces += 1;
                part.push(c);
            }
            '}' => {
                curly_braces -= 1;
                part.push(c);
            }
            '|' => {
                if curly_braces == 0 && !quoted {
                    parts.push(part.iter().collect());
                    part.clear();
                } else {
                    part.push(c);
                }
            }
            _ => {
                part.push(c);
            }
        });
        parts.push(part.into_iter().collect());
        if quoted {
            return Err(anyhow!("Unclosed quote: {quote_char}"));
        }

        let params: HashMap<String, String> = parts
            .iter()
            .filter_map(|part_tmp| {
                let pos = part_tmp.find('=')?;
                let k = part_tmp.get(0..pos)?.trim().to_string();
                let v = part_tmp.get(pos + 1..)?.trim().to_string();
                Some((k, v))
            })
            .collect();
        Ok(Self { params })
    }

    pub const fn params(&self) -> &HashMap<String, String> {
        &self.params
    }

    pub fn fix_values(&mut self) {
        self.params = self
            .params
            .iter()
            .map(|(k, v)| (k.to_owned(), v.replace("{{!}}", "|")))
            .collect();
        // TODO proper template replacement
    }

    /// Get a template parameter value by key (case-insensitive).
    pub fn get_value(&self, key: &str) -> Option<String> {
        self.params
            .iter()
            .find(|(k, _v)| k.eq_ignore_ascii_case(key))
            .map(|(_k, v)| v.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fix_values() {
        let mut t = Template {
            params: HashMap::from([("foo".to_string(), "bar{{!}}baz".to_string())]),
        };
        t.fix_values();
        assert_eq!(t.params.get("foo"), Some(&"bar|baz".to_string()));
    }

    #[test]
    fn test_new_from_params_simple() {
        let t =
            Template::new_from_params("".to_string(), "param1=value1|param2=value2".to_string())
                .unwrap();
        assert_eq!(t.params.get("param1"), Some(&"value1".to_string()));
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_with_spaces() {
        let t = Template::new_from_params(
            "".to_string(),
            "  param1  =  value1  |  param2  =  value2  ".to_string(),
        )
        .unwrap();
        assert_eq!(t.params.get("param1"), Some(&"value1".to_string()));
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_nested_curly_braces() {
        let t = Template::new_from_params(
            "".to_string(),
            "param1={{template|value}}|param2=value2".to_string(),
        )
        .unwrap();
        assert_eq!(
            t.params.get("param1"),
            Some(&"{{template|value}}".to_string())
        );
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_quoted_pipe() {
        let t = Template::new_from_params(
            "".to_string(),
            "param1=\"value|with|pipes\"|param2=value2".to_string(),
        )
        .unwrap();
        assert_eq!(
            t.params.get("param1"),
            Some(&"\"value|with|pipes\"".to_string())
        );
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_single_quoted_pipe() {
        let t = Template::new_from_params(
            "".to_string(),
            "param1='value|with|pipes'|param2=value2".to_string(),
        )
        .unwrap();
        assert_eq!(
            t.params.get("param1"),
            Some(&"'value|with|pipes'".to_string())
        );
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_unclosed_quote() {
        let result = Template::new_from_params("".to_string(), "param1=\"unclosed".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_new_from_params_empty() {
        let t = Template::new_from_params("".to_string(), "".to_string()).unwrap();
        assert_eq!(t.params.len(), 0);
    }

    #[test]
    fn test_new_from_params_no_equals() {
        let t =
            Template::new_from_params("".to_string(), "value1|value2|param1=value3".to_string())
                .unwrap();
        // Parameters without '=' are ignored in the filter_map
        assert_eq!(t.params.len(), 1);
        assert_eq!(t.params.get("param1"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_new_from_params_complex_nested() {
        let t = Template::new_from_params(
            "".to_string(),
            "p1={{cite web|url=http://example.com|title=Test}}|p2=simple".to_string(),
        )
        .unwrap();
        assert_eq!(
            t.params.get("p1"),
            Some(&"{{cite web|url=http://example.com|title=Test}}".to_string())
        );
        assert_eq!(t.params.get("p2"), Some(&"simple".to_string()));
    }

    #[test]
    fn test_fix_values_multiple_replacements() {
        let mut t = Template {
            params: HashMap::from([
                ("p1".to_string(), "a{{!}}b{{!}}c".to_string()),
                ("p2".to_string(), "no replacement".to_string()),
                ("p3".to_string(), "{{!}}start".to_string()),
            ]),
        };
        t.fix_values();
        assert_eq!(t.params.get("p1"), Some(&"a|b|c".to_string()));
        assert_eq!(t.params.get("p2"), Some(&"no replacement".to_string()));
        assert_eq!(t.params.get("p3"), Some(&"|start".to_string()));
    }
}
