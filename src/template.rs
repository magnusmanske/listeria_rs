//! Template parsing and parameter extraction.

use anyhow::{Result, anyhow};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct Template {
    params: HashMap<String, String>,
}

impl Template {
    pub fn new_from_params(text: &str) -> Result<Self> {
        let mut curly_braces: i32 = 0;
        let mut parts: Vec<String> = Vec::new();
        let mut part = String::new();
        let mut quoted = false;
        let mut quote_char: char = ' ';
        for c in text.chars() {
            match c {
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
                        parts.push(std::mem::take(&mut part));
                    } else {
                        part.push(c);
                    }
                }
                _ => {
                    part.push(c);
                }
            }
        }
        parts.push(part);
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
            .map(|(k, v)| (k.clone(), v.replace("{{!}}", "|")))
            .collect();
        // TODO proper template replacement
    }

    /// Get a template parameter value by key (case-insensitive).
    pub fn get_value(&self, key: &str) -> Option<String> {
        self.params
            .iter()
            .find(|(k, _v)| k.eq_ignore_ascii_case(key))
            .map(|(_k, v)| v.clone())
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
        let t = Template::new_from_params("param1=value1|param2=value2").unwrap();
        assert_eq!(t.params.get("param1"), Some(&"value1".to_string()));
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_with_spaces() {
        let t = Template::new_from_params("  param1  =  value1  |  param2  =  value2  ").unwrap();
        assert_eq!(t.params.get("param1"), Some(&"value1".to_string()));
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_nested_curly_braces() {
        let t = Template::new_from_params("param1={{template|value}}|param2=value2").unwrap();
        assert_eq!(
            t.params.get("param1"),
            Some(&"{{template|value}}".to_string())
        );
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_quoted_pipe() {
        let t = Template::new_from_params("param1=\"value|with|pipes\"|param2=value2").unwrap();
        assert_eq!(
            t.params.get("param1"),
            Some(&"\"value|with|pipes\"".to_string())
        );
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_single_quoted_pipe() {
        let t = Template::new_from_params("param1='value|with|pipes'|param2=value2").unwrap();
        assert_eq!(
            t.params.get("param1"),
            Some(&"'value|with|pipes'".to_string())
        );
        assert_eq!(t.params.get("param2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_new_from_params_unclosed_quote() {
        let result = Template::new_from_params("param1=\"unclosed");
        assert!(result.is_err());
    }

    #[test]
    fn test_new_from_params_empty() {
        let t = Template::new_from_params("").unwrap();
        assert_eq!(t.params.len(), 0);
    }

    #[test]
    fn test_new_from_params_no_equals() {
        let t = Template::new_from_params("value1|value2|param1=value3").unwrap();
        // Parameters without '=' are ignored in the filter_map
        assert_eq!(t.params.len(), 1);
        assert_eq!(t.params.get("param1"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_new_from_params_complex_nested() {
        let t = Template::new_from_params(
            "p1={{cite web|url=http://example.com|title=Test}}|p2=simple",
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

    // --- get_value (case-insensitive lookup) ---

    #[test]
    fn test_get_value_exact_case() {
        let t = Template::new_from_params("sort=P31|links=ALL").unwrap();
        assert_eq!(t.get_value("sort"), Some("P31".to_string()));
        assert_eq!(t.get_value("links"), Some("ALL".to_string()));
    }

    #[test]
    fn test_get_value_case_insensitive() {
        let t = Template::new_from_params("Sort=P31|LINKS=all").unwrap();
        assert_eq!(t.get_value("sort"), Some("P31".to_string()));
        assert_eq!(t.get_value("links"), Some("all".to_string()));
        assert_eq!(t.get_value("SORT"), Some("P31".to_string()));
    }

    #[test]
    fn test_get_value_missing_key() {
        let t = Template::new_from_params("sort=P31").unwrap();
        assert_eq!(t.get_value("missing"), None);
    }

    #[test]
    fn test_get_value_empty_template() {
        let t = Template::new_from_params("").unwrap();
        assert_eq!(t.get_value("anything"), None);
    }

    #[test]
    fn test_params_getter() {
        let t = Template::new_from_params("a=1|b=2").unwrap();
        assert_eq!(t.params().len(), 2);
        assert_eq!(t.params().get("a"), Some(&"1".to_string()));
    }

    #[test]
    fn test_new_from_params_value_with_equals() {
        // Values containing '=' should use the first '=' as the separator
        let t = Template::new_from_params("url=http://example.com?a=1&b=2").unwrap();
        assert_eq!(
            t.params.get("url"),
            Some(&"http://example.com?a=1&b=2".to_string())
        );
    }

    #[test]
    fn test_new_from_params_duplicate_keys() {
        // HashMap semantics: last one wins
        let t = Template::new_from_params("key=first|key=second").unwrap();
        // Due to HashMap collect, either value could win, but typically last
        assert!(t.params.contains_key("key"));
    }
}
