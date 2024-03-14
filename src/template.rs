use anyhow::{anyhow, Result};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct Template {
    pub title: String,
    pub params: HashMap<String, String>,
}

impl Template {
    pub fn new_from_params(title: String, text: String) -> Result<Self> {
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
            .filter_map(|part| {
                let pos = part.find('=')?;
                let k = part.get(0..pos)?.trim().to_string();
                let v = part.get(pos + 1..)?.trim().to_string();
                Some((k, v))
            })
            .collect();
        Ok(Self { title, params })
    }

    pub fn fix_values(&mut self) {
        self.params = self
            .params
            .iter()
            .map(|(k, v)| (k.to_owned(), v.replace("{{!}}", "|")))
            .collect();
        // TODO proper template replacement
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fix_values() {
        let mut t = Template {
            title: String::new(),
            params: HashMap::from([("foo".to_string(), "bar{{!}}baz".to_string())]),
        };
        t.fix_values();
        assert_eq!(t.params.get("foo"), Some(&"bar|baz".to_string()));
    }
}
