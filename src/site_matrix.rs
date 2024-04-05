use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashMap;
use wikibase::mediawiki::api::Api;

#[derive(Debug, Clone)]
pub struct SiteMatrix {
    site_matrix: Value,
}

impl SiteMatrix {
    /// Create a new SiteMatrix object
    pub async fn new(api: &Api) -> Result<Self> {
        let params = Self::str_vec_to_hashmap(&[("action", "sitematrix")]);
        let site_matrix = api.get_query_api_json(&params).await?;
        Ok(Self { site_matrix })
    }

    /// Convert a vector of string tuples to a hashmap of String keys and values
    pub fn str_vec_to_hashmap(v: &[(&str, &str)]) -> HashMap<String, String> {
        v.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Get the URL for a wiki from a site matrix entry
    fn get_url_for_wiki_from_site(&self, wiki: &str, site: &Value) -> Option<String> {
        self.get_value_from_site_matrix_entry(wiki, site, "dbname", "url")
    }

    /// Get the value for a key from a site matrix entry
    fn get_value_from_site_matrix_entry(
        &self,
        value: &str,
        site: &Value,
        key_match: &str,
        key_return: &str,
    ) -> Option<String> {
        if site["closed"].as_str().is_some() {
            return None;
        }
        if site["private"].as_str().is_some() {
            return None;
        }
        match site[key_match].as_str() {
            Some(site_url) => {
                if value == site_url {
                    site[key_return].as_str().map(|url| url.to_string())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Get the server URL for a wiki
    pub fn get_server_url_for_wiki(&self, wiki: &str) -> Result<String> {
        match wiki.replace('_', "-").as_str() {
            "be-taraskwiki" | "be-x-oldwiki" => {
                return Ok("https://be-tarask.wikipedia.org".to_string())
            }
            "metawiki" => return Ok("https://meta.wikimedia.org".to_string()),
            _ => {}
        }
        self.site_matrix["sitematrix"]
            .as_object()
            .ok_or_else(|| {
                anyhow!("ListeriaBot::get_server_url_for_wiki: sitematrix not an object")
            })?
            .iter()
            .filter_map(|(id, data)| match id.as_str() {
                "count" => None,
                "specials" => data
                    .as_array()?
                    .iter()
                    .filter_map(|site| self.get_url_for_wiki_from_site(wiki, site))
                    .next(),
                _other => match data["site"].as_array() {
                    Some(sites) => sites
                        .iter()
                        .filter_map(|site| self.get_url_for_wiki_from_site(wiki, site))
                        .next(),
                    None => None,
                },
            })
            .next()
            .ok_or(anyhow!(
                "AppState::get_server_url_for_wiki: Cannot find server for wiki '{wiki}'"
            ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_site_matrix() {
        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        let site_matrix = SiteMatrix::new(&api).await.unwrap();
        let url = site_matrix.get_server_url_for_wiki("wikidatawiki").unwrap();
        assert_eq!(url, "https://www.wikidata.org");
    }

    #[test]
    fn test_str_vec_to_hashmap() {
        let v = vec![("a", "b"), ("c", "d")];
        let h = SiteMatrix::str_vec_to_hashmap(&v);
        assert_eq!(h.get("a"), Some(&"b".to_string()));
        assert_eq!(h.get("c"), Some(&"d".to_string()));
    }

    #[test]
    fn test_get_value_from_site_matrix_entry() {
        let site = serde_json::json!({
            "dbname": "wikidatawiki",
            "url": "https://www.wikidata.org",
            "closed": false,
            "private": false
        });
        let site_matrix = SiteMatrix {
            site_matrix: serde_json::json!({}),
        };
        let url =
            site_matrix.get_value_from_site_matrix_entry("wikidatawiki", &site, "dbname", "url");
        assert_eq!(url, Some("https://www.wikidata.org".to_string()));
    }

    #[test]
    fn test_get_url_for_wiki_from_site() {
        let site = serde_json::json!({
            "dbname": "wikidatawiki",
            "url": "https://www.wikidata.org",
            "closed": false,
            "private": false
        });
        let site_matrix = SiteMatrix {
            site_matrix: serde_json::json!({}),
        };
        let url = site_matrix.get_url_for_wiki_from_site("wikidatawiki", &site);
        assert_eq!(url, Some("https://www.wikidata.org".to_string()));
    }

    #[test]
    fn test_get_server_url_for_wiki() {
        let site_matrix = SiteMatrix {
            site_matrix: serde_json::json!({
                "sitematrix": {
                    "count": 1,
                    "specials": [
                        {
                            "dbname": "wikidatawiki",
                            "url": "https://www.wikidata.org",
                            "closed": false,
                            "private": false
                        }
                    ]
                }
            }),
        };
        let url = site_matrix.get_server_url_for_wiki("wikidatawiki").unwrap();
        assert_eq!(url, "https://www.wikidata.org");
    }

    #[test]
    fn test_get_server_url_for_wiki_be_taraskwiki() {
        let site_matrix = SiteMatrix {
            site_matrix: serde_json::json!({
                "sitematrix": {
                    "count": 1,
                    "specials": [
                        {
                            "dbname": "be-taraskwiki",
                            "url": "https://be-tarask.wikipedia.org",
                            "closed": false,
                            "private": false
                        }
                    ]
                }
            }),
        };
        let url = site_matrix
            .get_server_url_for_wiki("be-taraskwiki")
            .unwrap();
        assert_eq!(url, "https://be-tarask.wikipedia.org");
    }

    #[test]
    fn test_get_server_url_for_wiki_metawiki() {
        let site_matrix = SiteMatrix {
            site_matrix: serde_json::json!({
                "sitematrix": {
                    "count": 1,
                    "specials": [
                        {
                            "dbname": "metawiki",
                            "url": "https://meta.wikimedia.org",
                            "closed": false,
                            "private": false
                        }
                    ]
                }
            }),
        };
        let url = site_matrix.get_server_url_for_wiki("metawiki").unwrap();
        assert_eq!(url, "https://meta.wikimedia.org");
    }

    #[test]
    fn test_get_server_url_for_wiki_not_found() {
        let site_matrix = SiteMatrix {
            site_matrix: serde_json::json!({
                "sitematrix": {
                    "count": 1,
                    "specials": [
                        {
                            "dbname": "wikidatawiki",
                            "url": "https://www.wikidata.org",
                            "closed": false,
                            "private": false
                        }
                    ]
                }
            }),
        };
        let url = site_matrix.get_server_url_for_wiki("notfoundwiki");
        assert!(url.is_err());
    }
}
