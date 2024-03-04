use crate::configuration::Configuration;
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SiteMatrix {
    site_matrix: Value,
}

impl SiteMatrix {
    pub async fn new(config: &Configuration) -> Result<Self> {
        // Load site matrix
        let api = config.get_default_wbapi()?;
        let params: HashMap<String, String> = [("action", "sitematrix")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let site_matrix = api.get_query_api_json(&params).await?;
        Ok(Self { site_matrix })
    }

    fn get_url_for_wiki_from_site(&self, wiki: &str, site: &Value) -> Option<String> {
        self.get_value_from_site_matrix_entry(wiki, site, "dbname", "url")
    }

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
