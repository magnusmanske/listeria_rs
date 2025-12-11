use anyhow::{Result, anyhow};
use mysql_async::{Conn, from_row, prelude::*};

#[derive(Debug, Clone, PartialEq)]
pub enum WikiStatus {
    Active,
    Ignored,
    Blocked,
}

impl WikiStatus {
    pub fn new_from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "active" => Ok(WikiStatus::Active),
            "ignored" => Ok(WikiStatus::Ignored),
            "blocked" => Ok(WikiStatus::Blocked),
            _ => Err(anyhow!("Unknown WikiStatus: {}", s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Wiki {
    id: usize,
    name: String,
    status: WikiStatus,
    timestamp: String,
    use_invoke: bool,
    use_cite_web: bool,
}

impl Wiki {
    pub fn from_row(r: (usize, String, String, String, bool, bool)) -> Result<Self> {
        Ok(Self {
            id: r.0,
            name: r.1,
            status: WikiStatus::new_from_str(&r.2)?,
            timestamp: r.3,
            use_invoke: r.4,
            use_cite_web: r.5,
        })
    }

    pub async fn from_db(conn: &mut Conn, wiki: &str) -> Result<Self> {
        let result = conn
	     .exec_iter(
	         "SELECT `id`,`name`,`status`,`timestamp`,`use_invoke`,`use_cite_web` FROM `wikis` WHERE `name`",
	         (wiki,),
	     )
	     .await?
	     .map_and_drop(from_row::<(usize, String, String, String, bool, bool)>)
	     .await?;
        let result = match result.first() {
            Some(r) => r,
            None => return Err(anyhow!("Wiki not found: {}", wiki)),
        };
        Self::from_row(result.to_owned())
    }

    pub const fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn status(&self) -> &WikiStatus {
        &self.status
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }

    pub const fn use_invoke(&self) -> bool {
        self.use_invoke
    }

    pub const fn use_cite_web(&self) -> bool {
        self.use_cite_web
    }

    pub fn is_active(&self) -> bool {
        self.status == WikiStatus::Active
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{configuration::Configuration, wiki_apis::WikiApis};

    use super::*;

    #[test]
    fn test_wiki_status_from_str() {
        assert_eq!(
            WikiStatus::new_from_str("active").unwrap(),
            WikiStatus::Active
        );
        assert_eq!(
            WikiStatus::new_from_str("ignored").unwrap(),
            WikiStatus::Ignored
        );
        assert_eq!(
            WikiStatus::new_from_str("blocked").unwrap(),
            WikiStatus::Blocked
        );
        assert!(WikiStatus::new_from_str("foo").is_err());
    }

    #[test]
    fn test_wiki_from_row() {
        let r = (
            1,
            "foo".to_string(),
            "active".to_string(),
            "20200825221705".to_string(),
            true,
            false,
        );
        let w = Wiki::from_row(r).unwrap();
        assert_eq!(w.id(), 1);
        assert_eq!(w.name(), "foo");
        assert_eq!(*w.status(), WikiStatus::Active);
        assert_eq!(w.timestamp(), "20200825221705");
        assert!(w.use_invoke());
        assert!(!w.use_cite_web());
    }

    #[test]
    fn test_wiki_from_row_error() {
        let r = (
            1,
            "foo".to_string(),
            "foo".to_string(),
            "20200825221705".to_string(),
            true,
            false,
        );
        assert!(Wiki::from_row(r).is_err());
    }

    #[test]
    fn test_wiki_is_active() {
        let w = Wiki {
            id: 1,
            name: "foo".to_string(),
            status: WikiStatus::Active,
            timestamp: "20200825221705".to_string(),
            use_invoke: true,
            use_cite_web: false,
        };
        assert!(w.is_active());
    }

    #[test]
    fn test_wiki_is_not_active() {
        let w = Wiki {
            id: 1,
            name: "foo".to_string(),
            status: WikiStatus::Ignored,
            timestamp: "20200825221705".to_string(),
            use_invoke: true,
            use_cite_web: false,
        };
        assert!(!w.is_active());
    }

    #[tokio::test]
    async fn test_wiki_use_flags() {
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let wiki_apis = WikiApis::new(Arc::new(config)).await.unwrap();
        let wikis = wiki_apis.get_all_wikis_in_database().await.unwrap();
        assert!(wikis.get("enwiki").unwrap().use_invoke());
        assert!(wikis.get("enwiki").unwrap().use_cite_web());
        assert!(!wikis.get("frwiki").unwrap().use_invoke());
        assert!(wikis.get("frwiki").unwrap().use_cite_web());
        assert!(!wikis.get("huwiki").unwrap().use_invoke());
        assert!(!wikis.get("huwiki").unwrap().use_cite_web());
    }
}
