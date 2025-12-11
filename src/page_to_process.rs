use mysql_async::from_row;

#[derive(Debug, Clone, Default)]
pub struct PageToProcess {
    id: u64,
    title: String,
    status: String,
    wiki: String,
}

impl PageToProcess {
    pub fn from_parts(parts: (u64, String, String, String)) -> Self {
        Self {
            id: parts.0,
            title: parts.1,
            status: parts.2,
            wiki: parts.3,
        }
    }

    pub fn from_row(row: mysql_async::Row) -> Self {
        let parts = from_row::<(u64, String, String, String)>(row);
        Self::from_parts(parts)
    }

    pub const fn id(&self) -> u64 {
        self.id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn wiki(&self) -> &str {
        &self.wiki
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_parts() {
        let p2p = PageToProcess::from_parts((
            123,
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
        ));
        assert_eq!(p2p.id(), 123);
        assert_eq!(p2p.title(), "foo");
        assert_eq!(p2p.status(), "bar");
        assert_eq!(p2p.wiki(), "baz");
    }
}
