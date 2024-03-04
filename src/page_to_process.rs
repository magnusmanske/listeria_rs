use mysql_async::from_row;

#[derive(Debug, Clone, Default)]
pub struct PageToProcess {
    pub id: u64,
    pub title: String,
    pub status: String,
    pub wiki: String,
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
}
