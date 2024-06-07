use std::collections::HashMap;
use wikimisc::{file_vec::FileVec, sparql_results::SparqlResultRows, sparql_value::SparqlValue};

const MAX_MEM_ROWS: usize = 5;

#[derive(Debug, Clone)]
pub struct SparqlTable {
    headers: HashMap<String, usize>,
    rows_file: FileVec<Vec<SparqlValue>>,
    rows_mem: Vec<Vec<SparqlValue>>,
    main_variable: Option<String>,
    use_disk: bool,
}

impl Default for SparqlTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SparqlTable {
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
            rows_file: FileVec::new(),
            rows_mem: Vec::new(),
            main_variable: None,
            use_disk: false,
        }
    }

    pub fn from_table(other: &SparqlTable) -> Self {
        Self {
            headers: other.headers.clone(),
            rows_file: FileVec::new(),
            rows_mem: Vec::new(),
            main_variable: other.main_variable.clone(),
            use_disk: false,
        }
    }

    pub fn len(&self) -> usize {
        self.rows_file.len() + self.rows_mem.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows_file.is_empty() && self.rows_mem.is_empty()
    }

    pub fn get_row_col(&self, row_id: usize, col_id: usize) -> Option<SparqlValue> {
        if self.use_disk {
            self.rows_file
                .get(row_id)?
                .get(col_id)
                .map(|v| v.to_owned())
        } else {
            self.rows_mem.get(row_id)?.get(col_id).map(|v| v.to_owned())
        }
    }

    /// Get the index of a variable in the table. Case-insensitive.
    pub fn get_var_index(&self, var: &str) -> Option<usize> {
        let var = var.to_lowercase();
        self.headers
            .iter()
            .find(|(name, _num)| name.to_lowercase() == var)
            .map(|(_, num)| *num)
    }

    pub fn push(&mut self, row: Vec<SparqlValue>) {
        if self.use_disk {
            self.rows_file.push(row);
        } else {
            self.rows_mem.push(row);
            self.flush_to_disk();
        }
    }

    fn flush_to_disk(&mut self) {
        if !self.use_disk && self.rows_mem.len() > MAX_MEM_ROWS {
            self.use_disk = true;
            for row in self.rows_mem.drain(..) {
                self.rows_file.push(row);
            }
        }
    }

    pub fn annotate_row(&self, row: &[SparqlValue]) -> HashMap<String, SparqlValue> {
        self.headers
            .iter()
            .enumerate()
            .map(|(i, (k, _))| (k.clone(), row[i].clone()))
            .collect()
    }

    pub fn get(&self, row_id: usize) -> Option<Vec<SparqlValue>> {
        if self.use_disk {
            self.rows_file.get(row_id).map(|r| r.to_owned())
        } else {
            self.rows_mem.get(row_id).map(|r| r.to_owned())
        }
    }

    pub fn headers(&self) -> &HashMap<String, usize> {
        &self.headers
    }

    fn push_sparql_result_row(&mut self, row: &HashMap<String, SparqlValue>) {
        if self.headers.is_empty() {
            self.headers = row
                .iter()
                .enumerate()
                .map(|(i, (k, _))| (k.clone(), i))
                .collect();
        }
        let new_row: Vec<SparqlValue> = row.iter().map(|(_, v)| v.clone()).collect();
        self.push(new_row);
    }

    pub fn main_variable(&self) -> Option<&String> {
        self.main_variable.as_ref()
    }

    pub fn set_main_variable(&mut self, main_variable: Option<String>) {
        self.main_variable = main_variable;
    }

    pub fn main_column(&self) -> Option<usize> {
        self.main_variable
            .as_ref()
            .and_then(|var| self.headers.get(var).copied())
    }
}

impl From<SparqlResultRows> for SparqlTable {
    fn from(rows: SparqlResultRows) -> Self {
        let mut table = Self::new();
        for row in rows {
            table.push_sparql_result_row(&row);
        }
        table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn test_push() {
        let mut table = SparqlTable::new();
        table.push(vec![SparqlValue::Literal("a".to_string())]);
        table.push(vec![SparqlValue::Literal("b".to_string())]);
        table.push(vec![SparqlValue::Literal("c".to_string())]);
        assert_eq!(table.len(), 3);
        assert_eq!(
            table.get(0),
            Some(vec![SparqlValue::Literal("a".to_string())])
        );
        assert_eq!(
            table.get(1),
            Some(vec![SparqlValue::Literal("b".to_string())])
        );
        assert_eq!(
            table.get(2),
            Some(vec![SparqlValue::Literal("c".to_string())])
        );
    }

    #[test]
    fn test_from_sparql_result_rows() {
        let rows = vec![
            HashMap::from_iter(vec![(
                "a".to_string(),
                SparqlValue::Literal("1".to_string()),
            )]),
            HashMap::from_iter(vec![(
                "a".to_string(),
                SparqlValue::Literal("2".to_string()),
            )]),
            HashMap::from_iter(vec![(
                "a".to_string(),
                SparqlValue::Literal("3".to_string()),
            )]),
        ];
        let table = SparqlTable::from(SparqlResultRows::from(rows));
        assert_eq!(table.len(), 3);
        assert_eq!(
            table.get(0),
            Some(vec![SparqlValue::Literal("1".to_string())])
        );
        assert_eq!(
            table.get(1),
            Some(vec![SparqlValue::Literal("2".to_string())])
        );
        assert_eq!(
            table.get(2),
            Some(vec![SparqlValue::Literal("3".to_string())])
        );
    }

    #[test]
    fn test_get_var_index() {
        let mut table = SparqlTable::new();
        table.headers.insert("a".to_string(), 0);
        table.headers.insert("b".to_string(), 1);
        assert_eq!(table.get_var_index("a"), Some(0));
        assert_eq!(table.get_var_index("b"), Some(1));
        assert_eq!(table.get_var_index("c"), None);
    }

    #[test]
    fn test_mem_to_disk() {
        let mut table = SparqlTable::new();
        // Add a few, should stay in memory for speed
        for row_id in 0..MAX_MEM_ROWS {
            table.push(vec![SparqlValue::Literal(row_id.to_string())]);
        }
        assert_eq!(table.len(), MAX_MEM_ROWS);
        assert_eq!(table.rows_mem.len(), MAX_MEM_ROWS);
        assert_eq!(table.rows_file.len(), 0);
        assert_eq!(table.use_disk, false);

        // Add one more, that should flush everything onto disk
        table.push(vec![SparqlValue::Literal("one too many".to_string())]);
        assert_eq!(table.len(), MAX_MEM_ROWS + 1);
        assert_eq!(table.rows_mem.len(), 0);
        assert_eq!(table.rows_file.len(), MAX_MEM_ROWS + 1);
        assert_eq!(table.use_disk, true);
    }
}
