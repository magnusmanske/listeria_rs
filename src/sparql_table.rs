use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use wikimisc::{sparql_results::SparqlResultRows, sparql_value::SparqlValue};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SparqlTable {
    headers: HashMap<String, usize>,
    rows: Vec<Vec<SparqlValue>>,
    main_variable: Option<String>,
}

impl SparqlTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_table(other: &SparqlTable) -> Self {
        Self {
            headers: other.headers.clone(),
            ..Default::default()
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn get_row_col(&self, row: usize, col: usize) -> Option<&SparqlValue> {
        self.rows.get(row).and_then(|r| r.get(col))
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
        self.rows.push(row);
    }

    pub fn annotate_row(&self, row: &[SparqlValue]) -> HashMap<String, SparqlValue> {
        self.headers
            .iter()
            .enumerate()
            .map(|(i, (k, _))| (k.clone(), row[i].clone()))
            .collect()
    }

    pub fn get(&self, row_id: usize) -> Option<Vec<SparqlValue>> {
        self.rows.get(row_id).map(|r| r.to_owned())
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
        self.rows.push(new_row);
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
