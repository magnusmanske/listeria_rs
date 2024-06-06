use std::cmp::Ordering;

use crate::result_row::ResultRow;

#[derive(Debug, Clone, Default)]
pub struct Results {
    results: Vec<ResultRow>,
    len: usize,
}

impl Results {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, row: ResultRow) {
        self.results.push(row);
        self.len += 1;
    }

    pub fn get(&self, pos: usize) -> Option<ResultRow> {
        if pos < self.len {
            return Some(self.results[pos].clone());
        }
        None
    }

    pub fn set(&mut self, pos: usize, row: ResultRow) {
        if pos < self.len {
            self.results[pos] = row;
        } else {
            panic!("Attempting to set out-of-bounds result {pos}");
        }
    }

    pub fn clear(&mut self) {
        self.results.clear();
        self.len = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn keep_marked(&mut self) {
        let mut write_pos = 0;
        for read_pos in 0..self.len {
            let row = self.get(read_pos).unwrap();
            if row.keep() {
                if read_pos != write_pos {
                    self.set(write_pos, row);
                }
                write_pos += 1;
            }
        }
        while self.len > write_pos {
            self.results.pop();
            self.len -= 1;
        }
    }

    pub fn sort_by<F>(&mut self, mut f: F)
    where
        F: FnMut(&ResultRow, &ResultRow) -> Ordering,
    {
        let n = self.len;
        for i in 0..n {
            let mut min_idx = i;
            for j in i + 1..n {
                let row_j = self.get(j).unwrap();
                let row_min_idx = self.get(min_idx).unwrap();
                if f(&row_j, &row_min_idx) == Ordering::Less {
                    min_idx = j;
                }
            }
            self.swap(i, min_idx);
        }
    }

    fn swap(&mut self, idx1: usize, idx2: usize) {
        if idx1 >= self.len || idx2 >= self.len {
            panic!("Attempting to swap out-of-bounds results");
        }
        let row1 = self.get(idx1).unwrap();
        let row2 = self.get(idx2).unwrap();
        self.set(idx1, row2);
        self.set(idx2, row1);
    }

    pub fn reverse(&mut self) {
        let mut front = 0;
        let mut end = self.len - 1;
        while front < end {
            self.swap(front, end);
            front += 1;
            end -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keep_marked() {
        let mut results = Results::new();
        results.push(ResultRow::new("a"));
        results.push(ResultRow::new("b"));
        results.push(ResultRow::new("c"));
        let mut row = results.get(0).unwrap();
        row.set_keep(true);
        results.set(0, row);
        let mut row = results.get(2).unwrap();
        row.set_keep(true);
        results.set(2, row);
        results.keep_marked();
        assert_eq!(results.len(), 2);
        assert_eq!(results.get(0).unwrap().entity_id(), "a");
        assert_eq!(results.get(1).unwrap().entity_id(), "c");
    }

    #[test]
    fn test_reverse() {
        let mut results = Results::new();
        results.push(ResultRow::new("a"));
        results.push(ResultRow::new("b"));
        results.push(ResultRow::new("c"));
        results.reverse();
        assert_eq!(results.len(), 3);
        assert_eq!(results.get(0).unwrap().entity_id(), "c");
        assert_eq!(results.get(1).unwrap().entity_id(), "b");
        assert_eq!(results.get(2).unwrap().entity_id(), "a");
    }

    #[test]
    fn test_sort_by() {
        let mut results = Results::new();
        results.push(ResultRow::new("c"));
        results.push(ResultRow::new("b"));
        results.push(ResultRow::new("a"));
        results.sort_by(|a, b| a.entity_id().cmp(b.entity_id()));
        assert_eq!(results.len(), 3);
        assert_eq!(results.get(0).unwrap().entity_id(), "a");
        assert_eq!(results.get(1).unwrap().entity_id(), "b");
        assert_eq!(results.get(2).unwrap().entity_id(), "c");
    }
}
