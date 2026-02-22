//! Table rows containing cells with formatted data.

use crate::{
    column_type::ColumnType, listeria_list::ListeriaList, result_cell::ResultCell,
    result_cell_part::ResultCellPart,
};
use futures::future::join_all;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::LazyLock;
use wikimisc::{
    sparql_table_vec::SparqlTableVec,
    wikibase::{Snak, SnakDataType, entity::EntityTrait},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResultRow {
    entity_id: String,
    cells: Vec<ResultCell>,
    section: usize,
    sortkey: String,
    keep: bool,
}

impl ResultRow {
    pub fn new(entity_id: &str) -> Self {
        Self {
            entity_id: entity_id.to_owned(),
            ..Default::default()
        }
    }

    pub const fn set_keep(&mut self, keep: bool) {
        self.keep = keep;
    }

    pub const fn keep(&self) -> bool {
        self.keep
    }

    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub const fn cells(&self) -> &Vec<ResultCell> {
        &self.cells
    }

    pub const fn cells_mut(&mut self) -> &mut Vec<ResultCell> {
        &mut self.cells
    }

    pub const fn section(&self) -> usize {
        self.section
    }

    pub const fn set_section(&mut self, section: usize) {
        self.section = section;
    }

    pub fn sortkey(&self) -> &str {
        &self.sortkey
    }

    pub fn set_cells(&mut self, cells: Vec<ResultCell>) {
        self.cells = cells;
    }

    /// Remove all but the first part of each cell
    pub fn remove_excess_files(&mut self) {
        self.cells.iter_mut().for_each(|cell| {
            if let Some(part) = cell.parts().first() {
                let has_files = matches!(*part.part(), ResultCellPart::File(_));
                if has_files {
                    let first_part = part.clone();
                    cell.set_parts(vec![first_part]);
                }
            }
        });
    }

    /// Remove shadow files from cells
    pub fn remove_shadow_files(&mut self, shadow_files: &HashSet<String>) {
        self.cells.iter_mut().for_each(|cell| {
            cell.set_parts(
                cell.parts()
                    .iter()
                    .filter(|part_with_reference| match part_with_reference.part() {
                        ResultCellPart::File(file) => !shadow_files.contains(file),
                        _ => true,
                    })
                    .cloned()
                    .collect(),
            );
        });
    }

    pub async fn from_columns(&mut self, list: &ListeriaList, sparql_table: &SparqlTableVec) {
        // Clone entity_id so futures can borrow it without conflicting with &mut self
        let entity_id = self.entity_id.clone();
        let futures: Vec<_> = list
            .columns()
            .iter()
            .map(|column| ResultCell::new(list, &entity_id, sparql_table, column))
            .collect();
        self.cells = join_all(futures).await;
    }

    pub fn set_sortkey(&mut self, sortkey: String) {
        self.sortkey = sortkey;
    }

    /// Get the sortkey for the label of the entity
    pub async fn get_sortkey_label(&self, list: &ListeriaList) -> String {
        if list.get_entity(self.entity_id()).await.is_some() {
            list.get_label_with_fallback(self.entity_id()).await
        } else {
            String::new()
        }
    }

    /// Get the sortkey for the family name of the entity
    pub async fn get_sortkey_family_name(&self, page: &ListeriaList) -> String {
        static RE_SR_JR: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r", [JS]r\.$").expect("RE_SR_JR does not parse"));
        static RE_BRACES: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\s+\(.+\)$").expect("RE_BRACES does not parse"));
        static RE_LAST_FIRST: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"^(?P<f>.+) (?P<l>\S+)$").expect("RE_LAST_FIRST does not parse")
        });
        match page.get_entity(&self.entity_id).await {
            Some(entity) => match entity.label_in_locale(page.language()) {
                Some(label) => {
                    let ret = RE_SR_JR.replace_all(label, "");
                    let ret = RE_BRACES.replace_all(&ret, "");
                    let ret = RE_LAST_FIRST.replace_all(&ret, "$l, $f");
                    ret.to_string()
                }
                None => entity.id().to_string(),
            },
            None => "".to_string(),
        }
    }

    fn no_value(datatype: &SnakDataType) -> String {
        match *datatype {
            SnakDataType::Time => "no time",
            SnakDataType::MonolingualText => "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            _ => "",
        }
        .to_string()
    }

    /// Get the sortkey for a property
    pub async fn get_sortkey_prop(
        &self,
        prop: &str,
        list: &ListeriaList,
        datatype: &SnakDataType,
    ) -> String {
        match list.get_entity(&self.entity_id).await {
            Some(entity) => {
                match list
                    .get_filtered_claims(&entity, prop)
                    .iter()
                    .filter(|statement| statement.property() == prop)
                    .map(|statement| statement.main_snak())
                    .next()
                {
                    Some(snak) => self.get_sortkey_from_snak(snak, list).await,
                    None => Self::no_value(datatype),
                }
            }
            None => Self::no_value(datatype),
        }
    }

    /// Get the sortkey for a sparql value
    pub fn get_sortkey_sparql(&self, variable: &str, list: &ListeriaList) -> String {
        let obj = ColumnType::Field(variable.to_lowercase());
        // TODO sort by actual sparql values instead?
        match list
            .columns()
            .iter()
            .enumerate()
            .find(|(_colnum, col)| *col.obj() == obj)
        {
            Some((colnum, _col)) => match self.cells.get(colnum) {
                Some(cell) => cell.get_sortkey(),
                None => String::new(),
            },
            None => String::new(),
        }
    }

    /// Get the sortkey from a snak
    async fn get_sortkey_from_snak(&self, snak: &Snak, list: &ListeriaList) -> String {
        match snak.data_value() {
            Some(data_value) => match data_value.value() {
                wikimisc::wikibase::value::Value::Coordinate(c) => format!(
                    "{}/{}/{}",
                    c.latitude(),
                    c.longitude(),
                    c.precision().unwrap_or(0.0)
                ),
                wikimisc::wikibase::value::Value::MonoLingual(m) => {
                    format!("{}:{}", m.language(), m.text())
                }
                wikimisc::wikibase::value::Value::Entity(entity) => {
                    // TODO language?
                    list.get_label_with_fallback(entity.id()).await
                }
                wikimisc::wikibase::value::Value::Quantity(q) => format!("{}", q.amount()),
                wikimisc::wikibase::value::Value::StringValue(s) => s.to_owned(),
                wikimisc::wikibase::value::Value::Time(t) => t.time().to_owned(),
                wikimisc::wikibase::value::Value::EntitySchema(v) => v.id().to_owned(),
            },
            None => "".to_string(),
        }
    }

    fn compare_entity_ids(&self, other: &ResultRow) -> Ordering {
        let id1 = self.entity_id[1..].parse::<usize>().unwrap_or(0);
        let id2 = other.entity_id[1..].parse::<usize>().unwrap_or(0);
        id1.cmp(&id2)
    }

    pub fn compare_to(&self, other: &ResultRow, datatype: &SnakDataType) -> Ordering {
        match datatype {
            SnakDataType::Quantity => {
                let va = self.sortkey.parse::<u64>().unwrap_or(0);
                let vb = other.sortkey.parse::<u64>().unwrap_or(0);
                if va == 0 && vb == 0 {
                    self.compare_entity_ids(other)
                } else {
                    va.cmp(&vb)
                }
            }
            _ => {
                if self.sortkey == other.sortkey {
                    self.compare_entity_ids(other)
                } else {
                    self.sortkey.cmp(&other.sortkey)
                }
            }
        }
    }

    /// Get the cells as tabbed data
    pub async fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize) -> Value {
        let mut ret = Vec::with_capacity(self.cells.len() + 1);
        ret.push(json!(self.section));
        for (colnum, cell) in self.cells.iter().enumerate() {
            ret.push(cell.as_tabbed_data(list, rownum, colnum).await);
        }
        json!(ret)
    }

    /// Get the cells as wikitext
    fn cells_as_wikitext(list: &ListeriaList, cells: &[String]) -> String {
        cells
            .iter()
            .enumerate()
            .filter_map(|(colnum, cell)| match list.column(colnum) {
                Some(column) => {
                    let value = cell.trim();
                    if value.is_empty() {
                        None
                    } else {
                        Some(format!("{} = {}", column.obj().as_key(), value))
                    }
                }
                _ => None,
            })
            .collect::<Vec<String>>()
            .join("\n| ")
    }

    /// Get the row as wikitext
    pub async fn as_wikitext(&self, list: &ListeriaList, rownum: usize) -> String {
        let futures: Vec<_> = self
            .cells
            .iter()
            .enumerate()
            .map(|(colnum, cell)| cell.as_wikitext(list, rownum, colnum))
            .collect();
        let cells = join_all(futures).await;
        match list.get_row_template() {
            Some(t) => format!(
                "{{{{{}\n| {}\n}}}}",
                t,
                Self::cells_as_wikitext(list, &cells)
            ),
            None => "|".to_string() + &cells.join("\n|"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_cell_part::{PartWithReference, ResultCellPart};
    use std::cmp::Ordering;

    // --- ResultRow basic construction and getters ---

    #[test]
    fn test_new_row() {
        let row = ResultRow::new("Q42");
        assert_eq!(row.entity_id(), "Q42");
        assert!(row.cells().is_empty());
        assert_eq!(row.section(), 0);
        assert_eq!(row.sortkey(), "");
        assert!(!row.keep());
    }

    #[test]
    fn test_set_keep() {
        let mut row = ResultRow::new("Q1");
        assert!(!row.keep());
        row.set_keep(true);
        assert!(row.keep());
        row.set_keep(false);
        assert!(!row.keep());
    }

    #[test]
    fn test_set_section() {
        let mut row = ResultRow::new("Q1");
        assert_eq!(row.section(), 0);
        row.set_section(5);
        assert_eq!(row.section(), 5);
    }

    #[test]
    fn test_set_sortkey() {
        let mut row = ResultRow::new("Q1");
        row.set_sortkey("abc".to_string());
        assert_eq!(row.sortkey(), "abc");
    }

    // --- no_value ---

    #[test]
    fn test_no_value_time() {
        assert_eq!(ResultRow::no_value(&SnakDataType::Time), "no time");
    }

    #[test]
    fn test_no_value_monolingual() {
        let result = ResultRow::no_value(&SnakDataType::MonolingualText);
        assert_eq!(result, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
    }

    #[test]
    fn test_no_value_string() {
        assert_eq!(ResultRow::no_value(&SnakDataType::String), "");
    }

    #[test]
    fn test_no_value_quantity() {
        assert_eq!(ResultRow::no_value(&SnakDataType::Quantity), "");
    }

    #[test]
    fn test_no_value_wikibase_item() {
        assert_eq!(ResultRow::no_value(&SnakDataType::WikibaseItem), "");
    }

    // --- compare_entity_ids ---

    #[test]
    fn test_compare_entity_ids_less() {
        let r1 = ResultRow::new("Q1");
        let r2 = ResultRow::new("Q100");
        assert_eq!(r1.compare_entity_ids(&r2), Ordering::Less);
    }

    #[test]
    fn test_compare_entity_ids_greater() {
        let r1 = ResultRow::new("Q500");
        let r2 = ResultRow::new("Q10");
        assert_eq!(r1.compare_entity_ids(&r2), Ordering::Greater);
    }

    #[test]
    fn test_compare_entity_ids_equal() {
        let r1 = ResultRow::new("Q42");
        let r2 = ResultRow::new("Q42");
        assert_eq!(r1.compare_entity_ids(&r2), Ordering::Equal);
    }

    // --- compare_to ---

    #[test]
    fn test_compare_to_string_different_keys() {
        let mut r1 = ResultRow::new("Q1");
        r1.set_sortkey("apple".to_string());
        let mut r2 = ResultRow::new("Q2");
        r2.set_sortkey("banana".to_string());
        assert_eq!(r1.compare_to(&r2, &SnakDataType::String), Ordering::Less);
    }

    #[test]
    fn test_compare_to_string_same_keys_falls_back_to_entity_id() {
        let mut r1 = ResultRow::new("Q10");
        r1.set_sortkey("same".to_string());
        let mut r2 = ResultRow::new("Q20");
        r2.set_sortkey("same".to_string());
        assert_eq!(r1.compare_to(&r2, &SnakDataType::String), Ordering::Less);
    }

    #[test]
    fn test_compare_to_quantity() {
        let mut r1 = ResultRow::new("Q1");
        r1.set_sortkey("100".to_string());
        let mut r2 = ResultRow::new("Q2");
        r2.set_sortkey("200".to_string());
        assert_eq!(r1.compare_to(&r2, &SnakDataType::Quantity), Ordering::Less);
    }

    #[test]
    fn test_compare_to_quantity_reverse() {
        let mut r1 = ResultRow::new("Q1");
        r1.set_sortkey("999".to_string());
        let mut r2 = ResultRow::new("Q2");
        r2.set_sortkey("1".to_string());
        assert_eq!(
            r1.compare_to(&r2, &SnakDataType::Quantity),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_to_quantity_both_zero_falls_back_to_entity_id() {
        let mut r1 = ResultRow::new("Q5");
        r1.set_sortkey("0".to_string());
        let mut r2 = ResultRow::new("Q10");
        r2.set_sortkey("0".to_string());
        assert_eq!(r1.compare_to(&r2, &SnakDataType::Quantity), Ordering::Less);
    }

    #[test]
    fn test_compare_to_quantity_unparseable_falls_back_to_entity_id() {
        let mut r1 = ResultRow::new("Q1");
        r1.set_sortkey("not_a_number".to_string());
        let mut r2 = ResultRow::new("Q2");
        r2.set_sortkey("also_not".to_string());
        // Both parse to 0, so falls back to entity id comparison
        assert_eq!(r1.compare_to(&r2, &SnakDataType::Quantity), Ordering::Less);
    }

    // --- remove_excess_files ---

    fn make_cell_with_parts(parts: Vec<ResultCellPart>) -> ResultCell {
        let pwrs: Vec<PartWithReference> = parts
            .into_iter()
            .map(|p| PartWithReference::new(p, None))
            .collect();
        serde_json::from_value(serde_json::json!({
            "parts": serde_json::to_value(&pwrs).unwrap(),
            "wdedit_class": null,
            "deduplicate_parts": true
        }))
        .unwrap()
    }

    #[test]
    fn test_remove_excess_files_keeps_first_file() {
        let mut row = ResultRow::new("Q1");
        let cell = make_cell_with_parts(vec![
            ResultCellPart::File("a.jpg".to_string()),
            ResultCellPart::File("b.jpg".to_string()),
            ResultCellPart::File("c.jpg".to_string()),
        ]);
        row.set_cells(vec![cell]);
        row.remove_excess_files();
        assert_eq!(row.cells()[0].parts().len(), 1);
        assert_eq!(
            row.cells()[0].parts()[0].part(),
            &ResultCellPart::File("a.jpg".to_string())
        );
    }

    #[test]
    fn test_remove_excess_files_leaves_non_files_alone() {
        let mut row = ResultRow::new("Q1");
        let cell = make_cell_with_parts(vec![
            ResultCellPart::Text("hello".to_string()),
            ResultCellPart::Text("world".to_string()),
        ]);
        row.set_cells(vec![cell]);
        row.remove_excess_files();
        assert_eq!(row.cells()[0].parts().len(), 2);
    }

    #[test]
    fn test_remove_excess_files_empty_cell() {
        let mut row = ResultRow::new("Q1");
        let cell = make_cell_with_parts(vec![]);
        row.set_cells(vec![cell]);
        row.remove_excess_files();
        assert!(row.cells()[0].parts().is_empty());
    }

    // --- remove_shadow_files ---

    #[test]
    fn test_remove_shadow_files_removes_matching() {
        let mut row = ResultRow::new("Q1");
        let cell = make_cell_with_parts(vec![
            ResultCellPart::File("shadow.jpg".to_string()),
            ResultCellPart::File("good.jpg".to_string()),
        ]);
        row.set_cells(vec![cell]);
        let shadow: HashSet<String> = ["shadow.jpg".to_string()].into();
        row.remove_shadow_files(&shadow);
        assert_eq!(row.cells()[0].parts().len(), 1);
        assert_eq!(
            row.cells()[0].parts()[0].part(),
            &ResultCellPart::File("good.jpg".to_string())
        );
    }

    #[test]
    fn test_remove_shadow_files_preserves_non_files() {
        let mut row = ResultRow::new("Q1");
        let cell = make_cell_with_parts(vec![
            ResultCellPart::Text("text".to_string()),
            ResultCellPart::File("shadow.jpg".to_string()),
        ]);
        row.set_cells(vec![cell]);
        let shadow: HashSet<String> = ["shadow.jpg".to_string()].into();
        row.remove_shadow_files(&shadow);
        assert_eq!(row.cells()[0].parts().len(), 1);
        assert_eq!(
            row.cells()[0].parts()[0].part(),
            &ResultCellPart::Text("text".to_string())
        );
    }

    #[test]
    fn test_remove_shadow_files_empty_shadow_set() {
        let mut row = ResultRow::new("Q1");
        let cell = make_cell_with_parts(vec![ResultCellPart::File("keep.jpg".to_string())]);
        row.set_cells(vec![cell]);
        let shadow: HashSet<String> = HashSet::new();
        row.remove_shadow_files(&shadow);
        assert_eq!(row.cells()[0].parts().len(), 1);
    }

    // --- Default trait ---

    #[test]
    fn test_default_row() {
        let row = ResultRow::default();
        assert_eq!(row.entity_id(), "");
        assert!(row.cells().is_empty());
        assert_eq!(row.section(), 0);
        assert_eq!(row.sortkey(), "");
        assert!(!row.keep());
    }

    // --- Clone trait ---

    #[test]
    fn test_clone_row() {
        let mut row = ResultRow::new("Q42");
        row.set_sortkey("key".to_string());
        row.set_section(3);
        row.set_keep(true);
        let cloned = row.clone();
        assert_eq!(cloned.entity_id(), "Q42");
        assert_eq!(cloned.sortkey(), "key");
        assert_eq!(cloned.section(), 3);
        assert!(cloned.keep());
    }

    // --- cells_mut ---

    #[test]
    fn test_cells_mut() {
        let mut row = ResultRow::new("Q1");
        row.set_cells(vec![make_cell_with_parts(vec![ResultCellPart::Number])]);
        assert_eq!(row.cells().len(), 1);
        row.cells_mut().clear();
        assert!(row.cells().is_empty());
    }
}
