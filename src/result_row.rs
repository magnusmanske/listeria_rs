use crate::column::ColumnType;
use crate::listeria_list::*;
use crate::result_cell::ResultCell;
use crate::result_cell_part::ResultCellPart;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashSet;
use wikimisc::sparql_table::SparqlTable;
use wikimisc::wikibase::entity::EntityTrait;
use wikimisc::wikibase::{Snak, SnakDataType};

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

    pub fn set_keep(&mut self, keep: bool) {
        self.keep = keep;
    }

    pub fn keep(&self) -> bool {
        self.keep
    }

    pub fn entity_id(&self) -> &String {
        &self.entity_id
    }

    pub fn cells(&self) -> &Vec<ResultCell> {
        &self.cells
    }

    pub fn cells_mut(&mut self) -> &mut Vec<ResultCell> {
        &mut self.cells
    }

    pub fn section(&self) -> usize {
        self.section
    }

    pub fn set_section(&mut self, section: usize) {
        self.section = section;
    }

    pub fn sortkey(&self) -> &String {
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
                    let mut parts = cell.parts().clone();
                    parts.truncate(1);
                    cell.set_parts(parts.to_vec());
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

    pub fn from_columns(&mut self, list: &ListeriaList, sparql_table: &SparqlTable) {
        self.cells.clear();
        for column in list.columns().iter() {
            let x = ResultCell::new(list, &self.entity_id, sparql_table, column);
            self.cells.push(x);
        }
    }

    pub fn set_sortkey(&mut self, sortkey: String) {
        self.sortkey = sortkey;
    }

    /// Get the sortkey for the label of the entity
    pub fn get_sortkey_label(&self, list: &ListeriaList) -> String {
        match list.get_entity(self.entity_id()) {
            Some(_entity) => list.get_label_with_fallback(self.entity_id()),
            None => "".to_string(),
        }
    }

    /// Get the sortkey for the family name of the entity
    pub fn get_sortkey_family_name(&self, page: &ListeriaList) -> String {
        lazy_static! {
            static ref RE_SR_JR: Regex =
                Regex::new(r", [JS]r\.$").expect("RE_SR_JR does not parse");
            static ref RE_BRACES: Regex =
                Regex::new(r"\s+\(.+\)$").expect("RE_BRACES does not parse");
            static ref RE_LAST_FIRST: Regex =
                Regex::new(r"^(?P<f>.+) (?P<l>\S+)$").expect("RE_LAST_FIRST does not parse");
        }
        match page.get_entity(&self.entity_id) {
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

    fn no_value(&self, datatype: &SnakDataType) -> String {
        match *datatype {
            SnakDataType::Time => "no time",
            SnakDataType::MonolingualText => "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            _ => "",
        }
        .to_string()
    }

    /// Get the sortkey for a property
    pub fn get_sortkey_prop(
        &self,
        prop: &str,
        list: &ListeriaList,
        datatype: &SnakDataType,
    ) -> String {
        match list.get_entity(&self.entity_id) {
            Some(entity) => {
                match list
                    .get_filtered_claims(&entity, prop)
                    .iter()
                    .filter(|statement| statement.property() == prop)
                    .map(|statement| statement.main_snak())
                    .next()
                {
                    Some(snak) => self.get_sortkey_from_snak(snak, list),
                    None => self.no_value(datatype),
                }
            }
            None => self.no_value(datatype),
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
    fn get_sortkey_from_snak(&self, snak: &Snak, list: &ListeriaList) -> String {
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
                    list.get_label_with_fallback(entity.id())
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
        let id1 = self.entity_id[1..]
            .parse::<usize>()
            .ok()
            .or(Some(0))
            .unwrap_or(0);
        let id2 = other.entity_id[1..]
            .parse::<usize>()
            .ok()
            .or(Some(0))
            .unwrap_or(0);
        id1.partial_cmp(&id2).unwrap_or(Ordering::Equal)
    }

    pub fn compare_to(&self, other: &ResultRow, datatype: &SnakDataType) -> Ordering {
        match datatype {
            SnakDataType::Quantity => {
                let va = self.sortkey.parse::<u64>().ok().or(Some(0)).unwrap_or(0);
                let vb = other.sortkey.parse::<u64>().ok().or(Some(0)).unwrap_or(0);
                if va == 0 && vb == 0 {
                    self.compare_entity_ids(other)
                } else {
                    va.partial_cmp(&vb).unwrap_or(Ordering::Equal)
                }
            }
            _ => {
                if self.sortkey == other.sortkey {
                    self.compare_entity_ids(other)
                } else {
                    self.sortkey
                        .partial_cmp(&other.sortkey)
                        .unwrap_or(Ordering::Equal)
                }
            }
        }
    }

    /// Get the cells as tabbed data
    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize) -> Value {
        let mut ret = vec![];
        for (colnum, cell) in self.cells.iter().enumerate() {
            ret.push(cell.as_tabbed_data(list, rownum, colnum));
        }
        ret.insert(0, json!(self.section));
        json!(ret)
    }

    /// Get the cells as wikitext
    fn cells_as_wikitext(&self, list: &ListeriaList, cells: &[String]) -> String {
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
    pub fn as_wikitext(&mut self, list: &ListeriaList, rownum: usize) -> String {
        let cells: Vec<String> = self
            .cells
            .iter_mut()
            .enumerate()
            .map(|(colnum, cell)| cell.as_wikitext(list, rownum, colnum))
            .collect();
        match list.get_row_template() {
            Some(t) => format!(
                "{{{{{}\n| {}\n}}}}",
                t,
                self.cells_as_wikitext(list, &cells)
            ),
            None => "|".to_string() + &cells.join("\n|"),
        }
    }
}
