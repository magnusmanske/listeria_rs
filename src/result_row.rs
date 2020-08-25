use crate::{serde_json, HashMap, SparqlValue};
use crate::result_cell::{ResultCell, ResultCellPart};
use crate::listeria_list::*;
use wikibase::entity::EntityTrait;
use regex::Regex;
use serde_json::Value;
use std::cmp::Ordering;
use wikibase::SnakDataType;

#[derive(Debug, Clone, Default)]
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

    pub fn set_keep(&mut self, keep:bool ) {
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

    pub fn set_section(&mut self, section:usize) {
        self.section = section ;
    }

    pub fn sortkey(&self) -> &String {
        &self.sortkey
    }

    pub fn set_cells(&mut self,cells: Vec<ResultCell>) {
        self.cells = cells;
    }

    pub fn remove_shadow_files(&mut self,shadow_files:&[String]) {
        self.cells.iter_mut().for_each(|cell|{
            cell.set_parts ( cell.parts().iter().filter(|part|{
                match part {
                    ResultCellPart::File(file) => !shadow_files.contains(file),
                    _ => true
                }
            })
            .cloned()
            .collect());
        });
    }

    pub async fn from_columns(&mut self, list:&ListeriaList, sparql_rows: &[&HashMap<String, SparqlValue>]) {
        self.cells.clear();
        for column in list.columns().iter() {
            let x = ResultCell::new(list, &self.entity_id, sparql_rows, column).await;
            self.cells.push(x);
        }
    }

    pub fn set_sortkey(&mut self, sortkey: String) {
        self.sortkey = sortkey;
    }

    pub fn get_sortkey_label(&self, page: &ListeriaList) -> String {
        match page.get_entity(self.entity_id.to_owned()) {
            Some(entity) => match entity.label_in_locale(&page.language()) {
                Some(label) => label.to_string(),
                None => entity.id().to_string(),
            },
            None => "".to_string(),
        }
    }

    pub fn get_sortkey_family_name(&self, page: &ListeriaList) -> String {
        // TODO lazy
        let re_sr_jr = Regex::new(r", [JS]r\.$").unwrap();
        let re_braces = Regex::new(r"\s+\(.+\)$").unwrap();
        let re_last_first = Regex::new(r"^(?P<f>.+) (?P<l>\S+)$").unwrap();
        match page.get_entity(self.entity_id.to_owned()) {
            Some(entity) => match entity.label_in_locale(&page.language()) {
                Some(label) => {
                    let ret = re_sr_jr.replace_all(label, "");
                    let ret = re_braces.replace_all(&ret, "");
                    let ret = re_last_first.replace_all(&ret, "$l, $f");
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

    pub fn get_sortkey_prop(
        &self,
        prop: &str,
        list: &ListeriaList,
        datatype: &SnakDataType,
    ) -> String {
        match list.get_entity(self.entity_id.to_owned()) {
            Some(entity) => {
                match list.get_filtered_claims(&entity,prop) // entity.claims()
                    .iter()
                    .filter(|statement| statement.property() == prop)
                    .map(|statement| statement.main_snak())
                    .next()
                {
                    Some(snak) => self.get_sortkey_from_snak(snak,list),
                    None => self.no_value(datatype),
                }
            }
            None => self.no_value(datatype),
        }
    }

    fn get_sortkey_from_snak(&self, snak: &wikibase::snak::Snak,list: &ListeriaList) -> String {
        match snak.data_value() {
            Some(data_value) => match data_value.value() {
                wikibase::value::Value::Coordinate(c) => format!(
                    "{}/{}/{}",
                    c.latitude(),
                    c.longitude(),
                    c.precision().unwrap_or(0.0)
                ),
                wikibase::value::Value::MonoLingual(m) => format!("{}:{}", m.language(), m.text()),
                wikibase::value::Value::Entity(entity) => {// TODO language
                    list.get_label_with_fallback(&entity.id())
                }
                wikibase::value::Value::Quantity(q) => format!("{}", q.amount()),
                wikibase::value::Value::StringValue(s) => s.to_owned(),
                wikibase::value::Value::Time(t) => t.time().to_owned(),
            },
            None => "".to_string(),
        }
    }

    fn compare_entiry_ids(&self, other: &ResultRow) -> Ordering {
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
        id1.partial_cmp(&id2).unwrap()
    }

    pub fn compare_to(&self, other: &ResultRow, datatype: &SnakDataType) -> Ordering {
        match datatype {
            SnakDataType::Quantity => {
                let va = self.sortkey.parse::<u64>().ok().or(Some(0)).unwrap_or(0);
                let vb = other.sortkey.parse::<u64>().ok().or(Some(0)).unwrap_or(0);
                if va == 0 && vb == 0 {
                    self.compare_entiry_ids(other)
                } else {
                    va.partial_cmp(&vb).unwrap()
                }
            }
            _ => {
                if self.sortkey == other.sortkey {
                    self.compare_entiry_ids(other)
                } else {
                    self.sortkey.partial_cmp(&other.sortkey).unwrap()
                }
            }
        }
    }

    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize) -> Value {
        let mut ret: Vec<Value> = self
            .cells
            .iter()
            .enumerate()
            .map(|(colnum, cell)| cell.as_tabbed_data(list, rownum, colnum))
            .collect();
        ret.insert(0, json!(self.section));
        json!(ret)
    }

    fn cells_as_wikitext(&self, list: &ListeriaList, cells: &[String]) -> String {
        cells
            .iter()
            .enumerate()
            .map(|(colnum, cell)| {
                let column = list.column(colnum).unwrap(); // TODO
                let key = column.obj.as_key();
                format!("{} = {}", key, &cell)
            })
            .collect::<Vec<String>>()
            .join("\n| ")
    }

    pub fn as_wikitext(&self, list: &ListeriaList, rownum: usize) -> String {
        let cells = self
            .cells
            .iter()
            .enumerate()
            .map(|(colnum, cell)| cell.as_wikitext(list, rownum, colnum))
            .collect::<Vec<String>>();
        match list.get_row_template() {
            Some(t) => format!(
                "{{{{{}\n| {}\n}}}}",
                t,
                self.cells_as_wikitext(list, &cells)
            ),
            None => "| ".to_string() + &cells.join("\n| "),
        }
    }
}
