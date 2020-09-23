use crate::result_cell::PartWithReference;
pub use crate::listeria_page::ListeriaPage;
pub use crate::listeria_list::ListeriaList;
pub use crate::render_wikitext::RendererWikitext;
pub use crate::render_tabbed_data::RendererTabbedData;
pub use crate::result_row::ResultRow;
pub use crate::column::*;
pub use crate::{SparqlValue,LinksType};
use regex::Regex;
use wikibase::entity::EntityTrait;

#[derive(Debug, Clone, PartialEq)]
pub enum ResultCellPart {
    Number,
    Entity((String, bool)),      // ID, try_localize
    LocalLink((String, String)), // Page, label
    Time(String),
    Location((f64, f64, Option<String>)),
    File(String),
    Uri(String),
    ExternalId((String, String)), // Property, ID
    Text(String),
    SnakList(Vec<PartWithReference>), // PP and PQP
}

impl ResultCellPart {
    pub fn from_sparql_value(v: &SparqlValue) -> Self {
        match v {
            SparqlValue::Entity(x) => ResultCellPart::Entity((x.to_owned(), true)),
            SparqlValue::File(x) => ResultCellPart::File(x.to_owned()),
            SparqlValue::Uri(x) => ResultCellPart::Uri(x.to_owned()),
            SparqlValue::Time(x) => ResultCellPart::Text(x.to_owned()),
            SparqlValue::Location(x) => ResultCellPart::Location((x.lat, x.lon, None)),
            SparqlValue::Literal(x) => ResultCellPart::Text(x.to_owned()),
        }
    }


    pub fn localize_item_links(&mut self,list: &ListeriaList) {
        match self {
            ResultCellPart::Entity((item, true)) => {
                match list.entity_to_local_link(&item) {
                    Some(ll) => *self = ll,
                    None => {}
                } ;
            }
            ResultCellPart::SnakList(v) => {
                for part_with_reference in v.iter_mut() {
                    part_with_reference.part.localize_item_links(list);
                }
                //Self::localize_item_links_in_parts(list,&mut v) ;
            }
            _ => {},
        }
    }

    pub fn from_snak(snak: &wikibase::Snak) -> Self {
        match &snak.data_value() {
            Some(dv) => match dv.value() {
                wikibase::Value::Entity(v) => ResultCellPart::Entity((v.id().to_string(), true)),
                wikibase::Value::StringValue(v) => match snak.datatype() {
                    wikibase::SnakDataType::CommonsMedia => ResultCellPart::File(v.to_string()),
                    wikibase::SnakDataType::ExternalId => {
                        ResultCellPart::ExternalId((snak.property().to_string(), v.to_string()))
                    }
                    _ => ResultCellPart::Text(v.to_string()),
                },
                wikibase::Value::Quantity(v) => ResultCellPart::Text(v.amount().to_string()),
                wikibase::Value::Time(v) => ResultCellPart::Time(ResultCellPart::reduce_time(&v)),
                wikibase::Value::Coordinate(v) => {
                    ResultCellPart::Location((*v.latitude(), *v.longitude(), None))
                }
                wikibase::Value::MonoLingual(v) => {
                    ResultCellPart::Text(v.language().to_string() + ":" + v.text())
                }
            },
            _ => ResultCellPart::Text("No/unknown value".to_string()),
        }
    }

    pub fn reduce_time(v: &wikibase::TimeValue) -> String {
        lazy_static! {
            static ref RE_DATE: Regex =
                Regex::new(r#"^\+{0,1}(-{0,1}\d+)-(\d{1,2})-(\d{1,2})T"#).unwrap();
        }
        let s = v.time().to_string();
        let (year, month, day) = match RE_DATE.captures(&s) {
            Some(caps) => (
                caps.get(1).unwrap().as_str().to_string(),
                caps.get(2).unwrap().as_str().to_string(),
                caps.get(3).unwrap().as_str().to_string(),
            ),
            None => {
                return s;
            }
        };
        match v.precision() {
            6 => format!("{}th millenium", year[0..year.len() - 4].to_string()),
            7 => format!("{}th century", year[0..year.len() - 3].to_string()),
            8 => format!("{}0s", year[0..year.len() - 2].to_string()),
            9 => year,
            10 => format!("{}-{}", year, month),
            11 => format!("{}-{}-{}", year, month, day),
            _ => s,
        }
    }

    fn tabbed_string_safe(&self, s: String) -> String {
        let ret = s.replace("\n", " ").replace("\t", " ");
        // 400 chars Max
        if ret.len() >= 380 {
            ret[0..380].to_string();
        }
        ret
    }

    pub fn as_wikitext(
        &self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
        partnum: usize,
    ) -> String {
        //format!("CELL ROW {} COL {} PART {}", rownum, colnum, partnum)
        match self {
            ResultCellPart::Number => format!("style='text-align:right'| {}", rownum + 1),
            ResultCellPart::Entity((id, try_localize)) => {
                if !try_localize {
                    return format!("[[:d:{}|{}]]", id, id);
                }
                let entity_id_link = format!("''[[:d:{}|{}]]''", id, id);
                match list.get_entity(id.to_owned()) {
                    Some(e) => {
                        let use_language = match e.label_in_locale(list.language()) {
                            Some(_) => list.language(),
                            None => list.default_language()
                        } ;

                        match e.label_in_locale(use_language) {
                            Some(l) => {
                                let labeled_entity_link = format!("''[[:d:{}|{}]]''", id, l);
                                match list.get_links_type() {
                                    LinksType::Text => l.to_string(),
                                    LinksType::Red | LinksType::RedOnly => {
                                        if list.local_page_exists(l) {
                                            labeled_entity_link
                                        } else {
                                            "[[".to_string() + &l.to_string() + "]]"
                                        }
                                    }
                                    LinksType::Reasonator => {
                                        format!("[https://reasonator.toolforge.org/?q={} {}]", id, l)
                                    }
                                    _ => labeled_entity_link,
                                }
                            }
                            None => entity_id_link,
                        }
                    },
                    None => entity_id_link,
                }
            }
            ResultCellPart::LocalLink((title, label)) => {
                if list.normalize_page_title(title) == list.normalize_page_title(label) {
                    "[[".to_string() + &label + "]]"
                } else {
                    "[[".to_string() + &title + "|" + &label + "]]"
                }
            }
            ResultCellPart::Time(time) => time.to_owned(),
            ResultCellPart::Location((lat, lon, region)) => {
                let entity_id = match list.results().get(rownum) {
                    Some(row) => Some(row.entity_id().to_string()),
                    None => None
                } ;
                list.get_location_template(*lat, *lon, entity_id, region.to_owned())
            }
            ResultCellPart::File(file) => {
                let thumb = list.thumbnail_size();
                format!(
                    "[[{}:{}|thumb|{}px|]]",
                    list.local_file_namespace_prefix(),
                    &file,
                    thumb
                )
            }
            ResultCellPart::Uri(url) => url.to_owned(),
            ResultCellPart::ExternalId((property, id)) => {
                match list.ecw.external_id_url(property, id) {
                    Some(url) => "[".to_string() + &url + " " + &id + "]",
                    None => id.to_owned(),
                }
            }
            ResultCellPart::Text(text) => text.to_owned(),
            ResultCellPart::SnakList(v) => v
                .iter()
                .map(|rcp| rcp.part.as_wikitext(list, rownum, colnum, partnum))
                .collect::<Vec<String>>()
                .join(" — "),
        }
    }

    pub fn as_tabbed_data(
        &self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
        partnum: usize,
    ) -> String {
        self.tabbed_string_safe(self.as_wikitext(list, rownum, colnum, partnum))
    }
}
