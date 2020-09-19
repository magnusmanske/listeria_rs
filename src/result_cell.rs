pub use crate::listeria_page::ListeriaPage;
pub use crate::listeria_list::ListeriaList;
pub use crate::render_wikitext::RendererWikitext;
pub use crate::render_tabbed_data::RendererTabbedData;
pub use crate::result_row::ResultRow;
pub use crate::column::*;
pub use crate::{SparqlValue,LinksType};
use regex::Regex;
use serde_json::Value;
use wikibase::entity::EntityTrait;
use std::collections::HashMap;

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
    SnakList(Vec<ResultCellPart>), // PP and PQP
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
                .map(|rcp| rcp.as_wikitext(list, rownum, colnum, partnum))
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

#[derive(Debug, Clone, PartialEq)]
pub struct Reference {

}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultCell {
    parts: Vec<ResultCellPart>,
    references: Vec<Vec<Reference>>,
    wdedit_class: Option<String>,
}

impl ResultCell {
    pub async fn new(
        list:&ListeriaList,
        entity_id: &str,
        sparql_rows: &[&HashMap<String, SparqlValue>],
        col: &Column,
    ) -> Self {
        let mut ret = Self { parts:vec![] , references:vec![] , wdedit_class:None };

        let entity = list.get_entity(entity_id.to_owned());
        match &col.obj {
            ColumnType::Item => {
                ret.parts.push(ResultCellPart::Entity((entity_id.to_owned(), false)));
            }
            ColumnType::Description => if let Some(e) = entity { match e.description_in_locale(list.language()) {
                Some(s) => {
                    ret.wdedit_class = Some("wd_desc".to_string());
                    ret.parts.push(ResultCellPart::Text(s.to_string()));
                }
                None => {
                    if let Ok(s) = list.get_autodesc_description(&e).await {
                        ret.parts.push(ResultCellPart::Text(s));
                    }
                }
            } },
            ColumnType::Field(varname) => {
                for row in sparql_rows.iter() {
                    if let Some(x) = row.get(varname) {
                        ret.parts.push(ResultCellPart::from_sparql_value(x));
                    }
                }
            }
            ColumnType::Property(property) => if let Some(e) = entity {
                ret.wdedit_class = Some(format!("wd_{}",property.to_lowercase()));
                list.get_filtered_claims(&e,property)
                    .iter()
                    .for_each(|statement| {
                        ret.parts
                            .push(ResultCellPart::from_snak(statement.main_snak()));
                    });
            },
            ColumnType::PropertyQualifier((p1, p2)) => if let Some(e) = entity {
                list.get_filtered_claims(&e,p1)
                    .iter()
                    .for_each(|statement| {
                        ret.get_parts_p_p(statement,p2)
                            .iter()
                            .for_each(|part|ret.parts.push(part.to_owned()));
                    });
            },
            ColumnType::PropertyQualifierValue((p1, q1, p2)) => if let Some(e) = entity {
                list.get_filtered_claims(&e,p1)
                //e.claims_with_property(p1.to_owned())
                    .iter()
                    .for_each(|statement| {
                        ret.get_parts_p_q_p(statement,q1,p2)
                            .iter()
                            .for_each(|part|ret.parts.push(part.to_owned()));
                    });
            },
            ColumnType::LabelLang(language) => if let Some(e) = entity {
                match e.label_in_locale(language) {
                    Some(s) => {
                        ret.parts.push(ResultCellPart::Text(s.to_string()));
                    }
                    None => if let Some(s) = e.label_in_locale(list.language()) {
                        ret.parts.push(ResultCellPart::Text(s.to_string()));
                    },
                }
            },
            ColumnType::Label => if let Some(e) = entity {
                ret.wdedit_class = Some("wd_label".to_string());
                let label = match e.label_in_locale(list.language()) {
                    Some(s) => s.to_string(),
                    None => entity_id.to_string(),
                };
                let local_page = match e.sitelinks() {
                    Some(sl) => sl
                        .iter()
                        .filter(|s| *s.site() == *list.wiki())
                        .map(|s| s.title().to_string())
                        .next(),
                    None => None,
                };
                match local_page {
                    Some(page) => {
                        ret.parts.push(ResultCellPart::LocalLink((page, label)));
                    }
                    None => {
                        ret.parts
                            .push(ResultCellPart::Entity((entity_id.to_string(), true)));
                    }
                }
            },
            ColumnType::Unknown => {} // Ignore
            ColumnType::Number => {
                ret.parts.push(ResultCellPart::Number);
            }
        }

        ret
    }

    fn get_parts_p_p(&self,statement:&wikibase::statement::Statement,property:&str) -> Vec<ResultCellPart> {
        statement
            .qualifiers()
            .iter()
            .filter(|snak|*snak.property()==*property)
            .map(|snak|ResultCellPart::SnakList (
                    vec![
                        ResultCellPart::from_snak(statement.main_snak()),
                        ResultCellPart::from_snak(snak)
                    ]
                )
            )
            .collect()
    }

    fn get_parts_p_q_p(&self,statement:&wikibase::statement::Statement,target_item:&str,property:&str) -> Vec<ResultCellPart> {
        let links_to_target = match statement.main_snak().data_value(){
            Some(dv) => {
                match dv.value() {
                    wikibase::value::Value::Entity(e) => e.id() == target_item,
                    _ => false
                }
            }
            None => false
        };
        if !links_to_target {
            return vec![];
        }
        self.get_parts_p_p(statement,property)
    }

    pub fn get_sortkey(&self) -> String {
        match self.parts.get(0) {
            Some(part) => {
                match part {
                    ResultCellPart::Entity((id,_)) => id.to_owned(),
                    ResultCellPart::LocalLink((page,_label)) => page.to_owned(),
                    ResultCellPart::Time(time) => time.to_owned(),
                    ResultCellPart::File(s) => s.to_owned(),
                    ResultCellPart::Uri(s) => s.to_owned(),
                    ResultCellPart::Text(s) => s.to_owned(),
                    ResultCellPart::ExternalId((_prop,id)) => id.to_owned(),
                    _ => String::new()
                }
            }
            None => String::new()
        }
    }

    pub fn parts(&self) -> &Vec<ResultCellPart> {
        &self.parts
    }

    pub fn parts_mut(&mut self) -> &mut Vec<ResultCellPart> {
        &mut self.parts
    }

    pub fn set_parts(&mut self, parts:Vec<ResultCellPart> ) {
        self.parts = parts ;
    }

    pub fn localize_item_links_in_parts(list: &ListeriaList, parts: &mut Vec<ResultCellPart>) {
        for part in parts.iter_mut() {
            match part {
                ResultCellPart::Entity((item, true)) => {
                    *part = match list.entity_to_local_link(&item) {
                        Some(ll) => ll,
                        None => part.to_owned(),
                    } ;
                }
                ResultCellPart::SnakList(v) => {
                    Self::localize_item_links_in_parts(list,v) ;
                }
                _ => {},
            }
        }
    }

    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> Value {
        let ret: Vec<String> = self
            .parts
            .iter()
            .enumerate()
            .map(|(partnum, part)| part.as_tabbed_data(list, rownum, colnum, partnum))
            .collect();
        json!(ret.join("<br/>"))
    }

    pub fn as_wikitext(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        let mut ret ;
        if list.template_params().wdedit {
            ret = match &self.wdedit_class {
                Some(class) => format!("class='{}'| ",class.to_owned()),
                None => " ".to_string()
            };
        } else {
            ret = " ".to_string();
        }
        ret += &self.parts
            .iter()
            .enumerate()
            .map(|(partnum, part)| part.as_wikitext(list, rownum, colnum, partnum))
            .collect::<Vec<String>>()
            .join("<br/>") ;
        ret
    }
}
