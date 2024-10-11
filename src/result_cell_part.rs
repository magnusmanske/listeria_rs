use crate::column::ColumnType;
use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::listeria_list::ListeriaList;
use crate::reference::Reference;
use crate::template_params::LinksType;
use era_date::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use wikimisc::sparql_value::SparqlValue;
use wikimisc::wikibase::entity::EntityTrait;
use wikimisc::wikibase::{Entity, Snak, SnakDataType, TimeValue, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartWithReference {
    part: ResultCellPart,
    references: Option<Vec<Reference>>,
}

impl PartWithReference {
    pub fn new(part: ResultCellPart, references: Option<Vec<Reference>>) -> Self {
        Self { part, references }
    }

    pub fn references(&self) -> &Option<Vec<Reference>> {
        &self.references
    }

    pub fn part(&self) -> &ResultCellPart {
        &self.part
    }

    pub fn part_mut(&mut self) -> &mut ResultCellPart {
        &mut self.part
    }

    pub fn as_wikitext(&mut self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        let wikitext_part = self.part.as_wikitext(list, rownum, colnum);
        let wikitext_reference = match &mut self.references {
            Some(references) => {
                let mut wikitext: Vec<String> = vec![];
                for reference in references.iter_mut() {
                    let r = reference.as_reference(list);
                    wikitext.push(r);
                }
                wikitext.join("")
            }
            None => String::new(),
        };
        wikitext_part + &wikitext_reference
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoDesc {
    // entity: Entity,
    entity_id: String,
    desc: Option<String>,
}

impl PartialEq for AutoDesc {
    fn eq(&self, other: &Self) -> bool {
        self.entity_id == other.entity_id && self.desc == other.desc
    }
}

impl AutoDesc {
    pub fn new(entity: &Entity) -> Self {
        Self {
            entity_id: entity.id().to_owned(),
            desc: None,
        }
    }

    pub fn set_description(&mut self, description: &str) {
        self.desc = Some(description.to_string());
    }

    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LinkTarget {
    Page,
    Category,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/* trunk-ignore(clippy/large_enum_variant) */
pub enum ResultCellPart {
    Number,
    Entity((String, bool)),                  // ID, try_localize
    EntitySchema(String),                    // ID
    LocalLink((String, String, LinkTarget)), // Page, label, LinkTarget
    Time(String),
    Location((f64, f64, Option<String>)),
    File(String),
    Uri(String),
    ExternalId((String, String)), // Property, ID
    Text(String),
    SnakList(Vec<PartWithReference>), // PP and PQP
    AutoDesc(AutoDesc),
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

    pub fn localize_item_links(
        &mut self,
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
    ) {
        match self {
            ResultCellPart::Entity((item, true)) => {
                if let Some(ll) = ecw.entity_to_local_link(item, wiki, language) {
                    *self = ll
                };
            }
            ResultCellPart::SnakList(v) => {
                for part_with_reference in v.iter_mut() {
                    part_with_reference
                        .part
                        .localize_item_links(ecw, wiki, language);
                }
            }
            _ => {}
        }
    }

    pub fn from_snak(snak: &Snak) -> Self {
        match &snak.data_value() {
            Some(dv) => match dv.value() {
                Value::Entity(v) => ResultCellPart::Entity((v.id().to_string(), true)),
                Value::StringValue(v) => match snak.datatype() {
                    SnakDataType::CommonsMedia => ResultCellPart::File(v.to_string()),
                    SnakDataType::ExternalId => {
                        ResultCellPart::ExternalId((snak.property().to_string(), v.to_string()))
                    }
                    _ => ResultCellPart::Text(v.to_string()),
                },
                Value::Quantity(v) => ResultCellPart::Text(v.amount().to_string()),
                Value::Time(v) => match ResultCellPart::reduce_time(v) {
                    Some(part) => ResultCellPart::Time(part),
                    None => ResultCellPart::Text("No/unknown value".to_string()),
                },
                Value::Coordinate(v) => {
                    ResultCellPart::Location((*v.latitude(), *v.longitude(), None))
                }
                Value::MonoLingual(v) => {
                    ResultCellPart::Text(v.language().to_string() + ":" + v.text())
                }
                Value::EntitySchema(v) => ResultCellPart::EntitySchema(v.id().to_string()),
            },
            _ => ResultCellPart::Text("No/unknown value".to_string()),
        }
    }

    pub fn reduce_time(v: &TimeValue) -> Option<String> {
        lazy_static! {
            static ref RE_DATE: Regex =
                Regex::new(r#"^\+?(-?\d+)-(\d{1,2})-(\d{1,2})T"#).expect("RE_DATE does not parse");
        }
        let s = v.time().to_string();
        let (year, month, day) = match RE_DATE.captures(&s) {
            Some(caps) => (
                caps.get(1)?.as_str().parse::<i32>().ok()?,
                caps.get(2)?.as_str().parse::<u8>().ok()?,
                caps.get(3)?.as_str().parse::<u8>().ok()?,
            ),
            None => {
                return Some(s);
            }
        };
        let precision = Precision::try_from(*v.precision() as u8).ok()?;
        Some(Era::new(year, month, day, precision).to_string())
    }

    fn tabbed_string_safe(&self, s: String) -> String {
        let ret = s.replace(['\n', '\t'], " ");
        // 400 chars Max
        if ret.len() >= 380 {
            ret[0..380].to_string();
        }
        ret
    }

    fn as_wikitext_entity(
        &self,
        list: &ListeriaList,
        id: &str,
        try_localize: bool,
        colnum: usize,
    ) -> String {
        if !try_localize {
            let is_item_column = match list.column(colnum) {
                Some(col) => *col.obj() == ColumnType::Item,
                None => false,
            };
            if list.is_main_wikibase_wiki() || is_item_column {
                return format!("[[{}|{}]]", list.get_item_wiki_target(id), id);
            } else {
                return format!("''[[{}|{}]]''", list.get_item_wiki_target(id), id);
            }
        }
        let entity_id_link = list.get_item_link_with_fallback(id);
        match list.get_entity(id) {
            Some(e) => {
                let use_language = match e.label_in_locale(list.language()) {
                    Some(_) => list.language().to_owned(),
                    None => list.default_language(),
                };
                let use_label = list.get_label_with_fallback_lang(id, &use_language);
                let labeled_entity_link = if list.is_main_wikibase_wiki() {
                    format!("[[{}|{}]]", list.get_item_wiki_target(id), use_label)
                } else {
                    format!("''[[{}|{}]]''", list.get_item_wiki_target(id), use_label)
                };
                Self::render_entity_link(list, use_label, id, labeled_entity_link)
            }
            None => entity_id_link,
        }
    }

    pub fn as_wikitext(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        match self {
            ResultCellPart::Number => format!("style='text-align:right'| {}", rownum + 1),
            ResultCellPart::Entity((id, try_localize)) => {
                self.as_wikitext_entity(list, id, *try_localize, colnum)
            }
            ResultCellPart::EntitySchema(id) => {
                format!("[[EntitySchema:{id}|{id}]]") // TODO use self.as_wikitext_entity ?
            }
            ResultCellPart::LocalLink((title, label, link_target)) => {
                let start = match link_target {
                    LinkTarget::Page => "[[",
                    LinkTarget::Category => "[[:",
                };
                if list
                    .normalize_page_title(list.page_title())
                    .replace(' ', "_")
                    == list.normalize_page_title(title).replace(' ', "_")
                {
                    label.to_string()
                } else if list.normalize_page_title(title) == list.normalize_page_title(label) {
                    format!("{start}{label}]]")
                } else {
                    format!("{start}{title}|{label}]]")
                }
            }
            ResultCellPart::Time(time) => time.to_owned(),
            ResultCellPart::Location((lat, lon, region)) => {
                let entity_id = list
                    .results()
                    .get(rownum)
                    .map(|e| e.entity_id().to_string());
                list.get_location_template(*lat, *lon, entity_id, region.to_owned())
            }
            ResultCellPart::File(file) => {
                let thumb = list.thumbnail_size();
                format!(
                    "[[{}:{}|center|{}px]]",
                    list.local_file_namespace_prefix(),
                    &file,
                    thumb
                )
            }
            ResultCellPart::Uri(url) => url.to_owned(),
            ResultCellPart::ExternalId((property, id)) => {
                match list.external_id_url(property, id) {
                    Some(url) => "[".to_string() + &url + " " + id + "]",
                    None => id.to_owned(),
                }
            }
            ResultCellPart::Text(text) => {
                match list.column(colnum) {
                    Some(col) => {
                        match col.obj() {
                            ColumnType::Property(p) => {
                                // Commons category
                                if p == "P373" {
                                    format!("[[:commons:Category:{}|{}]]", text, text)
                                } else {
                                    text.to_owned()
                                }
                            }
                            _ => text.to_owned(),
                        }
                    }
                    None => text.to_owned(),
                }
            }
            ResultCellPart::SnakList(v) => v
                .iter()
                .map(|rcp| rcp.part.as_wikitext(list, rownum, colnum))
                .collect::<Vec<String>>()
                .join(" — "),
            ResultCellPart::AutoDesc(ad) => {
                match &ad.desc {
                    Some(desc) => desc.to_owned(),
                    None => String::new(), // TODO check - manual description should have already been tried?
                }
            }
        }
    }

    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        self.tabbed_string_safe(self.as_wikitext(list, rownum, colnum))
    }

    fn render_entity_link(
        list: &ListeriaList,
        use_label: String,
        id: &str,
        labeled_entity_link: String,
    ) -> String {
        match list.get_links_type() {
            LinksType::Text => use_label,
            LinksType::Red | LinksType::RedOnly => {
                let contains_colon = use_label.contains(':');
                if list.local_page_exists(&use_label) {
                    let category_prefix = if contains_colon { ":" } else { "" };
                    format!("[[{}{} ({})|]]", category_prefix, &use_label, &id)
                } else if contains_colon {
                    format!("[[:{}|]]", &use_label)
                } else {
                    format!("[[{}]]", &use_label)
                }
            }
            LinksType::Reasonator => {
                format!("[https://reasonator.toolforge.org/?q={} {}]", id, use_label)
            }
            _ => labeled_entity_link,
        }
    }
}
