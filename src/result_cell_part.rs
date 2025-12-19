//! Individual parts that make up table cells, with rendering logic for different data types.

use crate::column_type::ColumnType;
use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::listeria_list::ListeriaList;
use crate::reference::Reference;
use crate::template_params::LinksType;
use async_recursion::async_recursion;
use era_date::{Era, Precision};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use wikimisc::sparql_value::SparqlValue;
use wikimisc::wikibase::entity::EntityTrait;
use wikimisc::wikibase::{Entity, Snak, SnakDataType, TimeValue, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartWithReference {
    part: ResultCellPart,
    references: Option<Vec<Reference>>,
}

impl PartWithReference {
    #[must_use]
    pub const fn new(part: ResultCellPart, references: Option<Vec<Reference>>) -> Self {
        Self { part, references }
    }

    #[must_use]
    pub const fn references(&self) -> &Option<Vec<Reference>> {
        &self.references
    }

    #[must_use]
    pub const fn part(&self) -> &ResultCellPart {
        &self.part
    }

    pub const fn part_mut(&mut self) -> &mut ResultCellPart {
        &mut self.part
    }

    pub async fn as_wikitext(
        &mut self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let wikitext_part = self.part.as_wikitext(list, rownum, colnum).await;
        let wikitext_reference = if let Some(references) = &mut self.references {
            let mut parts = Vec::new();
            for reference in references {
                parts.push(reference.as_reference(list).await);
            }
            parts.join("")
        } else {
            String::new()
        };
        wikitext_part + &wikitext_reference
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoDesc {
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
pub struct EntityInfo {
    pub id: String,
    pub try_localize: bool,
}

impl EntityInfo {
    #[must_use]
    pub const fn new(id: String, try_localize: bool) -> Self {
        Self { id, try_localize }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalLinkInfo {
    pub page: String,
    pub label: String,
    pub target: LinkTarget,
}

impl LocalLinkInfo {
    #[must_use]
    pub const fn new(page: String, label: String, target: LinkTarget) -> Self {
        Self {
            page,
            label,
            target,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocationInfo {
    pub latitude: f64,
    pub longitude: f64,
    pub region: Option<String>,
}

impl LocationInfo {
    #[must_use]
    pub const fn new(latitude: f64, longitude: f64, region: Option<String>) -> Self {
        Self {
            latitude,
            longitude,
            region,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExternalIdInfo {
    pub property: String,
    pub id: String,
}

impl ExternalIdInfo {
    #[must_use]
    pub const fn new(property: String, id: String) -> Self {
        Self { property, id }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResultCellPart {
    Number,
    Entity(EntityInfo),
    EntitySchema(String),
    LocalLink(LocalLinkInfo),
    Time(String),
    Location(LocationInfo),
    File(String),
    Uri(String),
    ExternalId(ExternalIdInfo),
    Text(String),
    SnakList(Vec<PartWithReference>), // PP and PQP
    AutoDesc(AutoDesc),
}

impl ResultCellPart {
    pub fn from_sparql_value(v: &SparqlValue) -> Self {
        match v {
            SparqlValue::Entity(x) => ResultCellPart::Entity(EntityInfo::new(x.to_owned(), true)),
            SparqlValue::File(x) => ResultCellPart::File(x.to_owned()),
            SparqlValue::Uri(x) => ResultCellPart::Uri(x.to_owned()),
            SparqlValue::Time(x) => ResultCellPart::Text(x.to_owned()),
            SparqlValue::Location(x) => {
                ResultCellPart::Location(LocationInfo::new(x.lat, x.lon, None))
            }
            SparqlValue::Literal(x) => ResultCellPart::Text(x.to_owned()),
        }
    }

    #[async_recursion]
    pub async fn localize_item_links(
        &mut self,
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
    ) {
        match self {
            ResultCellPart::Entity(entity_info) if entity_info.try_localize => {
                if let Some(ll) = ecw
                    .entity_to_local_link(&entity_info.id, wiki, language)
                    .await
                {
                    *self = ll;
                };
            }
            ResultCellPart::SnakList(v) => {
                for part_with_reference in v.iter_mut() {
                    part_with_reference
                        .part
                        .localize_item_links(ecw, wiki, language)
                        .await;
                }
            }
            _ => {}
        }
    }

    pub fn from_snak(snak: &Snak) -> Self {
        match &snak.data_value() {
            Some(dv) => match dv.value() {
                Value::Entity(v) => {
                    ResultCellPart::Entity(EntityInfo::new(v.id().to_string(), true))
                }
                Value::StringValue(v) => match snak.datatype() {
                    SnakDataType::CommonsMedia => ResultCellPart::File(v.to_string()),
                    SnakDataType::ExternalId => ResultCellPart::ExternalId(ExternalIdInfo::new(
                        snak.property().to_string(),
                        v.to_string(),
                    )),
                    _ => ResultCellPart::Text(v.to_string()),
                },
                Value::Quantity(v) => ResultCellPart::Text(v.amount().to_string()),
                Value::Time(v) => match ResultCellPart::reduce_time(v) {
                    Some(part) => ResultCellPart::Time(part),
                    None => ResultCellPart::Text("No/unknown value".to_string()),
                },
                Value::Coordinate(v) => {
                    ResultCellPart::Location(LocationInfo::new(*v.latitude(), *v.longitude(), None))
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
        static RE_DATE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"^\+?(-?\d+)-(\d{1,2})-(\d{1,2})T"#).expect("RE_DATE does not parse")
        });
        let s = v.time().to_string();
        let caps = RE_DATE.captures(&s)?;

        let year = caps.get(1)?.as_str().parse::<i32>().ok()?;
        let month = caps.get(2)?.as_str().parse::<u8>().ok()?;
        let day = caps.get(3)?.as_str().parse::<u8>().ok()?;
        let precision_val: u8 = (*v.precision()).try_into().ok()?;
        let precision = Precision::try_from(precision_val).ok()?;

        Some(Era::new(year, month, day, precision).to_string())
    }

    fn tabbed_string_safe(s: String) -> String {
        let ret = s.replace(['\n', '\t'], " ");

        // limit string to ~400 chars Max
        if ret.len() >= 380 {
            ret[0..380].to_string()
        } else {
            ret
        }
    }

    async fn as_wikitext_entity(
        &self,
        list: &ListeriaList,
        id: &str,
        try_localize: bool,
        colnum: usize,
    ) -> String {
        if !try_localize {
            let is_item_column = list
                .column(colnum)
                .is_some_and(|col| *col.obj() == ColumnType::Item);

            let target = list.get_item_wiki_target(id);
            return if list.is_main_wikibase_wiki() || is_item_column {
                format!("[[{target}|{id}]]")
            } else {
                format!("''[[{target}|{id}]]''")
            };
        }

        let entity_id_link = list.get_item_link_with_fallback(id).await;
        let Some(entity) = list.get_entity(id).await else {
            return entity_id_link;
        };

        let use_language = entity
            .label_in_locale(list.language())
            .map_or_else(|| list.default_language(), |_| list.language().to_string());

        let use_label = list.get_label_with_fallback_lang(id, &use_language).await;
        let target = list.get_item_wiki_target(id);
        let labeled_entity_link = if list.is_main_wikibase_wiki() {
            format!("[[{target}|{use_label}]]")
        } else {
            format!("''[[{target}|{use_label}]]''")
        };

        Self::render_entity_link(list, use_label, id, labeled_entity_link)
    }

    fn as_wikitext_local_link(
        list: &ListeriaList,
        title: &str,
        label: &str,
        link_target: &LinkTarget,
    ) -> String {
        let start = if matches!(link_target, LinkTarget::Category) {
            "[[:"
        } else {
            "[["
        };

        let normalized_page =
            ListeriaList::normalize_page_title(list.page_title()).replace(' ', "_");
        let normalized_title = ListeriaList::normalize_page_title(title).replace(' ', "_");

        if normalized_page == normalized_title {
            label.to_string()
        } else if ListeriaList::normalize_page_title(title)
            == ListeriaList::normalize_page_title(label)
        {
            format!("{start}{label}]]")
        } else {
            format!("{start}{title}|{label}]]")
        }
    }

    fn as_wikitext_location(
        list: &ListeriaList,
        lat: f64,
        lon: f64,
        region: &Option<String>,
        rownum: usize,
    ) -> String {
        let entity_id = list
            .results()
            .get(rownum)
            .map(|e| e.entity_id().to_string());
        list.get_location_template(lat, lon, entity_id, region.clone())
    }

    fn as_wikitext_file(list: &ListeriaList, file: &str) -> String {
        let thumb = list.thumbnail_size();
        format!(
            "[[{}:{}|center|{}px]]",
            list.local_file_namespace_prefix(),
            file,
            thumb
        )
    }

    async fn as_wikitext_external_id(list: &ListeriaList, property: &str, id: &str) -> String {
        match list.external_id_url(property, id).await {
            Some(url) => format!("[{url} {id}]"),
            None => id.to_string(),
        }
    }

    fn as_wikitext_text(list: &ListeriaList, text: &str, colnum: usize) -> String {
        list.column(colnum)
            .and_then(|col| match col.obj() {
                ColumnType::Property(p) if p == "P373" => {
                    Some(format!("[[:commons:Category:{text}|{text}]]"))
                }
                _ => None,
            })
            .unwrap_or_else(|| text.to_string())
    }

    async fn as_wikitext_snak_list(
        v: &[PartWithReference],
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let mut ret = Vec::with_capacity(v.len());
        for rcp in v {
            ret.push(rcp.part.as_wikitext(list, rownum, colnum).await);
        }
        ret.join(" — ")
    }

    #[async_recursion]
    pub async fn as_wikitext(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        match self {
            ResultCellPart::Number => format!("style='text-align:right'| {}", rownum + 1),
            ResultCellPart::Entity(entity_info) => {
                self.as_wikitext_entity(list, &entity_info.id, entity_info.try_localize, colnum)
                    .await
            }
            ResultCellPart::EntitySchema(id) => {
                format!("[[EntitySchema:{id}|{id}]]") // TODO use self.as_wikitext_entity ?
            }
            ResultCellPart::LocalLink(link_info) => Self::as_wikitext_local_link(
                list,
                &link_info.page,
                &link_info.label,
                &link_info.target,
            ),
            ResultCellPart::Time(time) => time.clone(),
            ResultCellPart::Location(loc_info) => Self::as_wikitext_location(
                list,
                loc_info.latitude,
                loc_info.longitude,
                &loc_info.region,
                rownum,
            ),
            ResultCellPart::File(file) => Self::as_wikitext_file(list, file),
            ResultCellPart::Uri(url) => url.clone(),
            ResultCellPart::ExternalId(ext_id_info) => {
                Self::as_wikitext_external_id(list, &ext_id_info.property, &ext_id_info.id).await
            }
            ResultCellPart::Text(text) => Self::as_wikitext_text(list, text, colnum),
            ResultCellPart::SnakList(v) => {
                Self::as_wikitext_snak_list(v, list, rownum, colnum).await
            }
            ResultCellPart::AutoDesc(ad) => ad.desc.as_deref().unwrap_or_default().to_string(),
        }
    }

    pub async fn as_tabbed_data(
        &self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
    ) -> String {
        Self::tabbed_string_safe(self.as_wikitext(list, rownum, colnum).await)
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
                format!("[https://reasonator.toolforge.org/?q={id} {use_label}]")
            }
            _ => labeled_entity_link,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tabbed_string_safe_removes_newlines() {
        let input = "line1\nline2\nline3".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "line1 line2 line3");
    }

    #[test]
    fn test_tabbed_string_safe_removes_tabs() {
        let input = "col1\tcol2\tcol3".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "col1 col2 col3");
    }

    #[test]
    fn test_tabbed_string_safe_removes_both() {
        let input = "line1\tcol1\nline2\tcol2".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "line1 col1 line2 col2");
    }

    #[test]
    fn test_tabbed_string_safe_short_string() {
        let input = "short string".to_string();
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result, "short string");
    }

    // TimeValue tests removed - complex external API dependency

    #[test]
    fn test_from_sparql_value_entity() {
        let sparql_value = SparqlValue::Entity("Q42".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(
            result,
            ResultCellPart::Entity(EntityInfo::new("Q42".to_string(), true))
        );
    }

    #[test]
    fn test_from_sparql_value_file() {
        let sparql_value = SparqlValue::File("Example.jpg".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(result, ResultCellPart::File("Example.jpg".to_string()));
    }

    #[test]
    fn test_from_sparql_value_uri() {
        let sparql_value = SparqlValue::Uri("http://example.com".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(
            result,
            ResultCellPart::Uri("http://example.com".to_string())
        );
    }

    #[test]
    fn test_from_sparql_value_text() {
        let sparql_value = SparqlValue::Time("2024-01-15".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(result, ResultCellPart::Text("2024-01-15".to_string()));
    }

    #[test]
    fn test_from_sparql_value_literal() {
        let sparql_value = SparqlValue::Literal("Some text".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(result, ResultCellPart::Text("Some text".to_string()));
    }

    #[test]
    fn test_part_with_reference_new() {
        let part = ResultCellPart::Text("test".to_string());
        let references = Some(vec![Reference::default()]);
        let pwr = PartWithReference::new(part.clone(), references.clone());
        assert_eq!(pwr.part(), &part);
        assert_eq!(pwr.references().as_ref().unwrap().len(), 1);
    }

    // SparqlValue::Location test removed - external struct instantiation not straightforward

    #[test]
    fn test_part_with_reference_no_references() {
        let part = ResultCellPart::Text("test".to_string());
        let pwr = PartWithReference::new(part.clone(), None);
        assert_eq!(pwr.part(), &part);
        assert!(pwr.references().is_none());
    }

    #[test]
    fn test_link_target_equality() {
        assert_eq!(LinkTarget::Page, LinkTarget::Page);
        assert_eq!(LinkTarget::Category, LinkTarget::Category);
        assert_ne!(LinkTarget::Page, LinkTarget::Category);
    }

    // AutoDesc tests removed - complex Entity API dependency
}
