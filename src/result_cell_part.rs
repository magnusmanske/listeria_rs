//! Individual parts that make up table cells, with rendering logic for different data types.

use crate::column_type::ColumnType;
use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::my_entity::MyEntity;
use crate::reference::Reference;
use crate::render_context::{normalize_page_title, RenderContext};
use crate::template_params::LinksType;
use era_date::{Era, Precision};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use wikimisc::sparql_value::SparqlValue;
use wikimisc::wikibase::entity::EntityTrait;
use wikimisc::wikibase::{Snak, SnakDataType, TimeValue, Value};

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
        &self,
        list: &impl RenderContext,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let wikitext_part = self.part.as_wikitext(list, rownum, colnum).await;
        let wikitext_reference = if let Some(references) = &self.references {
            let futures: Vec<_> = references
                .iter()
                .map(|reference| reference.as_reference(list))
                .collect();
            join_all(futures).await.join("")
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
    pub fn new(entity: &MyEntity) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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
    /// Page-unique anchor name for this location, used as the `name=` parameter
    /// in coordinate templates. Assigned during result processing so that
    /// duplicate HTML anchors are avoided when the same item has multiple
    /// coordinates or appears in multiple rows (see GitHub issue #136).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl LocationInfo {
    #[must_use]
    pub const fn new(latitude: f64, longitude: f64, region: Option<String>) -> Self {
        Self {
            latitude,
            longitude,
            region,
            name: None,
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
    Time(String, i32), // (display, sort_year)
    Location(LocationInfo),
    File(String),
    Uri(String),
    ExternalId(ExternalIdInfo),
    Text(String),
    SnakList(Vec<PartWithReference>), // PP and PQP
    AutoDesc(AutoDesc),
    Quantity(f64, Option<String>), // (amount, unit_entity_id)
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

    async fn localize_snak_list(
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
        v: &mut [PartWithReference],
    ) {
        for part_with_reference in v.iter_mut() {
            let result_cell_part = &mut part_with_reference.part;
            if let ResultCellPart::Entity(entity_info) = result_cell_part
                && entity_info.try_localize
                && let Some(ll) = ecw
                    .entity_to_local_link(&entity_info.id, wiki, language)
                    .await
            {
                *result_cell_part = ll;
            }
        }
    }

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
                Self::localize_snak_list(ecw, wiki, language, v).await;
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
                Value::Quantity(v) => {
                    let unit_id = Self::unit_entity_id_from_url(v.unit());
                    ResultCellPart::Quantity(*v.amount(), unit_id)
                }
                Value::Time(v) => match ResultCellPart::reduce_time(v) {
                    Some((display, year)) => ResultCellPart::Time(display, year),
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

    fn unit_entity_id_from_url(unit: &str) -> Option<String> {
        if unit == "1" {
            return None;
        }
        unit.rsplit('/')
            .next()
            .filter(|s| s.starts_with('Q') || s.starts_with('P'))
            .map(str::to_string)
    }

    /// Extracts the year from a Wikidata ISO time string like `+1900-00-00T00:00:00Z`.
    /// Returns `None` if the string cannot be parsed.
    pub fn time_sort_year(time_str: &str) -> Option<i32> {
        let s = time_str.strip_prefix('+').unwrap_or(time_str);
        let t_pos = s.find('T')?;
        let date_part = &s[..t_pos];
        let year_str = if let Some(after_sign) = date_part.strip_prefix('-') {
            let dash = after_sign.find('-')?;
            &date_part[..dash + 1]
        } else {
            let dash = date_part.find('-')?;
            &date_part[..dash]
        };
        year_str.parse::<i32>().ok()
    }

    /// Returns `(display_string, sort_year)` for a Wikidata time value.
    /// `sort_year` is the raw Wikidata year (before the century +1 correction)
    /// so that all time cells can be sorted chronologically by a plain integer.
    pub fn reduce_time(v: &TimeValue) -> Option<(String, i32)> {
        let s = v.time();
        // Parse format: +?(-?\d+)-(\d{1,2})-(\d{1,2})T...
        let s = s.strip_prefix('+').unwrap_or(s);

        let t_pos = s.find('T')?;
        let date_part = &s[..t_pos];

        // Split on '-' but handle negative years (leading '-')
        let (year_str, rest) = if let Some(after_sign) = date_part.strip_prefix('-') {
            // Negative year: find the next '-' after the leading '-'
            let dash = after_sign.find('-')?;
            (&date_part[..dash + 1], &after_sign[dash + 1..])
        } else {
            let dash = date_part.find('-')?;
            (&date_part[..dash], &date_part[dash + 1..])
        };

        let year = year_str.parse::<i32>().ok()?;
        let (month_str, day_str) = rest.split_once('-')?;
        let month = month_str.parse::<u8>().ok()?;
        let day = day_str.parse::<u8>().ok()?;
        let precision_val: u8 = (*v.precision()).try_into().ok()?;
        let precision = Precision::try_from(precision_val).ok()?;

        // Wikidata stores century-precision dates as the first year of the colloquial century
        // (e.g. year 1900 = "20th century" = "the 1900s"). era_date uses the mathematical
        // convention where year 1900 falls in the 19th century, so we add 1 for positive years.
        let era_year = if precision == Precision::Century && year > 0 {
            year + 1
        } else {
            year
        };
        let display = Era::new(era_year, month, day, precision).to_string();
        // Use the raw Wikidata year (not era_year) as the numeric sort key so that
        // century-precision dates (e.g. year=1900 → "20th century") sort correctly.
        Some((display, year))
    }

    fn tabbed_string_safe(s: String) -> String {
        let mut ret = s.replace(['\n', '\t'], " ");

        // limit string to ~400 bytes max. Use the largest char boundary at or
        // below 380 so we never slice inside a multi-byte UTF-8 code point
        // (which would panic in String::truncate).
        if ret.len() > 380 {
            let mut cut = 380;
            while cut > 0 && !ret.is_char_boundary(cut) {
                cut -= 1;
            }
            ret.truncate(cut);
        }
        ret
    }


    async fn as_wikitext_entity(
        &self,
        list: &impl RenderContext,
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
        list: &impl RenderContext,
        title: &str,
        label: &str,
        link_target: &LinkTarget,
    ) -> String {
        let start = if matches!(link_target, LinkTarget::Category) {
            "[[:"
        } else {
            "[["
        };

        let norm_title = normalize_page_title(title);
        let norm_page = normalize_page_title(list.page_title()).replace(' ', "_");

        if norm_page == norm_title.replace(' ', "_") {
            label.to_string()
        } else if norm_title == normalize_page_title(label) {
            format!("{start}{label}]]")
        } else {
            format!("{start}{title}|{label}]]")
        }
    }

    async fn as_wikitext_location(
        list: &impl RenderContext,
        loc_info: &LocationInfo,
        rownum: usize,
    ) -> String {
        // Prefer the explicitly-assigned, page-unique anchor name when
        // available (see ListProcessor::process_assign_location_names). Fall
        // back to the row's entity_id for safety if no name has been
        // assigned (e.g. in tests that bypass the processing pipeline).
        let entity_id = list
            .results()
            .get(rownum)
            .map(|e| e.entity_id().to_string());
        let name = loc_info.name.clone().or_else(|| entity_id.clone());
        let label = match &entity_id {
            Some(id) => {
                let l = list
                    .ecw()
                    .get_entity_label_with_fallback(id, list.language())
                    .await;
                if l == *id { None } else { Some(l) }
            }
            None => None,
        };
        list.get_location_template(
            loc_info.latitude,
            loc_info.longitude,
            name,
            loc_info.region.clone(),
            label,
        )
    }

    fn as_wikitext_file(list: &impl RenderContext, file: &str) -> String {
        let thumb = list.thumbnail_size();
        format!(
            "[[{}:{}|center|{}px]]",
            list.local_file_namespace_prefix(),
            file,
            thumb
        )
    }

    async fn as_wikitext_external_id(
        list: &impl RenderContext,
        property: &str,
        id: &str,
    ) -> String {
        match list.external_id_url(property, id).await {
            Some(url) => format!("[{url} {id}]"),
            None => id.to_string(),
        }
    }

    /// Converts a URI to wikitext. Wikipedia article URLs become interwiki
    /// links (`[[:en:Title|Title]]`); all other URIs are rendered as-is.
    fn uri_to_wikitext(url: &str) -> String {
        Self::wikipedia_url_to_wikilink(url).unwrap_or_else(|| url.to_string())
    }

    /// Parses `https://{lang}.wikipedia.org/wiki/{title}` and returns a
    /// MediaWiki interwiki link string, or `None` if the URL doesn't match.
    fn wikipedia_url_to_wikilink(url: &str) -> Option<String> {
        let without_scheme = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))?;
        let (host, path) = without_scheme.split_once('/')?;
        let lang = host.strip_suffix(".wikipedia.org")?;
        if lang.is_empty() {
            return None;
        }
        let title = path.strip_prefix("wiki/")?;
        // Drop any fragment (#Section) from the title
        let title = title.split('#').next().unwrap_or(title);
        if title.is_empty() {
            return None;
        }
        let display = title.replace('_', " ");
        Some(format!("[[:{}:{}|{}]]", lang, title, display))
    }

    fn as_wikitext_text(list: &impl RenderContext, text: &str, colnum: usize) -> String {
        // Newlines in cell values break wiki table structure: MediaWiki ends the
        // cell at the first bare newline, and lines starting with a space are
        // rendered as pre-formatted code blocks. Replace \n with <br/> to keep
        // multi-line values readable without corrupting the table (#98).
        let text = &text.replace('\n', "<br/>");
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
        list: &impl RenderContext,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let futures: Vec<_> = v
            .iter()
            .map(|rcp| rcp.part.as_wikitext(list, rownum, colnum))
            .collect();
        join_all(futures).await.join(" — ")
    }

    pub async fn as_wikitext(&self, list: &impl RenderContext, rownum: usize, colnum: usize) -> String {
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
            ResultCellPart::Time(time, _year) => time.clone(),
            ResultCellPart::Location(loc_info) => {
                Self::as_wikitext_location(list, loc_info, rownum).await
            }
            ResultCellPart::File(file) => Self::as_wikitext_file(list, file),
            ResultCellPart::Uri(url) => Self::uri_to_wikitext(url),
            ResultCellPart::ExternalId(ext_id_info) => {
                Self::as_wikitext_external_id(list, &ext_id_info.property, &ext_id_info.id).await
            }
            ResultCellPart::Text(text) => Self::as_wikitext_text(list, text, colnum),
            ResultCellPart::SnakList(v) => {
                Self::as_wikitext_snak_list(v, list, rownum, colnum).await
            }
            ResultCellPart::AutoDesc(ad) => ad.desc.as_deref().unwrap_or_default().to_string(),
            ResultCellPart::Quantity(amount, unit_id) => {
                let amount_str = amount.to_string();
                match unit_id {
                    Some(uid) => {
                        let label = list
                            .ecw()
                            .get_entity_label_with_fallback(uid, list.language())
                            .await;
                        format!("{amount_str} {label}")
                    }
                    None => amount_str,
                }
            }
        }
    }

    pub async fn as_tabbed_data(
        &self,
        list: &impl RenderContext,
        rownum: usize,
        colnum: usize,
    ) -> String {
        Self::tabbed_string_safe(self.as_wikitext(list, rownum, colnum).await)
    }

    fn render_entity_link(
        list: &impl RenderContext,
        use_label: String,
        id: &str,
        labeled_entity_link: String,
    ) -> String {
        match list.get_links_type() {
            LinksType::Text => use_label,
            LinksType::Red | LinksType::RedOnly => {
                // For categories/namespaced labels use the colon prefix to avoid
                // a category inclusion; for everything else use a plain link.
                // MediaWiki determines blue vs. red based on page existence;
                // we no longer synthesise a "(Q-id)" page that rarely exists (#137).
                if use_label.contains(':') {
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

    // --- from_snak ---

    #[test]
    fn test_from_snak_entity() {
        let snak = Snak::new_item("P31", "Q5");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::Entity(EntityInfo::new("Q5".to_string(), true))
        );
    }

    #[test]
    fn test_from_snak_string() {
        let snak = Snak::new_string("P1", "hello world");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("hello world".to_string()));
    }

    #[test]
    fn test_from_snak_url() {
        let snak = Snak::new_url("P856", "https://example.com");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::Text("https://example.com".to_string())
        );
    }

    #[test]
    fn test_from_snak_external_id() {
        let snak = Snak::new_external_id("P213", "0000-0001-2345-6789");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::ExternalId(ExternalIdInfo::new(
                "P213".to_string(),
                "0000-0001-2345-6789".to_string()
            ))
        );
    }

    #[test]
    fn test_from_snak_coordinate() {
        let snak = Snak::new_coordinate("P625", 48.8566, 2.3522);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Location(loc) => {
                assert!((loc.latitude - 48.8566).abs() < 0.0001);
                assert!((loc.longitude - 2.3522).abs() < 0.0001);
                assert!(loc.region.is_none());
            }
            other => panic!("Expected Location, got {:?}", other),
        }
    }

    #[test]
    fn test_from_snak_monolingual_text() {
        let snak = Snak::new_monolingual_text("P1476", "en", "Hello World");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("Hello World:en".to_string()));
    }

    #[test]
    fn test_from_snak_quantity_dimensionless() {
        let snak = Snak::new_quantity("P1082", 42.0);
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Quantity(42.0, None));
    }

    #[test]
    fn test_from_snak_quantity_with_unit() {
        use wikimisc::wikibase::{
            DataValue, DataValueType, Snak, SnakDataType, SnakType, Value,
        };
        use wikimisc::wikibase::value::QuantityValue;
        let snak = Snak::new(
            SnakDataType::Quantity,
            "P2048",
            SnakType::Value,
            Some(DataValue::new(
                DataValueType::Quantity,
                Value::Quantity(QuantityValue::new(
                    1.96,
                    None,
                    "http://www.wikidata.org/entity/Q11573",
                    None,
                )),
            )),
        );
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::Quantity(1.96, Some("Q11573".to_string()))
        );
    }

    #[test]
    fn test_unit_entity_id_from_url_dimensionless() {
        assert_eq!(ResultCellPart::unit_entity_id_from_url("1"), None);
    }

    #[test]
    fn test_unit_entity_id_from_url_valid_entity() {
        assert_eq!(
            ResultCellPart::unit_entity_id_from_url(
                "http://www.wikidata.org/entity/Q11573"
            ),
            Some("Q11573".to_string())
        );
    }

    #[test]
    fn test_unit_entity_id_from_url_unknown_format() {
        assert_eq!(
            ResultCellPart::unit_entity_id_from_url("https://example.com/unit/foo"),
            None
        );
    }

    #[test]
    fn test_from_snak_no_value() {
        let snak = Snak::new_no_value("P31", SnakDataType::WikibaseItem);
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("No/unknown value".to_string()));
    }

    #[test]
    fn test_from_snak_unknown_value() {
        let snak = Snak::new_unknown_value("P31", SnakDataType::WikibaseItem);
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("No/unknown value".to_string()));
    }

    // --- tabbed_string_safe edge cases ---

    #[test]
    fn test_tabbed_string_safe_long_string_truncated() {
        let input = "a".repeat(500);
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result.len(), 380);
    }

    #[test]
    fn test_tabbed_string_safe_exactly_380() {
        let input = "b".repeat(380);
        let result = ResultCellPart::tabbed_string_safe(input);
        assert_eq!(result.len(), 380);
    }

    #[test]
    fn test_tabbed_string_safe_empty() {
        let result = ResultCellPart::tabbed_string_safe(String::new());
        assert_eq!(result, "");
    }

    #[test]
    fn test_tabbed_string_safe_multibyte_truncation_boundary() {
        // '€' is 3 bytes in UTF-8. 130 copies = 390 bytes. Byte 380 lies in
        // the middle of a multibyte code point, so a naive truncate(380) would
        // panic. Verify that tabbed_string_safe keeps the truncation on a char
        // boundary and leaves a valid UTF-8 string <= 380 bytes.
        let input = "€".repeat(130);
        assert_eq!(input.len(), 390);
        let result = ResultCellPart::tabbed_string_safe(input);
        assert!(result.len() <= 380);
        // Confirm the result is still valid UTF-8 (it is, because it's a
        // String, but also make sure none of the '€' characters were cut off
        // mid-way — the length must be divisible by 3).
        assert_eq!(result.len() % 3, 0);
    }

    // --- from_sparql_value location ---

    #[test]
    fn test_from_sparql_value_location() {
        let sparql_value = SparqlValue::Location(wikimisc::lat_lon::LatLon::new(51.5074, -0.1278));
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        match result {
            ResultCellPart::Location(loc) => {
                assert!((loc.latitude - 51.5074).abs() < 0.0001);
                assert!((loc.longitude - (-0.1278)).abs() < 0.0001);
            }
            other => panic!("Expected Location, got {:?}", other),
        }
    }

    // --- EntityInfo / LocalLinkInfo / LocationInfo / ExternalIdInfo constructors ---

    #[test]
    fn test_entity_info_new() {
        let info = EntityInfo::new("Q42".to_string(), true);
        assert_eq!(info.id, "Q42");
        assert!(info.try_localize);
    }

    #[test]
    fn test_entity_info_no_localize() {
        let info = EntityInfo::new("Q1".to_string(), false);
        assert!(!info.try_localize);
    }

    #[test]
    fn test_local_link_info_new() {
        let info = LocalLinkInfo::new(
            "Berlin".to_string(),
            "Berlin label".to_string(),
            LinkTarget::Page,
        );
        assert_eq!(info.page, "Berlin");
        assert_eq!(info.label, "Berlin label");
        assert_eq!(info.target, LinkTarget::Page);
    }

    #[test]
    fn test_local_link_info_category() {
        let info = LocalLinkInfo::new(
            "Category:Cities".to_string(),
            "Cities".to_string(),
            LinkTarget::Category,
        );
        assert_eq!(info.target, LinkTarget::Category);
    }

    #[test]
    fn test_location_info_new() {
        let info = LocationInfo::new(48.8566, 2.3522, Some("FR-75".to_string()));
        assert!((info.latitude - 48.8566).abs() < 0.0001);
        assert!((info.longitude - 2.3522).abs() < 0.0001);
        assert_eq!(info.region, Some("FR-75".to_string()));
    }

    #[test]
    fn test_location_info_no_region() {
        let info = LocationInfo::new(0.0, 0.0, None);
        assert!(info.region.is_none());
    }

    #[test]
    fn test_external_id_info_new() {
        let info = ExternalIdInfo::new("P213".to_string(), "12345".to_string());
        assert_eq!(info.property, "P213");
        assert_eq!(info.id, "12345");
    }

    // --- reduce_time via from_snak ---

    #[test]
    fn test_from_snak_time_day_precision_produces_time_part() {
        // Precision 11 = day; confirmed working by the sections fixture (1879-03-14)
        let snak = Snak::new_time("P569", "+1879-03-14T00:00:00Z", 11);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "1879-03-14");
                assert_eq!(year, 1879);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_from_snak_time_common_era_day() {
        let snak = Snak::new_time("P569", "+1955-06-08T00:00:00Z", 11);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "1955-06-08");
                assert_eq!(year, 1955);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_century_1900_is_20th_century() {
        // Wikidata stores "20th century" (the 1900s) as year 1900 with precision 7.
        let snak = Snak::new_time("P569", "+1900-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "20th century");
                assert_eq!(year, 1900); // raw Wikidata year, not era_year
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_century_1800_is_19th_century() {
        let snak = Snak::new_time("P569", "+1800-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "19th century");
                assert_eq!(year, 1800); // raw Wikidata year
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_decade_1900s() {
        // Issue #44: decade precision should show "1900s" not "190s"
        let snak = Snak::new_time("P569", "+1900-00-00T00:00:00Z", 8);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, _year) => assert_eq!(s, "1900s"),
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    // --- time_sort_year ---

    #[test]
    fn test_time_sort_year_positive() {
        assert_eq!(ResultCellPart::time_sort_year("+1879-03-14T00:00:00Z"), Some(1879));
        assert_eq!(ResultCellPart::time_sort_year("+1900-00-00T00:00:00Z"), Some(1900));
        assert_eq!(ResultCellPart::time_sort_year("+0033-00-00T00:00:00Z"), Some(33));
    }

    #[test]
    fn test_time_sort_year_negative() {
        assert_eq!(ResultCellPart::time_sort_year("-0100-00-00T00:00:00Z"), Some(-100));
    }

    #[test]
    fn test_time_sort_year_no_sign() {
        // Without leading '+', year should still parse
        assert_eq!(ResultCellPart::time_sort_year("1955-06-08T00:00:00Z"), Some(1955));
    }

    // --- AutoDesc ---

    #[test]
    fn test_autodesc_new_sets_entity_id_and_no_desc() {
        use crate::my_entity::MyEntity;
        use wikimisc::wikibase::Entity;

        let json = serde_json::json!({
            "type": "item",
            "id": "Q42",
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "claims": {},
            "sitelinks": {}
        });
        let entity = Entity::new_from_json(&json).unwrap();
        let my_entity = MyEntity(entity);
        let ad = AutoDesc::new(&my_entity);
        assert_eq!(ad.entity_id(), "Q42");
        // description starts empty
        assert!(ad.desc.is_none());
    }

    #[test]
    fn test_autodesc_set_description() {
        use crate::my_entity::MyEntity;
        use wikimisc::wikibase::Entity;

        let json = serde_json::json!({
            "type": "item",
            "id": "Q1",
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "claims": {},
            "sitelinks": {}
        });
        let entity = Entity::new_from_json(&json).unwrap();
        let my_entity = MyEntity(entity);
        let mut ad = AutoDesc::new(&my_entity);
        ad.set_description("the universe");
        assert_eq!(ad.desc, Some("the universe".to_string()));
    }

    #[test]
    fn test_autodesc_equality() {
        use crate::my_entity::MyEntity;
        use wikimisc::wikibase::Entity;

        let make = |id: &str| {
            let json = serde_json::json!({
                "type": "item", "id": id,
                "labels": {}, "descriptions": {},
                "aliases": {}, "claims": {}, "sitelinks": {}
            });
            let entity = Entity::new_from_json(&json).unwrap();
            AutoDesc::new(&MyEntity(entity))
        };

        let a1 = make("Q5");
        let a2 = make("Q5");
        let a3 = make("Q6");
        assert_eq!(a1, a2);
        assert_ne!(a1, a3);
    }

    // --- PartWithReference::part_mut ---

    #[test]
    fn test_part_with_reference_part_mut() {
        let original = ResultCellPart::Text("before".to_string());
        let mut pwr = PartWithReference::new(original, None);
        // Mutate the inner part through part_mut()
        *pwr.part_mut() = ResultCellPart::Text("after".to_string());
        assert_eq!(pwr.part(), &ResultCellPart::Text("after".to_string()));
    }

    // --- as_wikitext_text newline sanitization (#98) ---

    #[test]
    fn test_time_sort_year_negative_year() {
        assert_eq!(ResultCellPart::time_sort_year("-0100-00-00T00:00:00Z"), Some(-100));
    }

    // --- wikipedia_url_to_wikilink (#138) ---

    #[test]
    fn test_wikipedia_url_to_wikilink_basic() {
        let result = ResultCellPart::wikipedia_url_to_wikilink(
            "https://en.wikipedia.org/wiki/Obelisk_(biology)",
        );
        assert_eq!(
            result,
            Some("[[:en:Obelisk_(biology)|Obelisk (biology)]]".to_string())
        );
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_other_lang() {
        let result = ResultCellPart::wikipedia_url_to_wikilink(
            "https://de.wikipedia.org/wiki/Berlin",
        );
        assert_eq!(result, Some("[[:de:Berlin|Berlin]]".to_string()));
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_with_fragment() {
        let result = ResultCellPart::wikipedia_url_to_wikilink(
            "https://en.wikipedia.org/wiki/Foo#Section",
        );
        assert_eq!(result, Some("[[:en:Foo|Foo]]".to_string()));
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_non_wikipedia_url() {
        let result =
            ResultCellPart::wikipedia_url_to_wikilink("https://example.com/page");
        assert_eq!(result, None);
    }

    #[test]
    fn test_wikipedia_url_to_wikilink_empty_title() {
        let result =
            ResultCellPart::wikipedia_url_to_wikilink("https://en.wikipedia.org/wiki/");
        assert_eq!(result, None);
    }
}
