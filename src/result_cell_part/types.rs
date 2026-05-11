//! Supporting value types referenced by `ResultCellPart` variants.
//!
//! Each struct here is a plain serializable container; rendering and
//! Snak conversion live in the parent module. Keeping these types in a
//! dedicated file makes the `ResultCellPart` enum easier to read and lets
//! the supporting types own their own unit tests.

use crate::my_entity::MyEntity;
use serde::{Deserialize, Serialize};
use wikimisc::wikibase::entity::EntityTrait;

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

    /// Returns the looked-up description, or `None` if `set_description` has
    /// not been called yet. Used by the renderer to substitute the resolved
    /// description text into the output.
    pub fn desc(&self) -> Option<&str> {
        self.desc.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::my_entity::MyEntity;
    use wikimisc::wikibase::Entity;

    fn make_test_entity(id: &str) -> MyEntity {
        let json = serde_json::json!({
            "type": "item",
            "id": id,
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "claims": {},
            "sitelinks": {}
        });
        MyEntity(Entity::new_from_json(&json).unwrap())
    }

    #[test]
    fn test_link_target_equality() {
        assert_eq!(LinkTarget::Page, LinkTarget::Page);
        assert_eq!(LinkTarget::Category, LinkTarget::Category);
        assert_ne!(LinkTarget::Page, LinkTarget::Category);
    }

    #[test]
    fn test_entity_info_new() {
        let info = EntityInfo::new("Q42".to_string(), true);
        assert_eq!(info.id, "Q42");
        assert!(info.try_localize);
    }

    #[test]
    fn test_entity_info_no_localize() {
        let info = EntityInfo::new("Q42".to_string(), false);
        assert_eq!(info.id, "Q42");
        assert!(!info.try_localize);
    }

    #[test]
    fn test_local_link_info_new() {
        let info = LocalLinkInfo::new(
            "Page_Title".to_string(),
            "Page Title".to_string(),
            LinkTarget::Page,
        );
        assert_eq!(info.page, "Page_Title");
        assert_eq!(info.label, "Page Title");
        assert_eq!(info.target, LinkTarget::Page);
    }

    #[test]
    fn test_local_link_info_category() {
        let info = LocalLinkInfo::new(
            "Category:Foo".to_string(),
            "Foo".to_string(),
            LinkTarget::Category,
        );
        assert_eq!(info.target, LinkTarget::Category);
    }

    #[test]
    fn test_location_info_new() {
        let info = LocationInfo::new(48.8566, 2.3522, Some("EU".to_string()));
        assert!((info.latitude - 48.8566).abs() < 1e-9);
        assert!((info.longitude - 2.3522).abs() < 1e-9);
        assert_eq!(info.region.as_deref(), Some("EU"));
        assert!(info.name.is_none());
    }

    #[test]
    fn test_location_info_no_region() {
        let info = LocationInfo::new(0.0, 0.0, None);
        assert!(info.region.is_none());
    }

    #[test]
    fn test_external_id_info_new() {
        let info = ExternalIdInfo::new("P213".to_string(), "0000-0001-2345-6789".to_string());
        assert_eq!(info.property, "P213");
        assert_eq!(info.id, "0000-0001-2345-6789");
    }

    #[test]
    fn test_autodesc_new_sets_entity_id_and_no_desc() {
        let ad = AutoDesc::new(&make_test_entity("Q42"));
        assert_eq!(ad.entity_id(), "Q42");
        assert!(ad.desc.is_none());
    }

    #[test]
    fn test_autodesc_set_description() {
        let mut ad = AutoDesc::new(&make_test_entity("Q1"));
        ad.set_description("the universe");
        assert_eq!(ad.desc, Some("the universe".to_string()));
        assert_eq!(ad.desc(), Some("the universe"));
    }

    #[test]
    fn test_autodesc_desc_returns_none_until_set() {
        let ad = AutoDesc::new(&make_test_entity("Q1"));
        assert_eq!(ad.desc(), None);
    }

    #[test]
    fn test_autodesc_equality() {
        let a1 = AutoDesc::new(&make_test_entity("Q5"));
        let a2 = AutoDesc::new(&make_test_entity("Q5"));
        let a3 = AutoDesc::new(&make_test_entity("Q6"));
        assert_eq!(a1, a2);
        assert_ne!(a1, a3);
    }
}
