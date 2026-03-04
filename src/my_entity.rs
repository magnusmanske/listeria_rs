//! A newtype wrapper around [`wikimisc::wikibase::Entity`] that provides a
//! [`serde::Deserialize`] implementation backed by the entity's own JSON
//! format (i.e. the same format produced by [`wikimisc::wikibase::EntityTrait::to_json`]).

use serde::{Deserialize, Deserializer, Serialize};
use std::ops::{Deref, DerefMut};
use wikimisc::wikibase::Entity;
use wikimisc::wikibase::EntityTrait;

/// A newtype wrapper around [`Entity`] that adds binary-format-safe
/// [`serde::Serialize`]/[`serde::Deserialize`] implementations.
/// The entity is serialized as a JSON string (via [`EntityTrait::to_json`])
/// so that it works correctly with both JSON and binary serializers
/// like bincode (used by the foyer disk cache).
#[derive(Debug, Clone)]
pub struct MyEntity(pub Entity);

// ── Conversions ──────────────────────────────────────────────────────────────

impl From<Entity> for MyEntity {
    fn from(entity: Entity) -> Self {
        Self(entity)
    }
}

impl From<MyEntity> for Entity {
    fn from(my_entity: MyEntity) -> Self {
        my_entity.0
    }
}

// ── Transparent access to the inner Entity ───────────────────────────────────

impl Deref for MyEntity {
    type Target = Entity;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MyEntity {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// ── serde ────────────────────────────────────────────────────────────────────

impl Serialize for MyEntity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize the entity's JSON representation as a String.
        // We avoid delegating to Entity::serialize because it uses
        // serialize_some/serialize_none (Option encoding) which is
        // incompatible with binary formats like bincode.
        let json_string =
            serde_json::to_string(&self.0.to_json()).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&json_string)
    }
}

impl<'de> Deserialize<'de> for MyEntity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize the JSON string, then reconstruct the entity.
        let json_string = String::deserialize(deserializer)?;
        let json: serde_json::Value =
            serde_json::from_str(&json_string).map_err(serde::de::Error::custom)?;
        Entity::new_from_json(&json)
            .map(MyEntity)
            .map_err(serde::de::Error::custom)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use wikimisc::wikibase::EntityTrait;

    fn make_entity_json(id: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "item",
            "id": id,
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "claims": {},
            "sitelinks": {}
        })
    }

    #[test]
    fn test_json_serialize_roundtrip() {
        let json = make_entity_json("Q42");
        let entity = Entity::new_from_json(&json).expect("entity from json failed");
        let my = MyEntity(entity);

        // Serialize to JSON string, then deserialize back
        let serialized = serde_json::to_string(&my).expect("serialize failed");
        let deserialized: MyEntity = serde_json::from_str(&serialized).expect("deserialize failed");
        assert_eq!(deserialized.id(), "Q42");
    }

    #[test]
    fn test_from_entity() {
        let json = make_entity_json("Q5");
        let entity = Entity::new_from_json(&json).expect("entity from json failed");
        let my = MyEntity::from(entity);
        assert_eq!(my.id(), "Q5");
    }

    #[test]
    fn test_into_entity() {
        let json = make_entity_json("Q7");
        let entity = Entity::new_from_json(&json).expect("entity from json failed");
        let my = MyEntity(entity);
        let back: Entity = my.into();
        assert_eq!(back.id(), "Q7");
    }

    #[test]
    fn test_deref() {
        let json = make_entity_json("Q10");
        let entity = Entity::new_from_json(&json).expect("entity from json failed");
        let my = MyEntity(entity);
        // Deref lets us call Entity methods directly.
        assert_eq!(my.id(), "Q10");
        assert!(my.labels().is_empty());
    }

    #[test]
    fn test_invalid_json_returns_error() {
        // A JSON string containing invalid entity JSON
        let bad = serde_json::json!("{\"no_id\": true}");
        let result: Result<MyEntity, _> = serde_json::from_value(bad);
        assert!(result.is_err());
    }

    /// Verify that MyEntity survives a bincode serialize→deserialize roundtrip.
    /// foyer uses bincode for its disk cache, so this must work correctly.
    #[test]
    fn test_bincode_roundtrip() {
        let json = serde_json::json!({
            "type": "item",
            "id": "Q42",
            "labels": {
                "en": { "language": "en", "value": "Douglas Adams" }
            },
            "descriptions": {},
            "aliases": {},
            "claims": {
                "P31": [{
                    "mainsnak": {
                        "snaktype": "value",
                        "property": "P31",
                        "datavalue": {
                            "value": { "entity-type": "item", "numeric-id": 5, "id": "Q5" },
                            "type": "wikibase-entityid"
                        }
                    },
                    "type": "statement",
                    "rank": "normal"
                }]
            },
            "sitelinks": {}
        });
        let entity = Entity::new_from_json(&json).expect("entity from json failed");
        let my = MyEntity(entity);

        // This is what foyer does: bincode serialize then deserialize
        let encoded = bincode::serialize(&my).expect("bincode serialize failed");
        let decoded: MyEntity = bincode::deserialize(&encoded).expect("bincode deserialize failed");

        assert_eq!(decoded.id(), "Q42");
        assert_eq!(
            decoded.label_in_locale("en").map(|s| s.to_string()),
            Some("Douglas Adams".to_string())
        );
    }
}
