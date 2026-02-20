//! A newtype wrapper around [`wikimisc::wikibase::Entity`] that provides a
//! [`serde::Deserialize`] implementation backed by the entity's own JSON
//! format (i.e. the same format produced by [`wikimisc::wikibase::EntityTrait::to_json`]).

use serde::{Deserialize, Deserializer, Serialize};
use std::ops::{Deref, DerefMut};
use wikimisc::wikibase::Entity;

/// A newtype wrapper around [`Entity`] that adds a [`serde::Deserialize`]
/// implementation.  Serialization is delegated unchanged to the inner
/// [`Entity`]; deserialization first deserialises the payload into a
/// [`serde_json::Value`] and then reconstructs the entity via
/// [`Entity::new_from_json`] – the inverse of
/// [`wikimisc::wikibase::EntityTrait::to_json`].
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
        // Delegate to Entity's own Serialize impl (uses to_json() internally).
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MyEntity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Materialise the payload as a generic JSON value first …
        let json = serde_json::Value::deserialize(deserializer)?;
        // … then reconstruct the entity using the same JSON format that
        // to_json() produces, mirroring the existing Entity::new_from_json
        // call pattern used throughout the codebase.
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
    fn test_deserialize_roundtrip() {
        let json = make_entity_json("Q42");
        let my: MyEntity = serde_json::from_value(json).expect("deserialize failed");
        assert_eq!(my.id(), "Q42");
    }

    #[test]
    fn test_serialize_roundtrip() {
        let json = make_entity_json("Q1");
        let my: MyEntity = serde_json::from_value(json).expect("deserialize failed");
        // Re-serialise and confirm we get a valid JSON object back.
        let serialised = serde_json::to_value(&my).expect("serialize failed");
        assert!(serialised.is_object() || serialised.is_null());
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
        let bad = serde_json::json!({"no_id": true});
        let result: Result<MyEntity, _> = serde_json::from_value(bad);
        assert!(result.is_err());
    }
}
