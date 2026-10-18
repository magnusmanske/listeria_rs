//! The parts of an entity a list can read.

use std::collections::HashSet;
use wikimisc::wikibase::{Entity, EntityTrait, Statement};

/// The parts of an entity a list can read: statements for some properties
/// (with some of their qualifiers, and their references or not), and labels,
/// descriptions, aliases and sitelinks in some languages and wikis.
/// [`Self::apply`] drops everything else.
///
/// Statements are most of an entity, but the rest adds up too: an item about
/// a person or a city carries labels, descriptions, aliases and sitelinks in
/// hundreds of languages and wikis, ~15–20 KB of JSON, of which a list reads
/// a few hundred bytes. References are most of the statements a list keeps
/// (72% for scholarly articles), but only shown with `references=all`.
///
/// Code that starts reading another part of an entity (another language,
/// wiki, qualifier…) must add it to the projection a list builds (see
/// `ListeriaList::entity_projection`), or it will silently find it missing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EntityProjection {
    properties: HashSet<String>,
    qualifiers: HashSet<String>,
    references: bool,
    label_languages: HashSet<String>,
    description_languages: HashSet<String>,
    alias_languages: HashSet<String>,
    sites: HashSet<String>,
}

impl EntityProjection {
    /// Keeps the statements for these properties.
    #[must_use]
    pub fn with_properties(mut self, properties: impl IntoIterator<Item = String>) -> Self {
        self.properties.extend(properties);
        self
    }

    /// Keeps the qualifiers for these properties (of the statements kept).
    #[must_use]
    pub fn with_qualifiers(mut self, properties: impl IntoIterator<Item = String>) -> Self {
        self.qualifiers.extend(properties);
        self
    }

    /// Keeps the references of the statements kept.
    #[must_use]
    pub const fn with_references(mut self, references: bool) -> Self {
        self.references = references;
        self
    }

    /// Keeps the labels in these languages. The first label is always kept,
    /// whatever its language: it is the last resort of the label fallback
    /// (see `EntityContainerWrapper::label_with_fallback_from_entity`).
    #[must_use]
    pub fn with_label_languages(mut self, languages: impl IntoIterator<Item = String>) -> Self {
        self.label_languages.extend(languages);
        self
    }

    /// Keeps the descriptions in these languages.
    #[must_use]
    pub fn with_description_languages(
        mut self,
        languages: impl IntoIterator<Item = String>,
    ) -> Self {
        self.description_languages.extend(languages);
        self
    }

    /// Keeps the aliases in these languages.
    #[must_use]
    pub fn with_alias_languages(mut self, languages: impl IntoIterator<Item = String>) -> Self {
        self.alias_languages.extend(languages);
        self
    }

    /// Keeps the sitelinks to these wikis.
    #[must_use]
    pub fn with_sites(mut self, sites: impl IntoIterator<Item = String>) -> Self {
        self.sites.extend(sites);
        self
    }

    /// Drops everything from `entity` that this projection does not keep.
    pub fn apply(&self, entity: &mut Entity) {
        let mut is_first = true;
        Self::retain(entity.labels_mut(), |label| {
            std::mem::take(&mut is_first) || self.label_languages.contains(label.language())
        });
        Self::retain(entity.descriptions_mut(), |d| {
            self.description_languages.contains(d.language())
        });
        Self::retain(entity.aliases_mut(), |a| {
            self.alias_languages.contains(a.language())
        });
        if let Some(sitelinks) = entity.sitelinks_mut() {
            Self::retain(sitelinks, |s| self.sites.contains(s.site()));
        }
        Self::retain(entity.claims_mut(), |s| {
            self.properties.contains(s.property())
        });
        entity
            .claims_mut()
            .iter_mut()
            .for_each(|statement| self.apply_to_statement(statement));
    }

    fn apply_to_statement(&self, statement: &mut Statement) {
        if !self.references && !statement.references().is_empty() {
            statement.set_references(Vec::new());
        }
        let is_kept = |property: &str| self.qualifiers.contains(property);
        if !statement.qualifiers().iter().all(|q| is_kept(q.property())) {
            let qualifiers = statement
                .qualifiers()
                .iter()
                .filter(|q| is_kept(q.property()))
                .cloned()
                .collect();
            statement.set_qualifier_snaks(qualifiers);
        }
    }

    fn retain<T>(values: &mut Vec<T>, keep: impl FnMut(&T) -> bool) {
        values.retain(keep);
        values.shrink_to_fit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wikimisc::wikibase::LocaleString;

    fn languages(values: &[LocaleString]) -> Vec<&str> {
        values.iter().map(LocaleString::language).collect()
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).to_string()).collect()
    }

    fn terms(languages: &[&str]) -> serde_json::Value {
        languages
            .iter()
            .map(|l| {
                (
                    (*l).to_string(),
                    json!({ "language": l, "value": format!("v-{l}") }),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    fn snak(property: &str) -> serde_json::Value {
        json!({ "snaktype": "value", "property": property, "datatype": "string",
                "datavalue": { "type": "string", "value": "x" } })
    }

    fn statement(property: &str, qualifiers: &[&str]) -> serde_json::Value {
        let qualifier_map: serde_json::Map<_, _> = qualifiers
            .iter()
            .map(|q| ((*q).to_string(), json!([snak(q)])))
            .collect();
        json!({
            "type": "statement", "rank": "normal", "id": format!("Q1${property}"),
            "mainsnak": snak(property),
            "qualifiers": qualifier_map,
            "qualifiers-order": qualifiers,
            "references": [ { "snaks": { "P248": [ snak("P248") ] }, "snaks-order": ["P248"] } ]
        })
    }

    fn item() -> Entity {
        Entity::new_from_json(&json!({
            "type": "item", "id": "Q1",
            "labels": terms(&["ar", "de", "en", "fr", "ja"]),
            "descriptions": terms(&["de", "en", "fr"]),
            "aliases": { "en": [ { "language": "en", "value": "a-en" } ],
                         "fr": [ { "language": "fr", "value": "a-fr" } ] },
            "sitelinks": {
                "dewiki": { "site": "dewiki", "title": "D", "badges": [] },
                "enwiki": { "site": "enwiki", "title": "E", "badges": [] },
                "frwiki": { "site": "frwiki", "title": "F", "badges": [] } },
            "claims": {
                "P31": [ statement("P31", &[]) ],
                "P39": [ statement("P39", &["P580", "P582"]) ] }
        }))
        .unwrap()
    }

    fn sites(entity: &Entity) -> Vec<&str> {
        entity
            .sitelinks()
            .as_ref()
            .unwrap()
            .iter()
            .map(|s| s.site().as_str())
            .collect()
    }

    #[test]
    fn test_default_keeps_only_the_first_label() {
        let mut entity = item();
        EntityProjection::default().apply(&mut entity);
        assert_eq!(languages(entity.labels()), ["ar"]);
        assert!(entity.descriptions().is_empty());
        assert!(entity.aliases().is_empty());
        assert!(sites(&entity).is_empty());
        assert!(entity.claims().is_empty());
        assert_eq!(entity.id(), "Q1");
    }

    #[test]
    fn test_terms_and_sitelinks_in_listed_languages_and_wikis() {
        let mut entity = item();
        EntityProjection::default()
            .with_label_languages(strings(&["en", "ja", "xx"]))
            .with_description_languages(strings(&["fr"]))
            .with_alias_languages(strings(&["en"]))
            .with_sites(strings(&["enwiki", "frwiki"]))
            .apply(&mut entity);
        // The first label stays, whatever its language: it is the label
        // fallback's last resort, which another kept label must not replace.
        assert_eq!(languages(entity.labels()), ["ar", "en", "ja"]);
        assert_eq!(entity.label_in_locale("en"), Some("v-en"));
        assert_eq!(languages(entity.descriptions()), ["fr"]);
        assert_eq!(languages(entity.aliases()), ["en"]);
        assert_eq!(sites(&entity), ["enwiki", "frwiki"]);
    }

    #[test]
    fn test_missing_sitelinks_stay_missing() {
        // `links=red_only` tells "no sitelinks" apart from "no sitelink here".
        let mut entity = item();
        *entity.sitelinks_mut() = None;
        EntityProjection::default()
            .with_sites(strings(&["enwiki"]))
            .apply(&mut entity);
        assert!(entity.sitelinks().is_none());
    }

    #[test]
    fn test_statements_drop_references_and_unread_qualifiers() {
        let mut entity = item();
        EntityProjection::default()
            .with_properties(strings(&["P39"]))
            .with_qualifiers(strings(&["P580"]))
            .apply(&mut entity);
        let claims = entity.claims();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].property(), "P39");
        let qualifiers: Vec<&str> = claims[0]
            .qualifiers()
            .iter()
            .map(|q| q.property())
            .collect();
        assert_eq!(qualifiers, ["P580"]);
        assert!(claims[0].references().is_empty());
        assert_eq!(claims[0].id().as_deref(), Some("Q1$P39"));
    }

    #[test]
    fn test_references_are_kept_on_request() {
        let mut entity = item();
        EntityProjection::default()
            .with_properties(strings(&["P31", "P39"]))
            .with_references(true)
            .apply(&mut entity);
        assert!(entity.claims().iter().all(|s| s.references().len() == 1));
    }
}
