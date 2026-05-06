//! Wrapper for entity container with caching and batch loading.

use crate::listeria_list::ListeriaList;
use crate::my_entity::MyEntity;

/// A handle to a cached [`MyEntity`]; derefs transparently to [`MyEntity`].
pub type EntityEntry = Arc<MyEntity>;
use crate::result_cell_part::{LinkTarget, LocalLinkInfo, PartWithReference, ResultCellPart};
use crate::result_row::ResultRow;
use crate::template_params::LinksType;
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

/// Characters that must be percent-encoded when an external-id value is
/// substituted into a formatter URL's `$1` placeholder. Keeps characters
/// that are generally safe inside a URL path/query value (alphanumerics,
/// `-_.~/:`) unencoded, encodes everything else — in particular `&`, `=`,
/// `?`, `#`, `+`, `%` and spaces — so an id like `base&id=2352486` cannot
/// inject extra query parameters into the resulting link.
const EXTERNAL_ID_ESCAPE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'#')
    .add(b'?')
    .add(b'{')
    .add(b'}')
    .add(b'[')
    .add(b']')
    .add(b'|')
    .add(b'\\')
    .add(b'^')
    .add(b'&')
    .add(b'=')
    .add(b'+')
    .add(b'%');
use rand::seq::SliceRandom;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use wikimisc::mediawiki::api::Api;
use wikimisc::sparql_table_vec::SparqlTableVec;
use wikimisc::wikibase::Entity;
use wikimisc::wikibase::EntityTrait;
use wikimisc::wikibase::StatementRank;
use wikimisc::wikibase::Value;
use wikimisc::wikibase::entity_container::EntityContainer;
use wikimisc::wikibase::snak::SnakDataType;

/// Maximum number of ids to hand to a single upstream `load_entities` call.
/// The upstream then sub-chunks this into individual `wbgetentities` API
/// requests (size 100 in bot mode).
const LOAD_CHUNK_SIZE: usize = 500;
/// Number of times to retry the subset of ids that the upstream silently
/// omitted from a chunk's response (see [`Self::load_chunk_with_retries`]).
const MAX_LOAD_RETRIES: usize = 3;
/// Wait between retries — gives the upstream API a moment to recover from a
/// transient overload (large response, brief network glitch).
const RETRY_BACKOFF_MS: u64 = 200;

/// Per-page in-memory entity store.
///
/// Earlier revisions used a foyer hybrid (RAM + disk) cache here. That
/// turned out to be the root cause of the persistent label-loss reported in
/// issue #167: under load, foyer would silently fail to return entries that
/// had been inserted (eviction-without-disk-promotion or bloom-filter-style
/// false-positives in the membership check), and the renderer fell back to
/// bare `[[Qxxx]]`. Since the bot processes one page at a time and the
/// working set per page comfortably fits in memory, a plain non-evicting
/// [`DashMap`] is the simplest correct choice.
#[derive(Clone, Debug)]
pub struct EntityContainerWrapper {
    entities: Arc<DashMap<String, Arc<MyEntity>>>,
}

impl EntityContainerWrapper {
    pub async fn new() -> Result<Self> {
        let ret = Self {
            entities: Arc::new(DashMap::new()),
        };
        // Pre-cache test entities if testing
        if cfg!(test) {
            let test_items: serde_json::Value =
                tokio::task::spawn_blocking(|| -> Result<serde_json::Value> {
                    let file = File::open("test_data/test_entities.json")?;
                    let reader = BufReader::new(file);
                    let v = serde_json::from_reader(reader)?;
                    Ok(v)
                })
                .await
                .map_err(|e| anyhow!("spawn_blocking join error: {e}"))??;
            for (_item, j) in test_items.as_object().ok_or(anyhow!("Not an object"))? {
                ret.set_entity_from_json(j)?;
            }
        }
        Ok(ret)
    }

    pub fn set_entity_from_json(&self, json: &serde_json::Value) -> Result<()> {
        let q = json["id"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing 'id' field"))?;
        let entity = Entity::new_from_json(json)?;
        self.entities
            .insert(q.to_string(), Arc::new(MyEntity(entity)));
        Ok(())
    }

    async fn load_entities_into_entity_cache(&self, api: &Api, ids: &[String]) -> Result<()> {
        let chunks = ids.chunks(LOAD_CHUNK_SIZE);
        for chunk in chunks {
            self.load_chunk_with_retries(api, chunk).await?;
        }
        Ok(())
    }

    /// Loads a single chunk via the upstream entity container, then verifies
    /// every requested id was actually inserted into our cache. Any ids that
    /// are missing are retried (with progressively smaller batches, since the
    /// most common failure mode is a large response timing out or being
    /// truncated). After [`MAX_LOAD_RETRIES`] attempts, persistently missing
    /// ids are logged and skipped — better to render a single bare `[[Q…]]`
    /// than to abort the whole page edit.
    ///
    /// This addresses the deterministic label-loss reported in #167: the
    /// upstream `wikibase::EntityContainer::load_entities` returns `Ok` even
    /// when some ids in a chunk silently never appear in the response (e.g.
    /// because the chunk's combined JSON was too large), and our previous
    /// code accepted that silent partial result.
    async fn load_chunk_with_retries(&self, api: &Api, chunk: &[String]) -> Result<()> {
        let mut to_load: Vec<String> = chunk.to_vec();
        for attempt in 0..=MAX_LOAD_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(RETRY_BACKOFF_MS)).await;
            }
            let entity_container = EntityContainer::new();
            if let Err(e) = entity_container.load_entities(api, &to_load).await {
                if attempt == MAX_LOAD_RETRIES {
                    return Err(anyhow!(
                        "Error loading entities after {MAX_LOAD_RETRIES} retries: {e}"
                    ));
                }
                continue;
            }
            self.store_entity_chunk(&to_load, entity_container).await?;

            let missing: Vec<String> = to_load
                .iter()
                .filter(|id| !self.entities.contains_key(*id))
                .cloned()
                .collect();
            if missing.is_empty() {
                return Ok(());
            }
            if attempt == MAX_LOAD_RETRIES {
                log::warn!(
                    "Could not load {} entities after {} retries: {:?}",
                    missing.len(),
                    MAX_LOAD_RETRIES,
                    missing
                );
                return Ok(());
            }
            to_load = missing;
        }
        Ok(())
    }

    async fn store_entity_chunk(
        &self,
        chunk: &[String],
        entity_container: EntityContainer,
    ) -> Result<()> {
        let self2 = self.clone();
        let chunk = chunk.to_vec();
        tokio::task::spawn_blocking(move || -> Result<()> {
            for entity_id in &chunk {
                if let Some(entity) = entity_container.get_entity(entity_id) {
                    let json: serde_json::Value = entity.to_json();
                    self2.set_entity_from_json(&json)?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking join error: {e}"))?
    }

    /// Removes IDs that are already loaded, removes duplicates, and shuffles
    /// the remaining IDs to average load times.
    async fn filter_ids(&self, original_ids: &[String]) -> Result<Vec<String>> {
        let new_ids: Vec<String> = original_ids
            .iter()
            .filter(|id| !self.entities.contains_key(*id))
            .map(|id| id.to_owned())
            .collect();
        tokio::task::spawn_blocking(move || Self::unique_shuffle_entity_ids(&new_ids))
            .await
            .map_err(|e| anyhow!("spawn_blocking join error: {e}"))?
    }

    fn unique_shuffle_entity_ids(ids: &[String]) -> Result<Vec<String>> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        ids.shuffle(&mut rand::rng());
        Ok(ids)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entities.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Loads the entities for the given IDs
    pub async fn load_entities(&self, api: &Api, ids: &[String]) -> Result<()> {
        let ids = self.filter_ids(ids).await?;
        if ids.is_empty() {
            return Ok(());
        }
        if cfg!(test) {
            log::warn!("ATTENTION: Trying to load items {ids:?}");
        }

        self.load_entities_into_entity_cache(api, &ids).await
    }

    pub async fn get_entity(&self, entity_id: &str) -> Option<EntityEntry> {
        if cfg!(test) {
            println!("{entity_id}\tentity_loaded");
        }
        self.entities.get(entity_id).map(|e| e.value().clone())
    }

    pub async fn get_local_entity_label(
        &self,
        entity: &MyEntity,
        language: &str,
    ) -> Option<String> {
        entity.label_in_locale(language).map(|s| s.to_string())
    }

    pub async fn get_entity_label_with_fallback(&self, entity_id: &str, language: &str) -> String {
        let Some(entity) = self.get_entity(entity_id).await else {
            return entity_id.to_string();
        };
        Self::label_with_fallback_from_entity(&entity, language, entity_id)
    }

    /// Returns an entity's label, falling back through `language` → `mul` →
    /// a built-in language list → any available label → the entity id.
    ///
    /// Takes an already-resolved entity so callers in synchronous contexts
    /// (e.g. [`ct_label`]) can avoid the async round-trip.
    pub fn label_with_fallback_from_entity(
        entity: &MyEntity,
        language: &str,
        entity_id: &str,
    ) -> String {
        if let Some(label) = entity.label_in_locale(language) {
            return label.to_string();
        }

        for lang in ["mul", "en", "de", "fr", "es", "it", "el", "nl"] {
            if let Some(label) = entity.label_in_locale(lang) {
                return label.to_string();
            }
        }

        if let Some(label) = entity.labels().first() {
            return label.value().to_string();
        }

        entity_id.to_string()
    }

    pub async fn entity_to_local_link(
        &self,
        item: &str,
        wiki: &str,
        language: &str,
    ) -> Option<ResultCellPart> {
        let entity = self.get_entity(item).await?;
        let page = entity
            .sitelinks()
            .as_ref()?
            .iter()
            .find(|s| *s.site() == wiki)
            .map(|s| s.title().to_string())?;

        let label = self
            .get_local_entity_label(&entity, language)
            .await
            .unwrap_or_else(|| page.to_string());

        Some(ResultCellPart::LocalLink(LocalLinkInfo::new(
            page,
            label,
            LinkTarget::Page,
        )))
    }

    pub async fn get_result_row(
        &self,
        entity_id: &str,
        sparql_table: &SparqlTableVec,
        list: &ListeriaList,
    ) -> Option<ResultRow> {
        if sparql_table.is_empty() {
            return None;
        }
        self.use_local_links(list, entity_id).await?;

        let mut row = ResultRow::new(entity_id);
        row.from_columns(list, sparql_table).await;
        Some(row)
    }

    async fn use_local_links(&self, list: &ListeriaList, entity_id: &str) -> Option<()> {
        if LinksType::Local == *list.template_params().links() {
            let entity = self.get_entity(entity_id).await?;
            entity
                .sitelinks()
                .as_ref()?
                .iter()
                .find(|s| *s.site() == *list.wiki())?;
        }
        Some(())
    }

    pub async fn external_id_url(&self, prop: &str, id: &str) -> Option<String> {
        let pi = self.get_entity(prop).await?;
        let mut claims: Vec<_> = pi
            .claims_with_property("P1630")
            .iter()
            .filter(|s| *s.rank() != StatementRank::Deprecated)
            .cloned()
            .collect();
        let has_preferred = claims.iter().any(|s| *s.rank() == StatementRank::Preferred);
        if has_preferred {
            claims.retain(|s| *s.rank() == StatementRank::Preferred);
        }
        let encoded_id = utf8_percent_encode(id, EXTERNAL_ID_ESCAPE).to_string();
        claims
            .iter()
            .filter_map(|s| {
                let data_value = s.main_snak().data_value().to_owned()?;
                match data_value.value() {
                    Value::StringValue(s2) => Some(s2.replace("$1", &encoded_id)),
                    Value::Coordinate(_coordinate) => None,
                    Value::MonoLingual(_mono_lingual_text) => None,
                    Value::Entity(_entity_value) => None,
                    Value::EntitySchema(_entity_value) => None,
                    Value::Quantity(_quantity_value) => None,
                    Value::Time(_time_value) => None,
                }
            })
            .next()
    }

    pub async fn get_datatype_for_property(&self, prop: &str) -> SnakDataType {
        #[allow(clippy::collapsible_match)]
        match self.get_entity(prop).await {
            Some(entity) => match &entity.0 {
                Entity::Property(p) => match p.datatype() {
                    Some(t) => t.to_owned(),
                    None => SnakDataType::String,
                },
                _ => SnakDataType::String,
            },
            None => SnakDataType::String,
        }
    }

    #[must_use]
    pub fn gather_entities_and_external_properties(parts: &[PartWithReference]) -> Vec<String> {
        let mut entities_to_load = Vec::new();
        Self::gather_entities_and_external_properties_into(parts, &mut entities_to_load);
        entities_to_load
    }

    fn gather_entities_and_external_properties_into(
        parts: &[PartWithReference],
        out: &mut Vec<String>,
    ) {
        for part_with_reference in parts {
            match part_with_reference.part() {
                ResultCellPart::Entity(entity_info) if entity_info.try_localize => {
                    out.push(entity_info.id.to_owned());
                }
                ResultCellPart::ExternalId(ext_id_info) => {
                    out.push(ext_id_info.property.to_owned());
                }
                ResultCellPart::SnakList(v) => {
                    Self::gather_entities_and_external_properties_into(v, out);
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_entity_caching() {
        let ecw = EntityContainerWrapper::new().await.unwrap();
        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        let ids: Vec<String> = ["Q1", "Q2", "Q3", "Q4", "Q5"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        ecw.load_entities(&api, &ids).await.unwrap();

        let e2 = ecw.get_entity("Q2").await.unwrap();
        assert_eq!(e2.id(), "Q2");
    }

    /// Regression test for #167: `filter_ids` must skip already-loaded
    /// entities while preserving unloaded ones. The previous foyer-backed
    /// implementation could falsely report unloaded entities as cached
    /// (hash-collision false positives), causing them to be silently dropped
    /// from the fetch list and rendered as bare `[[Qxxx]]` in the output.
    #[tokio::test]
    async fn test_filter_ids_only_skips_actually_inserted_entities() {
        let ecw = EntityContainerWrapper::new().await.unwrap();

        // Insert one entity directly via the JSON path (no network).
        let json = serde_json::json!({
            "type": "item",
            "id": "Q42",
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "claims": {},
            "sitelinks": {}
        });
        ecw.set_entity_from_json(&json).unwrap();

        let ids = vec![
            "Q42".to_string(),     // already loaded → must be filtered out
            "Q188451".to_string(), // never inserted → must remain
            "Q7777573".to_string(), // never inserted → must remain
        ];
        let mut filtered = ecw.filter_ids(&ids).await.unwrap();
        filtered.sort();
        assert_eq!(filtered, vec!["Q188451".to_string(), "Q7777573".to_string()]);
    }

    #[tokio::test]
    async fn test_len_grows_with_inserts() {
        let ecw = EntityContainerWrapper::new().await.unwrap();
        // `new()` pre-seeds the cache with test fixtures under cfg(test);
        // assert deltas from that baseline rather than absolute sizes.
        let baseline = ecw.len();

        // Use IDs that are not in the test fixtures.
        for id in ["Q9999991", "Q9999992", "Q9999993"] {
            let json = serde_json::json!({
                "type": "item",
                "id": id,
                "labels": {},
                "descriptions": {},
                "aliases": {},
                "claims": {},
                "sitelinks": {}
            });
            ecw.set_entity_from_json(&json).unwrap();
        }
        assert_eq!(ecw.len(), baseline + 3);

        // Re-inserting an existing ID does not double-count.
        let json = serde_json::json!({
            "type": "item",
            "id": "Q9999991",
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "claims": {},
            "sitelinks": {}
        });
        ecw.set_entity_from_json(&json).unwrap();
        assert_eq!(ecw.len(), baseline + 3);
    }

    /// Regression test for #167: an entity inserted via `set_entity_from_json`
    /// must be retrievable via `get_entity` afterwards, indefinitely. The
    /// previous foyer-backed implementation evicted entries from RAM under
    /// load and the disk-fallback path silently failed to return them,
    /// causing the renderer to fall back to bare `[[Qxxx]]`.
    #[tokio::test]
    async fn test_inserted_entity_is_retrievable() {
        let ecw = EntityContainerWrapper::new().await.unwrap();
        let json = serde_json::json!({
            "type": "item",
            "id": "Q12345",
            "labels": {"en": {"language": "en", "value": "test entity"}},
            "descriptions": {},
            "aliases": {},
            "claims": {},
            "sitelinks": {}
        });
        ecw.set_entity_from_json(&json).unwrap();

        let got = ecw.get_entity("Q12345").await.expect("entity must be present");
        assert_eq!(got.id(), "Q12345");
        assert_eq!(
            got.label_in_locale("en"),
            Some("test entity"),
            "label must be readable from the cached entity"
        );
    }
}
