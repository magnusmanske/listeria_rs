//! Wrapper for entity container with caching and batch loading.

use crate::listeria_list::ListeriaList;
use crate::result_cell_part::{LinkTarget, LocalLinkInfo, PartWithReference, ResultCellPart};
use crate::result_row::ResultRow;
use crate::template_params::LinksType;
use anyhow::{Result, anyhow};
use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};
use rand::seq::SliceRandom;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use wikimisc::mediawiki::api::Api;
use wikimisc::sparql_table_vec::SparqlTableVec;
use wikimisc::wikibase::Entity;
use wikimisc::wikibase::EntityTrait;
use wikimisc::wikibase::StatementRank;
use wikimisc::wikibase::Value;
use wikimisc::wikibase::entity_container::EntityContainer;
use wikimisc::wikibase::snak::SnakDataType;

const CACHE_CAPACITY_MB: usize = 512;
const RAM_CAPACITY: usize = 1500;

#[derive(Clone)]
pub struct EntityContainerWrapper {
    entities: HybridCache<String, String>,
    entity_count: Arc<AtomicUsize>,
    _temp_dir: Arc<tempfile::TempDir>,
}

impl std::fmt::Debug for EntityContainerWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityContainerWrapper")
            .field("entities", &self.entities)
            .finish_non_exhaustive()
    }
}

impl EntityContainerWrapper {
    pub async fn new() -> Result<Self> {
        let (entities, temp_dir) = Self::create_entity_container().await?;
        let ret = Self {
            entities,
            entity_count: Arc::new(AtomicUsize::new(0)),
            _temp_dir: Arc::new(temp_dir),
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

    pub async fn create_entity_container()
    -> Result<(HybridCache<String, String>, tempfile::TempDir)> {
        let dir = tempfile::tempdir()?;
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(CACHE_CAPACITY_MB * 1024 * 1024)
            .build()?;
        let engine_config = BlockEngineConfig::new(device);

        let hybrid: HybridCache<String, String> = HybridCacheBuilder::new()
            .memory(RAM_CAPACITY)
            .storage()
            .with_engine_config(engine_config)
            .with_compression(foyer::Compression::Lz4)
            .build()
            .await?;
        Ok((hybrid, dir))
    }

    pub fn set_entity_from_json(&self, json: &serde_json::Value) -> Result<()> {
        let q = json["id"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing 'id' field"))?;
        let json_string = json.to_string();
        if !self.entities.contains(q) {
            self.entity_count.fetch_add(1, Ordering::Relaxed);
        }
        self.entities.insert(q.to_string(), json_string);
        Ok(())
    }

    async fn load_entities_into_entity_cache(&self, api: &Api, ids: &[String]) -> Result<()> {
        let chunks = ids.chunks(500); // 500 is just some guess
        for chunk in chunks {
            let entity_container = EntityContainer::new();
            if let Err(e) = entity_container.load_entities(api, &chunk.into()).await {
                return Err(anyhow!("Error loading entities: {e}"));
            }
            self.store_entity_chunk(chunk, entity_container).await?;
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

    /// Removes IDs that are already loaded, removes duplicates, and shuffles the remaining IDs to average load times
    async fn filter_ids(&self, original_ids: &[String]) -> Result<Vec<String>> {
        let new_ids: Vec<String> = original_ids
            .iter()
            .filter(|id| !self.entities.contains(*id))
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
        self.entity_count.load(Ordering::Relaxed)
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

    pub async fn get_entity(&self, entity_id: &str) -> Option<Entity> {
        if cfg!(test) {
            println!("{entity_id}\tentity_loaded");
        }
        let cache_entry = self.entities.get(&entity_id.to_string()).await.ok()??;
        let json_string = cache_entry.to_string();
        let v = serde_json::from_str(&json_string).ok()?;
        Entity::new_from_json(&v).ok()
    }

    pub async fn get_local_entity_label(&self, entity_id: &str, language: &str) -> Option<String> {
        self.get_entity(entity_id)
            .await?
            .label_in_locale(language)
            .map(|s| s.to_string())
    }

    pub async fn get_entity_label_with_fallback(&self, entity_id: &str, language: &str) -> String {
        let Some(entity) = self.get_entity(entity_id).await else {
            return entity_id.to_string();
        };

        if let Some(label) = entity.label_in_locale(language) {
            return label.to_string();
        }

        for lang in ["mul", "en", "de", "fr", "es", "it", "el", "nl"] {
            if let Some(label) = entity.label_in_locale(lang) {
                return label.to_string();
            }
        }

        if let Some(entity2) = self.get_entity(entity_id).await
            && let Some(label) = entity2.labels().first()
        {
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
            .get_local_entity_label(item, language)
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
        claims
            .iter()
            .filter_map(|s| {
                let data_value = s.main_snak().data_value().to_owned()?;
                match data_value.value() {
                    Value::StringValue(s2) => {
                        Some(s2.to_owned().replace("$1", &urlencoding::decode(id).ok()?))
                    }
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
            Some(entity) => match entity {
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
        for part_with_reference in parts {
            match part_with_reference.part() {
                ResultCellPart::Entity(entity_info) if entity_info.try_localize => {
                    entities_to_load.push(entity_info.id.to_owned());
                }
                ResultCellPart::ExternalId(ext_id_info) => {
                    entities_to_load.push(ext_id_info.property.to_owned());
                }
                ResultCellPart::SnakList(v) => Self::gather_entities_and_external_properties(v)
                    .iter()
                    .for_each(|entity_id| entities_to_load.push(entity_id.to_string())),
                _ => {}
            }
        }
        entities_to_load
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
}
