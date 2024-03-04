use crate::configuration::Configuration;
use crate::entity_file_cache::EntityFileCache;
use crate::listeria_list::ListeriaList;
use crate::result_cell_part::PartWithReference;
use crate::result_cell_part::ResultCellPart;
use crate::result_row::ResultRow;
use crate::sparql_value::SparqlValue;
use crate::template_params::LinksType;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use wikibase::entity::*;
use wikibase::entity_container::EntityContainer;
use wikibase::mediawiki::api::Api;
use wikibase::snak::SnakDataType;

#[derive(Clone)]
pub struct EntityContainerWrapper {
    entities: EntityContainer,
    max_local_cached_entities: usize,
    entity_file_cache: EntityFileCache,
    profiling: bool,
}

impl std::fmt::Debug for EntityContainerWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityContainerWrapper")
            .field("entities", &self.entities)
            .finish()
    }
}

impl EntityContainerWrapper {
    pub fn new(config: Arc<Configuration>) -> Self {
        Self {
            entities: config.create_entity_container(),
            max_local_cached_entities: config.max_local_cached_entities(),
            entity_file_cache: EntityFileCache::new(),
            profiling: config.profiling(),
        }
    }

    async fn load_entities_into_entity_cache(&mut self, api: &Api, ids: &[String]) -> Result<()> {
        let chunks = ids.chunks(self.max_local_cached_entities);
        for chunk in chunks {
            if let Err(e) = self.entities.load_entities(api, &chunk.into()).await {
                return Err(anyhow!("Error loading entities: {e}"));
            }
            for entity_id in chunk {
                if let Some(entity) = self.entities.get_entity(entity_id) {
                    let json = entity.to_json();
                    self.entity_file_cache
                        .add_entity(entity_id, &json.to_string())?;
                }
            }
            self.entities.clear();
        }
        Ok(())
    }

    pub async fn load_entities(&mut self, api: &Api, ids: &[String]) -> Result<()> {
        let ids: Vec<String> = ids
            .iter()
            .filter(|id| !self.entities.has_entity(id.as_str()))
            .filter(|id| !self.entity_file_cache.has_entity(id))
            .map(|id| id.to_owned())
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let ids = self
            .entities
            .unique_shuffle_entity_ids(&ids)
            .map_err(|e| anyhow!("{e}"))?;
        if self.profiling {
            println!(
                "ECW::load_entities: loading {} entities on top of {} loaded, cutoff is {}",
                ids.len(),
                self.entities.len(),
                self.max_local_cached_entities
            );
        }
        if ids.len() + self.entities.len() > self.max_local_cached_entities {
            // Use entity cache
            self.load_entities_into_entity_cache(api, &ids).await?;
            Ok(())
        } else {
            match self.entities.load_entities(api, &ids).await {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!("Error loading entities: {e}")),
            }
        }
    }

    pub fn get_entity(&self, entity_id: &str) -> Option<Entity> {
        if let Some(entity) = self.entities.get_entity(entity_id) {
            return Some(entity);
        }
        let json_string = self.entity_file_cache.get_entity(entity_id)?;
        let json_value = serde_json::from_str(&json_string).ok()?;
        Entity::new_from_json(&json_value).ok()
    }

    pub fn get_local_entity_label(&self, entity_id: &str, language: &str) -> Option<String> {
        self.get_entity(entity_id)?
            .label_in_locale(language)
            .map(|s| s.to_string())
    }

    pub fn entity_to_local_link(
        &self,
        item: &str,
        wiki: &str,
        language: &str,
    ) -> Option<ResultCellPart> {
        let entity = match self.get_entity(item) {
            Some(e) => e,
            None => return None,
        };
        let page = match entity.sitelinks() {
            Some(sl) => sl
                .iter()
                .filter(|s| *s.site() == wiki)
                .map(|s| s.title().to_string())
                .next(),
            None => None,
        }?;
        let label = self
            .get_local_entity_label(item, language)
            .unwrap_or_else(|| page.clone());
        Some(ResultCellPart::LocalLink((page, label, false)))
    }

    pub fn get_result_row(
        &self,
        entity_id: &str,
        sparql_rows: &[&HashMap<String, SparqlValue>],
        list: &ListeriaList,
    ) -> Option<ResultRow> {
        if sparql_rows.is_empty() {
            return None;
        }
        if LinksType::Local == *list.template_params().links() {
            let entity = match self.get_entity(entity_id) {
                Some(e) => e,
                None => return None,
            };
            let page = match entity.sitelinks() {
                Some(sl) => sl
                    .iter()
                    .filter(|s| *s.site() == *list.wiki())
                    .map(|s| s.title().to_string())
                    .next(),
                None => None,
            };
            page.as_ref()?; // return None if no page on this wiki
        }

        let mut row = ResultRow::new(entity_id);
        row.from_columns(list, sparql_rows);
        Some(row)
    }

    pub fn external_id_url(&self, prop: &str, id: &str) -> Option<String> {
        let pi = self.get_entity(prop)?;
        pi.claims_with_property("P1630")
            .iter()
            .filter_map(|s| {
                let data_value = s.main_snak().data_value().to_owned()?;
                match data_value.value() {
                    wikibase::Value::StringValue(s) => {
                        Some(s.to_owned().replace("$1", &urlencoding::decode(&id).ok()?))
                    }
                    _ => None,
                }
            })
            .next()
    }

    pub fn get_datatype_for_property(&self, prop: &str) -> SnakDataType {
        match self.get_entity(prop) {
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

    pub fn gather_entities_and_external_properties(
        &self,
        parts: &[PartWithReference],
    ) -> Vec<String> {
        let mut entities_to_load = vec![];
        for part_with_reference in parts {
            match &part_with_reference.part {
                ResultCellPart::Entity((item, true)) => {
                    entities_to_load.push(item.to_owned());
                }
                ResultCellPart::ExternalId((property, _id)) => {
                    entities_to_load.push(property.to_owned());
                }
                ResultCellPart::SnakList(v) => self
                    .gather_entities_and_external_properties(&v)
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
        let config = Arc::new(Configuration::new_from_file("config.json").await.unwrap());
        let mut ecw = EntityContainerWrapper::new(config);
        let api = wikibase::mediawiki::api::Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        let ids: Vec<String> = ["Q1", "Q2", "Q3", "Q4", "Q5"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        ecw.max_local_cached_entities = 2;
        ecw.load_entities(&api, &ids).await.unwrap();
        assert_eq!(ecw.entities.len(), 0);

        let e2 = ecw.get_entity("Q2").unwrap();
        assert_eq!(e2.id(), "Q2");
    }
}
