use std::collections::HashMap;
use std::sync::Arc;
use crate::{PageParams,SparqlValue,LinksType};
use crate::listeria_list::ListeriaList;
use crate::result_row::ResultRow;
use crate::result_cell_part::PartWithReference;
use crate::result_cell_part::ResultCellPart;
use wikibase::mediawiki::api::Api;
use wikibase::entity::*;
use wikibase::snak::SnakDataType;
use wikibase::entity_container::EntityContainer;

#[derive(Debug, Clone)]
pub struct EntityContainerWrapper {
    entities: EntityContainer,
    page_params:Arc<PageParams>
}

impl EntityContainerWrapper {
    pub fn new(page_params:Arc<PageParams>) -> Self {
        Self {
            entities: EntityContainer::new(),
            page_params
        }
    }

    pub async fn load_entities(&mut self,api: &Api, ids: &Vec<String>) -> Result<(),String> {
        match self.entities.load_entities(api, ids).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error loading entities: {:?}", &e)),
        }
    }

    pub fn get_local_entity_label(&self, entity_id: &str, language: &str) -> Option<String> {
        self.entities
            .get_entity(entity_id.to_owned())?
            .label_in_locale(language)
            .map(|s| s.to_string())
    }

    pub fn entity_to_local_link(&self, item: &str, wiki: &str, language: &str) -> Option<ResultCellPart> {
        let entity = match self.entities.get_entity(item.to_owned()) {
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
        //let title = wikibase::mediawiki::title::Title::new_from_full(page,&mw_api);
        let label = self.get_local_entity_label(item, language).unwrap_or_else(|| page.clone());
        Some(ResultCellPart::LocalLink((page, label, false)))
    }

    pub async fn get_result_row(
        &self,
        entity_id: &str,
        sparql_rows: &[&HashMap<String, SparqlValue>],
        list: &ListeriaList,
    ) -> Option<ResultRow> {
        if let LinksType::Local = list.template_params().links {
            let entity = match self.entities.get_entity(entity_id.to_owned()) {
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
        row.from_columns(list,sparql_rows).await;
        Some(row)
    }

    pub fn external_id_url(&self, prop: &str, id: &str) -> Option<String> {
        let pi = self.entities.get_entity(prop.to_owned())?;
        pi.claims_with_property("P1630")
            .iter()
            .filter_map(|s| {
                let data_value = s.main_snak().data_value().to_owned()?;
                match data_value.value() {
                    wikibase::Value::StringValue(s) => 
                        Some(
                        s.to_owned()
                            .replace("$1", &urlencoding::decode(&id).ok()?),
                    ),
                    _ => None,
                }
            })
            .next()
    }

    pub fn get_filtered_claims(&self,e:&wikibase::entity::Entity,property:&str) -> Vec<wikibase::statement::Statement> {
        let mut ret : Vec<wikibase::statement::Statement> = e
            .claims_with_property(property)
            .iter()
            .map(|x|(*x).clone())
            .collect();

        if self.page_params.config.prefer_preferred() {
            let has_preferred = ret.iter().any(|x|*x.rank()==wikibase::statement::StatementRank::Preferred);
            if has_preferred {
                ret.retain(|x|*x.rank()==wikibase::statement::StatementRank::Preferred);
            }
            ret
        } else {
            ret
        }
    }

    pub fn get_datatype_for_property(&self,prop:&str) -> SnakDataType {
        match self.entities.get_entity(prop) {
            Some(entity) => {
                match entity {
                    Entity::Property(p) => {
                        match p.datatype() {
                            Some(t) => t.to_owned(),
                            None => SnakDataType::String
                        }
                    }
                    _ => SnakDataType::String
                }
            }
            None => SnakDataType::String
        }
    }

    pub fn gather_entities_and_external_properties(&self,parts:&[PartWithReference]) -> Vec<String> {
        let mut entities_to_load = vec![];
        for part_with_reference in parts {
            match &part_with_reference.part {
                ResultCellPart::Entity((item, true)) => {
                    entities_to_load.push(item.to_owned());
                }
                ResultCellPart::ExternalId((property, _id)) => {
                    entities_to_load.push(property.to_owned());
                }
                ResultCellPart::SnakList(v) => {
                    self.gather_entities_and_external_properties(&v)
                        .iter()
                        .for_each(|entity_id|entities_to_load.push(entity_id.to_string()))
                }
                _ => {}
            }
        }
        entities_to_load
    }

    pub fn entities(&self) -> &EntityContainer {
        &self.entities
    }

}
