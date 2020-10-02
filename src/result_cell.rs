use crate::result_cell_part::PartWithReference;
use crate::listeria_list::ListeriaList;
use crate::reference::Reference;
use crate::result_cell_part::ResultCellPart;
use crate::column::*;
use crate::{SparqlValue,ReferencesParameter};
use serde_json::Value;
use wikibase::entity::EntityTrait;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct ResultCell {
    parts: Vec<PartWithReference>,
    wdedit_class: Option<String>,
}

impl ResultCell {
    pub async fn new(
        list:&ListeriaList,
        entity_id: &str,
        sparql_rows: &[&HashMap<String, SparqlValue>],
        col: &Column,
    ) -> Self {
        let mut ret = Self { parts:vec![] , wdedit_class:None };

        let entity = list.get_entity(entity_id.to_owned());
        match &col.obj {
            ColumnType::Item => {
                ret.parts.push(PartWithReference::new(ResultCellPart::Entity((entity_id.to_owned(), false)),None));
            }
            ColumnType::Description => if let Some(e) = entity { match e.description_in_locale(list.language()) {
                Some(s) => {
                    ret.wdedit_class = Some("wd_desc".to_string());
                    ret.parts.push(PartWithReference::new(ResultCellPart::Text(s.to_string()),None));
                }
                None => {
                    if let Ok(s) = list.get_autodesc_description(&e).await {
                        ret.parts.push(PartWithReference::new(ResultCellPart::Text(s),None));
                    }
                }
            } },
            ColumnType::Field(varname) => {
                for row in sparql_rows.iter() {
                    if let Some(x) = row.get(varname) {
                        ret.parts.push(PartWithReference::new(ResultCellPart::from_sparql_value(x),None));
                    }
                }
            }
            ColumnType::Property(property) => if let Some(e) = entity {
                ret.wdedit_class = Some(format!("wd_{}",property.to_lowercase()));
                list.get_filtered_claims(&e,property)
                    .iter()
                    .for_each(|statement| {
                        let references = match list.get_reference_parameter() {
                            ReferencesParameter::All => Self::get_references_for_statement(&statement,list.language()),
                            _ => None
                        };
                        ret.parts
                            .push(PartWithReference::new(ResultCellPart::from_snak(statement.main_snak()),references));
                    });
            },
            ColumnType::PropertyQualifier((p1, p2)) => if let Some(e) = entity {
                list.get_filtered_claims(&e,p1)
                    .iter()
                    .for_each(|statement| {
                        ret.get_parts_p_p(statement,p2)
                            .iter()
                            .for_each(|part|ret.parts.push(PartWithReference::new(part.to_owned(),None)));
                    });
            },
            ColumnType::PropertyQualifierValue((p1, q1, p2)) => if let Some(e) = entity {
                list.get_filtered_claims(&e,p1)
                    .iter()
                    .for_each(|statement| {
                        ret.get_parts_p_q_p(statement,q1,p2)
                            .iter()
                            .for_each(|part|ret.parts.push(PartWithReference::new(part.to_owned(),None)));
                    });
            },
            ColumnType::LabelLang(language) => if let Some(e) = entity {
                match e.label_in_locale(language) {
                    Some(s) => {
                        ret.parts.push(PartWithReference::new(ResultCellPart::Text(s.to_string()),None));
                    }
                    None => if let Some(s) = e.label_in_locale(list.language()) {
                        ret.parts.push(PartWithReference::new(ResultCellPart::Text(s.to_string()),None));
                    },
                }
            },
            ColumnType::Label => if let Some(e) = entity {
                ret.wdedit_class = Some("wd_label".to_string());
                let label = match e.label_in_locale(list.language()) {
                    Some(s) => s.to_string(),
                    None => entity_id.to_string(),
                };
                let local_page = match e.sitelinks() {
                    Some(sl) => sl
                        .iter()
                        .filter(|s| *s.site() == *list.wiki())
                        .map(|s| s.title().to_string())
                        .next(),
                    None => None,
                };
                match local_page {
                    Some(page) => {
                        ret.parts.push(PartWithReference::new(ResultCellPart::LocalLink((page, label)),None));
                    }
                    None => {
                        ret.parts
                            .push(PartWithReference::new(ResultCellPart::Entity((entity_id.to_string(), true)),None));
                    }
                }
            },
            ColumnType::Unknown => {} // Ignore
            ColumnType::Number => {
                ret.parts.push(PartWithReference::new(ResultCellPart::Number,None));
            }
        }

        ret
    }

    fn get_parts_p_p(&self,statement:&wikibase::statement::Statement,property:&str) -> Vec<ResultCellPart> {
        statement
            .qualifiers()
            .iter()
            .filter(|snak|*snak.property()==*property)
            .map(|snak|ResultCellPart::SnakList (
                    vec![
                        PartWithReference::new(
                            ResultCellPart::from_snak(statement.main_snak()),
                            None
                        ),
                        PartWithReference::new(
                            ResultCellPart::from_snak(snak),
                            None
                        )
                    ]
                )
            )
            .collect()
    }

    fn get_references_for_statement(statement: &wikibase::statement::Statement,language:&str) -> Option<Vec<Reference>> {
        let references = statement.references() ;
        let mut ret : Vec<Reference> = vec![] ;
        for reference in references.iter() {
            if let Some(r) = Reference::new_from_snaks(reference.snaks(),language) {
                ret.push(r);    
            }

        }
        if ret.is_empty() { None } else { Some(ret) }
    }

    fn get_parts_p_q_p(&self,statement:&wikibase::statement::Statement,target_item:&str,property:&str) -> Vec<ResultCellPart> {
        let links_to_target = match statement.main_snak().data_value(){
            Some(dv) => {
                match dv.value() {
                    wikibase::value::Value::Entity(e) => e.id() == target_item,
                    _ => false
                }
            }
            None => false
        };
        if !links_to_target {
            return vec![];
        }
        self.get_parts_p_p(statement,property)
    }

    pub fn get_sortkey(&self) -> String {
        match self.parts.get(0) {
            Some(part_with_reference) => {
                match &part_with_reference.part {
                    ResultCellPart::Entity((id,_)) => id.to_owned(),
                    ResultCellPart::LocalLink((page,_label)) => page.to_owned(),
                    ResultCellPart::Time(time) => time.to_owned(),
                    ResultCellPart::File(s) => s.to_owned(),
                    ResultCellPart::Uri(s) => s.to_owned(),
                    ResultCellPart::Text(s) => s.to_owned(),
                    ResultCellPart::ExternalId((_prop,id)) => id.to_owned(),
                    _ => String::new()
                }
            }
            None => String::new()
        }
    }

    pub fn parts(&self) -> &Vec<PartWithReference> {
        &self.parts
    }

    pub fn parts_mut(&mut self) -> &mut Vec<PartWithReference> {
        &mut self.parts
    }

    pub fn set_parts(&mut self, parts:Vec<PartWithReference> ) {
        self.parts = parts ;
    }

    pub fn localize_item_links_in_parts(list: &ListeriaList, parts: &mut Vec<PartWithReference>) {
        for part_with_reference in parts.iter_mut() {
            part_with_reference.part.localize_item_links(list);
        }
    }

    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> Value {
        let ret: Vec<String> = self
            .parts
            .iter()
            .enumerate()
            .map(|(partnum, part_with_reference)| part_with_reference.part.as_tabbed_data(list, rownum, colnum, partnum))
            .collect();
        json!(ret.join("<br/>"))
    }

    pub fn as_wikitext(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        let mut ret ;
        if list.template_params().wdedit {
            ret = match &self.wdedit_class {
                Some(class) => format!("class='{}'| ",class.to_owned()),
                None => " ".to_string()
            };
        } else {
            ret = " ".to_string();
        }
        ret += &self.parts
            .iter()
            .enumerate()
            .map(|(partnum, part_with_reference)| part_with_reference.as_wikitext(list, rownum, colnum, partnum))
            .collect::<Vec<String>>()
            .join("<br/>") ;
        ret
    }
}
