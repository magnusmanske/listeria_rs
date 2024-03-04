use crate::column::*;
use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::listeria_list::ListeriaList;
use crate::reference::Reference;
use crate::result_cell_part::AutoDesc;
use crate::result_cell_part::PartWithReference;
use crate::result_cell_part::ResultCellPart;
use crate::sparql_value::SparqlValue;
use crate::template_params::ReferencesParameter;
use serde_json::Value;
use std::collections::HashMap;
use wikibase::entity::EntityTrait;

#[derive(Debug, Clone, PartialEq)]
pub struct ResultCell {
    parts: Vec<PartWithReference>,
    wdedit_class: Option<String>,
    deduplicate_parts: bool,
}

impl ResultCell {
    pub fn new(
        list: &ListeriaList,
        entity_id: &str,
        sparql_rows: &[&HashMap<String, SparqlValue>],
        col: &Column,
    ) -> Self {
        let mut ret = Self {
            parts: vec![],
            wdedit_class: None,
            deduplicate_parts: true,
        };

        let entity = list.get_entity(entity_id);
        match &col.obj {
            ColumnType::Qid => {
                ret.parts.push(PartWithReference::new(
                    ResultCellPart::Text(entity_id.to_string()),
                    None,
                ));
            }
            ColumnType::Item => {
                ret.parts.push(PartWithReference::new(
                    ResultCellPart::Entity((entity_id.to_owned(), false)),
                    None,
                ));
            }
            ColumnType::Description => {
                if let Some(e) = entity {
                    match e.description_in_locale(list.language()) {
                        Some(s) => {
                            ret.wdedit_class = match &list.header_template() {
                                Some(_) => None,
                                None => Some("wd_desc".to_string()),
                            };
                            let s = Self::fix_wikitext_for_output(s);
                            ret.parts
                                .push(PartWithReference::new(ResultCellPart::Text(s), None));
                        }
                        None => {
                            // TODO FIXME autodesc deactivated for performance reasons, actually fill later
                            ret.parts.push(PartWithReference::new(
                                ResultCellPart::AutoDesc(AutoDesc::new(&e)),
                                None,
                            ));
                            // if let Ok(s) = list.get_autodesc_description(&e).await {
                            //     ret.parts
                            //         .push(PartWithReference::new(ResultCellPart::Text(s), None));
                            // }
                        }
                    }
                }
            }
            ColumnType::Field(varname) => {
                let varname = varname.to_lowercase();
                let mut found_varname: Option<String> = None;
                for row in sparql_rows.iter() {
                    if found_varname.is_none() {
                        for x in row.keys() {
                            if x.to_lowercase() == varname {
                                found_varname = Some(x.to_string());
                            }
                        }
                    }
                    if let Some(ref the_varname) = found_varname {
                        if let Some(x) = row.get(the_varname) {
                            ret.parts.push(PartWithReference::new(
                                ResultCellPart::from_sparql_value(x),
                                None,
                            ));
                        }
                    }
                }
            }
            ColumnType::Property(property) => {
                if let Some(e) = entity {
                    ret.wdedit_class = match &list.header_template() {
                        Some(_) => None,
                        None => Some(format!("wd_{}", property.to_lowercase())),
                    };
                    list.get_filtered_claims(&e, property)
                        .iter()
                        .for_each(|statement| {
                            let references = match list.get_reference_parameter() {
                                ReferencesParameter::All => {
                                    Self::get_references_for_statement(&statement, list.language())
                                }
                                _ => None,
                            };
                            ret.parts.push(PartWithReference::new(
                                ResultCellPart::from_snak(statement.main_snak()),
                                references,
                            ));
                        });
                }
            }
            ColumnType::PropertyQualifier((p1, p2)) => {
                if let Some(e) = entity {
                    list.get_filtered_claims(&e, p1)
                        .iter()
                        .for_each(|statement| {
                            ret.get_parts_p_p(statement, p2).iter().for_each(|part| {
                                ret.parts
                                    .push(PartWithReference::new(part.to_owned(), None))
                            });
                        });
                }
            }
            ColumnType::PropertyQualifierValue((p1, q1, p2)) => {
                if let Some(e) = entity {
                    list.get_filtered_claims(&e, p1)
                        .iter()
                        .for_each(|statement| {
                            ret.get_parts_p_q_p(statement, q1, p2)
                                .iter()
                                .for_each(|part| {
                                    ret.parts
                                        .push(PartWithReference::new(part.to_owned(), None))
                                });
                        });
                }
            }
            ColumnType::LabelLang(language) => {
                if let Some(e) = entity {
                    match e.label_in_locale(language) {
                        Some(s) => {
                            ret.parts.push(PartWithReference::new(
                                ResultCellPart::Text(s.to_string()),
                                None,
                            ));
                        }
                        None => {
                            if let Some(s) = e.label_in_locale(list.language()) {
                                ret.parts.push(PartWithReference::new(
                                    ResultCellPart::Text(s.to_string()),
                                    None,
                                ));
                            }
                        }
                    }
                }
            }
            ColumnType::AliasLang(language) => {
                if let Some(e) = entity {
                    let mut aliases: Vec<String> = e
                        .aliases()
                        .iter()
                        .filter(|alias| alias.language() == language)
                        .map(|alias| alias.value().to_string())
                        .collect();
                    aliases.sort();
                    aliases.iter().for_each(|alias| {
                        ret.parts.push(PartWithReference::new(
                            ResultCellPart::Text(alias.to_owned()),
                            None,
                        ));
                    });
                }
            }
            ColumnType::Label => {
                if let Some(e) = entity {
                    ret.wdedit_class = match &list.header_template() {
                        Some(_) => None,
                        None => Some("wd_label".to_string()),
                    };
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
                            ret.parts.push(PartWithReference::new(
                                ResultCellPart::LocalLink((page, label, false)),
                                None,
                            ));
                        }
                        None => {
                            ret.parts.push(PartWithReference::new(
                                ResultCellPart::Entity((entity_id.to_string(), true)),
                                None,
                            ));
                        }
                    }
                }
            }
            ColumnType::Unknown => {} // Ignore
            ColumnType::Number => {
                ret.parts
                    .push(PartWithReference::new(ResultCellPart::Number, None));
            }
        }

        ret
    }

    fn fix_wikitext_for_output(s: &str) -> String {
        s.replace('\'', "&#39;").replace('<', "&lt;")
    }

    fn get_parts_p_p(
        &self,
        statement: &wikibase::statement::Statement,
        property: &str,
    ) -> Vec<ResultCellPart> {
        statement
            .qualifiers()
            .iter()
            .filter(|snak| *snak.property() == *property)
            .map(|snak| {
                ResultCellPart::SnakList(vec![
                    PartWithReference::new(ResultCellPart::from_snak(statement.main_snak()), None),
                    PartWithReference::new(ResultCellPart::from_snak(snak), None),
                ])
            })
            .collect()
    }

    fn get_references_for_statement(
        statement: &wikibase::statement::Statement,
        language: &str,
    ) -> Option<Vec<Reference>> {
        let references = statement.references();
        let mut ret: Vec<Reference> = vec![];
        for reference in references.iter() {
            if let Some(r) = Reference::new_from_snaks(reference.snaks(), language) {
                ret.push(r);
            }
        }
        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }

    fn get_parts_p_q_p(
        &self,
        statement: &wikibase::statement::Statement,
        target_item: &str,
        property: &str,
    ) -> Vec<ResultCellPart> {
        let links_to_target = match statement.main_snak().data_value() {
            Some(dv) => match dv.value() {
                wikibase::value::Value::Entity(e) => e.id() == target_item,
                _ => false,
            },
            None => false,
        };
        if !links_to_target {
            return vec![];
        }
        //self.get_parts_p_p(statement,property)
        statement
            .qualifiers()
            .iter()
            .filter(|snak| *snak.property() == *property)
            .map(|snak| {
                ResultCellPart::SnakList(vec![
                    /*
                    // prefix with the Q in p_q_p
                    PartWithReference::new(
                        ResultCellPart::from_snak(statement.main_snak()),
                        None
                    ),
                    */
                    PartWithReference::new(ResultCellPart::from_snak(snak), None),
                ])
            })
            .collect()
    }

    pub fn get_sortkey(&self) -> String {
        match self.parts.get(0) {
            Some(part_with_reference) => match &part_with_reference.part {
                ResultCellPart::Entity((id, _)) => id.to_owned(),
                ResultCellPart::LocalLink((page, _label, _)) => page.to_owned(),
                ResultCellPart::Time(time) => time.to_owned(),
                ResultCellPart::File(s) => s.to_owned(),
                ResultCellPart::Uri(s) => s.to_owned(),
                ResultCellPart::Text(s) => s.to_owned(),
                ResultCellPart::ExternalId((_prop, id)) => id.to_owned(),
                _ => String::new(),
            },
            None => String::new(),
        }
    }

    pub fn parts(&self) -> &Vec<PartWithReference> {
        &self.parts
    }

    pub fn parts_mut(&mut self) -> &mut Vec<PartWithReference> {
        &mut self.parts
    }

    pub fn set_parts(&mut self, parts: Vec<PartWithReference>) {
        self.parts = parts;
    }

    pub fn localize_item_links_in_parts(
        parts: &mut Vec<PartWithReference>,
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
    ) {
        for part_with_reference in parts.iter_mut() {
            part_with_reference
                .part
                .localize_item_links(ecw, wiki, language);
        }
    }

    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> Value {
        let mut ret = vec![];
        for (partnum, part_with_reference) in self.parts.iter().enumerate() {
            ret.push(
                part_with_reference
                    .part
                    .as_tabbed_data(list, rownum, colnum, partnum),
            );
        }
        json!(ret.join("<br/>"))
    }

    pub fn as_wikitext(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        let mut ret;
        if list.template_params().wdedit() && list.header_template().is_none() {
            ret = match &self.wdedit_class {
                Some(class) => format!("class='{}'| ", class.to_owned()),
                None => " ".to_string(),
            };
        } else {
            ret = " ".to_string();
        }

        let mut parts = vec![];
        for (partnum, part_with_reference) in self.parts.iter().enumerate() {
            parts.push(
                part_with_reference
                    .part
                    .as_wikitext(list, rownum, colnum, partnum),
            );
        }
        if self.deduplicate_parts {
            let mut parts2 = Vec::new();
            for part in &parts {
                if !parts2.contains(part) {
                    parts2.push(part.to_owned())
                }
            }
            parts = parts2;
        }
        ret += &parts.join("<br/>");
        ret
    }
}
