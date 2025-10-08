use crate::column::*;
use crate::entity_container_wrapper::EntityContainerWrapper;
use crate::listeria_list::ListeriaList;
use crate::reference::Reference;
use crate::result_cell_part::AutoDesc;
use crate::result_cell_part::LinkTarget;
use crate::result_cell_part::PartWithReference;
use crate::result_cell_part::ResultCellPart;
use crate::template_params::ReferencesParameter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wikimisc::sparql_table::SparqlTable;
use wikimisc::wikibase::entity::EntityTrait;
use wikimisc::wikibase::Statement;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultCell {
    parts: Vec<PartWithReference>,
    wdedit_class: Option<String>,
    deduplicate_parts: bool,
}

impl ResultCell {
    pub async fn new(
        list: &ListeriaList,
        entity_id: &str,
        sparql_table: &SparqlTable,
        col: &Column,
    ) -> Self {
        let mut ret = Self {
            parts: vec![],
            wdedit_class: None,
            deduplicate_parts: true,
        };

        let entity = list.get_entity(entity_id).await;
        match col.obj() {
            ColumnType::Qid => Self::ct_qid(&mut ret, entity_id),
            ColumnType::Item => Self::ct_item(&mut ret, entity_id),
            ColumnType::Description => Self::ct_description(&entity, list, &mut ret),
            ColumnType::Field(varname) => Self::ct_field(varname, sparql_table, &mut ret),
            ColumnType::Property(property) => Self::ct_property(&entity, &mut ret, list, property),
            ColumnType::PropertyQualifier((p1, p2)) => Self::ct_pq(&entity, list, p1, &mut ret, p2),
            ColumnType::PropertyQualifierValue((p1, q1, p2)) => {
                Self::ct_pqv(&entity, list, p1, &mut ret, q1, p2)
            }
            ColumnType::LabelLang(language) => {
                Self::ct_label_lang(&entity, language, &mut ret, list)
            }
            ColumnType::AliasLang(language) => Self::ct_alias_lang(&entity, language, &mut ret),
            ColumnType::Label => Self::ct_label(entity, &mut ret, list, entity_id),
            ColumnType::Number => Self::ct_number(&mut ret),
            ColumnType::Unknown => {} // Ignore
        }

        ret
    }

    fn fix_wikitext_for_output(s: &str) -> String {
        s.replace('\'', "&#39;").replace('<', "&lt;")
    }

    fn get_parts_p_p(&self, statement: &Statement, property: &str) -> Vec<ResultCellPart> {
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
        statement: &Statement,
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
        statement: &Statement,
        target_item: &str,
        property: &str,
    ) -> Vec<ResultCellPart> {
        let links_to_target = match statement.main_snak().data_value() {
            Some(dv) => match dv.value() {
                wikimisc::wikibase::value::Value::Entity(e) => e.id() == target_item,
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
                ResultCellPart::SnakList(vec![PartWithReference::new(
                    ResultCellPart::from_snak(snak),
                    None,
                )])
            })
            .collect()
    }

    pub fn get_sortkey(&self) -> String {
        match self.parts.first() {
            Some(part_with_reference) => match part_with_reference.part() {
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

    pub async fn localize_item_links_in_parts(
        parts: &mut [PartWithReference],
        ecw: &EntityContainerWrapper,
        wiki: &str,
        language: &str,
    ) {
        for part_with_reference in parts.iter_mut() {
            part_with_reference
                .part_mut()
                .localize_item_links(ecw, wiki, language)
                .await;
        }
    }

    pub async fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> Value {
        let mut ret = vec![];
        for part_with_reference in self.parts.iter() {
            ret.push(
                part_with_reference
                    .part()
                    .as_tabbed_data(list, rownum, colnum)
                    .await,
            );
        }
        json!(ret.join("<br/>"))
    }

    pub async fn as_wikitext(
        &mut self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
    ) -> String {
        let mut ret;
        if list.template_params().wdedit() && list.header_template().is_none() {
            ret = match &self.wdedit_class {
                Some(class) => format!("class='{}'| ", class.to_owned()),
                None => " ".to_string(),
            };
        } else {
            ret = " ".to_string();
        }

        let mut parts: Vec<String> = vec![];
        for part_with_reference in &mut self.parts {
            parts.push(part_with_reference.as_wikitext(list, rownum, colnum).await);
        }
        if self.deduplicate_parts {
            // Deduplicate but keep order?
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

    fn ct_number(ret: &mut ResultCell) {
        ret.parts
            .push(PartWithReference::new(ResultCellPart::Number, None));
    }

    fn ct_label(
        entity: Option<wikimisc::wikibase::Entity>,
        ret: &mut ResultCell,
        list: &ListeriaList,
        entity_id: &str,
    ) {
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
                        ResultCellPart::LocalLink((page, label, LinkTarget::Page)),
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

    fn ct_alias_lang(
        entity: &Option<wikimisc::wikibase::Entity>,
        language: &String,
        ret: &mut ResultCell,
    ) {
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

    fn ct_label_lang(
        entity: &Option<wikimisc::wikibase::Entity>,
        language: &str,
        ret: &mut ResultCell,
        list: &ListeriaList,
    ) {
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

    fn ct_pqv(
        entity: &Option<wikimisc::wikibase::Entity>,
        list: &ListeriaList,
        p1: &str,
        ret: &mut ResultCell,
        q1: &str,
        p2: &str,
    ) {
        if let Some(e) = entity {
            list.get_filtered_claims(e, p1)
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

    fn ct_pq(
        entity: &Option<wikimisc::wikibase::Entity>,
        list: &ListeriaList,
        p1: &str,
        ret: &mut ResultCell,
        p2: &str,
    ) {
        if let Some(e) = entity {
            list.get_filtered_claims(e, p1)
                .iter()
                .for_each(|statement| {
                    ret.get_parts_p_p(statement, p2).iter().for_each(|part| {
                        ret.parts
                            .push(PartWithReference::new(part.to_owned(), None))
                    });
                });
        }
    }

    fn ct_property(
        entity: &Option<wikimisc::wikibase::Entity>,
        ret: &mut ResultCell,
        list: &ListeriaList,
        property: &str,
    ) {
        if let Some(e) = entity {
            ret.wdedit_class = match &list.header_template() {
                Some(_) => None,
                None => Some(format!("wd_{}", property.to_lowercase())),
            };
            list.get_filtered_claims(e, property)
                .iter()
                .for_each(|statement| {
                    let references = match list.get_reference_parameter() {
                        ReferencesParameter::All => {
                            Self::get_references_for_statement(statement, list.language())
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

    fn ct_field(varname: &str, sparql_table: &SparqlTable, ret: &mut ResultCell) {
        let var_index = match sparql_table.get_var_index(varname) {
            Some(i) => i,
            None => return, // Nothing to do
        };
        for row_id in 0..sparql_table.len() {
            if let Some(x) = sparql_table.get_row_col(row_id, var_index) {
                ret.parts.push(PartWithReference::new(
                    ResultCellPart::from_sparql_value(&x),
                    None,
                ));
            }
        }
    }

    fn ct_description(
        entity: &Option<wikimisc::wikibase::Entity>,
        list: &ListeriaList,
        ret: &mut ResultCell,
    ) {
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
                    ret.parts.push(PartWithReference::new(
                        ResultCellPart::AutoDesc(AutoDesc::new(e)),
                        None,
                    ));
                }
            }
        }
    }

    fn ct_item(ret: &mut ResultCell, entity_id: &str) {
        ret.parts.push(PartWithReference::new(
            ResultCellPart::Entity((entity_id.to_owned(), false)),
            None,
        ));
    }

    fn ct_qid(ret: &mut ResultCell, entity_id: &str) {
        ret.parts.push(PartWithReference::new(
            ResultCellPart::Text(entity_id.to_string()),
            None,
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fix_wikitext_for_output() {
        assert_eq!(ResultCell::fix_wikitext_for_output("a'b<c"), "a&#39;b&lt;c");
    }
}
