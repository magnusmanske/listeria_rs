//! Section assignment for result rows.

use crate::listeria_list::ListeriaList;
use crate::template_params::SectionType;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use wikimisc::wikibase::SnakDataType;

impl super::ListProcessor {
    pub async fn process_assign_sections(list: &mut ListeriaList) -> Result<()> {
        list.profile("BEFORE list::process_assign_sections").await;

        let section_names = match list.template_params().section().clone() {
            SectionType::Property(p) => {
                list.load_row_entities().await?;
                let datatype = list.ecw().get_datatype_for_property(&p).await;
                list.profile("AFTER list::process_assign_sections 1").await;
                Self::get_section_names_for_rows(list, &p, &datatype).await?
            }
            SectionType::SparqlVariable(v) => {
                list.profile("AFTER list::process_assign_sections 1").await;
                Self::get_section_names_for_rows_sparql(list, &v)
            }
            SectionType::None => return Ok(()),
        };

        let section_count = Self::build_section_count(&section_names);
        list.profile("AFTER list::process_assign_sections 4").await;

        let valid_section_names =
            Self::build_valid_section_names(section_count, list.template_params().min_section());
        list.profile("AFTER list::process_assign_sections 6").await;

        let (name2id, id2name, misc_id) = Self::create_section_mappings(valid_section_names);
        list.profile("AFTER list::process_assign_sections 7").await;

        *list.section_id_to_name_mut() = id2name;
        list.profile("AFTER list::process_assign_sections 8").await;

        Self::assign_row_section_ids(list, section_names, name2id, misc_id)?;
        list.profile("AFTER list::process_assign_sections 9").await;

        Ok(())
    }

    async fn get_section_names_for_rows(
        list: &mut ListeriaList,
        section_property: &str,
        datatype: &SnakDataType,
    ) -> Result<Vec<String>> {
        let mut section_names_q: Vec<String> = Vec::with_capacity(list.results().len());
        for row in list.results().iter() {
            section_names_q.push(row.get_sortkey_prop(section_property, list, datatype).await);
        }
        list.profile("AFTER list::process_assign_sections 2").await;

        let mut unique_q = section_names_q.clone();
        unique_q.sort();
        unique_q.dedup();

        list.ecw().load_entities(list.wb_api(), &unique_q).await?;
        list.profile("AFTER list::process_assign_sections (load_entities) 3a")
            .await;

        let mut section_names = Vec::with_capacity(section_names_q.len());
        for q in section_names_q {
            let label = list.get_label_with_fallback(&q).await;
            section_names.push(label);
        }
        Ok(section_names)
    }

    fn get_section_names_for_rows_sparql(list: &ListeriaList, variable: &str) -> Vec<String> {
        list.results()
            .iter()
            .map(|row| row.get_sortkey_sparql(variable, list))
            .collect()
    }

    pub(crate) fn build_section_count(section_names: &[String]) -> HashMap<&String, u64> {
        let mut section_count = HashMap::new();
        for name in section_names {
            *section_count.entry(name).or_insert(0) += 1;
        }
        section_count
    }

    pub(crate) fn build_valid_section_names(
        section_count: HashMap<&String, u64>,
        min_section: u64,
    ) -> Vec<String> {
        let mut valid_section_names: Vec<String> = section_count
            .into_iter()
            .filter(|(name, count)| *count >= min_section && !name.trim().is_empty())
            .map(|(name, _count)| name.to_owned())
            .collect();
        valid_section_names.sort();
        valid_section_names
    }

    pub(crate) fn create_section_mappings(
        valid_section_names: Vec<String>,
    ) -> (HashMap<String, usize>, HashMap<usize, String>, usize) {
        let misc_id = valid_section_names.len();
        let mut names_with_misc = valid_section_names;
        names_with_misc.push("Misc".to_string());

        let mut name2id = HashMap::with_capacity(names_with_misc.len());
        let mut id2name = HashMap::with_capacity(names_with_misc.len());
        for (num, name) in names_with_misc.into_iter().enumerate() {
            name2id.insert(name.clone(), num);
            id2name.insert(num, name);
        }

        (name2id, id2name, misc_id)
    }

    pub(crate) fn assign_row_section_ids(
        list: &mut ListeriaList,
        section_names: Vec<String>,
        name2id: HashMap<String, usize>,
        misc_id: usize,
    ) -> Result<()> {
        if section_names.len() != list.results().len() {
            return Err(anyhow!(
                "assign_row_section_ids: section_names length ({}) != results length ({})",
                section_names.len(),
                list.results().len()
            ));
        }
        for (row_id, row) in list.results_mut().iter_mut().enumerate() {
            let section_id = match section_names.get(row_id) {
                Some(name) => name2id.get(name).copied().unwrap_or(misc_id),
                None => misc_id,
            };
            row.set_section(section_id);
        }
        Ok(())
    }
}

