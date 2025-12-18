//! Result processing and entity resolution.
//!
//! Transforms SPARQL results into structured data with resolved entities and references.

use crate::listeria_list::ListeriaList;
use crate::result_cell::ResultCell;
use crate::result_cell_part::{LinkTarget, ResultCellPart};
use crate::sparql_results::SparqlResults;
use crate::template_params::LinksType;
use crate::template_params::SectionType;
use crate::template_params::SortMode;
use crate::template_params::SortOrder;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use futures::future::join_all;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use wikimisc::sparql_value::SparqlValue;
use wikimisc::wikibase::{EntityTrait, SnakDataType};

const MAX_CONCURRENT_REDLINKS_REQUESTS: usize = 5;

/// Handles the processing of result data for ListeriaList
#[derive(Debug)]
pub struct ListProcessor;

impl ListProcessor {
    pub async fn process_items_to_local_links(list: &mut ListeriaList) -> Result<()> {
        let wiki = list.wiki().to_owned();
        let language = list.language().to_owned();
        let ecw = list.ecw().clone();

        for row_id in 0..list.results().len() {
            let row = match list.results_mut().get_mut(row_id) {
                Some(row) => row,
                None => continue,
            };
            for cell in row.cells_mut().iter_mut() {
                ResultCell::localize_item_links_in_parts(cell.parts_mut(), &ecw, &wiki, &language)
                    .await;
            }
        }
        Ok(())
    }

    pub fn process_excess_files(list: &mut ListeriaList) -> Result<()> {
        for row in list.results_mut().iter_mut() {
            row.remove_excess_files();
        }
        Ok(())
    }

    fn check_this_wiki_for_shadow_images(list: &ListeriaList) -> bool {
        list.page_params()
            .config()
            .check_for_shadow_images(list.wiki())
    }

    async fn fetch_file_info(
        list: &mut ListeriaList,
        param_list: &[HashMap<String, String>],
        files_to_check: Vec<String>,
    ) -> Vec<(String, Value)> {
        let page_params = list.page_params().clone();
        let api_read = page_params.mw_api();

        let mut futures = Vec::with_capacity(param_list.len());
        for params in param_list {
            futures.push(api_read.get_query_api_json(params));
        }
        list.profile(&format!(
            "ListProcessor::process_remove_shadow_files running {} futures",
            futures.len()
        ));

        join_all(futures)
            .await
            .iter()
            .zip(files_to_check)
            .filter_map(|(result, filename)| match result {
                Ok(j) => Some((filename, j.to_owned())),
                _ => None,
            })
            .collect()
    }

    fn identify_shadow_files(api_results: Vec<(String, Value)>) -> HashSet<String> {
        api_results
            .into_iter()
            .filter_map(|(filename, j)| {
                let could_be_local = j["query"]["pages"].as_object().is_none_or(|results| {
                    results
                        .iter()
                        .filter_map(|(_k, o)| o["imagerepository"].as_str())
                        .any(|s| s != "shared")
                });

                could_be_local.then_some(filename)
            })
            .collect()
    }

    fn remove_shadow_files_from_rows(list: &mut ListeriaList, shadow_files: &HashSet<String>) {
        for row_id in 0..list.results().len() {
            let row = match list.results_mut().get_mut(row_id) {
                Some(row) => row,
                None => continue,
            };
            row.remove_shadow_files(shadow_files);
        }
    }

    pub async fn process_remove_shadow_files(list: &mut ListeriaList) -> Result<()> {
        if !Self::check_this_wiki_for_shadow_images(list) {
            return Ok(());
        }
        let files_to_check = Self::get_files_to_check(list);
        list.shadow_files_mut().clear();
        let param_list = Self::get_param_list_for_files(list, &files_to_check);

        let api_results = Self::fetch_file_info(list, &param_list, files_to_check).await;
        let shadow_files = Self::identify_shadow_files(api_results);
        Self::remove_shadow_files_from_rows(list, &shadow_files);

        *list.shadow_files_mut() = shadow_files;

        Ok(())
    }

    fn get_files_to_check(list: &ListeriaList) -> Vec<String> {
        let mut files_to_check = vec![];
        for row in list.results().iter() {
            for cell in row.cells() {
                for part in cell.parts() {
                    if let ResultCellPart::File(file) = part.part() {
                        files_to_check.push(file.to_owned());
                    }
                }
            }
        }
        files_to_check.sort_unstable();
        files_to_check.dedup();
        files_to_check
    }

    /// Get parameters for fileinfo API
    fn get_param_list_for_files(
        list: &ListeriaList,
        files_to_check: &[String],
    ) -> Vec<HashMap<String, String>> {
        files_to_check
            .iter()
            .map(|filename| {
                let prefixed_filename = format!(
                    "{}:{}",
                    list.page_params().local_file_namespace_prefix(),
                    &filename
                );
                let params: HashMap<String, String> = [
                    ("action", "query"),
                    ("titles", prefixed_filename.as_str()),
                    ("prop", "imageinfo"),
                ]
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect();
                params
            })
            .collect()
    }

    pub async fn process_redlinks_only(list: &mut ListeriaList) -> Result<()> {
        if *list.get_links_type() != LinksType::RedOnly {
            return Ok(());
        }
        let keep_flags = Self::find_keep_flags(list).await;
        Self::set_keep_flags(list, keep_flags);
        list.results_mut().retain(|r| r.keep());
        Ok(())
    }

    pub async fn process_redlinks(list: &mut ListeriaList) -> Result<()> {
        if *list.get_links_type() != LinksType::RedOnly && *list.get_links_type() != LinksType::Red
        {
            return Ok(());
        }

        let ids = Self::collect_entity_ids_from_results(list);
        let labels = Self::get_labels_for_entity_ids(list, ids).await;
        Self::cache_local_page_existence(list, labels).await;

        Ok(())
    }

    fn collect_entity_ids_from_results(list: &ListeriaList) -> Vec<String> {
        let mut ids = vec![];
        for row in list.results().iter() {
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Entity(entity_info) = part.part()
                        && entity_info.try_localize
                    {
                        ids.push(entity_info.id.to_owned());
                    }
                });
            });
        }

        ids.sort();
        ids.dedup();
        ids
    }

    async fn get_labels_for_entity_ids(list: &mut ListeriaList, ids: Vec<String>) -> Vec<String> {
        let mut labels = vec![];
        for id in ids {
            if let Some(e) = list.get_entity(&id).await
                && let Some(l) = e.label_in_locale(list.language())
            {
                labels.push(l.to_string());
            }
        }

        labels.sort();
        labels.dedup();
        labels
    }

    async fn cache_local_page_existence(list: &mut ListeriaList, labels: Vec<String>) {
        let labels_per_chunk = if list.mw_api().user().is_bot() {
            500
        } else {
            50
        };

        let num_chunks = labels.len().div_ceil(labels_per_chunk);
        let mut futures = Vec::with_capacity(num_chunks);
        for chunk in labels.chunks(labels_per_chunk) {
            let future = list.cache_local_pages_exist(chunk);
            futures.push(future);
        }
        let stream =
            futures::stream::iter(futures).buffer_unordered(MAX_CONCURRENT_REDLINKS_REQUESTS);
        let results = stream.collect::<Vec<_>>().await;
        for (title, page_exists) in results.into_iter().flatten() {
            list.local_page_cache_mut().insert(title, page_exists);
        }
    }

    pub async fn process_sort_results(list: &mut ListeriaList) -> Result<()> {
        let sortkeys: Vec<String>;
        // Default
        let mut datatype = SnakDataType::String;
        list.profile("BEFORE process_sort_results SORTKEYS");
        match list.template_params().sort() {
            SortMode::Label => {
                list.load_row_entities().await?;
                let mut futures = Vec::with_capacity(list.results().len());
                for row in list.results().iter() {
                    futures.push(row.get_sortkey_label(list));
                }
                sortkeys = join_all(futures).await.to_vec();
            }
            SortMode::FamilyName => {
                let mut futures = Vec::with_capacity(list.results().len());
                for row in list.results().iter() {
                    futures.push(row.get_sortkey_family_name(list));
                }
                sortkeys = join_all(futures).await.to_vec();
            }
            SortMode::Property(prop) => {
                datatype = list.ecw().get_datatype_for_property(prop).await;
                let mut futures = Vec::with_capacity(list.results().len());
                for row in list.results().iter() {
                    futures.push(row.get_sortkey_prop(prop, list, &datatype));
                }
                sortkeys = join_all(futures).await.to_vec();
            }
            SortMode::SparqlVariable(variable) => {
                sortkeys = list
                    .results()
                    .iter()
                    .map(|row| row.get_sortkey_sparql(variable, list))
                    .collect();
            }
            SortMode::None => return Ok(()),
        }
        list.profile("AFTER process_sort_results SORTKEYS");

        let ret = Self::process_sort_results_finish(list, sortkeys, datatype);
        list.profile("AFTER process_sort_results_finish");
        ret
    }

    fn process_sort_results_finish(
        list: &mut ListeriaList,
        sortkeys: Vec<String>,
        datatype: SnakDataType,
    ) -> Result<()> {
        // Apply sortkeys
        if list.results().len() != sortkeys.len() {
            // Paranoia
            return Err(anyhow!("process_sort_results: sortkeys length mismatch"));
        }

        for row_id in 0..list.results().len() {
            if let Some(row) = list.results_mut().get_mut(row_id)
                && let Some(sk) = sortkeys.get(row_id)
            {
                row.set_sortkey(sk.to_owned());
            };
        }

        list.profile(&format!(
            "BEFORE process_sort_results_finish sort of {} items",
            list.results().len()
        ));
        list.results_mut()
            .sort_by(|a, b| a.compare_to(b, &datatype));
        list.profile("AFTER process_sort_results_finish sort");
        if *list.template_params().sort_order() == SortOrder::Descending {
            list.results_mut().reverse();
        }
        list.profile("AFTER process_sort_results_finish reverse");

        Ok(())
    }

    async fn get_section_names_for_rows(
        list: &mut ListeriaList,
        section_property: &str,
        datatype: &SnakDataType,
    ) -> Result<Vec<String>> {
        let mut section_names_q = vec![];
        for row in list.results().iter() {
            section_names_q.push(row.get_sortkey_prop(section_property, list, datatype).await);
        }
        list.profile("AFTER list::process_assign_sections 2");

        // Make sure section name items are loaded
        list.ecw()
            .load_entities(list.wb_api(), &section_names_q)
            .await?;
        list.profile("AFTER list::process_assign_sections 3a");

        let mut section_names = vec![];
        for q in section_names_q {
            let label = list.get_label_with_fallback(&q).await;
            section_names.push(label);
        }
        Ok(section_names)
    }

    fn build_section_count(section_names: &[String]) -> HashMap<&String, u64> {
        let mut section_count = HashMap::new();
        for name in section_names {
            *section_count.entry(name).or_insert(0) += 1;
        }
        section_count
    }

    fn build_valid_section_names(
        section_count: HashMap<&String, u64>,
        min_section: u64,
    ) -> Vec<String> {
        let mut valid_section_names: Vec<String> = section_count
            .into_iter()
            .filter(|(_name, count)| *count >= min_section)
            .map(|(name, _count)| name.to_owned())
            .collect();
        valid_section_names.sort();
        valid_section_names
    }

    fn create_section_mappings(
        valid_section_names: Vec<String>,
    ) -> (HashMap<String, usize>, HashMap<usize, String>, usize) {
        let misc_id = valid_section_names.len();
        let mut names_with_misc = valid_section_names;
        names_with_misc.push("Misc".to_string());

        let name2id: HashMap<String, usize> = names_with_misc
            .iter()
            .enumerate()
            .map(|(num, name)| (name.to_string(), num))
            .collect();

        let id2name: HashMap<usize, String> = name2id
            .iter()
            .map(|(name, id)| (*id, name.to_owned()))
            .collect();

        (name2id, id2name, misc_id)
    }

    pub async fn process_assign_sections(list: &mut ListeriaList) -> Result<()> {
        list.profile("BEFORE list::process_assign_sections");

        // TODO all SectionType options
        let section_property = match list.template_params().section() {
            SectionType::Property(p) => p,
            SectionType::SparqlVariable(_v) => {
                return Err(anyhow!("SPARQL variable section type not supported yet"));
            }
            SectionType::None => return Ok(()), // Nothing to do
        }
        .to_owned();
        list.load_row_entities().await?;
        let datatype = list
            .ecw()
            .get_datatype_for_property(&section_property)
            .await;
        list.profile("AFTER list::process_assign_sections 1");

        let section_names =
            Self::get_section_names_for_rows(list, &section_property, &datatype).await?;

        let section_count = Self::build_section_count(&section_names);
        list.profile("AFTER list::process_assign_sections 4");

        let valid_section_names =
            Self::build_valid_section_names(section_count, list.template_params().min_section());
        list.profile("AFTER list::process_assign_sections 6");

        let (name2id, id2name, misc_id) = Self::create_section_mappings(valid_section_names);
        list.profile("AFTER list::process_assign_sections 7");

        *list.section_id_to_name_mut() = id2name;
        list.profile("AFTER list::process_assign_sections 8");

        Self::assign_row_section_ids(list, section_names, name2id, misc_id)?;
        list.profile("AFTER list::process_assign_sections 9");

        Ok(())
    }

    fn assign_row_section_ids(
        list: &mut ListeriaList,
        section_names: Vec<String>,
        name2id: HashMap<String, usize>,
        misc_id: usize,
    ) -> Result<()> {
        for (row_id, row) in list.results_mut().iter_mut().enumerate() {
            let section_name = match section_names.get(row_id) {
                Some(name) => name,
                None => continue,
            };
            let section_id = match name2id.get(section_name) {
                Some(id) => *id,
                None => misc_id,
            };
            row.set_section(section_id);
        }
        Ok(())
    }

    async fn get_region_for_entity_id(list: &ListeriaList, entity_id: &str) -> Option<String> {
        let wikibase_key = list.template_params().wikibase().to_lowercase();
        let sparql = format!("SELECT ?q ?x {{ wd:{entity_id} wdt:P131* ?q . ?q wdt:P300 ?x }}");
        let mut sparql_results = SparqlResults::new(list.page_params().clone(), &wikibase_key);
        sparql_results.set_simulate(false);
        let mut region = String::new();
        let sparql_table = sparql_results.run_query(sparql).await.ok()?;
        let x_idx = sparql_table.get_var_index("x")?;
        for row_id in 0..sparql_table.len() {
            match sparql_table.get_row_col(row_id, x_idx) {
                Some(SparqlValue::Literal(r)) => {
                    if r.len() > region.len() {
                        region = r.to_string();
                    }
                }
                _ => continue,
            }
        }
        if region.is_empty() {
            None
        } else {
            Some(region)
        }
    }

    fn do_get_regions(list: &ListeriaList) -> bool {
        list.page_params()
            .config()
            .location_regions()
            .contains(&list.wiki().to_string())
    }

    pub async fn process_regions(list: &mut ListeriaList) -> Result<()> {
        if !Self::do_get_regions(list) {
            return Ok(());
        }

        let entity_ids = Self::process_regions_get_entity_ids(list);
        let entity_id2region = Self::process_regions_get_eneity_id2region(list, entity_ids).await;

        for row in list.results_mut().iter_mut() {
            let the_region = match entity_id2region.get(row.entity_id()) {
                Some(r) => r,
                None => continue,
            };
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    if let ResultCellPart::Location(loc_info) = part.part_mut() {
                        loc_info.region = Some(the_region.clone());
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn process_reference_items(list: &mut ListeriaList) -> Result<()> {
        let items_to_load = Self::collect_stated_in_items_from_references(list);
        if !items_to_load.is_empty() {
            list.ecw()
                .load_entities(list.wb_api(), &items_to_load)
                .await?;
        }
        Ok(())
    }

    fn collect_stated_in_items_from_references(list: &mut ListeriaList) -> Vec<String> {
        let mut items_to_load: Vec<String> = vec![];
        for row in list.results_mut().iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                for part_with_reference in cell.parts_mut().iter_mut() {
                    Self::collect_stated_in_from_part(part_with_reference, &mut items_to_load);
                }
            }
        }
        items_to_load.sort_unstable();
        items_to_load.dedup();
        items_to_load
    }

    fn collect_stated_in_from_part(
        part: &crate::result_cell_part::PartWithReference,
        items_to_load: &mut Vec<String>,
    ) {
        if let Some(references) = part.references() {
            for reference in references.iter() {
                if let Some(stated_in) = reference.stated_in() {
                    items_to_load.push(stated_in.to_string());
                }
            }
        }
    }

    pub async fn fix_local_links(list: &mut ListeriaList) -> Result<()> {
        let mw_api = list.mw_api();
        for row in list.results_mut().iter_mut() {
            for cell in row.cells_mut().iter_mut() {
                for part in cell.parts_mut().iter_mut() {
                    Self::fix_local_link_in_part(part, &mw_api);
                }
            }
        }
        Ok(())
    }

    fn fix_local_link_in_part(
        part: &mut crate::result_cell_part::PartWithReference,
        mw_api: &wikimisc::mediawiki::api::Api,
    ) {
        match part.part_mut() {
            ResultCellPart::LocalLink(link_info) => {
                Self::set_link_target_from_page(&link_info.page, &mut link_info.target, mw_api);
            }
            ResultCellPart::SnakList(v) => {
                for subpart in v.iter_mut() {
                    if let ResultCellPart::LocalLink(link_info) = subpart.part_mut() {
                        Self::set_link_target_from_page(
                            &link_info.page,
                            &mut link_info.target,
                            mw_api,
                        );
                    }
                }
            }
            _ => {}
        }
    }

    fn set_link_target_from_page(
        page: &str,
        link_target: &mut LinkTarget,
        mw_api: &wikimisc::mediawiki::api::Api,
    ) {
        let title = wikimisc::mediawiki::title::Title::new_from_full(page, mw_api);
        *link_target = match title.namespace_id() {
            14 => LinkTarget::Category,
            _ => LinkTarget::Page,
        };
    }

    pub async fn fill_autodesc(list: &mut ListeriaList) -> Result<()> {
        // Done in two different steps, otherwise get_autodesc_description() would borrow self when &mut self is already borrowed
        // TODO Maybe gather futures and run get_autodesc_description() in async/parallel?
        let autodescs = Self::fill_autodesc_gather_descriptions(list).await?;
        Self::fill_autodesc_set_descriptions(list, autodescs)?;
        Ok(())
    }

    fn fill_autodesc_set_descriptions(
        list: &mut ListeriaList,
        autodescs: HashMap<String, String>,
    ) -> Result<()> {
        for row in list.results_mut().iter_mut() {
            for cell in row.cells_mut() {
                for part_with_reference in cell.parts_mut() {
                    if let ResultCellPart::AutoDesc(ad) = part_with_reference.part_mut()
                        && let Some(desc) = autodescs.get(ad.entity_id())
                    {
                        ad.set_description(desc);
                    }
                }
            }
        }
        Ok(())
    }

    async fn fill_autodesc_gather_descriptions(
        list: &mut ListeriaList,
    ) -> Result<HashMap<String, String>> {
        let entity_ids = Self::collect_autodesc_entity_ids(list);
        Self::load_and_get_descriptions(list, entity_ids).await
    }

    fn collect_autodesc_entity_ids(list: &ListeriaList) -> Vec<String> {
        let mut entity_ids = Vec::new();
        for row in list.results().iter() {
            for cell in row.cells() {
                for part_with_reference in cell.parts() {
                    if let ResultCellPart::AutoDesc(ad) = part_with_reference.part() {
                        entity_ids.push(ad.entity_id().to_owned());
                    }
                }
            }
        }
        entity_ids
    }

    async fn load_and_get_descriptions(
        list: &mut ListeriaList,
        entity_ids: Vec<String>,
    ) -> Result<HashMap<String, String>> {
        let mut autodescs = HashMap::new();
        for entity_id in entity_ids {
            list.ecw()
                .load_entities(list.wb_api(), std::slice::from_ref(&entity_id))
                .await?;
            if let Some(entity) = list.ecw().get_entity(&entity_id).await
                && let Ok(desc) = list.get_autodesc_description(&entity).await
            {
                autodescs.insert(entity_id, desc);
            }
        }
        Ok(autodescs)
    }

    async fn find_keep_flags(list: &mut ListeriaList) -> Vec<bool> {
        let wiki = list.wiki().to_string();
        let ecw = list.ecw().clone();

        // First pass: collect entity IDs and check sitelinks
        let mut keep_flags = Vec::new();
        for row in list.results().iter() {
            let keep = ecw.get_entity(row.entity_id()).await.is_some_and(|entity| {
                entity
                    .sitelinks()
                    .as_ref()
                    .is_none_or(|sl| !sl.iter().any(|s| *s.site() == wiki))
            });
            keep_flags.push(keep);
        }
        keep_flags
    }

    fn process_regions_get_entity_ids(list: &mut ListeriaList) -> HashSet<String> {
        let mut entity_ids = HashSet::new();
        for row in list.results().iter() {
            row.cells().iter().for_each(|cell| {
                cell.parts().iter().for_each(|part| {
                    if let ResultCellPart::Location(_loc_info) = part.part() {
                        entity_ids.insert(row.entity_id().to_string());
                    }
                });
            });
        }
        entity_ids
    }

    async fn process_regions_get_eneity_id2region(
        list: &mut ListeriaList,
        entity_ids: HashSet<String>,
    ) -> HashMap<String, String> {
        let mut entity_id2region = HashMap::new();
        for entity_id in entity_ids {
            if let Some(region) = Self::get_region_for_entity_id(list, &entity_id).await {
                entity_id2region.insert(entity_id, region);
            }
        }
        entity_id2region
    }

    fn set_keep_flags(list: &mut ListeriaList, keep_flags: Vec<bool>) {
        for (row, keep) in list.results_mut().iter_mut().zip(keep_flags) {
            row.set_keep(keep);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::Configuration;
    use crate::page_params::PageParams;
    use crate::template::Template;
    use std::sync::Arc;
    use wikimisc::mediawiki::api::Api;

    async fn create_test_list() -> ListeriaList {
        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        let api = Arc::new(api);
        let config = Configuration::new_from_file("config.json").await.unwrap();
        let config = Arc::new(config);
        let page_params = PageParams::new(config, api, "Test:Page".to_string())
            .await
            .unwrap();
        let page_params = Arc::new(page_params);

        let template_text =
            "{{Wikidata list|columns=item|sparql=SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }}}";
        let template = Template::new_from_params(template_text).unwrap();

        ListeriaList::new(template, page_params).await.unwrap()
    }

    #[test]
    fn test_list_processor_is_debug() {
        // Verify that ListProcessor implements Debug
        let _ = format!("{:?}", ListProcessor);
    }

    #[tokio::test]
    async fn test_process_excess_files_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_excess_files(&mut list);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_items_to_local_links_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_items_to_local_links(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_redlinks_only_not_redonly() {
        let mut list = create_test_list().await;
        // Default links type should not be RedOnly
        let result = ListProcessor::process_redlinks_only(&mut list).await;
        assert!(result.is_ok());
        // Results should be unchanged since links type is not RedOnly
    }

    #[tokio::test]
    async fn test_process_sort_results_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_sort_results(&mut list).await;
        // Should succeed with empty results
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_reference_items_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_reference_items(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fix_local_links_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::fix_local_links(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fill_autodesc_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::fill_autodesc(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_assign_sections_none() {
        let mut list = create_test_list().await;
        // Default section type should be None
        let result = ListProcessor::process_assign_sections(&mut list).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_regions_with_empty_results() {
        let mut list = create_test_list().await;
        let result = ListProcessor::process_regions(&mut list).await;
        assert!(result.is_ok());
    }
}
