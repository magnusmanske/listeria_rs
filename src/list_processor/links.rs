//! Redlink, local-link, and link-fixing processing.

use crate::listeria_list::ListeriaList;
use crate::result_cell::ResultCell;
use crate::result_cell_part::{LinkTarget, ResultCellPart};
use crate::template_params::LinksType;
use anyhow::Result;
use futures::StreamExt;
use futures::future::join_all;
use std::sync::Arc;
use wikimisc::wikibase::EntityTrait;

const MAX_CONCURRENT_REDLINKS_REQUESTS: usize = 5;

impl super::ListProcessor {
    pub async fn process_items_to_local_links(list: &mut ListeriaList) -> Result<()> {
        let wiki = list.wiki().to_owned();
        let language = list.language().to_owned();
        let ecw = list.ecw().clone();

        let futures: Vec<_> = list
            .results_mut()
            .iter_mut()
            .map(|row| Self::process_items_to_local_links_row(&wiki, &language, &ecw, row))
            .collect();
        join_all(futures).await;
        Ok(())
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

    pub fn fix_local_links(list: &mut ListeriaList) -> Result<()> {
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

    async fn process_items_to_local_links_row(
        wiki: &str,
        language: &str,
        ecw: &crate::entity_container_wrapper::EntityContainerWrapper,
        row: &mut crate::result_row::ResultRow,
    ) {
        let futures: Vec<_> = row
            .cells_mut()
            .iter_mut()
            .map(|cell| {
                ResultCell::localize_item_links_in_parts(cell.parts_mut(), ecw, wiki, language)
            })
            .collect();
        futures::future::join_all(futures).await;
    }

    fn collect_entity_ids_from_results(list: &ListeriaList) -> Vec<String> {
        let mut ids = Vec::new();
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
        let ecw = list.ecw().clone();
        let language: Arc<str> = list.language().into();
        let futures: Vec<_> = ids
            .into_iter()
            .map(|id| {
                let ecw = ecw.clone();
                let language = Arc::clone(&language);
                async move {
                    ecw.get_entity(&id)
                        .await
                        .and_then(|e| e.label_in_locale(&language).map(|l| l.to_string()))
                }
            })
            .collect();
        let mut labels: Vec<String> = join_all(futures).await.into_iter().flatten().collect();
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

    async fn find_keep_flags(list: &mut ListeriaList) -> Vec<bool> {
        let wiki: Arc<str> = list.wiki().into();
        let ecw = list.ecw().clone();

        let futures: Vec<_> = list
            .results()
            .iter()
            .map(|row| {
                let ecw = ecw.clone();
                let wiki = Arc::clone(&wiki);
                let entity_id = row.entity_id().to_string();
                async move {
                    ecw.get_entity(&entity_id).await.is_some_and(|entity| {
                        entity
                            .sitelinks()
                            .as_ref()
                            .map_or_else(|| true, |sl| !sl.iter().any(|s| *s.site() == *wiki))
                    })
                }
            })
            .collect();
        join_all(futures).await
    }

    pub(crate) fn set_keep_flags(list: &mut ListeriaList, keep_flags: Vec<bool>) {
        for (row, keep) in list.results_mut().iter_mut().zip(keep_flags) {
            row.set_keep(keep);
        }
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

}
