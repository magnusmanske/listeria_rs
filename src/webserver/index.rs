//! The about page, and the update requests of the lists' "update" links
//! (`index.php?action=update&lang=de&page=…`).

use super::params::Params;
use super::{PageResult, WebError, WebState, bot_status, html};
use crate::wiki_repository::WikiNotRegistered;
use anyhow::anyhow;
use axum::extract::State;
use axum::http::StatusCode;
use wikimisc::escape;
use wikimisc::title::page_url;

const ABOUT: &str = "<div style='float:right'>
<a href='https://commons.wikimedia.org/wiki/File:Listeria_monocytogenes_01.jpg' target='_blank' title='Listeria monocytogenes. Biology pun. Sorry.'>
<img width='160' src='https://upload.wikimedia.org/wikipedia/commons/thumb/b/be/Listeria_monocytogenes_01.jpg/250px-Listeria_monocytogenes_01.jpg' alt='Listeria monocytogenes'>
</a></div>
<div class='lead'>
<p><a href='https://www.wikidata.org/wiki/User:ListeriaBot'>This bot</a> can generate and update lists on Wikipedia, based on <a href='https://query.wikidata.org/'>Wikidata queries</a>.</p>
<p>Check out <a href='http://magnusmanske.de/wordpress/?p=301'>my original blog post</a> explaining how this works, and the <a href='http://magnusmanske.de/wordpress/?p=650'>blog post for the current version 2</a>.</p>
<p><a href='botstatus.php'>Bot status</a> overview</p>
<!--<p><a href='dynamic.html'>Dynamic version</a></p>-->
</div>";

pub(super) async fn index(State(state): State<WebState>, params: Params) -> PageResult {
    let body = match params.get("action") {
        Some("update") => request_update(&state, &params).await?,
        _ => ABOUT.to_string(),
    };
    Ok(html::page("Listeria", "/", &body))
}

/// Moves a page to the front of the queue.
async fn request_update(
    state: &WebState,
    params: &Params,
) -> std::result::Result<String, WebError> {
    let project = params.get_or("project", "wikipedia");
    let language = params.get_or("lang", project);
    let page = params
        .get("page")
        .unwrap_or_default()
        .trim()
        .replace(' ', "_");
    if page.is_empty() {
        return Ok(alert("No page given."));
    }
    let Some(wiki) = wiki_for_language_project(language, project, |url| {
        state.site_matrix.get_wiki_for_server_url(url)
    }) else {
        return Err(unknown_wiki_error(language, project));
    };
    let template_q = state.config.get_template_start_q();
    let wiki_id = state
        .wikis
        .get_wiki_id(&wiki)
        .await
        .map_err(|error| wiki_lookup_error(error, &template_q))?;
    state.queue.request_update(wiki_id, &page).await?;

    let title = page.replace('_', " ");
    let page_link = match state.server_for_wiki(&wiki) {
        Some(server) => format!(
            "<a href='{}'><b>{}</b></a>",
            escape::html(&page_url(&server, &page)),
            escape::html(&title)
        ),
        None => format!("<b>{}</b>", escape::html(&title)),
    };
    Ok(format!(
        "<p>Your update request for {page_link} has been submitted, it should run within one minute.</p>\
         <p>Also, check out the {}!</p>",
        html::link(
            &bot_status::page_status_url(&wiki, &page),
            "current bot status"
        )
    ))
}

fn alert(message: &str) -> String {
    format!(
        "<div class='alert alert-warning'>{}</div>",
        escape::html(message)
    )
}

/// A `lang`/`project` pair SiteMatrix cannot resolve names no wiki at all:
/// a 404, like a wiki the bot has no lists on.
fn unknown_wiki_error(language: &str, project: &str) -> WebError {
    WebError::with_status(
        StatusCode::NOT_FOUND,
        anyhow!("Unknown wiki: {language}.{project}"),
    )
}

/// A failed `wikis` lookup as the page answers it. SiteMatrix knows the wiki,
/// so one without a row in the bot's `wikis` table has simply no list enrolled
/// yet: that is a 404 for the user, not a 500 for the bot's monitoring.
fn wiki_lookup_error(error: anyhow::Error, template_q: &str) -> WebError {
    let Some(not_registered) = error.downcast_ref::<WikiNotRegistered>() else {
        return error.into();
    };
    WebError::with_status(
        StatusCode::NOT_FOUND,
        anyhow!(
            "The wiki '{}' is not registered with Listeria, so there is no list to update. \
             The bot registers a wiki automatically when its local 'Wikidata list' template \
             is linked to the template's Wikidata item ({template_q}).",
            not_registered.wiki()
        ),
    )
}

/// The wiki of a language and project as the "update" links name them
/// (`lang=de`, `project=wikipedia` is `dewiki`); `wiki_for_server_url` finds
/// the wiki of a server URL.
fn wiki_for_language_project(
    language: &str,
    project: &str,
    wiki_for_server_url: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    let language = language.trim().to_lowercase();
    let project = project.trim().to_lowercase();
    let special = match language.as_str() {
        "commons" => Some("commonswiki"),
        "wikidata" => Some("wikidatawiki"),
        "meta" => Some("metawiki"),
        "species" => Some("specieswiki"),
        _ => None,
    };
    match special {
        Some(wiki) => Some(wiki.to_string()),
        None => wiki_for_server_url(&format!("https://{language}.{project}.org")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site_matrix(url: &str) -> Option<String> {
        match url {
            "https://de.wikipedia.org" => Some("dewiki".to_string()),
            "https://en.wikisource.org" => Some("enwikisource".to_string()),
            _ => None,
        }
    }

    #[test]
    fn test_wiki_for_language_project() {
        let wiki = |language, project| wiki_for_language_project(language, project, site_matrix);
        assert_eq!(wiki("de", "wikipedia").as_deref(), Some("dewiki"));
        assert_eq!(wiki(" DE ", "Wikipedia").as_deref(), Some("dewiki"));
        assert_eq!(wiki("en", "wikisource").as_deref(), Some("enwikisource"));
        assert_eq!(wiki("commons", "wikimedia").as_deref(), Some("commonswiki"));
        assert_eq!(
            wiki("wikidata", "wikidata").as_deref(),
            Some("wikidatawiki")
        );
        assert_eq!(wiki("meta", "wikimedia").as_deref(), Some("metawiki"));
        assert_eq!(wiki("species", "wikimedia").as_deref(), Some("specieswiki"));
        assert_eq!(wiki("xx", "wikipedia"), None);
    }

    /// A valid wiki without a `wikis` row is the user's answer to see (404),
    /// with a message that says what is missing; the failure is not reported
    /// as the bot's own.
    #[test]
    fn test_unregistered_wiki_is_not_found() {
        let error = anyhow::Error::new(WikiNotRegistered::new("tswiki"));
        let page_error = wiki_lookup_error(error, "Q19860885");
        assert_eq!(page_error.status(), StatusCode::NOT_FOUND);
        let message = format!("{:#}", page_error.error);
        assert!(message.contains("'tswiki'"), "{message}");
        assert!(message.contains("Q19860885"), "{message}");
    }

    /// A `lang`/`project` pair SiteMatrix knows nothing about is a 404 as
    /// well; the "update" links of copied list pages can name anything.
    #[test]
    fn test_unknown_wiki_is_not_found() {
        let page_error = unknown_wiki_error("xx", "wikipedia");
        assert_eq!(page_error.status(), StatusCode::NOT_FOUND);
        let message = format!("{:#}", page_error.error);
        assert!(message.contains("Unknown wiki: xx.wikipedia"), "{message}");
    }

    /// Any other database failure stays a 500.
    #[test]
    fn test_other_lookup_errors_stay_500() {
        let page_error = wiki_lookup_error(anyhow!("database went away"), "Q19860885");
        assert_eq!(page_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
