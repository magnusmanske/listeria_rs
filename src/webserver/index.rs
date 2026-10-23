//! The about page, and the update requests of the lists' "update" links
//! (`index.php?action=update&lang=de&page=…`).

use super::params::Params;
use super::{PageResult, WebState, bot_status, html};
use axum::extract::State;
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
<p><a href='dynamic.html'>Dynamic version</a></p>
</div>";

pub(super) async fn index(State(state): State<WebState>, params: Params) -> PageResult {
    let body = match params.get("action") {
        Some("update") => request_update(&state, &params).await?,
        _ => ABOUT.to_string(),
    };
    Ok(html::page("Listeria", "/", &body))
}

/// Moves a page to the front of the queue.
async fn request_update(state: &WebState, params: &Params) -> anyhow::Result<String> {
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
        return Ok(alert(&format!("Unknown wiki: {language}.{project}")));
    };
    let wiki_id = state.wikis.get_wiki_id(&wiki).await?;
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
}
