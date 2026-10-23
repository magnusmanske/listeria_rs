//! `botstatus.php`: the queue by group of wikis (Wikipedia, Wikisource, …),
//! wiki, status and page.
//!
//! - no `action`: the groups
//! - `action=group&group=wikisource`: the wikis of a group
//! - `action=wiki&wiki=dewiki[&status=FAIL]`: the statuses of a wiki's
//!   pages [and its pages with one status]
//! - `action=page&wiki=dewiki&page=…`: a page

use super::bot_status_repository::{PageDetails, PageRow, StatusCount};
use super::params::Params;
use super::{PageResult, WebState, html};
use axum::extract::State;
use std::collections::BTreeMap;
use wikimisc::escape;
use wikimisc::title::{page_url, percent_encode, ucfirst};

/// Most pages listed for a status of a wiki.
const MAX_LISTED_PAGES: usize = 10_000;
/// The statuses shown for each wiki, in this order; any others it has follow.
const MAIN_STATUSES: &[&str] = &["OK", "FAIL", "PAUSED", "TRANSLATION", "RUNNING", "WAITING"];
/// The group of wikis whose server is unknown.
const OTHER_GROUP: &str = "other";

pub(super) async fn bot_status(State(state): State<WebState>, params: Params) -> PageResult {
    let body = match params.get_or("action", "") {
        "" => groups_view(&wiki_summaries(&state).await?),
        "group" => group_view(params.get_or("group", ""), &wiki_summaries(&state).await?),
        "wiki" => {
            let wiki = params.get_or("wiki", "");
            let summary = wiki_summaries(&state)
                .await?
                .into_iter()
                .find(|summary| summary.wiki == wiki);
            let listed = match params.get("status").filter(|status| !status.is_empty()) {
                Some(status) => {
                    let limit = MAX_LISTED_PAGES as u64 + 1;
                    Some((status, state.bot_status.pages(wiki, status, limit).await?))
                }
                None => None,
            };
            wiki_view(wiki, summary.as_ref(), listed)
        }
        "page" => {
            let wiki = params.get_or("wiki", "");
            let page = params.get_or("page", "").replace(' ', "_");
            let details = state.bot_status.page(wiki, &page).await?;
            page_view(wiki, &page, details.as_ref(), state.server_for_wiki(wiki))
        }
        action => format!("<pre>Unknown action '{}'</pre>", escape::html(action)),
    };
    Ok(html::page("Listeria bot status", "botstatus.php", &body))
}

/// The URL of a page's status.
pub(super) fn page_status_url(wiki: &str, page: &str) -> String {
    format!(
        "botstatus.php?action=page&wiki={}&page={}",
        percent_encode(wiki),
        percent_encode(page)
    )
}

fn wiki_status_url(wiki: &str, status: Option<&str>) -> String {
    let url = format!("botstatus.php?action=wiki&wiki={}", percent_encode(wiki));
    match status {
        Some(status) => format!("{url}&status={}", percent_encode(status)),
        None => url,
    }
}

fn group_url(group: &str) -> String {
    format!("botstatus.php?action=group&group={}", percent_encode(group))
}

/// The pages of a wiki, by status.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WikiSummary {
    wiki: String,
    /// `https://de.wikipedia.org`, if known
    server: Option<String>,
    pages: BTreeMap<String, u64>,
}

impl WikiSummary {
    fn total(&self) -> u64 {
        self.pages.values().sum()
    }

    fn with_status(&self, status: &str) -> u64 {
        self.pages.get(status).copied().unwrap_or_default()
    }

    /// The project (`wikipedia`, `wikisource`, …), from the server's domain;
    /// Commons is a group of its own.
    fn group(&self) -> &str {
        let Some(server) = &self.server else {
            return OTHER_GROUP;
        };
        let host = server.rsplit("//").next().unwrap_or(server);
        let host = host.trim_end_matches('/');
        if host == "commons.wikimedia.org" {
            return "commons";
        }
        host.rsplit('.').nth(1).unwrap_or(OTHER_GROUP)
    }

    /// The statuses to show: the main ones, then any others this wiki has.
    fn statuses(&self) -> Vec<&str> {
        let others = self
            .pages
            .keys()
            .map(String::as_str)
            .filter(|status| !MAIN_STATUSES.contains(status));
        MAIN_STATUSES.iter().copied().chain(others).collect()
    }

    fn site_link(&self) -> String {
        match &self.server {
            Some(server) => {
                let host = server.rsplit("//").next().unwrap_or(server);
                html::link(server, host)
            }
            None => String::new(),
        }
    }
}

async fn wiki_summaries(state: &WebState) -> anyhow::Result<Vec<WikiSummary>> {
    let counts = state.bot_status.status_counts().await?;
    Ok(summarize(counts, |wiki| state.server_for_wiki(wiki)))
}

/// The summaries of the wikis in `counts`, by name.
fn summarize(
    counts: Vec<StatusCount>,
    server_for_wiki: impl Fn(&str) -> Option<String>,
) -> Vec<WikiSummary> {
    let mut pages: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    for count in counts {
        *pages
            .entry(count.wiki)
            .or_default()
            .entry(count.status)
            .or_default() += count.pages;
    }
    pages
        .into_iter()
        .map(|(wiki, pages)| WikiSummary {
            server: server_for_wiki(&wiki),
            wiki,
            pages,
        })
        .collect()
}

fn number_cell(number: u64) -> String {
    format!("<td class='num'>{}</td>", html::thousands(number))
}

fn groups_view(summaries: &[WikiSummary]) -> String {
    let mut groups: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    for summary in summaries {
        let (wikis, pages) = groups.entry(summary.group()).or_default();
        *wikis += 1;
        *pages += summary.total();
    }
    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|(_, (_, a)), (_, (_, b))| b.cmp(a));

    let mut ret = "<h1>Groups</h1><table class='table table-striped'>\
        <thead><tr><th>Group</th><th class='num'>Wikis</th><th class='num'>Listeria pages</th></tr></thead><tbody>"
        .to_string();
    for (group, (wikis, pages)) in &groups {
        ret += &format!(
            "<tr><th>{}</th>{}{}</tr>",
            html::link(&group_url(group), &ucfirst(group)),
            number_cell(*wikis),
            number_cell(*pages)
        );
    }
    ret += &format!(
        "<tr><th>Total</th>{}{}</tr></tbody></table>",
        number_cell(summaries.len() as u64),
        number_cell(summaries.iter().map(WikiSummary::total).sum())
    );
    ret
}

fn group_view(group: &str, summaries: &[WikiSummary]) -> String {
    let mut ret = format!(
        "<h1>{}</h1><table class='table table-striped'><thead><tr><th>Wiki</th><th>Site</th>\
         <th class='num'>OK</th><th class='num'>Other</th><th class='num'>Total</th></tr></thead><tbody>",
        escape::html(&ucfirst(group))
    );
    for summary in summaries.iter().filter(|summary| summary.group() == group) {
        let ok = summary.with_status("OK");
        ret += &format!(
            "<tr><th>{}</th><td>{}</td>{}{}{}</tr>",
            html::link(&wiki_status_url(&summary.wiki, None), &summary.wiki),
            summary.site_link(),
            number_cell(ok),
            number_cell(summary.total() - ok),
            number_cell(summary.total())
        );
    }
    ret += "</tbody></table>";
    ret
}

fn wiki_view(
    wiki: &str,
    summary: Option<&WikiSummary>,
    listed: Option<(&str, Vec<PageRow>)>,
) -> String {
    let Some(summary) = summary else {
        return format!(
            "<div class='alert alert-warning'>No pages of wiki '{}' are in the queue.</div>",
            escape::html(wiki)
        );
    };
    let heading = match &summary.server {
        Some(server) => html::link(server, wiki),
        None => escape::html(wiki).into_owned(),
    };
    let mut ret = format!(
        "<h1>{heading}</h1><table class='table table-striped'>\
         <thead><tr><th>Status</th><th class='num'>Pages</th></tr></thead><tbody>"
    );
    for status in summary.statuses() {
        ret += &format!(
            "<tr><th>{}</th>{}</tr>",
            html::link(&wiki_status_url(wiki, Some(status)), status),
            number_cell(summary.with_status(status))
        );
    }
    ret += "</tbody></table>";
    if let Some((status, pages)) = listed {
        ret += &pages_with_status(summary, status, &pages);
    }
    ret
}

fn pages_with_status(summary: &WikiSummary, status: &str, pages: &[PageRow]) -> String {
    let mut ret = format!("<h2 id='status_{0}'>{0}</h2>", escape::html(status));
    if pages.len() > MAX_LISTED_PAGES {
        ret += &format!(
            "<p>The first {} of {} pages:</p>",
            html::thousands(MAX_LISTED_PAGES as u64),
            html::thousands(summary.with_status(status))
        );
    }
    ret += "<table class='table table-striped'><thead><tr><th>Page</th><th>Note</th>\
        <th class='nowrap'>Last update</th><th class='num nowrap'>Time [sec]</th></tr></thead><tbody>";
    for row in pages.iter().take(MAX_LISTED_PAGES) {
        ret += &format!(
            "<tr><th>{} <small>[{}]</small></th><td>{}</td><td class='nowrap'>{}</td>{}</tr>",
            page_link(summary.server.as_deref(), &row.page),
            html::link(&page_status_url(&summary.wiki, &row.page), "details"),
            escape::html(&row.message),
            escape::html(&html::timestamp(&row.timestamp)),
            number_cell(row.runtime_sec.unwrap_or_default().max(0) as u64)
        );
    }
    ret += "</tbody></table>";
    ret
}

/// A link to a page on its wiki, or its title without a known server.
fn page_link(server: Option<&str>, page: &str) -> String {
    let title = page.replace('_', " ");
    match server {
        Some(server) => html::link(&page_url(server, page), &title),
        None => escape::html(&title).into_owned(),
    }
}

fn page_view(
    wiki: &str,
    page: &str,
    details: Option<&PageDetails>,
    server: Option<String>,
) -> String {
    let Some(details) = details else {
        return format!(
            "<div class='alert alert-warning'>Page '{}' of wiki '{}' is not in the queue.</div>",
            escape::html(&page.replace('_', " ")),
            escape::html(wiki)
        );
    };
    let wiki_link = match &server {
        Some(server) => html::link(server, &details.wiki),
        None => escape::html(&details.wiki).into_owned(),
    };
    let optional = |number: Option<i64>| number.map(|n| n.to_string()).unwrap_or_default();
    let rows = [
        ("Page", page_link(server.as_deref(), &details.page)),
        ("Wiki", wiki_link),
        (
            "Status",
            html::link(
                &wiki_status_url(&details.wiki, Some(&details.status)),
                &details.status,
            ),
        ),
        ("Note", escape::html(&details.message).into_owned()),
        (
            "Time",
            escape::html(&html::timestamp(&details.timestamp)).into_owned(),
        ),
        (
            "Last runtime",
            details
                .runtime_sec
                .map(|seconds| format!("{seconds} seconds"))
                .unwrap_or_default(),
        ),
        (
            "SPARQL",
            format!("<pre class='mb-0'>{}</pre>", escape::html(&details.sparql)),
        ),
        ("Priority", optional(details.priority)),
        ("Bot version", optional(details.bot_version)),
    ];
    let mut ret = "<table class='table table-striped'><tbody>".to_string();
    for (label, value) in rows {
        ret += &format!("<tr><th class='nowrap'>{label}</th><td>{value}</td></tr>");
    }
    ret += "</tbody></table>";
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(wiki: &str, status: &str, pages: u64) -> StatusCount {
        StatusCount {
            wiki: wiki.to_string(),
            status: status.to_string(),
            pages,
        }
    }

    fn server(wiki: &str) -> Option<String> {
        match wiki {
            "dewiki" => Some("https://de.wikipedia.org".to_string()),
            "enwiki" => Some("https://en.wikipedia.org".to_string()),
            "commonswiki" => Some("https://commons.wikimedia.org".to_string()),
            "metawiki" => Some("https://meta.wikimedia.org".to_string()),
            "wikidatawiki" => Some("https://www.wikidata.org".to_string()),
            "enwikisource" => Some("https://en.wikisource.org".to_string()),
            _ => None,
        }
    }

    fn summaries() -> Vec<WikiSummary> {
        summarize(
            vec![
                count("dewiki", "OK", 3189),
                count("dewiki", "FAIL", 359),
                count("dewiki", "DELETED", 2),
                count("enwiki", "OK", 1000),
                count("commonswiki", "OK", 10),
                count("wikidatawiki", "WAITING", 5),
                count("enwikisource", "OK", 1),
                count("unknownwiki", "OK", 1),
            ],
            server,
        )
    }

    fn summary(wiki: &str) -> WikiSummary {
        summaries().into_iter().find(|s| s.wiki == wiki).unwrap()
    }

    #[test]
    fn test_summarize() {
        let summaries = summaries();
        let wikis: Vec<_> = summaries.iter().map(|s| s.wiki.as_str()).collect();
        assert_eq!(
            wikis,
            [
                "commonswiki",
                "dewiki",
                "enwiki",
                "enwikisource",
                "unknownwiki",
                "wikidatawiki"
            ]
        );
        let dewiki = summary("dewiki");
        assert_eq!(dewiki.total(), 3550);
        assert_eq!(dewiki.with_status("FAIL"), 359);
        assert_eq!(dewiki.with_status("PAUSED"), 0);
    }

    #[test]
    fn test_group() {
        assert_eq!(summary("dewiki").group(), "wikipedia");
        assert_eq!(summary("commonswiki").group(), "commons");
        assert_eq!(summary("wikidatawiki").group(), "wikidata");
        assert_eq!(summary("enwikisource").group(), "wikisource");
        assert_eq!(summary("unknownwiki").group(), OTHER_GROUP);
        let meta = WikiSummary {
            wiki: "metawiki".to_string(),
            server: server("metawiki"),
            pages: BTreeMap::new(),
        };
        assert_eq!(meta.group(), "wikimedia");
    }

    #[test]
    fn test_statuses_main_ones_first() {
        assert_eq!(
            summary("dewiki").statuses(),
            [
                "OK",
                "FAIL",
                "PAUSED",
                "TRANSLATION",
                "RUNNING",
                "WAITING",
                "DELETED"
            ]
        );
    }

    #[test]
    fn test_groups_view_sorted_by_pages() {
        let html = groups_view(&summaries());
        let wikipedia = html.find(">Wikipedia<").unwrap();
        let commons = html.find(">Commons<").unwrap();
        let wikidata = html.find(">Wikidata<").unwrap();
        assert!(wikipedia < commons && commons < wikidata);
        assert!(html.contains("<td class='num'>4,550</td>"));
        // Total: 6 wikis, 4,567 pages
        assert!(
            html.contains("<tr><th>Total</th><td class='num'>6</td><td class='num'>4,567</td>")
        );
    }

    #[test]
    fn test_group_view() {
        let html = group_view("wikipedia", &summaries());
        assert!(html.contains("<h1>Wikipedia</h1>"));
        assert!(html.contains("botstatus.php?action=wiki&amp;wiki=dewiki"));
        assert!(html.contains("<a href='https://de.wikipedia.org'>de.wikipedia.org</a>"));
        assert!(html.contains(
            "<td class='num'>3,189</td><td class='num'>361</td><td class='num'>3,550</td>"
        ));
        assert!(!html.contains("commonswiki"));
    }

    #[test]
    fn test_wiki_view_with_pages() {
        let pages = vec![PageRow {
            page: "Benutzer:X/Liste_<1>".to_string(),
            message: "Namespace 0 not allowed".to_string(),
            timestamp: "20261020035631".to_string(),
            runtime_sec: Some(3),
        }];
        let html = wiki_view("dewiki", Some(&summary("dewiki")), Some(("FAIL", pages)));
        assert!(html.contains("<h1><a href='https://de.wikipedia.org'>dewiki</a></h1>"));
        assert!(html.contains("<h2 id='status_FAIL'>FAIL</h2>"));
        assert!(html.contains(">Benutzer:X/Liste &lt;1&gt;</a>"));
        assert!(html.contains("https://de.wikipedia.org/wiki/Benutzer:X/Liste_%3C1%3E"));
        assert!(html.contains("action=page&amp;wiki=dewiki&amp;page=Benutzer%3AX%2FListe_%3C1%3E"));
        assert!(html.contains("2026-10-20 03:56:31"));
        assert!(!html.contains("The first"));
    }

    #[test]
    fn test_wiki_view_unknown_wiki() {
        let html = wiki_view("<x>", None, None);
        assert!(html.contains("No pages of wiki '&lt;x&gt;'"));
    }

    #[test]
    fn test_page_view() {
        let details = PageDetails {
            page: "Benutzer:Magnus_Manske/listeria_test".to_string(),
            wiki: "dewiki".to_string(),
            status: "OK".to_string(),
            runtime_sec: Some(32),
            sparql: "SELECT ?item { ?item wdt:P31 wd:Q5 }".to_string(),
            priority: Some(0),
            bot_version: Some(2),
            ..Default::default()
        };
        let html = page_view("dewiki", &details.page, Some(&details), server("dewiki"));
        assert!(html.contains(">Benutzer:Magnus Manske/listeria test</a>"));
        assert!(html.contains("<td>32 seconds</td>"));
        assert!(html.contains("<th class='nowrap'>Bot version</th><td>2</td>"));
        assert!(page_view("dewiki", "Nope", None, None).contains("is not in the queue"));
    }

    #[test]
    fn test_page_status_url() {
        assert_eq!(
            page_status_url("dewiki", "A/B C&D"),
            "botstatus.php?action=page&wiki=dewiki&page=A%2FB%20C%26D"
        );
    }
}
