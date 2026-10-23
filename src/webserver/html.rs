//! The frame of the website's HTML pages, and formatting shared by them.

use axum::response::Html;
use wikimisc::escape;

const STYLESHEET: &str = "https://tools-static.wmflabs.org/cdnjs/ajax/libs/twitter-bootstrap/4.6.2/css/bootstrap.min.css";

/// A complete page: `title` in the navigation bar, linking to `home`, above
/// `body` (HTML).
pub(super) fn page(title: &str, home: &str, body: &str) -> Html<String> {
    let title = escape::html(title);
    let home = escape::html(home);
    Html(format!(
        r#"<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>{title}</title>
<link rel="stylesheet" href="{STYLESHEET}">
<style>.num {{ text-align: right; font-family: monospace; }} .nowrap {{ white-space: nowrap; }}</style>
</head><body>
<div class="container">
<nav class="navbar navbar-expand navbar-light bg-light mb-3">
<a class="navbar-brand" href="{home}">{title}</a>
<ul class="navbar-nav ml-auto">
<li class="nav-item"><a class="nav-link" target="_blank" href="https://magnustools.toolforge.org">Tools</a></li>
<li class="nav-item"><a class="nav-link" target="_blank" href="https://github.com/magnusmanske/listeria_rs">Git</a></li>
<li class="nav-item"><a class="nav-link" target="_blank" href="https://en.wikipedia.org/wiki/User_talk:Magnus_Manske">Talk</a></li>
</ul>
</nav>
{body}
</div>
</body></html>"#
    ))
}

/// `1234567` as `1,234,567`.
pub(super) fn thousands(number: u64) -> String {
    let digits = number.to_string();
    let mut ret = String::with_capacity(digits.len() + digits.len() / 3);
    for (position, digit) in digits.chars().enumerate() {
        if position > 0 && (digits.len() - position).is_multiple_of(3) {
            ret.push(',');
        }
        ret.push(digit);
    }
    ret
}

/// A queue timestamp (`20261020035631`) as `2026-10-20 03:56:31`; anything
/// else as it is.
pub(super) fn timestamp(ts: &str) -> String {
    if ts.len() != 14 || !ts.bytes().all(|b| b.is_ascii_digit()) {
        return ts.to_string();
    }
    format!(
        "{}-{}-{} {}:{}:{}",
        &ts[0..4],
        &ts[4..6],
        &ts[6..8],
        &ts[8..10],
        &ts[10..12],
        &ts[12..14]
    )
}

/// A link (HTML) to `href`, showing `text`; both are escaped.
pub(super) fn link(href: &str, text: &str) -> String {
    format!(
        "<a href='{}'>{}</a>",
        escape::html(href),
        escape::html(text)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thousands() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1000), "1,000");
        assert_eq!(thousands(299_228), "299,228");
        assert_eq!(thousands(1_234_567), "1,234,567");
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(timestamp("20261020035631"), "2026-10-20 03:56:31");
        assert_eq!(timestamp("2026-10-20 03:56:31"), "2026-10-20 03:56:31");
        assert_eq!(timestamp(""), "");
    }

    #[test]
    fn test_link_escapes() {
        assert_eq!(
            link("?a=1&b='x'", "<b>"),
            "<a href='?a=1&amp;b=&#39;x&#39;'>&lt;b&gt;</a>"
        );
    }

    #[test]
    fn test_page_escapes_title() {
        let Html(html) = page("<Listeria>", "/", "<p>body</p>");
        assert!(html.contains("<title>&lt;Listeria&gt;</title>"));
        assert!(html.contains("<p>body</p>"));
    }
}
