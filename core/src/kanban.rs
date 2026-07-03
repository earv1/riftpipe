//! Pure kanban board logic — parsing + serialization for the on-disk file format,
//! with **no I/O and no clock**. Shared by the native server (`src/app/kanban.rs`,
//! over `std::fs`) and the browser store (`riftpipe-web`, over OPFS), so the board
//! format has ONE definition and a board is portable between a native peer and a
//! browser peer.
//!
//! On-disk model:
//!   board.md                "# Title", then "- Column" per column (ordered)
//!   tickets/<id>/card.md     "# Card Title\n\n<markdown description>"
//!   tickets/<id>/meta.toml   column (str), position (int), done (bool)
//!   tickets/<id>/comments/<ts>__<author>.md

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Card {
    pub id: String,
    pub title: String,
    pub column: String,
    pub position: i64,
    pub done: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Comment {
    pub id: String,
    pub author: String,
    pub ts: String,
    pub text: String,
}

/// The structural fields of `meta.toml`, tolerant of missing/blank values.
pub struct Meta {
    pub column: Option<String>,
    pub position: i64,
    pub done: bool,
}

/// First markdown `# ` heading, trimmed.
pub fn first_heading(text: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(rest) = line.trim_end().strip_prefix("# ") {
            let h = rest.trim();
            if !h.is_empty() {
                return Some(h.to_string());
            }
        }
    }
    None
}

/// Parse `board.md` → (title, ordered columns). Robust to a missing/blank file.
pub fn parse_board_md(text: &str) -> (String, Vec<String>) {
    let title = first_heading(text).unwrap_or_else(|| "Board".to_string());
    let mut columns = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.trim_end().strip_prefix("- ") {
            let c = rest.trim();
            if !c.is_empty() {
                columns.push(c.to_string());
            }
        }
    }
    (title, columns)
}

/// Serialize (title, columns) → `board.md` text.
pub fn board_md(title: &str, columns: &[String]) -> String {
    let mut s = format!("# {title}\n\n");
    for c in columns {
        s.push_str("- ");
        s.push_str(c);
        s.push('\n');
    }
    s
}

/// Split `card.md` into (title, description).
pub fn split_card_md(text: &str) -> (String, String) {
    let lines: Vec<&str> = text.lines().collect();
    let mut title = String::new();
    let mut i = 0;
    while i < lines.len() {
        if let Some(rest) = lines[i].trim_end().strip_prefix("# ") {
            if !rest.trim().is_empty() {
                title = rest.trim().to_string();
                i += 1;
                break;
            }
        }
        i += 1;
    }
    let description = lines[i.min(lines.len())..]
        .join("\n")
        .trim_start_matches('\n')
        .trim_end()
        .to_string();
    (title, description)
}

/// Serialize (title, description) → `card.md` text.
pub fn card_md(title: &str, description: &str) -> String {
    if description.trim().is_empty() {
        format!("# {title}\n")
    } else {
        format!("# {title}\n\n{}\n", description.trim_end())
    }
}

/// Parse `meta.toml`. A deliberately tiny `key = value` reader (the file only ever
/// has three scalar fields) so the wasm build needs no TOML crate; the output of
/// [`meta_toml`] and of a real TOML writer both parse here.
pub fn parse_meta(text: &str) -> Meta {
    let mut meta = Meta { column: None, position: 0, done: false };
    for line in text.lines() {
        let Some((k, v)) = line.split_once('=') else { continue };
        let (k, v) = (k.trim(), v.trim());
        match k {
            "column" => {
                let s = unquote(v);
                meta.column = if s.is_empty() { None } else { Some(s) };
            }
            "position" => meta.position = v.parse().unwrap_or(0),
            "done" => meta.done = v == "true",
            _ => {}
        }
    }
    meta
}

/// Strip surrounding quotes from a TOML basic string and unescape `\\` / `\"`
/// (the only escapes [`meta_toml`] emits).
fn unquote(v: &str) -> String {
    let inner = v.strip_prefix('"').and_then(|s| s.strip_suffix('"')).unwrap_or(v);
    let mut out = String::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(n) = chars.next() {
                out.push(n); // \\ -> \, \" -> "
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Serialize structural fields → `meta.toml` text (valid TOML). Escapes `\` and `"`
/// in the column name so [`parse_meta`] round-trips it exactly.
pub fn meta_toml(column: &str, position: i64, done: bool) -> String {
    let esc = column.replace('\\', "\\\\").replace('"', "\\\"");
    format!("column = \"{esc}\"\nposition = {position}\ndone = {done}\n")
}

/// A card from its files' text (defaults for any missing/blank piece).
pub fn card_from_files(id: &str, card_md_text: &str, meta_text: &str, default_column: &str) -> Card {
    let meta = parse_meta(meta_text);
    Card {
        id: id.to_string(),
        title: first_heading(card_md_text).unwrap_or_else(|| id.to_string()),
        column: meta.column.unwrap_or_else(|| default_column.to_string()),
        position: meta.position,
        done: meta.done,
    }
}

/// Sanitize an author into a filename-safe slug `[a-z0-9-]+`, or `anon`.
pub fn sanitize_author(author: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in author.to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' {
            out.push(ch);
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let slug = out.trim_matches('-').to_string();
    if slug.is_empty() { "anon".to_string() } else { slug }
}

/// Parse a comment filename stem `<ts>__<author>` → (ts, author).
pub fn parse_comment_name(stem: &str) -> Option<(String, String)> {
    let sep = stem.find("__")?;
    Some((stem[..sep].to_string(), stem[sep + 2..].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn board_and_card_roundtrip() {
        let (t, cols) = parse_board_md("# My Board\n\n- Todo\n- Doing\n- Done\n");
        assert_eq!(t, "My Board");
        assert_eq!(cols, ["Todo", "Doing", "Done"]);
        assert_eq!(parse_board_md(&board_md(&t, &cols)), (t, cols));

        let (title, desc) = split_card_md("# Hello\n\nbody line\nmore");
        assert_eq!((title.as_str(), desc.as_str()), ("Hello", "body line\nmore"));
        assert_eq!(split_card_md(&card_md(&title, &desc)).0, "Hello");
    }

    #[test]
    fn meta_roundtrips_and_tolerates_junk() {
        let m = parse_meta(&meta_toml("In Progress", 7, true));
        assert_eq!(m.column.as_deref(), Some("In Progress"));
        assert_eq!((m.position, m.done), (7, true));
        let empty = parse_meta("");
        assert_eq!((empty.column, empty.position, empty.done), (None, 0, false));

        // Columns containing quotes/backslashes round-trip exactly.
        let weird = r#"a"b\c"#;
        let m = parse_meta(&meta_toml(weird, 0, false));
        assert_eq!(m.column.as_deref(), Some(weird));
    }

    #[test]
    fn authors_and_comment_names() {
        assert_eq!(sanitize_author("Alice Smith!"), "alice-smith");
        assert_eq!(sanitize_author("  "), "anon");
        assert_eq!(
            parse_comment_name("2026-06-27T18-00-00Z__alice"),
            Some(("2026-06-27T18-00-00Z".into(), "alice".into()))
        );
        assert_eq!(parse_comment_name("nodelimiter"), None);
    }
}
