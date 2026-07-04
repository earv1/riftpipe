//! The kanban "server" running in the browser — the same JSON API the SolidJS UI
//! calls, handled by wasm over **OPFS**, with **no local server**. It uses the
//! plain file-tree layout (`board.md` + `tickets/<id>/{card.md,meta.toml,comments/*}`)
//! via the [`crate::format`] logic — so per-card files stay independently mergeable
//! (no monolithic-board clobbering), and the generic riftpipe tree sync
//! ([`riftpipe_web::tree_sync`]) carries them to peers without knowing the layout.
//!
//! `kanbanHandle(method, path, body)` is the entry point; a thin shim in `api.ts`
//! routes the app's `fetch('/api/*')` here.

use crate::format as kb;
use crate::format::{Card, Comment};
use riftpipe_web::opfs::{list, opfs_root, read_text, subdir, write_text};
use riftpipe_web::tree_sync;
use wasm_bindgen::prelude::*;
use web_sys::FileSystemDirectoryHandle;

// ---------------------------------------------------------------------------
// Board reading / mutations (over the file tree, via the format logic)
// ---------------------------------------------------------------------------

/// Seed `board.md` on first use so a fresh browser has columns.
async fn ensure_seed(root: &FileSystemDirectoryHandle) {
    if read_text(root, "board.md").await.is_none() {
        let seed = kb::board_md("My Board", &["Todo".into(), "Doing".into(), "Done".into()]);
        let _ = write_text(root, "board.md", &seed).await;
    }
}

async fn read_card(tickets: &FileSystemDirectoryHandle, id: &str, default_col: &str) -> Card {
    let cdir = match subdir(tickets, id, false).await {
        Ok(d) => d,
        Err(_) => return kb::card_from_files(id, "", "", default_col),
    };
    let card_md = read_text(&cdir, "card.md").await.unwrap_or_default();
    let meta = read_text(&cdir, "meta.toml").await.unwrap_or_default();
    kb::card_from_files(id, &card_md, &meta, default_col)
}

async fn all_cards(root: &FileSystemDirectoryHandle, columns: &[String]) -> Vec<Card> {
    let default_col = columns.first().map(String::as_str).unwrap_or("Todo");
    let tickets = match subdir(root, "tickets", true).await {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    let mut cards = Vec::new();
    for id in list(&tickets).await {
        cards.push(read_card(&tickets, &id, default_col).await);
    }
    cards
}

/// Ensure `tickets/<id>/` (and its `comments/`) exist; return the card dir.
async fn ensure_card_dir(root: &FileSystemDirectoryHandle, id: &str) -> Result<FileSystemDirectoryHandle, JsValue> {
    let tickets = subdir(root, "tickets", true).await?;
    let cdir = subdir(&tickets, id, true).await?;
    subdir(&cdir, "comments", true).await?;
    Ok(cdir)
}

async fn write_meta(cdir: &FileSystemDirectoryHandle, column: &str, position: i64, done: bool) -> Result<(), JsValue> {
    write_text(cdir, "meta.toml", &kb::meta_toml(column, position, done)).await
}

async fn write_card(cdir: &FileSystemDirectoryHandle, title: &str, description: &str) -> Result<(), JsValue> {
    write_text(cdir, "card.md", &kb::card_md(title, description)).await
}

fn new_id() -> String {
    format!("tk_{:08x}", (js_sys::Math::random() * 4_294_967_296.0) as u32)
}

fn iso_now() -> String {
    js_sys::Date::new_0().to_iso_string().as_string().unwrap_or_default().replace(':', "-")
}

// ---------------------------------------------------------------------------
// Request handling
// ---------------------------------------------------------------------------

fn resp(status: u16, body: String) -> JsValue {
    let o = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&o, &"status".into(), &(status as f64).into());
    let _ = js_sys::Reflect::set(&o, &"body".into(), &body.into());
    o.into()
}

fn json_resp<T: serde::Serialize>(value: &T) -> JsValue {
    resp(200, serde_json::to_string(value).unwrap_or_else(|_| "null".into()))
}

fn summary(c: &Card) -> serde_json::Value {
    serde_json::json!({
        "id": c.id, "title": c.title, "column": c.column,
        "position": c.position, "done": c.done,
    })
}

/// Handle one API request entirely in the browser (over OPFS).
#[wasm_bindgen(js_name = kanbanHandle)]
pub async fn handle(method: String, path: String, body: String) -> JsValue {
    match route(&method, &path, &body).await {
        Ok(v) => v,
        Err(_) => resp(500, "internal error".into()),
    }
}

async fn route(method: &str, path: &str, body: &str) -> Result<JsValue, JsValue> {
    let clean = path.split('?').next().unwrap_or("");
    let segs: Vec<&str> = clean.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    let body: serde_json::Value = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);

    let root = opfs_root().await?;
    ensure_seed(&root).await;
    let (title, columns) = kb::parse_board_md(&read_text(&root, "board.md").await.unwrap_or_default());

    match (method, segs.as_slice()) {
        ("GET", ["api", "board"]) => {
            let cards: Vec<_> = all_cards(&root, &columns).await.iter().map(summary).collect();
            Ok(json_resp(&serde_json::json!({ "title": title, "columns": columns, "cards": cards })))
        }

        ("GET", ["api", "cards", id]) => {
            let tickets = subdir(&root, "tickets", true).await?;
            if !list(&tickets).await.iter().any(|n| n == id) {
                return Ok(resp(404, "Not Found".into()));
            }
            let default_col = columns.first().map(String::as_str).unwrap_or("Todo");
            Ok(json_resp(&summary(&read_card(&tickets, id, default_col).await)))
        }

        ("GET", ["api", "cards", id, "detail"]) => {
            let tickets = subdir(&root, "tickets", true).await?;
            if !list(&tickets).await.iter().any(|n| n == id) {
                return Ok(resp(404, "Not Found".into()));
            }
            let default_col = columns.first().map(String::as_str).unwrap_or("Todo");
            let card = read_card(&tickets, id, default_col).await;
            let cdir = subdir(&tickets, id, false).await?;
            let (_t, description) = kb::split_card_md(&read_text(&cdir, "card.md").await.unwrap_or_default());
            let comments = read_comments(&cdir).await;
            Ok(json_resp(&serde_json::json!({
                "id": card.id, "title": card.title, "column": card.column,
                "position": card.position, "done": card.done,
                "description": description, "comments": comments,
            })))
        }

        ("POST", ["api", "cards"]) => {
            let column = body.get("column").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| columns.first().cloned().unwrap_or_else(|| "Todo".into()));
            let title = body.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let id = new_id();
            let existing = all_cards(&root, &columns).await;
            let max_pos = existing.iter().filter(|c| c.column == column).map(|c| c.position).max().unwrap_or(-1);
            let title = if title.is_empty() { id.clone() } else { title.to_string() };
            let cdir = ensure_card_dir(&root, &id).await?;
            write_card(&cdir, &title, "").await?;
            write_meta(&cdir, &column, max_pos + 1, false).await?;
            tree_sync::push_text(&format!("tickets/{id}/card.md"), &kb::card_md(&title, ""));
            tree_sync::push_lww(
                &format!("tickets/{id}/meta.toml"),
                kb::meta_toml(&column, max_pos + 1, false).as_bytes(),
            );
            Ok(json_resp(&summary(&Card { id, title, column, position: max_pos + 1, done: false })))
        }

        ("PATCH", ["api", "cards", id]) => {
            let tickets = subdir(&root, "tickets", true).await?;
            if !list(&tickets).await.iter().any(|n| n == id) {
                return Ok(resp(404, "Not Found".into()));
            }
            let default_col = columns.first().map(String::as_str).unwrap_or("Todo");
            let mut card = read_card(&tickets, id, default_col).await;
            let cdir = subdir(&tickets, id, false).await?;
            let (_old_title, mut description) = kb::split_card_md(&read_text(&cdir, "card.md").await.unwrap_or_default());

            if let Some(c) = body.get("column").and_then(|v| v.as_str()) { card.column = c.to_string(); }
            if let Some(p) = body.get("position").and_then(|v| v.as_i64()) { card.position = p; }
            if let Some(d) = body.get("done").and_then(|v| v.as_bool()) { card.done = d; }
            if let Some(t) = body.get("title").and_then(|v| v.as_str()) {
                card.title = if t.is_empty() { id.to_string() } else { t.to_string() };
            }
            let touched_text = body.get("title").is_some() || body.get("description").is_some();
            if let Some(desc) = body.get("description").and_then(|v| v.as_str()) { description = desc.to_string(); }

            // Always persist structural fields; only re-serialize card.md when the
            // title/description actually changed (avoids prose drift).
            write_meta(&cdir, &card.column, card.position, card.done).await?;
            tree_sync::push_lww(
                &format!("tickets/{id}/meta.toml"),
                kb::meta_toml(&card.column, card.position, card.done).as_bytes(),
            );
            if touched_text {
                write_card(&cdir, &card.title, &description).await?;
                tree_sync::push_text(&format!("tickets/{id}/card.md"), &kb::card_md(&card.title, &description));
            }
            Ok(json_resp(&summary(&card)))
        }

        ("POST", ["api", "cards", id, "comments"]) => {
            let tickets = subdir(&root, "tickets", true).await?;
            if !list(&tickets).await.iter().any(|n| n == id) {
                return Ok(resp(404, "Not Found".into()));
            }
            let text = body.get("text").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
            if text.is_empty() {
                return Ok(resp(400, "Comment text is required".into()));
            }
            let author = kb::sanitize_author(body.get("author").and_then(|v| v.as_str()).unwrap_or("anon"));
            let ts = iso_now();
            let name = format!("{ts}__{author}");
            let cdir = subdir(&tickets, id, false).await?;
            let comments = subdir(&cdir, "comments", true).await?;
            write_text(&comments, &format!("{name}.md"), &text).await?;
            tree_sync::push_text(&format!("tickets/{id}/comments/{name}.md"), &text);
            Ok(json_resp(&Comment { id: name, author, ts, text }))
        }

        _ => Ok(resp(404, "Not Found".into())),
    }
}

async fn read_comments(cdir: &FileSystemDirectoryHandle) -> Vec<Comment> {
    let comments = match subdir(cdir, "comments", false).await {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for fname in list(&comments).await {
        let Some(stem) = fname.strip_suffix(".md") else { continue };
        let Some((ts, author)) = kb::parse_comment_name(stem) else { continue };
        let text = read_text(&comments, &fname).await.unwrap_or_default().trim_end().to_string();
        out.push(Comment { id: stem.to_string(), author, ts, text });
    }
    out.sort_by(|a, b| a.ts.cmp(&b.ts));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// **The kanban server, in the browser:** drive the same JSON API the SolidJS
    /// UI uses — create/read/patch/comment — entirely through `kanbanHandle` over
    /// OPFS, no localhost process. Each call reloads from OPFS, so a card created
    /// in one call being visible in the next proves serverless persistence.
    #[wasm_bindgen_test]
    async fn kanban_handler_runs_in_browser_over_opfs() {
        let created = unpack(
            handle("POST".into(), "/api/cards".into(), r#"{"title":"made in browser","column":"Doing"}"#.into()).await,
        );
        assert_eq!(created.0, 200);
        let id = json_str(&created.1, "id");
        assert!(id.starts_with("tk_"));

        // A separate call (fresh OPFS read) sees the new card.
        let board = unpack(handle("GET".into(), "/api/board".into(), String::new()).await);
        assert!(board.1.contains("made in browser"), "board: {}", board.1);

        // Patch + comment, then detail reflects both — all serverless.
        handle("PATCH".into(), format!("/api/cards/{id}"), r#"{"done":true,"description":"no server!"}"#.into()).await;
        handle("POST".into(), format!("/api/cards/{id}/comments"), r#"{"author":"Claude","text":"hello"}"#.into()).await;
        let detail = unpack(handle("GET".into(), format!("/api/cards/{id}/detail"), String::new()).await);
        assert!(detail.1.contains("no server!"), "detail: {}", detail.1);
        assert!(detail.1.contains("hello"), "detail: {}", detail.1);
    }

    fn unpack(v: JsValue) -> (u16, String) {
        let status = js_sys::Reflect::get(&v, &"status".into()).unwrap().as_f64().unwrap() as u16;
        let body = js_sys::Reflect::get(&v, &"body".into()).unwrap().as_string().unwrap();
        (status, body)
    }

    fn json_str(json: &str, key: &str) -> String {
        let v = js_sys::JSON::parse(json).unwrap();
        js_sys::Reflect::get(&v, &key.into()).unwrap().as_string().unwrap()
    }
}
