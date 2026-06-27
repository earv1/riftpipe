//! The **kanban "server" running in the browser** — the same JSON file-API the
//! SolidJS UI calls (`/api/board`, `/api/cards/...`), but handled by wasm over
//! **OPFS** instead of a localhost process. `kanbanHandle(method, path, body)` is
//! the entry point; a thin shim in `api.ts` routes the app's `fetch('/api/*')`
//! here, so the components are unchanged and there is **no local server**.
//!
//! v1 storage is the whole board in one OPFS file (`board.json`) — simple, and
//! enough to run the app single-browser. The per-card-file layout + `RiftDoc`
//! sync over the signaling/WebRTC link is the next layer (multi-peer).

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use crate::{opfs_read, opfs_write};

const BOARD_FILE: &str = "board.json";

#[derive(Serialize, Deserialize, Clone)]
struct Card {
    id: String,
    title: String,
    column: String,
    position: i64,
    done: bool,
    #[serde(default)]
    description: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Comment {
    id: String,
    author: String,
    ts: String,
    text: String,
}

#[derive(Serialize, Deserialize)]
struct Board {
    title: String,
    columns: Vec<String>,
    cards: Vec<Card>,
    #[serde(default)]
    comments: std::collections::HashMap<String, Vec<Comment>>,
}

impl Default for Board {
    fn default() -> Self {
        Board {
            title: "My Board".into(),
            columns: vec!["Todo".into(), "Doing".into(), "Done".into()],
            cards: Vec::new(),
            comments: Default::default(),
        }
    }
}

async fn load_board() -> Board {
    match opfs_read(BOARD_FILE).await {
        Ok(Some(bytes)) => serde_json::from_slice(&bytes).unwrap_or_default(),
        _ => Board::default(),
    }
}

async fn save_board(board: &Board) {
    if let Ok(bytes) = serde_json::to_vec(board) {
        let _ = opfs_write(BOARD_FILE, &bytes).await;
    }
}

fn new_id() -> String {
    let r = (js_sys::Math::random() * 4_294_967_296.0) as u32;
    format!("tk_{r:08x}")
}

fn iso_now() -> String {
    js_sys::Date::new_0().to_iso_string().as_string().unwrap_or_default()
}

fn sanitize_author(author: &str) -> String {
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
    if slug.is_empty() { "anon".into() } else { slug }
}

/// `{status, body}` the JS shim turns back into a `Response`.
fn resp(status: u16, body: String) -> JsValue {
    let o = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&o, &"status".into(), &(status as f64).into());
    let _ = js_sys::Reflect::set(&o, &"body".into(), &body.into());
    o.into()
}

fn json_resp<T: Serialize>(value: &T) -> JsValue {
    resp(200, serde_json::to_string(value).unwrap_or_else(|_| "null".into()))
}

/// Board-list view of a card (no description), matching the native server.
fn card_summary(c: &Card) -> serde_json::Value {
    serde_json::json!({
        "id": c.id, "title": c.title, "column": c.column,
        "position": c.position, "done": c.done,
    })
}

/// Handle one API request entirely in the browser. `method` is GET/POST/PATCH,
/// `path` is e.g. `/api/cards/tk_x/detail`, `body` is the request JSON (or "").
#[wasm_bindgen(js_name = kanbanHandle)]
pub async fn handle(method: String, path: String, body: String) -> JsValue {
    let clean = path.split('?').next().unwrap_or("");
    let segs: Vec<&str> = clean.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    let body: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    let mut board = load_board().await;

    match (method.as_str(), segs.as_slice()) {
        ("GET", ["api", "board"]) => {
            let cards: Vec<_> = board.cards.iter().map(card_summary).collect();
            json_resp(&serde_json::json!({
                "title": board.title, "columns": board.columns, "cards": cards,
            }))
        }

        ("GET", ["api", "cards", id]) => match board.cards.iter().find(|c| c.id == *id) {
            Some(c) => json_resp(&card_summary(c)),
            None => resp(404, "Not Found".into()),
        },

        ("GET", ["api", "cards", id, "detail"]) => match board.cards.iter().find(|c| c.id == *id) {
            Some(c) => {
                let comments = board.comments.get(*id).cloned().unwrap_or_default();
                json_resp(&serde_json::json!({
                    "id": c.id, "title": c.title, "column": c.column,
                    "position": c.position, "done": c.done,
                    "description": c.description, "comments": comments,
                }))
            }
            None => resp(404, "Not Found".into()),
        },

        ("POST", ["api", "cards"]) => {
            let column = body.get("column").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| board.columns.first().cloned().unwrap_or_else(|| "Todo".into()));
            let title = body.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let id = new_id();
            let max_pos = board.cards.iter().filter(|c| c.column == column).map(|c| c.position).max().unwrap_or(-1);
            let card = Card {
                id: id.clone(),
                title: if title.is_empty() { id.clone() } else { title },
                column,
                position: max_pos + 1,
                done: false,
                description: String::new(),
            };
            board.cards.push(card.clone());
            save_board(&board).await;
            json_resp(&card_summary(&card))
        }

        ("PATCH", ["api", "cards", id]) => {
            let Some(card) = board.cards.iter_mut().find(|c| c.id == *id) else {
                return resp(404, "Not Found".into());
            };
            if let Some(c) = body.get("column").and_then(|v| v.as_str()) {
                card.column = c.to_string();
            }
            if let Some(p) = body.get("position").and_then(|v| v.as_i64()) {
                card.position = p;
            }
            if let Some(d) = body.get("done").and_then(|v| v.as_bool()) {
                card.done = d;
            }
            if let Some(t) = body.get("title").and_then(|v| v.as_str()) {
                card.title = if t.is_empty() { id.to_string() } else { t.to_string() };
            }
            if let Some(desc) = body.get("description").and_then(|v| v.as_str()) {
                card.description = desc.to_string();
            }
            let summary = card_summary(card);
            save_board(&board).await;
            json_resp(&summary)
        }

        ("POST", ["api", "cards", id, "comments"]) => {
            if !board.cards.iter().any(|c| c.id == *id) {
                return resp(404, "Not Found".into());
            }
            let text = body.get("text").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
            if text.is_empty() {
                return resp(400, "Comment text is required".into());
            }
            let author = sanitize_author(body.get("author").and_then(|v| v.as_str()).unwrap_or("anon"));
            let ts = iso_now();
            let comment = Comment {
                id: format!("{ts}__{author}"),
                author,
                ts,
                text,
            };
            board.comments.entry(id.to_string()).or_default().push(comment.clone());
            save_board(&board).await;
            json_resp(&comment)
        }

        _ => resp(404, "Not Found".into()),
    }
}
