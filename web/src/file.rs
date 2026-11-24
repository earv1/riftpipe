//! The generic, app-agnostic **file API** riftpipe exposes to JS: read, write,
//! and list files on the synced OPFS tree. This is the library's honest surface
//! — a shared file tree that syncs — the same shape the native CLI presents as
//! real files. An app builds its domain model (a board, a wiki, …) on top in
//! plain JS; the library never knows the layout.
//!
//! A `writeFile` both persists to OPFS *and* pushes the change to peers (the
//! text-vs-LWW channel is the library's single classification rule, so a browser
//! write and a CLI edit of the same path are carried identically). A remote
//! merge lands in OPFS and fires a `riftpipe:file-changed` CustomEvent on
//! `window` (detail: the path), so JS can `watch` the tree per path.

use wasm_bindgen::prelude::*;

/// Read a file at `path` (e.g. `tickets/abc/card.md`) as text — `null`/undefined
/// if it doesn't exist.
#[wasm_bindgen(js_name = readFile)]
pub async fn read_file(path: String) -> Option<String> {
    crate::opfs::read_path(&path).await
}

/// Write `content` to `path` on the synced tree: persist to OPFS **and** push the
/// change to peers. Creates parent directories as needed.
#[wasm_bindgen(js_name = writeFile)]
pub async fn write_file(path: String, content: String) -> Result<(), JsValue> {
    crate::opfs::write_path(&path, content.as_bytes()).await?;
    crate::tree_sync::push(&path, content.as_bytes());
    Ok(())
}

/// Entry names directly under directory `path` (`""` = the tree root), as a JS
/// array of strings. Empty if the directory doesn't exist.
#[wasm_bindgen(js_name = listDir)]
pub async fn list_dir(path: String) -> Vec<String> {
    crate::opfs::list_path(&path).await
}

/// An opaque id (hex) of the board the file API currently addresses. It changes
/// when the page switches boards, so a deferred write (e.g. a debounced history
/// event) can tell it would now land on a different board.
#[wasm_bindgen(js_name = activeBoard)]
pub fn active_board() -> String {
    crate::opfs::active_board().iter().map(|b| format!("{b:02x}")).collect()
}
