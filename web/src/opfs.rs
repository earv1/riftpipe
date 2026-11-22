//! OPFS (Origin Private File System) helpers — the browser's private, serverless
//! filesystem, wrapped as a small path/tree API. App-agnostic: any file-based app
//! (a folder of documents) persists and syncs through these.
//!
//! The synced tree is **scoped per board**: board `topic`'s files live under
//! `.boards/<hex topic>/` (dot-prefixed, so never synced itself). OPFS is one
//! per origin but trust is per board, so one shared tree would let a claimed
//! board's files be primed or pushed into whatever other board this browser
//! opens next — scoping makes that impossible by construction, in any number of
//! tabs. The path API ([`read_path`] / [`write_path`] / [`list_path`]) addresses
//! the active board's tree.

use std::cell::RefCell;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemRemoveOptions, FileSystemWritableFileStream,
};

/// Parent of every board's tree: `.boards/<hex topic>/`.
const BOARDS_DIR: &str = ".boards";

thread_local! {
    /// The board (topic) the path API is scoped to — set on connect. Unset before
    /// the first connect: then it's the board this page will connect to
    /// ([`crate::tree_sync::page_board_topic`]), so pre-connect reads see the
    /// same tree.
    static ACTIVE_BOARD: RefCell<Option<[u8; 32]>> = const { RefCell::new(None) };
}

/// Scope the path API to board `topic` (on connect).
pub(crate) fn set_active_board(topic: [u8; 32]) {
    ACTIVE_BOARD.with(|b| *b.borrow_mut() = Some(topic));
}

/// The board the path API currently addresses.
pub(crate) fn active_board() -> [u8; 32] {
    ACTIVE_BOARD.with(|b| *b.borrow()).unwrap_or_else(crate::tree_sync::page_board_topic)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The root of board `topic`'s synced tree (created on demand).
pub async fn board_root(topic: &[u8; 32]) -> Result<FileSystemDirectoryHandle, JsValue> {
    let boards = subdir(&opfs_root().await?, BOARDS_DIR, true).await?;
    subdir(&boards, &hex(topic), true).await
}

/// The OPFS root directory for this origin.
pub async fn opfs_root() -> Result<FileSystemDirectoryHandle, JsValue> {
    let nav = web_sys::window().ok_or_else(|| JsValue::from_str("no window"))?.navigator();
    Ok(JsFuture::from(nav.storage().get_directory()).await?.unchecked_into())
}

/// A child directory handle (optionally created).
pub async fn subdir(parent: &FileSystemDirectoryHandle, name: &str, create: bool) -> Result<FileSystemDirectoryHandle, JsValue> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(create);
    Ok(JsFuture::from(parent.get_directory_handle_with_options(name, &opts)).await?.unchecked_into())
}

/// Read a file in `dir` as text, or `None` if it doesn't exist (or isn't UTF-8).
pub async fn read_text(dir: &FileSystemDirectoryHandle, name: &str) -> Option<String> {
    let handle: FileSystemFileHandle =
        JsFuture::from(dir.get_file_handle(name)).await.ok()?.unchecked_into();
    let file: File = JsFuture::from(handle.get_file()).await.ok()?.unchecked_into();
    let buf = JsFuture::from(file.array_buffer()).await.ok()?;
    String::from_utf8(js_sys::Uint8Array::new(&buf).to_vec()).ok()
}

/// Write text to a file in `dir` (created if absent).
pub async fn write_text(dir: &FileSystemDirectoryHandle, name: &str, content: &str) -> Result<(), JsValue> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(true);
    let handle: FileSystemFileHandle =
        JsFuture::from(dir.get_file_handle_with_options(name, &opts)).await?.unchecked_into();
    let w: FileSystemWritableFileStream =
        JsFuture::from(handle.create_writable()).await?.unchecked_into();
    JsFuture::from(w.write_with_u8_array(content.as_bytes())?).await?;
    JsFuture::from(w.close()).await?;
    Ok(())
}

/// Write bytes to a path like `notes/<id>/doc.md` on the active board's tree,
/// creating dirs as needed.
pub async fn write_path(path: &str, bytes: &[u8]) -> Result<(), JsValue> {
    write_path_in(&active_board(), path, bytes).await
}

/// Write bytes to `path` on board `topic`'s tree — the sync layer lands a peer's
/// merged file in the board it arrived on, even if the page has since switched.
pub async fn write_path_in(topic: &[u8; 32], path: &str, bytes: &[u8]) -> Result<(), JsValue> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let Some((file, dirs)) = parts.split_last() else { return Ok(()) };
    let mut dir = board_root(topic).await?;
    for d in dirs {
        dir = subdir(&dir, d, true).await?;
    }
    write_text(&dir, file, &String::from_utf8_lossy(bytes)).await
}

/// Read an OPFS file at a nested `path` like `tickets/<id>/card.md` as text, or
/// `None` if it doesn't exist (or isn't UTF-8). The path-addressed read that the
/// generic file API (`readFile`) exposes to JS.
pub async fn read_path(path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let (file, dirs) = parts.split_last()?;
    let mut dir = board_root(&active_board()).await.ok()?;
    for d in dirs {
        dir = subdir(&dir, d, false).await.ok()?;
    }
    read_text(&dir, file).await
}

/// Entry names directly under a nested directory `path` (`""` = root). Empty if
/// the directory doesn't exist. Powers the generic `listDir` JS export.
pub async fn list_path(path: &str) -> Vec<String> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let Ok(mut dir) = board_root(&active_board()).await else { return Vec::new() };
    for d in parts {
        match subdir(&dir, d, false).await {
            Ok(sub) => dir = sub,
            Err(_) => return Vec::new(),
        }
    }
    list(&dir).await
}

/// Entry names in a directory (drives the OPFS `keys()` async iterator).
pub async fn list(dir: &FileSystemDirectoryHandle) -> Vec<String> {
    let mut names = Vec::new();
    let iter = dir.keys();
    while let Ok(promise) = iter.next() {
        let Ok(res) = JsFuture::from(promise).await else { break };
        let done = js_sys::Reflect::get(&res, &"done".into()).ok().and_then(|v| v.as_bool()).unwrap_or(true);
        if done {
            break;
        }
        if let Some(name) = js_sys::Reflect::get(&res, &"value".into()).ok().and_then(|v| v.as_string()) {
            names.push(name);
        }
    }
    names
}

/// Push **every** file of board `topic`'s tree into the active sync, so a peer
/// we connect to merges with our pre-existing state — not just live edits. This
/// is **folder-generic**: `.md` files sync as text CRDTs, everything else as LWW,
/// with no knowledge of any app's layout. Only that board's own tree is read, so
/// another board's files (claimed or not) never reach this board's swarm.
pub async fn prime_all(topic: [u8; 32]) {
    let Ok(root) = board_root(&topic).await else { return };
    for (path, text) in tree_files(root).await {
        // The text-vs-LWW channel comes from the library's single
        // classification rule, inside `push`.
        crate::tree_sync::push(&path, text.as_bytes());
    }
}

/// Every syncable file under `root` as `(path, text)`. Iterative DFS (a name is
/// a file if it reads as one, else a subdirectory to descend into).
/// Dot-prefixed entries are machine-local (`.site`, editor droppings) and must
/// never cross the wire — the native tree driver's `hidden()` rule.
pub async fn tree_files(root: FileSystemDirectoryHandle) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut stack = vec![(root, String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        for name in list(&dir).await {
            if name.starts_with('.') {
                continue;
            }
            let path = if prefix.is_empty() { name.clone() } else { format!("{prefix}/{name}") };
            if let Some(text) = read_text(&dir, &name).await {
                out.push((path, text));
            } else if let Ok(sub) = subdir(&dir, &name, false).await {
                stack.push((sub, path));
            }
        }
    }
    out
}

/// One-time upgrade from the pre-scoping layout, where the synced tree sat at
/// the OPFS root: move the root's (non-dot) text files and directories into
/// board `into`'s tree, overwriting (the root copy is the real data; a seed may
/// have been written before this ran). Binary root files (`RiftDoc::persist`)
/// stay. Returns whether anything moved.
pub async fn adopt_legacy_root(into: &[u8; 32]) -> bool {
    let Ok(root) = opfs_root().await else { return false };
    let mut moved = false;
    for name in list(&root).await {
        if name.starts_with('.') {
            continue;
        }
        if read_text(&root, &name).await.is_none() && subdir(&root, &name, false).await.is_err() {
            continue; // binary file: not part of the synced tree
        }
        let Ok(sub) = subdir(&root, &name, false).await else {
            // A text file at the root.
            if let Some(text) = read_text(&root, &name).await {
                if write_path_in(into, &name, text.as_bytes()).await.is_ok() {
                    let _ = JsFuture::from(root.remove_entry(&name)).await;
                    moved = true;
                }
            }
            continue;
        };
        for (path, text) in tree_files(sub).await {
            let _ = write_path_in(into, &format!("{name}/{path}"), text.as_bytes()).await;
        }
        let opts = FileSystemRemoveOptions::new();
        opts.set_recursive(true);
        let _ = JsFuture::from(root.remove_entry_with_options(&name, &opts)).await;
        moved = true;
    }
    moved
}

/// Write `bytes` to OPFS file `name` at the root (created if absent).
pub async fn opfs_write(name: &str, bytes: &[u8]) -> Result<(), JsValue> {
    let dir = opfs_root().await?;
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(true);
    let handle = JsFuture::from(dir.get_file_handle_with_options(name, &opts))
        .await?
        .unchecked_into::<FileSystemFileHandle>();
    let writable = JsFuture::from(handle.create_writable())
        .await?
        .unchecked_into::<FileSystemWritableFileStream>();
    JsFuture::from(writable.write_with_u8_array(bytes)?).await?;
    JsFuture::from(writable.close()).await?;
    Ok(())
}

/// Read OPFS file `name` at the root, or `None` if it doesn't exist.
pub async fn opfs_read(name: &str) -> Result<Option<Vec<u8>>, JsValue> {
    let dir = opfs_root().await?;
    let handle = match JsFuture::from(dir.get_file_handle(name)).await {
        Ok(h) => h.unchecked_into::<FileSystemFileHandle>(),
        Err(_) => return Ok(None), // not found
    };
    let file = JsFuture::from(handle.get_file())
        .await?
        .unchecked_into::<File>();
    let buf = JsFuture::from(file.array_buffer()).await?;
    Ok(Some(js_sys::Uint8Array::new(&buf).to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    /// Each board's tree is its own: files written on (claimed) board A are not
    /// readable, listable, or primed on board B — and a merge that lands for A
    /// while B is active still goes to A.
    #[wasm_bindgen_test]
    async fn board_trees_are_isolated() {
        let (a, b) = (riftpipe_core::crypto::generate_secret(), riftpipe_core::crypto::generate_secret());
        set_active_board(a);
        write_path("tickets/x/card.md", b"board A only").await.unwrap();
        assert_eq!(read_path("tickets/x/card.md").await.as_deref(), Some("board A only"));

        set_active_board(b);
        assert!(read_path("tickets/x/card.md").await.is_none(), "B can't read A's file");
        assert!(list_path("").await.is_empty(), "B's tree starts empty");
        write_path_in(&a, "late.md", b"merged for A").await.unwrap();
        assert!(tree_files(board_root(&b).await.unwrap()).await.is_empty(), "priming B pushes none of A's files");

        let mut on_a: Vec<String> = tree_files(board_root(&a).await.unwrap()).await.into_iter().map(|(p, _)| p).collect();
        on_a.sort();
        assert_eq!(on_a, vec!["late.md".to_string(), "tickets/x/card.md".to_string()]);
    }

    /// A pre-scoping tree at the OPFS root moves into the board it's adopted by.
    #[wasm_bindgen_test]
    async fn legacy_root_tree_is_adopted_once() {
        let c = riftpipe_core::crypto::generate_secret();
        let root = opfs_root().await.unwrap();
        write_text(&subdir(&root, "legacytickets", true).await.unwrap(), "card.md", "old card").await.unwrap();
        assert!(adopt_legacy_root(&c).await, "the root tree moved");
        assert!(subdir(&root, "legacytickets", false).await.is_err(), "gone from the root");
        set_active_board(c);
        assert_eq!(read_path("legacytickets/card.md").await.as_deref(), Some("old card"));
    }
}
