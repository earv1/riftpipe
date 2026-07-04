//! OPFS (Origin Private File System) helpers — the browser's private, serverless
//! filesystem, wrapped as a small path/tree API. App-agnostic: any file-based app
//! (a folder of documents) persists and syncs through these.

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

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

/// Write bytes to an OPFS path like `notes/<id>/doc.md`, creating dirs as
/// needed. Used by the sync layer to land a peer's merged file.
pub async fn write_path(path: &str, bytes: &[u8]) -> Result<(), JsValue> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let Some((file, dirs)) = parts.split_last() else { return Ok(()) };
    let mut dir = opfs_root().await?;
    for d in dirs {
        dir = subdir(&dir, d, true).await?;
    }
    write_text(&dir, file, &String::from_utf8_lossy(bytes)).await
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

/// Push **every** existing OPFS file (whole tree) into the active sync, so a peer
/// we connect to merges with our pre-existing state — not just live edits. This is
/// **folder-generic**: `.md` files sync as text CRDTs, everything else as LWW, with
/// no knowledge of any app's layout. Any file-based app gets mesh sync for free.
/// Distinct paths union; same-path files resolve by origin in `core::sync`.
pub async fn prime_all() {
    let Ok(root) = opfs_root().await else { return };
    // Iterative DFS over the OPFS tree (a name is a file if it reads as one, else a
    // subdirectory to descend into).
    let mut stack = vec![(root, String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        for name in list(&dir).await {
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            if let Some(text) = read_text(&dir, &name).await {
                if name.ends_with(".md") {
                    crate::tree_sync::push_text(&path, &text);
                } else {
                    crate::tree_sync::push_lww(&path, text.as_bytes());
                }
            } else if let Ok(sub) = subdir(&dir, &name, false).await {
                stack.push((sub, path));
            }
        }
    }
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
