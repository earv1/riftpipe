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

use riftpipe_core::sync::{is_text_path, DurablePath, Restored, Syncer};

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

/// Parent of every board's tree: `.boards/<hex topic>/`.
const BOARDS_DIR: &str = ".boards";
/// Never-synced dot-dir holding one durable sync record per path (see
/// [`queue_record`] / [`restore_syncer`]) — a dot-dir, so `tree_files` already
/// skips it.
const SYNC_DIR: &str = ".riftpipe-sync";

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

/// Read a file in `dir` as raw bytes, or `None` if it doesn't exist.
pub async fn read_bytes(dir: &FileSystemDirectoryHandle, name: &str) -> Option<Vec<u8>> {
    let handle: FileSystemFileHandle =
        JsFuture::from(dir.get_file_handle(name)).await.ok()?.unchecked_into();
    let file: File = JsFuture::from(handle.get_file()).await.ok()?.unchecked_into();
    let buf = JsFuture::from(file.array_buffer()).await.ok()?;
    Some(js_sys::Uint8Array::new(&buf).to_vec())
}

/// Write text to a file in `dir` (created if absent).
pub async fn write_text(dir: &FileSystemDirectoryHandle, name: &str, content: &str) -> Result<(), JsValue> {
    write_bytes(dir, name, content.as_bytes()).await
}

/// Write bytes to a file in `dir` (created if absent).
pub async fn write_bytes(dir: &FileSystemDirectoryHandle, name: &str, bytes: &[u8]) -> Result<(), JsValue> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(true);
    let handle: FileSystemFileHandle =
        JsFuture::from(dir.get_file_handle_with_options(name, &opts)).await?.unchecked_into();
    let w: FileSystemWritableFileStream =
        JsFuture::from(handle.create_writable()).await?.unchecked_into();
    JsFuture::from(w.write_with_u8_array(bytes)?).await?;
    JsFuture::from(w.close()).await?;
    Ok(())
}

/// Write bytes to a path like `notes/<id>/doc.md` on the active board's tree,
/// creating dirs as needed. An app (local) write: it first lets any queued merged
/// write of the same path land, so the older merge can't overwrite it afterwards.
pub async fn write_path(path: &str, bytes: &[u8]) -> Result<(), JsValue> {
    let topic = active_board();
    writes_settled(topic, Some(path)).await;
    write_path_in(&topic, path, bytes).await
}

/// Write bytes to `path` on board `topic`'s tree — the sync layer lands a peer's
/// merged file in the board it arrived on, even if the page has since switched.
pub async fn write_path_in(topic: &[u8; 32], path: &str, bytes: &[u8]) -> Result<(), JsValue> {
    write_path_under(&board_root(topic).await?, path, bytes).await
}

/// Write bytes to `path` under directory `root`, creating dirs as needed.
async fn write_path_under(root: &FileSystemDirectoryHandle, path: &str, bytes: &[u8]) -> Result<(), JsValue> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let Some((file, dirs)) = parts.split_last() else { return Ok(()) };
    let mut dir = root.clone();
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

/// Never-synced marker in a board's tree: the board is claimed.
const CLAIMED_MARKER: &str = ".claimed";

/// Record next to board `topic`'s files that it is claimed — so a browser that
/// loses its localStorage (trust log) but keeps OPFS still treats it as claimed.
pub async fn mark_board_claimed(topic: &[u8; 32]) {
    if let Ok(root) = board_root(topic).await {
        let _ = write_text(&root, CLAIMED_MARKER, "claimed\n").await;
    }
}

/// Is board `topic` marked claimed ([`mark_board_claimed`])?
pub async fn board_claimed(topic: &[u8; 32]) -> bool {
    match board_root(topic).await {
        Ok(root) => read_text(&root, CLAIMED_MARKER).await.is_some(),
        Err(_) => false,
    }
}

/// Push **every** file of board `topic`'s tree into the active sync, so a peer
/// we connect to merges with our pre-existing state — not just live edits. This
/// is **folder-generic**: `.md` files sync as text CRDTs, everything else as LWW,
/// with no knowledge of any app's layout. Only that board's own tree is read, so
/// another board's files (claimed or not) never reach this board's swarm.
/// Kept only for the legacy WebRTC `connect_and_sync` path — `iroh_connect` uses
/// [`restore_syncer`] instead, which resumes durable history/LWW versions.
pub async fn prime_all(topic: [u8; 32]) {
    let Ok(root) = board_root(&topic).await else { return };
    for (path, text) in tree_files(root).await {
        // The text-vs-LWW channel comes from the library's single
        // classification rule, inside `push`.
        crate::tree_sync::push(&path, text.as_bytes());
    }
}

/// One path's not-yet-written durable state: the merged file bytes and/or its
/// record, plus what to run once the file has landed.
#[derive(Default)]
struct PendingWrite {
    file: Option<Vec<u8>>,
    record: Option<Vec<u8>>,
    after: Vec<Box<dyn FnOnce()>>,
}

/// Every durable write of the mesh path — merged files AND their records — goes
/// through ONE serialized queue, latest-wins per `(board, path)`, and each entry
/// writes its file BEFORE its record. So a record never lands ahead of the file it
/// describes: a reload mid-write can only see the new file with the old record
/// (restore treats it as a local edit = the merge again: duplication, never loss),
/// never the new record with the old file (restore would author a revert of the
/// merge and broadcast it mesh-wide). Rapid merges to one file can't land out of
/// order either.
#[derive(Default)]
struct WriteQueue {
    pending: std::collections::BTreeMap<([u8; 32], String), PendingWrite>,
    /// The entry being written right now (already popped from `pending`).
    in_flight: Option<([u8; 32], String)>,
    draining: bool,
}
thread_local! {
    static WRITES: RefCell<WriteQueue> = RefCell::new(WriteQueue::default());
}

/// Add to `key`'s pending entry via `f` and make sure the drain runs.
fn enqueue(key: ([u8; 32], String), f: impl FnOnce(&mut PendingWrite, bool)) {
    let spawn = WRITES.with(|q| {
        let mut q = q.borrow_mut();
        let in_flight = q.in_flight.as_ref() == Some(&key);
        f(q.pending.entry(key).or_default(), in_flight);
        !std::mem::replace(&mut q.draining, true)
    });
    if spawn {
        wasm_bindgen_futures::spawn_local(drain_writes());
    }
}

/// Queue a merged file for board `topic` (replacing any not-yet-written bytes for
/// that path); `after` runs once it has landed (change events, UI nudges). Queue
/// its record with [`queue_record`] right after, so the pair is written in order.
pub(crate) fn queue_merged_file(topic: [u8; 32], path: String, bytes: Vec<u8>, after: impl FnOnce() + 'static) {
    enqueue((topic, path), |e, _| {
        e.file = Some(bytes);
        e.after.push(Box::new(after));
    });
}

/// Queue `rec` for board `topic`: the record of a merged file queued just before
/// it, or of a file restore found on disk.
pub(crate) fn queue_record(topic: [u8; 32], rec: &DurablePath) {
    let bytes = postcard::to_allocvec(rec).unwrap_or_default();
    enqueue((topic, rec.path.clone()), |e, _| e.record = Some(bytes));
}

/// Queue `rec` after a LOCAL write of `written` to its path (the app wrote the
/// file itself, then pushed). If a merged file for that path is still queued or
/// being written, it is older than this write — re-queue `written` so the file
/// ends as what the record describes.
pub(crate) fn queue_local_record(topic: [u8; 32], rec: &DurablePath, written: &[u8]) {
    let bytes = postcard::to_allocvec(rec).unwrap_or_default();
    enqueue((topic, rec.path.clone()), |e, in_flight| {
        if in_flight || e.file.is_some() {
            e.file = Some(written.to_vec());
        }
        e.record = Some(bytes);
    });
}

/// Drain [`WRITES`] one entry at a time until it's empty: file, then record.
async fn drain_writes() {
    loop {
        let next = WRITES.with(|q| {
            let mut q = q.borrow_mut();
            let next = q.pending.pop_first();
            q.in_flight = next.as_ref().map(|(k, _)| k.clone());
            if next.is_none() {
                q.draining = false;
            }
            next
        });
        let Some(((topic, path), w)) = next else { break };
        if let Some(bytes) = &w.file {
            let _ = write_path_in(&topic, &path, bytes).await;
        }
        for f in w.after {
            f();
        }
        if let Some(rec) = &w.record {
            if let Ok(root) = board_root(&topic).await {
                if let Ok(dir) = subdir(&root, SYNC_DIR, true).await {
                    let _ = write_bytes(&dir, &format!("{}.rec", riftpipe_core::sync::record_name(&path)), rec).await;
                }
            }
        }
    }
}

/// Wait until no write for board `topic` (only for `path`, if given) is queued or
/// in flight — so a restore (e.g. a fast in-tab switch back to this board) reads
/// settled file/record pairs, and a local write lands after any older merge.
/// Bounded: a wedged OPFS write can't hang the caller forever.
async fn writes_settled(topic: [u8; 32], path: Option<&str>) {
    let hit = |k: &([u8; 32], String)| k.0 == topic && path.is_none_or(|p| k.1 == p);
    for _ in 0..1_000 {
        let busy = WRITES.with(|q| {
            let q = q.borrow();
            q.in_flight.as_ref().is_some_and(hit) || q.pending.keys().any(hit)
        });
        if !busy {
            return;
        }
        gloo_timers::future::TimeoutFuture::new(10).await;
    }
}

/// Wait for every queued write to land — tests only (production code never waits
/// on the write queue except [`restore_syncer`]'s per-board settle).
#[cfg(test)]
pub(crate) async fn flush_records() {
    for _ in 0..500 {
        let idle = WRITES.with(|q| {
            let q = q.borrow();
            !q.draining && q.pending.is_empty()
        });
        if idle {
            return;
        }
        gloo_timers::future::TimeoutFuture::new(10).await;
    }
}

/// Rebuild board `topic`'s Syncer from its durable records + OPFS files, BEFORE the
/// mesh starts: unchanged files resume their exact history (same CRDT origin, same LWW
/// version); files changed while closed become local edits on top; files without a
/// usable record are primed as new (the old `prime_all` behaviour); records of files
/// that are gone are deleted. Nothing is broadcast
/// — the NeighborUp catch-up ships the whole state.
pub async fn restore_syncer(topic: [u8; 32], agent: String) -> Syncer {
    let mut syncer = Syncer::new(agent);
    writes_settled(topic, None).await;
    let Ok(root) = board_root(&topic).await else { return syncer };
    let mut recs: std::collections::HashMap<String, DurablePath> = std::collections::HashMap::new();
    let records = subdir(&root, SYNC_DIR, false).await.ok();
    if let Some(dir) = &records {
        for name in list(dir).await {
            if !name.ends_with(".rec") {
                continue;
            }
            let Some(bytes) = read_bytes(dir, &name).await else { continue };
            if let Ok(rec) = postcard::from_bytes::<DurablePath>(&bytes) {
                recs.insert(rec.path.clone(), rec);
            }
        }
    }
    let now = js_sys::Date::now() as u64;
    for (path, text) in tree_files(root).await {
        match recs.remove(&path).map(|rec| syncer.restore(rec, Some(text.as_bytes()), now)) {
            Some(Restored::Clean) => {}
            Some(Restored::Changed(_)) => {
                if let Some(r) = syncer.durable(&path) {
                    queue_record(topic, &r);
                }
            }
            Some(Restored::Unusable) | None => {
                if is_text_path(&path) {
                    let _ = syncer.local_text(&path, &text);
                } else {
                    let _ = syncer.local_lww(&path, text.into_bytes(), now);
                }
                if let Some(r) = syncer.durable(&path) {
                    queue_record(topic, &r);
                }
            }
        }
    }
    // Records whose file is gone (deleted while closed): drop them, as the native
    // `TreePeer::seed` does, so they don't pile up.
    if let Some(dir) = &records {
        for path in recs.into_keys() {
            let name = format!("{}.rec", riftpipe_core::sync::record_name(&path));
            let _ = JsFuture::from(dir.remove_entry(&name)).await;
        }
    }
    syncer
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

/// Root-level file extensions that belong to a pre-scoping synced tree. Other
/// root files — e.g. `RiftDoc::persist(name)` documents, which live at the OPFS
/// root by design — are never part of a board and stay where they are.
const LEGACY_ROOT_FILE_EXTS: &[&str] = &["md", "markdown", "txt", "toml", "json", "jsonl", "yaml", "yml", "csv"];

/// The OPFS root's entries that made up the pre-scoping synced tree: its
/// (non-dot) directories, and root files with a known tree extension.
async fn legacy_root_entries(root: &FileSystemDirectoryHandle) -> Vec<String> {
    let mut names = Vec::new();
    for name in list(root).await {
        if name.starts_with('.') {
            continue;
        }
        let tree_file = name.rsplit_once('.').is_some_and(|(_, ext)| LEGACY_ROOT_FILE_EXTS.contains(&ext));
        if tree_file || subdir(root, &name, false).await.is_ok() {
            names.push(name);
        }
    }
    names
}

/// One-time upgrade from the pre-scoping layout, where the synced tree sat at
/// the OPFS root: move it ([`legacy_root_entries`]) into board `into`'s tree,
/// overwriting (the root copy is the real data; a seed may have been written
/// before this ran). Returns whether anything moved.
pub async fn adopt_legacy_root(into: &[u8; 32]) -> bool {
    let (Ok(root), Ok(dest)) = (opfs_root().await, board_root(into).await) else { return false };
    let names = legacy_root_entries(&root).await;
    move_entries(&root, names, &dest).await
}

/// Where a pre-scoping root tree goes when it can't safely be adopted by a
/// board: dot-prefixed, so it is never synced, primed, adopted or wiped.
pub const LEGACY_QUARANTINE_DIR: &str = ".legacy-root";

/// Does the OPFS root still hold a pre-scoping synced tree?
pub async fn has_legacy_root() -> bool {
    match opfs_root().await {
        Ok(root) => !legacy_root_entries(&root).await.is_empty(),
        Err(_) => false,
    }
}

/// Move the pre-scoping root tree into [`LEGACY_QUARANTINE_DIR`], kept as is
/// (same paths) and out of every board. Returns whether anything moved.
pub async fn quarantine_legacy_root() -> bool {
    let Ok(root) = opfs_root().await else { return false };
    let Ok(dest) = subdir(&root, LEGACY_QUARANTINE_DIR, true).await else { return false };
    let names = legacy_root_entries(&root).await;
    move_entries(&root, names, &dest).await
}

/// Move the entries `names` of `src` (files and whole directories) to the same
/// paths under `dest`, never losing data: each text file is removed only after
/// its copy was written; dot-prefixed and non-UTF-8 files are left where they
/// are; a directory is then removed only if it ended up empty (non-recursive —
/// a failure just leaves it). Returns whether any file moved.
async fn move_entries(src: &FileSystemDirectoryHandle, names: Vec<String>, dest: &FileSystemDirectoryHandle) -> bool {
    let mut moved = false;
    // (parent, name) of every directory visited, parents before children.
    let mut dirs: Vec<(FileSystemDirectoryHandle, String)> = Vec::new();
    let mut stack: Vec<(FileSystemDirectoryHandle, String, Vec<String>)> = vec![(src.clone(), String::new(), names)];
    while let Some((dir, prefix, entries)) = stack.pop() {
        for name in entries {
            if name.starts_with('.') {
                continue;
            }
            let path = if prefix.is_empty() { name.clone() } else { format!("{prefix}/{name}") };
            if let Some(text) = read_text(&dir, &name).await {
                if write_path_under(dest, &path, text.as_bytes()).await.is_ok()
                    && JsFuture::from(dir.remove_entry(&name)).await.is_ok()
                {
                    moved = true;
                }
            } else if let Ok(sub) = subdir(&dir, &name, false).await {
                let children = list(&sub).await;
                dirs.push((dir.clone(), name));
                stack.push((sub, path, children));
            }
        }
    }
    // Deepest first; a non-empty directory (kept files) fails to remove: fine.
    for (parent, name) in dirs.into_iter().rev() {
        let _ = JsFuture::from(parent.remove_entry(&name)).await;
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

    /// The claimed marker is per board and never part of the synced tree.
    #[wasm_bindgen_test]
    async fn claimed_marker_is_per_board_and_unsynced() {
        let (a, b) = (riftpipe_core::crypto::generate_secret(), riftpipe_core::crypto::generate_secret());
        assert!(!board_claimed(&a).await);
        mark_board_claimed(&a).await;
        assert!(board_claimed(&a).await && !board_claimed(&b).await);
        assert!(tree_files(board_root(&a).await.unwrap()).await.is_empty(), "the marker is never primed");
    }

    /// Moving the legacy root never loses data: a file is removed only after its
    /// copy is written, and files the move can't carry (dot-prefixed, non-UTF-8)
    /// stay put along with their directory.
    #[wasm_bindgen_test]
    async fn legacy_move_keeps_what_it_cannot_carry() {
        let c = riftpipe_core::crypto::generate_secret();
        let root = opfs_root().await.unwrap();
        let dir = subdir(&root, "legacykeep", true).await.unwrap();
        write_text(&dir, "card.md", "text card").await.unwrap();
        write_text(&dir, ".local", "machine-local").await.unwrap();
        write_bytes(&dir, "blob.bin", &[0xff, 0xfe, 0x00, 0x80]).await.unwrap();
        assert!(adopt_legacy_root(&c).await);

        set_active_board(c);
        assert_eq!(read_path("legacykeep/card.md").await.as_deref(), Some("text card"));
        let kept = subdir(&root, "legacykeep", false).await.expect("the directory stays: it isn't empty");
        let mut left = list(&kept).await;
        left.sort();
        assert_eq!(left, vec![".local".to_string(), "blob.bin".to_string()], "only the moved file left");
        let opts = web_sys::FileSystemRemoveOptions::new();
        opts.set_recursive(true);
        let _ = JsFuture::from(root.remove_entry_with_options("legacykeep", &opts)).await;
    }

    /// `RiftDoc::persist` documents live at the OPFS root by design: never
    /// adopted into a board, even when their bytes happen to be valid UTF-8.
    #[wasm_bindgen_test]
    async fn legacy_adoption_skips_riftdoc_root_files() {
        let c = riftpipe_core::crypto::generate_secret();
        opfs_write("riftdoc-notes", b"utf8 crdt bytes").await.unwrap();
        opfs_write("legacy-board.md", b"# board").await.unwrap();
        adopt_legacy_root(&c).await;
        assert_eq!(opfs_read("riftdoc-notes").await.unwrap().as_deref(), Some(&b"utf8 crdt bytes"[..]), "RiftDoc file stays");
        set_active_board(c);
        assert!(read_path("riftdoc-notes").await.is_none(), "not adopted");
        assert_eq!(read_path("legacy-board.md").await.as_deref(), Some("# board"), "a tree file is adopted");
    }

    /// A quarantined legacy root keeps its files, out of every board: never in a
    /// board tree, and never adopted afterwards.
    #[wasm_bindgen_test]
    async fn quarantined_legacy_root_stays_out_of_boards() {
        let c = riftpipe_core::crypto::generate_secret();
        let root = opfs_root().await.unwrap();
        write_text(&subdir(&root, "claimedtickets", true).await.unwrap(), "card.md", "secret card").await.unwrap();
        assert!(has_legacy_root().await);
        assert!(quarantine_legacy_root().await);
        assert!(!has_legacy_root().await, "nothing left to adopt");
        let q = subdir(&subdir(&root, LEGACY_QUARANTINE_DIR, false).await.unwrap(), "claimedtickets", false).await.unwrap();
        assert_eq!(read_text(&q, "card.md").await.as_deref(), Some("secret card"), "kept, same path");
        assert!(!adopt_legacy_root(&c).await, "a later adoption finds nothing");
        assert!(tree_files(board_root(&c).await.unwrap()).await.is_empty());
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

    /// The `TextDelta` origin carried for `path` in `msgs`.
    fn text_origin(msgs: &[riftpipe_core::sync::SyncMsg], path: &str) -> Option<(String, usize)> {
        msgs.iter()
            .find_map(|m| match m {
                riftpipe_core::sync::SyncMsg::TextDelta { path: p, origin, .. } if p == path => {
                    Some(origin.clone())
                }
                _ => None,
            })
            .expect("a TextDelta for the path")
    }

    /// The `Lww` version carried for `path` in `msgs`.
    fn lww_version(msgs: &[riftpipe_core::sync::SyncMsg], path: &str) -> u64 {
        msgs.iter()
            .find_map(|m| match m {
                riftpipe_core::sync::SyncMsg::Lww { path: p, version, .. } if p == path => {
                    Some(*version)
                }
                _ => None,
            })
            .expect("an Lww for the path")
    }

    /// A restart resumes the SAME CRDT history (not a fresh re-prime): the text
    /// origin and LWW version survive, and a file changed while closed becomes a
    /// local edit that still merges cleanly with the pre-restart history.
    #[wasm_bindgen_test]
    async fn restore_syncer_resumes_history_and_lww_versions() {
        let topic = riftpipe_core::crypto::generate_secret();
        write_path_in(&topic, "a.md", b"# one\n").await.unwrap();
        write_path_in(&topic, "m.toml", b"x = 1\n").await.unwrap();

        let mut s1 = Syncer::new("t1");
        s1.local_text("a.md", "# one\n");
        s1.local_lww("m.toml", b"x = 1\n".to_vec(), 100);
        queue_record(topic, &s1.durable("a.md").unwrap());
        queue_record(topic, &s1.durable("m.toml").unwrap());
        flush_records().await;

        let s2 = restore_syncer(topic, "t2".into()).await;
        let s1_full = s1.full_state();
        assert_eq!(text_origin(&s2.full_state(), "a.md"), text_origin(&s1_full, "a.md"));
        assert_eq!(lww_version(&s2.full_state(), "m.toml"), 100);

        write_path_in(&topic, "a.md", b"# two\n").await.unwrap();
        let s3 = restore_syncer(topic, "t3".into()).await;

        let mut c = Syncer::new("c");
        let mut persisted = None;
        for m in s1.full_state().into_iter().chain(s3.full_state()) {
            if let (Some((p, bytes)), _) = c.apply(m) {
                if p == "a.md" {
                    persisted = Some(bytes);
                }
            }
        }
        assert_eq!(persisted.as_deref(), Some(&b"# two\n"[..]));
        assert_eq!(text_origin(&c.full_state(), "a.md"), text_origin(&s1_full, "a.md"));
    }

    /// Two records queued for the same path before either write lands: only the
    /// latest survives on disk.
    #[wasm_bindgen_test]
    async fn record_queue_keeps_the_latest_per_path() {
        use riftpipe_core::sync::DurableKind;
        let topic = riftpipe_core::crypto::generate_secret();
        let rec_v1 = DurablePath { path: "p.md".into(), kind: DurableKind::Lww { version: 1 }, disk: [0; 32] };
        let rec_v2 = DurablePath { path: "p.md".into(), kind: DurableKind::Lww { version: 2 }, disk: [1; 32] };
        queue_record(topic, &rec_v1);
        queue_record(topic, &rec_v2);
        flush_records().await;

        let dir = subdir(&board_root(&topic).await.unwrap(), SYNC_DIR, false).await.unwrap();
        let name = format!("{}.rec", riftpipe_core::sync::record_name("p.md"));
        let bytes = read_bytes(&dir, &name).await.expect("record written");
        let decoded: DurablePath = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, rec_v2);
    }

    /// `path`'s text on board `topic`'s tree (`None` if absent).
    async fn read_in(topic: &[u8; 32], path: &str) -> Option<String> {
        let parts: Vec<&str> = path.split('/').collect();
        let (file, dirs) = parts.split_last()?;
        let mut dir = board_root(topic).await.ok()?;
        for d in dirs {
            dir = subdir(&dir, d, false).await.ok()?;
        }
        read_text(&dir, file).await
    }

    /// `path`'s durable record on board `topic`, if written.
    async fn record_in(topic: &[u8; 32], path: &str) -> Option<DurablePath> {
        let dir = subdir(&board_root(topic).await.ok()?, SYNC_DIR, false).await.ok()?;
        let bytes = read_bytes(&dir, &format!("{}.rec", riftpipe_core::sync::record_name(path))).await?;
        postcard::from_bytes(&bytes).ok()
    }

    /// A merged file and its record: whenever the record is on disk, the file it
    /// describes already is too (a reload in between would otherwise restore the
    /// OLD file against the NEW record and author a revert of the merge).
    #[wasm_bindgen_test]
    async fn record_never_lands_before_its_merged_file() {
        let topic = riftpipe_core::crypto::generate_secret();
        let path = "tickets/deep/nested/card.md";
        write_path_in(&topic, path, b"# old\n").await.unwrap();
        let mut s = Syncer::new("m1");
        s.local_text(path, "# old\n# merged\n");
        let rec = s.durable(path).unwrap();
        queue_merged_file(topic, path.into(), b"# old\n# merged\n".to_vec(), || {});
        queue_record(topic, &rec);
        for _ in 0..500 {
            if record_in(&topic, path).await.is_some() {
                assert_eq!(read_in(&topic, path).await.as_deref(), Some("# old\n# merged\n"), "record landed before its file");
                break;
            }
            gloo_timers::future::TimeoutFuture::new(1).await;
        }
        flush_records().await;
        assert_eq!(record_in(&topic, path).await, Some(rec));
    }

    /// A restore that starts while the board's merged file + record are still being
    /// written (a fast in-tab switch back) waits for them: it resumes the merged
    /// history cleanly instead of reading a half-written pair.
    #[wasm_bindgen_test]
    async fn restore_waits_for_in_flight_writes() {
        let topic = riftpipe_core::crypto::generate_secret();
        let mut s1 = Syncer::new("w1");
        s1.local_text("a.md", "# merged\n");
        queue_merged_file(topic, "a.md".into(), b"# merged\n".to_vec(), || {});
        queue_record(topic, &s1.durable("a.md").unwrap());
        let s2 = restore_syncer(topic, "w2".into()).await;
        assert_eq!(text_origin(&s2.full_state(), "a.md"), text_origin(&s1.full_state(), "a.md"));
        assert_eq!(s2.durable("a.md"), s1.durable("a.md"), "restored Clean: same history, no new local edit");
    }

    /// A local write made while an older merged file for the same path is queued
    /// or mid-write wins on disk — the file ends as what its record says.
    #[wasm_bindgen_test]
    async fn local_write_wins_over_a_pending_merged_file() {
        let topic = riftpipe_core::crypto::generate_secret();
        set_active_board(topic);
        queue_merged_file(topic, "x.toml".into(), b"merged".to_vec(), || {});
        write_path("x.toml", b"local").await.unwrap();
        let mut s = Syncer::new("l1");
        s.local_lww("x.toml", b"local".to_vec(), 5);
        let rec = s.durable("x.toml").unwrap();
        queue_local_record(topic, &rec, b"local");
        flush_records().await;
        assert_eq!(read_in(&topic, "x.toml").await.as_deref(), Some("local"));
        assert_eq!(record_in(&topic, "x.toml").await, Some(rec));
    }

    /// A record whose file was deleted while the tab was closed is dropped on
    /// restore (like the native `seed`), not kept forever.
    #[wasm_bindgen_test]
    async fn restore_drops_records_of_deleted_files() {
        let topic = riftpipe_core::crypto::generate_secret();
        let mut s1 = Syncer::new("o1");
        s1.local_text("gone.md", "# gone\n");
        s1.local_text("kept.md", "# kept\n");
        write_path_in(&topic, "kept.md", b"# kept\n").await.unwrap();
        queue_record(topic, &s1.durable("gone.md").unwrap());
        queue_record(topic, &s1.durable("kept.md").unwrap());
        flush_records().await;
        assert!(record_in(&topic, "gone.md").await.is_some());

        let _ = restore_syncer(topic, "o2".into()).await;
        assert!(record_in(&topic, "gone.md").await.is_none(), "orphan record dropped");
        assert!(record_in(&topic, "kept.md").await.is_some(), "a live file's record stays");
    }
}
