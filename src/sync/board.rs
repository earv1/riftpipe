//! Board sync — the native driver for the shared `riftpipe_core::sync` protocol
//! (the same one the browser speaks over its gossip mesh / WebRTC links). It
//! binds that pure protocol to a directory + a split link: remote edits land on
//! disk, local edits (any editor touching the files) are watched and pushed.
//! Text files (`*.md`) merge as CRDTs, structural files as LWW — all conflict
//! resolution lives in `riftpipe_core::sync::Syncer`; this module is only I/O.
//!
//! Transport-blind: takes the `net::{Sink, Source}` halves, so it runs over
//! whatever dialed the link (WebRTC via signaling today; iroh works the same).

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use riftpipe_core::sync::{SyncMsg, Syncer};

use crate::net::{anyerr, Result, Sink, Source};

fn escapes(path: &str) -> bool {
    Path::new(path).components().any(|c| matches!(c, Component::ParentDir | Component::RootDir))
}

fn rel_of(full: &Path, dir: &Path) -> Option<String> {
    let rel = full.strip_prefix(dir).ok()?.to_string_lossy().replace('\\', "/");
    if rel.is_empty() || rel.contains("/.") { None } else { Some(rel) }
}

/// Sync the board `dir` both ways over an established link. Blocks until the
/// link drops.
pub async fn run(
    sink: Box<dyn Sink>,
    mut source: Box<dyn Source>,
    dir: &Path,
) -> Result<()> {
    use notify::{RecursiveMode, Watcher};

    std::fs::create_dir_all(dir).map_err(anyerr)?;
    // Canonicalize so it matches the watcher's event paths (on macOS /var is a
    // symlink to /private/var, which would otherwise break strip_prefix).
    let dir = std::fs::canonicalize(dir).map_err(anyerr)?;

    let sink = Arc::new(tokio::sync::Mutex::new(sink));
    let syncer = Arc::new(Mutex::new(Syncer::new(format!("n{:08x}", rand::random::<u32>()))));
    // Last bytes written from a remote merge per path — the watcher skips a file
    // whose content equals what we just wrote (the echo of a remote update), so a
    // remote edit isn't pushed straight back (which would ping-pong LWW versions).
    // Content-based, not time-based, so a *genuine* local edit right after a remote
    // write is still pushed.
    let echo: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Native local edits → push. A notify callback (sync) feeds paths to a task.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(ev) = res {
            for p in ev.paths {
                let _ = tx.send(p);
            }
        }
    })
    .map_err(anyerr)?;
    watcher.watch(&dir, RecursiveMode::Recursive).map_err(anyerr)?;

    let (w_sink, w_syncer, w_echo, w_dir) = (sink.clone(), syncer.clone(), echo.clone(), dir.clone());
    tokio::spawn(async move {
        while let Some(path) = rx.recv().await {
            let Some(rel) = rel_of(&path, &w_dir) else { continue };
            if !path.is_file() {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else { continue };
            // Skip the echo of a file we just wrote from a remote merge (same bytes).
            if w_echo.lock().unwrap().get(&rel).is_some_and(|b| b == &bytes) {
                continue;
            }
            let msg = if rel.ends_with(".md") {
                w_syncer.lock().unwrap().local_text(&rel, &String::from_utf8_lossy(&bytes))
            } else {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                Some(w_syncer.lock().unwrap().local_lww(&rel, bytes, now))
            };
            if let Some(msg) = msg {
                if let Ok(b) = postcard::to_allocvec(&msg) {
                    let _ = w_sink.lock().await.send(b).await;
                }
            }
        }
    });

    // Handshake: advertise our version vectors so the peer sends what we lack.
    if let Ok(b) = postcard::to_allocvec(&syncer.lock().unwrap().hello()) {
        let _ = sink.lock().await.send(b).await;
    }

    // Remote edits → disk (+ any handshake replies go back out over the sink).
    while let Ok(Some(bytes)) = source.recv().await {
        let Ok(msg) = postcard::from_bytes::<SyncMsg>(&bytes) else { continue };
        let (persist, replies) = syncer.lock().unwrap().apply(msg);
        if let Some((path, bytes)) = persist {
            if !escapes(&path) {
                echo.lock().unwrap().insert(path.clone(), bytes.clone());
                let full = dir.join(&path);
                if let Some(parent) = full.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(&full, &bytes);
                println!("SYNCED:{path}");
            }
        }
        for reply in replies {
            if let Ok(b) = postcard::to_allocvec(&reply) {
                let _ = sink.lock().await.send(b).await;
            }
        }
    }
    drop(watcher);
    Ok(())
}
