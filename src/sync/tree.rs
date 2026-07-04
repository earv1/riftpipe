//! Tree sync — the native driver for the shared `riftpipe_core::sync` protocol
//! (the same one the browser speaks over its gossip mesh / WebRTC links). It
//! binds that pure protocol to ANY file tree + a split link: remote edits land
//! on disk, local edits (any editor touching the files) are watched and pushed.
//! Text files (`*.md`) merge as CRDTs, everything else as LWW, and dot-paths
//! never sync — all conflict resolution lives in `riftpipe_core::sync::Syncer`;
//! this module is only I/O. The kanban app is the showcase consumer (its board
//! directory is just such a tree), not the owner.
//!
//! Transport-blind: takes the `net::{Sink, Source}` halves, so it runs over
//! whatever dialed the link — WebRTC via signaling for browser peers, or a
//! native authenticated+negotiated iroh session (`riftpipe connect --accept` /
//! a ticket); `main.rs` owns that dialing, [`run_over`] is the common entry.
//!
//! Shape matches the sibling sessions (`folder::session`, `pipe::session`): the
//! caller owns the state ([`TreePeer`]) and the watcher channel; [`run`] is a
//! single `tokio::select!` loop — no spawned tasks, no shared-state locks.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, MissedTickBehavior};

use riftpipe_core::sync::{SyncMsg, Syncer};

use crate::net::{anyerr, Result, Sink, Source};

fn escapes(path: &str) -> bool {
    Path::new(path).components().any(|c| matches!(c, Component::ParentDir | Component::RootDir))
}

/// Any path with a `.`-prefixed component is machine-local (top-level `.site`
/// — the per-machine site id — nested `tickets/.hidden`, editor droppings) and
/// must never cross the wire, in EITHER direction.
fn hidden(rel: &str) -> bool {
    rel.split('/').any(|c| c.starts_with('.'))
}

fn rel_of(full: &Path, dir: &Path) -> Option<String> {
    let rel = full.strip_prefix(dir).ok()?.to_string_lossy().replace('\\', "/");
    if rel.is_empty() || hidden(&rel) {
        None
    } else {
        Some(rel)
    }
}

/// Create the dir if needed and canonicalize it so it matches the
/// watcher's event paths (on macOS /var is a symlink to /private/var, which
/// would otherwise break `strip_prefix`).
pub fn prepare_dir(dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).map_err(anyerr)?;
    std::fs::canonicalize(dir).map_err(anyerr)
}

/// One peer's tree-sync state, owned by the caller (like `Workspace` for
/// folder mode): the protocol `Syncer` plus a content-hash memo.
pub struct TreePeer {
    syncer: Syncer,
    /// blake3 of the last-known content per rel path — updated on every push
    /// and every remote persist. A push is skipped when the on-disk hash equals
    /// this (covers both the echo of a remote write and a no-op event), and it
    /// is what the fallback poll diffs against. Hashes, not bytes, so it never
    /// holds file copies.
    seen: HashMap<String, [u8; 32]>,
    /// Paths already reported as parked (resync retries exhausted in the core
    /// `Syncer`), so the stderr breadcrumb fires once per park — and again if a
    /// path recovers and later parks anew.
    parked_reported: HashSet<String>,
}

impl TreePeer {
    pub fn new() -> Self {
        TreePeer {
            syncer: Syncer::new(format!("n{:08x}", rand::random::<u32>())),
            seen: HashMap::new(),
            parked_reported: HashSet::new(),
        }
    }

    /// Core is no-I/O, so IT records parked paths and WE print them: eprintln
    /// for every newly-parked path, and forget recovered ones so a re-park is
    /// reported again.
    fn report_parked(&mut self) {
        let now: HashSet<String> = self.syncer.parked_paths().into_iter().collect();
        for p in &now {
            if !self.parked_reported.contains(p) {
                eprintln!(
                    "[tree] {p}: full resync failed repeatedly — parked (holding local copy; \
                     will recover when a mergeable state arrives)"
                );
            }
        }
        self.parked_reported = now;
    }
}

impl Default for TreePeer {
    fn default() -> Self {
        Self::new()
    }
}

/// Start a recursive watcher on `dir`, feeding event paths into the returned
/// channel. If the watcher can't start, warn and return `(None, rx)` — the
/// caller passes `poll = watcher.is_none()` to [`run`] so the session degrades
/// to the fallback poll instead of aborting (folder mode does the same).
pub fn watch(dir: &Path) -> (Option<RecommendedWatcher>, UnboundedReceiver<PathBuf>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let mut watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(ev) = res {
            for p in ev.paths {
                let _ = tx.send(p);
            }
        }
    }) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("[tree] watcher unavailable ({e}); falling back to poll");
            return (None, rx);
        }
    };
    if let Err(e) = watcher.watch(dir, RecursiveMode::Recursive) {
        eprintln!("[tree] watch failed ({e}); falling back to poll");
        return (None, rx);
    }
    (Some(watcher), rx)
}

/// Push one local file if its content changed since we last saw it. No-ops on
/// dot-paths, non-files, and unchanged content; records the hash on push.
async fn push_local(
    peer: &mut TreePeer,
    path: &Path,
    dir: &Path,
    sink: &mut dyn Sink,
) -> Result<()> {
    let Some(rel) = rel_of(path, dir) else { return Ok(()) };
    if !path.is_file() {
        return Ok(());
    }
    let Ok(bytes) = std::fs::read(path) else { return Ok(()) };
    let hash = *blake3::hash(&bytes).as_bytes();
    // Same content we last pushed or persisted (remote-write echo / no-op event).
    if peer.seen.get(&rel) == Some(&hash) {
        return Ok(());
    }
    let msg = if rel.ends_with(".md") {
        peer.syncer.local_text(&rel, &String::from_utf8_lossy(&bytes))
    } else {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Some(peer.syncer.local_lww(&rel, bytes, now))
    };
    peer.seen.insert(rel, hash);
    if let Some(msg) = msg {
        if let Ok(b) = postcard::to_allocvec(&msg) {
            sink.send(b).await?;
        }
    }
    Ok(())
}

/// Collect every regular file under `dir`, skipping any dot-component (manual
/// recursion — the tree is small and this avoids a walkdir dependency).
fn scan_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        if e.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let p = e.path();
        if p.is_dir() {
            scan_files(&p, out);
        } else if p.is_file() {
            out.push(p);
        }
    }
}

/// Sync the tree at `dir` both ways over an established link: one `select!`
/// loop over watcher events (`rx`), incoming messages, and — when `poll` is set
/// because the watcher failed — a slow rescan. Returns when the link closes
/// (`source.recv()` yields `Ok(None)` or an error is unrecoverable).
pub async fn run(
    peer: &mut TreePeer,
    rx: &mut UnboundedReceiver<PathBuf>,
    poll: bool,
    sink: &mut dyn Sink,
    source: &mut dyn Source,
    dir: &Path,
) -> Result<()> {
    let mut rescan = interval(Duration::from_secs(2));
    rescan.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut rx_open = true;

    // Handshake: advertise our version vectors so the peer sends what we lack.
    if let Ok(b) = postcard::to_allocvec(&peer.syncer.hello()) {
        sink.send(b).await?;
    }

    loop {
        tokio::select! {
            // Native local edits → push (the watcher feeds paths into `rx`).
            ev = rx.recv(), if rx_open => {
                let Some(first) = ev else {
                    // Watcher gone (e.g. it failed after creation); the poll arm
                    // is the safety net — never abort the session over it.
                    rx_open = false;
                    continue;
                };
                // Coalesce a burst of events into a de-duplicated batch.
                let mut batch = HashSet::new();
                batch.insert(first);
                while let Ok(p) = rx.try_recv() {
                    batch.insert(p);
                }
                for p in batch {
                    push_local(peer, &p, dir, sink).await?;
                }
            }
            // Fallback poll: rescan for files whose content drifted from `seen`.
            _ = rescan.tick(), if poll => {
                let mut files = Vec::new();
                scan_files(dir, &mut files);
                for p in files {
                    push_local(peer, &p, dir, sink).await?;
                }
            }
            // Remote edits → disk (+ any handshake replies go back out).
            msg = source.recv() => {
                let bytes = match msg {
                    Err(_) | Ok(None) => return Ok(()),
                    Ok(Some(b)) => b,
                };
                let Ok(msg) = postcard::from_bytes::<SyncMsg>(&bytes) else { continue };
                let (persist, replies) = peer.syncer.apply(msg);
                peer.report_parked();
                if let Some((path, bytes)) = persist {
                    // Refuse escaping paths AND dot-paths from the wire — `.site`
                    // and friends are machine-local and must not be overwritten.
                    if !escapes(&path) && !hidden(&path) {
                        peer.seen.insert(path.clone(), *blake3::hash(&bytes).as_bytes());
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
                        sink.send(b).await?;
                    }
                }
            }
        }
    }
}

/// The common session entry over any established link: prepare `dir`, start
/// the watcher (degrading to the poll fallback if it can't — never aborts),
/// and drive [`run`] until the link closes. Both the WebRTC-via-signaling path
/// and the native iroh ticket/accept paths end up here.
pub async fn run_over(sink: &mut dyn Sink, source: &mut dyn Source, dir: &str) -> Result<()> {
    let dir = prepare_dir(Path::new(dir))?;
    let (watcher, mut rx) = watch(&dir);
    let mut peer = TreePeer::new();
    run(&mut peer, &mut rx, watcher.is_none(), sink, source, &dir).await
}

/// Dial a peer's room over WebRTC (via the signaling server) and sync `dir`
/// both ways — the native end of browser↔native collaboration. Blocks until
/// the link drops. `counters` tracks the session's bytes for the caller to
/// report (a signaling session has no iroh `Endpoint`, so the tmux metrics
/// side-car can't run here — the caller prints totals instead).
pub async fn connect(
    signal: &str,
    room: &str,
    dir: &str,
    counters: std::sync::Arc<crate::net::Counters>,
) -> Result<()> {
    let link = crate::net::webrtc::connect_via_signaling(signal, room).await?;
    let (mut sink, mut source) = link.into_halves(counters);
    eprintln!("[riftpipe] connected to room '{room}' (WebRTC); syncing {dir}");
    run_over(&mut sink, &mut source, dir).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;
    use tokio::time::{sleep, timeout};

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("riftpipe-tree-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::canonicalize(&d).unwrap()
    }

    /// Drive two crossed TreePeers over a mock link: feed `feed` (paths
    /// relative to `da`) to A as watcher events, run both sessions until
    /// `settled()` holds, keep pumping briefly so in-flight messages land, then
    /// tear down by dropping A's send half — B's `source.recv()` sees the
    /// close and its `run()` returns (the session-end path under test).
    async fn converge(da: &Path, db: &Path, feed: &[&str], settled: impl Fn() -> bool) {
        let (la, lb) = mock_pair();
        let (mut sink_a, mut source_a) = la.into_halves();
        let (mut sink_b, mut source_b) = lb.into_halves();

        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
        let (_tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
        for f in feed {
            tx_a.send(da.join(f)).unwrap();
        }

        let db_owned = db.to_path_buf();
        let b_task = tokio::spawn(async move {
            let mut pb = TreePeer::new();
            run(&mut pb, &mut rx_b, false, &mut sink_b, &mut source_b, &db_owned)
                .await
                .expect("B's session ends cleanly when A hangs up");
        });

        {
            let mut pa = TreePeer::new();
            let run_a = run(&mut pa, &mut rx_a, false, &mut sink_a, &mut source_a, da);
            tokio::pin!(run_a);
            loop {
                tokio::select! {
                    r = &mut run_a => panic!("A's session ended early: {r:?}"),
                    _ = sleep(Duration::from_millis(20)) => {
                        if settled() {
                            break;
                        }
                    }
                }
            }
            // Settled — keep both sides pumping a little longer so anything
            // still in flight (e.g. a wrongly-pushed dotfile) would land and
            // be caught by the assertions.
            tokio::select! {
                r = &mut run_a => panic!("A's session ended early: {r:?}"),
                _ = sleep(Duration::from_millis(300)) => {}
            }
        } // run_a dropped here, releasing the borrow on sink_a
        drop(sink_a); // close B's source → B's run() returns
        b_task.await.unwrap();
    }

    #[tokio::test]
    async fn two_peers_converge_text_and_lww() {
        let da = dir("conv-a");
        let db = dir("conv-b");
        std::fs::create_dir_all(da.join("tickets/tk_1")).unwrap();
        std::fs::write(da.join("tickets/tk_1/card.md"), "# card one\n\nbody\n").unwrap();
        std::fs::write(da.join("tickets/tk_1/meta.toml"), "column = \"Todo\"\nposition = 0\n").unwrap();

        let (cb, mb) = (db.join("tickets/tk_1/card.md"), db.join("tickets/tk_1/meta.toml"));
        timeout(
            Duration::from_secs(15),
            converge(
                &da,
                &db,
                &["tickets/tk_1/card.md", "tickets/tk_1/meta.toml"],
                || cb.is_file() && mb.is_file(),
            ),
        )
        .await
        .expect("tree sync timed out");

        assert_eq!(
            std::fs::read(db.join("tickets/tk_1/card.md")).unwrap(),
            b"# card one\n\nbody\n",
            "text file arrives byte-identical via the CRDT path",
        );
        assert_eq!(
            std::fs::read(db.join("tickets/tk_1/meta.toml")).unwrap(),
            b"column = \"Todo\"\nposition = 0\n",
            "structural file arrives via LWW",
        );
        std::fs::remove_dir_all(&da).ok();
        std::fs::remove_dir_all(&db).ok();
    }

    #[tokio::test]
    async fn dot_paths_never_sync() {
        let da = dir("dot-a");
        let db = dir("dot-b");
        std::fs::create_dir_all(da.join("tickets")).unwrap();
        std::fs::write(da.join(".site"), "site-a\n").unwrap();
        std::fs::write(da.join("tickets/.hidden"), "shh\n").unwrap();
        // A normal sentinel file: once it lands on B, A's whole batch (which
        // included the dot-paths) has been processed.
        std::fs::write(da.join("board.md"), "# Board\n\n- Todo\n").unwrap();

        let sentinel = db.join("board.md");
        timeout(
            Duration::from_secs(15),
            converge(
                &da,
                &db,
                &[".site", "tickets/.hidden", "board.md"],
                || sentinel.is_file(),
            ),
        )
        .await
        .expect("tree sync timed out");

        assert!(!db.join(".site").exists(), "top-level .site must never sync");
        assert!(!db.join("tickets/.hidden").exists(), "nested dotfile must never sync");
        std::fs::remove_dir_all(&da).ok();
        std::fs::remove_dir_all(&db).ok();
    }
}
