//! Folder mode (DESIGN.md §17) — multiplex many resources over one link, each
//! driven by its own [`SyncStrategy`](crate::sync::strategy::SyncStrategy). This is the
//! `--pipe` reconnecting session generalized from one document to a whole
//! [`Workspace`]: same link plumbing, same reconnect loop, but every frame is
//! tagged with a **resource id** (the relative path) and local changes are
//! discovered by **watching** the filesystem (an OS event nudges a rescan;
//! a slow poll stays on as a safety net) rather than read from stdin.
//!
//! ## Wire messages
//! Folder mode speaks the ONE shared wire protocol — postcard-encoded
//! [`riftpipe_core::sync::SyncMsg`], same as the tree/mesh drivers — via its
//! **strategy channel**:
//!   * `StratDelta`               — a patch to [`merge`](crate::sync::strategy::SyncStrategy::merge).
//!   * `StratAdvert reply: true`  — an advertisement that *expects a reply*
//!     (sent on connect, on a local change, and on the heartbeat).
//!   * `StratAdvert reply: false` — an advertisement *in reply* (no further
//!     reply, so the exchange can't ping-pong forever).
//!   * `StratDelete`              — a tombstone: "I deleted this path" (see Deletes).
//!
//! Non-strategy variants (`Hello`, `TextDelta`, …) are ignored here, as the
//! text/LWW `Syncer` ignores strategy messages — one envelope, two dialects.
//!
//! Why request/reply: a push-capable algorithm (text CRDT) ships its change
//! immediately as a `StratDelta`. A pull-only one (rsync) can't — it needs the
//! peer's signatures first. So a local change also sends an advert; the peer
//! answers with *its* signatures, and we then compute and push the rsync
//! delta. Receiving an unknown path auto-creates the resource (peer-driven
//! discovery).
//!
//! ## Deletes
//! A file-backed resource that was materialized at least once (non-empty
//! `seen` fingerprint history) and is now missing from disk is a local delete:
//! the resource becomes a tombstone (deterministic deletion id + its `seen`)
//! and a `StratDelete` goes out. A peer applies it only if its own current
//! fingerprint is one the deleter had seen; otherwise it holds a concurrent
//! edit, which wins — it answers with a self-contained `StratDelta`, and the
//! deleter, receiving a delta for a tombstoned path, resurrects the file. An
//! incremental delta that can't stand alone instead re-asserts the delete (at
//! most once per heartbeat). An advert for a tombstoned path is answered with
//! the `StratDelete` (how a peer that was offline learns of it). A file
//! re-created locally drops its tombstone and remembers all its deletion `ids`
//! as `superseded`, so an old copy of any of them is rejected. A delete for a
//! path already deleted here merges its ids into the tombstone; ids are
//! deterministic (same content + history → same id), so independent deletes
//! that never met still match. Tombstones and last-known files persist in
//! `<root>/.riftpipe-folder-index`, so deletes made while offline are
//! detected at startup.
//!
//! Known limit (deliberate): the edit-wins check compares *strategy*
//! fingerprints, i.e. only what the algorithm syncs. For `lww-record` that is
//! the records; comments, layout and lines without `=` are never synced (any
//! remote merge already rewrites the file canonically and drops them), so a
//! remote delete drops them too — a comment-only file counts as empty. A
//! stricter raw-bytes check would make every hand-formatted file undeletable
//! from another peer (it would resurrect on the deleter forever).

use std::sync::Arc;
use std::time::Duration;

use iroh::Endpoint;
use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, MissedTickBehavior};

use riftpipe_core::sync::{SyncMsg, PROTOCOL_VERSION};

use crate::net::secure::authenticate;
use crate::net::negotiate::negotiate_session_halves;
use crate::net::transport::{accept_link, connect_link};
use crate::net::{Counters, Result, Sink, Source};
use crate::sync::pipe::{Role, SessionOutcome};
use crate::sync::workspace::{Hash, Tombstone, Workspace, DELETION_IDS_CAP, SEEN_CAP};

/// Re-advertise cadence — and the liveness beat: every heartbeat starts with a
/// `Version` frame, so even an empty workspace proves it's alive.
const HEARTBEAT: Duration = Duration::from_secs(5);
/// A peer that sends NOTHING for this long is treated as dead, even if the
/// transport never reports a close (a WebRTC data channel to a vanished peer
/// keeps accepting sends and never yields EOF). 3 × HEARTBEAT: tolerates two
/// lost/late beats. Known limit: a single frame that takes longer than this
/// to arrive (a huge rsync delta on a slow link) trips it and the session
/// reconnects.
pub const RECV_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

fn advert(path: &str, sv: Vec<u8>, reply: bool) -> Vec<u8> {
    encode(&SyncMsg::StratAdvert { path: path.to_string(), sv, reply })
}

fn delta(path: &str, bytes: Vec<u8>) -> Vec<u8> {
    encode(&SyncMsg::StratDelta { path: path.to_string(), bytes })
}

fn delete_frame(path: &str, t: &Tombstone) -> Vec<u8> {
    encode(&SyncMsg::StratDelete { path: path.to_string(), ids: t.ids.clone(), seen: t.seen.clone() })
}

fn encode(msg: &SyncMsg) -> Vec<u8> {
    // postcard on an in-memory Vec cannot fail for these shapes.
    postcard::to_allocvec(msg).expect("SyncMsg encodes")
}

/// One local scan: new files + changed files → frames to send (delta, then a
/// reply-expecting advert, per changed resource), and a `StratDelete` per
/// file that vanished from disk. Picks up newly-created files (including
/// re-creations of tombstoned paths), then for each resource `observe`s its
/// bytes; on a real change it queues a delta and a reply-expecting advert.
/// (Self-write safe: merging a remote change stores bytes that `observe` then
/// reports as *unchanged*, so a scan triggered by our own write is a no-op —
/// no echo, no extra dedup needed.)
fn collect_local(ws: &mut Workspace) -> Vec<Vec<u8>> {
    let _ = ws.refresh_disk(); // new files, and re-creations of tombstoned paths
    let mut out = Vec::new();
    let mut deleted = Vec::new();
    let mut changed = false;
    for path in ws.paths() {
        let Some(res) = ws.get_mut(&path) else { continue };
        if res.locally_deleted() {
            deleted.push(path);
            continue;
        }
        let bytes = res.backing.load();
        if res.strategy.observe(&bytes) {
            res.note_state();
            changed = true;
            if let Some(d) = res.strategy.push_delta() {
                out.push(delta(&path, d));
            }
            out.push(advert(&path, res.strategy.state_vector(), true));
        }
    }
    for path in deleted {
        if let Some(t) = ws.record_local_delete(&path) {
            out.push(delete_frame(&path, &t));
        }
    }
    if changed {
        ws.mark_dirty();
    }
    out
}

/// Version frame + a reply-expecting advert for every resource (connect +
/// heartbeat). Re-arms every tombstone's once-per-heartbeat re-assert guard.
/// Tombstoned paths aren't resources, so they're never advertised.
fn advertise_all(ws: &mut Workspace) -> Vec<Vec<u8>> {
    ws.rearm_tombstones();
    let mut out = vec![encode(&SyncMsg::Version { proto: PROTOCOL_VERSION })];
    for path in ws.paths() {
        let sv = match ws.get_mut(&path) {
            Some(r) => r.strategy.state_vector(),
            None => continue,
        };
        out.push(advert(&path, sv, true));
    }
    out
}

/// Apply one decoded inbound message; the frames to send back. `Version` and
/// non-strategy messages yield nothing (the session handles `Version`).
/// Receiving an unknown path auto-creates the resource (peer-driven discovery).
fn apply_inbound(ws: &mut Workspace, msg: SyncMsg) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    match msg {
        SyncMsg::StratDelta { path, bytes } => {
            if ws.is_tombstoned(&path) {
                delta_for_tombstone(ws, &path, &bytes, &mut out);
                return out;
            }
            let mut stored = false;
            if let Some(res) = ws.ensure(&path) {
                if let Some(nb) = res.strategy.merge(&bytes) {
                    res.store(&nb);
                    stored = true;
                }
            }
            if stored {
                ws.mark_dirty();
            }
        }
        SyncMsg::StratAdvert { path, sv, reply } => {
            if let Some(t) = ws.tombstone(&path) {
                out.push(delete_frame(&path, t)); // we deleted it — tell them
                return out;
            }
            if let Some(res) = ws.ensure(&path) {
                if let Some(d) = res.strategy.delta_since(&sv) {
                    out.push(delta(&path, d));
                }
                if reply {
                    out.push(advert(&path, res.strategy.state_vector(), false));
                }
            }
        }
        SyncMsg::StratDelete { path, ids, seen } => remote_delete(ws, &path, &ids, &seen, &mut out),
        _ => {}
    }
    out
}

/// A delta for a path we deleted means the peer holds content we lack. If it
/// materializes standalone, edit wins → resurrect. If not (an incremental
/// delta), re-assert the delete — once per heartbeat — so the peer either
/// agrees or answers with a self-contained delta.
fn delta_for_tombstone(ws: &mut Workspace, path: &str, bytes: &[u8], out: &mut Vec<Vec<u8>>) {
    let Some(mut fresh) = ws.fresh_strategy(path) else { return };
    if let Some(nb) = fresh.merge(bytes) {
        ws.resurrect(path, fresh, &nb);
        return;
    }
    if let Some(t) = ws.tombstone_mut(path) {
        if !t.reasserted {
            t.reasserted = true;
            out.push(delete_frame(path, t));
        }
    }
}

/// A peer deleted `path`. Delete here unless we hold content the deleter
/// never saw (a concurrent edit — edit wins: answer with a self-contained
/// delta so the deleter resurrects) or we re-created the file after any of
/// those deletions. Already deleted here: merge the peer's ids + fingerprints
/// into our tombstone (so a later re-creation overrides them too). Never-known
/// paths: no-op. Only the newest `DELETION_IDS_CAP` ids / `SEEN_CAP`
/// fingerprints are considered, so a peer can't make us hold a huge list.
fn remote_delete(ws: &mut Workspace, path: &str, ids: &[u64], seen: &[Hash], out: &mut Vec<Vec<u8>>) {
    let ids = &ids[ids.len().saturating_sub(DELETION_IDS_CAP)..];
    let seen = &seen[seen.len().saturating_sub(SEEN_CAP)..];
    if ws.is_tombstoned(path) {
        ws.merge_remote_delete(path, ids, seen);
        return;
    }
    let Some(empty_sv) = ws.fresh_strategy(path).map(|s| s.state_vector()) else { return };
    let Some(res) = ws.get_mut(path) else { return }; // never had it
    let bytes = res.backing.load();
    if res.strategy.observe(&bytes) {
        res.note_state(); // an unscanned local edit counts
    }
    let full = res.strategy.delta_since(&empty_sv); // self-contained: merges into a fresh strategy
    let keep = if ids.iter().any(|id| res.superseded.contains(id)) {
        true
    } else if bytes.is_empty() || full.is_none() {
        false
    } else {
        !seen.contains(&res.strategy.fingerprint())
    };
    if keep {
        if let Some(d) = full {
            out.push(delta(path, d));
        }
        ws.mark_dirty();
    } else if let Err(e) = ws.apply_remote_delete(path, ids, seen) {
        eprintln!("[folder] could not delete {path} (peer's delete; kept, will retry): {e}");
    }
}

/// Send every frame in order; `false` on the first send error, so the caller
/// can treat that as `LinkClosed`.
async fn send_all(sink: &mut dyn Sink, frames: Vec<Vec<u8>>) -> bool {
    for f in frames {
        if sink.send(f).await.is_err() {
            return false;
        }
    }
    true
}

/// One sync session over a single link, driving every resource in `ws`. Returns
/// `LinkClosed` on any link error, or after `RECV_IDLE_TIMEOUT` of silence (so
/// the reconnect loop re-dials); folder mode has no stdin, so it never returns
/// `StdinClosed`. `watch_rx` is owned by the reconnect loop (fed by a
/// filesystem watcher) and **persists across sessions**. The folder index
/// (tombstones + last-known files) is persisted whenever it changed.
pub async fn session(
    ws: &mut Workspace,
    watch_rx: &mut UnboundedReceiver<()>,
    sink: &mut dyn Sink,
    source: &mut dyn Source,
) -> Result<SessionOutcome> {
    // Safety-net poll: watchers can drop events on some network/edge
    // filesystems, so we still rescan slowly. This is the fallback, NOT the
    // primary path — the `watch_rx` arm below is what reacts promptly.
    let mut fallback = interval(Duration::from_secs(2));
    fallback.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat = interval(HEARTBEAT);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let idle = tokio::time::sleep(RECV_IDLE_TIMEOUT);
    tokio::pin!(idle);
    let mut mismatch_warned = false;

    // On connect, announce our protocol version and advertise everything we
    // hold so the peer can reconcile.
    if !send_all(sink, advertise_all(ws)).await {
        return Ok(SessionOutcome::LinkClosed);
    }

    loop {
        // Persist after every handled arm; a no-op when nothing changed.
        ws.save_index_if_dirty();
        tokio::select! {
            // Biased, polled top to bottom: the liveness beat first (a peer
            // streaming at us must still hear from us), inbound frames before
            // local rescans (an endless stream of fs events must not starve
            // the link), and the idle timeout LAST, so a frame that is already
            // queued when the deadline passes always wins over declaring the
            // peer dead.
            biased;
            _ = heartbeat.tick() => {
                if !send_all(sink, advertise_all(ws)).await {
                    return Ok(SessionOutcome::LinkClosed);
                }
            }
            msg = source.recv() => {
                let bytes = match msg {
                    Err(_) | Ok(None) => return Ok(SessionOutcome::LinkClosed),
                    Ok(Some(b)) => b,
                };
                idle.as_mut().reset(tokio::time::Instant::now() + RECV_IDLE_TIMEOUT);
                // Undecodable or non-strategy messages are skipped, exactly as
                // the tree/mesh drivers skip strategy messages — one envelope,
                // two dialects, no cross-talk.
                let Ok(msg) = postcard::from_bytes::<SyncMsg>(&bytes) else { continue };
                if let SyncMsg::Version { proto } = msg {
                    if proto != PROTOCOL_VERSION && !mismatch_warned {
                        mismatch_warned = true;
                        eprintln!(
                            "[folder] protocol-mismatch theirs={proto} ours={PROTOCOL_VERSION} — \
                             sync may be incomplete",
                        );
                    }
                    continue;
                }
                if !send_all(sink, apply_inbound(ws, msg)).await {
                    return Ok(SessionOutcome::LinkClosed);
                }
            }
            // Primary path: the OS told us something under the root changed.
            nudge = watch_rx.recv() => {
                // `None` means the watcher was dropped (shouldn't happen while a
                // session runs — the reconnect loop holds it); the fallback poll
                // covers us either way.
                if nudge.is_some() {
                    while watch_rx.try_recv().is_ok() {} // coalesce a burst of events
                    if !send_all(sink, collect_local(ws)).await {
                        return Ok(SessionOutcome::LinkClosed);
                    }
                }
            }
            // Fallback: rescan slowly in case the watcher missed an event.
            _ = fallback.tick() => {
                if !send_all(sink, collect_local(ws)).await {
                    return Ok(SessionOutcome::LinkClosed);
                }
            }
            _ = &mut idle => {
                eprintln!(
                    "[folder] no frames from peer for {}s — treating the link as dead",
                    RECV_IDLE_TIMEOUT.as_secs()
                );
                return Ok(SessionOutcome::LinkClosed);
            }
        }
    }
}

/// Run folder sync with reconnection: keep one workspace for the process
/// lifetime and repeatedly (re)establish a link, reconciling on each connect.
#[allow(clippy::too_many_arguments)]
pub async fn run_folder_reconnecting(
    endpoint: Endpoint,
    role: Role,
    secret: [u8; 32],
    counters: Arc<Counters>,
    mut ws: Workspace,
    metrics: Option<(String, String)>, // (path, title)
    process: Option<String>,           // the in-memory `process` sidecar path
) -> Result<()> {
    if let Some(p) = process {
        crate::monitor::process::spawn(p, ws.registry());
    }

    // Watch the workspace root for filesystem events so local changes are
    // detected by the OS instead of a tight poll (the `--pipe` path is already
    // event-driven via stdin; this brings folder mode back in line). notify runs
    // its handler synchronously on its own thread, so we bridge events into an
    // async channel as a coalesced "something changed" nudge — we re-scan on it
    // and never need the paths. The receiver persists across reconnect sessions
    // (so events during a reconnect gap aren't lost), and `_watcher` is kept
    // alive for the whole function. If the watcher can't start, the slow
    // fallback poll in `session` still keeps things in sync.
    let (watch_tx, mut watch_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let _watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if res.is_ok() {
            let _ = watch_tx.send(());
        }
    }) {
        Ok(mut w) => {
            if let Err(e) = w.watch(ws.root(), RecursiveMode::Recursive) {
                eprintln!("[riftpipe] watch failed ({e}); falling back to poll");
            }
            Some(w)
        }
        Err(e) => {
            eprintln!("[riftpipe] watcher unavailable ({e}); falling back to poll");
            None
        }
    };

    let mut metrics_started = false;
    let base = Duration::from_millis(200);
    let mut backoff = base;

    loop {
        let link = match &role {
            Role::Accept => accept_link(&endpoint).await,
            Role::Connect(addr) => connect_link(&endpoint, addr.clone()).await,
        };
        if let Ok(mut link) = link {
            if authenticate(&mut link, &secret).await.is_ok() {
                backoff = base;
                if !metrics_started {
                    if let Some((path, title)) = &metrics {
                        crate::monitor::metrics::spawn(
                            endpoint.clone(),
                            link.remote_id(),
                            counters.clone(),
                            path.clone(),
                            title.clone(),
                        );
                        metrics_started = true;
                    }
                }
                // Negotiate the transport + maybe upgrade to WebRTC (shared with
                // --pipe). `s` (and its keepalive) stays in scope for the session.
                let mut s = negotiate_session_halves(link, counters.clone()).await;
                eprintln!("[riftpipe] connected — syncing folder ({:?})", s.transport);
                match session(&mut ws, &mut watch_rx, &mut *s.sink, &mut *s.source).await? {
                    SessionOutcome::StdinClosed => {
                        s.sink.finish().await;
                        return Ok(());
                    }
                    SessionOutcome::LinkClosed => {
                        eprintln!("[riftpipe] disconnected — reconnecting…");
                    }
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::manifest::Manifest;
    use std::path::PathBuf;

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("riftpipe-folder-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Deliver one frame to `ws` the way `session` does; returns its replies.
    fn deliver(ws: &mut Workspace, frame: &[u8]) -> Vec<Vec<u8>> {
        match postcard::from_bytes::<SyncMsg>(frame) {
            Ok(SyncMsg::Version { .. }) | Err(_) => Vec::new(),
            Ok(msg) => apply_inbound(ws, msg),
        }
    }

    /// Shuttle frames between A and B until neither produces more (≤ 32 rounds,
    /// else panic — that's a ping-pong bug). `to_a` = frames headed for A.
    fn pump(a: &mut Workspace, b: &mut Workspace, mut to_a: Vec<Vec<u8>>, mut to_b: Vec<Vec<u8>>) {
        for _ in 0..32 {
            if to_a.is_empty() && to_b.is_empty() {
                return;
            }
            let (mut next_a, mut next_b) = (Vec::new(), Vec::new());
            for f in to_b.drain(..) {
                next_a.extend(deliver(b, &f));
            }
            for f in to_a.drain(..) {
                next_b.extend(deliver(a, &f));
            }
            to_a = next_a;
            to_b = next_b;
        }
        panic!("frames still flowing after 32 rounds");
    }

    /// Initial sync: both scan, pump, then one heartbeat round, pump.
    fn sync_pair(a: &mut Workspace, b: &mut Workspace) {
        let (fa, fb) = (collect_local(a), collect_local(b));
        pump(a, b, fb, fa);
        let (ha, hb) = (advertise_all(a), advertise_all(b));
        pump(a, b, hb, ha);
    }

    fn text_manifest() -> Manifest {
        Manifest::parse(
            "default = \"rsync-file\"\n[[rule]]\nglob = \"**/*.md\"\nalgo = \"text-crdt\"\n[[rule]]\nglob = \"**/meta.toml\"\nalgo = \"lww-record\"\n",
        )
        .unwrap()
    }

    #[test]
    fn strategy_messages_round_trip_and_junk_is_skippable() {
        let f = delta("docs/a.md", b"payload".to_vec());
        match postcard::from_bytes::<SyncMsg>(&f).unwrap() {
            SyncMsg::StratDelta { path, bytes } => {
                assert_eq!(path, "docs/a.md");
                assert_eq!(bytes, b"payload");
            }
            _ => panic!("expected StratDelta"),
        }
        let f = advert("docs/a.md", b"sv".to_vec(), true);
        assert!(matches!(
            postcard::from_bytes::<SyncMsg>(&f).unwrap(),
            SyncMsg::StratAdvert { reply: true, .. }
        ));
        // Truncated/junk frames decode-fail (the session skips them, no panic).
        assert!(postcard::from_bytes::<SyncMsg>(&f[..1]).is_err());
        assert!(postcard::from_bytes::<SyncMsg>(&[0xff, 0xff]).is_err());
    }

    /// Walk the advert -> reply -> delta exchange (what `session` does) by hand
    /// and confirm a file A holds lands on B's disk via rsync.
    #[test]
    fn one_file_syncs_from_a_to_b() {
        let ra = dir("a");
        let rb = dir("b");
        let content = b"rsync this whole file across the link, please and thanks";
        std::fs::write(ra.join("a.bin"), content).unwrap();

        let mut a = Workspace::new(&ra, Manifest::default(), false).unwrap();
        let mut b = Workspace::new(&rb, Manifest::default(), false).unwrap();

        // A's poll: observe local file into its strategy, then advertise.
        let a_req = {
            let r = a.get_mut("a.bin").unwrap();
            let bytes = r.backing.load();
            assert!(r.strategy.observe(&bytes));
            advert("a.bin", r.strategy.state_vector(), true)
        };

        // B receives the advert: nothing to push (B is empty) -> replies with
        // its own (empty) advertisement.
        let b_rep = {
            let Ok(SyncMsg::StratAdvert { path, sv, reply: true }) =
                postcard::from_bytes::<SyncMsg>(&a_req)
            else {
                panic!("expected a reply-expecting StratAdvert")
            };
            let r = b.ensure(&path).unwrap();
            assert!(r.strategy.delta_since(&sv).is_none());
            advert(&path, r.strategy.state_vector(), false)
        };

        // A receives the reply: computes the rsync delta for B and pushes it.
        let a_delta = {
            let Ok(SyncMsg::StratAdvert { path, sv, reply: false }) =
                postcard::from_bytes::<SyncMsg>(&b_rep)
            else {
                panic!("expected a no-reply StratAdvert")
            };
            let r = a.get_mut(&path).unwrap();
            let d = r.strategy.delta_since(&sv).expect("A has content B lacks");
            delta(&path, d)
        };

        // B receives the delta: merges and writes to disk.
        {
            let Ok(SyncMsg::StratDelta { path, bytes }) = postcard::from_bytes::<SyncMsg>(&a_delta)
            else {
                panic!("expected StratDelta")
            };
            let r = b.ensure(&path).unwrap();
            let nb = r.strategy.merge(&bytes).expect("B materializes content");
            r.backing.store(&nb);
        }

        assert_eq!(std::fs::read(rb.join("a.bin")).unwrap(), content);
        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    /// A non-strategy message (the tree dialect) arriving on a folder session
    /// must be ignored — no resource created, no reply. (`apply_inbound`'s
    /// `_ => {}` arm; here we just pin that the dialects are distinguishable
    /// from the strategy messages `StratAdvert` / `StratDelta` / `StratDelete`.)
    #[test]
    fn tree_dialect_messages_are_not_strategy_messages() {
        let hello = postcard::to_allocvec(&SyncMsg::Hello {
            versions: vec![],
            lww_versions: vec![],
        })
        .unwrap();
        assert!(!matches!(
            postcard::from_bytes::<SyncMsg>(&hello).unwrap(),
            SyncMsg::StratAdvert { .. } | SyncMsg::StratDelta { .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn silent_peer_is_declared_dead_after_idle_timeout() {
        let root = dir("idle-silent");
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        let (a, b) = crate::net::mock_pair();
        let (mut sa, mut ra_src) = a.into_halves();
        let (_ps, _pr) = b.into_halves();
        let (_ta, mut wa) = tokio::sync::mpsc::unbounded_channel::<()>();
        let t0 = tokio::time::Instant::now();
        let out = session(&mut ws, &mut wa, &mut sa, &mut ra_src).await.unwrap();
        assert!(matches!(out, SessionOutcome::LinkClosed));
        assert!(t0.elapsed() >= RECV_IDLE_TIMEOUT && t0.elapsed() < RECV_IDLE_TIMEOUT + Duration::from_secs(1));
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test(start_paused = true)]
    async fn chatty_peer_keeps_the_session_alive() {
        let root = dir("idle-chatty");
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        let (a, b) = crate::net::mock_pair();
        let (mut sa, mut ra_src) = a.into_halves();
        let (mut ps, pr) = b.into_halves();
        let (_ta, mut wa) = tokio::sync::mpsc::unbounded_channel::<()>();
        let t0 = tokio::time::Instant::now();
        let peer = async move {
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                ps.send(
                    postcard::to_allocvec(&SyncMsg::Version {
                        proto: riftpipe_core::sync::PROTOCOL_VERSION,
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
            }
            drop(ps);
            drop(pr);
        };
        let (out, ()) = tokio::join!(session(&mut ws, &mut wa, &mut sa, &mut ra_src), peer);
        let out = out.unwrap();
        assert!(matches!(out, SessionOutcome::LinkClosed));
        assert!(t0.elapsed() >= Duration::from_secs(40));
        std::fs::remove_dir_all(&root).ok();
    }

    /// A `Source` whose single frame becomes ready at exactly `at` (then stays
    /// silent). Cancel-safe: the deadline is fixed, so re-creating the `recv`
    /// future on every `select!` iteration doesn't move it.
    struct FrameAt {
        at: tokio::time::Instant,
        sent: bool,
    }

    #[async_trait::async_trait]
    impl Source for FrameAt {
        async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
            if self.sent {
                std::future::pending::<()>().await;
            }
            tokio::time::sleep_until(self.at).await;
            self.sent = true;
            Ok(Some(encode(&SyncMsg::Version { proto: PROTOCOL_VERSION })))
        }
    }

    /// A frame that is ready at the very instant the idle deadline passes must
    /// win: both timers fire in the same wakeup, and the biased `select!` polls
    /// `source.recv()` before the idle arm. (Unbiased, this usually lost the
    /// race and dropped a live link.) The frame resets the timer, so the session
    /// ends one full timeout later.
    #[tokio::test(start_paused = true)]
    async fn frame_arriving_at_the_idle_deadline_beats_the_timeout() {
        let root = dir("idle-race");
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        let (a, b) = crate::net::mock_pair();
        let (mut sa, _unused_src) = a.into_halves();
        let (_ps, _pr) = b.into_halves();
        let (_ta, mut wa) = tokio::sync::mpsc::unbounded_channel::<()>();
        let t0 = tokio::time::Instant::now();
        let mut src = FrameAt { at: t0 + RECV_IDLE_TIMEOUT, sent: false };
        let out = session(&mut ws, &mut wa, &mut sa, &mut src).await.unwrap();
        assert!(matches!(out, SessionOutcome::LinkClosed));
        assert!(t0.elapsed() >= RECV_IDLE_TIMEOUT * 2, "idle won the race at {:?}", t0.elapsed());
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_sends_version_even_with_an_empty_workspace() {
        let root = dir("idle-beat");
        let mut ws = Workspace::new(&root, Manifest::default(), false).unwrap();
        let (a, b) = crate::net::mock_pair();
        let (mut sa, mut ra_src) = a.into_halves();
        let (ps, mut pr) = b.into_halves();
        let (_ta, mut wa) = tokio::sync::mpsc::unbounded_channel::<()>();
        let t0 = tokio::time::Instant::now();
        let peer = async move {
            let deadline = t0 + Duration::from_secs(11);
            let mut count = 0;
            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }
                match tokio::time::timeout(deadline - now, pr.recv()).await {
                    Ok(Ok(Some(bytes))) => {
                        if matches!(
                            postcard::from_bytes::<SyncMsg>(&bytes),
                            Ok(SyncMsg::Version { .. })
                        ) {
                            count += 1;
                        }
                    }
                    _ => break,
                }
            }
            drop(ps);
            drop(pr);
            count
        };
        let (_sess_out, count) = tokio::join!(session(&mut ws, &mut wa, &mut sa, &mut ra_src), peer);
        assert!(count >= 3);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn pump_syncs_new_files_both_ways() {
        let ra = dir("pump-a");
        let rb = dir("pump-b");
        std::fs::write(ra.join("x.bin"), b"rsync bytes").unwrap();
        std::fs::write(rb.join("notes.md"), "# hi\n").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();

        sync_pair(&mut a, &mut b);

        assert_eq!(std::fs::read(ra.join("notes.md")).unwrap(), b"# hi\n");
        assert_eq!(std::fs::read(rb.join("x.bin")).unwrap(), b"rsync bytes");

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    #[test]
    fn delete_propagates_and_stays_deleted() {
        let ra = dir("del-a");
        let rb = dir("del-b");
        std::fs::write(ra.join("x.bin"), b"binary-ish payload").unwrap();
        std::fs::write(ra.join("y.md"), "# doc\n").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        std::fs::remove_file(ra.join("x.bin")).unwrap();
        std::fs::remove_file(ra.join("y.md")).unwrap();
        let fa = collect_local(&mut a);
        assert_eq!(fa.len(), 2);
        for f in &fa {
            assert!(matches!(
                postcard::from_bytes::<SyncMsg>(f).unwrap(),
                SyncMsg::StratDelete { .. }
            ));
        }

        pump(&mut a, &mut b, vec![], fa);

        assert!(!rb.join("x.bin").exists());
        assert!(!rb.join("y.md").exists());
        assert_eq!(b.tombstone_paths(), vec!["x.bin".to_string(), "y.md".to_string()]);
        assert!(!ra.join("x.bin").exists());
        assert!(!ra.join("y.md").exists());

        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);
        assert!(!ra.join("x.bin").exists());
        assert!(!ra.join("y.md").exists());
        assert!(!rb.join("x.bin").exists());
        assert!(!rb.join("y.md").exists());

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    #[test]
    fn delete_reaches_a_peer_that_was_offline() {
        let ra = dir("off-a");
        let rb = dir("off-b");
        std::fs::write(ra.join("z.bin"), b"gone when we reconnect").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        std::fs::remove_file(ra.join("z.bin")).unwrap();
        let _dropped = collect_local(&mut a); // the link was down: discarded

        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);

        assert!(!rb.join("z.bin").exists());

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    #[test]
    fn concurrent_rsync_edit_beats_delete() {
        let ra = dir("cr-a");
        let rb = dir("cr-b");
        std::fs::write(ra.join("f.bin"), b"v1").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        std::fs::remove_file(ra.join("f.bin")).unwrap();
        std::fs::write(rb.join("f.bin"), b"edited by B").unwrap();

        let (fa, fb) = (collect_local(&mut a), collect_local(&mut b));
        pump(&mut a, &mut b, fb, fa);

        assert_eq!(std::fs::read(ra.join("f.bin")).unwrap(), b"edited by B");
        assert_eq!(std::fs::read(rb.join("f.bin")).unwrap(), b"edited by B");
        assert!(!a.is_tombstoned("f.bin"));
        assert!(!b.is_tombstoned("f.bin"));

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    #[test]
    fn concurrent_text_edit_beats_delete() {
        let ra = dir("ct-a");
        let rb = dir("ct-b");
        std::fs::write(ra.join("g.md"), "line one\n").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        std::fs::remove_file(ra.join("g.md")).unwrap();
        std::fs::write(rb.join("g.md"), "line one\nline two from B\n").unwrap();

        let (fa, fb) = (collect_local(&mut a), collect_local(&mut b));
        pump(&mut a, &mut b, fb, fa);

        assert_eq!(
            std::fs::read(ra.join("g.md")).unwrap(),
            b"line one\nline two from B\n"
        );
        assert_eq!(
            std::fs::read(rb.join("g.md")).unwrap(),
            b"line one\nline two from B\n"
        );

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    #[test]
    fn recreated_file_rejects_the_old_tombstone() {
        let ra = dir("rc-a");
        let rb = dir("rc-b");
        std::fs::write(ra.join("h.bin"), b"same bytes").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        std::fs::remove_file(ra.join("h.bin")).unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);
        assert!(b.is_tombstoned("h.bin"));

        std::fs::write(ra.join("h.bin"), b"same bytes").unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);

        assert_eq!(std::fs::read(ra.join("h.bin")).unwrap(), b"same bytes");
        assert_eq!(std::fs::read(rb.join("h.bin")).unwrap(), b"same bytes");
        assert!(!b.is_tombstoned("h.bin"));

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    #[test]
    fn lww_record_delete_survives_canonical_rewrite() {
        let ra = dir("lw-a");
        let rb = dir("lw-b");
        std::fs::write(ra.join("meta.toml"), "b = 2\n# note\na=1\n").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        assert_eq!(std::fs::read(rb.join("meta.toml")).unwrap(), b"a = 1\nb = 2\n");

        std::fs::remove_file(ra.join("meta.toml")).unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);

        assert!(!rb.join("meta.toml").exists());

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    /// Pins the documented lww-record limit (module doc, "Known limit"): a
    /// local comment the algorithm never syncs is not a concurrent edit, so a
    /// peer's delete still applies — the same bytes any remote merge's
    /// canonical rewrite would drop. A real record edit still wins.
    #[test]
    fn lww_record_remote_delete_ignores_unsynced_comments_but_not_records() {
        let ra = dir("lc-a");
        let rb = dir("lc-b");
        std::fs::write(ra.join("meta.toml"), "a = 1\n").unwrap();
        std::fs::create_dir_all(ra.join("x")).unwrap();
        std::fs::write(ra.join("x/meta.toml"), "a = 1\n").unwrap();
        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        // B: a comment on one file, a new record on the other — unscanned.
        std::fs::write(rb.join("meta.toml"), "a = 1\n# B's note\n").unwrap();
        std::fs::write(rb.join("x/meta.toml"), "a = 1\nb = 2\n").unwrap();
        std::fs::remove_file(ra.join("meta.toml")).unwrap();
        std::fs::remove_file(ra.join("x/meta.toml")).unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);

        assert!(!rb.join("meta.toml").exists(), "comment-only change is not an edit");
        assert!(!ra.join("meta.toml").exists());
        assert_eq!(std::fs::read(rb.join("x/meta.toml")).unwrap(), b"a = 1\nb = 2\n");
        assert_eq!(std::fs::read(ra.join("x/meta.toml")).unwrap(), b"a = 1\nb = 2\n");

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    #[test]
    fn restart_remembers_an_offline_delete() {
        let ra = dir("rs-a");
        let rb = dir("rs-b");
        std::fs::write(ra.join("k.bin"), b"will be deleted offline").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        a.save_index_if_dirty();
        drop(a);

        std::fs::remove_file(ra.join("k.bin")).unwrap();
        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        assert!(a.is_tombstoned("k.bin"));

        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);
        assert!(!rb.join("k.bin").exists());

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    /// Both peers delete the same file at once (each then ignores — merges —
    /// the other's delete, since the path is already gone), then A restores
    /// byte-identical content. The restore must survive: B's re-asserted
    /// deletion is one A's re-creation overrode. Then a genuinely new delete of
    /// the restored file by B must still apply (fresh id, not the overruled one).
    #[test]
    fn concurrent_deletes_then_identical_restore_survives() {
        let ra = dir("cd-a");
        let rb = dir("cd-b");
        std::fs::write(ra.join("h.bin"), b"precious").unwrap();
        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        std::fs::remove_file(ra.join("h.bin")).unwrap();
        std::fs::remove_file(rb.join("h.bin")).unwrap();
        let (fa, fb) = (collect_local(&mut a), collect_local(&mut b));
        pump(&mut a, &mut b, fb, fa);
        assert!(a.is_tombstoned("h.bin") && b.is_tombstoned("h.bin"));

        std::fs::write(ra.join("h.bin"), b"precious").unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);
        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);
        assert_eq!(std::fs::read(ra.join("h.bin")).unwrap(), b"precious");
        assert_eq!(std::fs::read(rb.join("h.bin")).unwrap(), b"precious");
        assert!(!a.is_tombstoned("h.bin") && !b.is_tombstoned("h.bin"));

        std::fs::remove_file(rb.join("h.bin")).unwrap();
        let fb = collect_local(&mut b);
        pump(&mut a, &mut b, fb, vec![]);
        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);
        assert!(!ra.join("h.bin").exists() && !rb.join("h.bin").exists());

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    /// Same, but the two deletes NEVER reach each other (link down; on
    /// reconnect neither advertises a deleted path). Deterministic deletion
    /// ids make both tombstones carry the same id, so A's restore still
    /// overrides B's deletion.
    #[test]
    fn deletes_that_never_met_then_identical_restore_survives() {
        let ra = dir("dn-a");
        let rb = dir("dn-b");
        std::fs::write(ra.join("n.bin"), b"keep me").unwrap();
        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);

        std::fs::remove_file(ra.join("n.bin")).unwrap();
        std::fs::remove_file(rb.join("n.bin")).unwrap();
        let (_dropped_a, _dropped_b) = (collect_local(&mut a), collect_local(&mut b));
        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);

        std::fs::write(ra.join("n.bin"), b"keep me").unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);
        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);
        assert_eq!(std::fs::read(ra.join("n.bin")).unwrap(), b"keep me");
        assert_eq!(std::fs::read(rb.join("n.bin")).unwrap(), b"keep me");

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    /// A deletes and sends, then crashes before saving its index: on restart
    /// the delete is re-detected from the stale index and must mint the SAME
    /// id B adopted, so an identical restore on A still survives.
    #[test]
    fn crash_before_index_save_then_identical_restore_survives() {
        let ra = dir("cs-a");
        let rb = dir("cs-b");
        std::fs::write(ra.join("c.bin"), b"crash test").unwrap();
        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();
        sync_pair(&mut a, &mut b);
        a.save_index_if_dirty();

        std::fs::remove_file(ra.join("c.bin")).unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);
        assert!(b.is_tombstoned("c.bin"));
        drop(a); // crash: the tombstone was never saved

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        assert_eq!(a.tombstone("c.bin").unwrap().ids, b.tombstone("c.bin").unwrap().ids);

        std::fs::write(ra.join("c.bin"), b"crash test").unwrap();
        let fa = collect_local(&mut a);
        pump(&mut a, &mut b, vec![], fa);
        let (ha, hb) = (advertise_all(&mut a), advertise_all(&mut b));
        pump(&mut a, &mut b, hb, ha);
        assert_eq!(std::fs::read(ra.join("c.bin")).unwrap(), b"crash test");
        assert_eq!(std::fs::read(rb.join("c.bin")).unwrap(), b"crash test");

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }

    /// A peer's delete for a path we already deleted is merged, not ignored:
    /// its id joins our tombstone, so our later re-creation overrides it and
    /// the peer's re-assert of that id is rejected. A huge id list is capped.
    #[test]
    fn already_deleted_path_merges_a_peers_deletion_id() {
        let ra = dir("mi-a");
        std::fs::write(ra.join("m.bin"), b"mine").unwrap();
        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let _ = collect_local(&mut a);
        std::fs::remove_file(ra.join("m.bin")).unwrap();
        let _ = collect_local(&mut a);
        let seen = a.tombstone("m.bin").unwrap().seen.clone();

        let peer_delete = encode(&SyncMsg::StratDelete {
            path: "m.bin".into(),
            ids: vec![0xDEAD_BEEF],
            seen: seen.clone(),
        });
        assert!(deliver(&mut a, &peer_delete).is_empty());
        assert!(a.tombstone("m.bin").unwrap().ids.contains(&0xDEAD_BEEF));

        let flood = encode(&SyncMsg::StratDelete { path: "m.bin".into(), ids: (0..1000).collect(), seen: vec![] });
        let _ = deliver(&mut a, &flood);
        assert!(a.tombstone("m.bin").unwrap().ids.len() <= DELETION_IDS_CAP);
        // The flood pushed 0xDEAD_BEEF out of the capped list; merge it back.
        let _ = deliver(&mut a, &peer_delete);

        std::fs::write(ra.join("m.bin"), b"mine").unwrap();
        let _ = collect_local(&mut a);
        let replies = deliver(&mut a, &peer_delete);
        assert_eq!(std::fs::read(ra.join("m.bin")).unwrap(), b"mine");
        assert!(!a.is_tombstoned("m.bin"));
        assert!(matches!(
            postcard::from_bytes::<SyncMsg>(&replies[0]).unwrap(),
            SyncMsg::StratDelta { .. }
        ));

        std::fs::remove_dir_all(&ra).ok();
    }

    #[tokio::test]
    async fn live_sessions_propagate_a_delete() {
        use crate::sync::workspace::INDEX_FILE;

        let ra = dir("live-a");
        let rb = dir("live-b");
        std::fs::write(ra.join("a.bin"), b"a live file to delete").unwrap();

        let mut a = Workspace::new(&ra, text_manifest(), false).unwrap();
        let mut b = Workspace::new(&rb, text_manifest(), false).unwrap();

        let (link_a, link_b) = crate::net::mock_pair();
        let (mut sa, mut ra_src) = link_a.into_halves();
        let (mut sb, mut rb_src) = link_b.into_halves();
        let (_ta, mut wa) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (_tb, mut wb) = tokio::sync::mpsc::unbounded_channel::<()>();

        let sess_a = session(&mut a, &mut wa, &mut sa, &mut ra_src);
        let sess_b = session(&mut b, &mut wb, &mut sb, &mut rb_src);

        let script = async {
            while !rb.join("a.bin").is_file() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            std::fs::remove_file(ra.join("a.bin")).unwrap();
            while rb.join("a.bin").exists() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
            assert!(!ra.join("a.bin").exists());
            assert!(!rb.join("a.bin").exists());
        };

        tokio::select! {
            r = sess_a => panic!("A ended early: {r:?}"),
            r = sess_b => panic!("B ended early: {r:?}"),
            r = tokio::time::timeout(Duration::from_secs(20), script) => {
                r.expect("delete did not propagate");
            }
        }

        assert!(ra.join(INDEX_FILE).is_file());

        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }
}
