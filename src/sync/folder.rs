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

use std::sync::Arc;
use std::time::Duration;

use iroh::Endpoint;
use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, MissedTickBehavior};

use riftpipe_core::sync::SyncMsg;

use crate::net::secure::authenticate;
use crate::net::negotiate::negotiate_session_halves;
use crate::net::transport::{accept_link, connect_link};
use crate::net::{Counters, Result, Sink, Source};
use crate::sync::pipe::{Role, SessionOutcome};
use crate::sync::workspace::Workspace;

fn advert(path: &str, sv: Vec<u8>, reply: bool) -> Vec<u8> {
    encode(&SyncMsg::StratAdvert { path: path.to_string(), sv, reply })
}

fn delta(path: &str, bytes: Vec<u8>) -> Vec<u8> {
    encode(&SyncMsg::StratDelta { path: path.to_string(), bytes })
}

fn encode(msg: &SyncMsg) -> Vec<u8> {
    // postcard on an in-memory Vec cannot fail for these shapes.
    postcard::to_allocvec(msg).expect("SyncMsg encodes")
}

/// Scan local backings for changes and push them — the shared body of the
/// filesystem-watch arm and the fallback poll. Picks up newly-created files,
/// then for each resource `observe`s its bytes; on a real change it pushes a
/// delta and a reply-expecting advert. Returns `false` if a send failed, so the caller
/// can treat that as `LinkClosed`. (Self-write safe: merging a remote change
/// stores bytes that `observe` then reports as *unchanged*, so a scan triggered
/// by our own write is a no-op — no echo, no extra dedup needed.)
async fn scan_local(ws: &mut Workspace, sink: &mut dyn Sink) -> bool {
    let _ = ws.refresh_disk(); // pick up newly-created files
    for path in ws.paths() {
        // Observe under a short borrow, then send without holding it.
        let (changed, push, sv) = match ws.get_mut(&path) {
            Some(res) => {
                let bytes = res.backing.load();
                if res.strategy.observe(&bytes) {
                    (true, res.strategy.push_delta(), res.strategy.state_vector())
                } else {
                    (false, None, Vec::new())
                }
            }
            None => continue,
        };
        if changed {
            if let Some(d) = push {
                if sink.send(delta(&path, d)).await.is_err() {
                    return false;
                }
            }
            if sink.send(advert(&path, sv, true)).await.is_err() {
                return false;
            }
        }
    }
    true
}

/// One sync session over a single link, driving every resource in `ws`. Returns
/// `LinkClosed` on any link error (so the reconnect loop re-dials); folder mode
/// has no stdin, so it never returns `StdinClosed`. `watch_rx` is owned by the
/// reconnect loop (fed by a filesystem watcher) and **persists across sessions**.
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
    let mut heartbeat = interval(Duration::from_secs(5));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Announce our protocol version first, so a mismatched peer is surfaced.
    if sink
        .send(encode(&SyncMsg::Version { proto: riftpipe_core::sync::PROTOCOL_VERSION }))
        .await
        .is_err()
    {
        return Ok(SessionOutcome::LinkClosed);
    }
    // On connect, advertise everything we hold so the peer can reconcile.
    for path in ws.paths() {
        let sv = match ws.get_mut(&path) {
            Some(r) => r.strategy.state_vector(),
            None => continue,
        };
        if sink.send(advert(&path, sv, true)).await.is_err() {
            return Ok(SessionOutcome::LinkClosed);
        }
    }

    loop {
        tokio::select! {
            // Primary path: the OS told us something under the root changed.
            nudge = watch_rx.recv() => {
                // `None` means the watcher was dropped (shouldn't happen while a
                // session runs — the reconnect loop holds it); the fallback poll
                // covers us either way.
                if nudge.is_some() {
                    while watch_rx.try_recv().is_ok() {} // coalesce a burst of events
                    if !scan_local(ws, sink).await {
                        return Ok(SessionOutcome::LinkClosed);
                    }
                }
            }
            // Fallback: rescan slowly in case the watcher missed an event.
            _ = fallback.tick() => {
                if !scan_local(ws, sink).await {
                    return Ok(SessionOutcome::LinkClosed);
                }
            }
            msg = source.recv() => {
                let bytes = match msg {
                    Err(_) | Ok(None) => return Ok(SessionOutcome::LinkClosed),
                    Ok(Some(b)) => b,
                };
                // Undecodable or non-strategy messages are skipped, exactly as
                // the tree/mesh drivers skip strategy messages — one envelope,
                // two dialects, no cross-talk.
                let Ok(msg) = postcard::from_bytes::<SyncMsg>(&bytes) else { continue };
                let mut outgoing: Vec<Vec<u8>> = Vec::new();
                match msg {
                    SyncMsg::StratDelta { path, bytes } => {
                        if let Some(res) = ws.ensure(&path) {
                            if let Some(nb) = res.strategy.merge(&bytes) {
                                res.backing.store(&nb);
                            }
                        }
                    }
                    SyncMsg::StratAdvert { path, sv, reply } => {
                        if let Some(res) = ws.ensure(&path) {
                            if let Some(d) = res.strategy.delta_since(&sv) {
                                outgoing.push(delta(&path, d));
                            }
                            if reply {
                                outgoing.push(advert(&path, res.strategy.state_vector(), false));
                            }
                        }
                    }
                    SyncMsg::Version { proto } => {
                        if proto != riftpipe_core::sync::PROTOCOL_VERSION {
                            eprintln!(
                                "[folder] protocol-mismatch theirs={proto} ours={} — \
                                 sync may be incomplete",
                                riftpipe_core::sync::PROTOCOL_VERSION,
                            );
                        }
                    }
                    _ => {}
                }
                for f in outgoing {
                    if sink.send(f).await.is_err() {
                        return Ok(SessionOutcome::LinkClosed);
                    }
                }
            }
            _ = heartbeat.tick() => {
                for path in ws.paths() {
                    let sv = match ws.get_mut(&path) {
                        Some(r) => r.strategy.state_vector(),
                        None => continue,
                    };
                    if sink.send(advert(&path, sv, true)).await.is_err() {
                        return Ok(SessionOutcome::LinkClosed);
                    }
                }
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
    /// must be ignored — no resource created, no reply. (The session loop's
    /// `_ => {}` arm; here we just pin that the dialects are distinguishable.)
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
}
