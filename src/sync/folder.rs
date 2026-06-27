//! Folder mode (DESIGN.md §17) — multiplex many resources over one link, each
//! driven by its own [`Syncer`](crate::sync::syncer::Syncer). This is the
//! `--pipe` reconnecting session generalized from one document to a whole
//! [`Workspace`]: same link plumbing, same reconnect loop, but every frame is
//! tagged with a **resource id** (the relative path) and local changes are
//! discovered by **watching** the filesystem (an OS event nudges a rescan;
//! a slow poll stays on as a safety net) rather than read from stdin.
//!
//! ## Wire framing
//! `[path_len: u16 LE][path bytes][tag: u8][payload]`. Tags:
//!   * `DELTA`    — a patch to [`merge`](crate::sync::syncer::Syncer::merge).
//!   * `SYNCREQ`  — an advertisement that *expects a reply* (sent on connect,
//!     on a local change, and on the heartbeat).
//!   * `SYNCREP`  — an advertisement *in reply* to a `SYNCREQ` (no further
//!     reply, so the exchange can't ping-pong forever).
//!
//! Why request/reply: a push-capable algorithm (text CRDT) ships its change
//! immediately as a `DELTA`. A pull-only one (rsync) can't — it needs the
//! peer's signatures first. So a local change also sends a `SYNCREQ`; the peer
//! answers with a `SYNCREP` carrying *its* signatures, and we then compute and
//! push the rsync `DELTA`. Receiving an unknown path auto-creates the resource
//! (peer-driven discovery).

use std::sync::Arc;
use std::time::Duration;

use iroh::Endpoint;
use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, MissedTickBehavior};

use crate::net::secure::authenticate;
use crate::net::transport::{accept_link, connect_link};
use crate::net::{Counters, Result};
use crate::sync::pipe::{PipeSink, PipeSource, Role, SessionOutcome};
use crate::sync::workspace::Workspace;

const TAG_DELTA: u8 = 0;
const TAG_SYNCREQ: u8 = 1;
const TAG_SYNCREP: u8 = 2;

fn frame(path: &str, tag: u8, payload: &[u8]) -> Vec<u8> {
    let p = path.as_bytes();
    let mut v = Vec::with_capacity(p.len() + payload.len() + 3);
    v.extend_from_slice(&(p.len() as u16).to_le_bytes());
    v.extend_from_slice(p);
    v.push(tag);
    v.extend_from_slice(payload);
    v
}

fn parse_frame(bytes: &[u8]) -> Option<(&str, u8, &[u8])> {
    if bytes.len() < 3 {
        return None;
    }
    let plen = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    if bytes.len() < 2 + plen + 1 {
        return None;
    }
    let path = std::str::from_utf8(&bytes[2..2 + plen]).ok()?;
    let tag = bytes[2 + plen];
    Some((path, tag, &bytes[2 + plen + 1..]))
}

/// Scan local backings for changes and push them — the shared body of the
/// filesystem-watch arm and the fallback poll. Picks up newly-created files,
/// then for each resource `observe`s its bytes; on a real change it pushes a
/// `DELTA` and a `SYNCREQ`. Returns `false` if a send failed, so the caller
/// can treat that as `LinkClosed`. (Self-write safe: merging a remote change
/// stores bytes that `observe` then reports as *unchanged*, so a scan triggered
/// by our own write is a no-op — no echo, no extra dedup needed.)
async fn scan_local(ws: &mut Workspace, sink: &mut dyn PipeSink) -> bool {
    let _ = ws.refresh_disk(); // pick up newly-created files
    for path in ws.paths() {
        // Observe under a short borrow, then send without holding it.
        let (changed, push, sv) = match ws.get_mut(&path) {
            Some(res) => {
                let bytes = res.backing.load();
                if res.syncer.observe(&bytes) {
                    (true, res.syncer.push_delta(), res.syncer.state_vector())
                } else {
                    (false, None, Vec::new())
                }
            }
            None => continue,
        };
        if changed {
            if let Some(d) = push {
                if sink.send(frame(&path, TAG_DELTA, &d)).await.is_err() {
                    return false;
                }
            }
            if sink.send(frame(&path, TAG_SYNCREQ, &sv)).await.is_err() {
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
    sink: &mut dyn PipeSink,
    source: &mut dyn PipeSource,
) -> Result<SessionOutcome> {
    // Safety-net poll: watchers can drop events on some network/edge
    // filesystems, so we still rescan slowly. This is the fallback, NOT the
    // primary path — the `watch_rx` arm below is what reacts promptly.
    let mut fallback = interval(Duration::from_secs(2));
    fallback.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat = interval(Duration::from_secs(5));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // On connect, advertise everything we hold so the peer can reconcile.
    for path in ws.paths() {
        let sv = match ws.get_mut(&path) {
            Some(r) => r.syncer.state_vector(),
            None => continue,
        };
        if sink.send(frame(&path, TAG_SYNCREQ, &sv)).await.is_err() {
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
                let Some((path, tag, payload)) = parse_frame(&bytes) else { continue };
                let path = path.to_string();
                let payload = payload.to_vec();
                let mut outgoing: Vec<Vec<u8>> = Vec::new();
                if let Some(res) = ws.ensure(&path) {
                    match tag {
                        TAG_DELTA => {
                            if let Some(nb) = res.syncer.merge(&payload) {
                                res.backing.store(&nb);
                            }
                        }
                        TAG_SYNCREQ => {
                            if let Some(d) = res.syncer.delta_since(&payload) {
                                outgoing.push(frame(&path, TAG_DELTA, &d));
                            }
                            outgoing.push(frame(&path, TAG_SYNCREP, &res.syncer.state_vector()));
                        }
                        TAG_SYNCREP => {
                            if let Some(d) = res.syncer.delta_since(&payload) {
                                outgoing.push(frame(&path, TAG_DELTA, &d));
                            }
                        }
                        _ => {}
                    }
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
                        Some(r) => r.syncer.state_vector(),
                        None => continue,
                    };
                    if sink.send(frame(&path, TAG_SYNCREQ, &sv)).await.is_err() {
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
                eprintln!("[riftpipe] connected — syncing folder");
                let (mut sink, mut source) = link.into_halves(counters.clone());
                match session(&mut ws, &mut watch_rx, &mut sink, &mut source).await? {
                    SessionOutcome::StdinClosed => {
                        sink.finish().await;
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
    fn frame_round_trips() {
        let f = frame("docs/a.md", TAG_DELTA, b"payload");
        let (p, t, pl) = parse_frame(&f).unwrap();
        assert_eq!(p, "docs/a.md");
        assert_eq!(t, TAG_DELTA);
        assert_eq!(pl, b"payload");
        // truncated frames don't panic
        assert!(parse_frame(&f[..2]).is_some() || parse_frame(&f[..1]).is_none());
        assert!(parse_frame(b"").is_none());
    }

    /// Walk the REQ -> REP -> DELTA exchange (what `session` does) by hand and
    /// confirm a file A holds lands on B's disk via rsync.
    #[test]
    fn one_file_syncs_from_a_to_b() {
        let ra = dir("a");
        let rb = dir("b");
        let content = b"rsync this whole file across the link, please and thanks";
        std::fs::write(ra.join("a.bin"), content).unwrap();

        let mut a = Workspace::new(&ra, Manifest::default(), false).unwrap();
        let mut b = Workspace::new(&rb, Manifest::default(), false).unwrap();

        // A's poll: observe local file into its syncer, then advertise (REQ).
        let a_req = {
            let r = a.get_mut("a.bin").unwrap();
            let bytes = r.backing.load();
            assert!(r.syncer.observe(&bytes));
            frame("a.bin", TAG_SYNCREQ, &r.syncer.state_vector())
        };

        // B receives REQ: nothing to push (B is empty) -> replies REP(empty).
        let b_rep = {
            let (path, tag, payload) = parse_frame(&a_req).unwrap();
            assert_eq!(tag, TAG_SYNCREQ);
            let r = b.ensure(path).unwrap();
            assert!(r.syncer.delta_since(payload).is_none());
            frame(path, TAG_SYNCREP, &r.syncer.state_vector())
        };

        // A receives REP: computes the rsync delta for B and pushes DELTA.
        let a_delta = {
            let (path, _tag, payload) = parse_frame(&b_rep).unwrap();
            let r = a.get_mut(path).unwrap();
            let d = r.syncer.delta_since(payload).expect("A has content B lacks");
            frame(path, TAG_DELTA, &d)
        };

        // B receives DELTA: merges and writes to disk.
        {
            let (path, tag, payload) = parse_frame(&a_delta).unwrap();
            assert_eq!(tag, TAG_DELTA);
            let r = b.ensure(path).unwrap();
            let nb = r.syncer.merge(payload).expect("B materializes content");
            r.backing.store(&nb);
        }

        assert_eq!(std::fs::read(rb.join("a.bin")).unwrap(), content);
        std::fs::remove_dir_all(&ra).ok();
        std::fs::remove_dir_all(&rb).ok();
    }
}
