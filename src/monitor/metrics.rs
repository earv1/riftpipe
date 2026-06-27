//! Metrics emission (DESIGN.md §14.2). riftpipe renders nothing itself — it
//! writes a one-line status to a file every ~0.5s and lets **tmux** display it (a
//! thin pane per peer). This is decoupled from the sync loop: a side-car task
//! that reads the byte counters and polls the connection path. No TUI, no
//! drawing — tmux is the compositor.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::{Endpoint, EndpointId};

use crate::net::Counters;

/// How the connection is currently routed (drives the relay warning, §14.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum ConnKind {
    #[default]
    Unknown,
    Direct,
    Relay,
}

/// Is the live connection to `peer` direct (holepunched) or via a relay? Only the
/// actively-used path is considered.
pub async fn connection_kind(endpoint: &Endpoint, peer: EndpointId) -> ConnKind {
    let Some(info) = endpoint.remote_info(peer).await else {
        return ConnKind::Unknown;
    };
    let (mut direct, mut relay) = (false, false);
    for a in info.addrs() {
        if format!("{:?}", a.usage()) == "Active" {
            if a.addr().is_ip() {
                direct = true;
            }
            if a.addr().is_relay() {
                relay = true;
            }
        }
    }
    if direct {
        ConnKind::Direct
    } else if relay {
        ConnKind::Relay
    } else {
        ConnKind::Unknown
    }
}

/// The one-line status tmux displays.
pub fn format_line(title: &str, conn: ConnKind, sent: u64, recv: u64, rate: f64) -> String {
    let c = match conn {
        ConnKind::Direct => "direct",
        ConnKind::Relay => "RELAY",
        ConnKind::Unknown => "…",
    };
    let warn = if conn == ConnKind::Relay {
        " ⚠relay(n0:metadata exposed)"
    } else {
        ""
    };
    format!("{title}  {c}{warn}  ↑{sent}B ↓{recv}B  {rate:.0}B/s")
}

/// Spawn a decoupled writer that refreshes `path` every ~0.5s — wired to a tmux
/// pane. Independent of the sync loop, so it adds no cruft to it.
pub fn spawn(
    endpoint: Endpoint,
    peer: EndpointId,
    counters: Arc<Counters>,
    path: String,
    title: String,
) {
    tokio::spawn(async move {
        let mut last_total = 0u64;
        let mut last = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let sent = counters.sent.load(Ordering::Relaxed);
            let recv = counters.recv.load(Ordering::Relaxed);
            let total = sent + recv;
            let dt = last.elapsed().as_secs_f64().max(0.001);
            let rate = total.saturating_sub(last_total) as f64 / dt;
            last_total = total;
            last = Instant::now();
            let conn = connection_kind(&endpoint, peer).await;
            let _ = std::fs::write(&path, format_line(&title, conn, sent, recv, rate) + "\n");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_shows_warning_direct_does_not() {
        let relay = format_line("a.txt", ConnKind::Relay, 100, 50, 200.0);
        assert!(relay.contains("RELAY") && relay.contains("⚠"));
        let direct = format_line("a.txt", ConnKind::Direct, 100, 50, 200.0);
        assert!(direct.contains("direct") && !direct.contains("⚠"));
    }
}
