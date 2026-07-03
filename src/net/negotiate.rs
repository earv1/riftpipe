//! Connection-time capability / transport negotiation
//! (`docs/planned/transport-negotiation.md`).
//!
//! Runs over the already-authenticated iroh [`Link`] as a `CAPS` exchange: each
//! side advertises the transports it can attempt; both deterministically pick the
//! highest mutually-supported rung of the ladder. The iroh `Link` is always the
//! **floor** (every peer lists `IrohRelay`), so negotiation can only *upgrade* a
//! pair — never leave them unable to talk. This is the seam the WebRTC data plane
//! and the browser build plug into later; today only the iroh rungs are realized.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::net::transport::IrohLink;
use crate::net::webrtc::upgrade_to_webrtc;
use crate::net::{anyerr, Counters, Link, Result, Sink, Source};

/// Wire-format version of the capability exchange. Bumped on any breaking change
/// to [`Caps`]; both peers are the same binary in our demos, so a mismatch is a
/// real (rejected) incompatibility rather than something to paper over. (Reject
/// on mismatch is a TODO once we actually ship two versions.)
pub const PROTO_VERSION: u16 = 1;

/// The transport ladder. `rank()` defines a *global* total order so both peers,
/// computing the intersection independently, always select the same rung.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transport {
    /// Direct WebRTC DataChannel (browser `web-sys` / native `webrtc-rs`).
    /// LAN/NAT-direct data; needs both sides to carry a WebRTC stack. Not yet built.
    WebrtcDirect,
    /// Direct iroh QUIC (native↔native hole-punch, §14.1). The native best case.
    IrohDirect,
    /// iroh over relay — the universal floor, and the channel that brokers the
    /// WebRTC handshake. Every peer advertises this.
    IrohRelay,
}

impl Transport {
    /// Higher = more preferred / more direct. The selection key.
    fn rank(self) -> u8 {
        match self {
            Transport::WebrtcDirect => 2,
            Transport::IrohDirect => 1,
            Transport::IrohRelay => 0,
        }
    }
}

/// What a peer can attempt + a tie-breaker. Postcard-encoded like [`crate::net::secure::Ticket`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Caps {
    pub proto_version: u16,
    /// Transports this peer can attempt, the peer's own preference order. Selection
    /// uses the global `rank()`, but the list is kept ordered for intent/future use.
    pub transports: Vec<Transport>,
    pub role: Role,
    /// Deterministic offerer selection for symmetric upgrades (lower offers). A
    /// per-process random value; collisions are astronomically unlikely.
    pub tie_break: u64,
}

/// Informational — who's on each end. Drives nothing today; documents intent and
/// will inform defaults (e.g. a `Server` as a hub) when multi-peer lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Browser,
    Native,
    Server,
}

impl Caps {
    /// What the native CLI advertises: WebRTC preferred (it carries `webrtc-rs`),
    /// then iroh-direct, with the relay floor. Native↔native now upgrades to a
    /// WebRTC data channel (brokered over the iroh link); the iroh link stays as
    /// control/fallback, and an upgrade failure transparently stays on iroh.
    pub fn native() -> Self {
        Caps {
            proto_version: PROTO_VERSION,
            transports: vec![
                Transport::WebrtcDirect,
                Transport::IrohDirect,
                Transport::IrohRelay,
            ],
            role: Role::Native,
            tie_break: rand::random(),
        }
    }
}

/// The agreed result of a negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    /// The chosen transport — identical on both peers.
    pub transport: Transport,
    /// Whether *we* send the WebRTC offer (only meaningful for `WebrtcDirect`).
    /// Anti-symmetric: exactly one peer gets `true`.
    pub we_offer: bool,
}

/// Pick the highest-ranked transport both peers list. Deterministic and symmetric
/// in `transport` (max over the intersection), anti-symmetric in `we_offer`.
pub fn negotiate(local: &Caps, remote: &Caps) -> Outcome {
    let transport = local
        .transports
        .iter()
        .copied()
        .filter(|t| remote.transports.contains(t))
        .max_by_key(|t| t.rank())
        // Both peers advertise the floor, so the intersection is never empty; the
        // fallback is pure defense.
        .unwrap_or(Transport::IrohRelay);

    Outcome {
        transport,
        // `<=` so an (astronomically rare) tie_break collision makes BOTH sides
        // offer — which fails cleanly ("expected answer, got offer") rather than
        // both answering and hanging forever waiting for an offer.
        we_offer: local.tie_break <= remote.tie_break,
    }
}

/// Exchange `CAPS` over the link and return the negotiated outcome. Send-then-recv,
/// same shape as [`crate::net::secure::authenticate`] — `send` is buffered, so both
/// sides may send first without deadlock.
pub async fn exchange_caps(link: &mut dyn Link, local: &Caps) -> Result<Outcome> {
    let bytes = postcard::to_allocvec(local).map_err(anyerr)?;
    link.send(bytes).await?;
    let peer = link
        .recv()
        .await?
        .ok_or_else(|| anyerr("caps: peer sent no capabilities"))?;
    let remote: Caps = postcard::from_bytes(&peer).map_err(anyerr)?;
    Ok(negotiate(local, &remote))
}

/// Negotiate the data transport over the (authenticated) iroh `link`, optionally
/// upgrade to WebRTC, and return the session's send/recv halves
/// (`docs/planned/transport-negotiation.md`). Shared by `--pipe` and folder mode.
///
/// On a WebRTC upgrade the iroh link is returned as the `keepalive` (control /
/// fallback — and it keeps the QUIC connection alive so metrics' `connection_kind`
/// still resolves); on iroh transport it's consumed into the halves. An upgrade
/// failure transparently falls back to iroh. The returned `Transport` is what the
/// data plane actually ended up using.
pub async fn negotiate_session_halves(
    mut link: IrohLink,
    counters: Arc<Counters>,
) -> (Box<dyn Sink>, Box<dyn Source>, Option<IrohLink>, Transport) {
    let outcome = exchange_caps(&mut link, &Caps::native()).await;
    if let Ok(o) = &outcome {
        if o.transport == Transport::WebrtcDirect {
            match upgrade_to_webrtc(&mut link, o.we_offer).await {
                Ok(w) => {
                    let (sink, source) = w.into_halves(counters);
                    return (Box::new(sink), Box::new(source), Some(link), Transport::WebrtcDirect);
                }
                Err(e) => eprintln!("[riftpipe] webrtc upgrade failed ({e}); staying on iroh"),
            }
        }
    }
    // Either iroh was chosen, caps failed, or the WebRTC upgrade fell back. We're
    // on the iroh link now; report iroh-direct rather than the unrealized webrtc.
    let transport = match outcome {
        Ok(o) if o.transport != Transport::WebrtcDirect => o.transport,
        _ => Transport::IrohDirect,
    };
    let (sink, source) = link.into_halves(counters);
    (Box::new(sink), Box::new(source), None, transport)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;

    fn caps(transports: Vec<Transport>, tie_break: u64) -> Caps {
        Caps {
            proto_version: PROTO_VERSION,
            transports,
            role: Role::Native,
            tie_break,
        }
    }

    #[test]
    fn picks_highest_common_rung() {
        // Both speak WebRTC + relay → WebRTC wins.
        let a = caps(vec![Transport::WebrtcDirect, Transport::IrohRelay], 1);
        let b = caps(vec![Transport::WebrtcDirect, Transport::IrohRelay], 2);
        assert_eq!(negotiate(&a, &b).transport, Transport::WebrtcDirect);
    }

    #[test]
    fn native_pair_negotiates_webrtc() {
        // Two native CLIs both carry webrtc-rs → upgrade to a WebRTC data channel.
        let (a, b) = (Caps::native(), Caps::native());
        assert_eq!(negotiate(&a, &b).transport, Transport::WebrtcDirect);
    }

    #[test]
    fn falls_back_to_relay_floor_when_no_overlap_above_it() {
        // A browser (webrtc only) vs an iroh-only native: only the relay floor is
        // shared → relay. (Both must always list the floor.)
        let browser = caps(vec![Transport::WebrtcDirect, Transport::IrohRelay], 1);
        let native_iroh = caps(vec![Transport::IrohDirect, Transport::IrohRelay], 2);
        assert_eq!(negotiate(&browser, &native_iroh).transport, Transport::IrohRelay);
    }

    #[test]
    fn offerer_is_deterministic_and_anti_symmetric() {
        let a = caps(vec![Transport::WebrtcDirect, Transport::IrohRelay], 10);
        let b = caps(vec![Transport::WebrtcDirect, Transport::IrohRelay], 20);
        // Lower tie_break offers; exactly one side gets we_offer.
        assert!(negotiate(&a, &b).we_offer);
        assert!(!negotiate(&b, &a).we_offer);
    }

    #[tokio::test]
    async fn exchange_over_link_agrees_on_both_ends() {
        let webrtc = vec![Transport::WebrtcDirect, Transport::IrohRelay];
        let ca = caps(webrtc.clone(), 1);
        let cb = caps(webrtc, 2);
        let (mut la, mut lb) = mock_pair();
        let (ra, rb) = tokio::join!(exchange_caps(&mut la, &ca), exchange_caps(&mut lb, &cb));
        let (oa, ob) = (ra.unwrap(), rb.unwrap());
        // Same transport on both ends; opposite offerer roles.
        assert_eq!(oa.transport, Transport::WebrtcDirect);
        assert_eq!(ob.transport, Transport::WebrtcDirect);
        assert_ne!(oa.we_offer, ob.we_offer);
    }
}
