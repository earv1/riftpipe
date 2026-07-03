//! WebRTC data-plane `Link` + iroh-brokered signaling
//! (`docs/planned/transport-negotiation.md` §3).
//!
//! When negotiation ([`crate::net::negotiate`]) selects `WebrtcDirect`, the two
//! peers run a **non-trickle** offer/answer over the already-authenticated iroh
//! `Link` (iroh's relay is the signaling rendezvous — we run no signaling server),
//! establish an `RTCDataChannel`, and wrap it as a [`Link`] so the existing sync
//! stack runs over it unchanged. Native↔native uses `webrtc-rs` on both ends; the
//! browser side will use the platform WebRTC via `web-sys` behind the same flow.
//!
//! Non-trickle = each side gathers ICE fully, then sends one complete SDP. That
//! collapses signaling to exactly two messages (Offer, Answer) and avoids a
//! candidate-streaming protocol over the link.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::net::{anyerr, Counters, Link, Result};

/// Label for the single data channel a session uses.
const CHANNEL_LABEL: &str = "riftpipe";

/// Signaling exchanged over the iroh link to broker the WebRTC channel.
#[derive(Serialize, Deserialize)]
enum Signal {
    /// Full SDP offer with gathered candidates (non-trickle).
    Offer(String),
    /// Full SDP answer with gathered candidates.
    Answer(String),
}

/// A [`Link`] over an established WebRTC `RTCDataChannel`. Outbound writes go
/// straight to the channel; inbound messages are pumped from the channel's
/// `on_message` callback into a queue that `recv` drains. Holds the peer
/// connection so it stays alive for the link's lifetime.
pub struct WebrtcLink {
    _pc: Arc<RTCPeerConnection>,
    dc: Arc<RTCDataChannel>,
    inbound: mpsc::Receiver<Vec<u8>>,
}

#[async_trait]
impl Link for WebrtcLink {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        self.dc.send(&Bytes::from(msg)).await.map_err(anyerr)?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        // `None` once the channel closes and the queue drains — a clean EOF, same
        // contract as the iroh link.
        Ok(self.inbound.recv().await)
    }

    async fn done(&mut self) -> Result<()> {
        let _ = self.dc.close().await;
        Ok(())
    }
}

impl WebrtcLink {
    /// Split into independent send/recv halves for the event-driven session — the
    /// WebRTC analogue of `IrohLink::into_halves`. `counters` is shared with the
    /// metrics writer so byte rates show through whichever transport is live.
    pub fn into_halves(self, counters: Arc<Counters>) -> (WebrtcSink, WebrtcSource) {
        (
            WebrtcSink {
                _pc: self._pc.clone(),
                dc: self.dc,
                counters: counters.clone(),
            },
            WebrtcSource {
                _pc: self._pc,
                inbound: self.inbound,
                counters,
            },
        )
    }
}

/// Send half of a split WebRTC link.
pub struct WebrtcSink {
    _pc: Arc<RTCPeerConnection>, // keep the connection alive while either half lives
    dc: Arc<RTCDataChannel>,
    counters: Arc<Counters>,
}

/// Receive half of a split WebRTC link.
pub struct WebrtcSource {
    _pc: Arc<RTCPeerConnection>,
    inbound: mpsc::Receiver<Vec<u8>>,
    counters: Arc<Counters>,
}

#[async_trait::async_trait]
impl crate::net::Sink for WebrtcSink {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        self.counters
            .sent
            .fetch_add(msg.len() as u64, Ordering::Relaxed);
        self.dc.send(&Bytes::from(msg)).await.map_err(anyerr)?;
        Ok(())
    }

    async fn finish(&mut self) {
        let _ = self.dc.close().await;
    }
}

#[async_trait::async_trait]
impl crate::net::Source for WebrtcSource {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        let msg = self.inbound.recv().await;
        if let Some(b) = &msg {
            self.counters.recv.fetch_add(b.len() as u64, Ordering::Relaxed);
        }
        Ok(msg)
    }
}

/// ICE servers from the environment (self-host-friendly, never n0):
/// - `RIFTPIPE_STUN` — comma-separated STUN URLs (e.g. `stun:stun.l.google.com:19302`).
/// - `RIFTPIPE_TURN` (+ `RIFTPIPE_TURN_USER` / `RIFTPIPE_TURN_PASS`) — a TURN relay
///   for the hostile-NAT fallback the design calls user-supplied (§transport-negotiation).
///
/// Empty by default → **host candidates only** (LAN / loopback / public-IP direct),
/// which keeps offline/loopback runs working with no external dependency.
fn ice_servers() -> Vec<RTCIceServer> {
    let mut servers = Vec::new();
    if let Ok(stun) = std::env::var("RIFTPIPE_STUN") {
        for url in stun.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            servers.push(RTCIceServer {
                urls: vec![url.to_string()],
                ..Default::default()
            });
        }
    }
    if let Ok(turn) = std::env::var("RIFTPIPE_TURN") {
        if !turn.trim().is_empty() {
            servers.push(RTCIceServer {
                urls: vec![turn.trim().to_string()],
                username: std::env::var("RIFTPIPE_TURN_USER").unwrap_or_default(),
                credential: std::env::var("RIFTPIPE_TURN_PASS").unwrap_or_default(),
            });
        }
    }
    servers
}

/// Build a peer connection with the configured ICE servers. With
/// `RIFTPIPE_FORCE_RELAY` set, ICE is restricted to **relay (TURN) candidates
/// only** — host and srflx are banned, so the peers must traverse the TURN server.
/// This exercises the worst-case (symmetric/CGNAT) path on a single machine.
async fn new_peer_connection() -> Result<Arc<RTCPeerConnection>> {
    use webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy;
    let api = APIBuilder::new()
        .with_media_engine(MediaEngine::default())
        .build();
    let config = RTCConfiguration {
        ice_servers: ice_servers(),
        ice_transport_policy: if std::env::var("RIFTPIPE_FORCE_RELAY").is_ok() {
            RTCIceTransportPolicy::Relay
        } else {
            RTCIceTransportPolicy::default()
        },
        ..Default::default()
    };
    let pc = api.new_peer_connection(config).await.map_err(anyerr)?;
    Ok(Arc::new(pc))
}

/// Wire a data channel's callbacks: `on_message` → inbound queue, `on_open` →
/// deliver the (now usable) channel through `ready`. Both senders are `mpsc`
/// (clonable), so this works whether called directly (offerer) or inside the
/// `on_data_channel` `FnMut` (answerer).
fn wire_channel(
    dc: &Arc<RTCDataChannel>,
    inbound: mpsc::Sender<Vec<u8>>,
    ready: mpsc::Sender<Arc<RTCDataChannel>>,
) {
    let dc_for_open = dc.clone();
    dc.on_open(Box::new(move || {
        let ready = ready.clone();
        let dc = dc_for_open.clone();
        Box::pin(async move {
            let _ = ready.send(dc).await;
        })
    }));
    dc.on_message(Box::new(move |msg: DataChannelMessage| {
        let inbound = inbound.clone();
        Box::pin(async move {
            let _ = inbound.send(msg.data.to_vec()).await;
        })
    }));
}

/// Establish a WebRTC `Link` over the signaling `link`. `we_offer` (from
/// [`crate::net::negotiate::Outcome`]) makes this side the offerer; exactly one
/// peer offers. Returns once the data channel is open.
pub async fn upgrade_to_webrtc(link: &mut dyn Link, we_offer: bool) -> Result<WebrtcLink> {
    let pc = new_peer_connection().await?;
    let (in_tx, in_rx) = mpsc::channel::<Vec<u8>>(256);
    let (ready_tx, mut ready_rx) = mpsc::channel::<Arc<RTCDataChannel>>(1);

    if we_offer {
        let dc = pc
            .create_data_channel(CHANNEL_LABEL, None)
            .await
            .map_err(anyerr)?;
        wire_channel(&dc, in_tx, ready_tx);

        let offer = pc.create_offer(None).await.map_err(anyerr)?;
        pc.set_local_description(offer).await.map_err(anyerr)?;
        wait_for_ice(&pc).await;
        let local = pc
            .local_description()
            .await
            .ok_or_else(|| anyerr("webrtc: no local description after gathering"))?;
        send_signal(link, &Signal::Offer(local.sdp)).await?;

        match recv_signal(link).await? {
            Signal::Answer(sdp) => {
                let answer = RTCSessionDescription::answer(sdp).map_err(anyerr)?;
                pc.set_remote_description(answer).await.map_err(anyerr)?;
            }
            _ => return Err(anyerr("webrtc: expected answer, got offer")),
        }
    } else {
        // The channel arrives via `on_data_channel`; wire it as it appears.
        let in_tx2 = in_tx.clone();
        let ready_tx2 = ready_tx.clone();
        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let inbound = in_tx2.clone();
            let ready = ready_tx2.clone();
            Box::pin(async move {
                wire_channel(&dc, inbound, ready);
            })
        }));

        match recv_signal(link).await? {
            Signal::Offer(sdp) => {
                let offer = RTCSessionDescription::offer(sdp).map_err(anyerr)?;
                pc.set_remote_description(offer).await.map_err(anyerr)?;
            }
            _ => return Err(anyerr("webrtc: expected offer, got answer")),
        }
        let answer = pc.create_answer(None).await.map_err(anyerr)?;
        pc.set_local_description(answer).await.map_err(anyerr)?;
        wait_for_ice(&pc).await;
        let local = pc
            .local_description()
            .await
            .ok_or_else(|| anyerr("webrtc: no local description after gathering"))?;
        send_signal(link, &Signal::Answer(local.sdp)).await?;
    }

    let dc = ready_rx
        .recv()
        .await
        .ok_or_else(|| anyerr("webrtc: data channel never opened"))?;
    Ok(WebrtcLink {
        _pc: pc,
        dc,
        inbound: in_rx,
    })
}

/// Block until ICE gathering completes (non-trickle), so `local_description`
/// carries every candidate.
async fn wait_for_ice(pc: &RTCPeerConnection) {
    let mut done = pc.gathering_complete_promise().await;
    let _ = done.recv().await;
}

async fn send_signal(link: &mut dyn Link, s: &Signal) -> Result<()> {
    link.send(postcard::to_allocvec(s).map_err(anyerr)?).await
}

async fn recv_signal(link: &mut dyn Link) -> Result<Signal> {
    let bytes = link
        .recv()
        .await?
        .ok_or_else(|| anyerr("webrtc: signaling link closed"))?;
    postcard::from_bytes(&bytes).map_err(anyerr)
}

// ---------------------------------------------------------------------------
// Browser↔native bridge: connect over the SAME WebSocket signaling server the
// browser uses (rooms by connection id), speaking the *browser's JSON protocol*
// (`{"type":"role"|"sdp",...}`) — so a native webrtc-rs peer and a browser
// web-sys peer establish WebRTC with each other. (`docs/planned/transport-negotiation.md`)
// ---------------------------------------------------------------------------

fn sdp_json(sdp: &str) -> String {
    serde_json::json!({ "type": "sdp", "sdp": sdp }).to_string()
}

fn json_field(text: &str, expect_type: &str, field: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    if v.get("type")?.as_str()? != expect_type {
        return None;
    }
    v.get(field)?.as_str().map(str::to_string)
}

/// Connect to the peer sharing `room` via the WebSocket signaling server at
/// `ws_url`, establishing a WebRTC `Link` (the native end of the browser↔native
/// bridge). The server assigns the offerer role and relays the offer/answer.
pub async fn connect_via_signaling(ws_url: &str, room: &str) -> Result<WebrtcLink> {
    // A `/` before the query is required — `ws://host:port?room=…` has an empty
    // path and yields an invalid HTTP request line (tungstenite won't normalize it).
    let url = format!("{}/?room={room}", ws_url.trim_end_matches('/'));
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.map_err(anyerr)?;
    let (mut write, mut read) = ws.split();

    // Wait for the server's role assignment.
    let we_offer = loop {
        match read.next().await {
            Some(Ok(WsMessage::Text(t))) => {
                if let Some(role) = json_field(&t, "role", "role") {
                    break role == "offerer";
                }
                if t.contains("room full") || t.contains("capacity") {
                    return Err(anyerr("signaling: room unavailable"));
                }
            }
            Some(Ok(_)) => continue,
            _ => return Err(anyerr("signaling: closed before role")),
        }
    };

    let pc = new_peer_connection().await?;
    let (in_tx, in_rx) = mpsc::channel::<Vec<u8>>(256);
    let (ready_tx, mut ready_rx) = mpsc::channel::<Arc<RTCDataChannel>>(1);

    if we_offer {
        let dc = pc.create_data_channel(CHANNEL_LABEL, None).await.map_err(anyerr)?;
        wire_channel(&dc, in_tx, ready_tx);
        let offer = pc.create_offer(None).await.map_err(anyerr)?;
        pc.set_local_description(offer).await.map_err(anyerr)?;
        wait_for_ice(&pc).await;
        let local = pc.local_description().await.ok_or_else(|| anyerr("no local sdp"))?;
        write.send(WsMessage::text(sdp_json(&local.sdp))).await.map_err(anyerr)?;
        let answer = recv_sdp_ws(&mut read).await?;
        pc.set_remote_description(RTCSessionDescription::answer(answer).map_err(anyerr)?)
            .await
            .map_err(anyerr)?;
    } else {
        let in_tx2 = in_tx.clone();
        let ready_tx2 = ready_tx.clone();
        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let inbound = in_tx2.clone();
            let ready = ready_tx2.clone();
            Box::pin(async move {
                wire_channel(&dc, inbound, ready);
            })
        }));
        let offer = recv_sdp_ws(&mut read).await?;
        pc.set_remote_description(RTCSessionDescription::offer(offer).map_err(anyerr)?)
            .await
            .map_err(anyerr)?;
        let answer = pc.create_answer(None).await.map_err(anyerr)?;
        pc.set_local_description(answer).await.map_err(anyerr)?;
        wait_for_ice(&pc).await;
        let local = pc.local_description().await.ok_or_else(|| anyerr("no local sdp"))?;
        write.send(WsMessage::text(sdp_json(&local.sdp))).await.map_err(anyerr)?;
    }

    let dc = ready_rx.recv().await.ok_or_else(|| anyerr("webrtc: data channel never opened"))?;
    Ok(WebrtcLink { _pc: pc, dc, inbound: in_rx })
}

async fn recv_sdp_ws<S>(read: &mut S) -> Result<String>
where
    S: StreamExt<Item = std::result::Result<WsMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match read.next().await {
            Some(Ok(WsMessage::Text(t))) => {
                if let Some(sdp) = json_field(&t, "sdp", "sdp") {
                    return Ok(sdp);
                }
                if t.contains("peer-left") {
                    return Err(anyerr("signaling: peer left"));
                }
            }
            Some(Ok(_)) => continue,
            _ => return Err(anyerr("signaling: closed during sdp exchange")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;

    /// Two `webrtc-rs` peers establish a real data channel over a mock signaling
    /// pair (standing in for the iroh link) and exchange a message — exercises the
    /// full offer/answer + the `WebrtcLink` over loopback host candidates.
    #[tokio::test]
    async fn establishes_and_carries_a_message() {
        let (mut sa, mut sb) = mock_pair();
        let (ra, rb) = tokio::join!(
            upgrade_to_webrtc(&mut sa, true),
            upgrade_to_webrtc(&mut sb, false),
        );
        let mut la = ra.expect("offerer established");
        let mut lb = rb.expect("answerer established");

        la.send(b"hello over webrtc".to_vec()).await.unwrap();
        let got = lb.recv().await.unwrap().expect("message arrives");
        assert_eq!(got, b"hello over webrtc");

        // And the other direction.
        lb.send(b"reply".to_vec()).await.unwrap();
        assert_eq!(la.recv().await.unwrap().unwrap(), b"reply");
    }
}
