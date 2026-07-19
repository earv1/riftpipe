//! Gossip-mesh transport for the browser: a shared file tree is an **iroh-gossip topic**, and
//! peers broadcast `SyncMsg`s epidemically — no fixed hub, the swarm self-organizes
//! and survives any peer leaving. The N-peer-safe merge in `riftpipe_core::sync`
//! handles convergence; this module is just the transport + membership.
//!
//! Topology: the topic is `H(board secret)` (32 bytes), so everyone opening the
//! same share link lands on the same topic — and a stranger who only knows the
//! host's public key cannot derive it (see `core::crypto` / `SECURITY.md`). A
//! joiner seeds the host's address (from the ticket) into a memory lookup and
//! bootstraps off it; after that, gossip membership spreads and the host is no
//! longer special. Every frame is AEAD-sealed with a key derived from the same
//! secret, so the relay and non-holders see only ciphertext.

use futures_util::StreamExt;
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_gossip::proto::TopicId;

/// The gossip topic for a board secret — `H(secret)`, underivable without the
/// share link. Identical to the native `sync::mesh::topic_of`.
pub fn topic_of(secret: &[u8; 32]) -> TopicId {
    TopicId::from_bytes(riftpipe_core::crypto::topic_from_secret(secret))
}

/// A joined gossip mesh for one shared tree: broadcast bytes to everyone, receive a
/// stream of events (messages + membership changes).
pub struct Mesh {
    endpoint: Endpoint,
    _router: Router,
    _gossip: Gossip,
    sender: GossipSender,
    receiver: GossipReceiver,
    my_id: EndpointId,
    /// The peer we bootstrapped off (`None` when we host) — kept so a TOTAL
    /// neighbor loss can re-join it: hyparview heals only from its passive
    /// view and never re-dials the original bootstrap on its own.
    bootstrap: Option<EndpointId>,
    /// Seals/opens every gossip frame with the key derived from the board
    /// secret — the relay and non-holders see only ciphertext.
    cipher: riftpipe_core::crypto::MeshCipher,
}

impl Mesh {
    /// Join the board identified by `secret`. `bootstrap` is the host's address
    /// (from the ticket) for a joiner, or `None` for the host itself (it starts
    /// the swarm). The `secret` both names the topic (`H(secret)`) and keys the
    /// frame cipher, so only link-holders find and can read the board.
    pub async fn join(
        sk: SecretKey,
        secret: [u8; 32],
        bootstrap: Option<EndpointAddr>,
    ) -> Result<Mesh, String> {
        let my_id = sk.public();
        // Seed the bootstrap address so we can dial it without waiting on discovery.
        let lookup = MemoryLookup::new();
        let mut boot = Vec::new();
        if let Some(addr) = bootstrap {
            boot.push(addr.id);
            lookup.add_endpoint_info(addr);
        }
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(sk)
            .address_lookup(lookup)
            .bind()
            .await
            .map_err(|e| e.to_string())?;
        // The library's 4 KiB frame limit stays at its default: big messages are
        // chunked (`core::sync::encode_mesh_frames`) so no peer — however old —
        // ever receives an over-limit frame.
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();
        let bootstrap = boot.first().copied();
        let topic = gossip
            .subscribe(topic_of(&secret), boot)
            .await
            .map_err(|e| e.to_string())?;
        let (sender, receiver) = topic.split();
        let cipher = riftpipe_core::crypto::MeshCipher::from_secret(&secret);
        Ok(Mesh {
            endpoint,
            _router: router,
            _gossip: gossip,
            sender,
            receiver,
            my_id,
            bootstrap,
            cipher,
        })
    }

    pub fn my_id(&self) -> EndpointId {
        self.my_id
    }

    /// This peer's dialable address (share it as the bootstrap ticket).
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Wait until this peer has a dialable (relay) address to share.
    pub async fn wait_for_addr(&self) -> EndpointAddr {
        for _ in 0..400 {
            let a = self.endpoint.addr();
            if !a.addrs.is_empty() {
                return a;
            }
            gloo_timers::future::TimeoutFuture::new(50).await;
        }
        self.endpoint.addr()
    }

    /// Broadcast `bytes` to every peer in the mesh.
    pub async fn broadcast(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.sender
            .broadcast(bytes.into())
            .await
            .map_err(|e| e.to_string())
    }

    /// The next mesh event (a received message, or a neighbor up/down).
    pub async fn next(&mut self) -> Option<Event> {
        match self.receiver.next().await {
            Some(Ok(ev)) => Some(ev),
            _ => None,
        }
    }

    /// Split into a broadcast sender, an event receiver, the bootstrap peer
    /// (for a re-join after total neighbor loss), the frame cipher (shared —
    /// the recv task and the push path both seal/open with it), and a keep-alive
    /// handle (the endpoint/router/gossip must outlive both).
    fn into_parts(
        self,
    ) -> (
        GossipSender,
        GossipReceiver,
        Option<EndpointId>,
        std::rc::Rc<riftpipe_core::crypto::MeshCipher>,
        MeshKeepAlive,
    ) {
        (
            self.sender,
            self.receiver,
            self.bootstrap,
            std::rc::Rc::new(self.cipher),
            MeshKeepAlive { _endpoint: self.endpoint, _router: self._router, _gossip: self._gossip },
        )
    }
}

/// Keeps the mesh's endpoint/router/gossip alive for the sync's lifetime.
pub struct MeshKeepAlive {
    _endpoint: Endpoint,
    _router: Router,
    _gossip: Gossip,
}

impl MeshKeepAlive {
    /// Close the endpoint (and await it) so a reconnect can rebind the same key
    /// cleanly — a plain drop is async and would race the relay re-registration.
    async fn close(self) {
        self._endpoint.close().await;
    }
}

/// What travels over the gossip topic — the shared wire type, single-sourced in
/// core so native and browser peers speak the same layout.
use riftpipe_core::sync::MeshMsg;

/// File-tree sync over the gossip mesh. Local pushes broadcast to everyone; received
/// messages merge via the N-peer-safe `Syncer`. A new neighbor is caught up by
/// re-broadcasting our full tree. Tracks direct neighbors + a gossiped routing map.
pub struct GossipTreeSync {
    syncer: std::rc::Rc<std::cell::RefCell<riftpipe_core::sync::Syncer>>,
    sender: GossipSender,
    cipher: std::rc::Rc<riftpipe_core::crypto::MeshCipher>,
    neighbors: std::rc::Rc<std::cell::RefCell<std::collections::BTreeSet<[u8; 32]>>>,
    routing: std::rc::Rc<std::cell::RefCell<std::collections::BTreeMap<[u8; 32], Vec<[u8; 32]>>>>,
    versions: std::rc::Rc<std::cell::RefCell<riftpipe_core::sync::VersionMonitor>>,
    _keep: MeshKeepAlive,
}

/// Surface a flagged peer: console.warn + a `riftpipe:protocol-mismatch`
/// CustomEvent on `window` (detail: `{peer, theirs, ours}` JSON string), so
/// ANY app on the library can show it — nothing app-specific here.
fn report_mismatch(p: &riftpipe_core::sync::PeerVersion) {
    let ours = riftpipe_core::sync::PROTOCOL_VERSION;
    let peer = hex32(&p.id);
    web_sys::console::warn_1(
        &format!(
            "[mesh] protocol-mismatch peer={peer} theirs={} ours={ours} — \
             sync with that peer may be incomplete",
            p.proto,
        )
        .into(),
    );
    if let Some(w) = web_sys::window() {
        let detail = format!(r#"{{"peer":"{peer}","theirs":{},"ours":{ours}}}"#, p.proto);
        let init = web_sys::CustomEventInit::new();
        init.set_detail(&wasm_bindgen::JsValue::from_str(&detail));
        if let Ok(ev) =
            web_sys::CustomEvent::new_with_event_init_dict("riftpipe:protocol-mismatch", &init)
        {
            let _ = w.dispatch_event(&ev);
        }
    }
}

impl GossipTreeSync {
    pub fn new(mesh: Mesh, on_merged: std::rc::Rc<dyn Fn(String, Vec<u8>)>) -> GossipTreeSync {
        use std::cell::RefCell;
        use std::rc::Rc;
        let my_id = *mesh.my_id().as_bytes();
        let (sender, mut receiver, bootstrap, cipher, keep) = mesh.into_parts();
        let agent = format!("g{:08x}", (js_sys::Math::random() * 4_294_967_296.0) as u32);
        let syncer = Rc::new(RefCell::new(riftpipe_core::sync::Syncer::new(agent)));
        let neighbors = Rc::new(RefCell::new(std::collections::BTreeSet::new()));
        let routing = Rc::new(RefCell::new(std::collections::BTreeMap::new()));
        let versions = Rc::new(RefCell::new(riftpipe_core::sync::VersionMonitor::new()));

        let (sy, nb, rt, vs, snd, cph) = (
            syncer.clone(),
            neighbors.clone(),
            routing.clone(),
            versions.clone(),
            sender.clone(),
            cipher.clone(),
        );
        wasm_bindgen_futures::spawn_local(async move {
            // A failed send must not kill the session, but it must be VISIBLE —
            // a silently dropped broadcast reads as "synced". Big messages go
            // out as chunk frames (each under the 4 KiB limit), and every frame
            // is AEAD-sealed so only link-holders can read/inject.
            async fn send(
                snd: &GossipSender,
                cph: &riftpipe_core::crypto::MeshCipher,
                gm: &MeshMsg,
                neighbors_only: bool,
            ) {
                for b in riftpipe_core::sync::encode_mesh_frames(gm) {
                    let sealed = cph.seal(&b);
                    let n = sealed.len();
                    let r = if neighbors_only {
                        snd.broadcast_neighbors(sealed.into()).await
                    } else {
                        snd.broadcast(sealed.into()).await
                    };
                    if let Err(e) = r {
                        web_sys::console::warn_1(
                            &format!("[mesh] broadcast of {n} bytes failed: {e}").into(),
                        );
                    }
                }
            }
            // Mirror the library's authoritative neighbor set into the shared
            // cell (for the `connectedPeers()` debug accessor + Presence).
            let snapshot = |receiver: &GossipReceiver| -> Vec<[u8; 32]> {
                receiver.neighbors().map(|n| *n.as_bytes()).collect()
            };
            // Reassembles incoming chunk frames into whole messages.
            let mut assembler = riftpipe_core::sync::MeshAssembler::new();
            while let Some(Ok(ev)) = receiver.next().await {
                match ev {
                    Event::Received(m) => {
                        // Open the sealed frame (an outsider's or a corrupt frame
                        // won't decrypt — drop it). Then plain frames pass through
                        // and chunk frames buffer until whole.
                        let Some(plain) = cph.open(&m.content) else { continue };
                        let Some(gm) = assembler.feed(&plain) else { continue };
                        match gm {
                            MeshMsg::Sync(msg) => {
                                let (persist, replies) = sy.borrow_mut().apply(msg);
                                if let Some((path, bytes)) = persist {
                                    on_merged(path, bytes);
                                }
                                for r in replies {
                                    send(&snd, &cph, &MeshMsg::Sync(r), false).await;
                                }
                            }
                            MeshMsg::Presence { id, neighbors } => {
                                if id != my_id {
                                    let now = js_sys::Date::now() as u64;
                                    if let Some(p) = vs.borrow_mut().on_presence(id, now) {
                                        report_mismatch(&p);
                                    }
                                }
                                rt.borrow_mut().insert(id, neighbors);
                            }
                            MeshMsg::Version { id, proto } => {
                                if id != my_id {
                                    if let Some(p) = vs.borrow_mut().on_version(id, proto) {
                                        report_mismatch(&p);
                                    }
                                }
                            }
                            // feed() never yields a Chunk (it refuses nesting).
                            MeshMsg::Chunk { .. } => {}
                        }
                    }
                    Event::NeighborUp(_) => {
                        // Announce our protocol version BEFORE the Presence —
                        // that ordering is what lets peers deduce "Presence
                        // but no Version = pre-versioning peer".
                        let ours = riftpipe_core::sync::PROTOCOL_VERSION;
                        send(&snd, &cph, &MeshMsg::Version { id: my_id, proto: ours }, false).await;
                        // Catch the new neighbor up with our whole tree —
                        // direct neighbors only; the swarm doesn't need it.
                        let full = sy.borrow().full_state();
                        for msg in full {
                            send(&snd, &cph, &MeshMsg::Sync(msg), true).await;
                        }
                        let list = snapshot(&receiver);
                        *nb.borrow_mut() = list.iter().copied().collect();
                        rt.borrow_mut().insert(my_id, list.clone());
                        send(&snd, &cph, &MeshMsg::Presence { id: my_id, neighbors: list }, false).await;
                    }
                    Event::NeighborDown(_) => {
                        let ours = riftpipe_core::sync::PROTOCOL_VERSION;
                        send(&snd, &cph, &MeshMsg::Version { id: my_id, proto: ours }, false).await;
                        let list = snapshot(&receiver);
                        *nb.borrow_mut() = list.iter().copied().collect();
                        // Total neighbor loss: hyparview refills only from its
                        // passive view — it never re-dials the original
                        // bootstrap. We still hold its address (MemoryLookup),
                        // so ask the swarm to re-join it.
                        if list.is_empty() {
                            if let Some(b) = bootstrap {
                                if let Err(e) = snd.join_peers(vec![b]).await {
                                    web_sys::console::warn_1(
                                        &format!("[mesh] re-join failed: {e}").into(),
                                    );
                                }
                            }
                        }
                        rt.borrow_mut().insert(my_id, list.clone());
                        send(&snd, &cph, &MeshMsg::Presence { id: my_id, neighbors: list }, false).await;
                    }
                    Event::Lagged => {}
                }
            }
        });

        // Grace expiry for announcement-less (pre-versioning) peers: a quiet
        // mesh may only ever see ONE presence from an old peer. The loop holds
        // a Weak so it dies with the sync (no timer leak past teardown).
        let weak_vs = Rc::downgrade(&versions);
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                gloo_timers::future::TimeoutFuture::new(5_000).await;
                let Some(vs) = weak_vs.upgrade() else { break };
                for p in vs.borrow_mut().tick(js_sys::Date::now() as u64) {
                    report_mismatch(&p);
                }
            }
        });

        GossipTreeSync { syncer, sender, cipher, neighbors, routing, versions, _keep: keep }
    }

    pub fn push_text(&self, path: &str, content: &str) {
        if let Some(msg) = self.syncer.borrow_mut().local_text(path, content) {
            self.broadcast(MeshMsg::Sync(msg));
        }
    }

    pub fn push_lww(&self, path: &str, bytes: &[u8]) {
        let now = js_sys::Date::now() as u64;
        let msg = self.syncer.borrow_mut().local_lww(path, bytes.to_vec(), now);
        self.broadcast(MeshMsg::Sync(msg));
    }

    fn broadcast(&self, gm: MeshMsg) {
        let frames = riftpipe_core::sync::encode_mesh_frames(&gm);
        let sender = self.sender.clone();
        let cipher = self.cipher.clone();
        wasm_bindgen_futures::spawn_local(async move {
            for b in frames {
                let sealed = cipher.seal(&b);
                let n = sealed.len();
                if let Err(e) = sender.broadcast(sealed.into()).await {
                    web_sys::console::warn_1(
                        &format!("[mesh] broadcast of {n} bytes failed: {e}").into(),
                    );
                }
            }
        });
    }

    /// Direct neighbors in the swarm (this peer's connected peers), as hex ids.
    pub fn peers(&self) -> Vec<String> {
        self.neighbors.borrow().iter().map(hex32).collect()
    }

    /// Protocol-version status as JSON: our version, every peer's known (or
    /// deduced-v0) version, and the mismatched subset.
    pub fn protocol_status(&self) -> String {
        let vs = self.versions.borrow();
        let peers: std::collections::BTreeMap<String, u32> =
            vs.peers().into_iter().map(|p| (hex32(&p.id), p.proto)).collect();
        let mismatched: Vec<String> = vs.mismatched().iter().map(|p| hex32(&p.id)).collect();
        serde_json::json!({
            "ours": riftpipe_core::sync::PROTOCOL_VERSION,
            "peers": peers,
            "mismatched": mismatched,
        })
        .to_string()
    }

    /// The gossiped routing map: `id -> [neighbor ids]` across the whole mesh.
    pub fn routing_map(&self) -> std::collections::BTreeMap<String, Vec<String>> {
        self.routing
            .borrow()
            .iter()
            .map(|(k, v)| (hex32(k), v.iter().map(hex32).collect()))
            .collect()
    }

    /// Leave the mesh, closing the endpoint (awaited) for a clean rebind.
    async fn close(self) {
        self._keep.close().await;
    }
}

fn hex32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

thread_local! {
    /// The active gossip tree sync, if the app is on the mesh transport.
    static GOSSIP: std::cell::RefCell<Option<GossipTreeSync>> = const { std::cell::RefCell::new(None) };
}

/// Install the active gossip sync (replacing any prior one).
pub(crate) fn set_active(bs: GossipTreeSync) {
    GOSSIP.with(|c| *c.borrow_mut() = Some(bs));
}

/// Tear down any active gossip sync, closing the endpoint (awaited) so a reconnect
/// under the same persisted identity rebinds cleanly.
pub(crate) async fn clear_active() {
    if let Some(bs) = GOSSIP.with(|c| c.borrow_mut().take()) {
        bs.close().await;
    }
}

/// Route a text push to the mesh if active; `true` if it was handled.
pub(crate) fn try_push_text(path: &str, content: &str) -> bool {
    GOSSIP.with(|c| match c.borrow().as_ref() {
        Some(bs) => {
            bs.push_text(path, content);
            true
        }
        None => false,
    })
}

/// Route a structural push to the mesh if active; `true` if it was handled.
pub(crate) fn try_push_lww(path: &str, bytes: &[u8]) -> bool {
    GOSSIP.with(|c| match c.borrow().as_ref() {
        Some(bs) => {
            bs.push_lww(path, bytes);
            true
        }
        None => false,
    })
}

/// Debug: this peer's direct neighbors in the mesh (hex ids), as a JSON array.
#[wasm_bindgen::prelude::wasm_bindgen(js_name = connectedPeers)]
pub fn connected_peers() -> String {
    GOSSIP.with(|c| {
        let peers = c.borrow().as_ref().map(|bs| bs.peers()).unwrap_or_default();
        serde_json::to_string(&peers).unwrap_or_else(|_| "[]".into())
    })
}

/// Protocol-version status for the active mesh, as JSON:
/// `{"ours":N,"peers":{"<hexid>":M,...},"mismatched":["<hexid>",...]}`.
/// (Apps can also just listen for the `riftpipe:protocol-mismatch`
/// CustomEvent on `window` instead of polling this.)
#[wasm_bindgen::prelude::wasm_bindgen(js_name = protocolStatus)]
pub fn protocol_status() -> String {
    GOSSIP.with(|c| {
        c.borrow()
            .as_ref()
            .map(|bs| bs.protocol_status())
            .unwrap_or_else(|| {
                format!(
                    r#"{{"ours":{},"peers":{{}},"mismatched":[]}}"#,
                    riftpipe_core::sync::PROTOCOL_VERSION,
                )
            })
    })
}

/// Debug: the gossiped routing map `id -> [neighbors]` across the mesh, as JSON.
#[wasm_bindgen::prelude::wasm_bindgen(js_name = routingMap)]
pub fn routing_map() -> String {
    GOSSIP.with(|c| {
        let map = c.borrow().as_ref().map(|bs| bs.routing_map()).unwrap_or_default();
        serde_json::to_string(&map).unwrap_or_else(|_| "{}".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    async fn sleep_ms(ms: u32) {
        gloo_timers::future::TimeoutFuture::new(ms).await;
    }

    /// Two browser peers join the same gossip topic (one bootstraps off the other)
    /// and a broadcast reaches the other — proves the mesh transport works in-browser
    /// over n0's relay. Needs network.
    #[wasm_bindgen_test]
    async fn gossip_roundtrip_two_peers() {
        // A shared board secret (would ride the share link) derives the topic +
        // frame cipher for both peers.
        let secret = riftpipe_core::crypto::generate_secret();
        let host_sk = SecretKey::generate();
        let mut host = Mesh::join(host_sk, secret, None).await.expect("host join");
        for _ in 0..200 {
            if !host.addr().addrs.is_empty() {
                break;
            }
            sleep_ms(50).await;
        }

        let join_sk = SecretKey::generate();
        let mut joiner = Mesh::join(join_sk, secret, Some(host.addr()))
            .await
            .expect("joiner join");

        // Joiner broadcasts once it sees a neighbor; host must receive it.
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(ev) = joiner.next().await {
                if let Event::NeighborUp(_) = ev {
                    let _ = joiner.broadcast(b"ping-over-gossip".to_vec()).await;
                }
            }
        });

        let mut got = None;
        for _ in 0..400 {
            if let Some(Event::Received(m)) = host.next().await {
                got = Some(m.content.to_vec());
                break;
            }
        }
        assert_eq!(
            got.as_deref(),
            Some(&b"ping-over-gossip"[..]),
            "host received the gossip broadcast",
        );
    }

    /// A file pushed on one peer syncs to another over the gossip mesh — proves
    /// tree sync (not just raw bytes) works over gossip.
    #[wasm_bindgen_test]
    async fn tree_syncs_over_gossip_mesh() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let secret = riftpipe_core::crypto::generate_secret();
        let host_sk = SecretKey::generate();
        let host_mesh = Mesh::join(host_sk, secret, None).await.expect("host");
        for _ in 0..200 {
            if !host_mesh.addr().addrs.is_empty() {
                break;
            }
            sleep_ms(50).await;
        }
        let host_addr = host_mesh.addr();
        let host_bs = GossipTreeSync::new(host_mesh, Rc::new(|_, _| {}));

        let join_sk = SecretKey::generate();
        let joiner_mesh = Mesh::join(join_sk, secret, Some(host_addr)).await.expect("joiner");
        let got: Rc<RefCell<Vec<(String, Vec<u8>)>>> = Rc::new(RefCell::new(Vec::new()));
        let g = got.clone();
        let _joiner_bs =
            GossipTreeSync::new(joiner_mesh, Rc::new(move |p, b| g.borrow_mut().push((p, b))));

        // Let the swarm form, then the host pushes a file.
        sleep_ms(2500).await;
        host_bs.push_text("notes/x/doc.md", "# gossip doc\n");

        for _ in 0..300 {
            if got.borrow().iter().any(|(p, _)| p == "notes/x/doc.md") {
                break;
            }
            sleep_ms(50).await;
        }
        assert!(
            got.borrow()
                .iter()
                .any(|(p, b)| p == "notes/x/doc.md" && b == b"# gossip doc\n"),
            "joiner received the file over the gossip mesh: {:?}",
            got.borrow(),
        );
    }
}
