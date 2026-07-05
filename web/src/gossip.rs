//! Gossip-mesh transport for the browser: a shared file tree is an **iroh-gossip topic**, and
//! peers broadcast `SyncMsg`s epidemically — no fixed hub, the swarm self-organizes
//! and survives any peer leaving. The N-peer-safe merge in `riftpipe_core::sync`
//! handles convergence; this module is just the transport + membership.
//!
//! Topology: the topic is the *host* EndpointId (32 bytes), so everyone opening the
//! same share link lands on the same topic. A joiner seeds the host's address (from
//! the ticket) into a memory lookup and bootstraps off it; after that, gossip
//! membership spreads and the host is no longer special.

use futures_util::StreamExt;
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_gossip::proto::TopicId;

/// The topic for a tree hosted by `host` — deterministic, so all peers agree.
pub fn topic_of(host: EndpointId) -> TopicId {
    TopicId::from_bytes(*host.as_bytes())
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
}

impl Mesh {
    /// Join the mesh for `host`'s tree. `bootstrap` is the host's address (from the
    /// ticket) for a joiner, or `None` for the host itself (it starts the swarm).
    pub async fn join(
        sk: SecretKey,
        host: EndpointId,
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
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();
        let topic = gossip
            .subscribe(topic_of(host), boot)
            .await
            .map_err(|e| e.to_string())?;
        let (sender, receiver) = topic.split();
        Ok(Mesh {
            endpoint,
            _router: router,
            _gossip: gossip,
            sender,
            receiver,
            my_id,
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

    /// Split into a broadcast sender, an event receiver, and a keep-alive handle
    /// (the endpoint/router/gossip must outlive both).
    fn into_parts(self) -> (GossipSender, GossipReceiver, MeshKeepAlive) {
        (
            self.sender,
            self.receiver,
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
    neighbors: std::rc::Rc<std::cell::RefCell<std::collections::BTreeSet<[u8; 32]>>>,
    routing: std::rc::Rc<std::cell::RefCell<std::collections::BTreeMap<[u8; 32], Vec<[u8; 32]>>>>,
    _keep: MeshKeepAlive,
}

impl GossipTreeSync {
    pub fn new(mesh: Mesh, on_merged: std::rc::Rc<dyn Fn(String, Vec<u8>)>) -> GossipTreeSync {
        use std::cell::RefCell;
        use std::rc::Rc;
        let my_id = *mesh.my_id().as_bytes();
        let (sender, mut receiver, keep) = mesh.into_parts();
        let agent = format!("g{:08x}", (js_sys::Math::random() * 4_294_967_296.0) as u32);
        let syncer = Rc::new(RefCell::new(riftpipe_core::sync::Syncer::new(agent)));
        let neighbors = Rc::new(RefCell::new(std::collections::BTreeSet::new()));
        let routing = Rc::new(RefCell::new(std::collections::BTreeMap::new()));

        let (sy, nb, rt, snd) = (syncer.clone(), neighbors.clone(), routing.clone(), sender.clone());
        wasm_bindgen_futures::spawn_local(async move {
            let broadcast = |gm: &MeshMsg| {
                postcard::to_allocvec(gm).ok().map(|b| b.into())
            };
            while let Some(Ok(ev)) = receiver.next().await {
                match ev {
                    Event::Received(m) => {
                        let Ok(gm) = postcard::from_bytes::<MeshMsg>(&m.content) else { continue };
                        match gm {
                            MeshMsg::Sync(msg) => {
                                let (persist, replies) = sy.borrow_mut().apply(msg);
                                if let Some((path, bytes)) = persist {
                                    on_merged(path, bytes);
                                }
                                for r in replies {
                                    if let Some(b) = broadcast(&MeshMsg::Sync(r)) {
                                        let _ = snd.broadcast(b).await;
                                    }
                                }
                            }
                            MeshMsg::Presence { id, neighbors } => {
                                rt.borrow_mut().insert(id, neighbors);
                            }
                        }
                    }
                    Event::NeighborUp(id) => {
                        nb.borrow_mut().insert(*id.as_bytes());
                        // Catch the new neighbor up with our whole tree.
                        let full = sy.borrow().full_state();
                        for msg in full {
                            if let Some(b) = broadcast(&MeshMsg::Sync(msg)) {
                                let _ = snd.broadcast(b).await;
                            }
                        }
                        let list: Vec<[u8; 32]> = nb.borrow().iter().copied().collect();
                        rt.borrow_mut().insert(my_id, list.clone());
                        if let Some(b) = broadcast(&MeshMsg::Presence { id: my_id, neighbors: list }) {
                            let _ = snd.broadcast(b).await;
                        }
                    }
                    Event::NeighborDown(id) => {
                        nb.borrow_mut().remove(id.as_bytes());
                        let list: Vec<[u8; 32]> = nb.borrow().iter().copied().collect();
                        rt.borrow_mut().insert(my_id, list.clone());
                        if let Some(b) = broadcast(&MeshMsg::Presence { id: my_id, neighbors: list }) {
                            let _ = snd.broadcast(b).await;
                        }
                    }
                    Event::Lagged => {}
                }
            }
        });

        GossipTreeSync { syncer, sender, neighbors, routing, _keep: keep }
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
        if let Ok(bytes) = postcard::to_allocvec(&gm) {
            let sender = self.sender.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = sender.broadcast(bytes.into()).await;
            });
        }
    }

    /// Direct neighbors in the swarm (this peer's connected peers), as hex ids.
    pub fn peers(&self) -> Vec<String> {
        self.neighbors.borrow().iter().map(hex32).collect()
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
        let host_sk = SecretKey::generate();
        let host_id = host_sk.public();
        let mut host = Mesh::join(host_sk, host_id, None).await.expect("host join");
        for _ in 0..200 {
            if !host.addr().addrs.is_empty() {
                break;
            }
            sleep_ms(50).await;
        }

        let join_sk = SecretKey::generate();
        let mut joiner = Mesh::join(join_sk, host_id, Some(host.addr()))
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

        let host_sk = SecretKey::generate();
        let host_id = host_sk.public();
        let host_mesh = Mesh::join(host_sk, host_id, None).await.expect("host");
        for _ in 0..200 {
            if !host_mesh.addr().addrs.is_empty() {
                break;
            }
            sleep_ms(50).await;
        }
        let host_addr = host_mesh.addr();
        let host_bs = GossipTreeSync::new(host_mesh, Rc::new(|_, _| {}));

        let join_sk = SecretKey::generate();
        let joiner_mesh = Mesh::join(join_sk, host_id, Some(host_addr)).await.expect("joiner");
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
