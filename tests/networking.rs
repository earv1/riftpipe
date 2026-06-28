//! Integration test for the connect/negotiate/transport stack over a **real**
//! (loopback) iroh connection — the path `docs/planned/transport-negotiation.md`
//! describes, end to end:
//!
//!   real iroh link  →  BLAKE3 auth  →  CAPS negotiation  →  WebRTC upgrade
//!                    →  data over the negotiated transport
//!
//! Exercises `net::{transport, secure, negotiate, webrtc}` and the shared
//! `sync::pipe::negotiate_session_halves` glue together, rather than any single
//! layer in isolation (those have unit tests).
//!
//! Structure notes:
//! - **Multi-thread runtime** — iroh's magicsock/relay actors need real worker
//!   threads (production runs under multi-thread `#[tokio::main]`).
//! - **Each peer runs its full accept/connect→auth sequence concurrently**, never
//!   barrier-joining "both connected" *before* auth: a server's `accept_bi`
//!   resolves only once the client writes its first bytes (in auth), so a barrier
//!   there deadlocks. This mirrors the real two-process flow.
//! - **`online()` + a hard timeout** — endpoints acquire a relay home (so the
//!   address is complete) and any connectivity/ICE stall fails loudly. The test
//!   therefore needs network + a reachable relay, like the manual loopback demo.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use riftpipe::net::negotiate::{exchange_caps, Caps, Transport};
use riftpipe::net::secure::authenticate;
use riftpipe::net::transport::{accept_link, bind_accept, bind_connect, connect_link, local_addr};
use riftpipe::net::Counters;
use riftpipe::sync::pipe::negotiate_session_halves;

const SECRET: [u8; 32] = [7u8; 32];
const BUDGET: Duration = Duration::from_secs(60);

/// Capability negotiation runs over the real iroh link and both ends agree: two
/// native peers pick WebRTC, with exactly one designated as the offerer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capability_negotiation_over_real_iroh() {
    tokio::time::timeout(BUDGET, async {
        let server = bind_accept().await.expect("bind accept");
        let client = bind_connect().await.expect("bind connect");
        let _ = tokio::time::timeout(Duration::from_secs(10), server.online()).await;
        let _ = tokio::time::timeout(Duration::from_secs(10), client.online()).await;
        let addr = local_addr(&server).await;

        // Each side: connect/accept → auth → caps, as one concurrent sequence.
        let server_side = async {
            let mut la = accept_link(&server).await.expect("accept link");
            authenticate(&mut la, &SECRET).await.expect("server auth");
            let caps = Caps::native();
            exchange_caps(&mut la, &caps).await.expect("server caps")
        };
        let client_side = async {
            let mut lb = connect_link(&client, addr).await.expect("connect link");
            authenticate(&mut lb, &SECRET).await.expect("client auth");
            let caps = Caps::native();
            exchange_caps(&mut lb, &caps).await.expect("client caps")
        };
        let (oa, ob) = tokio::join!(server_side, client_side);

        assert_eq!(oa.transport, Transport::WebrtcDirect);
        assert_eq!(ob.transport, Transport::WebrtcDirect);
        assert_ne!(oa.we_offer, ob.we_offer, "exactly one peer offers");
    })
    .await
    .expect("caps negotiation completes within budget");
}

/// The full stack: from a real authenticated iroh link, `negotiate_session_halves`
/// negotiates WebRTC, brokers the offer/answer over iroh, establishes a data
/// channel, and hands back working halves. Data then flows over the *WebRTC*
/// transport both directions, and the byte counters track it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_upgrades_to_webrtc_and_carries_data() {
    tokio::time::timeout(BUDGET, async {
        let server = bind_accept().await.expect("bind accept");
        let client = bind_connect().await.expect("bind connect");
        let _ = tokio::time::timeout(Duration::from_secs(10), server.online()).await;
        let _ = tokio::time::timeout(Duration::from_secs(10), client.online()).await;
        let addr = local_addr(&server).await;

        let ca = Arc::new(Counters::default());
        let cb = Arc::new(Counters::default());

        // Each side runs its full handshake (accept/connect → auth → negotiate +
        // WebRTC upgrade) concurrently, returning the session halves + transport.
        let ca2 = ca.clone();
        let server_side = async {
            let mut la = accept_link(&server).await.expect("accept link");
            authenticate(&mut la, &SECRET).await.expect("server auth");
            negotiate_session_halves(la, ca2).await
        };
        let cb2 = cb.clone();
        let client_side = async {
            let mut lb = connect_link(&client, addr).await.expect("connect link");
            authenticate(&mut lb, &SECRET).await.expect("client auth");
            negotiate_session_halves(lb, cb2).await
        };
        let ((mut sink_a, mut src_a, _keep_a, ta), (mut sink_b, mut src_b, _keep_b, tb)) =
            tokio::join!(server_side, client_side);

        // The data plane actually upgraded to WebRTC on both ends.
        assert_eq!(ta, Transport::WebrtcDirect, "server upgraded to webrtc");
        assert_eq!(tb, Transport::WebrtcDirect, "client upgraded to webrtc");

        // And the negotiated transport carries traffic both ways.
        sink_a.send(b"server->client".to_vec()).await.unwrap();
        assert_eq!(
            src_b.recv().await.unwrap().expect("client receives"),
            b"server->client"
        );
        sink_b.send(b"client->server".to_vec()).await.unwrap();
        assert_eq!(
            src_a.recv().await.unwrap().expect("server receives"),
            b"client->server"
        );

        // Byte counters flowed through the WebRTC halves (so metrics still work).
        assert!(ca.sent.load(Ordering::Relaxed) >= b"server->client".len() as u64);
        assert!(cb.recv.load(Ordering::Relaxed) >= b"server->client".len() as u64);
    })
    .await
    .expect("full stack completes within budget");
}

/// The native end of the **browser↔native bridge**: two native `webrtc-rs` peers
/// connect through the WebSocket signaling server — the *same* server and JSON
/// protocol the browser uses — and exchange data over WebRTC. Since the wire is
/// identical to the browser's `connect_via_signaling`, a browser peer can connect
/// to a native peer the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_peers_bridge_via_signaling_server() {
    use riftpipe::net::webrtc::connect_via_signaling;
    use riftpipe::net::Link;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(riftpipe::signal::serve_on(listener));
    let url = format!("ws://{addr}/");
    let room = "native-bridge-it";

    let (ra, rb) = tokio::time::timeout(BUDGET, async {
        tokio::join!(connect_via_signaling(&url, room), connect_via_signaling(&url, room))
    })
    .await
    .expect("connect via signaling within budget");
    let (mut la, mut lb) = (ra.expect("peer A"), rb.expect("peer B"));

    la.send(b"native over signaling".to_vec()).await.unwrap();
    assert_eq!(lb.recv().await.unwrap().expect("B receives"), b"native over signaling");
    lb.send(b"reply".to_vec()).await.unwrap();
    assert_eq!(la.recv().await.unwrap().expect("A receives"), b"reply");
}
