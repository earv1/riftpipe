//! One REAL integration test (DESIGN.md §5/§11): two actual iroh endpoints in one
//! process, talking over a real QUIC connection on loopback, converging via the
//! same `sync_full` driver as everything else. This proves the transport is
//! wired correctly end to end — not mocked.

use std::time::Duration;

use riftpipe::net::sync_full;
use riftpipe::crdt::text::EgWalkerText;
use riftpipe::net::transport::{accept_link, bind_accept, bind_connect, connect_link, local_addr};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_real_iroh_clients_converge() {
    let server = bind_accept().await.expect("bind accept endpoint");
    let client = bind_connect().await.expect("bind connect endpoint");
    let server_addr = local_addr(&server).await;

    let mut doc_s = EgWalkerText::new("server");
    doc_s.edit_to("from-server\n");
    let mut doc_c = EgWalkerText::new("client");
    doc_c.edit_to("from-client\n");

    let server_side = async {
        let mut link = accept_link(&server).await.expect("accept link");
        sync_full(&mut doc_s, &mut link, 1).await.expect("server sync");
    };
    let client_side = async {
        let mut link = connect_link(&client, server_addr.clone())
            .await
            .expect("connect link");
        sync_full(&mut doc_c, &mut link, 1).await.expect("client sync");
    };

    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(server_side, client_side);
    })
    .await
    .expect("real iroh exchange timed out");

    assert_eq!(doc_s.content(), doc_c.content(), "real replicas diverged");
    assert!(doc_s.content().contains("from-server"));
    assert!(doc_s.content().contains("from-client"));

    server.close().await;
    client.close().await;
}
