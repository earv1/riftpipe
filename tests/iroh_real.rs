//! One REAL integration test (DESIGN.md §5/§11): two actual iroh endpoints in one
//! process, talking over a real QUIC connection on loopback, converging via the
//! same `sync_full` driver as everything else. This proves the transport is
//! wired correctly end to end — not mocked.

use std::time::Duration;

use riftpipe::sync::sync_full;
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

/// `IrohSource::recv` must be cancel-safe: the folder session's `select!`
/// drops an in-flight `recv` on every timer tick. Here the receiver wraps each
/// `recv` in a tiny timeout (cancelling it mid-frame, over and over) while the
/// sender streams large frames that span many QUIC packets; every frame must
/// still arrive whole and in order. (With `read_exact`, a cancelled read lost
/// its partial bytes and the length-prefixed framing desynced.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_recv_never_loses_or_corrupts_frames() {
    use std::sync::Arc;
    use riftpipe::net::{Counters, Sink, Source};

    const FRAMES: usize = 40;
    fn frame(i: usize) -> Vec<u8> {
        (0..(100_000 + i * 997)).map(|j| (i * 31 + j) as u8).collect()
    }

    let server = bind_accept().await.expect("bind accept endpoint");
    let client = bind_connect().await.expect("bind connect endpoint");
    let server_addr = local_addr(&server).await;

    tokio::time::timeout(Duration::from_secs(30), async {
        let receiver = async {
            let link = accept_link(&server).await.expect("accept link");
            let (_sink, mut src) = link.into_halves(Arc::new(Counters::default()));
            let (mut got, mut cancelled) = (Vec::new(), 0usize);
            while got.len() < FRAMES {
                match tokio::time::timeout(Duration::from_micros(50), src.recv()).await {
                    Err(_) => cancelled += 1,
                    Ok(r) => got.push(r.expect("recv ok").expect("frame, not EOF")),
                }
            }
            (got, cancelled)
        };
        let sender = async {
            let link = connect_link(&client, server_addr.clone()).await.expect("connect link");
            let (mut sink, _src) = link.into_halves(Arc::new(Counters::default()));
            for i in 0..FRAMES {
                sink.send(frame(i)).await.expect("send");
            }
            sink
        };
        let ((got, cancelled), _sink) = tokio::join!(receiver, sender);
        assert!(cancelled > 0, "the test never cancelled a recv");
        for (i, f) in got.iter().enumerate() {
            assert!(*f == frame(i), "frame {i} corrupted (len {} vs {})", f.len(), frame(i).len());
        }
    })
    .await
    .expect("cancel-safety exchange timed out");

    server.close().await;
    client.close().await;
}
