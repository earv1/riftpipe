//! Integration tests over the in-memory mock transport (DESIGN.md §5). These
//! exercise the exact `sync_full` driver the real iroh transport uses, but with
//! zero sockets — so they're fast and deterministic. Mock client 1, 2, ... N.

use riftpipe::net::{mock_pair, MockNet};
use riftpipe::sync::sync_full;
use riftpipe::crdt::text::EgWalkerText;

fn doc(name: &str, text: &str) -> EgWalkerText {
    let mut d = EgWalkerText::new(name);
    d.edit_to(text);
    d
}

#[tokio::test]
async fn two_mock_clients_converge() {
    let (mut l1, mut l2) = mock_pair();
    let mut a = doc("a", "alpha\n");
    let mut b = doc("b", "beta\n");

    tokio::join!(
        async { sync_full(&mut a, &mut l1, 1).await.unwrap() },
        async { sync_full(&mut b, &mut l2, 1).await.unwrap() },
    );

    assert_eq!(a.content(), b.content(), "replicas diverged");
    assert!(a.content().contains("alpha") && a.content().contains("beta"));
}

#[tokio::test]
async fn three_mock_clients_converge_over_bus() {
    let net = MockNet::new();
    // All ports must be subscribed before any sends.
    let (mut p0, mut p1, mut p2) = (net.port(0), net.port(1), net.port(2));
    let mut d0 = doc("c0", "c0-hi\n");
    let mut d1 = doc("c1", "c1-hi\n");
    let mut d2 = doc("c2", "c2-hi\n");

    // Each client broadcasts once and merges the two others' states.
    tokio::join!(
        async { sync_full(&mut d0, &mut p0, 2).await.unwrap() },
        async { sync_full(&mut d1, &mut p1, 2).await.unwrap() },
        async { sync_full(&mut d2, &mut p2, 2).await.unwrap() },
    );

    assert_eq!(d0.content(), d1.content());
    assert_eq!(d1.content(), d2.content());
    for needle in ["c0-hi", "c1-hi", "c2-hi"] {
        assert!(d0.content().contains(needle), "lost {needle}: {:?}", d0.content());
    }
}
