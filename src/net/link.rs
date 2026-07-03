//! Transport-agnostic messaging (DESIGN.md §5). A `Link` is a message-oriented
//! bidirectional byte channel to one peer; everything above it (the sync
//! drivers in `sync/`) is identical whether the link is in-memory (tests) or
//! real iroh QUIC.
//!
//! This is the seam that makes integration tests cheap: spin up N mock clients
//! over `MockNet`/`mock_pair`, run the SAME `sync::sync_full` driver the real
//! transport uses, and assert convergence — no sockets required.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};

pub type BoxErr = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, BoxErr>;

pub fn anyerr<E: std::fmt::Display>(e: E) -> BoxErr {
    e.to_string().into()
}

/// A bidirectional, message-framed channel to a single peer.
#[async_trait]
pub trait Link: Send {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()>;
    /// Next message, or `None` when the peer/link is closed.
    async fn recv(&mut self) -> Result<Option<Vec<u8>>>;
    /// Gracefully signal "no more data" and wait for the peer to do the same, so
    /// buffered messages are delivered before the link is dropped. Default no-op
    /// (in-memory links need no teardown); the iroh link finishes its QUIC stream
    /// here, which is required or the last message gets reset on drop.
    async fn done(&mut self) -> Result<()> {
        Ok(())
    }
}

/// The send half of a split link. Splitting lets a session push (on a local
/// change) and pull (on arrival) concurrently instead of in lockstep rounds.
/// Every transport's halves implement these two traits, so everything above
/// (`sync::pipe`, `sync::folder`, `sync::board`) is transport-blind.
#[async_trait]
pub trait Sink: Send {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()>;
    /// Signal end-of-data so the last message is delivered before drop.
    async fn finish(&mut self);
}

/// The receive half of a split link.
#[async_trait]
pub trait Source: Send {
    /// Next message, or `None` when the peer/link is closed.
    async fn recv(&mut self) -> Result<Option<Vec<u8>>>;
}

// ---------------------------------------------------------------------------
// Byte-counting wrapper — instruments any link for the stats overlay (§14.2)
// without touching the sync driver.
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Counters {
    pub sent: AtomicU64,
    pub recv: AtomicU64,
}

pub struct CountingLink<L: Link> {
    inner: L,
    counters: Arc<Counters>,
}

impl<L: Link> CountingLink<L> {
    pub fn new(inner: L) -> (Self, Arc<Counters>) {
        let counters = Arc::new(Counters::default());
        (
            CountingLink {
                inner,
                counters: counters.clone(),
            },
            counters,
        )
    }
}

#[async_trait]
impl<L: Link> Link for CountingLink<L> {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        self.counters.sent.fetch_add(msg.len() as u64, Ordering::Relaxed);
        self.inner.send(msg).await
    }
    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        let got = self.inner.recv().await?;
        if let Some(b) = &got {
            self.counters.recv.fetch_add(b.len() as u64, Ordering::Relaxed);
        }
        Ok(got)
    }
    async fn done(&mut self) -> Result<()> {
        self.inner.done().await
    }
}

// ---------------------------------------------------------------------------
// Mock point-to-point link (two ends wired by a pair of mpsc channels).
// ---------------------------------------------------------------------------

pub struct MockLink {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

/// Two ends of an in-memory link.
pub fn mock_pair() -> (MockLink, MockLink) {
    let (tx_a, rx_b) = mpsc::unbounded_channel();
    let (tx_b, rx_a) = mpsc::unbounded_channel();
    (
        MockLink { tx: tx_a, rx: rx_a },
        MockLink { tx: tx_b, rx: rx_b },
    )
}

#[async_trait]
impl Link for MockLink {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        self.tx.send(msg).map_err(anyerr)
    }
    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.rx.recv().await)
    }
}

// ---------------------------------------------------------------------------
// Mock broadcast bus — N clients, each sees everyone else's messages.
// ---------------------------------------------------------------------------

/// A shared in-memory bus. Create it, then hand each client a `port(id)`.
pub struct MockNet {
    tx: broadcast::Sender<(usize, Vec<u8>)>,
}

impl Default for MockNet {
    fn default() -> Self {
        Self::new()
    }
}

impl MockNet {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(4096);
        Self { tx }
    }
    /// A port for client `id`. Ports must be created before any sends so each is
    /// subscribed (broadcast only delivers messages sent after subscription).
    pub fn port(&self, id: usize) -> MockPort {
        MockPort {
            id,
            tx: self.tx.clone(),
            rx: self.tx.subscribe(),
        }
    }
}

pub struct MockPort {
    id: usize,
    tx: broadcast::Sender<(usize, Vec<u8>)>,
    rx: broadcast::Receiver<(usize, Vec<u8>)>,
}

#[async_trait]
impl Link for MockPort {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        self.tx.send((self.id, msg)).map_err(anyerr)?;
        Ok(())
    }
    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            match self.rx.recv().await {
                Ok((from, msg)) if from != self.id => return Ok(Some(msg)),
                Ok(_) => continue, // skip our own broadcast
                Err(broadcast::error::RecvError::Closed) => return Ok(None),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }
}
