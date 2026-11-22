//! Real iroh 1.0 transport (DESIGN.md §5/§11). Implements `net::Link` over a QUIC
//! bidirectional stream with length-prefixed framing, plus helpers to bind
//! endpoints and obtain accept/connect links. The sync logic on top is the same
//! `sync::sync_full` the mock transport uses.

use std::time::Duration;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use iroh::endpoint::{presets, Connection, QuicTransportConfig, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::io::AsyncWriteExt;

use crate::net::{anyerr, Counters, Link, Result};

/// Application-layer protocol id — peers must agree on this to connect.
pub const ALPN: &[u8] = b"riftpipe/0";

/// QUIC liveness tuning: send keep-alives every 2s and time a connection out
/// after 6s of silence, so a vanished peer is detected quickly (the default is
/// ~30s) — which is what makes reconnection feel responsive.
fn quic_config() -> QuicTransportConfig {
    QuicTransportConfig::builder()
        .keep_alive_interval(Duration::from_secs(2))
        .max_idle_timeout(Some(
            Duration::from_secs(6).try_into().expect("valid idle timeout"),
        ))
        .default_path_keep_alive_interval(Duration::from_secs(2))
        .default_path_max_idle_timeout(Duration::from_secs(6))
        .build()
}

/// Bind an endpoint that accepts incoming riftpipe connections.
pub async fn bind_accept() -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .transport_config(quic_config())
        .bind()
        .await
        .map_err(anyerr)
}

/// Bind an endpoint used to dial out.
pub async fn bind_connect() -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .transport_config(quic_config())
        .bind()
        .await
        .map_err(anyerr)
}

/// This endpoint's dialable address. Waits briefly for the socket's direct
/// addresses so loopback works without relay/discovery.
pub async fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    for _ in 0..100 {
        let addr = endpoint.addr();
        if !addr.addrs.is_empty() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    endpoint.addr()
}

/// Accept one incoming connection and turn its first bi-stream into a `Link`.
pub async fn accept_link(endpoint: &Endpoint) -> Result<IrohLink> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| anyerr("endpoint closed"))?;
    let conn = incoming.await.map_err(anyerr)?;
    let (send, recv) = conn.accept_bi().await.map_err(anyerr)?;
    Ok(IrohLink { conn, send, recv, frames: FrameBuf::default() })
}

/// Dial `addr` and open a bi-stream `Link`.
pub async fn connect_link(endpoint: &Endpoint, addr: EndpointAddr) -> Result<IrohLink> {
    let conn = endpoint.connect(addr, ALPN).await.map_err(anyerr)?;
    let (send, recv) = conn.open_bi().await.map_err(anyerr)?;
    Ok(IrohLink { conn, send, recv, frames: FrameBuf::default() })
}

/// A `Link` over a QUIC bidirectional stream. Messages are framed as a u32
/// little-endian length followed by that many bytes. Holds the `Connection` so
/// it stays open until the link is dropped.
pub struct IrohLink {
    conn: Connection,
    send: SendStream,
    recv: RecvStream,
    frames: FrameBuf,
}

/// Inbound bytes not yet returned as a whole frame. Lives in the link (not in
/// a `recv` future's locals) so `recv` is **cancel-safe**: the session's
/// `select!` drops an in-flight `recv` on every timer tick, and a frame read
/// halfway must survive that, or the length-prefixed framing desyncs.
#[derive(Default)]
struct FrameBuf {
    pending: BytesMut,
}

/// Max bytes pulled from the stream per read.
const READ_CHUNK: usize = 64 * 1024;

impl FrameBuf {
    /// Split off the next complete `u32 LE length + body` frame, if buffered.
    fn take_frame(&mut self) -> Option<Vec<u8>> {
        let len: [u8; 4] = self.pending.get(..4)?.try_into().ok()?;
        let n = u32::from_le_bytes(len) as usize;
        if self.pending.len() < 4 + n {
            return None;
        }
        self.pending.advance(4);
        Some(self.pending.split_to(n).to_vec())
    }

    /// Next frame from `recv`. Cancel-safe: the only await is
    /// `RecvStream::read_chunk` (documented cancel-safe), and its bytes are
    /// appended to `pending` before anything else can be dropped. A clean end
    /// of stream (or a read error) between frames is `Ok(None)`, as before;
    /// inside a frame it is an error (truncated frame).
    async fn next(&mut self, recv: &mut RecvStream) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some(f) = self.take_frame() {
                return Ok(Some(f));
            }
            match recv.read_chunk(READ_CHUNK).await {
                Ok(Some(chunk)) => self.pending.extend_from_slice(&chunk),
                Ok(None) | Err(_) if self.pending.is_empty() => return Ok(None),
                Ok(None) => return Err(anyerr("iroh link: stream ended mid-frame")),
                Err(e) => return Err(anyerr(e)),
            }
        }
    }
}

impl IrohLink {
    /// The peer's endpoint id (for `connection_kind` lookups).
    pub fn remote_id(&self) -> EndpointId {
        self.conn.remote_id()
    }

    /// Split into independent send/recv halves for event-driven sync — so a peer
    /// can push (on a local change) and pull (on arrival) concurrently, instead
    /// of lockstep rounds. `counters` is shared with the metrics writer.
    pub fn into_halves(self, counters: Arc<Counters>) -> (IrohSink, IrohSource) {
        (
            IrohSink {
                _conn: self.conn,
                send: self.send,
                counters: counters.clone(),
            },
            IrohSource {
                recv: self.recv,
                frames: self.frames,
                counters,
            },
        )
    }
}

/// Send half of a split iroh link.
pub struct IrohSink {
    _conn: Connection, // keep the connection alive while either half lives
    send: SendStream,
    counters: Arc<Counters>,
}

/// Receive half of a split iroh link.
pub struct IrohSource {
    recv: RecvStream,
    frames: FrameBuf,
    counters: Arc<Counters>,
}

#[async_trait]
impl crate::net::Sink for IrohSink {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        self.counters
            .sent
            .fetch_add(msg.len() as u64, Ordering::Relaxed);
        let len = (msg.len() as u32).to_le_bytes();
        self.send.write_all(&len).await.map_err(anyerr)?;
        self.send.write_all(&msg).await.map_err(anyerr)?;
        self.send.flush().await.map_err(anyerr)?;
        Ok(())
    }

    /// Signal end-of-data so the last message is delivered before drop.
    async fn finish(&mut self) {
        let _ = self.send.finish();
    }
}

#[async_trait]
impl crate::net::Source for IrohSource {
    /// Await the next message — costs zero traffic/CPU while none arrives.
    /// Cancel-safe (see [`FrameBuf`]).
    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        let msg = self.frames.next(&mut self.recv).await?;
        if let Some(b) = &msg {
            self.counters.recv.fetch_add(b.len() as u64, Ordering::Relaxed);
        }
        Ok(msg)
    }
}

#[async_trait]
impl Link for IrohLink {
    async fn send(&mut self, msg: Vec<u8>) -> Result<()> {
        let len = (msg.len() as u32).to_le_bytes();
        self.send.write_all(&len).await.map_err(anyerr)?;
        self.send.write_all(&msg).await.map_err(anyerr)?;
        self.send.flush().await.map_err(anyerr)?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        // A clean stream end between frames is `None`. Cancel-safe.
        self.frames.next(&mut self.recv).await
    }

    async fn done(&mut self) -> Result<()> {
        // Signal end-of-data so our buffered messages are delivered reliably
        // (without this, dropping the SendStream resets it and the last message
        // is lost). Then read to EOF as a barrier so we don't tear down the
        // connection before the peer has consumed what we sent.
        self.send.finish().ok();
        let mut sink = [0u8; 64];
        loop {
            match self.recv.read(&mut sink).await {
                Ok(Some(_)) => continue, // trailing bytes (none expected)
                Ok(None) | Err(_) => break, // peer finished / stream ended
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frames reassemble across arbitrary chunk boundaries — including a split
    /// inside the length prefix — and nothing is consumed until a frame is
    /// whole (what makes a cancelled `recv` lossless).
    #[test]
    fn frame_buf_reassembles_across_chunk_boundaries() {
        let mut wire = Vec::new();
        for body in [&b"hello"[..], b"", b"a longer third frame"] {
            wire.extend_from_slice(&(body.len() as u32).to_le_bytes());
            wire.extend_from_slice(body);
        }
        for step in 1..=wire.len() {
            let mut fb = FrameBuf::default();
            let mut got = Vec::new();
            for chunk in wire.chunks(step) {
                fb.pending.extend_from_slice(chunk);
                while let Some(f) = fb.take_frame() {
                    got.push(f);
                }
            }
            assert_eq!(got, vec![b"hello".to_vec(), Vec::new(), b"a longer third frame".to_vec()], "step {step}");
            assert!(fb.pending.is_empty());
        }
    }
}
