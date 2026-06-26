//! Real iroh 1.0 transport (DESIGN.md §5/§11). Implements `net::Link` over a QUIC
//! bidirectional stream with length-prefixed framing, plus helpers to bind
//! endpoints and obtain accept/connect links. The sync logic on top is the same
//! `net::sync_full` the mock transport uses.

use std::time::Duration;

use async_trait::async_trait;
use iroh::endpoint::{presets, Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::io::AsyncWriteExt;

use crate::net::{anyerr, Link, Result};

/// Application-layer protocol id — peers must agree on this to connect.
pub const ALPN: &[u8] = b"autoshare/0";

/// Bind an endpoint that accepts incoming autoshare connections.
pub async fn bind_accept() -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .map_err(anyerr)
}

/// Bind an endpoint used to dial out.
pub async fn bind_connect() -> Result<Endpoint> {
    Endpoint::bind(presets::N0).await.map_err(anyerr)
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
    Ok(IrohLink { conn, send, recv })
}

/// Dial `addr` and open a bi-stream `Link`.
pub async fn connect_link(endpoint: &Endpoint, addr: EndpointAddr) -> Result<IrohLink> {
    let conn = endpoint.connect(addr, ALPN).await.map_err(anyerr)?;
    let (send, recv) = conn.open_bi().await.map_err(anyerr)?;
    Ok(IrohLink { conn, send, recv })
}

/// A `Link` over a QUIC bidirectional stream. Messages are framed as a u32
/// little-endian length followed by that many bytes. Holds the `Connection` so
/// it stays open until the link is dropped.
pub struct IrohLink {
    conn: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl IrohLink {
    /// The peer's endpoint id (for `connection_kind` lookups).
    pub fn remote_id(&self) -> EndpointId {
        self.conn.remote_id()
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
        let mut len = [0u8; 4];
        // A clean stream end shows up as an EOF on the length read.
        if self.recv.read_exact(&mut len).await.is_err() {
            return Ok(None);
        }
        let n = u32::from_le_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        self.recv.read_exact(&mut buf).await.map_err(anyerr)?;
        Ok(Some(buf))
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
