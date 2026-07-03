//! iroh in the browser: a relay-brokered `Link`. Browsers can't UDP-hole-punch,
//! so traffic rides n0's public relays (end-to-end encrypted — the relay can't
//! read it). Length-prefixed framing identical to the native `IrohLink`, so the
//! sync layer runs over it unchanged. No signaling server, nothing to host —
//! the connection id in the URL is an iroh ticket (the host's relay address).

use iroh::endpoint::{presets, Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, SecretKey};

/// ALPN for the kanban sync protocol.
pub const ALPN: &[u8] = b"riftpipe/kanban/0";

async fn sleep_ms(ms: u32) {
    gloo_timers::future::TimeoutFuture::new(ms).await;
}

/// Bind an endpoint that accepts incoming connections (the board "host"). Binding
/// under a *persisted* `sk` keeps the same EndpointId — and so the same shareable
/// ticket — across page reloads.
pub async fn bind_accept(sk: SecretKey) -> Result<Endpoint, String> {
    Endpoint::builder(presets::N0)
        .secret_key(sk)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .map_err(|e| e.to_string())
}

/// Bind an endpoint used only to dial out (the "joiner").
pub async fn bind_connect(sk: SecretKey) -> Result<Endpoint, String> {
    Endpoint::builder(presets::N0)
        .secret_key(sk)
        .bind()
        .await
        .map_err(|e| e.to_string())
}

/// Wait for the endpoint to acquire a dialable (relay) address before we share
/// it — in the browser that's the relay assignment, not a direct socket.
pub async fn wait_for_addr(ep: &Endpoint) -> EndpointAddr {
    for _ in 0..400 {
        let a = ep.addr();
        if !a.addrs.is_empty() {
            return a;
        }
        sleep_ms(50).await;
    }
    ep.addr()
}

/// Encode a dialable address as a shareable ticket string (postcard + hex).
pub fn ticket_of(addr: &EndpointAddr) -> String {
    let bytes = postcard::to_allocvec(addr).unwrap_or_default();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a ticket string back into a dialable address.
pub fn addr_of(ticket: &str) -> Result<EndpointAddr, String> {
    let bytes: Vec<u8> = (0..ticket.len() / 2)
        .map(|i| u8::from_str_radix(&ticket[i * 2..i * 2 + 2], 16))
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    postcard::from_bytes(&bytes).map_err(|e| e.to_string())
}

async fn write_framed(s: &mut SendStream, msg: &[u8]) -> Result<(), String> {
    s.write_all(&(msg.len() as u32).to_le_bytes())
        .await
        .map_err(|e| e.to_string())?;
    s.write_all(msg).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn read_framed(r: &mut RecvStream) -> Option<Vec<u8>> {
    let mut len = [0u8; 4];
    if r.read_exact(&mut len).await.is_err() {
        return None; // clean EOF
    }
    let n = u32::from_le_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await.ok()?;
    Some(buf)
}

/// A relay-brokered link over a QUIC bi-stream — same framing as native `IrohLink`.
pub struct IrohLink {
    _conn: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl IrohLink {
    /// Dial a peer's ticket address and open the sync stream (joiner side).
    pub async fn connect(ep: &Endpoint, addr: EndpointAddr) -> Result<IrohLink, String> {
        let conn = ep.connect(addr, ALPN).await.map_err(|e| e.to_string())?;
        let (send, recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
        Ok(IrohLink { _conn: conn, send, recv })
    }

    /// Accept one incoming connection's sync stream (host side).
    pub async fn accept(ep: &Endpoint) -> Result<IrohLink, String> {
        let incoming = ep
            .accept()
            .await
            .ok_or_else(|| "endpoint closed".to_string())?;
        let conn = incoming.await.map_err(|e| e.to_string())?;
        let (send, recv) = conn.accept_bi().await.map_err(|e| e.to_string())?;
        Ok(IrohLink { _conn: conn, send, recv })
    }

    pub async fn send(&mut self, msg: &[u8]) -> Result<(), String> {
        write_framed(&mut self.send, msg).await
    }

    /// `None` on a clean stream close.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        read_framed(&mut self.recv).await
    }

    /// Split into independent send/recv halves so a peer can push and pull
    /// concurrently (what `BoardSync` needs).
    pub fn into_halves(self) -> (IrohSink, IrohSource) {
        (
            IrohSink { _conn: self._conn, send: self.send },
            IrohSource { recv: self.recv },
        )
    }
}

/// Send half of a split iroh link.
pub struct IrohSink {
    _conn: Connection, // keep the connection alive while either half lives
    send: SendStream,
}

impl IrohSink {
    pub async fn send(&mut self, msg: &[u8]) -> Result<(), String> {
        write_framed(&mut self.send, msg).await
    }
}

/// Receive half of a split iroh link.
pub struct IrohSource {
    recv: RecvStream,
}

impl IrohSource {
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        read_framed(&mut self.recv).await
    }
}
