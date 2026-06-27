//! Minimal WebSocket **signaling** server for browser↔browser WebRTC
//! (`docs/planned/transport-negotiation.md`). The connection id in the page URL
//! (`location.hash`) is a **room**: the two peers that join a room are paired, and
//! the server relays their WebRTC offer/answer between them. It is **content-blind**
//! — it forwards opaque payloads, never parsing SDP — and holds no state beyond
//! "who is in which room," so a shared link is all two people need to connect.
//!
//! This is the one server browser↔browser needs (the brokered handshake; the data
//! then flows **direct** over the WebRTC channel). Self-hostable, tiny.
//!
//! Protocol: a client connects to `ws://host:port/?room=<id>`. When the room
//! reaches two peers, the server tells the **second** joiner it is the `offerer`
//! and the first the `answerer` (`{"type":"role","role":...}`); thereafter every
//! message a peer sends is relayed verbatim to the other peer in the room.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

type Tx = UnboundedSender<Message>;
/// room id → the peers currently in it (id + outbound channel). Max 2.
type Rooms = Arc<Mutex<HashMap<String, Vec<(u64, Tx)>>>>;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Caps so a public, self-hostable instance can't be trivially exhausted by a
/// client opening many unique rooms or a giant room id (room ids are caller-chosen).
const MAX_ROOMS: usize = 50_000;
const MAX_ROOM_ID_LEN: usize = 128;

/// Serve the signaling server on `127.0.0.1:port`. Blocks (runs forever).
pub async fn serve(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    eprintln!("[signal] signaling server on ws://127.0.0.1:{port}/?room=<id>");
    serve_on(listener).await;
    Ok(())
}

/// Accept loop over an already-bound listener (so tests can use an ephemeral port).
pub async fn serve_on(listener: TcpListener) {
    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));
    while let Ok((stream, _)) = listener.accept().await {
        let rooms = rooms.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, rooms).await {
                eprintln!("[signal] connection error: {e}");
            }
        });
    }
}

// The large-Err lint fires on tungstenite's handshake callback signature
// (`Result<Response, ErrorResponse>`), whose error type we don't control.
#[allow(clippy::result_large_err)]
async fn handle(stream: TcpStream, rooms: Rooms) -> Result<(), Box<dyn std::error::Error>> {
    // Capture the room id from the upgrade request's query string.
    let room_cell = Arc::new(Mutex::new(String::new()));
    let rc = room_cell.clone();
    let ws = tokio_tungstenite::accept_hdr_async(stream, move |req: &Request, resp: Response| {
        if let Some(q) = req.uri().query() {
            for kv in q.split('&') {
                if let Some(v) = kv.strip_prefix("room=") {
                    *rc.lock().unwrap() = v.to_string();
                }
            }
        }
        Ok(resp)
    })
    .await?;

    let room = room_cell.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if room.is_empty() || room.len() > MAX_ROOM_ID_LEN {
        return Ok(()); // no/oversized room → nothing to do
    }

    let (mut write, mut read) = ws.split();
    let (tx, mut rx) = unbounded_channel::<Message>();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    // Join the room; assign roles once the pair is complete. `joined` gates the
    // relay loop + leave logic so a *rejected* (room-full / over-capacity) peer can
    // never inject into or tear down an established pair.
    let joined = {
        let mut guard = rooms.lock().unwrap_or_else(|e| e.into_inner());
        match guard.get(&room).map(Vec::len) {
            Some(n) if n >= 2 => {
                let _ = tx.send(err_msg("room full"));
                false
            }
            None if guard.len() >= MAX_ROOMS => {
                let _ = tx.send(err_msg("server at capacity"));
                false
            }
            _ => {
                let peers = guard.entry(room.clone()).or_default();
                peers.push((id, tx.clone()));
                if peers.len() == 2 {
                    // Newest joiner offers; the one already waiting answers.
                    let _ = peers[1].1.send(role_msg("offerer"));
                    let _ = peers[0].1.send(role_msg("answerer"));
                }
                true
            }
        }
    };

    // Pump outbound channel → socket.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
    });

    // A rejected peer was never added to the room: deliver its error and leave,
    // running NEITHER the relay loop nor the leave/peer-left logic.
    if !joined {
        drop(tx); // flush the queued error, then the writer task ends
        let _ = writer.await;
        return Ok(());
    }

    // Relay everything this peer sends to the *other* peer in the room.
    while let Some(Ok(msg)) = read.next().await {
        if msg.is_text() || msg.is_binary() {
            relay(&rooms, &room, id, msg);
        } else if msg.is_close() {
            break;
        }
    }

    // Leave the room.
    {
        let mut guard = rooms.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(peers) = guard.get_mut(&room) {
            peers.retain(|(pid, _)| *pid != id);
            // Tell a lingering peer the other side left.
            for (_, peer_tx) in peers.iter() {
                let _ = peer_tx.send(Message::text("{\"type\":\"peer-left\"}"));
            }
            if peers.is_empty() {
                guard.remove(&room);
            }
        }
    }
    writer.abort();
    Ok(())
}

fn role_msg(role: &str) -> Message {
    Message::text(format!("{{\"type\":\"role\",\"role\":\"{role}\"}}"))
}

fn err_msg(error: &str) -> Message {
    Message::text(format!("{{\"type\":\"error\",\"error\":\"{error}\"}}"))
}

/// Forward `msg` to every peer in `room` other than `from`.
fn relay(rooms: &Rooms, room: &str, from: u64, msg: Message) {
    let guard = rooms.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(peers) = guard.get(room) {
        for (pid, tx) in peers {
            if *pid != from {
                let _ = tx.send(msg.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;

    async fn next_text<S>(ws: &mut S) -> String
    where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => return t.to_string(),
                Some(Ok(_)) => continue,
                _ => panic!("stream closed before a text message"),
            }
        }
    }

    #[tokio::test]
    async fn pairs_a_room_assigns_roles_and_relays() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_on(listener));
        let url = format!("ws://{addr}/?room=demo");

        // First peer joins and waits.
        let (mut a, _) = connect_async(&url).await.unwrap();
        // Second peer joins → roles are assigned to both.
        let (mut b, _) = connect_async(&url).await.unwrap();

        let role_b = next_text(&mut b).await;
        let role_a = next_text(&mut a).await;
        assert!(role_b.contains("offerer"), "2nd joiner offers: {role_b}");
        assert!(role_a.contains("answerer"), "1st joiner answers: {role_a}");

        // Offerer's payload is relayed verbatim to the answerer.
        b.send(Message::text("{\"type\":\"signal\",\"sdp\":\"OFFER\"}")).await.unwrap();
        let got = next_text(&mut a).await;
        assert!(got.contains("OFFER"), "relayed to peer: {got}");

        // And the reverse direction.
        a.send(Message::text("ANSWER")).await.unwrap();
        let got = next_text(&mut b).await;
        assert_eq!(got, "ANSWER");
    }

    #[tokio::test]
    async fn third_peer_in_a_room_is_rejected() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_on(listener));
        let url = format!("ws://{addr}/?room=full");

        let (mut _a, _) = connect_async(&url).await.unwrap();
        let (mut _b, _) = connect_async(&url).await.unwrap();
        let (mut c, _) = connect_async(&url).await.unwrap();
        let msg = next_text(&mut c).await;
        assert!(msg.contains("room full"), "3rd peer rejected: {msg}");
    }
}
