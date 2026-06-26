//! Secure "connect anywhere" layer (DESIGN.md §7, §8).
//!
//! Encryption + peer authentication come **for free** from iroh: every QUIC
//! connection is end-to-end encrypted (TLS 1.3), and because you dial an
//! `EndpointId` that *is* the peer's ed25519 public key, you're cryptographically
//! talking to the right peer — no MITM. iroh's relays + discovery let peers reach
//! each other across NATs by identity, not IP.
//!
//! What this module adds is **authorization**: a secret capability in the ticket,
//! proven with a BLAKE3 challenge-response over the (already encrypted) channel,
//! so only peers holding the secret may join the document.

use serde::{Deserialize, Serialize};

use iroh::EndpointAddr;

use crate::net::{anyerr, Link, Result};

/// A copy-pasteable invite: where to dial + the shared secret capability.
#[derive(Serialize, Deserialize)]
pub struct Ticket {
    pub addr: EndpointAddr,
    /// 256-bit secret — both the doc's access capability and identity (§7).
    pub secret: [u8; 32],
}

impl Ticket {
    pub fn new(addr: EndpointAddr) -> Self {
        Ticket {
            addr,
            secret: rand::random(),
        }
    }

    /// A single base32 token (uppercase alphanumeric — clean to copy in a shell).
    /// Postcard (compact binary) keeps it short.
    pub fn encode(&self) -> String {
        let bytes = postcard::to_allocvec(self).expect("serialize ticket");
        data_encoding::BASE32_NOPAD.encode(&bytes)
    }

    pub fn decode(s: &str) -> Result<Ticket> {
        let bytes = data_encoding::BASE32_NOPAD
            .decode(s.trim().as_bytes())
            .map_err(anyerr)?;
        postcard::from_bytes(&bytes).map_err(anyerr)
    }
}

fn response(secret: &[u8; 32], nonce: &[u8]) -> Vec<u8> {
    let mut h = blake3::Hasher::new();
    h.update(secret);
    h.update(nonce);
    h.finalize().as_bytes().to_vec()
}

/// Mutual challenge-response over an (encrypted) link. Each side proves it holds
/// `secret` without revealing it: exchange fresh nonces, return
/// `BLAKE3(secret ‖ peer_nonce)`, verify against `BLAKE3(secret ‖ my_nonce)`.
/// Fresh nonces defeat replay. Rejects a peer that doesn't hold the secret.
pub async fn authenticate(link: &mut dyn Link, secret: &[u8; 32]) -> Result<()> {
    let my_nonce: [u8; 16] = rand::random();
    link.send(my_nonce.to_vec()).await?;
    let peer_nonce = link
        .recv()
        .await?
        .ok_or_else(|| anyerr("auth: peer sent no nonce"))?;

    link.send(response(secret, &peer_nonce)).await?;
    let peer_resp = link
        .recv()
        .await?
        .ok_or_else(|| anyerr("auth: peer sent no response"))?;

    if peer_resp != response(secret, &my_nonce) {
        return Err(anyerr("auth failed: peer does not hold the doc secret"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::mock_pair;

    #[tokio::test]
    async fn matching_secret_authenticates() {
        let secret = [42u8; 32];
        let (mut a, mut b) = mock_pair();
        let (ra, rb) = tokio::join!(authenticate(&mut a, &secret), authenticate(&mut b, &secret));
        assert!(ra.is_ok() && rb.is_ok());
    }

    #[tokio::test]
    async fn mismatched_secret_rejected() {
        let (mut a, mut b) = mock_pair();
        let (ra, rb) = tokio::join!(
            authenticate(&mut a, &[1u8; 32]),
            authenticate(&mut b, &[2u8; 32]),
        );
        assert!(ra.is_err() || rb.is_err(), "mismatched secrets must fail");
    }
}
