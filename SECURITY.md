# Security & trust model

This document describes **who can read and write a shared riftpipe board**, what
is enforced cryptographically, and what is not yet. It is deliberately honest
about the boundary: a board is **sealed to link-holders** — outsiders, the relay,
and a DHT are locked out — but among link-holders there is not yet any authorship
or read/write distinction.

Diagrams here are generated from [`likec4/security.likec4`](likec4/security.likec4)
via `npm run diagrams` — edit the model, not the images.

## TL;DR

- **The share link is a bearer capability.** Anyone who has the URL can join a
  board, read everything, and write anything. There are no accounts, roles, or
  per-person access.
- **But only link-holders can find or read a board.** The gossip topic is
  `H(secret)` and every message is AEAD-sealed with a key derived from the same
  secret — so a stranger who only learns the host's public key (or, later, a DHT)
  cannot derive the topic, and the relay carries only ciphertext. (Shipped as
  `PROTOCOL_VERSION` 2; the secret rides the link fragment.)
- **Among holders there is not yet any authenticity.** One shared symmetric key
  means any *holder* can still forge messages attributed to other peers, freeze a
  file, or replace a document — sealing stops outsiders, not insiders. Closing
  that needs **signing** (below), which isn't built yet.
- **Restricted (claimed) boards seal content under a rotating epoch key.** Each
  approved device gets the key wrapped to its own encryption key; removing a
  device rotates to a new epoch wrapped only to those remaining, so the removed
  device decrypts nothing from then on (`PROTOCOL_VERSION` 4). **Open boards are
  unchanged:** the link is still full read + write access, and removing one
  person there still means minting a new board (new secret ⇒ new link) and
  re-sharing with everyone else.

If a board is sensitive, treat the URL like a password: possession is full access,
and anyone you share it with is fully trusted until signing lands.

## Today: sealed to link-holders

![Sealed board — today](docs/diagrams/sealed_today.png)

### What a share link grants

The share link's URL fragment (`#…`) carries a **ticket**: a 32-byte random
**secret** plus the host's relay/direct address (`hex(postcard((secret, addr)))`,
identical on browser and CLI). The fragment never leaves the browser — it isn't
sent to the Pages server, and doesn't appear in referrers or server logs (the one
property that makes a URL an acceptable place to put a secret at all).

Possession of that link is **full read + write**. There is no invite step, no
approval, no viewer/editor distinction — yet.

### The rendezvous id is derived from the secret

A board is an [iroh-gossip](https://github.com/n0-computer/iroh-gossip) topic, and
the `TopicId` is **`blake3::derive_key("…topic…", secret)`** — a one-way function
of the secret. A stranger who learns the host's public `EndpointId` (which
discovery infrastructure sees, and which a DHT would broadcast) *cannot* derive
the topic, so they can't find the board. This is Hypercore's
`discoveryKey = hash(publicKey)` idea, applied to our gossip topic — and it means
the upcoming DHT/pkarr discovery work no longer widens the attack surface.

### Every frame is sealed

Each gossip frame is AEAD-sealed with **XChaCha20-Poly1305** under a *separate*
key, `blake3::derive_key("…aead…", secret)` (domain-separated from the topic id,
so neither reveals the other). XChaCha's 192-bit nonce lets every peer use a fresh
random nonce with no shared counter — essential in a mesh where everyone encrypts
independently. The relay and any non-holder see only ciphertext and cannot inject
a frame that will decrypt. A peer on the wrong secret lands on a different topic
and never even becomes a neighbor (verified: a one-byte-corrupted ticket never
syncs). Sealing wraps each frame *after* chunking, so it composes with the 4 KiB
chunk framing (a 12 KiB file syncs sealed + chunked, browser↔CLI).

The lot lives in [`core/src/crypto.rs`](core/src/crypto.rs) so native
(`sync::mesh`) and browser (`web::gossip`) derive the identical topic + key and
interoperate.

### What is NOT yet enforced — the insider gap

![Remaining: signing](docs/diagrams/remaining.png)

Sealing stops *outsiders*. It does nothing about a misbehaving *holder*, because
every holder shares the one symmetric key. Gossip's `delivered_from` is the
relaying peer, not the author, and frames are sealed but **not signed**, so any
link-holder can still:

- **Forge `Presence`/`Version`** for other peers (the routing map and the
  version-mismatch banner trust the claimed id).
- **Freeze a structural file** by broadcasting an `Lww` with `version = u64::MAX`
  (LWW versions are unvalidated wall-clock ms — nothing newer ever wins).
- **Replace a whole text document** by crafting one with a higher origin agent id,
  which the origin-conflict adoption rule in `core::sync` accepts.

Closing this is the **signing** follow-on: a per-message Ed25519 signature makes
authorship provable (killing the forgeries), and a read/write key split — a
signing key distinct from the read/seal key — enables genuine **view-only links**.
Both are natural once messages are signed; neither is built yet.

### What the transport already protected

Independently of the above, the iroh/QUIC transport encrypts each hop, so the n0
relay only ever saw ciphertext in transit; the app-layer sealing additionally
blinds the relay to the *topic* and makes the content unreadable even to a peer
that reaches the swarm on a guessed address. The relay still sees **connection
metadata** (which endpoints talk, when).

## Restricted boards: epoch-sealed content

A **restricted** CLI board (`connect … --restricted`) or a **claimed** browser
board means true **Restricted** access, not "view open, write gated": content
is sealed a *second* time, inside the outer board-secret frame, under the
current **epoch key** (`core::epoch`, `PROTOCOL_VERSION` 4). The topic and the
outer seal stay exactly as described above — a rotated topic would split the
swarm and strand late joiners — so content, not the rendezvous, is what
rotates.

- **Two-layer seal.** `MeshMsg::Sealed` carries `Sync`/`SignedSync` sealed
  under `MeshCipher::from_epoch(key)`, bound to `(seq, id)` as AEAD associated
  data so a header can't be swapped onto another body. A not-yet-approved
  device still exchanges `Request`/`Trust`/`Epoch` control messages under the
  outer seal only — it just can't open `Sealed` content.
- **Wrapped keys travel Admin-signed.** `MeshMsg::Epoch(EpochAnnounce)` carries
  one wrapped copy of the epoch key per authorized device, signed by an Admin;
  peers accept an announce only from a signer their trust log already knows is
  an Admin. An 8-byte key fingerprint (not the key) identifies "which epoch,"
  so two Admins rotating at the same instant converge on the same winner
  everywhere.
- **Any online Admin rotates on revoke, and auto add-wraps on grant.** An
  Admin rotates whenever the epoch it holds still wraps a device the trust log
  has just revoked (`riftpipe revoke <dir> <id>`, or the browser owner's
  "Remove"); any online Admin also wraps a newly-granted device into the
  current epoch without a rotation, which is what makes `--grant`/"Approve"
  deliver read access, not just write access.
- **Older epochs keep working for the session.** Keys are kept as long as the
  process runs, so content sealed under any epoch you hold still opens; a
  message for an epoch you don't hold yet is parked (bounded) and replayed
  once its key arrives.
- **What a revoked device sees.** It stays a gossip member — it still opens
  `Presence`/`Version`/the trust log (including its own `Revoke`)/`Request`s/
  `Epoch` announces, so it learns device ids and how many devices are wrapped
  in, but not the keys. Every content message sealed under the new epoch fails
  to open (native logs `[epoch] no key for epoch N — this device is not
  wrapped in`; the browser reports `canRead: false`), and its own writes are
  rejected everywhere. It keeps whatever it already decrypted. During the
  propagation window, a remaining member that hasn't yet seen the revoke
  still seals under the old epoch, which the revoked device can still read —
  inherent to eventual consistency, not a bug. A member that has seen the
  revoke but not yet the new epoch holds its writes (it never seals under an
  epoch that still wraps a revoked device) and sends them once the new epoch
  arrives.

## Passkey device linking (protocol v5)

On a claimed browser board the owner can register a **passkey** ("🔑 Claim with
passkey" / "🔑 Add passkey"); another of the owner's devices that has the same
passkey (synced by iCloud Keychain / Google Password Manager / 1Password, a
security key, or a phone) then joins with **"🔑 Link with my passkey"** — no
approve click. Two trust-log entries carry it (`core::identity`,
`PROTOCOL_VERSION` 5); the WebAuthn ceremony is `web/src/passkey.rs`, the
check `core::passkey`.

- **Registration is Admin-signed.** `TrustEntry::Passkey { cred, role, by, … }`
  is signed by `by`'s Ed25519 device key and applies only while `by` is an
  Admin. It stores public data only (credential id, COSE alg, public key, RP
  ID). There's no proof-of-possession assertion at registration — an Admin can
  already grant anyone, so a bogus registration only hurts its own board.
- **Every link is verified offline on every peer**, native and browser.
  `TrustEntry::PasskeyLink { device, cred_id, nonce, assertion }` applies only
  if the assertion checks out against the registered key: ES256 or EdDSA
  signature over `authenticatorData ‖ SHA-256(clientDataJSON)`,
  `rpIdHash == SHA-256(rp_id)`, the user-present flag set,
  `type == "webauthn.get"`, and the challenge equal to
  `blake3("riftpipe-passkey-link-v1", founder, device, nonce)` — so an assertion
  is scoped to **one board and one device** and can't be replayed onto another
  board that registered the same passkey, or for another device.
- **A linked device is an Admin** (the role stored in the registration; the UI
  always registers Admin — the passkey is the owner's identity). A passkey
  speaks only while its registrant is still an Admin.
- **Reading starts once an online Admin wraps it in.** The linked device
  broadcasts its `PasskeyLink` then its read request; any online Admin holding
  the epoch key wraps it into the current epoch, same as after an approve.
- **v4 peers don't understand it.** A v4 peer fails to decode the new `Trust`
  frames and skips them, so it never authorizes a passkey-linked device and
  drops its writes; the version bump surfaces the mismatch.

Known limits:

- **No attestation**: any authenticator is accepted (ES256 or EdDSA only —
  e.g. Windows Hello's RS256 keys can't be registered).
- **No passkey revocation yet.** A compromised passkey keeps linking devices
  for as long as its registrant is an Admin — mint a new board.
- **Revoking a passkey-linked device doesn't stick against the passkey
  holder**, who can simply link again. By design: the passkey *is* the owner.
- **The sign counter is ignored** (synced passkeys report 0), so a cloned
  authenticator isn't detected.
- **RP ID = the page hostname.** Every project page on `<user>.github.io`
  shares the passkey (a page there could request an assertion from it), and a
  board claimed on one host can't be passkey-linked from another.
- **IP-address origins are unsupported** (WebAuthn rejects them); the passkey
  buttons are hidden there — use `localhost`, not `127.0.0.1`.
- **A still-open v4 tab** rewriting `riftpipe:trust:v2:<topic>` can drop the
  v5 entries from localStorage; gossip restores them from peers.

## Still open

- **Signing (insider authenticity) + read/write split** — described above; the
  next security step.
- **Anyone-with-link Viewer mode.** `core::epoch::AccessMode::LinkViewer`
  exists and both drivers route reads/writes through the one `mode()` switch,
  but it isn't reachable yet — it needs an Admin-signed access setting to pick
  it, with no content-wire change once that lands.
- **Passkey revocation** (an Admin-signed entry retiring a registered passkey),
  the QR / short-code device-link variant, and a CLI passkey ceremony (native
  peers verify passkey entries but can't create or assert passkeys).

### Known limits

- **A self-removal with no admin online holds every member's writes.** A
  device may revoke itself, but only an Admin rotates the epoch. Until an
  Admin comes online and rotates, the latest epoch still wraps a revoked
  device, so remaining members hold their writes (never sealing under an epoch
  the leaver can read) and send them once the new epoch arrives.
- **Browser upgrade from the shared-tree layout.** The old single
  `riftpipe:trust` log migrates into this browser's own hosted board only if
  this device founded it. That log was written by whichever board was open
  last, so a log this device founded on a *joined* board it first claimed
  lands on the hosted board (which then looks claimed by this device), along
  with the old shared OPFS tree adopted there. A tree that can't be adopted
  safely is kept in `.legacy-root/` (never synced). See
  `docs/security/architecture.md`.

## How other local-first / P2P systems handle this

Researched 2026-07-19 from primary sources. The recurring patterns below are what
the shipped sealing model was built from.

| System | Share capability | Rendezvous vs read key | Read/write split | Revocation | Authenticity |
|---|---|---|---|---|---|
| **riftpipe (today)** | Link = 32-byte secret + host address | **`topic = H(secret)`** — blinded; frames AEAD-sealed with `KDF(secret)` | None yet (link = full access) | Restricted boards: rotate + re-wrap to remaining; open boards: new link | Sealed but **unsigned** (any holder can forge) |
| **[iroh-docs](https://docs.rs/iroh-docs/latest/iroh_docs/sync/enum.Capability.html)** | `DocTicket` = capability + peer addrs | NamespaceId (public key) is the read cap | **Yes**: `Read(NamespaceId)` vs `Write(NamespaceSecret)` | None documented | Namespace + author signatures |
| **[Hypercore](https://docs.pears.com/building-blocks/hypercore)** | 32-byte public key | **`discoveryKey = hash(publicKey)`** — swarm can't derive read key | Yes (single-writer: secret signs, public reads); Autobase adds `addWriter`/`removeWriter` | Writer removal via Autobase | Per-block signatures |
| **[Earthstar](https://earthstar-project.org/docs/how-it-works)** | Share **address** (read) + **secret** (write) | Address = discovery **and** read | **Yes** (address vs secret; `~@name` path ownership) | None — "make a new share and migrate" | Per-document signatures |
| **[Willow/Meadowcap](https://willowprotocol.org/specs/meadowcap/index.html)** | Unforgeable capability token bound to a pubkey | Peers must *prove* read cap during sync | **Yes** (distinct read/write cap types; delegation chains) | Expiry + owner overwrite | Capability-chain-verified signatures |
| **[Yjs / y-webrtc](https://github.com/yjs/y-webrtc)** | Room name (+ optional password) | Password encrypts signaling traffic — server can't read | None (password gates entry, not role) | New room/password | None (auth is the provider's job) |
| **[@localfirst/auth](https://github.com/local-first-web/auth)** | Seitan-style single-use **invitation** | Transport-agnostic; peers mutually authenticate against a signed team chain | Role-based (lockbox-distributed role keys) | **First-class**: remove member/device + key rotation & re-encryption | Hash-chained signed actions |
| **[Keyhive/Beelay](https://www.inkandswitch.com/keyhive/notebook/)** (Ink & Switch) | Doc = public key delegating to keys/groups | Sync servers get only ciphertext ("pull") | **Yes** (pull < read < write < admin) | **Coordination-free**, via BeeKEM (TreeKEM-style group rekey) | Ed25519-signed ops w/ audience + timestamp binding |
| **[Cabal](https://cabal.chat/)** | Single shared key (`cabal://…`) | Key = discovery + full access (like us) | None | Subjective moderation (per-user, after the fact) | — |
| **[SSB private groups](https://github.com/ssbc/private-group-spec)** | Shared symmetric group key | `group_id` is safe to publish; content is envelope-encrypted | None (one group key) | New **epoch** key on exclusion | — |

### The patterns worth stealing

1. **Blind the rendezvous layer** — derive the discovery/topic id from a one-way
   function of a secret the discovery infrastructure never sees (Hypercore's
   `hash(publicKey)`; y-webrtc's password-encrypted signaling; Keyhive's
   ciphertext-only sync servers). *Shipped* as `topic = H(secret)` + AEAD frames.
2. **Split read from write** — the write key is always a *signing* key, which buys
   authenticity for free. *Not yet:* riftpipe currently has one secret = full
   access, like Cabal and passwordless y-webrtc. This is the signing follow-on.
3. **Sign every operation** — a peer that can *receive* data still can't *inject
   accepted* data without a signing key. *Not yet* — this is what would close the
   forge/freeze/replace insider abuses listed above.
4. **Rotation is the revocation story** — no surveyed system revokes a bearer
   capability per-person; they all rekey an encrypted group and let past-replicated
   data stay readable. Our "new secret ⇒ new link" ("New board") is the same shape.

### The bearer-URL caveat, from the W3C

The [W3C TAG's *Good Practices for Capability URLs*](https://www.w3.org/TR/capability-urls/)
names the two structural limits we inherit by putting a secret in a link: it's
hard to tell a leaked capability from a legitimate one ("there is nothing to
distinguish legitimate access from illegitimate access"), and revocation is
all-or-nothing because the URL "tends to be designed to be the same for everyone."
Their mitigations — HTTPS, `rel="noreferrer"`, keeping secrets in the fragment
(which we do), and issuing *multiple* capability URLs so one can be revoked
without cutting everyone off — are the tools this and the read/write follow-on
lean on.

## Reporting

This is a personal research project, not a production service. If you find a
security issue, open an issue describing it. Nothing here has undergone a formal
security audit; the threat model above is the honest current state, not a
guarantee.
