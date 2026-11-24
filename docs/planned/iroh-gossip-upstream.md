# Draft upstream issue: iroh-gossip — a peer that restarts under the same key

Target: `iroh-gossip` 0.101.0 (`src/net.rs`, `src/proto/hyparview.rs`,
`src/proto/plumtree.rs`). Workarounds live in `core::reconnect::SwapRejoin` and
the native mesh driver (`src/sync/tree.rs` `Outbound`, `src/sync/mesh.rs`).

## Setup

Peers A and B are active neighbors on a topic. A restarts under the same
`SecretKey` before B's QUIC connection to the old A times out, and re-dials B.
`net::PeerState::accept_conn` swaps the new connection in as B's active send
connection. B's topic state keeps A as an active neighbor, so B emits neither
`NeighborDown` nor `NeighborUp` for A. The restarted A starts with empty
protocol state.

## Problem 1: the restarted peer never becomes a neighbor (hyparview)

The peer that sent the last `Neighbor` of the original handshake (usually the
joiner, B) keeps a stale `pending_neighbor_requests` entry for A. When A sends
`Join`, B's `send_neighbor` reply is suppressed by that entry. A never gets
`NeighborUp`, and both sides report connected/waiting with nothing flowing.

Workaround: B notices an accepted connection from a peer that is already an
active neighbor and calls `join_peers([A])` about 0.5 s later. A then adds B, and
B's `Neighbor` reply clears the stale entry.

## Problem 2: `Scope::Neighbors` never reaches a lazy peer, and fresh neighbors turn lazy right away

`plumtree::broadcast` with `Scope::Neighbors` calls only `eager_push`: lazy
peers get nothing, not even an `IHave`. In a topology with a cycle (for example
a triangle A–B–C), plumtree prunes edges to lazy whenever a duplicate `Gossip`
arrives (`on_gossip` → `add_lazy` + `Prune`). That happens within milliseconds of
a `NeighborUp`: the per-neighbor catch-up's swarm-scoped frames reach the new
neighbor directly and through the third peer. The silent swap above keeps a stale
lazy entry for the restarted peer as well (no `NeighborUp`, so no `add_eager`).

Result: an app that answers a neighbor's request with a neighbors-scoped reply
(for example a catch-up "Hello" → gap edits) silently fails to deliver it
whenever the requester is lazy on the replier's side. Swarm-scoped messages
still arrive, via the third peer or via `IHave` → `Graft`. Traces from our e2e:
the member received the restarted host's `Hello` and produced its reply, and the
host received nothing directly from the member for the rest of the run.

Workaround: our native driver sends request replies swarm-wide. That costs one
swarm flood per reply, where one message to one peer would do.

## Suggested fixes

- When `accept_conn` replaces the active connection of a peer that is an active
  neighbor, treat it as a reconnect: reset that peer's protocol state (drop
  `pending_neighbor_requests` for it, and `add_eager` in plumtree). Or emit
  `NeighborDown` + `NeighborUp` so applications can redo their per-neighbor
  catch-up.
- Make `Scope::Neighbors` deliver to all active-view peers, eager or lazy. The
  scope is documented as "direct neighbors", and lazy peers are direct neighbors
  too. Or add a direct send to one neighbor (`send_to(peer)`), which is what a
  request/reply needs.

## Reproduction

`projects/kanban/e2e/run-cli-host-restart-restricted.sh` in this repo (three
native peers, the host restarts) failed about 1 run in 6 before the workaround.
