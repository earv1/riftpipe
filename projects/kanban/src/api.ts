// The app-facing API barrel. There is **no server and no domain logic here**:
//   - the board model (getBoard/addCard/patchCard/…) lives in `board.ts`, built
//     on the generic file API (`fs.ts`) — read/write/list/watch over the synced
//     OPFS tree, the same file layout the native CLI materializes;
//   - this module adds transport/lifecycle (connect, start-fresh), the change
//     subscription (`watch`), and the auth surface.
// riftpipe's wasm bundle is a generic file-sync library; nothing kanban-specific
// is baked into it.
import {
  connectAndSync,
  connectionId,
  configureIce,
  irohConnect,
  connectedPeers,
  routingMap,
  deviceId,
  lockBoard,
  grantDevice,
  revokeDevice,
  boardMembers,
  requestAccess,
  pendingRequests,
  authState,
  passkeySupported,
  claimWithPasskey,
  linkWithPasskey,
  meshStatus as wasmMeshStatus,
  meshStats as wasmMeshStats,
} from "../wasm/pkg/kanban_wasm.js";
import { ready, watch, read, write, list } from "./fs.ts";
import { readEvents } from "./events.ts";

// Domain model + types (Board, Card, Comment, CardDetail, getBoard, addCard,
// patchCard, getCardDetail, addComment, …) — over the file API.
export * from "./board.ts";
// The file-tree change subscription: fires on local writes, remote merges, and
// sibling-tab edits. The app calls `watch(() => refresh())` once.
export { watch, read as readFile, write as writeFile, list as listDir } from "./fs.ts";
// The synced, cross-peer event log (history + debugging).
export { logEvent, readEvents, onEvent, type RiftEvent } from "./events.ts";
// Live peer-to-peer connection status, for a status pill or similar UI.
export type MeshStatus = {
  state: "idle" | "waiting" | "connected" | "reconnecting" | "offline";
  peers: number;
};
export const meshStatus = (): MeshStatus => JSON.parse(wasmMeshStatus());
/** The lib fires `riftpipe:mesh-status` whenever the connection state changes. */
export function onMeshStatus(cb: (s: MeshStatus) => void): void {
  window.addEventListener("riftpipe:mesh-status", (e) => {
    try {
      cb(JSON.parse((e as CustomEvent<string>).detail));
    } catch {
      /* ignore */
    }
  });
}

// The auth surface, backed entirely by the lib (wasm). Also on window.riftpipe
// for console use:
//   riftpipe.deviceId()          → this browser's device id
//   riftpipe.lockBoard()         → become this board's author (locks it)
//   riftpipe.requestAccess()     → ask the author for write access
//   riftpipe.pendingRequests()   → author-only: who's asking [{device,name,safety}]
//   riftpipe.grantDevice(id)     → author-only: approve a device to write
//   riftpipe.revokeDevice(id)    → admin-only: remove a device (rotates the read key)
//   riftpipe.boardMembers()      → authorized devices [{device,role,founder,me}]
//   riftpipe.authState()         → { locked, device, authorized, isAuthor, author, requested, canRead, epoch, isAdmin, passkeys, passkeyHere }
//   riftpipe.passkeySupported()  → can this page use passkey device-linking (secure domain origin)?
//   riftpipe.claimWithPasskey()  → claim the board (if unclaimed) and register a passkey — no approve click for other devices
//   riftpipe.linkWithPasskey()   → join a claimed board by asserting its registered passkey — no approve click
export type AuthState = {
  locked: boolean;
  device: string;
  authorized: boolean;
  isAuthor: boolean;
  author: string;
  requested: boolean;
  canRead: boolean;
  epoch: number;
  isAdmin: boolean;
  passkeys: number;
  passkeyHere: boolean;
};
export type PendingRequest = { device: string; name: string; safety: string };
export type Member = { device: string; role: "admin" | "member"; founder: boolean; me: boolean };
export const auth = {
  deviceId: (): string => deviceId(),
  lockBoard: (): boolean => lockBoard(),
  requestAccess: (): boolean => requestAccess(),
  pending: (): PendingRequest[] => JSON.parse(pendingRequests()),
  grantDevice: (id: string): boolean => grantDevice(id),
  revokeDevice: (id: string): boolean => revokeDevice(id),
  members: (): Member[] => JSON.parse(boardMembers()),
  state: (): AuthState => JSON.parse(authState()),
  passkeySupported: (): boolean => passkeySupported(),
  claimWithPasskey: (): Promise<boolean> => claimWithPasskey(),
  linkWithPasskey: (): Promise<boolean> => linkWithPasskey(),
};
(globalThis as any).riftpipe = {
  connectedPeers: () => JSON.parse(connectedPeers()),
  routingMap: () => JSON.parse(routingMap()),
  deviceId: () => deviceId(),
  lockBoard: () => lockBoard(),
  requestAccess: () => requestAccess(),
  pendingRequests: () => JSON.parse(pendingRequests()),
  grantDevice: (id: string) => grantDevice(id),
  revokeDevice: (id: string) => revokeDevice(id),
  boardMembers: () => JSON.parse(boardMembers()),
  authState: () => JSON.parse(authState()),
  readEvents: () => readEvents(),
  passkeySupported: () => passkeySupported(),
  claimWithPasskey: () => claimWithPasskey(),
  linkWithPasskey: () => linkWithPasskey(),
  meshStatus: () => meshStatus(),
  // Anti-entropy + outbound-queue counters (diagnostics; see the lib's `meshStats`).
  meshStats: () => JSON.parse(wasmMeshStats()),
  readFile: (p: string) => read(p),
  writeFile: (p: string, c: string) => write(p, c),
  listDir: (p: string) => list(p),
};

// Build-time config (Vite env). Set these for a deployed/cross-network build:
//   VITE_SIGNAL_URL  — public signaling server, e.g. wss://signal.example.com
//   VITE_STUN        — STUN url (defaults to a public one so hole-punch works)
//   VITE_TURN/_USER/_PASS — optional relay for hostile NATs
const env: Record<string, string | undefined> = (import.meta as any).env || {};

/**
 * Can this browser persist the board? We need OPFS with main-thread
 * `createWritable` (Chrome/Edge/Firefox; Safari/iOS 18.4+). Older WebKit has
 * OPFS reads but not writes — the board would render yet lose every edit.
 */
export function storageSupported(): boolean {
  return (
    typeof navigator !== "undefined" &&
    !!navigator.storage?.getDirectory &&
    typeof globalThis.FileSystemFileHandle !== "undefined" &&
    "createWritable" in FileSystemFileHandle.prototype
  );
}

/**
 * Signaling server URL. Priority: `?signal=…` (per-link override) → build-time
 * `VITE_SIGNAL_URL` (the deployed default) → localhost:9000 (dev). A page served
 * over HTTPS (e.g. GitHub Pages) must use `wss://` here — browsers block `ws://`.
 */
function signalUrl(): string {
  const override = new URL(location.href).searchParams.get("signal");
  return (
    override ??
    env.VITE_SIGNAL_URL ??
    `ws://${location.hostname || "localhost"}:9000`
  );
}

/**
 * Connect peer-to-peer and sync the board. A peer's merged edit lands in the
 * synced OPFS tree and fires `riftpipe:file-changed`, which `watch` delivers —
 * so connecting needs no callback; the caller just `watch`es.
 *
 * Default transport is **iroh** — no signaling server, no host you run (traffic
 * rides n0's free relays). The URL hash carries the host's ticket; an empty hash
 * means *this* tab is the host, and we write its ticket into the URL so it's
 * shareable. `?transport=ws` (or `VITE_TRANSPORT=ws`) selects the WebSocket-
 * signaling + WebRTC path instead (used by the native bridge).
 */
export async function connectPeer(): Promise<boolean> {
  await ready();
  // The lib nudges the app on every merge via `riftpipe:file-changed`; connect
  // just needs a callback argument, so pass a no-op.
  const noop = () => {};
  const transport =
    env.VITE_TRANSPORT ??
    new URLSearchParams(location.search).get("transport") ??
    "iroh";

  if (transport === "iroh") {
    const ticket = location.hash.slice(1);
    try {
      const result = await irohConnect(ticket, noop);
      // Host (empty ticket in): publish the returned ticket to the URL to share.
      if (typeof result === "string" && result) location.hash = result;
      return true;
    } catch (e) {
      console.warn("iroh connect failed; working solo:", e);
      return false;
    }
  }

  // WebSocket signaling + WebRTC (native bridge / legacy).
  const id = connectionId();
  if (!id) return false;
  configureIce(
    env.VITE_STUN ?? "stun:stun.l.google.com:19302",
    env.VITE_TURN ?? "",
    env.VITE_TURN_USER ?? "",
    env.VITE_TURN_PASS ?? "",
    false,
  );
  await connectAndSync(signalUrl(), id, noop);
  return true;
}

/**
 * Start a fresh board: drop the persisted iroh identity (a new key ⇒ a new
 * EndpointId ⇒ a new share ticket) and the board secret (⇒ a new topic), clear
 * the URL hash, and reload — the tab comes back as the host of an empty board
 * and writes its fresh ticket into the URL. Each board's files live in their
 * own OPFS directory (keyed by topic), so the new board starts empty and the
 * old board's files stay with the old board (reopen its link to get them back)
 * — they are never wiped, and never carried into the new board.
 */
export async function newBoard(): Promise<void> {
  // Nothing in OPFS is wiped: boards live under `.boards/<topic>/`, and a
  // pre-scoping tree was already adopted or quarantined (`.legacy-root/`, never
  // synced) on the first connect — decided once, so a new board can't adopt it.
  localStorage.removeItem("riftpipe:iroh_sk");
  // Drop the board secret too: a new board must mint a fresh secret, so it gets
  // a new topic (H(secret)) and a brand-new, unrelated share link.
  localStorage.removeItem("riftpipe:board_secret");
  // Drop the legacy (board-unscoped) trust log: it belonged to the old board.
  // The per-board claim + epoch announces (`riftpipe:trust:v2:<topic>`,
  // `riftpipe:epochs:<topic>`) are kept ON PURPOSE: the new board has a new
  // topic so they can't collide, and they keep the old board claimed (sealed,
  // owner-only) if this device reopens it with nobody else online. Cost: one
  // small localStorage entry per board ever claimed/joined.
  localStorage.removeItem("riftpipe:trust");
  location.hash = "";
  location.reload();
}
