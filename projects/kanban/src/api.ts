// API layer — backed by the in-browser **Rust kanban server** (wasm + OPFS), not
// a localhost process. The shapes are unchanged; the only difference is the
// transport: each request is handled by `kanbanHandle` from the `riftpipe-web`
// wasm package instead of crossing the network. No local server.
//
// (The exported function signatures are identical to the old fetch-based API, so
// the SolidJS components don't change.)
import init, {
  kanbanHandle,
  connectAndSync,
  connectionId,
  configureIce,
  irohConnect,
} from "../../../web/pkg/riftpipe_web.js";

// Build-time config (Vite env). Set these for a deployed/cross-network build:
//   VITE_SIGNAL_URL  — public signaling server, e.g. wss://signal.example.com
//   VITE_STUN        — STUN url (defaults to a public one so hole-punch works)
//   VITE_TURN/_USER/_PASS — optional relay for hostile NATs
const env: Record<string, string | undefined> = (import.meta as any).env || {};

export interface Card {
  id: string;
  title: string;
  column: string;
  position: number;
  done: boolean;
}

export interface Board {
  title: string;
  columns: string[];
  cards: Card[];
}

export interface Comment {
  id: string;
  author: string;
  ts: string;
  text: string;
}

export interface CardDetail extends Card {
  description: string;
  comments: Comment[];
}

// Initialize the wasm module once, lazily.
let _ready: Promise<unknown> | null = null;
function ready(): Promise<unknown> {
  return (_ready ??= init());
}

// Without a server there's no SSE; a successful local mutation notifies the app
// to refresh (remote peer merges notify via connectPeer's callback).
let _onLocalChange: (() => void) | null = null;
export function onLocalChange(cb: () => void): void {
  _onLocalChange = cb;
}

interface ApiResponse {
  status: number;
  body: string;
}

/** Call the in-browser handler; returns a tiny Response-like object. */
async function api(
  method: string,
  path: string,
  body?: unknown,
): Promise<{ status: number; json: () => any }> {
  await ready();
  const payload = body === undefined ? "" : JSON.stringify(body);
  const res = (await kanbanHandle(method, path, payload)) as ApiResponse;
  if (method !== "GET" && res.status < 400) _onLocalChange?.();
  return { status: res.status, json: () => JSON.parse(res.body) };
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
 * Connect peer-to-peer and sync the board. `onRemote` fires whenever a peer's
 * edit is merged into local OPFS, so the caller can refetch.
 *
 * Default transport is **iroh** — no signaling server, no host you run (traffic
 * rides n0's free relays). The URL hash carries the host's ticket; an empty hash
 * means *this* tab is the host, and we write its ticket into the URL so it's
 * shareable. `?transport=ws` (or `VITE_TRANSPORT=ws`) selects the WebSocket-
 * signaling + WebRTC path instead (used by the native bridge).
 */
export async function connectPeer(onRemote: () => void): Promise<boolean> {
  await ready();
  const transport =
    env.VITE_TRANSPORT ??
    new URLSearchParams(location.search).get("transport") ??
    "iroh";

  if (transport === "iroh") {
    const ticket = location.hash.slice(1);
    try {
      const result = await irohConnect(ticket, onRemote);
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
  await connectAndSync(signalUrl(), id, onRemote);
  return true;
}

export async function getBoard(): Promise<Board> {
  return (await api("GET", "/api/board")).json();
}

export async function getCard(id: string): Promise<Card | null> {
  const r = await api("GET", `/api/cards/${id}`);
  return r.status === 404 ? null : r.json();
}

export async function getCardDetail(id: string): Promise<CardDetail | null> {
  const r = await api("GET", `/api/cards/${id}/detail`);
  return r.status === 404 ? null : r.json();
}

export async function addComment(
  id: string,
  text: string,
  author?: string,
): Promise<Comment> {
  const body = author === undefined ? { text } : { author, text };
  return (await api("POST", `/api/cards/${id}/comments`, body)).json();
}

export async function addCard(column: string, title: string): Promise<Card> {
  return (await api("POST", "/api/cards", { column, title })).json();
}

export async function patchCard(
  id: string,
  patch: Partial<Card> & { description?: string },
): Promise<Card> {
  return (await api("PATCH", `/api/cards/${id}`, patch)).json();
}
