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
} from "../../../web/pkg/riftpipe_web.js";

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

/** Signaling server URL — `?signal=ws://…` overrides the default (port 9000). */
function signalUrl(): string {
  const override = new URL(location.href).searchParams.get("signal");
  return override ?? `ws://${location.hostname || "localhost"}:9000`;
}

/**
 * If the page URL carries a connection id (`#<id>`), connect peer-to-peer over
 * WebRTC and sync the board. `onRemote` fires whenever a peer's edit is merged
 * into local OPFS, so the caller can refetch. Returns false if there's no id
 * (single-player). Sharing the link == sharing the board.
 */
export async function connectPeer(onRemote: () => void): Promise<boolean> {
  await ready();
  const id = connectionId();
  if (!id) return false;
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
