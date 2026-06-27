// API layer — backed by the in-browser **Rust kanban server** (wasm + OPFS), not
// a localhost process. The shapes are unchanged; the only difference is the
// transport: each request is handled by `kanbanHandle` from the `riftpipe-web`
// wasm package instead of crossing the network. No local server.
//
// (The exported function signatures are identical to the old fetch-based API, so
// the SolidJS components don't change.)
import init, { kanbanHandle } from "../../../web/pkg/riftpipe_web.js";

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
  return { status: res.status, json: () => JSON.parse(res.body) };
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
