// The **event log** — a thin TS wrapper over riftpipe's generic event exports.
// The library keeps a per-device, synced append-only log (merges across peers
// into one history) and auto-records what it knows (remote merges, auth
// transitions). Apps add their own semantic events via `logEvent`. This feeds
// the Activity panel and per-card History (`historyStore.ts`), plus debugging.
import { logEvent as wasmLogEvent, readEvents as wasmReadEvents } from "../wasm/pkg/kanban_wasm.js";
import { ready, watchSiblings } from "./fs.ts";
import { affectsEventLog } from "./history.ts";

export type RiftEvent = {
  ts: string;
  device: string;
  kind: string;
  [field: string]: unknown;
};

/** Append a semantic event to this device's synced log (e.g. a card move). */
export async function logEvent(kind: string, data: Record<string, unknown> = {}): Promise<void> {
  await ready();
  await wasmLogEvent(kind, JSON.stringify(data));
}

/** The merged, time-sorted history across all peers. */
export async function readEvents(): Promise<RiftEvent[]> {
  await ready();
  return JSON.parse(await wasmReadEvents());
}

/** Tail the log live: `cb` fires as each event is appended (local or the lib's). */
export function onEvent(cb: (e: RiftEvent) => void): void {
  if (typeof window === "undefined") return;
  window.addEventListener("riftpipe:event", (e) => {
    try {
      cb(JSON.parse((e as CustomEvent).detail as string));
    } catch {
      /* malformed line — skip */
    }
  });
}

// "The log changed" subscription for `historyStore.ts`: a single hook that
// fires on any of the ways the merged log can change in this tab — a local
// append, a remote merge landing via the lib's `riftpipe:file-changed`, an
// in-tab board switch (path ""), or an append/merge relayed from a sibling tab.
// Callers don't care which path fired; they just re-read with `readEvents()`.
const subscribers = new Set<() => void>();
let installed = false;
const eventsBc = typeof BroadcastChannel !== "undefined" ? new BroadcastChannel("riftpipe:events") : null;

function notifyAll(): void {
  for (const s of subscribers) s();
}

function install(): void {
  if (installed || typeof window === "undefined") return;
  installed = true;
  window.addEventListener("riftpipe:event", () => {
    notifyAll();
    try {
      eventsBc?.postMessage(1);
    } catch {
      /* channel closed */
    }
  });
  window.addEventListener("riftpipe:file-changed", (e) => {
    const path = (e as CustomEvent).detail as string;
    if (affectsEventLog(path)) notifyAll();
  });
  eventsBc?.addEventListener("message", () => notifyAll());
  // A sibling's board switch isn't ours, so only its events/* relays count.
  watchSiblings((path) => {
    if (typeof path === "string" && path.startsWith("events/")) notifyAll();
  });
}

/** Subscribe to "the merged event log may have changed"; returns an unsubscribe. */
export function watchEvents(cb: () => void): () => void {
  install();
  subscribers.add(cb);
  return () => {
    subscribers.delete(cb);
  };
}
