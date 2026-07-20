// The **event log** — a thin TS wrapper over riftpipe's generic event exports.
// The library keeps a per-device, synced append-only log (merges across peers
// into one history) and auto-records what it knows (remote merges, auth
// transitions). Apps add their own semantic events via `logEvent`. For now this
// feeds debugging and a future history/timeline view; nothing renders it yet.
import { logEvent as wasmLogEvent, readEvents as wasmReadEvents } from "../wasm/pkg/kanban_wasm.js";
import { ready } from "./fs.ts";

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
