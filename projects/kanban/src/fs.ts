// The **file API** — a thin TS wrapper over riftpipe's generic wasm file exports
// (`readFile`/`writeFile`/`listDir`). This is the whole surface the app uses to
// talk to the synced tree: read a path, write a path (which persists to OPFS
// *and* pushes to peers), list a directory, and watch for changes. Nothing here
// knows what a "board" or "card" is — the kanban model is built on top in
// `board.ts`, exactly as any other file-based app would build its own.
import init, { readFile, writeFile as wasmWriteFile, listDir } from "../wasm/pkg/kanban_wasm.js";

// Initialize the wasm module once, lazily — every file op awaits it, so callers
// (board.ts and up) never touch init.
let _ready: Promise<unknown> | null = null;
export function ready(): Promise<unknown> {
  return (_ready ??= init());
}

// Cross-tab bridge: tabs in one browser profile share one OPFS tree but not one
// JS heap, so a write in tab A must nudge tab B to re-read. Every change (local,
// remote-merge, or sibling) funnels through `watch`.
const bc: BroadcastChannel | null =
  typeof BroadcastChannel !== "undefined" ? new BroadcastChannel("riftpipe:fs") : null;

/** Read a file as text, or `null` if it doesn't exist. */
export async function read(path: string): Promise<string | null> {
  await ready();
  return (await readFile(path)) ?? null;
}

/**
 * Write `content` to `path` on the synced tree — persists to OPFS and pushes the
 * change to peers (the lib picks the text-CRDT vs LWW channel by path). Then
 * fire a local change event so this tab (and siblings) re-read.
 */
export async function write(path: string, content: string): Promise<void> {
  await ready();
  await wasmWriteFile(path, content);
  fireChanged(path);
}

/** Entry names directly under directory `path` (`""` = tree root); `[]` if absent. */
export async function list(path: string): Promise<string[]> {
  await ready();
  return await listDir(path);
}

/** Announce a local write to this tab (the lib fires this itself on a remote merge). */
function fireChanged(path: string): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent("riftpipe:file-changed", { detail: path }));
  }
}

/**
 * Watch the tree: `cb(path)` fires on every change — a local `write`, a remote
 * merge (the lib's `riftpipe:file-changed` event), or a sibling tab's write
 * (BroadcastChannel). Local/remote changes are relayed to siblings here; a
 * sibling-originated change is not re-broadcast, so there are no loops.
 */
export function watch(cb: (path: string) => void): void {
  if (typeof window !== "undefined") {
    window.addEventListener("riftpipe:file-changed", (e) => {
      const path = (e as CustomEvent).detail as string;
      cb(path);
      try { bc?.postMessage(path); } catch { /* channel closed */ }
    });
  }
  bc?.addEventListener("message", (e) => cb(e.data as string));
}
