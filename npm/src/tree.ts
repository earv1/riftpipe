// Tree sync (`riftpipe connect`): sync a whole directory with a mesh or a
// single peer. Remote writes and deletes land on disk and each is announced
// as a `SYNCED:{path}` / `DELETED:{path}` breadcrumb on stdout — the
// programmatic surface.

import { spawnRiftpipe } from "./process.js";
import type { PendingRequest } from "./auth.js";

export interface TreeSessionOpts {
  cwd?: string;
  /** Signaling server URL for connection-id fallback targets. */
  signal?: string;
  /** Device-linking auth: sign writes and enforce authorization. On `host()`,
   * also self-signs the genesis. On `join()`, a keyless device broadcasts a
   * signed join request instead of reading/writing until an admin approves it. */
  restricted?: boolean;
  /** `host()` only: device ids to authorize (as Members) at startup. */
  grant?: string[];
  /** `join()` only: display name on the broadcast join request. */
  name?: string;
}

/** A peer speaking a different riftpipe protocol version (0 = pre-versioning). */
export interface ProtocolMismatch {
  peer: string;
  theirs: number;
  ours: number;
}

/** Parse a restricted peer's inbox line for a new or changed join request:
 *   [auth] access-request device=<64 hex> safety=<xxxx xxxx> name="..."
 * `null` for any other line, or one whose name isn't valid JSON (a garbled
 * line must not throw inside the stderr handler and take the host down). */
export function parseAccessRequestLine(line: string): PendingRequest | null {
  const r = line.match(/access-request device=([0-9a-f]{64}) safety=([0-9a-f]{4} [0-9a-f]{4}) name=(.*)$/);
  if (!r) return null;
  let name: unknown;
  try {
    name = JSON.parse(r[3]);
  } catch {
    return null;
  }
  return typeof name === "string" ? { device: r[1], safety: r[2], name } : null;
}

/** A live tree-sync session over `dir`. */
export class TreeSession {
  /**
   * Host a gossip-mesh swarm for `dir` (`connect --accept`). The ticket it
   * prints joins browser AND CLI peers to the same mesh.
   */
  static host(dir: string, opts: TreeSessionOpts = {}): TreeSession {
    const args = ["connect", "--accept", dir];
    if (opts.restricted) args.push("--restricted");
    for (const id of opts.grant ?? []) args.push("--grant", id);
    return new TreeSession(args, opts);
  }

  /**
   * Join a peer's tree: `target` is a mesh ticket (from `host` or a browser
   * share link's `#…` hex), an iroh ticket, or a signaling connection id.
   */
  static join(target: string, dir: string, opts: TreeSessionOpts = {}): TreeSession {
    const args = ["connect", target, dir];
    if (opts.signal) args.push("--signal", opts.signal);
    if (opts.restricted) args.push("--restricted");
    if (opts.name) args.push("--name", opts.name);
    return new TreeSession(args, opts);
  }

  #proc: ReturnType<typeof spawnRiftpipe>;
  #syncedCbs: Array<(path: string) => void> = [];
  #deletedCbs: Array<(path: string) => void> = [];
  #mismatchCbs: Array<(m: ProtocolMismatch) => void> = [];
  #accessRequestCbs: Array<(r: PendingRequest) => void> = [];
  #readKeyCbs: Array<(epoch: number) => void> = [];

  private constructor(args: string[], opts: TreeSessionOpts) {
    this.#proc = spawnRiftpipe(args, opts);
    this.#proc.stdoutLines((line) => {
      if (line.startsWith("SYNCED:")) {
        const path = line.slice("SYNCED:".length);
        for (const cb of this.#syncedCbs) cb(path);
      }
      if (line.startsWith("DELETED:")) {
        const path = line.slice("DELETED:".length);
        for (const cb of this.#deletedCbs) cb(path);
      }
    });
    // The CLI prints one stable line per mismatched peer:
    //   [mesh] protocol-mismatch peer=<hex> theirs=<n> ours=<n> — ...
    this.#proc.stderrLines((line) => {
      const m = line.match(/protocol-mismatch peer=([0-9a-f]+) theirs=(\d+) ours=(\d+)/);
      if (m) {
        const ev = { peer: m[1], theirs: Number(m[2]), ours: Number(m[3]) };
        for (const cb of this.#mismatchCbs) cb(ev);
      }
      const req = parseAccessRequestLine(line);
      if (req) {
        for (const cb of this.#accessRequestCbs) cb(req);
      }
      const k = line.match(/\[epoch\] read key acquired \(epoch (\d+)\)/);
      if (k) {
        const epoch = Number(k[1]);
        for (const cb of this.#readKeyCbs) cb(epoch);
      }
    });
  }

  /** The mesh ticket (host side; a joiner's promise never resolves). */
  ticket(): Promise<string> {
    return this.#proc.ticket;
  }

  /** Subscribe to remote-write breadcrumbs: fires with each synced rel path. */
  onSynced(cb: (path: string) => void): void {
    this.#syncedCbs.push(cb);
  }

  /** Subscribe to remote-delete breadcrumbs: fires with each rel path a peer's
   *  delete removed from disk. */
  onDeleted(cb: (path: string) => void): void {
    this.#deletedCbs.push(cb);
  }

  /** Fires once per peer whose riftpipe protocol version differs from ours. */
  onProtocolMismatch(cb: (m: ProtocolMismatch) => void): void {
    this.#mismatchCbs.push(cb);
  }

  /** Restricted mode: fires on a new or changed join request this peer sees
   * (any restricted peer, not just an Admin — an Admin additionally gets an
   * approve hint, logged but not parsed here). */
  onAccessRequest(cb: (r: PendingRequest) => void): void {
    this.#accessRequestCbs.push(cb);
  }

  /** Restricted mode: fires once this (joiner) device is wrapped into an
   * epoch and can read sealed content. */
  onReadKey(cb: (epoch: number) => void): void {
    this.#readKeyCbs.push(cb);
  }

  /** Wait until `path` (relative to the dir) has been synced from a peer. */
  synced(path: string): Promise<string> {
    return new Promise((res) => {
      this.onSynced((p) => {
        if (p === path) res(p);
      });
    });
  }

  /** Wait until `path` (relative to the dir) has been deleted by a peer. */
  deleted(path: string): Promise<string> {
    return new Promise((res) => {
      this.onDeleted((p) => {
        if (p === path) res(p);
      });
    });
  }

  /** Stop the session. */
  close(): Promise<void> {
    return this.#proc.close();
  }
}
