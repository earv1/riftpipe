// Tree sync (`riftpipe connect`): sync a whole directory with a mesh or a
// single peer. Remote writes land on disk and each is announced as a
// `SYNCED:{path}` breadcrumb on stdout — the programmatic surface.

import { spawnRiftpipe } from "./process.js";

export interface TreeSessionOpts {
  cwd?: string;
  /** Signaling server URL for connection-id fallback targets. */
  signal?: string;
}

/** A live tree-sync session over `dir`. */
export class TreeSession {
  /**
   * Host a gossip-mesh swarm for `dir` (`connect --accept`). The ticket it
   * prints joins browser AND CLI peers to the same mesh.
   */
  static host(dir: string, opts: TreeSessionOpts = {}): TreeSession {
    return new TreeSession(["connect", "--accept", dir], opts);
  }

  /**
   * Join a peer's tree: `target` is a mesh ticket (from `host` or a browser
   * share link's `#…` hex), an iroh ticket, or a signaling connection id.
   */
  static join(target: string, dir: string, opts: TreeSessionOpts = {}): TreeSession {
    const args = ["connect", target, dir];
    if (opts.signal) args.push("--signal", opts.signal);
    return new TreeSession(args, opts);
  }

  #proc: ReturnType<typeof spawnRiftpipe>;
  #syncedCbs: Array<(path: string) => void> = [];

  private constructor(args: string[], opts: TreeSessionOpts) {
    this.#proc = spawnRiftpipe(args, opts);
    this.#proc.stdoutLines((line) => {
      if (line.startsWith("SYNCED:")) {
        const path = line.slice("SYNCED:".length);
        for (const cb of this.#syncedCbs) cb(path);
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

  /** Wait until `path` (relative to the dir) has been synced from a peer. */
  synced(path: string): Promise<string> {
    return new Promise((res) => {
      this.onSynced((p) => {
        if (p === path) res(p);
      });
    });
  }

  /** Stop the session. */
  close(): Promise<void> {
    return this.#proc.close();
  }
}
