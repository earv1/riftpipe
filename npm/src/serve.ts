// `riftpipe serve`: static-host a directory with SSE change events at
// `/events`. Pair it with a share/join/connect session on the same dir for a
// live-updating static site.

import { spawnRiftpipe } from "./process.js";

/** One `/events` frame: a non-dot file under the served dir changed. */
export interface ChangeEvent {
  type: "change";
  path: string;
}

export interface ServeOpts {
  port?: number;
  cwd?: string;
}

/** A running `riftpipe serve` with a typed SSE client. */
export class Serve {
  /** Start serving `dir`; resolves once the HTTP server answers. */
  static async start(dir: string, opts: ServeOpts = {}): Promise<Serve> {
    const port = opts.port ?? 8080;
    const proc = spawnRiftpipe(["serve", dir, "--port", String(port)], opts);
    const serve = new Serve(proc, `http://localhost:${port}`);
    // Ready when the socket answers (any response — a 404 counts).
    const deadline = Date.now() + 15_000;
    for (;;) {
      try {
        await fetch(serve.url + "/", { signal: AbortSignal.timeout(1000) });
        return serve;
      } catch {
        if (Date.now() > deadline) {
          await serve.close();
          throw new Error(`riftpipe serve did not come up on ${serve.url}`);
        }
        await new Promise((r) => setTimeout(r, 150));
      }
    }
  }

  #proc: ReturnType<typeof spawnRiftpipe>;
  /** Base URL of the static host. */
  readonly url: string;

  private constructor(proc: ReturnType<typeof spawnRiftpipe>, url: string) {
    this.#proc = proc;
    this.url = url;
  }

  /**
   * Subscribe to `/events`: an async iterator of change events. SSE comment
   * lines (keepalives and flush padding) are filtered out. Iteration ends
   * when the server goes away or `signal` aborts.
   */
  async *events(signal?: AbortSignal): AsyncGenerator<ChangeEvent> {
    const res = await fetch(this.url + "/events", { signal });
    if (!res.ok || !res.body) throw new Error(`/events responded ${res.status}`);
    const decoder = new TextDecoder();
    let buf = "";
    for await (const chunk of res.body) {
      buf += decoder.decode(chunk as Uint8Array, { stream: true });
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trimEnd();
        buf = buf.slice(nl + 1);
        if (!line.startsWith("data: ")) continue; // comments/blank separators
        try {
          yield JSON.parse(line.slice("data: ".length)) as ChangeEvent;
        } catch {
          // malformed frame — skip
        }
      }
    }
  }

  /** Stop the server. */
  close(): Promise<void> {
    return this.#proc.close();
  }
}
