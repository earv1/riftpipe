// The `--pipe` editor-stream protocol (DESIGN.md §15): riftpipe as a CRDT
// sync daemon reading local edits on stdin and writing remote edits on
// stdout, both as line-delimited JSON in char-offset coordinates.

import { spawnRiftpipe } from "./process.js";

/** One edit, exactly as the wire carries it (serde: tag "op", lowercase). */
export type EditOp =
  | { op: "snapshot"; text: string }
  | { op: "insert"; pos: number; text: string }
  | { op: "delete"; pos: number; len: number };

export interface PipeSessionOpts {
  cwd?: string;
}

/** A live `--pipe` session on one text document. */
export class PipeSession {
  /** Host: share `file` and print a ticket peers join with. */
  static share(file: string, opts: PipeSessionOpts = {}): PipeSession {
    return new PipeSession(["share", file, "--pipe"], opts);
  }

  /** Join a shared document via its ticket. */
  static join(ticket: string, file: string, opts: PipeSessionOpts = {}): PipeSession {
    return new PipeSession(["join", ticket, file, "--pipe"], opts);
  }

  #proc: ReturnType<typeof spawnRiftpipe>;
  #remoteCbs: Array<(op: EditOp) => void> = [];

  private constructor(args: string[], opts: PipeSessionOpts) {
    this.#proc = spawnRiftpipe(args, opts);
    this.#proc.stdoutLines((line) => {
      let op: EditOp;
      try {
        op = JSON.parse(line) as EditOp;
      } catch {
        return; // non-protocol stdout noise
      }
      for (const cb of this.#remoteCbs) cb(op);
    });
  }

  /** The share ticket (host side; a joiner's promise never resolves). */
  ticket(): Promise<string> {
    return this.#proc.ticket;
  }

  /** Send a local edit to the daemon. */
  send(op: EditOp): void {
    this.#proc.child.stdin!.write(JSON.stringify(op) + "\n");
  }

  /** Subscribe to remote edits (what the other peers changed). */
  onRemote(cb: (op: EditOp) => void): void {
    this.#remoteCbs.push(cb);
  }

  /** End the session: close stdin (the daemon exits cleanly) and wait. */
  async close(): Promise<void> {
    this.#proc.child.stdin!.end();
    await this.#proc.close();
  }
}

/** Apply one wire op to a string — the reference client-side interpretation. */
export function applyEditOp(text: string, op: EditOp): string {
  switch (op.op) {
    case "snapshot":
      return op.text;
    case "insert": {
      const chars = [...text];
      return chars.slice(0, op.pos).join("") + op.text + chars.slice(op.pos).join("");
    }
    case "delete": {
      const chars = [...text];
      return chars.slice(0, op.pos).join("") + chars.slice(op.pos + op.len).join("");
    }
  }
}
