// Shared child-process plumbing: spawn riftpipe, split its streams into
// lines, and surface the ticket a hosting command prints.
//
// Conventions of the CLI this leans on:
//   * tickets go to STDERR as a bare line (`share` prints a base32-ish token,
//     `connect --accept` a hex mesh ticket), plus a `<path>.ticket` sidecar;
//   * protocol output (`--pipe` JSON lines, `SYNCED:` breadcrumbs) is STDOUT;
//   * everything else on stderr is human-facing status.

import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import { riftpipeBinary } from "./binary.js";

export interface SpawnHandle {
  /** The underlying child process (for advanced use). */
  child: ChildProcess;
  /** Resolves when the process exits; rejects if it exits before `ready`. */
  exited: Promise<number | null>;
  /** Stop the process (SIGTERM) and wait for it to go. */
  close(): Promise<void>;
}

/** A stderr line that looks like a ticket: one long unbroken token. */
const TICKET = /^[A-Z2-7]{40,}$|^[0-9a-fA-F]{40,}$/;

export function spawnRiftpipe(
  args: string[],
  opts: { cwd?: string } = {},
): SpawnHandle & {
  stdoutLines(cb: (line: string) => void): void;
  stderrLines(cb: (line: string) => void): void;
  /** The first ticket-shaped stderr line (hosting commands print exactly one). */
  ticket: Promise<string>;
} {
  const child = spawn(riftpipeBinary(), args, {
    cwd: opts.cwd,
    stdio: ["pipe", "pipe", "pipe"],
  });

  const stdoutCbs: Array<(line: string) => void> = [];
  const stderrCbs: Array<(line: string) => void> = [];
  createInterface({ input: child.stdout! }).on("line", (l) => {
    for (const cb of stdoutCbs) cb(l);
  });

  let resolveTicket: (t: string) => void;
  let rejectTicket: (e: Error) => void;
  const ticket = new Promise<string>((res, rej) => {
    resolveTicket = res;
    rejectTicket = rej;
  });
  ticket.catch(() => {}); // not every caller awaits the ticket

  createInterface({ input: child.stderr! }).on("line", (l) => {
    if (TICKET.test(l.trim())) resolveTicket(l.trim());
    for (const cb of stderrCbs) cb(l);
  });

  const exited = new Promise<number | null>((res, rej) => {
    child.on("error", (e) => {
      rejectTicket(e);
      rej(e);
    });
    child.on("exit", (code) => {
      rejectTicket(new Error(`riftpipe exited (code ${code}) before printing a ticket`));
      res(code);
    });
  });
  exited.catch(() => {}); // surfaced via close()/await sites, not unhandled

  return {
    child,
    exited,
    ticket,
    stdoutLines: (cb) => stdoutCbs.push(cb),
    stderrLines: (cb) => stderrCbs.push(cb),
    async close() {
      if (child.exitCode === null) {
        child.kill("SIGTERM");
        // Escalate if it lingers — a close() must never hang the caller.
        const hammer = setTimeout(() => child.kill("SIGKILL"), 3000);
        hammer.unref?.();
        await exited.catch(() => {});
        clearTimeout(hammer);
      } else {
        await exited.catch(() => {});
      }
    },
  };
}

/** One-shot riftpipe invocation (e.g. `requests`/`approve`/`members`, `device-id`):
 * spawn, collect both streams, resolve once the process exits. */
export function runRiftpipe(
  args: string[],
  opts: { cwd?: string } = {},
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(riftpipeBinary(), args, { cwd: opts.cwd, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout!.on("data", (d) => {
      stdout += d;
    });
    child.stderr!.on("data", (d) => {
      stderr += d;
    });
    child.on("error", reject);
    child.on("exit", (code) => resolve({ code, stdout, stderr }));
  });
}
