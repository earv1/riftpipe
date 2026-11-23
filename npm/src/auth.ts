// Live admin commands for a running `connect … --restricted` peer. These drive
// it through the filesystem, the same as the CLI: `requests`/`members --json`
// read the peer's `.riftpipe-auth.json` snapshot; `approve`/`grant`/`revoke`
// append to its `.riftpipe-control` dotfile, which the running mesh loop polls.

import { runRiftpipe } from "./process.js";

/** A device's join request, waiting for an admin to approve it (`riftpipe requests --json`). */
export interface PendingRequest {
  device: string;
  name: string;
  safety: string;
}

/** An authorized device on a restricted board (`riftpipe members --json`). */
export interface Member {
  device: string;
  role: "admin" | "member";
  founder: boolean;
  me: boolean;
}

async function jsonList<T>(verb: "requests" | "members", dir: string): Promise<T[]> {
  const { code, stdout, stderr } = await runRiftpipe([verb, dir, "--json"]);
  if (code !== 0) {
    throw new Error(`riftpipe ${verb} failed: ${stderr.trim()}`);
  }
  return JSON.parse(stdout) as T[];
}

/** A restricted board's pending join requests. */
export function requests(dir: string): Promise<PendingRequest[]> {
  return jsonList<PendingRequest>("requests", dir);
}

/** A restricted board's authorized devices. */
export function members(dir: string): Promise<Member[]> {
  return jsonList<Member>("members", dir);
}

/** Approve a pending join request by device id or safety code (e.g. `"a1b2 c3d4"`). */
export async function approve(dir: string, target: string): Promise<void> {
  const { code, stderr } = await runRiftpipe(["approve", dir, target]);
  if (code !== 0) {
    throw new Error(`riftpipe approve failed: ${stderr.trim()}`);
  }
}

/** Authorize a device by id on a running restricted board. */
export async function grant(dir: string, id: string): Promise<void> {
  const { code, stderr } = await runRiftpipe(["grant", dir, id]);
  if (code !== 0) {
    throw new Error(`riftpipe grant failed: ${stderr.trim()}`);
  }
}

/** Remove a device from a running restricted board (rotates the read key). */
export async function revoke(dir: string, id: string): Promise<void> {
  const { code, stderr } = await runRiftpipe(["revoke", dir, id]);
  if (code !== 0) {
    throw new Error(`riftpipe revoke failed: ${stderr.trim()}`);
  }
}

/** This device's id for `dir` (creating its identity if absent). */
export async function deviceId(dir: string): Promise<string> {
  const { code, stdout, stderr } = await runRiftpipe(["device-id", dir]);
  if (code !== 0) {
    throw new Error(`riftpipe device-id failed: ${stderr.trim()}`);
  }
  return stdout.trim();
}
