// End-to-end through the package boundary: import the built dist/ and drive
// the REAL riftpipe binary (found via riftpipeBinary()'s cargo fallback).
// Prereq: `cargo build` in the repo root.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { PipeSession, TreeSession, Serve, applyEditOp, riftpipeBinary } from "../dist/index.js";

const within = (ms, p, what) =>
  Promise.race([
    p,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`timed out: ${what}`)), ms).unref()),
  ]);

test("binary resolves", () => {
  const bin = riftpipeBinary();
  assert.ok(bin.length > 0);
  assert.ok(existsSync(bin), `expected a real dev binary at ${bin} — run cargo build first`);
});

test("pipe: edits round-trip host <-> joiner", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "riftpipe-npm-pipe-"));
  const fileA = join(dir, "a.md");
  const fileB = join(dir, "b.md");
  writeFileSync(fileA, "");
  writeFileSync(fileB, "");

  const host = PipeSession.share(fileA);
  t.after(() => host.close());
  const ticket = await within(20_000, host.ticket(), "share ticket");
  assert.match(ticket, /^[A-Z2-7]{40,}$/);

  const peer = PipeSession.join(ticket, fileB);
  t.after(() => peer.close());
  let peerText = "";
  const peerGotEdit = new Promise((res) => {
    peer.onRemote((op) => {
      peerText = applyEditOp(peerText, op);
      if (peerText.includes("hello from host")) res(peerText);
    });
  });
  let hostText = "hello from host\n";
  const hostGotEdit = new Promise((res) => {
    host.onRemote((op) => {
      hostText = applyEditOp(hostText, op);
      if (hostText.includes("reply from peer")) res(hostText);
    });
  });

  host.send({ op: "snapshot", text: "hello from host\n" });
  assert.equal(await within(30_000, peerGotEdit, "host edit reaching peer"), "hello from host\n");

  peer.send({ op: "insert", pos: peerText.length, text: "reply from peer\n" });
  assert.equal(
    await within(30_000, hostGotEdit, "peer edit reaching host"),
    "hello from host\nreply from peer\n",
  );
});

test("serve: static files + SSE change events, dotfiles silent", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "riftpipe-npm-serve-"));
  writeFileSync(join(dir, "index.html"), "<h1>riftpipe</h1>");
  const site = await Serve.start(dir, { port: 18923 });
  t.after(() => site.close());

  const res = await fetch(site.url + "/index.html");
  assert.equal(res.status, 200);
  assert.match(await res.text(), /riftpipe/);

  const ac = new AbortController();
  t.after(() => ac.abort());
  const events = site.events(ac.signal);
  const first = within(20_000, events.next(), "first SSE change event");
  // A dotfile change must never be announced; a real file must.
  writeFileSync(join(dir, ".hidden"), "no event for me");
  setTimeout(() => writeFileSync(join(dir, "data.txt"), "fresh"), 500);
  const { value } = await first;
  assert.deepEqual(value, { type: "change", path: "data.txt" });
});

test("tree: mesh host + joiner converge, SYNCED breadcrumbs fire", async (t) => {
  const dirA = mkdtempSync(join(tmpdir(), "riftpipe-npm-tree-a-"));
  const dirB = mkdtempSync(join(tmpdir(), "riftpipe-npm-tree-b-"));
  writeFileSync(join(dirA, "note.md"), "# from the host\n");

  const host = TreeSession.host(dirA);
  t.after(() => host.close());
  const ticket = await within(30_000, host.ticket(), "mesh ticket");
  assert.match(ticket, /^[0-9a-f]{40,}$/i);

  const joiner = TreeSession.join(ticket, dirB);
  t.after(() => joiner.close());
  const arrived = await within(90_000, joiner.synced("note.md"), "note.md syncing to joiner");
  assert.equal(arrived, "note.md");
  assert.equal(readFileSync(join(dirB, "note.md"), "utf8"), "# from the host\n");

  // Reverse direction: the joiner writes, the host's breadcrumb fires.
  const hostSaw = host.synced("reply.md");
  writeFileSync(join(dirB, "reply.md"), "# from the joiner\n");
  await within(90_000, hostSaw, "reply.md syncing back to host");
  assert.equal(readFileSync(join(dirA, "reply.md"), "utf8"), "# from the joiner\n");
});
