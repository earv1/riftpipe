import assert from "node:assert/strict";
import {
  boardActivity,
  cardHistory,
  dedupe,
  describe as describeEvent,
  deviceHandle,
  deviceLabel,
  type HistoryEvent,
  knownTitles,
  relativeTime,
  type Viewer,
} from "./history.ts";

const NOW = Date.parse("2026-07-20T12:00:00Z");

Deno.test("relativeTime", () => {
  assert.equal(relativeTime("2026-07-20T11:59:30Z", NOW), "just now");
  assert.equal(relativeTime("2026-07-20T11:55:00Z", NOW), "5 min ago");
  assert.equal(relativeTime("2026-07-20T09:00:00Z", NOW), "3 h ago");
  assert.equal(relativeTime("2026-07-19T12:00:00Z", NOW), "yesterday");
  assert.equal(relativeTime("2026-07-17T12:00:00Z", NOW), "3 days ago");
  assert.equal(relativeTime("2026-07-01T12:00:00Z", NOW), "2026-07-01");
  assert.equal(relativeTime("bogus", NOW), "bogus");
  assert.equal(relativeTime(new Date(NOW + 10_000).toISOString(), NOW), "just now");
});

Deno.test("deviceHandle and deviceLabel", () => {
  const device = "00ff123456789abc";
  assert.equal(deviceHandle(device), "amber-wren");

  const v: Viewer = { me: "aaaa", owner: "cccc" };
  assert.equal(deviceLabel("aaaa", v), "You");
  assert.equal(deviceLabel("bbbb", v), `${deviceHandle("bbbb")} ${"bbbb".slice(0, 8)}`);
  const meOwner: Viewer = { me: "aaaa", owner: "aaaa" };
  assert.equal(deviceLabel("aaaa", meOwner), "You (owner)");
  const otherOwner: Viewer = { me: "aaaa", owner: "cccc" };
  assert.equal(deviceLabel("cccc", otherOwner), `${deviceHandle("cccc")} cccc (owner)`);
  assert.equal(deviceLabel(device, v), `amber-wren ${device.slice(0, 8)}`);
});

const titleOf = (id: string): string => (id === "tk_1" ? "alpha" : id);

Deno.test("describe — board-wide (inCard=false)", () => {
  const v: Viewer = { me: "me", owner: "me" };
  const e = (kind: string, rest: Record<string, unknown> = {}): HistoryEvent =>
    ({ ts: "t", device: "me", kind, ...rest } as HistoryEvent);

  assert.equal(
    describeEvent(e("card.create", { id: "tk_1", title: "alpha", column: "Todo" }), titleOf, v, false),
    "created “alpha” in Todo",
  );
  assert.equal(
    describeEvent(e("card.move", { id: "tk_1", from: "Todo", to: "Done" }), titleOf, v, false),
    "moved “alpha” from Todo to Done",
  );
  assert.equal(
    describeEvent(e("card.check", { id: "tk_1", done: true }), titleOf, v, false),
    "marked “alpha” done",
  );
  assert.equal(
    describeEvent(e("card.check", { id: "tk_1", done: false }), titleOf, v, false),
    "reopened “alpha”",
  );
  assert.equal(
    describeEvent(e("card.edit", { id: "tk_1", field: "title", value: "beta" }), titleOf, v, false),
    "renamed a card to “beta”",
  );
  assert.equal(
    describeEvent(e("card.edit", { id: "tk_1", field: "description" }), titleOf, v, false),
    "edited the description of “alpha”",
  );
  assert.equal(
    describeEvent(e("comment.add", { id: "tk_1" }), titleOf, v, false),
    "commented on “alpha”",
  );
  assert.equal(describeEvent(e("auth.claim"), titleOf, v, false), "claimed the board");
  assert.equal(describeEvent(e("auth.request"), titleOf, v, false), "asked to join");
  assert.equal(
    describeEvent(e("auth.grant", { target: "me" }), titleOf, v, false),
    "approved you",
  );
  assert.equal(describeEvent(e("auth.grant"), titleOf, v, false), "approved a device");
  assert.equal(
    describeEvent(e("auth.revoke", { target: "me" }), titleOf, v, false),
    "removed you",
  );
  assert.equal(describeEvent(e("auth.revoke"), titleOf, v, false), "removed a device");
  assert.equal(
    describeEvent(e("mesh.merge", { path: "board.md" }), titleOf, v, false),
    "received board.md",
  );
  assert.equal(describeEvent(e("something.else"), titleOf, v, false), "something.else");
});

Deno.test("describe — in card (inCard=true)", () => {
  const v: Viewer = { me: "me", owner: "me" };
  const e = (kind: string, rest: Record<string, unknown> = {}): HistoryEvent =>
    ({ ts: "t", device: "me", kind, ...rest } as HistoryEvent);

  assert.equal(
    describeEvent(e("card.create", { id: "tk_1", title: "alpha", column: "Todo" }), titleOf, v, true),
    "created in Todo",
  );
  assert.equal(
    describeEvent(e("card.move", { id: "tk_1", from: "Todo", to: "Done" }), titleOf, v, true),
    "moved from Todo to Done",
  );
  assert.equal(
    describeEvent(e("card.check", { id: "tk_1", done: true }), titleOf, v, true),
    "marked done",
  );
  assert.equal(
    describeEvent(e("card.check", { id: "tk_1", done: false }), titleOf, v, true),
    "reopened",
  );
  assert.equal(
    describeEvent(e("card.edit", { id: "tk_1", field: "title", value: "beta" }), titleOf, v, true),
    "renamed to “beta”",
  );
  assert.equal(
    describeEvent(e("card.edit", { id: "tk_1", field: "description" }), titleOf, v, true),
    "edited the description",
  );
  assert.equal(describeEvent(e("comment.add", { id: "tk_1" }), titleOf, v, true), "commented");
});

Deno.test("cardHistory — filters, drops non-card events, newest first, no mutation", () => {
  const evs: HistoryEvent[] = [
    { ts: "1", device: "me", kind: "card.create", id: "tk_1" },
    { ts: "2", device: "me", kind: "auth.claim" },
    { ts: "3", device: "me", kind: "card.move", id: "tk_1", from: "a", to: "b" },
    { ts: "4", device: "me", kind: "card.create", id: "tk_2" },
  ];
  const snapshot = [...evs];
  const h = cardHistory(evs, "tk_1");
  assert.deepEqual(h.map((e) => e.ts), ["3", "1"]);
  assert.deepEqual(evs, snapshot);
});

Deno.test("boardActivity — hides mesh.* by default, caps + totals", () => {
  const evs: HistoryEvent[] = [
    { ts: "1", device: "d", kind: "card.create", id: "tk_1" },
    { ts: "2", device: "d", kind: "mesh.merge", path: "x" },
    { ts: "3", device: "d", kind: "card.move", id: "tk_1" },
    { ts: "4", device: "d", kind: "card.check", id: "tk_1" },
    { ts: "5", device: "d", kind: "comment.add", id: "tk_1" },
  ];
  const hidden = boardActivity(evs, { showSync: false, limit: 2 });
  assert.deepEqual(hidden.items.map((e) => e.ts), ["5", "4"]);
  assert.equal(hidden.total, 4);

  const shown = boardActivity(evs, { showSync: true, limit: 2 });
  assert.deepEqual(shown.items.map((e) => e.ts), ["5", "4"]);
  assert.equal(shown.total, 5);
});

Deno.test("knownTitles — create then rename gives the renamed title", () => {
  const evs: HistoryEvent[] = [
    { ts: "1", device: "d", kind: "card.create", id: "tk_1", title: "alpha" },
    { ts: "2", device: "d", kind: "card.edit", id: "tk_1", field: "title", value: "alpha-renamed" },
  ];
  const titles = knownTitles(evs);
  assert.equal(titles.get("tk_1"), "alpha-renamed");
});

Deno.test("dedupe — removes exact duplicates, keeps order", () => {
  const a: HistoryEvent = { ts: "1", device: "d", kind: "card.create", id: "tk_1" };
  const b: HistoryEvent = { ts: "2", device: "d", kind: "card.move", id: "tk_1" };
  const out = dedupe([a, b, { ...a }]);
  assert.deepEqual(out, [a, b]);
});
