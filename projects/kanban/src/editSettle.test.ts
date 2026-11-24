import assert from "node:assert/strict";
import { createEditSettler } from "./editSettle.ts";

// A manual clock: timers fire only when the test advances time.
function harness(board = "A") {
  let now = 0;
  let nextId = 1;
  const timers = new Map<number, { at: number; fn: () => void }>();
  const logged: Record<string, unknown>[] = [];
  const state = { board };
  const s = createEditSettler({
    settleMs: 1500,
    board: () => state.board,
    log: (data) => {
      logged.push({ board: state.board, ...data });
      return Promise.resolve();
    },
    setTimer: (fn, ms) => {
      const id = nextId++;
      timers.set(id, { at: now + ms, fn });
      return id;
    },
    clearTimer: (h) => void timers.delete(h as number),
  });
  const advance = (ms: number) => {
    now += ms;
    for (const [id, t] of [...timers]) {
      if (t.at <= now) {
        timers.delete(id);
        t.fn();
      }
    }
  };
  return { s, logged, state, advance, timers };
}

Deno.test("editSettler — keystrokes coalesce into one edit with the final value", () => {
  const { s, logged, advance } = harness();
  s.queue("tk_1", "title", "alpha", "alpha-");
  advance(500);
  s.queue("tk_1", "title", "alpha-", "alpha-r");
  advance(1000);
  s.queue("tk_1", "title", "alpha-r", "alpha-renamed");
  advance(1499);
  assert.equal(logged.length, 0);
  advance(1);
  assert.deepEqual(logged, [{ board: "A", id: "tk_1", field: "title", value: "alpha-renamed" }]);
  assert.equal(s.pendingCount(), 0);
});

Deno.test("editSettler — no event when the field ends where it started", () => {
  const { s, logged, advance } = harness();
  s.queue("tk_1", "description", "x", "xy");
  s.queue("tk_1", "description", "xy", "x");
  advance(2000);
  assert.deepEqual(logged, []);
});

Deno.test("editSettler — a board switch before the edit settles drops it", () => {
  const { s, logged, state, advance } = harness("A");
  s.queue("tk_1", "title", "alpha", "secret title on A");
  state.board = "B";
  advance(2000);
  assert.deepEqual(logged, [], "A's title must never be written into B's log");
});

Deno.test("editSettler — typing resumed on another board starts over there", () => {
  const { s, logged, state, advance } = harness("A");
  s.queue("tk_1", "title", "alpha", "alpha-a");
  state.board = "B";
  s.queue("tk_1", "title", "beta", "beta-b");
  advance(2000);
  assert.deepEqual(logged, [{ board: "B", id: "tk_1", field: "title", value: "beta-b" }]);
});

Deno.test("editSettler — flush writes pending edits now, on their own board", async () => {
  const { s, logged, advance, timers } = harness("A");
  s.queue("tk_1", "title", "alpha", "alpha-renamed");
  s.queue("tk_2", "description", "", "notes");
  await s.flush();
  assert.deepEqual(logged, [
    { board: "A", id: "tk_1", field: "title", value: "alpha-renamed" },
    { board: "A", id: "tk_2", field: "description" },
  ]);
  assert.equal(timers.size, 0);
  advance(2000);
  assert.equal(logged.length, 2, "a flushed edit is not written again by its timer");
});

Deno.test("editSettler — flush after a board switch drops instead of misfiling", async () => {
  const { s, logged, state } = harness("A");
  s.queue("tk_1", "title", "alpha", "alpha-renamed");
  state.board = "B";
  await s.flush();
  assert.deepEqual(logged, []);
});
