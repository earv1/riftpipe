// Unit tests for TreeSession's stderr line parsers (no binary needed).
import { test } from "node:test";
import assert from "node:assert/strict";

import { parseAccessRequestLine } from "../dist/tree.js";

const device = "ab".repeat(32);

test("parseAccessRequestLine: reads a well-formed access-request line", () => {
  assert.deepEqual(parseAccessRequestLine(`[auth] access-request device=${device} safety=a1b2 c3d4 name="Bob \\"B\\""`), {
    device,
    safety: "a1b2 c3d4",
    name: 'Bob "B"',
  });
});

test("parseAccessRequestLine: a garbled or non-string name is ignored, not thrown", () => {
  assert.equal(parseAccessRequestLine(`[auth] access-request device=${device} safety=a1b2 c3d4 name="Bob`), null);
  assert.equal(parseAccessRequestLine(`[auth] access-request device=${device} safety=a1b2 c3d4 name=42`), null);
  assert.equal(parseAccessRequestLine("[mesh] joined board"), null);
});
