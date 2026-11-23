// End-to-end test of the history view over the real sealed mesh:
//   - the board-wide Activity panel and per-card History update live on both
//     peers (no reload), with "You" vs a peer's handle + short id;
//   - per-keystroke title saves coalesce into ONE card.edit;
//   - events/* changes never re-render the board (boardRefreshes unchanged);
//   - mesh.merge rows hide behind "Show sync events";
//   - 120 concurrent appends lose nothing and rotate into immutable segments,
//     which sync to the peer (after one settling append — gossip drops under a
//     burst are only repaired by the next push or a neighbour join);
//   - on a claimed board the log is sealed like content: a late joiner and an
//     unapproved peer can't read the owner's post-claim events.
//
// Run: node e2e/history.mjs   (needs `vite preview` on PORT, default 8142)
import { chromium } from "playwright";

const PORT = process.env.PORT || "8142";
const base = `http://127.0.0.1:${PORT}/`;
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const fail = (m) => { console.error("FAIL:", m); process.exitCode = 1; throw new Error(m); };

// Poll `fn` every 200 ms until it returns a truthy value; `null` on timeout.
async function until(fn, ms = 10000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const v = await fn();
    if (v) return v;
    await wait(200);
  }
  return null;
}
async function addCard(page, title) {
  const i = page.locator(".add-card input").first();
  await i.fill(title); await i.press("Enter");
  await page.waitForSelector(".card .title", { timeout: 8000 });
}
async function join(browser, ticket) {
  const p = await (await browser.newContext()).newPage();
  await p.goto(base + "#" + ticket, { waitUntil: "load" });
  await p.waitForSelector(".add-card input", { timeout: 20000 });
  return p;
}
const authState = (page) => page.evaluate(() => window.riftpipe.authState());
async function waitState(page, pred, ms = 10000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const s = await authState(page); if (pred(s)) return s; await wait(250); }
  return authState(page);
}
const ev = (page) => page.evaluate(() => window.riftpipe.readEvents());
// The rendered event rows under `scope` (".activity" or ".drawer .card-history").
const rows = (page, scope) =>
  page.$$eval(`${scope} .event-row`, (els) =>
    els.map((el) => ({
      kind: el.dataset.kind,
      who: el.querySelector(".event-who")?.textContent ?? "",
      what: el.querySelector(".event-what")?.textContent ?? "",
      when: el.querySelector(".event-when")?.textContent ?? "",
      dt: el.querySelector(".event-when")?.getAttribute("datetime") ?? "",
    })),
  );
const sameList = (x, y) => JSON.stringify(x) === JSON.stringify(y);

const browser = await chromium.launch({ headless: true });
try {
  // 1. Setup: A hosts, B joins; both open the Activity panel.
  const a = await (await browser.newContext()).newPage();
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  const aId = await a.evaluate(() => window.riftpipe.deviceId());
  const aShort = aId.slice(0, 8);
  const b = await join(browser, ticket);
  await wait(1500);
  for (const p of [a, b]) {
    await p.locator(".activity-toggle").click();
    await p.waitForSelector(".activity", { timeout: 5000 });
  }
  console.log(`A hosting (device ${aShort}…); B joined; Activity open on both`);

  // 2. Own event shows as "You".
  await addCard(a, "alpha");
  const own = await until(async () =>
    (await rows(a, ".activity")).find((r) => r.what === "created “alpha” in Todo" && r.who.startsWith("You")));
  if (!own) fail(`A's Activity should show 'You · created “alpha” in Todo', got ${JSON.stringify(await rows(a, ".activity"))}`);
  console.log("✓ A's own event appears live as", JSON.stringify(own.who));

  // 3. Live on the peer, labelled with A's handle + short id (not "You").
  const peer = await until(async () =>
    (await rows(b, ".activity")).find((r) => r.what === "created “alpha” in Todo"), 15000);
  if (!peer) fail(`B's Activity should show A's card.create live, got ${JSON.stringify(await rows(b, ".activity"))}`);
  if (!peer.who.includes(aShort) || peer.who.startsWith("You")) fail(`B should label A's row with ${aShort}, got ${JSON.stringify(peer.who)}`);
  console.log("✓ B sees A's event live, labelled", JSON.stringify(peer.who));

  // 4. Check → "marked … done" on the peer.
  await a.locator(".card", { hasText: "alpha" }).locator('input[type="checkbox"]').check();
  if (!(await until(async () => (await rows(b, ".activity")).some((r) => r.what === "marked “alpha” done"), 15000))) {
    fail(`B's Activity should show 'marked “alpha” done', got ${JSON.stringify(await rows(b, ".activity"))}`);
  }
  console.log("✓ B sees the check live");

  // 5. Rename via per-keystroke live saves → exactly one settled card.edit.
  const cardId = (await ev(a)).find((e) => e.kind === "card.create" && e.title === "alpha")?.id;
  if (!cardId) fail("A should have a card.create event for alpha");
  await a.locator(".card .title-link", { hasText: "alpha" }).click();
  await a.waitForSelector(".drawer .detail-title", { timeout: 8000 });
  await a.locator(".drawer .detail-title").click();
  await a.keyboard.press("End");
  await a.locator(".drawer .detail-title").pressSequentially("-renamed", { delay: 40 });
  await wait(2500);
  const edits = (await ev(a)).filter((e) => e.kind === "card.edit" && e.field === "title" && e.id === cardId);
  if (edits.length !== 1) fail(`typing a rename should log exactly 1 title card.edit, got ${edits.length}: ${JSON.stringify(edits)}`);
  if (edits[0].value !== "alpha-renamed") fail(`the card.edit should carry the final title, got ${JSON.stringify(edits[0])}`);
  console.log("✓ 8 keystrokes coalesced into 1 card.edit (value alpha-renamed)");

  // 6. Per-card History in the drawer, newest first.
  const expected = ["commented", "renamed to “alpha-renamed”", "marked done", "created in Todo"];
  await a.locator(".drawer .add-comment textarea").fill("hello history");
  await a.locator(".drawer .add-comment button").click();
  const aHist = await until(async () => {
    const w = (await rows(a, ".drawer .card-history")).map((r) => r.what).slice(0, 4);
    return sameList(w, expected) ? w : null;
  });
  if (!aHist) fail(`A's card History should be ${JSON.stringify(expected)}, got ${JSON.stringify(await rows(a, ".drawer .card-history"))}`);
  await a.locator(".drawer-close").click();
  await a.waitForSelector(".drawer", { state: "detached", timeout: 8000 });
  console.log("✓ A's card History:", JSON.stringify(aHist));

  // 7. Same History on B.
  if (!(await until(async () => (await b.locator(".card .title", { hasText: "alpha-renamed" }).count()) > 0, 15000))) {
    fail("B should receive the renamed card alpha-renamed");
  }
  await b.locator(".card .title-link", { hasText: "alpha-renamed" }).click();
  await b.waitForSelector(".drawer .card-history", { timeout: 8000 });
  const bHist = await until(async () => {
    const w = (await rows(b, ".drawer .card-history")).map((r) => r.what).slice(0, 4);
    return sameList(w, expected) ? w : null;
  }, 15000);
  if (!bHist) fail(`B's card History should be ${JSON.stringify(expected)}, got ${JSON.stringify(await rows(b, ".drawer .card-history"))}`);
  await b.locator(".drawer-close").click();
  await b.waitForSelector(".drawer", { state: "detached", timeout: 8000 });
  console.log("✓ B's card History matches");

  // 8. Relative times with a machine-readable datetime.
  const aRows = await rows(a, ".activity");
  if (aRows.length === 0) fail("A's Activity should have rows");
  for (const r of aRows) {
    if (!/^(just now|\d+ min ago)$/.test(r.when)) fail(`row time should be relative, got ${JSON.stringify(r)}`);
    if (Number.isNaN(Date.parse(r.dt))) fail(`row datetime should parse, got ${JSON.stringify(r)}`);
  }
  console.log(`✓ ${aRows.length} rows show relative times ("${aRows[0].when}")`);

  // 9. An events/* change reaches B's Activity without re-rendering B's board.
  const n0 = await b.evaluate(() => window.riftpipe.boardRefreshes());
  await a.evaluate(() => window.riftpipe.logEvent("test.ping", { n: 1 }));
  if (!(await until(async () => (await rows(b, ".activity")).some((r) => r.kind === "test.ping"), 15000))) {
    fail("B's Activity should show A's test.ping");
  }
  await wait(1000);
  const n1 = await b.evaluate(() => window.riftpipe.boardRefreshes());
  if (n1 !== n0) fail(`an events/* change must not refresh the board: boardRefreshes ${n0} → ${n1}`);
  console.log(`✓ test.ping reached B's Activity; board refreshes unchanged (${n0})`);

  // 10. Sync events are hidden until toggled on.
  if ((await rows(b, ".activity")).some((r) => r.kind === "mesh.merge")) fail("mesh.merge rows should be hidden by default");
  await b.locator(".activity .activity-sync-toggle").click();
  if (!(await until(async () => (await rows(b, ".activity")).some((r) => r.kind === "mesh.merge")))) {
    fail("B's Activity should show mesh.merge rows once 'Show sync events' is on");
  }
  console.log("✓ mesh.merge rows hidden by default, shown with the toggle");

  // 11. 120 concurrent appends: none lost, the log rotates, segments sync.
  await a.evaluate(() =>
    Promise.all(Array.from({ length: 120 }, (_, i) => window.riftpipe.logEvent("test.bulk", { i, pad: "x".repeat(200) }))));
  const bulk = (await ev(a)).filter((e) => e.kind === "test.bulk");
  if (bulk.length !== 120) fail(`A should hold all 120 concurrent appends, got ${bulk.length}`);
  if (new Set(bulk.map((e) => e.i)).size !== 120) fail("A's 120 appends should have 120 distinct i");
  const names = await a.evaluate(() => window.riftpipe.listDir("events"));
  const segRe = new RegExp(`^${aId}\\.\\d{6}\\.jsonl$`);
  if (!names.includes(`${aId}.jsonl`)) fail(`events/ should hold the active log ${aId}.jsonl, got ${JSON.stringify(names)}`);
  const segs = names.filter((n) => segRe.test(n));
  if (segs.length < 1) fail(`events/ should hold at least one rotated segment, got ${JSON.stringify(names)}`);
  const bBulkCount = async () => (await ev(b)).filter((e) => e.kind === "test.bulk").length;
  // Gossip is best-effort and only repairs on a neighbour join: under a burst
  // the active log's LAST update can be dropped, leaving B stale until A's next
  // append. Measure that gap (informational), then append once more so the
  // active log goes out as one uncontended message, and require convergence.
  const t0 = Date.now();
  const unsettled = await until(async () => ((await bBulkCount()) === 120 ? true : null), 10000);
  console.log(unsettled
    ? `  (info) B converged on its own after ${((Date.now() - t0) / 1000).toFixed(1)} s`
    : `  (info) B still stale after 10 s without a further append: ${await bBulkCount()}/120 bulk events`);
  await a.evaluate(() => window.riftpipe.logEvent("test.settle", {}));
  const bConverged = await until(async () => {
    const bNames = await b.evaluate(() => window.riftpipe.listDir("events"));
    return (await bBulkCount()) === 120 && segs.every((s) => bNames.includes(s)) ? true : null;
  }, 20000);
  if (!bConverged) {
    const have = new Set((await ev(b)).filter((e) => e.kind === "test.bulk").map((e) => e.i));
    const missing = Array.from({ length: 120 }, (_, i) => i).filter((i) => !have.has(i));
    const bNames = await b.evaluate(() => window.riftpipe.listDir("events"));
    fail(`B should receive all 120 bulk events and A's segments, got ${have.size}; missing i=${JSON.stringify(missing)}; A segments ${JSON.stringify(segs)}; B events/ ${JSON.stringify(bNames)}`);
  }
  console.log(`✓ 120 concurrent appends kept; ${segs.length} segment(s) rotated; all 120 + the segments reached B`);

  // 12. Claimed board: the log is sealed like content.
  await a.locator(".lock-btn").click();
  const claimed = await waitState(a, (s) => s.locked && s.isAuthor && s.canRead);
  if (!(claimed.locked && claimed.isAuthor && claimed.canRead)) fail(`A should own the claimed board, got ${JSON.stringify(claimed)}`);
  await addCard(a, "after-claim");
  await a.evaluate(() => window.riftpipe.logEvent("test.secret", {}));
  const c = await join(browser, ticket);
  await wait(5000);
  const cEvents = await ev(c);
  if (cEvents.some((e) => e.device === aId)) fail(`late joiner C must read none of A's events, got ${JSON.stringify(cEvents.filter((e) => e.device === aId).slice(0, 3))}`);
  if (cEvents.some((e) => e.title === "after-claim")) fail("late joiner C must not read the after-claim card.create");
  await c.locator(".activity-toggle").click();
  await c.waitForSelector(".activity", { timeout: 5000 });
  await wait(500);
  if ((await rows(c, ".activity")).some((r) => r.who.includes(aShort))) fail("C's Activity must show no rows from A");
  const bEvents = await ev(b);
  if (bEvents.some((e) => e.kind === "test.secret")) fail("unapproved B must not read A's post-claim test.secret");
  if (bEvents.some((e) => e.title === "after-claim")) fail("unapproved B must not read A's post-claim card.create");
  console.log(`✓ sealed: late joiner C sees 0 of A's events (${cEvents.length} total); unapproved B sees nothing post-claim`);

  console.log("\nPASS — history view: live per-card + board activity, coalesced edits, segment rotation, no board re-render, sealed on claimed boards");
} finally {
  await browser.close();
}
