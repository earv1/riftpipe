// End-to-end READ REVOCATION proof over the real sealed mesh, browser side: the
// owner claims the board, approves B and C (both wrapped into epoch 1 and able
// to read), then removes C. The removal rotates the read key to epoch 2 wrapped
// to {A, B} — C, still connected, can decrypt nothing sealed under epoch 2; B
// keeps reading and writing normally.
//
// Run: node e2e/epoch-revoke.mjs   (needs `./run.sh serve` on PORT, default 8140)
import { chromium } from "playwright";

const PORT = process.env.PORT || "8140";
const base = `http://127.0.0.1:${PORT}/`;
const fail = (m) => { console.error("FAIL:", m); process.exitCode = 1; throw new Error(m); };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// Poll until the board on `page` contains a card titled `title`.
async function hasCard(page, title) {
  return page.evaluate(
    (t) => Array.from(document.querySelectorAll(".card .title")).some((e) => e.textContent?.trim() === t),
    title,
  );
}
async function waitCard(page, title, ms = 8000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (await hasCard(page, title)) return true; await wait(200); }
  return false;
}
async function addCard(page, title) {
  const input = page.locator(".add-card input").first();
  await input.fill(title);
  await input.press("Enter");
}
const authState = (page) => page.evaluate(() => window.riftpipe.authState());

async function waitState(page, pred, ms = 10000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const s = await authState(page); if (pred(s)) return s; await wait(250); }
  return authState(page);
}
async function join(browser, ticket) {
  const p = await (await browser.newContext()).newPage();
  await p.goto(base + "#" + ticket, { waitUntil: "load" });
  await p.waitForSelector(".add-card input", { timeout: 20000 });
  return p;
}

const browser = await chromium.launch({ headless: true });
try {
  // 1. A hosts; B and C join.
  const a = await (await browser.newContext()).newPage();
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  console.log(`A hosting (${ticket.length}-char ticket)`);

  const b = await join(browser, ticket);
  const c = await join(browser, ticket);
  await wait(1500);
  const bId = (await authState(b)).device;
  const cId = (await authState(c)).device;
  console.log(`B joined (device ${bId.slice(0, 10)}…); C joined (device ${cId.slice(0, 10)}…)`);

  // 2. A claims the board.
  await a.locator(".lock-btn").click();
  const aState = await waitState(a, (s) => s.locked && s.isAuthor && s.canRead && s.epoch >= 1);
  if (!(aState.locked && aState.isAuthor && aState.canRead && aState.epoch >= 1)) {
    fail(`A should be the claimed, read-capable author with epoch >= 1, got ${JSON.stringify(aState)}`);
  }
  console.log("✓ A claimed the board (epoch 1)");

  // 3. B and C ask, A approves both.
  for (const [p, id] of [[b, bId], [c, cId]]) {
    await p.locator(".ask-btn").click();
    const approveBtn = a.locator(".req", { hasText: id.slice(0, 8) }).locator(".approve-btn");
    await approveBtn.waitFor({ timeout: 10000 });
    await approveBtn.click();
    const s = await waitState(p, (s) => s.authorized && s.canRead);
    if (!(s.authorized && s.canRead)) fail(`device ${id.slice(0, 8)} should be authorized and able to read, got ${JSON.stringify(s)}`);
  }
  console.log("✓ B and C approved and wrapped into the epoch");

  // 4. A writes before revoking; both members should see it.
  await addCard(a, "before-revoke");
  if (!(await waitCard(b, "before-revoke", 10000))) fail("B should read the pre-revoke card");
  if (!(await waitCard(c, "before-revoke", 10000))) fail("C should read the pre-revoke card");
  console.log("✓ B and C both read the pre-revoke card");

  // 5. A removes C — rotates the read key to epoch 2, wrapped to {A, B} only.
  const revokeBtn = a.locator(".member", { hasText: cId.slice(0, 8) }).locator(".revoke-btn");
  await revokeBtn.waitFor({ timeout: 8000 });
  await revokeBtn.click();
  const aAfterRevoke = await waitState(a, (s) => s.epoch >= 2);
  if (!(aAfterRevoke.epoch >= 2)) fail(`A should have rotated to epoch >= 2, got ${JSON.stringify(aAfterRevoke)}`);
  const cAfterRevoke = await waitState(c, (s) => !s.authorized && !s.canRead);
  if (!(!cAfterRevoke.authorized && !cAfterRevoke.canRead)) fail(`C should be unauthorized with no read key, got ${JSON.stringify(cAfterRevoke)}`);
  const bAfterRevoke = await waitState(b, (s) => s.canRead && s.epoch >= 2);
  if (!(bAfterRevoke.canRead && bAfterRevoke.epoch >= 2)) fail(`B should keep reading under epoch >= 2, got ${JSON.stringify(bAfterRevoke)}`);
  console.log("✓ C removed: read key rotated; B still reads, C does not");

  // 6/7. Remaining members keep syncing post-rotation.
  await addCard(a, "after-revoke");
  await addCard(b, "b-after-revoke");
  await addCard(c, "c-after-revoke");
  if (!(await waitCard(b, "after-revoke", 10000))) fail("remaining members keep syncing: B should read A's post-rotation card");
  if (!(await waitCard(a, "b-after-revoke", 10000))) fail("remaining members keep syncing: A should read B's post-rotation card");
  console.log("✓ A and B keep syncing after the rotation");

  // 8. C must not decrypt anything new; A and B must not see C's write.
  await wait(4000);
  if (await hasCard(c, "after-revoke")) fail("revoked C must not decrypt A's post-rotation card");
  if (await hasCard(c, "b-after-revoke")) fail("revoked C must not decrypt B's post-rotation card");
  if (await hasCard(a, "c-after-revoke")) fail("revoked C's write must not reach A");
  if (await hasCard(b, "c-after-revoke")) fail("revoked C's write must not reach B");
  console.log("✓ revoked C neither reads post-rotation content nor writes anything that lands");

  console.log("\nPASS — read revocation: revoked browser can't decrypt new content; remaining members keep syncing");
} finally {
  await browser.close();
}
