// Card deletes on a CLAIMED board over the real sealed mesh: a delete is
// content, so it is signed, sealed and trust-gated like a write. The owner A
// claims, approves B and C; B (authorized) deletes a card and it leaves A and
// C. A then removes C; C's later delete drops the card on C's own board only —
// A and B keep it.
//
// Run: node e2e/delete-claimed.mjs   (needs `./run.sh serve` on PORT, default 8140)
import { chromium } from "playwright";

const PORT = process.env.PORT || "8140";
const base = `http://127.0.0.1:${PORT}/`;
const fail = (m) => { console.error("FAIL:", m); process.exitCode = 1; throw new Error(m); };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

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
/** Poll until the board on `page` has no card titled `title`. */
async function waitNoCard(page, title, ms = 15000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (!(await hasCard(page, title))) return true; await wait(200); }
  return false;
}
async function addCard(page, title) {
  const input = page.locator(".add-card input").first();
  await input.fill(title);
  await input.press("Enter");
}
async function deleteCardVia(page, title) {
  await page.locator(".card .title-link", { hasText: title }).click();
  await page.locator(".drawer .card-delete").click();
}
const authState = (page) => page.evaluate(() => window.riftpipe.authState());

async function waitState(page, pred, ms = 10000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const s = await authState(page); if (pred(s)) return s; await wait(250); }
  return authState(page);
}
async function newPage(browser) {
  const p = await (await browser.newContext()).newPage();
  p.on("dialog", (d) => d.accept());
  return p;
}
async function join(browser, ticket) {
  const p = await newPage(browser);
  await p.goto(base + "#" + ticket, { waitUntil: "load" });
  await p.waitForSelector(".add-card input", { timeout: 20000 });
  return p;
}

const browser = await chromium.launch({ headless: true });
try {
  // 1. A hosts; B and C join; A claims and approves both.
  const a = await newPage(browser);
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

  await a.locator(".lock-btn").click();
  const aState = await waitState(a, (s) => s.locked && s.isAuthor && s.canRead && s.epoch >= 1);
  if (!(aState.locked && aState.isAuthor && aState.canRead && aState.epoch >= 1)) {
    fail(`A should be the claimed, read-capable author with epoch >= 1, got ${JSON.stringify(aState)}`);
  }
  for (const [p, id] of [[b, bId], [c, cId]]) {
    await p.locator(".ask-btn").click();
    const approveBtn = a.locator(".req", { hasText: id.slice(0, 8) }).locator(".approve-btn");
    await approveBtn.waitFor({ timeout: 10000 });
    await approveBtn.click();
    const s = await waitState(p, (s) => s.authorized && s.canRead);
    if (!(s.authorized && s.canRead)) fail(`device ${id.slice(0, 8)} should be authorized and able to read, got ${JSON.stringify(s)}`);
  }
  console.log("✓ A claimed the board; B and C approved");

  // 2. A adds two cards; B and C show both.
  await addCard(a, "doomed-claimed");
  await addCard(a, "kept-claimed");
  for (const [p, n] of [[b, "B"], [c, "C"]]) {
    if (!(await waitCard(p, "doomed-claimed", 15000)) || !(await waitCard(p, "kept-claimed", 15000))) {
      fail(`${n} should show both cards`);
    }
  }
  console.log("✓ B and C show both cards");

  // 3. B (authorized) deletes doomed through the UI.
  await deleteCardVia(b, "doomed-claimed");
  if (!(await waitNoCard(b, "doomed-claimed", 10000))) fail("B's own board should drop the card it deleted");
  if (!(await waitNoCard(a, "doomed-claimed", 15000))) fail("B's authorized delete should reach A");
  if (!(await waitNoCard(c, "doomed-claimed", 15000))) fail("B's authorized delete should reach C");
  console.log("✓ an authorized device's delete — sealed under the epoch — lands");

  // 4. A removes C.
  const revokeBtn = a.locator(".member", { hasText: cId.slice(0, 8) }).locator(".revoke-btn");
  await revokeBtn.waitFor({ timeout: 8000 });
  await revokeBtn.click();
  const cAfter = await waitState(c, (s) => !s.canRead);
  if (cAfter.canRead) fail(`C should lose its read key, got ${JSON.stringify(cAfter)}`);
  console.log("✓ C removed");

  // 5. C deletes kept through the UI: its own board drops it, nobody else's.
  await deleteCardVia(c, "kept-claimed");
  if (!(await waitNoCard(c, "kept-claimed", 10000))) fail("C's own board should drop the card it deleted");
  await wait(5000);
  if (!(await hasCard(a, "kept-claimed"))) fail("a removed device's delete must not reach A");
  if (!(await hasCard(b, "kept-claimed"))) fail("a removed device's delete must not reach B");
  console.log("✓ a removed device's delete never lands");

  console.log("\nPASS — claimed-board deletes: an authorized delete lands, a removed device's delete doesn't");
} finally {
  await browser.close();
}
