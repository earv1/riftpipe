// End-to-end test of the "lock = authorship" auth UI over the real sealed mesh.
//
// Flow:
//   1. Window A hosts an empty board (unlocked). Its auth bar shows a Lock
//      button; anyone can edit.
//   2. Window B joins A's share link. While unlocked, B can add a card and it
//      syncs to A (open edit).
//   3. A clicks "Lock board" → A becomes the author. B, still un-granted, drops
//      to "view only"; a card B writes now must NOT appear on A.
//   4. A grants B's device id. B returns to "you can edit"; a card B writes now
//      DOES appear on A.
//
// Run: node e2e/lock-grant.mjs   (needs `./run.sh serve` on PORT, default 8140)
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

const browser = await chromium.launch({ headless: true });
try {
  const a = await (await browser.newContext()).newPage();
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  console.log(`A hosting (${ticket.length}-char ticket)`);

  const b = await (await browser.newContext()).newPage();
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  await wait(1500);
  const bId = (await authState(b)).device;
  console.log(`B joined (device ${bId.slice(0, 10)}…)`);

  // 1. Both start unlocked — Lock button visible on A.
  if ((await authState(a)).locked) fail("A should start unlocked");
  if (!(await a.locator(".lock-btn").isVisible())) fail("A should show a Lock button while unlocked");

  // 2. Open edit: B → A while unlocked.
  await addCard(b, "open-edit");
  if (!(await waitCard(a, "open-edit"))) fail("unlocked: B's card should sync to A");
  console.log("✓ unlocked: B's edit synced to A");

  // 3. A locks → becomes author; B drops to view-only.
  await a.locator(".lock-btn").click();
  await wait(2500);
  const aState = await authState(a);
  if (!aState.locked || !aState.isAuthor) fail(`A should be locked author, got ${JSON.stringify(aState)}`);
  console.log("✓ A locked the board and is the author");

  const bState = await authState(b);
  if (!bState.locked) fail("B should observe the board is now locked");
  if (bState.authorized) fail("B should be un-authorized right after A locks");
  await b.waitForSelector(".auth-status.viewer", { timeout: 5000 });
  console.log("✓ B dropped to view-only after lock");

  // B's write must be rejected by A now.
  await addCard(b, "blocked-write");
  await wait(3000);
  if (await hasCard(a, "blocked-write")) fail("locked: un-granted B's card must NOT reach A");
  console.log("✓ locked: un-granted B's write was rejected by A");

  // 4. A grants B → B can edit again.
  await a.locator(".grant-input").fill(bId);
  await a.locator(".grant-btn").click();
  await wait(2500);
  const bAfter = await authState(b);
  if (!bAfter.authorized) fail(`B should be authorized after grant, got ${JSON.stringify(bAfter)}`);
  await b.waitForSelector(".auth-status.editor", { timeout: 5000 });
  console.log("✓ B authorized after grant");

  await addCard(b, "granted-write");
  if (!(await waitCard(a, "granted-write"))) fail("granted: B's card should now sync to A");
  console.log("✓ granted: B's write synced to A");

  console.log("\nPASS — lock=authorship + grant flow works end to end over the mesh");
} finally {
  await browser.close();
}
