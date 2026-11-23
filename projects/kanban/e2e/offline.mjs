// Offline edits over the real iroh mesh: A hosts, B joins; B goes offline
// (status "offline"), adds a card, comes back online, and the card must reach A
// with B reconnected.
//
// Caveat: Playwright's offline emulation flips navigator.onLine and fires the
// offline/online events, but may not cut an already-established relay
// WebSocket — so this proves the status + "online ⇒ retry now" path and that
// offline-made edits land, not a real network outage (see the roadmap's
// real-network notes).
import { chromium } from "playwright";

const PORT = process.env.PORT || "8151";
const base = `http://127.0.0.1:${PORT}/`;
const rnd = () => Math.random().toString(16).slice(2, 7);

async function addCard(pg, title) {
  const inp = pg.locator(".add-card input").first();
  await inp.fill(title);
  await inp.press("Enter");
  await pg.waitForSelector(`text=${title}`, { timeout: 10000 });
}

async function join(browser, ticket, tag) {
  const pg = await (await browser.newContext()).newPage();
  pg.on("pageerror", (e) => console.log(`${tag} pageerror:`, e.message));
  await pg.goto(base + "#" + ticket, { waitUntil: "load" });
  await pg.reload({ waitUntil: "load" }); // fresh load = clean join
  await pg.waitForSelector(".add-card input", { timeout: 20000 });
  await pg.waitForTimeout(3500); // let the swarm form
  return pg;
}

const status = (pg) => pg.evaluate(() => globalThis.riftpipe.meshStatus());
async function waitStatus(pg, state, ms) {
  const t = Date.now();
  while (Date.now() - t < ms) {
    if ((await status(pg)).state === state) return;
    await pg.waitForTimeout(500);
  }
  throw new Error(`status never became ${state} (last ${JSON.stringify(await status(pg))})`);
}

const browser = await chromium.launch();
let code = 1;
try {
  const r = rnd();
  const ctxA = await browser.newContext();
  const a = await ctxA.newPage();
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, null, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  await addCard(a, "seed-" + r);

  const b = await join(browser, ticket, "B");
  const ctxB = b.context();
  await b.waitForSelector(`text=seed-${r}`, { timeout: 45000 });
  await waitStatus(b, "connected", 45e3);
  console.log("A hosts, B joined and converged");

  await ctxB.setOffline(true);
  await waitStatus(b, "offline", 15e3);
  await addCard(b, "offline-" + r);
  console.log("B went offline and added a card");
  await b.waitForTimeout(5000);
  await ctxB.setOffline(false);

  await a.waitForSelector(`text=offline-${r}`, { timeout: 90000 });
  await waitStatus(b, "connected", 90e3);
  console.log("B back online:", JSON.stringify(await status(b)));

  console.log("PASS: offline edits sync after coming back online");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
