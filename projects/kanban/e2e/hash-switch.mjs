// In-tab board switch over the real iroh mesh, both ways, with no reload: A
// hosts board A, C hosts board C. A pastes C's link into its hash (same tab ⇒
// same page-lifetime iroh Node, re-subscribed to C's topic, key never rebound)
// and must sync with C both ways; then A switches back to its own link and must
// show only board A's cards again.
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

async function host(browser, tag) {
  const pg = await (await browser.newContext()).newPage();
  pg.on("pageerror", (e) => console.log(`${tag} pageerror:`, e.message));
  await pg.goto(base, { waitUntil: "load" });
  await pg.waitForSelector(".add-card input", { timeout: 20000 });
  await pg.waitForFunction(() => location.hash.length > 1, null, { timeout: 40000 });
  return pg;
}

const status = (pg) => pg.evaluate(() => globalThis.riftpipe.meshStatus());

const browser = await chromium.launch();
let code = 1;
try {
  const r = rnd();
  const a = await host(browser, "A");
  const ticketA = await a.evaluate(() => location.hash.slice(1));
  await addCard(a, "a-own-" + r);
  const c = await host(browser, "C");
  const ticketC = await c.evaluate(() => location.hash.slice(1));
  await addCard(c, "c-card-" + r);
  console.log("A and C each host their own board");

  // A → board C, same tab.
  await a.evaluate((t) => { location.hash = t; }, ticketC);
  await a.waitForSelector(`text=c-card-${r}`, { timeout: 45000 });
  await addCard(a, "a-in-c-" + r);
  await c.waitForSelector(`text=a-in-c-${r}`, { timeout: 45000 });
  console.log("A switched to board C and syncs both ways:", JSON.stringify(await status(a)));

  // A → back to board A, same tab.
  await a.evaluate((t) => { location.hash = t; }, ticketA);
  await a.waitForSelector(`text=a-own-${r}`, { timeout: 30000 });
  await a.waitForTimeout(3000);
  const leaked = await a.locator(`text=c-card-${r}`).count();
  if (leaked !== 0) throw new Error(`board C's card is still shown on board A (${leaked})`);
  // Board A has no peer now: the pill must say so, not keep board C's "connected".
  const pill = (await a.locator(".mesh-status").textContent()) ?? "";
  if (!pill.includes("waiting")) throw new Error(`stale status pill after the switch back: "${pill}"`);
  console.log("A switched back to board A; board C's cards are gone; pill:", pill);

  console.log("PASS: in-tab board switch both ways — no reload, no rebind");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
