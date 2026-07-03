// 3-browser gossip mesh: A hosts, B and C join A's link. Each makes a card, and
// ALL THREE must see ALL THREE cards — messages route through the swarm, not a
// fixed hub. Also logs the routing map (debug view of the topology).
import { chromium } from "playwright";

const PORT = process.env.PORT || "8129";
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

const browser = await chromium.launch();
let code = 1;
try {
  const a = await (await browser.newContext()).newPage();
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  const ta = "cardA-" + rnd();
  await addCard(a, ta);
  console.log(`A hosts (${ticket.length}c) + made ${ta}`);

  const b = await join(browser, ticket, "B");
  const tb = "cardB-" + rnd();
  await addCard(b, tb);
  const c = await join(browser, ticket, "C");
  const tc = "cardC-" + rnd();
  await addCard(c, tc);
  console.log("B, C joined + made their cards");

  for (const [name, pg] of [["A", a], ["B", b], ["C", c]]) {
    for (const t of [ta, tb, tc]) {
      await pg.waitForSelector(`text=${t}`, { timeout: 45000 });
    }
    console.log(`${name} sees all three cards`);
  }

  const rm = await a.evaluate(() => globalThis.riftpipe.routingMap());
  console.log("routing map (A's view):", JSON.stringify(rm));
  console.log("PASS: 3-peer gossip mesh — all peers see all cards");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
