// "Merge both boards": two browsers that EACH already have a board (a card of
// their own) connect over iroh and must end up seeing BOTH cards — the union.
// A hosts + makes card-A; B (separate context) makes card-B while solo, then joins
// A's ticket. Both must then show card-A and card-B.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8128";
const base = `http://127.0.0.1:${PORT}/`;
const rnd = () => Math.random().toString(16).slice(2, 7);

const browser = await chromium.launch();
let code = 1;
try {
  // A: host, create card-A.
  const a = await (await browser.newContext()).newPage();
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  const titleA = "cardA-" + rnd();
  const inA = a.locator(".add-card input").first();
  await inA.fill(titleA);
  await inA.press("Enter");
  await a.waitForSelector(`text=${titleA}`, { timeout: 10000 });
  console.log(`A hosts (ticket ${ticket.length} chars) + made ${titleA}`);

  // B: its own context. Solo first → create card-B (a pre-existing board).
  const bctx = await browser.newContext();
  const b = await bctx.newPage();
  b.on("pageerror", (e) => console.log("B pageerror:", e.message));
  await b.goto(base, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  const titleB = "cardB-" + rnd();
  const inB = b.locator(".add-card input").first();
  await inB.fill(titleB);
  await inB.press("Enter");
  await b.waitForSelector(`text=${titleB}`, { timeout: 10000 });
  console.log(`B made ${titleB} while solo`);

  // B opens A's link fresh (new tab / reload — the common, reliable path with a
  // clean teardown). Same context keeps card-B in OPFS + B's identity in localStorage.
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.reload({ waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  console.log("B joined A; waiting for the boards to merge...");

  // The merge: each side must show BOTH cards.
  await a.waitForSelector(`text=${titleB}`, { timeout: 30000 });
  await b.waitForSelector(`text=${titleA}`, { timeout: 30000 });
  await a.waitForSelector(`text=${titleA}`, { timeout: 5000 });
  await b.waitForSelector(`text=${titleB}`, { timeout: 5000 });
  console.log("PASS: both boards merged — each browser sees card-A AND card-B");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
