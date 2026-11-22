// Board isolation: a browser's OPFS is one per origin but each board's files live
// in their own directory (keyed by the board topic), so a CLAIMED board's files
// never reach another board this browser opens. B makes card-B on its own board
// and claims it; B then opens A's (open) link — A must NOT get card-B, while B
// sees A's card. Back on its own board, B still has card-B and none of A's.
// (Before per-board scoping, opening A's link primed B's whole tree — claimed or
// not — into A's swarm as plain Sync.)
import { chromium } from "playwright";

const PORT = process.env.PORT || "8128";
const base = `http://127.0.0.1:${PORT}/`;
const rnd = () => Math.random().toString(16).slice(2, 7);
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const hasCard = (page, title) => page.locator(`text=${title}`).count().then((n) => n > 0);

async function addCard(page, title) {
  const input = page.locator(".add-card input").first();
  await input.fill(title);
  await input.press("Enter");
  await page.waitForSelector(`text=${title}`, { timeout: 10000 });
}

const browser = await chromium.launch();
let code = 1;
try {
  // A: host of an open board, with card-A.
  const a = await (await browser.newContext()).newPage();
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticketA = await a.evaluate(() => location.hash.slice(1));
  const titleA = "cardA-" + rnd();
  await addCard(a, titleA);
  console.log(`A hosts an open board + made ${titleA}`);

  // B: its own context and board — card-B, then claim it.
  const b = await (await browser.newContext()).newPage();
  b.on("pageerror", (e) => console.log("B pageerror:", e.message));
  await b.goto(base, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  await b.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticketB = await b.evaluate(() => location.hash.slice(1));
  const titleB = "cardB-" + rnd();
  await addCard(b, titleB);
  await b.locator(".lock-btn").click();
  await b.waitForFunction(() => {
    const s = window.riftpipe.authState();
    return s.locked && s.isAuthor;
  }, null, { timeout: 10000 });
  console.log(`B made ${titleB} on its own board and claimed it`);

  // B opens A's link (same context: same OPFS + identity).
  await b.goto(base + "#" + ticketA, { waitUntil: "load" });
  await b.reload({ waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  await b.waitForSelector(`text=${titleA}`, { timeout: 30000 });
  console.log("✓ B joined A's board and reads A's card");
  if (await hasCard(b, titleB)) throw new Error("B's claimed card is showing on A's board in B's tab");

  // Give B's (would-be) prime ample time to cross, then check A.
  await wait(5000);
  if (await hasCard(a, titleB)) throw new Error("claimed board B's card leaked into board A");
  console.log("✓ claimed board B's card did NOT reach board A");

  // Back on its own board, B has card-B and none of A's.
  await b.goto(base + "#" + ticketB, { waitUntil: "load" });
  await b.reload({ waitUntil: "load" });
  await b.waitForSelector(`text=${titleB}`, { timeout: 20000 });
  await wait(1500);
  if (await hasCard(b, titleA)) throw new Error("board A's card leaked into B's own board");
  console.log("✓ B's own board kept card-B and got none of A's");

  console.log("PASS: boards are isolated — a claimed board's files never reach another board");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
