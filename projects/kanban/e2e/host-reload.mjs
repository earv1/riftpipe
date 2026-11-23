// Host reload + long gap over the real iroh mesh. A hosts, B joins.
//   Phase 1 (quick reload): A reloads; meanwhile B adds a card and edits an
//     existing card. The reloaded A (same key, restored history, re-dials its
//     peer book; Hello on NeighborUp) must get both, live sync must resume, and
//     B's LWW event log must converge byte-for-byte on A (no re-stamp loss).
//   Phase 2 (long gap): A leaves the page entirely; B notices (status
//     "reconnecting"), makes gap edits, then A comes back on its link and must
//     pull them; B must reach "connected" again.
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
async function cardPath(pg, title) {
  return pg.evaluate(async (title) => {
    const r = globalThis.riftpipe;
    for (const id of await r.listDir("tickets")) {
      const p = `tickets/${id}/card.md`;
      if (((await r.readFile(p)) ?? "").startsWith(`# ${title}`)) return p;
    }
    return null;
  }, title);
}
const writeFileOn = (pg, p, c) => pg.evaluate(([p, c]) => globalThis.riftpipe.writeFile(p, c), [p, c]);
const readFileOn = (pg, p) => pg.evaluate((p) => globalThis.riftpipe.readFile(p), p);

const browser = await chromium.launch();
let code = 1;
try {
  const r = rnd();
  const a = await (await browser.newContext()).newPage();
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, null, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  await addCard(a, "seed-" + r);
  console.log(`A hosts (${ticket.length}c) + made seed-${r}`);

  const b = await join(browser, ticket, "B");
  await b.waitForSelector(`text=seed-${r}`, { timeout: 45000 });
  await waitStatus(a, "connected", 45e3);
  await waitStatus(b, "connected", 45e3);
  console.log("A and B connected and converged");

  // --- Phase 1: quick reload of the host ---
  await a.reload({ waitUntil: "load" });
  await addCard(b, "gap1-" + r);
  const p = await cardPath(b, "seed-" + r);
  if (!p) throw new Error("B can't find the seed card's path");
  await writeFileOn(b, p, `# seed-edited1-${r}\n`);
  console.log("A reloaded; B made gap edits (new card + edit of", p + ")");
  await a.waitForSelector(`text=gap1-${r}`, { timeout: 60000 });
  await a.waitForSelector(`text=seed-edited1-${r}`, { timeout: 60000 });
  await addCard(a, "after1-" + r);
  await b.waitForSelector(`text=after1-${r}`, { timeout: 45000 });
  const evPath = "events/" + (await b.evaluate(() => globalThis.riftpipe.deviceId())) + ".jsonl";
  {
    const t = Date.now();
    let ea = null, eb = null;
    while (Date.now() - t < 60e3) {
      ea = await readFileOn(a, evPath);
      eb = await readFileOn(b, evPath);
      if (ea !== null && ea === eb && ea.includes("gap1-")) break;
      await a.waitForTimeout(500);
    }
    if (!(ea !== null && ea === eb && ea.includes("gap1-"))) {
      throw new Error(`B's event log never converged on A (A ${ea?.length ?? "null"}B, B ${eb?.length ?? "null"}B)`);
    }
  }
  console.log("PHASE 1 OK");

  // --- Phase 2: long gap (the host's page is gone until B notices) ---
  await a.goto("about:blank");
  await waitStatus(b, "reconnecting", 75e3);
  console.log("B noticed the host is gone:", JSON.stringify(await status(b)));
  await addCard(b, "gap2-" + r);
  await writeFileOn(b, p, `# seed-edited2-${r}\n`);
  await a.goto(base + "#" + ticket, { waitUntil: "load" });
  await a.waitForSelector(`text=gap2-${r}`, { timeout: 90000 });
  await a.waitForSelector(`text=seed-edited2-${r}`, { timeout: 90000 });
  await waitStatus(b, "connected", 90e3);
  console.log("PHASE 2 OK");

  console.log("PASS: host reload + long gap — joiner's gap edits reach the returning host");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
