// N-browser gossip mesh (default 5): A hosts, the rest join A's link — NO
// reload crutch (a join must be clean on first load). Two phases:
//   1. staged joins — each peer adds a card right after joining; everyone
//      must converge (late joiners get history via catch-up).
//   2. LIVE — after ALL peers are in, every peer adds a second card; everyone
//      must see everything WITHOUT any reload. (The reported bug: a third
//      peer needed a refresh to sync.)
// On failure, each missing card is diagnosed: present in the page's OPFS
// (via kanbanHandle) but not the DOM = stale-UI bug; absent from OPFS = sync
// bug. Routing map + connected peers are dumped either way.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8129";
const N = parseInt(process.env.PEERS || "5", 10);
const base = `http://127.0.0.1:${PORT}/`;
const rnd = () => Math.random().toString(16).slice(2, 7);
const names = Array.from({ length: N }, (_, i) => String.fromCharCode(65 + i));

async function addCard(pg, title) {
  const inp = pg.locator(".add-card input").first();
  await inp.fill(title);
  await inp.press("Enter");
  await pg.waitForSelector(`text=${title}`, { timeout: 15000 });
}

// Board titles as the page's wasm handler sees them (OPFS truth, not DOM).
async function opfsTitles(pg) {
  return await pg.evaluate(async () => {
    const r = await globalThis.riftpipe.kanbanHandle("GET", "/api/board", "");
    return JSON.parse(r.body ?? r).cards.map((c) => c.title);
  }).catch(() => null);
}

async function diagnose(name, pg, missing) {
  const opfs = await opfsTitles(pg);
  const inOpfs = opfs ? opfs.includes(missing) : "unknown";
  const peers = await pg.evaluate(() => globalThis.riftpipe.connectedPeers()).catch(() => "n/a");
  console.log(
    `DIAG ${name}: missing "${missing}" in DOM · in OPFS: ${inOpfs} ` +
    `(${inOpfs === true ? "STALE-UI BUG" : inOpfs === false ? "SYNC BUG" : "?"}) · ` +
    `connectedPeers: ${JSON.stringify(peers)}`,
  );
}

async function expectAll(pages, titles) {
  let failed = 0;
  for (const [name, pg] of pages) {
    for (const t of titles) {
      try {
        await pg.waitForSelector(`text=${t}`, { timeout: 60000 });
      } catch {
        failed++;
        await diagnose(name, pg, t);
      }
    }
    if (!failed) console.log(`${name} sees all ${titles.length} cards`);
  }
  return failed;
}

const browser = await chromium.launch();
let code = 1;
try {
  // A hosts.
  const a = await (await browser.newContext()).newPage();
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  const pages = [["A", a]];
  const round1 = ["A-" + rnd()];
  await addCard(a, round1[0]);
  console.log(`A hosts (ticket ${ticket.length}c) + made ${round1[0]}`);

  // Peers join sequentially — first load only, NO reload.
  for (let i = 1; i < N; i++) {
    const name = names[i];
    const pg = await (await browser.newContext()).newPage();
    pg.on("pageerror", (e) => console.log(`${name} pageerror:`, e.message));
    await pg.goto(base + "#" + ticket, { waitUntil: "load" });
    await pg.waitForSelector(".add-card input", { timeout: 20000 });
    await pg.waitForTimeout(2500); // let the swarm admit the newcomer
    pages.push([name, pg]);
    const t = `${name}-` + rnd();
    round1.push(t);
    await addCard(pg, t);
    console.log(`${name} joined + made ${t}`);
  }

  console.log(`== phase 1: ${N} peers, staged-join convergence ==`);
  let failures = await expectAll(pages, round1);

  console.log("== phase 2: LIVE edits after all peers joined ==");
  const round2 = [];
  for (const [name, pg] of pages) {
    const t = `${name}-live-` + rnd();
    round2.push(t);
    await addCard(pg, t);
  }
  failures += await expectAll(pages, round2);

  console.log("== phase 3: solo tab pastes the share link (hashchange join, no reload) ==");
  const solo = await (await browser.newContext()).newPage();
  solo.on("pageerror", (e) => console.log("SOLO pageerror:", e.message));
  await solo.goto(base, { waitUntil: "load" }); // no hash → it hosts its own board
  await solo.waitForSelector(".add-card input", { timeout: 20000 });
  await solo.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  await solo.waitForTimeout(1500); // established solo host
  // Simulate pasting A's link into the address bar of the open tab.
  await solo.evaluate((t) => { location.hash = t; }, ticket);
  await solo.waitForTimeout(4000); // let the hashchange reconnect settle
  const soloCard = "SOLO-live-" + rnd();
  const t2 = `A-after-solo-` + rnd();
  await addCard(a, t2); // A edits after the paste — does the solo tab get it?
  let soloFailures = 0;
  try {
    await solo.waitForSelector(`text=${t2}`, { timeout: 60000 });
    console.log("solo tab received A's post-paste card (join worked, no reload)");
  } catch {
    soloFailures++;
    await diagnose("SOLO", solo, t2);
  }
  try {
    await addCard(solo, soloCard); // and does its own edit reach A?
    await a.waitForSelector(`text=${soloCard}`, { timeout: 60000 });
    console.log("A received the solo tab's card");
  } catch {
    soloFailures++;
    await diagnose("A", a, soloCard);
  }
  failures += soloFailures;

  const rm = await a.evaluate(() => globalThis.riftpipe.routingMap()).catch(() => null);
  console.log("routing map (A's view):", JSON.stringify(rm));

  if (failures === 0) {
    console.log(`PASS: ${N}-peer mesh — staged joins AND live edits all converge, zero reloads`);
    code = 0;
  } else {
    console.log(`FAIL: ${failures} missing card sightings (see DIAG lines)`);
  }
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
