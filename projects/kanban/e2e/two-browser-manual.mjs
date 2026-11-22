// Manual-style live test: two async Playwright windows collaborate on a kanban
// board over the SEALED iroh-gossip mesh (protocol v3: topic = H(secret),
// AEAD-sealed frames — no signaling server, no host you run). Drives it
// bidirectionally, inspects the runtime security state, and screenshots both
// windows so you can see it working.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8127";
const base = `http://127.0.0.1:${PORT}/`;
const SHOT = process.env.SHOT_DIR || "/tmp/riftpipe-shots";
import { mkdirSync } from "node:fs";
mkdirSync(SHOT, { recursive: true });

const log = (...a) => console.log(...a);
const browser = await chromium.launch();
let code = 1;
try {
  // ── Window A: opens fresh → becomes the host; its sealed ticket lands in the hash.
  const a = await (await browser.newContext()).newPage();
  a.on("pageerror", (e) => log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  log("A: loaded, hosting…");
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  log(`A: sealed share ticket acquired (${ticket.length} hex chars = 32-byte secret + host addr)`);

  // Best-effort: probe the runtime security surface the lib exposes (wasm
  // exports may be under window.riftpipe, or a global). Never fails the test.
  const probe = async (page, tag) => {
    try {
      const r = await page.evaluate(() => {
        const g = globalThis;
        const rp = g.riftpipe || g;
        const call = (n) => (typeof rp[n] === "function" ? rp[n]() : null);
        return { proto: call("protocolStatus"), peers: call("connectedPeers") };
      });
      log(`${tag}: protocolStatus=${r.proto ?? "(n/a)"} peers=${r.peers ?? "(n/a)"}`);
    } catch { /* accessor not global — the sealed ticket + sync are the real proof */ }
  };
  await probe(a, "A");

  // ── Window B: opens the #ticket link → derives topic = H(secret), joins the sealed mesh.
  const b = await (await browser.newContext()).newPage();
  b.on("pageerror", (e) => log("B pageerror:", e.message));
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  log("B: loaded from the share link; letting the relay connection form…");
  await b.waitForTimeout(6000);

  await probe(b, "B");

  // ── A → B: create a card in A, expect it in B (synced over the sealed mesh).
  const t1 = "from-A-" + Math.random().toString(16).slice(2, 8);
  const ai = a.locator(".add-card input").first();
  await ai.fill(t1);
  await ai.press("Enter");
  await a.waitForSelector(`text=${t1}`, { timeout: 10000 });
  await b.waitForSelector(`text=${t1}`, { timeout: 30000 });
  log(`A→B OK: "${t1}" created in A appeared in B`);

  // ── B → A: the reverse direction (N-writer mesh, both can edit).
  const t2 = "from-B-" + Math.random().toString(16).slice(2, 8);
  const bi = b.locator(".add-card input").first();
  await bi.fill(t2);
  await bi.press("Enter");
  await b.waitForSelector(`text=${t2}`, { timeout: 10000 });
  await a.waitForSelector(`text=${t2}`, { timeout: 30000 });
  log(`B→A OK: "${t2}" created in B appeared in A`);

  // Both windows now show both cards — capture them.
  await a.screenshot({ path: `${SHOT}/window-A.png`, fullPage: true });
  await b.screenshot({ path: `${SHOT}/window-B.png`, fullPage: true });
  log(`screenshots: ${SHOT}/window-A.png , ${SHOT}/window-B.png`);

  log("PASS: two browsers sync bidirectionally over the sealed (v3) iroh mesh");
  code = 0;
} catch (e) {
  log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
