// Two-real-browser end-to-end check of the serverless kanban: two isolated
// browser contexts (independent OPFS) load the static bundle at the same
// connection-id link, connect peer-to-peer over WebRTC (brokered by the local
// signaling server), and a card created in one must appear in the other.
//
// Run via run.sh, which builds the bundle and starts the static server + signal.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8123";
const ROOM = "pw-" + Math.random().toString(16).slice(2, 10);
// 127.0.0.1 (not localhost) so the page's signalUrl (ws://<hostname>:9000) is IPv4,
// matching the IPv4-bound signal server — localhost may resolve to ::1.
const url = `http://127.0.0.1:${PORT}/#${ROOM}`;

const browser = await chromium.launch();
let code = 1;
try {
  const ctxA = await browser.newContext();
  const ctxB = await browser.newContext();
  const a = await ctxA.newPage();
  const b = await ctxB.newPage();
  for (const [name, p] of [["A", a], ["B", b]]) {
    p.on("console", (m) => { if (m.type() === "error") console.log(`${name} console:`, m.text()); });
    p.on("pageerror", (e) => console.log(`${name} pageerror:`, e.message));
  }

  console.log(`loading ${url} in two isolated contexts...`);
  await a.goto(url, { waitUntil: "load" });
  await b.goto(url, { waitUntil: "load" });

  // Both boards render (their add-card inputs exist).
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await b.waitForSelector(".add-card input", { timeout: 20000 });

  // Let the WebRTC handshake complete (both pages call connectPeer on mount).
  await a.waitForTimeout(3500);

  // Create a card in A.
  const title = `synced-${ROOM}`;
  console.log(`creating card "${title}" in A...`);
  const input = a.locator(".add-card input").first();
  await input.fill(title);
  await input.press("Enter");

  // A shows it locally (local-change refresh)...
  await a.waitForSelector(`text=${title}`, { timeout: 10000 });
  console.log("A shows the card locally");

  // ...and B receives it over the link (the real test).
  await b.waitForSelector(`text=${title}`, { timeout: 25000 });
  console.log("PASS: card created in A appeared in B over WebRTC (no server in the data path)");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
