// Two real browsers collaborate on a kanban board over **iroh** — no signaling
// server, no host you run (traffic rides n0's public relays). Browser A opens the
// app fresh and becomes the host (its ticket lands in the URL hash); browser B
// opens that #ticket link and joins. A card created in A must appear in B.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8127";
const base = `http://127.0.0.1:${PORT}/`;

const browser = await chromium.launch();
let code = 1;
try {
  // A: host. Opening with no hash makes this tab the host; its ticket is written
  // into location.hash for sharing.
  const a = await (await browser.newContext()).newPage();
  a.on("console", (m) => { if (m.type() === "error") console.log("A console:", m.text()); });
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  console.log("A loaded; waiting for it to host (ticket in URL)...");
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  console.log(`host ticket acquired (${ticket.length} chars)`);

  // B: joins via the ticket link.
  const b = await (await browser.newContext()).newPage();
  b.on("console", (m) => { if (m.type() === "error") console.log("B console:", m.text()); });
  b.on("pageerror", (e) => console.log("B pageerror:", e.message));
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  console.log("B loaded; letting the relay connection establish...");
  await b.waitForTimeout(6000);

  // A creates a card → B must show it (synced over the iroh relay).
  const title = "iroh-" + Math.random().toString(16).slice(2, 8);
  const input = a.locator(".add-card input").first();
  await input.fill(title);
  await input.press("Enter");
  await a.waitForSelector(`text=${title}`, { timeout: 10000 });
  console.log(`A created "${title}"`);

  await b.waitForSelector(`text=${title}`, { timeout: 30000 });
  console.log("PASS: card synced A→B over iroh's relay — no signaling server, no host");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
