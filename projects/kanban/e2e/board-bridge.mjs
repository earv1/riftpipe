// Bidirectional browser↔native board collaboration. A browser runs the kanban app
// at a connection-id link; a native peer (`riftpipe kanban connect`) joins the same
// room, syncing into NDIR.
//   browser→native: a card created in the UI must land on the native disk.
//   native→browser: editing the native card.md must update the browser UI.
import { chromium } from "playwright";
import { readdirSync, existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";

const PORT = process.env.PORT || "8125";
const ROOM = process.env.ROOM;
const SIGNAL_URL = process.env.SIGNAL_URL || "ws://127.0.0.1:9021";
const NDIR = process.env.NDIR;
const TITLE = "from-browser-to-native";
const url = `http://127.0.0.1:${PORT}/?signal=${encodeURIComponent(SIGNAL_URL)}#${ROOM}`;

const browser = await chromium.launch();
let code = 1;
try {
  const page = await browser.newPage();
  page.on("console", (m) => { if (m.type() === "error") console.log("page console:", m.text()); });
  page.on("pageerror", (e) => console.log("pageerror:", e.message));

  await page.goto(url, { waitUntil: "load" });
  await page.waitForSelector(".add-card input", { timeout: 20000 });
  await page.waitForTimeout(4000); // connectPeer handshake with the native peer

  // --- browser -> native ---
  console.log(`creating card "${TITLE}" in the browser...`);
  const input = page.locator(".add-card input").first();
  await input.fill(TITLE);
  await input.press("Enter");
  await page.waitForSelector(`text=${TITLE}`, { timeout: 10000 });

  let cardPath = null;
  for (let i = 0; i < 60; i++) {
    try {
      const tickets = readdirSync(join(NDIR, "tickets"));
      if (tickets.length) {
        const p = join(NDIR, "tickets", tickets[0], "card.md");
        if (existsSync(p)) { cardPath = p; break; }
      }
    } catch {}
    await sleep(200);
  }
  if (!cardPath) throw new Error("browser→native: card.md never appeared on native disk");
  console.log("browser→native OK:", JSON.stringify(readFileSync(cardPath, "utf8").trim()));

  // --- native -> browser ---
  console.log("editing the native card.md (any editor would do)...");
  writeFileSync(cardPath, "# edited-by-native\n");
  await page.waitForSelector("text=edited-by-native", { timeout: 25000 });
  console.log("native→browser OK: the native edit appeared in the browser UI");

  console.log("PASS: bidirectional browser↔native board collaboration");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
