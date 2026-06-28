// Browser→native board sync: a browser runs the kanban app at a connection-id
// link; a native peer (`riftpipe kanban connect`) joins the same room. A card
// created through the browser UI must land on the native peer's disk. The browser
// half is here; run-board-bridge.sh runs the native receiver and checks the files.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8125";
const ROOM = process.env.ROOM;
const SIGNAL_URL = process.env.SIGNAL_URL || "ws://127.0.0.1:9021";
const TITLE = process.env.CARD_TITLE || "from-browser-to-native";
const url = `http://127.0.0.1:${PORT}/?signal=${encodeURIComponent(SIGNAL_URL)}#${ROOM}`;

const browser = await chromium.launch();
let code = 1;
try {
  const page = await browser.newPage();
  page.on("console", (m) => { if (m.type() === "error") console.log("page console:", m.text()); });
  page.on("pageerror", (e) => console.log("pageerror:", e.message));

  console.log(`loading kanban app ${url}`);
  await page.goto(url, { waitUntil: "load" });
  await page.waitForSelector(".add-card input", { timeout: 20000 });
  // Let connectPeer finish the WebRTC handshake with the native peer.
  await page.waitForTimeout(4000);

  console.log(`creating card "${TITLE}" in the browser...`);
  const input = page.locator(".add-card input").first();
  await input.fill(TITLE);
  await input.press("Enter");
  await page.waitForSelector(`text=${TITLE}`, { timeout: 10000 });
  console.log("browser shows the card locally");

  // Give the push time to reach the native peer + be written to disk.
  await page.waitForTimeout(2500);
  console.log("browser side done (run-board-bridge.sh verifies the native disk)");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
