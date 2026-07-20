// Open two REAL, interactive browser windows already synced over the sealed
// mesh — the first hosts, the second joins its share link. Leaves them open so
// you can add/move cards and watch them converge. Ctrl-C (or kill the process)
// to close. Not a test; a convenience for poking at the running app.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8140";
const base = `http://127.0.0.1:${PORT}/`;

// Headed, windowed, no automation-y flags — real windows you can click in.
const browser = await chromium.launch({ headless: false, args: ["--window-size=900,720"] });

const host = await (await browser.newContext({ viewport: null })).newPage();
await host.goto(base, { waitUntil: "load" });
await host.waitForSelector(".add-card input", { timeout: 20000 });
await host.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
const ticket = await host.evaluate(() => location.hash.slice(1));
console.log(`window 1 hosting; sealed ticket ${ticket.length} chars`);

const joiner = await (await browser.newContext({ viewport: null })).newPage();
await joiner.goto(base + "#" + ticket, { waitUntil: "load" });
await joiner.waitForSelector(".add-card input", { timeout: 20000 });
console.log("window 2 joined via the share link");
console.log("\nTwo synced windows are open. Add a card in either — it appears in both.");
console.log("Leave this process running; kill it (or close the windows) when done.\n");

// Stay alive so the windows remain open and interactive.
await new Promise(() => {});
