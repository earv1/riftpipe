// Capture screenshots of the auth bar in its four states for visual evidence.
import { chromium } from "playwright";
const PORT = process.env.PORT || "8140";
const base = `http://127.0.0.1:${PORT}/`;
const out = process.env.OUT || "/tmp";
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const browser = await chromium.launch({ headless: true });
const a = await (await browser.newContext({ viewport: { width: 1100, height: 720 } })).newPage();
await a.goto(base, { waitUntil: "load" });
await a.waitForSelector(".add-card input", { timeout: 20000 });
await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
const ticket = await a.evaluate(() => location.hash.slice(1));

const b = await (await browser.newContext({ viewport: { width: 1100, height: 720 } })).newPage();
await b.goto(base + "#" + ticket, { waitUntil: "load" });
await b.waitForSelector(".add-card input", { timeout: 20000 });
await wait(1500);

await a.locator(".auth-bar").screenshot({ path: `${out}/auth-unlocked.png` });
await a.locator(".lock-btn").click();
await wait(2500);
await a.locator(".auth-bar").screenshot({ path: `${out}/auth-owner-empty.png` });
await b.locator(".auth-bar").screenshot({ path: `${out}/auth-viewer.png` });
// B asks; the request lands in A's inbox.
await b.locator(".ask-btn").click();
await wait(2000);
await b.locator(".auth-bar").screenshot({ path: `${out}/auth-viewer-waiting.png` });
await a.locator(".req").first().waitFor({ timeout: 8000 });
await a.locator(".auth-bar").screenshot({ path: `${out}/auth-owner-inbox.png` });
await a.locator(".approve-btn").first().click();
await wait(2500);
await b.locator(".auth-bar").screenshot({ path: `${out}/auth-editor.png` });
console.log("wrote auth-unlocked/owner-empty/viewer/viewer-waiting/owner-inbox/editor.png to", out);
await browser.close();
