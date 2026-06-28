// Cross-stack bridge check: a real browser (web-sys WebRTC) connects to a native
// peer (webrtc-rs, started by run-bridge.sh) through the signaling server, and
// they exchange a message over WebRTC. Asserts the browser received the native
// peer's message; run-bridge.sh asserts the native side received the browser's.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8124";
const ROOM = process.env.ROOM;
const SIGNAL_URL = process.env.SIGNAL_URL || "ws://127.0.0.1:9020";
// 127.0.0.1 (not localhost) so the browser and the IPv4-bound signal server agree.
const url = `http://127.0.0.1:${PORT}/projects/kanban/e2e/bridge.html?signal=${encodeURIComponent(SIGNAL_URL)}#${ROOM}`;

const browser = await chromium.launch();
let code = 1;
try {
  const page = await browser.newPage();
  page.on("console", (m) => { if (m.type() === "error") console.log("page console:", m.text()); });
  page.on("pageerror", (e) => console.log("pageerror:", e.message));

  console.log(`loading ${url}`);
  await page.goto(url, { waitUntil: "load" });
  await page.waitForFunction(
    () => {
      const t = document.getElementById("result")?.textContent || "";
      return t.startsWith("GOT:") || t.startsWith("ERR:");
    },
    { timeout: 30000 },
  );
  const text = await page.locator("#result").textContent();
  console.log("browser result:", text);
  if (text === "GOT:hello-from-native") {
    console.log("PASS: browser (web-sys) received the native (webrtc-rs) peer's message");
    code = 0;
  } else {
    console.log("FAIL: unexpected browser result");
  }
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
