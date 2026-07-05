// Browser hosts a board over the iroh gossip mesh; the CLI joins the *share
// link* directly (`riftpipe connect <href> <dir>`) — ZERO signaling server.
//   browser→cli: a card created in the UI must land on the CLI peer's disk.
//   cli→browser: editing the synced card.md must update the browser UI.
import { chromium } from "playwright";
import { spawn } from "child_process";
import { readdirSync, existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";

const PORT = process.env.PORT || "8137";
const NDIR = process.env.NDIR;
const BIN = process.env.BIN || "./target/debug/riftpipe";
const TITLE = "from-browser-to-cli";
const url = `http://127.0.0.1:${PORT}/`; // no hash: this tab becomes the host

const browser = await chromium.launch();
let cli = null;
let code = 1;
try {
  const page = await browser.newPage();
  page.on("console", (m) => { if (m.type() === "error") console.log("page console:", m.text()); });
  page.on("pageerror", (e) => console.log("pageerror:", e.message));

  await page.goto(url, { waitUntil: "load" });
  await page.waitForSelector(".add-card input", { timeout: 20000 });

  // The host publishes its ticket into the URL hash once it has a relay address.
  await page.waitForFunction(() => location.hash.length > 1, null, { timeout: 60000 });
  const href = await page.evaluate(() => location.href);
  console.log("share link:", href.slice(0, 60) + "...");

  // The CLI joins the share link — no signaling, straight onto the gossip mesh.
  console.log(`spawning: ${BIN} connect <href> ${NDIR}`);
  cli = spawn(BIN, ["connect", href, NDIR], { stdio: ["ignore", "pipe", "pipe"] });
  let cliOut = "";
  cli.stdout.on("data", (d) => { cliOut += d; process.stdout.write("[cli] " + d); });
  cli.stderr.on("data", (d) => process.stdout.write("[cli!] " + d));
  cli.on("exit", (c) => console.log("[cli] exited:", c));

  // Give the CLI a moment to join the topic before the first edit.
  await sleep(5000);

  // --- browser -> cli ---
  console.log(`creating card "${TITLE}" in the browser...`);
  const input = page.locator(".add-card input").first();
  await input.fill(TITLE);
  await input.press("Enter");
  await page.waitForSelector(`text=${TITLE}`, { timeout: 10000 });

  let cardPath = null;
  for (let i = 0; i < 300; i++) { // 60s budget
    try {
      const tickets = readdirSync(join(NDIR, "tickets"));
      for (const t of tickets) {
        const p = join(NDIR, "tickets", t, "card.md");
        if (existsSync(p) && readFileSync(p, "utf8").includes(TITLE)) { cardPath = p; break; }
      }
      if (cardPath) break;
    } catch {}
    await sleep(200);
  }
  if (!cardPath) {
    console.log("cli stdout so far:", JSON.stringify(cliOut.slice(-500)));
    throw new Error("browser→cli: card.md never appeared on the CLI peer's disk");
  }
  console.log("browser→cli OK:", cardPath, JSON.stringify(readFileSync(cardPath, "utf8").trim()));

  // --- cli -> browser ---
  console.log("editing the synced card.md on the CLI side...");
  writeFileSync(cardPath, "# edited-by-cli\n");
  await page.waitForSelector("text=edited-by-cli", { timeout: 60000 });
  console.log("cli→browser OK: the CLI-side edit appeared in the browser UI");

  console.log("PASS: browser hosts, CLI joins the share link, bidirectional sync");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  if (cli) cli.kill();
  await browser.close();
}
process.exit(code);
