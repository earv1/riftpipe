// The CLI hosts a seeded board (`connect --accept NDIR`); a browser joins its
// ticket. The CLI is killed, the browser edits during the gap (a new card + an
// edit of the seed card), and the CLI restarts on the same dir. It must come
// back as the SAME board (same EndpointId, same secret), pull the browser's gap
// edits onto disk, and live sync must resume (a CLI-side edit reaches the UI).
import { chromium } from "playwright";
import { spawn } from "child_process";
import { readdirSync, existsSync, readFileSync, writeFileSync, mkdirSync, realpathSync, rmSync } from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";

const PORT = process.env.PORT || "8153";
const NDIR = process.env.NDIR;
const BIN = process.env.BIN || "./target/debug/riftpipe";
const base = `http://127.0.0.1:${PORT}/`;
const rnd = () => Math.random().toString(16).slice(2, 7);

async function addCard(pg, title) {
  const inp = pg.locator(".add-card input").first();
  await inp.fill(title);
  await inp.press("Enter");
  await pg.waitForSelector(`text=${title}`, { timeout: 10000 });
}
const status = (pg) => pg.evaluate(() => globalThis.riftpipe.meshStatus());
async function waitStatus(pg, state, ms) {
  const t = Date.now();
  while (Date.now() - t < ms) {
    if ((await status(pg)).state === state) return;
    await pg.waitForTimeout(500);
  }
  throw new Error(`status never became ${state} (last ${JSON.stringify(await status(pg))})`);
}
const writeFileOn = (pg, p, c) => pg.evaluate(([p, c]) => globalThis.riftpipe.writeFile(p, c), [p, c]);

// The CLI canonicalizes its dir (macOS: /var → /private/var) and writes the
// ticket sidecar next to the canonical path.
const ticketFile = realpathSync(NDIR) + ".ticket";
let cli = null;

/** Start `connect --accept NDIR`; resolve `{ id, ticket }` once it's hosting. */
async function startCli(tag) {
  rmSync(ticketFile, { force: true });
  cli = spawn(BIN, ["connect", "--accept", NDIR], { stdio: ["ignore", "pipe", "pipe"] });
  let err = "";
  cli.stdout.on("data", (d) => process.stdout.write(`[cli${tag}] ` + d));
  cli.stderr.on("data", (d) => { err += d; process.stdout.write(`[cli${tag}!] ` + d); });
  cli.on("exit", (c, s) => console.log(`[cli${tag}] exited:`, c ?? s));
  let id = null;
  for (let i = 0; i < 300 && !id; i++) { // 60s
    const m = err.match(/\[mesh\] host id (\S+)/);
    if (m) id = m[1];
    else await sleep(200);
  }
  if (!id) throw new Error(`CLI ${tag} never printed its host id`);
  let ticket = "";
  for (let i = 0; i < 150 && !ticket; i++) { // 30s
    if (existsSync(ticketFile)) ticket = readFileSync(ticketFile, "utf8").trim();
    if (!ticket) await sleep(200);
  }
  if (!ticket) throw new Error(`CLI ${tag} never wrote ${ticketFile}`);
  return { id, ticket };
}

const seedCard = join(NDIR, "tickets", "tk_seed0001", "card.md");
function gapCardOnDisk(title) {
  try {
    for (const t of readdirSync(join(NDIR, "tickets"))) {
      const p = join(NDIR, "tickets", t, "card.md");
      if (existsSync(p) && readFileSync(p, "utf8").includes(title)) return true;
    }
  } catch {}
  return false;
}

const browser = await chromium.launch();
let code = 1;
try {
  const r = rnd();
  mkdirSync(join(NDIR, "tickets", "tk_seed0001"), { recursive: true });
  writeFileSync(join(NDIR, "board.md"), "# CLI Board\n\n- Todo\n- Doing\n- Done\n");
  writeFileSync(seedCard, "# seed-card\n");
  writeFileSync(join(NDIR, "tickets", "tk_seed0001", "meta.toml"), 'column = "Todo"\nposition = 1\ndone = false\n');

  // Run 1: the CLI hosts; the browser joins its ticket.
  const run1 = await startCli("1");
  console.log("CLI run 1 hosts as", run1.id);
  const page = await browser.newPage();
  page.on("pageerror", (e) => console.log("pageerror:", e.message));
  await page.goto(base + "#" + run1.ticket, { waitUntil: "load" });
  await page.waitForSelector("text=seed-card", { timeout: 60000 });
  await waitStatus(page, "connected", 60e3);
  console.log("browser joined the CLI board and sees the seed card");

  // Kill the host; the browser edits during the gap.
  const exited = new Promise((res) => cli.once("exit", res));
  cli.kill("SIGTERM");
  await exited;
  cli = null;
  await sleep(3000);
  const gap = "gap-card-" + r;
  await addCard(page, gap);
  await writeFileOn(page, "tickets/tk_seed0001/card.md", "# seed-edited-in-gap\n");
  console.log("CLI stopped; browser made gap edits");

  // Run 2: same dir ⇒ same identity + board secret.
  const run2 = await startCli("2");
  if (run1.id !== run2.id) throw new Error(`restarted CLI has a different EndpointId (${run1.id} vs ${run2.id})`);
  if (run1.ticket.slice(0, 64) !== run2.ticket.slice(0, 64)) throw new Error("restarted CLI has a different board secret");
  console.log("CLI run 2: same EndpointId, same board secret");

  let landed = false;
  for (let i = 0; i < 450 && !landed; i++) { // 90s
    landed = gapCardOnDisk(gap) && existsSync(seedCard) && readFileSync(seedCard, "utf8") === "# seed-edited-in-gap\n";
    if (!landed) await sleep(200);
  }
  if (!landed) {
    throw new Error(`restarted CLI never pulled the browser's gap edits (seed card: ${JSON.stringify(readFileSync(seedCard, "utf8"))}, gap card on disk: ${gapCardOnDisk(gap)})`);
  }
  console.log("browser gap edits landed on the restarted CLI's disk");

  writeFileSync(seedCard, "# edited-by-cli-after-restart\n");
  await page.waitForSelector("text=edited-by-cli-after-restart", { timeout: 60000 });
  console.log("live sync resumed: a CLI-side edit reached the browser");

  console.log("PASS: CLI host restart (same identity) — browser gap edits land, live sync resumes");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  if (cli) cli.kill();
  await browser.close();
}
process.exit(code);
