// Browser ↔ CLI request-to-join interop over the real sealed mesh, both
// directions:
//
//   Phase 1 — browser owner approves a CLI joiner:
//     A (browser) hosts + claims a board and writes a card. A keyless CLI peer
//     joins the share link with `connect … --restricted --name cli-e2e`, which
//     auto-broadcasts a signed join request. A's inbox shows it with the same
//     safety code the CLI printed; until A clicks Approve the CLI reads nothing.
//     After Approve the CLI reads A's card, and its edit reaches A.
//
//   Phase 2 — CLI owner approves a browser joiner:
//     A CLI founder hosts a `--restricted` board with a pre-written card. B
//     (browser) opens its ticket, drops to "no access", and clicks "Ask to join".
//     `riftpipe requests <dir> --json` lists B's request; `riftpipe approve <dir>
//     <safety-code>` grants it. B then reads the founder's card, and B's card
//     reaches the founder's disk.
//
// Run: see run-request-approve-browser.sh (env PORT, WORK, BIN).
import { chromium } from "playwright";
import { spawn, execFileSync } from "child_process";
import { readdirSync, existsSync, readFileSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";

const PORT = process.env.PORT || "8143";
const WORK = process.env.WORK;
const BIN = process.env.BIN || "./target/debug/riftpipe";
const base = `http://127.0.0.1:${PORT}/`;
const fail = (m) => { throw new Error(m); };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// Poll until the board on `page` contains a card titled `title`.
async function hasCard(page, title) {
  return page.evaluate(
    (t) => Array.from(document.querySelectorAll(".card .title")).some((e) => e.textContent?.trim() === t),
    title,
  );
}
async function waitCard(page, title, ms = 8000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (await hasCard(page, title)) return true; await wait(200); }
  return false;
}
async function addCard(page, title) {
  const input = page.locator(".add-card input").first();
  await input.fill(title);
  await input.press("Enter");
}
const authState = (page) => page.evaluate(() => window.riftpipe.authState());

// The path of the card.md under `dir/tickets/*/` whose text contains `text`, or null.
function cardWithText(dir, text) {
  let ids;
  try { ids = readdirSync(join(dir, "tickets")); } catch { return null; }
  for (const id of ids) {
    const p = join(dir, "tickets", id, "card.md");
    try { if (existsSync(p) && readFileSync(p, "utf8").includes(text)) return p; } catch {}
  }
  return null;
}
async function waitCardFile(dir, text, ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const p = cardWithText(dir, text); if (p) return p; await wait(250); }
  return null;
}

// A long-running `riftpipe connect …`, its stderr collected into `.err`.
function spawnCli(tag, args) {
  console.log(`spawning: riftpipe ${args.map((a) => (a.length > 60 ? a.slice(0, 40) + "…" : a)).join(" ")}`);
  const proc = spawn(BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
  const cli = { proc, err: "" };
  proc.stdout.on("data", (d) => process.stdout.write(`[${tag}] ` + d));
  proc.stderr.on("data", (d) => { cli.err += d; process.stdout.write(`[${tag}!] ` + d); });
  proc.on("exit", (c) => console.log(`[${tag}] exited:`, c));
  return cli;
}
// A one-shot CLI call's stdout (throws on a non-zero exit).
const run = (args) => execFileSync(BIN, args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
// `riftpipe requests|members <dir> --json`, or null while the peer hasn't written its state yet.
function cliJson(verb, dir) {
  try { return JSON.parse(run([verb, dir, "--json"])); } catch { return null; }
}
async function poll(fn, ms, every = 500) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const v = await fn(); if (v) return v; await wait(every); }
  return null;
}

const browser = await chromium.launch({ headless: true });
const clis = [];
let code = 1;
try {
  // ---------------- Phase 1: browser owner approves a CLI joiner ----------------
  console.log("== phase 1: browser owner approves a CLI joiner ==");
  const a = await (await browser.newContext()).newPage();
  a.on("pageerror", (e) => console.log("A pageerror:", e.message));
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, null, { timeout: 60000 });
  await a.locator(".lock-btn").click();
  if (!(await poll(async () => (await authState(a)).isAuthor, 10000, 250))) fail("A should become the author after Claim");
  const aDevice = (await authState(a)).device;
  await addCard(a, "owner-secret");
  await a.waitForSelector("text=owner-secret", { timeout: 10000 });
  const href = await a.evaluate(() => location.href);
  console.log(`✓ A hosts + claimed (device ${aDevice.slice(0, 10)}…) and wrote owner-secret`);

  const J = join(WORK, "j1");
  mkdirSync(J, { recursive: true });
  const jId = run(["device-id", J]).trim();
  const joiner = spawnCli("cli-j", ["connect", href, J, "--restricted", "--name", "cli-e2e"]);
  clis.push(joiner);

  const codeRe = /join request name=".*?" safety=([0-9a-f]{4} [0-9a-f]{4})/;
  const joinCode = await poll(() => joiner.err.match(codeRe)?.[1], 20000, 200);
  if (!joinCode) fail("the CLI joiner never printed its join-request safety code");
  console.log(`✓ CLI joiner ${jId.slice(0, 10)}… asked with safety code ${joinCode}`);

  const row = a.locator(".req", { hasText: "cli-e2e" });
  await row.waitFor({ timeout: 30000 });
  const rowText = await row.innerText();
  if (!rowText.includes(joinCode)) fail(`A's request row should show safety code ${joinCode}, got ${JSON.stringify(rowText)}`);
  const pending = await a.evaluate(() => window.riftpipe.pendingRequests());
  if (!pending.some((r) => r.device === jId)) fail(`A's pendingRequests() should list ${jId}, got ${JSON.stringify(pending)}`);
  console.log("✓ A's inbox shows the CLI's named request with the matching safety code");

  await wait(3000);
  if (cardWithText(J, "owner-secret")) fail("an unapproved CLI joiner read the owner's card");
  console.log("✓ before approval: the CLI joiner can't read the owner's card");

  await row.locator(".approve-btn").click();
  const jCard = await waitCardFile(J, "owner-secret", 60000);
  if (!jCard) fail("approved: the CLI joiner never read the owner's card");
  console.log("✓ approved: the CLI joiner reads the owner's card:", jCard);

  writeFileSync(jCard, "# edited-by-cli\n");
  await a.waitForSelector("text=edited-by-cli", { timeout: 60000 });
  console.log("✓ the approved CLI joiner's edit reached the browser owner");

  // The joiner rewrites its state file as the grant lands; poll briefly for it.
  const listsJoinerAndOwner = (ms) =>
    ms.some((m) => m.device === jId && m.role === "member" && m.me) && ms.some((m) => m.device === aDevice && m.founder);
  const jMembers = (await poll(() => { const m = cliJson("members", J) ?? []; return listsJoinerAndOwner(m) ? m : null; }, 5000))
    ?? cliJson("members", J) ?? [];
  if (!jMembers.some((m) => m.device === jId && m.role === "member" && m.me))
    fail(`CLI members should list itself as a member, got ${JSON.stringify(jMembers)}`);
  if (!jMembers.some((m) => m.device === aDevice && m.founder))
    fail(`CLI members should list the browser owner as founder, got ${JSON.stringify(jMembers)}`);
  console.log("✓ CLI `members` lists itself (member) and the browser owner (founder)");
  joiner.proc.kill();

  // ---------------- Phase 2: CLI owner approves a browser joiner ----------------
  console.log("== phase 2: CLI owner approves a browser joiner ==");
  const F = join(WORK, "f2");
  mkdirSync(join(F, "tickets", "tk_clifounder"), { recursive: true });
  writeFileSync(join(F, "tickets", "tk_clifounder", "card.md"), "# from-cli-founder\n");
  writeFileSync(join(F, "tickets", "tk_clifounder", "meta.toml"), 'column = "Todo"\nposition = 0\ndone = false\n');
  const founder = spawnCli("cli-f", ["connect", "--accept", F, "--restricted"]);
  clis.push(founder);
  const ticket = await poll(() => { try { return readFileSync(`${F}.ticket`, "utf8").trim(); } catch { return null; } }, 30000, 300);
  if (!ticket) fail("the CLI founder never wrote its ticket");
  console.log(`✓ CLI founder hosts a restricted board (${ticket.length}-char ticket)`);

  const b = await (await browser.newContext()).newPage();
  b.on("pageerror", (e) => console.log("B pageerror:", e.message));
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  await b.waitForSelector(".auth-status.no-access", { timeout: 40000 });
  await wait(3000);
  if (await hasCard(b, "from-cli-founder")) fail("an unapproved browser joiner read the CLI founder's card");
  console.log("✓ B joined the CLI board with no access and can't read its card");

  await b.locator(".ask-btn").click();
  const bId = (await authState(b)).device;
  const bReq = await poll(() => (cliJson("requests", F) ?? []).find((r) => r.device === bId), 30000);
  if (!bReq) fail(`the CLI founder's requests never listed B (${bId})`);
  console.log(`✓ CLI \`requests\` lists B's request (safety ${bReq.safety})`);

  console.log(run(["approve", F, bReq.safety]).trim());
  await b.waitForSelector(".auth-status.editor", { timeout: 30000 });
  if (!(await waitCard(b, "from-cli-founder", 30000))) fail("approved: B never read the CLI founder's card");
  console.log("✓ approved: B can edit and reads the CLI founder's card");

  await addCard(b, "from-browser-joiner");
  if (!(await waitCardFile(F, "from-browser-joiner", 60000))) fail("approved: B's card never reached the CLI founder's disk");
  console.log("✓ the approved browser joiner's card reached the CLI founder");

  const fRequests = await poll(() => { const r = cliJson("requests", F); return r && r.length === 0 ? r : null; }, 10000);
  if (!fRequests) fail(`the CLI founder's requests should be empty after approve, got ${JSON.stringify(cliJson("requests", F))}`);
  const fMembers = cliJson("members", F) ?? [];
  if (!fMembers.some((m) => m.device === bId && m.role === "member"))
    fail(`CLI founder members should list B as a member, got ${JSON.stringify(fMembers)}`);
  console.log("✓ CLI founder: no pending requests, B listed as a member");

  console.log("\nPASS — CLI↔browser request/approve interop, both directions");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
} finally {
  for (const c of clis) c.proc.kill();
  await browser.close();
}
process.exit(code);
