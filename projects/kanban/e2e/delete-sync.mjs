// Tree/mesh deletes over the REAL iroh mesh, three peers: browser A (host),
// browser B (joins the ticket) and the CLI C (`riftpipe connect <href> <dir>`).
//   1. B deletes a comment → it leaves A's board and C's disk.
//   2. B goes offline (its OPFS keeps a stale copy of the card); A deletes the
//      card → C's tickets/<id>/card.md + meta.toml go away (three-peer convergence).
//   3. B comes back as a late joiner with that stale copy: the card is removed
//      there too and never comes back anywhere.
//   4. The CLI deletes a card directory on disk → both browsers drop the card.
//   5. Leftovers: a ticket dir with files but no card.md (a deleted card whose meta a
//      concurrent edit kept) shows as a leftover; "Remove leftovers" clears it everywhere.
import { chromium } from "playwright";
import { spawn } from "child_process";
import { readdirSync, existsSync, readFileSync, rmSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";

const PORT = process.env.PORT || "8155";
const NDIR = process.env.NDIR;
const BIN = process.env.BIN || "./target/debug/riftpipe";
const base = `http://127.0.0.1:${PORT}/`;
const rnd = Math.random().toString(16).slice(2, 7);
const DOOMED = `doomed-${rnd}`;
const KEPT = `kept-${rnd}`;
const COMMENT = `bye-${rnd}`;

const dialogs = [];
function wirePage(pg, tag) {
  pg.on("dialog", (d) => { dialogs.push(`${tag}:${d.type()}:${d.message()}`); d.accept(); });
  pg.on("pageerror", (e) => console.log(`[${tag}] pageerror:`, e.message));
  pg.on("console", (m) => { if (m.type() === "error") console.log(`[${tag}] console:`, m.text()); });
  return pg;
}
async function addCard(pg, title) {
  const inp = pg.locator(".add-card input").first();
  await inp.fill(title);
  await inp.press("Enter");
  await pg.waitForSelector(`.card .title >> text=${title}`, { timeout: 10000 });
}
const waitCard = (pg, title, ms = 60000) =>
  pg.waitForSelector(`.card .title >> text=${title}`, { timeout: ms });
/** Wait until no card on `pg` is titled `title`. */
const waitGone = (pg, title, ms = 60000) =>
  pg.waitForFunction(
    (t) => !Array.from(document.querySelectorAll(".card .title")).some((e) => e.textContent?.trim() === t),
    title,
    { timeout: ms, polling: 200 },
  );
const hasCard = (pg, title) =>
  pg.evaluate((t) => Array.from(document.querySelectorAll(".card .title")).some((e) => e.textContent?.trim() === t), title);
/** Poll `fn` every 200 ms until it is truthy; throw `what` after `ms`. */
async function waitPath(fn, what, ms = 60000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (fn()) return; await sleep(200); }
  throw new Error(what);
}
/** The CLI-side card id whose card.md is titled `title`, or null. */
function cardIdByTitle(title) {
  try {
    for (const t of readdirSync(join(NDIR, "tickets"))) {
      const p = join(NDIR, "tickets", t, "card.md");
      if (existsSync(p) && readFileSync(p, "utf8").split("\n").some((l) => l.trim() === `# ${title}`)) return t;
    }
  } catch {}
  return null;
}
function commentFiles(id) {
  try { return readdirSync(join(NDIR, "tickets", id, "comments")).filter((f) => f.endsWith(".md")); } catch { return []; }
}
async function openCard(pg, title) {
  await pg.locator(".card .title-link", { hasText: title }).click();
  await pg.waitForSelector(".drawer .add-comment textarea", { timeout: 10000 });
}
async function closeCard(pg) {
  await pg.locator(".drawer-close").click();
  await pg.waitForSelector(".drawer", { state: "detached", timeout: 10000 });
}

const browser = await chromium.launch();
let cli = null;
let cliOut = "";
let code = 1;
try {
  // Peers.
  const ctxA = await browser.newContext();
  const a = wirePage(await ctxA.newPage(), "A");
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, null, { timeout: 60000 });
  const href = await a.evaluate(() => location.href);
  const ticket = await a.evaluate(() => location.hash.slice(1));
  console.log(`A hosting (${ticket.length}-char ticket)`);

  const ctxB = await browser.newContext();
  const b = wirePage(await ctxB.newPage(), "B");
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });

  cli = spawn(BIN, ["connect", href, NDIR], { stdio: ["ignore", "pipe", "pipe"] });
  cli.stdout.on("data", (d) => { cliOut += d; process.stdout.write("[cli] " + d); });
  cli.stderr.on("data", (d) => process.stdout.write("[cli!] " + d));
  cli.on("exit", (c, s) => console.log("[cli] exited:", c ?? s));
  await sleep(5000);

  // 1. A adds both cards and comments on doomed.
  await addCard(a, DOOMED);
  await addCard(a, KEPT);
  await openCard(a, DOOMED);
  await a.locator(".drawer .add-comment textarea").fill(COMMENT);
  await a.locator(".drawer .add-comment button").click();
  await a.waitForSelector(".drawer .comment-text", { timeout: 10000 });
  await closeCard(a);
  console.log("A added both cards and a comment on the doomed one");

  // 2. B and the CLI hold everything.
  await waitCard(b, DOOMED);
  await waitCard(b, KEPT);
  let doomedId = null, keptId = null;
  await waitPath(() => (doomedId = cardIdByTitle(DOOMED)) && (keptId = cardIdByTitle(KEPT)), "the CLI never received both cards");
  await waitPath(() => commentFiles(doomedId).length === 1, `the CLI never received the comment (has ${JSON.stringify(commentFiles(doomedId))})`);
  console.log(`✓ B shows both cards; the CLI holds ${doomedId} (+1 comment) and ${keptId}`);

  // 3. B deletes the comment.
  await openCard(b, DOOMED);
  await b.waitForSelector(".drawer .comment-text", { timeout: 30000 });
  await b.locator(".drawer .comment-delete").click();
  await b.waitForSelector(".drawer .comments-empty", { timeout: 10000 });
  if (!dialogs.some((d) => d.startsWith("B:confirm:") && /comment/i.test(d))) {
    throw new Error(`deleting a comment didn't ask for confirmation (dialogs: ${JSON.stringify(dialogs)})`);
  }
  await closeCard(b);
  await waitPath(() => commentFiles(doomedId).length === 0, "B's comment delete never reached the CLI's disk");
  await openCard(a, DOOMED);
  await a.waitForSelector(".drawer .comments-empty", { timeout: 60000 });
  await closeCard(a);
  console.log("✓ B's comment delete reached A and the CLI");

  // 4. B goes offline with a stale copy of the doomed card in its OPFS.
  await sleep(1500);
  await b.close();
  console.log("B closed (stale OPFS copy of the doomed card)");

  // 5. A deletes the card: three-peer convergence (A + CLI; B is offline).
  await openCard(a, DOOMED);
  await a.locator(".drawer .card-delete").click();
  await waitGone(a, DOOMED);
  const cardMd = join(NDIR, "tickets", doomedId, "card.md");
  const metaToml = join(NDIR, "tickets", doomedId, "meta.toml");
  await waitPath(() => !existsSync(cardMd) && !existsSync(metaToml), "A's card delete never removed card.md + meta.toml on the CLI");
  console.log("✓ A's card delete removed card.md and meta.toml on the CLI");

  // 6. B comes back as a late joiner with the stale copy.
  const b2 = wirePage(await ctxB.newPage(), "B2");
  await b2.goto(base + "#" + ticket, { waitUntil: "load" });
  await waitCard(b2, KEPT);
  await waitGone(b2, DOOMED);
  console.log("✓ late joiner B dropped its stale copy of the doomed card");
  await sleep(5000);
  if (await hasCard(a, DOOMED)) throw new Error("the doomed card came back on A after B rejoined");
  if (existsSync(cardMd)) throw new Error("the doomed card.md came back on the CLI after B rejoined");
  if (await hasCard(b2, DOOMED)) throw new Error("the doomed card came back on B after rejoining");
  console.log("✓ nothing came back (5 s window)");

  // 7. The CLI deletes a card directory on disk.
  rmSync(join(NDIR, "tickets", keptId), { recursive: true, force: true });
  await waitGone(a, KEPT);
  await waitGone(b2, KEPT);
  console.log("✓ the CLI's on-disk delete reached both browsers");

  // 8. Leftovers: a ticket dir with a meta.toml but no card.md.
  const ORPHAN = `tk_orphan${rnd}`;
  mkdirSync(join(NDIR, "tickets", ORPHAN), { recursive: true });
  writeFileSync(join(NDIR, "tickets", ORPHAN, "meta.toml"), 'column = "Todo"\n');
  const leftover = (pg) => pg.locator(".leftover", { hasText: ORPHAN });
  await leftover(a).waitFor({ timeout: 60000 });
  await leftover(b2).waitFor({ timeout: 60000 });
  console.log("✓ both browsers show the leftover ticket dir");
  await leftover(a).locator(".leftover-clean").click();
  await waitPath(() => !existsSync(join(NDIR, "tickets", ORPHAN, "meta.toml")), "Remove leftovers never reached the CLI's disk");
  await leftover(b2).waitFor({ state: "detached", timeout: 60000 });
  await leftover(a).waitFor({ state: "detached", timeout: 10000 });
  console.log("✓ Remove leftovers cleared it on A, B and the CLI");

  console.log("PASS: deletes converge across browser+browser+CLI, incl. an offline-at-delete late joiner");
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
  console.log("cli stdout (tail):", JSON.stringify(cliOut.slice(-1500)));
} finally {
  if (cli) cli.kill();
  await browser.close();
}
process.exit(code);
