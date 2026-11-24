// End-to-end test of the library event log over the real sealed mesh:
//   - the app's semantic events (card.create / card.check / comment.add) land;
//   - the library's auto events (auth.claim/request/grant, mesh.merge) land;
//   - per-device logs MERGE across peers — B's history includes A's events.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8140";
const base = `http://127.0.0.1:${PORT}/`;
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const fail = (m) => { console.error("FAIL:", m); process.exitCode = 1; throw new Error(m); };
const events = (page) => page.evaluate(() => window.riftpipe.readEvents());
const kinds = (evs) => [...new Set(evs.map((e) => e.kind))].sort();
async function addCard(page, title) {
  const i = page.locator(".add-card input").first();
  await i.fill(title); await i.press("Enter");
  await page.waitForSelector(".card .title", { timeout: 8000 });
}

const browser = await chromium.launch({ headless: true });
try {
  const a = await (await browser.newContext()).newPage();
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));

  // App semantic events: create a card, check it, comment on it.
  await addCard(a, "alpha");
  await a.locator(".card", { hasText: "alpha" }).locator('input[type="checkbox"]').check();
  await a.locator(".card .title-link", { hasText: "alpha" }).click();
  await a.waitForSelector(".drawer .add-comment textarea", { timeout: 8000 });
  await a.locator(".drawer .add-comment textarea").fill("first note");
  await a.locator(".drawer .add-comment button").click();
  await a.waitForSelector(".drawer .comment-text", { timeout: 8000 });
  await a.locator(".drawer-close").click();
  await a.waitForSelector(".drawer", { state: "detached", timeout: 8000 });
  await wait(300);

  let aEvents = await events(a);
  for (const k of ["card.create", "card.check", "comment.add"]) {
    if (!aEvents.some((e) => e.kind === k)) fail(`A missing semantic event ${k}; have ${JSON.stringify(kinds(aEvents))}`);
  }
  const created = aEvents.find((e) => e.kind === "card.create");
  if (created.title !== "alpha") fail(`card.create should carry title alpha, got ${JSON.stringify(created)}`);
  if (!created.ts || !created.device) fail(`event missing ts/device: ${JSON.stringify(created)}`);
  console.log("✓ app semantic events logged:", JSON.stringify(kinds(aEvents)));

  // Second peer joins; run the auth flow so auth.* events fire.
  const b = await (await browser.newContext()).newPage();
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  await wait(1500);

  await a.locator(".lock-btn").click();          // auth.claim (A)
  await wait(2000);
  await b.locator(".ask-btn").click();           // auth.request (B)
  await wait(1500);
  const approve = a.locator(".req").first().locator(".approve-btn");
  await approve.waitFor({ timeout: 8000 });
  await approve.click();                          // auth.grant (A)
  await wait(3000);

  aEvents = await events(a);
  for (const k of ["auth.claim", "auth.grant"]) {
    if (!aEvents.some((e) => e.kind === k)) fail(`A missing ${k}; have ${JSON.stringify(kinds(aEvents))}`);
  }
  console.log("✓ library auth events logged on A:", JSON.stringify(kinds(aEvents).filter((k) => k.startsWith("auth"))));

  // Authorship: grant is authored by the approver (A) and names the approved
  // device (B) as `target` — event data can't overwrite `device`.
  const aId = await a.evaluate(() => window.riftpipe.deviceId());
  const bId = await b.evaluate(() => window.riftpipe.deviceId());
  const grant = aEvents.find((e) => e.kind === "auth.grant");
  if (grant.device !== aId || grant.target !== bId) {
    fail(`auth.grant should be {device: A, target: B}, got ${JSON.stringify(grant)} (A ${aId.slice(0, 8)}, B ${bId.slice(0, 8)})`);
  }
  const claim = aEvents.find((e) => e.kind === "auth.claim");
  if (claim.device !== aId) fail(`auth.claim should be authored by A, got ${JSON.stringify(claim)}`);
  const req = (await events(b)).find((e) => e.kind === "auth.request" && e.device === bId);
  if (!req) fail("B's auth.request should be authored by B");
  console.log("✓ grant names the right devices: device=A (approver), target=B");

  // Cross-peer merge: B's history should include A's card + auth events, plus
  // B's own auth.request, plus mesh.merge (B received A's files).
  const bEvents = await events(b);
  const bk = kinds(bEvents);
  for (const k of ["card.create", "auth.claim", "auth.grant", "auth.request", "mesh.merge"]) {
    if (!bEvents.some((e) => e.kind === k)) fail(`B's merged history missing ${k}; have ${JSON.stringify(bk)}`);
  }
  // Two distinct device logs are represented in the merged view.
  const devices = new Set(bEvents.map((e) => e.device));
  if (devices.size < 2) fail(`expected events from 2 devices in merged log, saw ${devices.size}`);
  // Sorted oldest→newest by ts.
  const ts = bEvents.map((e) => e.ts);
  if (ts.join("|") !== [...ts].sort().join("|")) fail("events not time-sorted");
  console.log("✓ B's merged cross-peer history:", JSON.stringify(bk), `from ${devices.size} devices`);

  // Revoke: authored by A, `target` = the removed device B.
  const revokeBtn = a.locator(".member", { hasText: bId.slice(0, 8) }).locator(".revoke-btn");
  await revokeBtn.waitFor({ timeout: 8000 });
  await revokeBtn.click();
  let revoke = null;
  for (let k = 0; k < 25 && !revoke; k++) {
    revoke = (await events(a)).find((e) => e.kind === "auth.revoke") ?? null;
    if (!revoke) await wait(200);
  }
  if (!revoke) fail("A should log auth.revoke after removing B");
  if (revoke.device !== aId || revoke.target !== bId) fail(`auth.revoke should be {device: A, target: B}, got ${JSON.stringify(revoke)}`);
  console.log("✓ revoke names the right devices: device=A (remover), target=B");

  console.log("\nPASS — library event log records app + auth + merge events and merges across peers");
} finally {
  await browser.close();
}
