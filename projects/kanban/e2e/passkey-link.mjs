// End-to-end PASSKEY DEVICE-LINKING proof over the real sealed mesh, browser
// side: A claims the board with a passkey (a CDP virtual authenticator stands in
// for Face ID / Touch ID). The same passkey is then copied to B's authenticator —
// the platform syncing it to the owner's second device — and B joins by asserting
// it: every peer verifies the assertion offline, B becomes an Admin with no
// approve click, A wraps it into the epoch, and B reads, writes and stays linked
// across a reload.
//
// Run: node e2e/passkey-link.mjs   (needs `./run.sh serve` on PORT, default 8140)
import { chromium } from "playwright";

const PORT = process.env.PORT || "8140";
// WebAuthn needs a domain RP ID: `localhost` is a secure context and a valid RP
// ID, `127.0.0.1` isn't (IP-address origins are rejected).
const base = `http://localhost:${PORT}/`;
const fail = (m) => { console.error("FAIL:", m); process.exitCode = 1; throw new Error(m); };
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

async function waitState(page, pred, ms = 10000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const s = await authState(page); if (pred(s)) return s; await wait(250); }
  return authState(page);
}
// A CDP virtual platform authenticator on `page`: resident keys, user
// verification and presence all auto-approved, so ceremonies need no prompt.
async function addAuthenticator(page) {
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("WebAuthn.enable");
  const { authenticatorId } = await cdp.send("WebAuthn.addVirtualAuthenticator", { options: {
    protocol: "ctap2", transport: "internal", hasResidentKey: true,
    hasUserVerification: true, isUserVerified: true, automaticPresenceSimulation: true } });
  return { cdp, authenticatorId };
}

const browser = await chromium.launch({ headless: true });
try {
  // 1. A hosts and claims the board with a passkey.
  const a = await (await browser.newContext()).newPage();
  const va = await addAuthenticator(a);
  await a.goto(base, { waitUntil: "load" });
  await a.waitForSelector(".add-card input", { timeout: 20000 });
  await a.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await a.evaluate(() => location.hash.slice(1));
  console.log(`A hosting (${ticket.length}-char ticket)`);
  if (!(await a.evaluate(() => window.riftpipe.passkeySupported()))) fail("passkeys should be supported on http://localhost");

  const claimBtn = a.locator(".passkey-claim-btn");
  await claimBtn.waitFor({ timeout: 10000 });
  await claimBtn.click();
  const aState = await waitState(a, (s) => s.locked && s.isAuthor && s.passkeys === 1 && s.canRead);
  if (!(aState.locked && aState.isAuthor && aState.passkeys === 1 && aState.canRead)) {
    fail(`A should be the claimed, read-capable author with one passkey, got ${JSON.stringify(aState)}`);
  }
  await a.locator(".passkey-on").waitFor({ timeout: 5000 });
  console.log("✓ A claimed with a passkey");

  // 2. Export the passkey from A's authenticator.
  const { credentials } = await va.cdp.send("WebAuthn.getCredentials", { authenticatorId: va.authenticatorId });
  if (!(credentials.length === 1 && credentials[0].rpId === "localhost")) {
    fail(`A's authenticator should hold one localhost passkey, got ${JSON.stringify(credentials.map((c) => c.rpId))}`);
  }

  // 3. A writes before B links.
  await addCard(a, "before-link");

  // 4. B opens the board: claimed, no access, but a passkey link is offered.
  const b = await (await browser.newContext()).newPage();
  const vb = await addAuthenticator(b);
  // The platform syncing the passkey to the owner's second device (iCloud
  // Keychain / Google Password Manager).
  await vb.cdp.send("WebAuthn.addCredential", { authenticatorId: vb.authenticatorId, credential: credentials[0] });
  await b.goto(base + "#" + ticket, { waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  const bBefore = await waitState(b, (s) => s.locked && !s.authorized && s.passkeyHere, 15000);
  if (!(bBefore.locked && !bBefore.authorized && bBefore.passkeyHere)) {
    fail(`B should see a claimed board, no access, with a passkey usable here, got ${JSON.stringify(bBefore)}`);
  }
  const bId = bBefore.device;
  await wait(2000);
  if (await hasCard(b, "before-link")) fail("B must not read sealed content before it links");
  console.log(`✓ B sees the claimed board, no access, passkey link offered (device ${bId.slice(0, 10)}…)`);

  // 5. B links with the passkey — no approve click on A.
  await b.locator(".passkey-link-btn").click();
  const bLinked = await waitState(b, (s) => s.authorized && s.isAdmin, 10000);
  if (!(bLinked.authorized && bLinked.isAdmin)) fail(`B should be an authorized Admin after linking, got ${JSON.stringify(bLinked)}`);
  const bReads = await waitState(b, (s) => s.canRead, 15000);
  if (!bReads.canRead) fail(`A should wrap B into the epoch, got ${JSON.stringify(bReads)}`);
  if (!(await waitCard(b, "before-link", 10000))) fail("B should read the card A wrote before B linked");
  const members = await a.evaluate(() => window.riftpipe.boardMembers());
  if (!members.some((m) => m.device === bId && m.role === "admin")) {
    fail(`A should list B as an admin member, got ${JSON.stringify(members)}`);
  }
  // B's read request is pruned on A as soon as the link lands: no approval pending.
  let pendingB = true;
  for (const end = Date.now() + 5000; Date.now() < end; await wait(250)) {
    const pending = await a.evaluate(() => window.riftpipe.pendingRequests());
    pendingB = pending.some((r) => r.device === bId);
    if (!pendingB) break;
  }
  if (pendingB) fail("A should have no pending request from B — linking needs no approve click");
  console.log("✓ B linked by passkey: Admin on A's trust log, wrapped into the epoch, reads pre-link content — no approval");

  // 6. B writes; A sees it.
  await addCard(b, "linked-write");
  if (!(await waitCard(a, "linked-write", 10000))) fail("A should read the passkey-linked device's write");
  console.log("✓ B's write reaches A");

  // 7. B's linked access persists across a reload.
  await b.reload({ waitUntil: "load" });
  await b.waitForSelector(".add-card input", { timeout: 20000 });
  const bAfterReload = await waitState(b, (s) => s.authorized && s.isAdmin && s.canRead, 15000);
  if (!(bAfterReload.authorized && bAfterReload.isAdmin && bAfterReload.canRead)) {
    fail(`linked access persists across reload, got ${JSON.stringify(bAfterReload)}`);
  }
  console.log("✓ B stays linked across a reload");

  console.log("\nPASS — passkey claim + passkey device-link (no approval), verified offline by every peer");
} finally {
  await browser.close();
}
