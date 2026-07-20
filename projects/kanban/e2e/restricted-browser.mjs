// Restricted-mode device-linking auth, END-TO-END in real browsers. Three
// async Playwright windows over the sealed iroh mesh:
//
//   Founder (restricted; grants Member's device id) ── Member (authorized)
//                                                    └─ Outsider (ungranted)
//
// Read is open (all hold the link), so the founder's card reaches everyone; only
// WRITES are gated — the Member's card syncs, the Outsider's is rejected.
//
// Restricted mode is set via riftpipe.configureAuth() (persisted to
// localStorage) and picked up on the app's FIRST connect after a fresh load —
// no racy in-tab reconnect. Each participant keeps its persistent context so its
// device identity (and localStorage) is stable across loads.
import { chromium } from "playwright";

const PORT = process.env.PORT || "8129";
const base = `http://127.0.0.1:${PORT}/`;
const log = (...a) => console.log(...a);

async function pageIn(ctx, hash = "") {
  const p = await ctx.newPage();
  p.on("pageerror", (e) => log("pageerror:", e.message));
  await p.goto(base + hash, { waitUntil: "load" });
  await p.waitForSelector(".add-card input", { timeout: 20000 });
  await p.waitForFunction(() => globalThis.riftpipe?.deviceId, { timeout: 20000 });
  return p;
}
const deviceId = (p) => p.evaluate(() => globalThis.riftpipe.deviceId());
const setAuth = (p, restricted, grants) =>
  p.evaluate(([r, g]) => globalThis.riftpipe.configureAuth(r, g), [restricted, grants]);
const addCard = async (p, title) => {
  const i = p.locator(".add-card input").first();
  await i.fill(title);
  await i.press("Enter");
  await p.waitForSelector(`text=${title}`, { timeout: 10000 });
};
const hasCard = (p, title) =>
  p.locator(`text=${title}`).first().waitFor({ timeout: 8000 }).then(() => true).catch(() => false);

const browser = await chromium.launch();
// Persistent contexts so each participant's device id survives reloads.
const founderCtx = await browser.newContext();
const memberCtx = await browser.newContext();
const outsiderCtx = await browser.newContext();
let code = 1;
try {
  // 1) Read the member's + outsider's device ids, and mark them restricted
  //    joiners (persist to their localStorage), then close the throwaway pages.
  let p = await pageIn(memberCtx);
  const memberId = await deviceId(p);
  await setAuth(p, true, []);
  await p.close();

  p = await pageIn(outsiderCtx);
  const outsiderId = await deviceId(p);
  await setAuth(p, true, []);
  await p.close();
  log(`member id  : ${memberId}`);
  log(`outsider id: ${outsiderId} (will NOT be granted)`);

  // 2) Founder: mark restricted + grant ONLY the member, then reload to host it.
  const founder = await pageIn(founderCtx);
  await setAuth(founder, true, [memberId]);
  await founder.reload({ waitUntil: "load" });
  await founder.waitForSelector(".add-card input", { timeout: 20000 });
  await founder.waitForFunction(() => location.hash.length > 1, { timeout: 40000 });
  const ticket = await founder.evaluate(() => location.hash.slice(1));
  log(`founder hosting restricted board (sealed ticket ${ticket.length} chars)`);

  // 3) Member + Outsider join the founder's ticket on a fresh load (restricted).
  const member = await pageIn(memberCtx, "#" + ticket);
  const outsider = await pageIn(outsiderCtx, "#" + ticket);
  await founder.waitForTimeout(8000); // mesh + trust-log catch-up

  // Founder writes → reaches everyone (read is open to link-holders).
  const fCard = "founder-" + Math.random().toString(16).slice(2, 7);
  await addCard(founder, fCard);
  const fToMember = await hasCard(member, fCard);
  const fToOutsider = await hasCard(outsider, fCard);
  log(`founder→member: ${fToMember}   founder→outsider: ${fToOutsider}   (both expected TRUE — read is open)`);

  // Member (authorized) writes → reaches the founder.
  const mCard = "member-" + Math.random().toString(16).slice(2, 7);
  await addCard(member, mCard);
  const mToFounder = await hasCard(founder, mCard);
  log(`member→founder: ${mToFounder}   (expected TRUE — authorized writer)`);

  // Outsider (ungranted) writes → REJECTED, must NOT reach the founder.
  const oCard = "outsider-" + Math.random().toString(16).slice(2, 7);
  await addCard(outsider, oCard);
  await founder.waitForTimeout(5000); // ample time for a rejected write to (not) land
  const oToFounder = await hasCard(founder, oCard);
  log(`outsider→founder: ${oToFounder}   (expected FALSE — unauthorized write rejected)`);

  const pass = fToMember && fToOutsider && mToFounder && !oToFounder;
  log(pass
    ? "PASS: restricted mode enforces in the browser — authorized writes sync, unauthorized rejected, read open"
    : "FAIL: restricted-mode enforcement did not behave as expected");
  code = pass ? 0 : 1;
} catch (e) {
  log("FAIL:", e.message);
} finally {
  await browser.close();
}
process.exit(code);
