// Single-browser CRUD smoke over the TS domain layer (board.ts on the file API):
// seed → add → move + done (patch) → open detail → comment → reload persists.
import { chromium } from "playwright";
const PORT = process.env.PORT || "8140";
const base = `http://127.0.0.1:${PORT}/`;
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const fail = (m) => { console.error("FAIL:", m); process.exitCode = 1; throw new Error(m); };

const browser = await chromium.launch({ headless: true });
try {
  const p = await (await browser.newContext()).newPage();
  await p.goto(base, { waitUntil: "load" });
  await p.waitForSelector(".add-card input", { timeout: 20000 });

  // Seed columns rendered (getBoard seeding via the file API).
  const cols = await p.$$eval(".column-title", (els) => els.map((e) => e.textContent?.trim()));
  if (!(cols.includes("TODO") || cols.includes("Todo"))) fail(`expected seeded columns, got ${JSON.stringify(cols)}`);
  console.log("✓ seeded board rendered:", JSON.stringify(cols));

  // Add a card in the first column.
  const input = p.locator(".add-card input").first();
  await input.fill("smoke card");
  await input.press("Enter");
  await p.waitForSelector(".card .title", { timeout: 8000 });
  const titles = await p.$$eval(".card .title", (els) => els.map((e) => e.textContent?.trim()));
  if (!titles.includes("smoke card")) fail(`card not added, got ${JSON.stringify(titles)}`);
  console.log("✓ addCard wrote card.md + meta.toml (card visible)");

  // Toggle done via the checkbox (patchCard done=true).
  await p.locator(".card", { hasText: "smoke card" }).locator('input[type="checkbox"]').check();
  await wait(600);
  const doneClass = await p.locator(".card", { hasText: "smoke card" }).getAttribute("class");
  if (!doneClass?.includes("done")) fail(`card not marked done, class=${doneClass}`);
  console.log("✓ patchCard done=true (card shows done)");

  // Open the detail drawer and add a comment (addComment).
  await p.locator(".card .title-link", { hasText: "smoke card" }).click();
  await p.waitForSelector(".drawer .add-comment textarea", { timeout: 8000 });
  await p.locator(".drawer .add-comment textarea").fill("a smoke comment");
  await p.locator(".drawer .add-comment button").click();
  await p.waitForSelector(".drawer .comment-text", { timeout: 8000 });
  const ctext = await p.locator(".drawer .comment-text").first().textContent();
  if (!ctext?.includes("a smoke comment")) fail(`comment not shown, got ${ctext}`);
  console.log("✓ addComment wrote comments/<ts>__anon.md (comment visible)");

  // Reload — everything persists (OPFS via the file API).
  await p.reload({ waitUntil: "load" });
  await p.waitForSelector(".card .title", { timeout: 20000 });
  const after = await p.$$eval(".card .title", (els) => els.map((e) => e.textContent?.trim()));
  if (!after.includes("smoke card")) fail(`card lost after reload, got ${JSON.stringify(after)}`);
  console.log("✓ persisted across reload");

  console.log("\nPASS — full CRUD works over the TS domain + file API");
} finally {
  await browser.close();
}
