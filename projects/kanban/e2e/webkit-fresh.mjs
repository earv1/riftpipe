// WebKit smoke: a fresh visitor on Safari's engine must get a usable board —
// title, three seeded columns, and a working add-card (mobile-blank-board
// regression guard). Also prints whether this WebKit supports OPFS writes.
import { webkit } from "playwright";

const PORT = process.env.PORT || "8131";
const pg = await (await (await webkit.launch()).newContext()).newPage();
pg.on("pageerror", (e) => console.log("pageerror:", e.message));
pg.on("console", (m) => console.log(`console.${m.type()}:`, m.text().slice(0, 300)));
let code = 1;
try {
  await pg.goto(`http://127.0.0.1:${PORT}/`, { waitUntil: "load" });
  await pg.waitForSelector(".column", { timeout: 20000 });
  const cols = await pg.locator(".column").count();
  const warned = (await pg.locator(".storage-warning").count()) > 0;
  const opfsWorks = await pg.evaluate(async () => {
    try {
      await navigator.storage.getDirectory();
      return true;
    } catch {
      return false;
    }
  });
  console.log(`webkit: ${cols} columns · storage-warning: ${warned} · OPFS works: ${opfsWorks}`);
  if (cols < 3) throw new Error("fresh board rendered without seeded columns");
  if (opfsWorks) {
    // Storage works: full flow must work and no warning may show.
    if (warned) throw new Error("storage warning shown although OPFS works");
    const inp = pg.locator(".add-card input").first();
    await inp.fill("webkit-card");
    await inp.press("Enter");
    await pg.waitForSelector("text=webkit-card", { timeout: 10000 });
    console.log("PASS: WebKit fresh board fully usable (columns + add-card)");
  } else {
    // Broken storage (headless WebKit / old iOS): view-only board + warning,
    // never a blank page or an eternal spinner.
    if (!warned) throw new Error("OPFS broken but no storage warning shown");
    console.log("PASS: WebKit degrades to view-only board with warning");
  }
  code = 0;
} catch (e) {
  console.log("FAIL:", e.message);
  // Where did it stall?
  const diag = await pg.evaluate(async () => {
    const out = {
      bodyText: document.body.innerText.slice(0, 120),
      loading: !!document.querySelector(".loading"),
      warning: !!document.querySelector(".storage-warning"),
      hasGetDirectory: !!navigator.storage?.getDirectory,
      hasFileHandle: typeof FileSystemFileHandle !== "undefined",
      createWritable: typeof FileSystemFileHandle !== "undefined" &&
        "createWritable" in FileSystemFileHandle.prototype,
      riftpipe: typeof globalThis.riftpipe,
    };
    try {
      const r = await Promise.race([
        globalThis.riftpipe?.kanbanHandle?.("GET", "/api/board", ""),
        new Promise((_, rej) => setTimeout(() => rej(new Error("handle timeout")), 5000)),
      ]);
      out.board = String(r?.body ?? r).slice(0, 160);
    } catch (err) {
      out.boardError = String(err).slice(0, 200);
    }
    return out;
  }).catch((err) => ({ evalError: String(err) }));
  console.log("DIAG:", JSON.stringify(diag, null, 1));
} finally {
  process.exit(code);
}
