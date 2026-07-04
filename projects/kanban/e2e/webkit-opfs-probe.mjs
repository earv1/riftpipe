// Which raw OPFS operation hangs or fails in WebKit? Each step is raced
// against a 4s timeout so a never-resolving promise is visible.
import { webkit } from "playwright";
const PORT = process.env.PORT || "8131";
const pg = await (await (await webkit.launch()).newContext()).newPage();
await pg.goto(`http://127.0.0.1:${PORT}/`, { waitUntil: "load" });
const out = await pg.evaluate(async () => {
  const t = (p) => Promise.race([p, new Promise((_, r) => setTimeout(() => r(new Error("TIMEOUT")), 4000))]);
  const log = [];
  const step = async (name, fn) => {
    try {
      const v = await t(fn());
      log.push(`${name}: ok${v !== undefined ? " (" + String(v).slice(0, 40) + ")" : ""}`);
      return v;
    } catch (e) {
      log.push(`${name}: ${String(e).slice(0, 120)}`);
      throw e;
    }
  };
  try {
    const root = await step("getDirectory", () => navigator.storage.getDirectory());
    const fh = await step("getFileHandle(create)", () => root.getFileHandle("probe.txt", { create: true }));
    const w = await step("createWritable", () => fh.createWritable());
    await step("write", () => w.write("hello"));
    await step("close", () => w.close());
    const f = await step("getFile", () => fh.getFile());
    await step("text", async () => await f.text());
  } catch (_e) { /* log already captured */ }
  return log;
});
console.log(out.join("\n"));
await pg.context().browser().close();
