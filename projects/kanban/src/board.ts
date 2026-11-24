// The kanban **domain model**, in plain TypeScript over the generic file API
// (`fs.ts`). This is the app; riftpipe knows none of it. It reads and writes the
// same on-disk layout the native CLI materializes as real files, so a board is
// portable between the browser and a `riftpipe connect` working copy:
//
//   board.md                 "# Title", then "- Column" per column (ordered)
//   tickets/<id>/card.md      "# Card Title\n\n<markdown description>"
//   tickets/<id>/meta.toml    column (str), position (int), done (bool)
//   tickets/<id>/comments/<ts>__<author>.md
//
// (This was previously Rust — `kanban-wasm`'s handler.rs/format.rs. It moved here
// so the wasm bundle stays a generic file-sync library with nothing kanban-
// specific baked in.)
import { read, write, list, remove, isDeleted, boardId } from "./fs.ts";
import { logEvent } from "./events.ts";
import { createEditSettler, EDIT_SETTLE_MS } from "./editSettle.ts";

export interface Card {
  id: string;
  title: string;
  column: string;
  position: number;
  done: boolean;
}
export interface Comment {
  id: string;
  author: string;
  ts: string;
  text: string;
}
export interface Board {
  title: string;
  columns: string[];
  cards: Card[];
  /** Ids of `tickets/<id>/` dirs that hold files but whose `card.md` is DELETED (tombstoned): a
   *  deleted card whose other files a concurrent edit kept (edit beats delete, per file). Shown
   *  for the user to clean up, never swept automatically. A dir whose `card.md` is merely
   *  missing (not tombstoned) is a card still arriving and is not listed. */
  leftovers: string[];
}
export interface CardDetail extends Card {
  description: string;
  comments: Comment[];
}

const DEFAULT_COLUMNS = ["Todo", "Doing", "Done"];

// ---------------------------------------------------------------------------
// Pure format helpers — parse/serialize the on-disk text (no I/O), a faithful
// port of the old Rust `format` module. Kept exported so they're unit-testable.
// ---------------------------------------------------------------------------

/** First markdown `# ` heading, trimmed; `null` if none. */
export function firstHeading(text: string): string | null {
  for (const raw of text.split("\n")) {
    const line = raw.replace(/\s+$/, "");
    if (line.startsWith("# ")) {
      const h = line.slice(2).trim();
      if (h) return h;
    }
  }
  return null;
}

/** Parse `board.md` → title + ordered columns. Robust to a missing/blank file. */
export function parseBoardMd(text: string): { title: string; columns: string[] } {
  const title = firstHeading(text) ?? "Board";
  const columns: string[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.replace(/\s+$/, "");
    if (line.startsWith("- ")) {
      const c = line.slice(2).trim();
      if (c) columns.push(c);
    }
  }
  return { title, columns };
}

/** Serialize title + columns → `board.md` text. */
export function boardMd(title: string, columns: string[]): string {
  return `# ${title}\n\n` + columns.map((c) => `- ${c}\n`).join("");
}

/** Split `card.md` into title + description. */
export function splitCardMd(text: string): { title: string; description: string } {
  const lines = text.split("\n");
  let title = "";
  let i = 0;
  while (i < lines.length) {
    const line = lines[i].replace(/\s+$/, "");
    if (line.startsWith("# ") && line.slice(2).trim()) {
      title = line.slice(2).trim();
      i += 1;
      break;
    }
    i += 1;
  }
  const description = lines
    .slice(Math.min(i, lines.length))
    .join("\n")
    .replace(/^\n+/, "")
    .replace(/\s+$/, "");
  return { title, description };
}

/** Serialize title + description → `card.md` text. */
export function cardMd(title: string, description: string): string {
  if (!description.trim()) return `# ${title}\n`;
  return `# ${title}\n\n${description.replace(/\s+$/, "")}\n`;
}

/** Parse `meta.toml` (a tiny `key = value` reader — three scalar fields). */
export function parseMeta(text: string): { column: string | null; position: number; done: boolean } {
  let column: string | null = null;
  let position = 0;
  let done = false;
  for (const line of text.split("\n")) {
    const idx = line.indexOf("=");
    if (idx < 0) continue;
    const k = line.slice(0, idx).trim();
    const v = line.slice(idx + 1).trim();
    if (k === "column") {
      const s = unquote(v);
      column = s === "" ? null : s;
    } else if (k === "position") {
      const n = parseInt(v, 10);
      position = Number.isNaN(n) ? 0 : n;
    } else if (k === "done") {
      done = v === "true";
    }
  }
  return { column, position, done };
}

/** Strip surrounding quotes from a TOML basic string and unescape `\\` / `\"`. */
function unquote(v: string): string {
  let inner = v;
  if (inner.length >= 2 && inner.startsWith('"') && inner.endsWith('"')) {
    inner = inner.slice(1, -1);
  }
  let out = "";
  for (let i = 0; i < inner.length; i++) {
    if (inner[i] === "\\" && i + 1 < inner.length) {
      out += inner[i + 1];
      i += 1;
    } else {
      out += inner[i];
    }
  }
  return out;
}

/** Serialize structural fields → `meta.toml` text (valid TOML, escaping the column). */
export function metaToml(column: string, position: number, done: boolean): string {
  const esc = column.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  return `column = "${esc}"\nposition = ${position}\ndone = ${done}\n`;
}

/** A card from its files' text (defaults for any missing/blank piece). */
export function cardFromFiles(id: string, cardMdText: string, metaText: string, defaultColumn: string): Card {
  const meta = parseMeta(metaText);
  return {
    id,
    title: firstHeading(cardMdText) ?? id,
    column: meta.column ?? defaultColumn,
    position: meta.position,
    done: meta.done,
  };
}

/** Sanitize an author into a filename-safe slug `[a-z0-9-]+`, or `anon`. */
export function sanitizeAuthor(author: string): string {
  let out = "";
  let prevDash = false;
  for (const ch of author.toLowerCase()) {
    if (/[a-z0-9-]/.test(ch)) {
      out += ch;
      prevDash = false;
    } else if (!prevDash) {
      out += "-";
      prevDash = true;
    }
  }
  const slug = out.replace(/^-+/, "").replace(/-+$/, "");
  return slug || "anon";
}

/** Parse a comment filename stem `<ts>__<author>` → [ts, author], or null. */
export function parseCommentName(stem: string): [string, string] | null {
  const sep = stem.indexOf("__");
  if (sep < 0) return null;
  return [stem.slice(0, sep), stem.slice(sep + 2)];
}

// ---------------------------------------------------------------------------
// Operations — the board API, over the file tree. Each mutation writes the
// relevant file(s); `fs.write` persists to OPFS and syncs to peers.
// ---------------------------------------------------------------------------

function newId(): string {
  return "tk_" + Math.floor(Math.random() * 4_294_967_296).toString(16).padStart(8, "0");
}

/** ISO timestamp with `:` → `-` so it's a safe filename (matches the CLI). */
function isoNow(): string {
  return new Date().toISOString().replace(/:/g, "-");
}

/** Read `board.md`, seeding default columns on first use so a fresh board isn't
 *  columnless. The seed is written before we connect, so its push is a no-op —
 *  a real host board arriving over sync overwrites it. */
async function ensureSeed(): Promise<string> {
  const existing = await read("board.md");
  if (existing != null) return existing;
  const seed = boardMd("My Board", DEFAULT_COLUMNS);
  await write("board.md", seed);
  return seed;
}

async function readCard(id: string, defaultCol: string): Promise<Card> {
  const cardMdText = (await read(`tickets/${id}/card.md`)) ?? "";
  const metaText = (await read(`tickets/${id}/meta.toml`)) ?? "";
  return cardFromFiles(id, cardMdText, metaText, defaultCol);
}

/** `readCard`, or `null` when `card.md` is absent — being deleted (card.md goes first) — or
 *  not a card. */
async function readCardIfPresent(id: string, defaultCol: string): Promise<Card | null> {
  const cardMdText = await read(`tickets/${id}/card.md`);
  if (cardMdText === null) return null;
  const metaText = (await read(`tickets/${id}/meta.toml`)) ?? "";
  return cardFromFiles(id, cardMdText, metaText, defaultCol);
}

async function allCards(columns: string[]): Promise<{ cards: Card[]; leftovers: string[] }> {
  const defaultCol = columns[0] ?? "Todo";
  const ids = await list("tickets");
  const cards: Card[] = [];
  const leftovers: string[] = [];
  for (const id of ids) {
    if (id.startsWith(".")) continue;
    const card = await readCardIfPresent(id, defaultCol);
    if (card) cards.push(card);
    else if ((await isDeleted(`tickets/${id}/card.md`)) && (await filesUnder(`tickets/${id}`)).length > 0) {
      leftovers.push(id);
    }
  }
  return { cards, leftovers: leftovers.sort() };
}

export async function getBoard(): Promise<Board> {
  const { title, columns } = parseBoardMd(await ensureSeed());
  const { cards, leftovers } = await allCards(columns);
  return { title, columns, cards, leftovers };
}

export async function getCard(id: string): Promise<Card | null> {
  const { columns } = parseBoardMd(await ensureSeed());
  return readCardIfPresent(id, columns[0] ?? "Todo");
}

export async function getCardDetail(id: string): Promise<CardDetail | null> {
  const { columns } = parseBoardMd(await ensureSeed());
  const card = await readCardIfPresent(id, columns[0] ?? "Todo");
  if (!card) return null;
  const { description } = splitCardMd((await read(`tickets/${id}/card.md`)) ?? "");
  const comments = await readComments(id);
  return { ...card, description, comments };
}

async function readComments(id: string): Promise<Comment[]> {
  const out: Comment[] = [];
  for (const fname of await list(`tickets/${id}/comments`)) {
    if (!fname.endsWith(".md")) continue;
    const stem = fname.slice(0, -3);
    const parsed = parseCommentName(stem);
    if (!parsed) continue;
    const [ts, author] = parsed;
    const text = ((await read(`tickets/${id}/comments/${fname}`)) ?? "").replace(/\s+$/, "");
    out.push({ id: stem, author, ts, text });
  }
  out.sort((a, b) => a.ts.localeCompare(b.ts));
  return out;
}

export async function addCard(column: string, title: string): Promise<Card> {
  const { columns } = parseBoardMd(await ensureSeed());
  const col = column && column.length ? column : (columns[0] ?? "Todo");
  const id = newId();
  const existing = (await allCards(columns)).cards;
  const maxPos = existing
    .filter((c) => c.column === col)
    .reduce((m, c) => Math.max(m, c.position), -1);
  const finalTitle = title && title.length ? title : id;
  await write(`tickets/${id}/card.md`, cardMd(finalTitle, ""));
  await write(`tickets/${id}/meta.toml`, metaToml(col, maxPos + 1, false));
  await logEvent("card.create", { id, column: col, title: finalTitle });
  return { id, title: finalTitle, column: col, position: maxPos + 1, done: false };
}

/** Live saves fire per keystroke; history records ONE card.edit per field once
*  typing settles (`editSettle.ts`) — on the board it was typed on, or not at all. */
export { EDIT_SETTLE_MS };
const editSettler = createEditSettler({
  settleMs: EDIT_SETTLE_MS,
  board: boardId,
  log: (data) => logEvent("card.edit", data),
});

/** Write pending (unsettled) card.edit events now, before the page switches
 *  boards or goes away. */
export function flushPendingEdits(): Promise<void> {
  return editSettler.flush();
}

if (typeof window !== "undefined") {
  window.addEventListener("pagehide", () => void flushPendingEdits());
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") void flushPendingEdits();
  });
}

export async function patchCard(
  id: string,
  patch: Partial<Card> & { description?: string },
): Promise<Card> {
  const { columns } = parseBoardMd(await ensureSeed());
  const card = await readCard(id, columns[0] ?? "Todo");
  const before = { ...card };
  const oldCardMd = (await read(`tickets/${id}/card.md`)) ?? "";
  let { description } = splitCardMd(oldCardMd);
  const oldDescription = description;

  if (patch.column !== undefined) card.column = patch.column;
  if (patch.position !== undefined) card.position = patch.position;
  if (patch.done !== undefined) card.done = patch.done;
  if (patch.title !== undefined) card.title = patch.title === "" ? id : patch.title;
  const touchedText = patch.title !== undefined || patch.description !== undefined;
  if (patch.description !== undefined) description = patch.description;

  // Always persist structural fields; only re-serialize card.md when title or
  // description changed (avoids prose drift).
  await write(`tickets/${id}/meta.toml`, metaToml(card.column, card.position, card.done));
  if (touchedText) await write(`tickets/${id}/card.md`, cardMd(card.title, description));

  // Record semantic history — the same events the old server logged.
  if (card.column !== before.column) await logEvent("card.move", { id, from: before.column, to: card.column });
  if (card.done !== before.done) await logEvent("card.check", { id, done: card.done });
  if (card.title !== before.title) editSettler.queue(id, "title", before.title, card.title);
  if (patch.description !== undefined && description !== oldDescription) {
    editSettler.queue(id, "description", oldDescription, description);
  }
  return card;
}

export async function addComment(id: string, text: string, author?: string): Promise<Comment> {
  const clean = text.trim();
  const auth = sanitizeAuthor(author ?? "anon");
  const ts = isoNow();
  const name = `${ts}__${auth}`;
  await write(`tickets/${id}/comments/${name}.md`, clean);
  await logEvent("comment.add", { id, comment: name });
  return { id: name, author: auth, ts, text: clean };
}

/** Every file under `dir` (recursively), skipping dot-entries. */
async function filesUnder(dir: string): Promise<string[]> {
  const out: string[] = [];
  for (const name of await list(dir)) {
    if (name.startsWith(".")) continue;
    const p = `${dir}/${name}`;
    if ((await read(p)) !== null) out.push(p);
    else out.push(...(await filesUnder(p)));
  }
  return out;
}

/** Delete a card: card.md FIRST (so no peer renders a half-deleted card), then every other file
 *  under tickets/<id>/, meta.toml last. */
export async function deleteCard(id: string): Promise<void> {
  const { columns } = parseBoardMd(await ensureSeed());
  const title = (await readCardIfPresent(id, columns[0] ?? "Todo"))?.title ?? id;
  const dir = `tickets/${id}`;
  await remove(`${dir}/card.md`);
  for (const p of (await filesUnder(dir)).filter((p) => p !== `${dir}/meta.toml`).sort()) await remove(p);
  await remove(`${dir}/meta.toml`);
  await logEvent("card.delete", { id, title });
}

/** Remove the leftover files of a deleted card (`Board.leftovers`): everything under
 *  tickets/<id>/, meta.toml last. A no-op unless its card.md is (still) deleted — a card that came
 *  back, or one still arriving, is never touched. */
export async function cleanLeftovers(id: string): Promise<void> {
  const dir = `tickets/${id}`;
  if ((await read(`${dir}/card.md`)) !== null || !(await isDeleted(`${dir}/card.md`))) return;
  for (const p of (await filesUnder(dir)).filter((p) => p !== `${dir}/meta.toml`).sort()) await remove(p);
  await remove(`${dir}/meta.toml`);
}

/** Delete one comment of card `id`. */
export async function deleteComment(id: string, commentId: string): Promise<void> {
  await remove(`tickets/${id}/comments/${commentId}.md`);
  await logEvent("comment.delete", { id, comment: commentId });
}
