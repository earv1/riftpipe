// Kanban board server — a small JSON file-API over a "board" directory.
//
// Data model (on disk):
//   <DIR>/board.md               "# <Title>" line, then one "- <Column>" per column (in order)
//   <DIR>/tickets/<id>/card.md   "# <Card Title>\n\n<markdown description>"
//   <DIR>/tickets/<id>/meta.toml column (string), position (number), done (boolean)
//   <id> = "tk_" + 8 lowercase hex chars
//
// Everything is read/written as plain files so the board stays human-editable
// and git-friendly. The server is deliberately defensive: missing or malformed
// files fall back to sensible defaults rather than throwing.

import { parse as parseToml, stringify as stringifyToml } from "@std/toml";
import { join } from "@std/path";
import { serveDir } from "@std/http/file-server";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Card {
  id: string;
  title: string;
  column: string;
  position: number;
  done: boolean;
}

interface Board {
  title: string;
  columns: string[];
  cards: Card[];
}

interface Comment {
  id: string;
  author: string;
  ts: string;
  text: string;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DIR = Deno.env.get("KANBAN_DIR") ?? "./board";
const PORT = Number(Deno.env.get("KANBAN_PORT")) || 8000;

const ticketsDir = () => join(DIR, "tickets");
const cardDir = (id: string) => join(ticketsDir(), id);

// ---------------------------------------------------------------------------
// Change-event log (for history, one day)
//
// Every mutation appends a line to events/<site>.jsonl. The key trick: each
// replica writes to its OWN file (named by a per-machine site id), so two peers
// never touch the same file — the log merges across machines with zero
// conflicts. "History" is just every events/*.jsonl merged and sorted by time.
// The board files remain the source of truth; this is a purely additive trail.
// The site id lives in a dotfile (<DIR>/.site) which riftpipe's scan skips, so
// it stays machine-local and each replica gets a distinct events file.
// ---------------------------------------------------------------------------

interface ChangeEvent {
  ts: string;
  site: string;
  kind: string;
  [field: string]: unknown;
}

let SITE: string | null = null;
async function siteId(): Promise<string> {
  if (SITE) return SITE;
  const path = join(DIR, ".site");
  const existing = (await readTextSafe(path)).trim();
  if (existing) return (SITE = existing);
  SITE = crypto.randomUUID().replaceAll("-", "").slice(0, 8);
  try {
    await Deno.mkdir(DIR, { recursive: true });
    await Deno.writeTextFile(path, SITE);
  } catch { /* non-fatal: fall back to an in-memory id for this run */ }
  return SITE;
}

/** Append one change event. Never throws — logging must not break a mutation. */
async function appendEvent(
  kind: string,
  fields: Record<string, unknown>,
): Promise<void> {
  try {
    const site = await siteId();
    const dir = join(DIR, "events");
    await Deno.mkdir(dir, { recursive: true });
    const event: ChangeEvent = { ts: new Date().toISOString(), site, kind, ...fields };
    await Deno.writeTextFile(join(dir, `${site}.jsonl`), JSON.stringify(event) + "\n", {
      append: true,
    });
  } catch { /* swallow */ }
}

/** Merge every events/*.jsonl, sorted oldest→newest (last `limit` kept). */
async function readHistory(limit = 1000): Promise<ChangeEvent[]> {
  const events: ChangeEvent[] = [];
  try {
    for await (const entry of Deno.readDir(join(DIR, "events"))) {
      if (!entry.isFile || !entry.name.endsWith(".jsonl")) continue;
      const text = await readTextSafe(join(DIR, "events", entry.name));
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        try {
          events.push(JSON.parse(line) as ChangeEvent);
        } catch { /* skip a malformed line */ }
      }
    }
  } catch { /* no events/ yet */ }
  events.sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0));
  return events.slice(-limit);
}

// ---------------------------------------------------------------------------
// Small filesystem helpers (all non-throwing where it matters)
// ---------------------------------------------------------------------------

/** Read a text file, returning "" if it does not exist or cannot be read. */
async function readTextSafe(path: string): Promise<string> {
  try {
    return await Deno.readTextFile(path);
  } catch {
    return "";
  }
}

/** Extract the first markdown "# " heading from text, or undefined. */
function firstHeading(text: string): string | undefined {
  for (const line of text.split("\n")) {
    const m = line.match(/^#\s+(.*\S)\s*$/);
    if (m) return m[1];
  }
  return undefined;
}

/** Generate a fresh card id: "tk_" + 8 lowercase hex chars. */
function newId(): string {
  return "tk_" + crypto.randomUUID().replaceAll("-", "").slice(0, 8);
}

// ---------------------------------------------------------------------------
// board.md parsing / column helpers
// ---------------------------------------------------------------------------

/** Parse board.md into a title + ordered column list (robust to a missing file). */
async function readBoardMeta(): Promise<{ title: string; columns: string[] }> {
  const text = await readTextSafe(join(DIR, "board.md"));
  const title = firstHeading(text) ?? "Board";
  const columns: string[] = [];
  for (const line of text.split("\n")) {
    const m = line.match(/^-\s+(.*\S)\s*$/);
    if (m) columns.push(m[1]);
  }
  return { title, columns };
}

// ---------------------------------------------------------------------------
// Card meta parsing / reading
// ---------------------------------------------------------------------------

/**
 * Read a single card from <DIR>/tickets/<id>.
 * Falls back to defaults for any missing/blank/invalid piece.
 */
async function readCard(id: string, columns: string[]): Promise<Card> {
  const defaultColumn = columns[0] ?? "Todo";

  // meta.toml — tolerate missing/blank/invalid TOML.
  let meta: Record<string, unknown> = {};
  const metaText = await readTextSafe(join(cardDir(id), "meta.toml"));
  if (metaText.trim()) {
    try {
      meta = parseToml(metaText) as Record<string, unknown>;
    } catch {
      meta = {};
    }
  }

  const column = typeof meta.column === "string" && meta.column
    ? meta.column
    : defaultColumn;
  const position = typeof meta.position === "number" ? meta.position : 0;
  const done = typeof meta.done === "boolean" ? meta.done : false;

  // card.md — title comes from the first heading, fallback to the id.
  const cardText = await readTextSafe(join(cardDir(id), "card.md"));
  const title = firstHeading(cardText) ?? id;

  return { id, title, column, position, done };
}

/** Read the whole board: meta + every card under tickets/. */
async function readBoard(): Promise<Board> {
  const { title, columns } = await readBoardMeta();

  const cards: Card[] = [];
  try {
    for await (const entry of Deno.readDir(ticketsDir())) {
      if (!entry.isDirectory) continue;
      cards.push(await readCard(entry.name, columns));
    }
  } catch {
    // tickets/ dir missing — that's fine, no cards yet.
  }

  // Order is the frontend's responsibility; return in any order.
  return { title, columns, cards };
}

// ---------------------------------------------------------------------------
// Writing helpers
// ---------------------------------------------------------------------------

/** Serialize a card's meta fields to meta.toml. */
async function writeMeta(id: string, card: Card): Promise<void> {
  const toml = stringifyToml({
    column: card.column,
    position: card.position,
    done: card.done,
  });
  await Deno.writeTextFile(join(cardDir(id), "meta.toml"), toml);
}

/** Serialize a card's title + description to card.md. */
async function writeCardMd(
  id: string,
  title: string,
  description: string,
): Promise<void> {
  const body = description.trim().length > 0
    ? `# ${title}\n\n${description.trimEnd()}\n`
    : `# ${title}\n`;
  await Deno.writeTextFile(join(cardDir(id), "card.md"), body);
}

/** Split card.md into its title (first heading) and the remaining description. */
function splitCardMd(text: string): { title: string; description: string } {
  const lines = text.split("\n");
  let title = "";
  let i = 0;
  for (; i < lines.length; i++) {
    const m = lines[i].match(/^#\s+(.*\S)\s*$/);
    if (m) {
      title = m[1];
      i++;
      break;
    }
  }
  // Description = everything after the heading, with leading blank lines trimmed.
  const description = lines.slice(i).join("\n").replace(/^\n+/, "").trimEnd();
  return { title, description };
}

// ---------------------------------------------------------------------------
// Comments — one markdown file per comment under tickets/<id>/comments/
//
// Filename scheme: "<ts>__<author>.md" where <ts> is a filename-safe ISO
// timestamp (":" → "-", so it still sorts chronologically) and <author> is
// sanitized to [a-z0-9-]+. The double-underscore separator keeps parsing
// unambiguous even though the timestamp itself contains "-". File contents are
// the raw markdown comment body.
// ---------------------------------------------------------------------------

const commentsDir = (id: string) => join(cardDir(id), "comments");

/** Sanitize an author into a filename-safe slug: [a-z0-9-]+, or "anon". */
function sanitizeAuthor(author: string): string {
  const slug = author.toLowerCase().replace(/[^a-z0-9-]+/g, "-");
  return slug || "anon";
}

/** Read every comment for a ticket, oldest first. Robust to a missing dir. */
async function readComments(id: string): Promise<Comment[]> {
  const comments: Comment[] = [];
  try {
    for await (const entry of Deno.readDir(commentsDir(id))) {
      if (!entry.isFile || !entry.name.endsWith(".md")) continue;
      const name = entry.name.slice(0, -".md".length);
      const sep = name.indexOf("__");
      // Skip files that don't match the "<ts>__<author>" scheme.
      if (sep < 0) continue;
      const ts = name.slice(0, sep);
      const author = name.slice(sep + 2);
      const text = (await readTextSafe(join(commentsDir(id), entry.name))).trimEnd();
      comments.push({ id: name, author, ts, text });
    }
  } catch {
    // comments/ dir missing — no comments yet.
  }
  comments.sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0));
  return comments;
}

/** Create a new comment file and record a change event. */
async function addComment(
  id: string,
  author: string,
  text: string,
): Promise<Comment> {
  const slug = sanitizeAuthor(author);
  const ts = new Date().toISOString().replaceAll(":", "-");
  const name = `${ts}__${slug}`;
  await Deno.mkdir(commentsDir(id), { recursive: true });
  await Deno.writeTextFile(join(commentsDir(id), `${name}.md`), text);
  await appendEvent("comment.add", { id, comment: name });
  return { id: name, author: slug, ts, text };
}

// ---------------------------------------------------------------------------
// API operations
// ---------------------------------------------------------------------------

/** Create a new card at the bottom of its column. */
async function createCard(column: string, title: string): Promise<Card> {
  const board = await readBoard();
  const col = column || board.columns[0] || "Todo";

  // Next position = (max position in that column) + 1.
  const inColumn = board.cards.filter((c) => c.column === col);
  const maxPos = inColumn.reduce((m, c) => Math.max(m, c.position), -1);

  const id = newId();
  const card: Card = {
    id,
    title: title || id,
    column: col,
    position: maxPos + 1,
    done: false,
  };

  // Lay out the on-disk structure: card dir + an empty comments/ dir.
  await Deno.mkdir(cardDir(id), { recursive: true });
  await Deno.mkdir(join(cardDir(id), "comments"), { recursive: true });
  await writeCardMd(id, card.title, "");
  await writeMeta(id, card);

  await appendEvent("card.create", { id, column: col, title: card.title });
  return card;
}

interface CardPatch {
  column?: string;
  position?: number;
  done?: boolean;
  title?: string;
  description?: string;
}

/** Apply a partial update to an existing card and persist the result. */
async function patchCard(id: string, patch: CardPatch): Promise<Card | null> {
  // Ensure the card actually exists before mutating.
  try {
    const stat = await Deno.stat(cardDir(id));
    if (!stat.isDirectory) return null;
  } catch {
    return null;
  }

  const { columns } = await readBoardMeta();
  const current = await readCard(id, columns);
  // Snapshot originals so we can record exactly what changed.
  const before = { column: current.column, done: current.done, title: current.title };

  // --- meta fields (column / position / done) ---
  if (typeof patch.column === "string") current.column = patch.column;
  if (typeof patch.position === "number") current.position = patch.position;
  if (typeof patch.done === "boolean") current.done = patch.done;
  await writeMeta(id, current);

  // --- card.md (title / description) ---
  // Preserve whichever part is not being changed.
  let descriptionChanged = false;
  if (patch.title !== undefined || patch.description !== undefined) {
    const existing = splitCardMd(await readTextSafe(join(cardDir(id), "card.md")));
    const title = patch.title !== undefined ? patch.title : existing.title;
    const description = patch.description !== undefined
      ? patch.description
      : existing.description;
    descriptionChanged = patch.description !== undefined &&
      patch.description !== existing.description;
    await writeCardMd(id, title || id, description);
    current.title = title || id;
  }

  // --- record change events (additive history) ---
  if (current.column !== before.column) {
    await appendEvent("card.move", { id, from: before.column, to: current.column });
  }
  if (current.done !== before.done) {
    await appendEvent("card.check", { id, done: current.done });
  }
  if (current.title !== before.title) {
    await appendEvent("card.edit", { id, field: "title", value: current.title });
  }
  if (descriptionChanged) {
    await appendEvent("card.edit", { id, field: "description" });
  }

  return current;
}

// ---------------------------------------------------------------------------
// Server-Sent Events — push change notifications to subscribed clients
//
// One background task watches DIR with Deno.watchFs and broadcasts a small
// JSON message for each changed ticket / board edit. Both local mutations
// (our own POST/PATCH writes) and riftpipe-delivered file changes flow through
// the same path: a file changes on disk → watchFs fires → we broadcast. The
// client then refetches only the affected card (or board meta).
// ---------------------------------------------------------------------------

const encoder = new TextEncoder();
const sseClients = new Set<ReadableStreamDefaultController<Uint8Array>>();

/** Send one SSE message object to every connected client. */
function broadcast(msg: Record<string, unknown>): void {
  const frame = encoder.encode(`data: ${JSON.stringify(msg)}\n\n`);
  for (const controller of sseClients) {
    try {
      controller.enqueue(frame);
    } catch {
      // A dead controller; it'll be cleaned up on cancel.
    }
  }
}

/** Build an SSE Response that registers/unregisters its controller. */
function sseResponse(): Response {
  let self: ReadableStreamDefaultController<Uint8Array>;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      self = controller;
      sseClients.add(controller);
    },
    cancel() {
      sseClients.delete(self);
    },
  });
  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "connection": "keep-alive",
    },
  });
}

/** Map a changed filesystem path to an SSE message, or null to ignore it. */
function messageForPath(path: string): Record<string, unknown> | null {
  const norm = path.replaceAll("\\", "/");
  // Ignore the per-machine event log and the .site dotfile (pure noise).
  if (norm.includes("/events/") || norm.endsWith("/.site")) return null;
  const m = norm.match(/\/tickets\/([^/]+)(?:\/|$)/);
  if (m) return { type: "ticket", id: m[1] };
  if (norm.endsWith("board.md")) return { type: "board" };
  return null;
}

/** Watch DIR and broadcast debounced change notifications. Never exits. */
async function watchLoop(): Promise<void> {
  // Collect distinct messages across a short debounce window, then flush.
  let pending = new Map<string, Record<string, unknown>>();
  let timer: number | undefined;

  const flush = () => {
    timer = undefined;
    const batch = pending;
    pending = new Map();
    for (const msg of batch.values()) broadcast(msg);
  };

  for (;;) {
    try {
      await Deno.mkdir(DIR, { recursive: true });
      const watcher = Deno.watchFs(DIR);
      for await (const ev of watcher) {
        for (const p of ev.paths) {
          const msg = messageForPath(p);
          if (!msg) continue;
          pending.set(JSON.stringify(msg), msg);
        }
        if (pending.size > 0 && timer === undefined) {
          timer = setTimeout(flush, 80);
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`[kanban] watch error: ${message} — retrying in 1s`);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

// ---------------------------------------------------------------------------
// HTTP routing
// ---------------------------------------------------------------------------

const CARD_ID_RE = /^\/api\/cards\/([^/]+)$/;
const CARD_DETAIL_RE = /^\/api\/cards\/([^/]+)\/detail$/;
const CARD_COMMENTS_RE = /^\/api\/cards\/([^/]+)\/comments$/;

async function handler(req: Request): Promise<Response> {
  try {
    const url = new URL(req.url);
    const path = url.pathname;

    // GET /api/board
    if (path === "/api/board" && req.method === "GET") {
      return Response.json(await readBoard());
    }

    // GET /api/history — merged change-event log (newest last)
    if (path === "/api/history" && req.method === "GET") {
      return Response.json(await readHistory());
    }

    // GET /api/events — SSE stream of change notifications
    if (path === "/api/events" && req.method === "GET") {
      return sseResponse();
    }

    // GET /api/cards/:id/detail — full ticket detail (incl. description + comments)
    const detailMatch = path.match(CARD_DETAIL_RE);
    if (detailMatch && req.method === "GET") {
      const id = detailMatch[1];
      try {
        const stat = await Deno.stat(cardDir(id));
        if (!stat.isDirectory) return new Response("Not Found", { status: 404 });
      } catch {
        return new Response("Not Found", { status: 404 });
      }
      const { columns } = await readBoardMeta();
      const card = await readCard(id, columns);
      const { description } = splitCardMd(
        await readTextSafe(join(cardDir(id), "card.md")),
      );
      const comments = await readComments(id);
      return Response.json({
        id: card.id,
        title: card.title,
        column: card.column,
        position: card.position,
        done: card.done,
        description,
        comments,
      });
    }

    // POST /api/cards/:id/comments — add a comment to a ticket
    const commentsMatch = path.match(CARD_COMMENTS_RE);
    if (commentsMatch && req.method === "POST") {
      const id = commentsMatch[1];
      try {
        const stat = await Deno.stat(cardDir(id));
        if (!stat.isDirectory) return new Response("Not Found", { status: 404 });
      } catch {
        return new Response("Not Found", { status: 404 });
      }
      const body = await req.json() as { author?: string; text?: string };
      const text = (body.text ?? "").trim();
      if (!text) return new Response("Comment text is required", { status: 400 });
      const comment = await addComment(id, body.author ?? "anon", text);
      return Response.json(comment);
    }

    // GET /api/cards/:id — a single card
    const getIdMatch = path.match(CARD_ID_RE);
    if (getIdMatch && req.method === "GET") {
      const id = getIdMatch[1];
      try {
        const stat = await Deno.stat(cardDir(id));
        if (!stat.isDirectory) return new Response("Not Found", { status: 404 });
      } catch {
        return new Response("Not Found", { status: 404 });
      }
      const { columns } = await readBoardMeta();
      return Response.json(await readCard(id, columns));
    }

    // POST /api/cards
    if (path === "/api/cards" && req.method === "POST") {
      const body = await req.json() as { column?: string; title?: string };
      const card = await createCard(body.column ?? "", body.title ?? "");
      return Response.json(card);
    }

    // PATCH /api/cards/:id
    const idMatch = path.match(CARD_ID_RE);
    if (idMatch && req.method === "PATCH") {
      const body = await req.json() as CardPatch;
      const card = await patchCard(idMatch[1], body);
      if (!card) return new Response("Not Found", { status: 404 });
      return Response.json(card);
    }

    // Unknown /api route.
    if (path.startsWith("/api/")) {
      return new Response("Not Found", { status: 404 });
    }

    // --- Static files (built SPA) ---
    // Serve from dist/; on 404 fall back to index.html (client-side routing).
    // If dist/ doesn't exist (dev mode), just 404 — never crash.
    try {
      const res = await serveDir(req, { fsRoot: "dist", quiet: true });
      if (res.status === 404) {
        try {
          const html = await Deno.readTextFile(join("dist", "index.html"));
          return new Response(html, {
            status: 200,
            headers: { "content-type": "text/html; charset=utf-8" },
          });
        } catch {
          return new Response("Not Found", { status: 404 });
        }
      }
      return res;
    } catch {
      return new Response("Not Found", { status: 404 });
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return new Response(message, { status: 500 });
  }
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

console.error(`[kanban] serving ${DIR} on http://localhost:${PORT}`);
// Start the single filesystem watcher that powers SSE push updates.
watchLoop();
Deno.serve({ port: PORT }, handler);
