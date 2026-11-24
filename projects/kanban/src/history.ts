// Pure history helpers: device labels, relative times, per-kind English
// descriptions, and the filters the Activity panel and per-card History use.
// No imports — runs under `deno test` without wasm, and is easy to unit test
// in isolation from the board/event-log machinery.

/** One JSON event line: `{ts, device, kind, …data}` — the same shape as `RiftEvent`. */
export type HistoryEvent = { ts: string; device: string; kind: string; [field: string]: unknown };

/** Who's looking: my device id, and the board owner's (for "You"/"(owner)" labels). */
export type Viewer = { me: string; owner: string };

/** Event kinds that belong to a single card (have a card `id` field). */
export const CARD_KINDS = ["card.create", "card.move", "card.check", "card.edit", "comment.add",
  "card.delete", "comment.delete"];

export const ADJECTIVES = [
  "amber", "brisk", "calm", "dusky", "eager", "fern", "gilded", "hazy",
  "ivory", "jade", "keen", "lunar", "misty", "noble", "olive", "plum",
];
export const ANIMALS = [
  "otter", "heron", "lynx", "finch", "badger", "marten", "ibex", "koi",
  "newt", "owl", "puffin", "quail", "raven", "stoat", "tern", "wren",
];

/** The short id shown alongside a device's handle. */
export function shortId(device: string): string {
  return device.slice(0, 8);
}

/** A stable, human-pronounceable handle for a device id, e.g. "amber-wren". */
export function deviceHandle(device: string): string {
  const a = parseInt(device.slice(0, 2), 16);
  const b = parseInt(device.slice(2, 4), 16);
  if (Number.isNaN(a) || Number.isNaN(b)) return "unknown";
  return `${ADJECTIVES[a % 16]}-${ANIMALS[b % 16]}`;
}

/** The label shown for `device` in the viewer `v`'s context: "You", a handle,
 *  or either with " (owner)" appended. */
export function deviceLabel(device: string, v: Viewer): string {
  const base = device !== "" && device === v.me ? "You" : `${deviceHandle(device)} ${shortId(device)}`;
  return device !== "" && device === v.owner ? `${base} (owner)` : base;
}

/** A relative rendering of an ISO timestamp against `nowMs`, coarsening as the
 *  gap grows; falls back to the raw string for unparseable timestamps. */
export function relativeTime(ts: string, nowMs: number): string {
  const t = Date.parse(ts);
  if (Number.isNaN(t)) return ts;
  const s = Math.round((nowMs - t) / 1000);
  if (s < 45) return "just now";
  const m = Math.round(s / 60);
  if (m < 60) return `${m} min ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h} h ago`;
  const d = Math.round(h / 24);
  if (d === 1) return "yesterday";
  if (d < 7) return `${d} days ago`;
  return new Date(t).toISOString().slice(0, 10);
}

/** Whether a `riftpipe:file-changed` path can change the merged event log: any
 *  `events/*` file, or `""` — the library's "the page switched boards" nudge
 *  (the whole tree, events included, is now another board's). */
export function affectsEventLog(path: unknown): boolean {
  return typeof path === "string" && (path === "" || path.startsWith("events/"));
}

/** A full local date-time for an event's tooltip; the raw string when `ts`
 *  doesn't parse (instead of "Invalid Date"). */
export function absoluteTime(ts: string): string {
  const t = Date.parse(ts);
  return Number.isNaN(t) ? ts : new Date(t).toLocaleString();
}

/** Whether `e` is one of the per-card event kinds (has a string `id`). */
export function isCardEvent(e: HistoryEvent): boolean {
  return typeof e.id === "string" && CARD_KINDS.includes(e.kind);
}

/** This card's events, newest first. Does not mutate `evs`. */
export function cardHistory(evs: HistoryEvent[], id: string): HistoryEvent[] {
  return evs.filter((e) => isCardEvent(e) && e.id === id).reverse();
}

/** The board-wide activity feed: optionally hides `mesh.*` sync events, caps to
 *  the newest `limit` (newest first), and reports the pre-cap total. */
export function boardActivity(
  evs: HistoryEvent[],
  opts: { showSync: boolean; limit: number },
): { items: HistoryEvent[]; total: number } {
  const shown = evs.filter((e) => opts.showSync || !e.kind.startsWith("mesh."));
  return { items: shown.slice(-opts.limit).reverse(), total: shown.length };
}

/** Card id → its latest known title, from `card.create`/title `card.edit`
 *  events, oldest to newest. */
export function knownTitles(evs: HistoryEvent[]): Map<string, string> {
  const titles = new Map<string, string>();
  for (const e of evs) {
    if (e.kind === "card.create" && typeof e.title === "string" && typeof e.id === "string") {
      titles.set(e.id, e.title);
    } else if (
      e.kind === "card.edit" && e.field === "title" && typeof e.value === "string" &&
      typeof e.id === "string"
    ) {
      titles.set(e.id, e.value);
    }
  }
  return titles;
}

/** Whether `x` has an event's required shape: an object with string `ts`,
 *  `device` and `kind`. Any peer with the link can write `events/*.jsonl` on an
 *  open board, so a line like `null`, `{}` or `{"kind":1}` must not reach the
 *  renderers (which call string methods on those fields). */
export function isWellFormedEvent(x: unknown): x is HistoryEvent {
  if (typeof x !== "object" || x === null || Array.isArray(x)) return false;
  const e = x as Record<string, unknown>;
  return typeof e.ts === "string" && typeof e.device === "string" && typeof e.kind === "string";
}

/** Only the well-formed events of `xs` (`[]` if `xs` isn't an array), in order. */
export function wellFormed(xs: unknown): HistoryEvent[] {
  return Array.isArray(xs) ? xs.filter(isWellFormedEvent) : [];
}

/** Drop exact duplicate lines (e.g. a merge seeing the same event twice),
 *  keeping the first occurrence and the original order. */
export function dedupe<T extends HistoryEvent>(evs: T[]): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const e of evs) {
    const key = JSON.stringify(e);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(e);
  }
  return out;
}

/** `evs` with each event replaced by its equal (same JSON) object from the
 *  previous load, when there is one — so a keyed list (Solid's `<For>`) keeps
 *  its existing rows and only builds rows for new events. Returns the list and
 *  the key → object map to pass as `prev` next time. */
export function keepIdentity<T>(prev: Map<string, T>, evs: T[]): { list: T[]; byKey: Map<string, T> } {
  const byKey = new Map<string, T>();
  const list = evs.map((e) => {
    const key = JSON.stringify(e);
    const kept = prev.get(key) ?? e;
    byKey.set(key, kept);
    return kept;
  });
  return { list, byKey };
}

/** A one-line English description of event `e`. `titleOf` resolves a card id
 *  to its (possibly historical) title; `inCard` drops the card's own name
 *  since it's already shown as the context (the card detail drawer). */
export function describe(
  e: HistoryEvent,
  titleOf: (id: string) => string,
  v: Viewer,
  inCard: boolean,
): string {
  const str = (x: unknown): string => (typeof x === "string" ? x : "?");
  const q = (s: string): string => `“${s}”`;
  const id = str(e.id);
  const T = q(titleOf(id));
  const who = (d: unknown): string => (d === v.me ? "you" : deviceLabel(str(d), v));

  switch (e.kind) {
    case "card.create": {
      const col = str(e.column);
      if (inCard) return `created in ${col}`;
      const title = typeof e.title === "string" ? e.title : titleOf(id);
      return `created ${q(title)} in ${col}`;
    }
    case "card.move": {
      const from = str(e.from);
      const to = str(e.to);
      return inCard ? `moved from ${from} to ${to}` : `moved ${T} from ${from} to ${to}`;
    }
    case "card.check":
      if (e.done === true) return inCard ? "marked done" : `marked ${T} done`;
      return inCard ? "reopened" : `reopened ${T}`;
    case "card.edit":
      if (e.field === "title") {
        const value = q(str(e.value));
        return inCard ? `renamed to ${value}` : `renamed a card to ${value}`;
      }
      return inCard ? "edited the description" : `edited the description of ${T}`;
    case "comment.add":
      return inCard ? "commented" : `commented on ${T}`;
    case "card.delete": {
      if (inCard) return "deleted the card";
      return `deleted ${q(typeof e.title === "string" ? e.title : titleOf(id))}`;
    }
    case "comment.delete":
      return inCard ? "deleted a comment" : `deleted a comment on ${T}`;
    case "auth.claim":
      return "claimed the board";
    case "auth.request":
      return "asked to join";
    case "auth.grant":
      return typeof e.target === "string" ? `approved ${who(e.target)}` : "approved a device";
    case "auth.revoke":
      return typeof e.target === "string" ? `removed ${who(e.target)}` : "removed a device";
    case "mesh.merge":
      return `received ${str(e.path)}`;
    case "mesh.delete": {
      // A batch (a peer deleted several files at once, e.g. a card): `paths` + common `dir`.
      const n = Array.isArray(e.paths) ? e.paths.length : 1;
      if (n <= 1) return `removed ${str(e.path)}`;
      return typeof e.dir === "string" && e.dir !== "" ? `removed ${n} files under ${e.dir}` : `removed ${n} files`;
    }
    default:
      return e.kind;
  }
}
