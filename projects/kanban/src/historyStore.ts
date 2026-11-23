// The shared, live history store: the Activity panel and per-card History both
// read this and nothing else — it never touches the board store, so `events/*`
// changes never re-render the board (`App.tsx`'s `watch` ignores them too).
import { createSignal } from "solid-js";
import { readEvents, watchEvents, type RiftEvent } from "./events.ts";
import { dedupe, keepIdentity, wellFormed } from "./history.ts";

const [events, setEvents] = createSignal<RiftEvent[]>([]);
const [now, setNow] = createSignal(Date.now());
export { events, now };

const REFRESH_DEBOUNCE_MS = 150;
let started = false;
let timer: ReturnType<typeof setTimeout> | null = null;
// The last load's events by JSON key: unchanged events keep their objects, so
// the panels' <For> lists reuse rows instead of rebuilding them all.
let byKey = new Map<string, RiftEvent>();

async function reload(): Promise<void> {
  try {
    // Malformed lines (any peer can write events/* on an open board) are dropped.
    const next = keepIdentity(byKey, dedupe(wellFormed(await readEvents())) as RiftEvent[]);
    byKey = next.byKey;
    setEvents(next.list);
  } catch {
    /* storage unavailable — keep the last view */
  }
}

function schedule(): void {
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    void reload();
  }, REFRESH_DEBOUNCE_MS);
}

/** Start once, on first display (Activity panel or a card drawer): load the
 *  merged log and re-read (debounced) whenever it changes. Not started at app
 *  load — a peer nobody is looking at shouldn't re-read every log on every
 *  merge. */
export function startHistory(): void {
  if (started) return;
  started = true;
  watchEvents(schedule);
  setInterval(() => setNow(Date.now()), 30_000);
  void reload();
}
