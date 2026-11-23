// The shared, live history store: the Activity panel and per-card History both
// read this and nothing else — it never touches the board store, so `events/*`
// changes never re-render the board (`App.tsx`'s `watch` ignores them too).
import { createSignal } from "solid-js";
import { readEvents, watchEvents, type RiftEvent } from "./events.ts";
import { dedupe, wellFormed } from "./history.ts";

const [events, setEvents] = createSignal<RiftEvent[]>([]);
const [now, setNow] = createSignal(Date.now());
export { events, now };

const REFRESH_DEBOUNCE_MS = 150;
let started = false;
let timer: ReturnType<typeof setTimeout> | null = null;

async function reload(): Promise<void> {
  try {
    // Malformed lines (any peer can write events/* on an open board) are dropped.
    setEvents(dedupe(wellFormed(await readEvents())));
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

/** Start once: load the merged log and re-read (debounced) whenever it changes. */
export function startHistory(): void {
  if (started) return;
  started = true;
  watchEvents(schedule);
  setInterval(() => setNow(Date.now()), 30_000);
  void reload();
}
