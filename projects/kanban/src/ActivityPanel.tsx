// The board-wide Activity panel and the shared event row (also used by the
// per-card History in the card drawer). Reads only the history store, so it
// updates live as the merged event log changes and never re-renders the board.
import { createSignal, For, onMount, Show } from "solid-js";
import {
  boardActivity,
  describe,
  deviceLabel,
  historyEvents,
  historyNow,
  isCardEvent,
  relativeTime,
  type HistoryEvent,
  type Viewer,
} from "./api.ts";

/** The panel shows at most this many of the newest entries. */
export const ACTIVITY_LIMIT = 200;

/** One event: who, when, what. A card event in the board-wide panel is a
 *  `<button>` (keyboard-focusable, Enter/Space) that opens the card. */
export function EventRow(props: {
  event: HistoryEvent;
  viewer: Viewer;
  titleOf: (id: string) => string;
  inCard: boolean;
  onOpenCard?: (id: string) => void;
}) {
  const e = props.event;
  const open = props.onOpenCard && isCardEvent(e) ? props.onOpenCard : undefined;
  const body = () => (
    <>
      <span class="event-who" title={`device ${e.device}`}>{deviceLabel(e.device, props.viewer)}</span>
      <time class="event-when" datetime={e.ts} title={new Date(e.ts).toLocaleString()}>
        {relativeTime(e.ts, historyNow())}
      </time>
      <span class="event-what">{describe(e, props.titleOf, props.viewer, props.inCard)}</span>
    </>
  );
  return (
    <li classList={{ "event-row": true, clickable: !!open }} data-kind={e.kind}>
      {open
        ? (
          <button type="button" class="event-open" onClick={() => open(e.id as string)}>
            {body()}
          </button>
        )
        : body()}
    </li>
  );
}

export function ActivityPanel(props: {
  viewer: () => Viewer;
  titleOf: (id: string) => string;
  onOpenCard: (id: string) => void;
  onClose: () => void;
}) {
  const [showSync, setShowSync] = createSignal(false);
  const view = () => boardActivity(historyEvents(), { showSync: showSync(), limit: ACTIVITY_LIMIT });
  let panel: HTMLElement | undefined;
  // Opening moves focus into the panel so keyboard users land on it.
  onMount(() => panel?.focus());
  return (
    <aside class="activity" id="activity-panel" aria-labelledby="activity-title" tabindex="-1" ref={panel}>
      <div class="activity-head">
        <h2 id="activity-title">Activity</h2>
        <button
          class="activity-close"
          type="button"
          title="Close"
          aria-label="Close activity"
          onClick={() => props.onClose()}
        >
          ×
        </button>
      </div>
      <label class="activity-sync">
        <input
          type="checkbox"
          class="activity-sync-toggle"
          checked={showSync()}
          onChange={(ev) => setShowSync(ev.currentTarget.checked)}
        />
        Show sync events
      </label>
      <ol class="activity-list">
        <For each={view().items} fallback={<li class="event-empty">No activity yet.</li>}>
          {(ev) => (
            <EventRow
              event={ev}
              viewer={props.viewer()}
              titleOf={props.titleOf}
              inCard={false}
              onOpenCard={props.onOpenCard}
            />
          )}
        </For>
      </ol>
      <Show when={view().total > view().items.length}>
        <div class="activity-more">
          Showing the latest {view().items.length} of {view().total}
        </div>
      </Show>
    </aside>
  );
}
