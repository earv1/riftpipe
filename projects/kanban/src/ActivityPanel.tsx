// The board-wide Activity panel and the shared event row (also used by the
// per-card History in the card drawer). Reads only the history store, so it
// updates live as the merged event log changes and never re-renders the board.
import { createSignal, For, Show } from "solid-js";
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

export function EventRow(props: {
  event: HistoryEvent;
  viewer: Viewer;
  titleOf: (id: string) => string;
  inCard: boolean;
  onOpenCard?: (id: string) => void;
}) {
  const e = props.event;
  const clickable = () => !!(props.onOpenCard && isCardEvent(e));
  return (
    <li
      classList={{ "event-row": true, clickable: clickable() }}
      data-kind={e.kind}
      onClick={() => {
        if (clickable()) props.onOpenCard!(e.id as string);
      }}
    >
      <span class="event-who" title={`device ${e.device}`}>{deviceLabel(e.device, props.viewer)}</span>
      <time class="event-when" datetime={e.ts} title={new Date(e.ts).toLocaleString()}>
        {relativeTime(e.ts, historyNow())}
      </time>
      <span class="event-what">{describe(e, props.titleOf, props.viewer, props.inCard)}</span>
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
  return (
    <aside class="activity">
      <div class="activity-head">
        <h2>Activity</h2>
        <button class="activity-close" type="button" title="Close" onClick={() => props.onClose()}>
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
