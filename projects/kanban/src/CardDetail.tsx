import { createSignal, createEffect, For, Show } from "solid-js";
import {
  getCardDetail,
  patchCard,
  addComment,
  type CardDetail as CardDetailT,
} from "./api.ts";

function formatTs(ts: string): string {
  const d = new Date(ts);
  if (isNaN(d.getTime())) return ts;
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function CardDetail(props: {
  id: string;
  onClose: () => void;
  refreshSignal: () => number;
}) {
  const [detail, setDetail] = createSignal<CardDetailT | null>(null);
  const [loading, setLoading] = createSignal(true);
  const [notFound, setNotFound] = createSignal(false);

  // Local editable text — kept separate from server detail so a remote
  // update never clobbers what the user is mid-typing.
  const [title, setTitle] = createSignal("");
  const [description, setDescription] = createSignal("");

  // Focus flags: while a field is focused we don't overwrite its local value.
  let titleFocused = false;
  let descFocused = false;

  const [comment, setComment] = createSignal("");

  const load = async () => {
    const d = await getCardDetail(props.id);
    setLoading(false);
    if (!d) {
      setNotFound(true);
      setDetail(null);
      return;
    }
    setNotFound(false);
    setDetail(d);
    // Only seed local fields if the user isn't actively editing them.
    if (!titleFocused) setTitle(d.title);
    if (!descFocused) setDescription(d.description);
  };

  // Initial load + re-fetch whenever the id or the refresh tick changes.
  createEffect(() => {
    props.id;
    props.refreshSignal();
    void load();
  });

  // Live save — NO debounce. Each field keeps at most one request in flight and
  // immediately re-saves the newest value when that request lands, so keystrokes
  // stream out as fast as the round-trips allow (not "only when you stop"), and
  // a slower older request can never overwrite a newer one.
  function liveSaver(key: "title" | "description", value: () => string) {
    let inFlight = false;
    let dirty = false;
    const run = async () => {
      if (inFlight) {
        dirty = true; // the in-flight loop will pick up the newer value
        return;
      }
      inFlight = true;
      try {
        do {
          dirty = false;
          const patch = key === "title"
            ? { title: value() }
            : { description: value() };
          await patchCard(props.id, patch);
        } while (dirty);
      } finally {
        inFlight = false;
      }
    };
    return () => void run();
  }

  const saveTitle = liveSaver("title", title);
  const saveDescription = liveSaver("description", description);

  const onTitleInput = (v: string) => {
    setTitle(v);
    saveTitle();
  };

  const onDescInput = (v: string) => {
    setDescription(v);
    saveDescription();
  };

  const toggleDone = async (done: boolean) => {
    await patchCard(props.id, { done });
  };

  const submitComment = async (e: Event) => {
    e.preventDefault();
    const text = comment().trim();
    if (!text) return;
    setComment("");
    await addComment(props.id, text);
    // Refresh to pick up the new comment (peers see it via SSE too).
    void load();
  };

  return (
    <>
      <div class="drawer-backdrop" onClick={() => props.onClose()} />
      <aside class="drawer">
        <button class="drawer-close" type="button" onClick={() => props.onClose()}>
          ×
        </button>

        <Show
          when={!loading()}
          fallback={<div class="drawer-loading">loading…</div>}
        >
          <Show
            when={!notFound()}
            fallback={
              <div class="drawer-notfound">
                <p>not found</p>
                <button type="button" onClick={() => props.onClose()}>
                  Close
                </button>
              </div>
            }
          >
            <input
              class="detail-title"
              type="text"
              value={title()}
              onFocus={() => (titleFocused = true)}
              onBlur={() => {
                titleFocused = false;
                void saveTitle();
              }}
              onInput={(e) => onTitleInput(e.currentTarget.value)}
            />

            <div class="detail-meta">
              <label class="detail-done">
                <input
                  type="checkbox"
                  checked={detail()?.done ?? false}
                  onChange={(e) => void toggleDone(e.currentTarget.checked)}
                />
                Done
              </label>
              <span class="detail-column">{detail()?.column}</span>
            </div>

            <label class="detail-label">Description</label>
            <textarea
              class="detail-description"
              value={description()}
              onFocus={() => (descFocused = true)}
              onBlur={() => {
                descFocused = false;
                void saveDescription();
              }}
              onInput={(e) => onDescInput(e.currentTarget.value)}
            />

            <h3 class="detail-label">Comments</h3>
            <div class="comments">
              <For
                each={detail()?.comments ?? []}
                fallback={<div class="comments-empty">No comments yet.</div>}
              >
                {(c) => (
                  <div class="comment">
                    <div class="comment-head">
                      <span class="comment-author">{c.author}</span>
                      <span class="comment-ts">{formatTs(c.ts)}</span>
                    </div>
                    <div class="comment-text">{c.text}</div>
                  </div>
                )}
              </For>
            </div>

            <form class="add-comment" onSubmit={submitComment}>
              <textarea
                placeholder="Add a comment…"
                value={comment()}
                onInput={(e) => setComment(e.currentTarget.value)}
              />
              <button type="submit">Comment</button>
            </form>
          </Show>
        </Show>
      </aside>
    </>
  );
}
