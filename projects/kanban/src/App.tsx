import { createSignal, onMount, For, Show } from "solid-js";
import { createStore, reconcile } from "solid-js/store";
import { getBoard, addCard, patchCard, connectPeer, newBoard, watch, storageSupported, auth, type AuthState, type PendingRequest, type Card } from "./api.ts";
import { CardDetail } from "./CardDetail.tsx";

export function App() {
  const [board, setBoard] = createStore<{
    title: string;
    columns: string[];
    cards: Card[];
    loaded: boolean;
  }>({ title: "", columns: [], cards: [], loaded: false });

  const [openId, setOpenId] = createSignal<string | null>(null);
  const [detailTick, setDetailTick] = createSignal(0);
  // OPFS failed at runtime (e.g. WebKit without working storage) — the board
  // is view-only and nothing persists; say so instead of spinning forever.
  const [storageBroken, setStorageBroken] = createSignal(false);
  // Auth/login state (from the lib): whether the board is locked (has an
  // author), and whether this device is the author / authorized to write.
  const [authState, setAuthState] = createSignal<AuthState>(
    { locked: false, device: "", authorized: false, isAuthor: false, author: "", requested: false },
  );
  // The author's inbox: who has asked for write access (device id + safety code).
  const [pending, setPending] = createSignal<PendingRequest[]>([]);
  const refreshAuth = () => {
    setAuthState(auth.state());
    setPending(auth.pending());
  };
  const lock = () => { auth.lockBoard(); refreshAuth(); };
  const askForAccess = () => { auth.requestAccess(); refreshAuth(); };
  const approve = (device: string) => { auth.grantDevice(device); refreshAuth(); };

  // A mesh peer announced a different riftpipe protocol version (or predates
  // versioning). Detection lives in the library; we only render its event.
  const [versionMismatch, setVersionMismatch] = createSignal<{ theirs: number; ours: number } | null>(null);
  // Drag-and-drop: which card is being dragged, and which column is hovered.
  const [draggedId, setDraggedId] = createSignal<string | null>(null);
  const [dragOverCol, setDragOverCol] = createSignal<string | null>(null);

  // Reload the whole board (cheap — it's local OPFS) and nudge an open card.
  const refresh = async () => {
    try {
      const b = await getBoard();
      setBoard("title", b.title);
      setBoard("columns", reconcile(b.columns));
      setBoard("cards", reconcile(b.cards, { key: "id" }));
      setBoard("loaded", true);
      if (openId()) setDetailTick((n) => n + 1);
    } catch (_e) {
      // Transient once loaded — the next event retries. But if the very first
      // load fails, storage is broken: show a default board + warning rather
      // than an eternal spinner.
      if (!board.loaded) {
        setBoard("title", "My Board");
        setBoard("columns", reconcile(["Todo", "Doing", "Done"]));
        setBoard("cards", reconcile([], { key: "id" }));
        setBoard("loaded", true);
        setStorageBroken(true);
      }
    }
  };

  onMount(() => {
    // One subscription for every change to the synced tree — our own edits, a
    // peer's merged edit, or another tab in this profile. All funnel through the
    // file API's `watch`; each just re-reads the board. No SSE, no server.
    watch(() => void refresh());

    // Connect P2P when the URL carries a connection id / ticket. No id =>
    // single-player, no server. Merges arrive via `watch` above.
    let lastHash: string | null = null; // null => never connected (empty hash is valid)
    const connect = async () => {
      if (location.hash === lastHash) return; // e.g. the host writing its own ticket
      lastHash = location.hash;
      try {
        await connectPeer();
      } catch (_e) {
        // no peer / signaling unavailable — runs fine solo
      }
      lastHash = location.hash; // a host may have just set the hash; don't re-trigger
    };

    (async () => {
      await refresh();
      await connect();
      refreshAuth();
    })();

    // Poll the lib's auth state — it changes as the board is locked or as
    // grants/genesis arrive from peers over the mesh.
    setInterval(refreshAuth, 2000);

    // Pasting a share link into this tab changes the hash — reconnect to it.
    window.addEventListener("hashchange", () => void connect());

    // The library fires this when a peer speaks a different protocol version.
    window.addEventListener("riftpipe:protocol-mismatch", (e) => {
      try {
        const d = JSON.parse((e as CustomEvent<string>).detail);
        setVersionMismatch({ theirs: d.theirs, ours: d.ours });
      } catch (_e) {
        setVersionMismatch({ theirs: -1, ours: -1 });
      }
    });
  });

  const cardsIn = (col: string): Card[] =>
    board.cards
      .filter((c) => c.column === col)
      .sort((a, b) => a.position - b.position);

  const toggleDone = async (card: Card) => {
    // SSE round-trip (file write → watchFs → event) delivers the update.
    await patchCard(card.id, { done: !card.done });
  };

  // Drop a card into a column: move it (and append to the bottom of that column).
  const moveToColumn = async (cardId: string, col: string) => {
    const card = board.cards.find((c) => c.id === cardId);
    if (!card || card.column === col) return;
    const end = board.cards
      .filter((c) => c.column === col)
      .reduce((m, c) => Math.max(m, c.position), 0) + 1;
    await patchCard(cardId, { column: col, position: end });
  };

  return (
    <Show when={board.loaded} fallback={<div class="loading">loading…</div>}>
      <Show when={!storageSupported() || storageBroken()}>
        <div class="storage-warning">
          This browser can't store boards (OPFS unavailable — needs Safari/iOS
          18.4+, Chrome, Edge, or Firefox). You can look around, but edits
          won't be saved or synced.
        </div>
      </Show>
      <Show when={versionMismatch()}>
        {(m) => (
          <div class="version-warning">
            A connected peer runs a different riftpipe version
            {m().theirs >= 0 ? ` (theirs v${m().theirs}, yours v${m().ours})` : ""} —
            sync with it may be incomplete. Everyone should reload or update to
            the same version.
          </div>
        )}
      </Show>
      <header class="topbar">
        <h1>{board.title}</h1>
        <button
          class="new-board"
          title="Start a fresh, empty board with a new share link"
          onClick={() => {
            if (
              confirm(
                "Start a fresh board? This clears the local board and its share link.",
              )
            ) {
              void newBoard();
            }
          }}
        >
          New board
        </button>
      </header>
      <div class="auth-bar">
        <Show when={!authState().locked}>
          <button
            class="lock-btn"
            title="Become this board's owner — only you (and people you grant) can edit; others can view"
            onClick={lock}
          >
            🔓 Claim board (you become the owner)
          </button>
          <span class="auth-hint">anyone with the link can edit until you claim it</span>
        </Show>
        <Show when={authState().locked && authState().isAuthor}>
          <span class="auth-status owner">🔒 You own this board</span>
          <Show
            when={pending().length > 0}
            fallback={<span class="auth-hint">no access requests — people who ask will show up here</span>}
          >
            <span class="auth-hint">access requests:</span>
            <For each={pending()}>
              {(req) => (
                <span class="req">
                  <span class="req-id" title={`device ${req.device}\nsafety code ${req.safety}`}>
                    {req.name || "someone"} · {req.device.slice(0, 8)}… · {req.safety}
                  </span>
                  <button class="approve-btn" onClick={() => approve(req.device)}>Approve</button>
                </span>
              )}
            </For>
          </Show>
        </Show>
        <Show when={authState().locked && authState().authorized && !authState().isAuthor}>
          <span class="auth-status editor">🔒 Claimed · you can edit</span>
        </Show>
        <Show when={authState().locked && !authState().authorized}>
          <span class="auth-status viewer">🔒 Claimed · view only</span>
          <Show
            when={!authState().requested}
            fallback={<span class="auth-hint">✓ access requested — waiting for the owner to approve</span>}
          >
            <button class="ask-btn" onClick={askForAccess}>Ask for edit access</button>
          </Show>
        </Show>
        <span class="device-id" title="this browser's device id">
          id: {authState().device.slice(0, 10)}…
        </span>
      </div>
      <main class="columns">
        <For each={board.columns}>
          {(col) => (
            <section
              classList={{ column: true, "drag-over": dragOverCol() === col }}
              onDragOver={(e) => {
                e.preventDefault(); // allow drop
                setDragOverCol(col);
              }}
              onDragLeave={(e) => {
                // Only clear when truly leaving the column, not entering a child.
                if (!e.currentTarget.contains(e.relatedTarget as Node)) {
                  setDragOverCol((c) => (c === col ? null : c));
                }
              }}
              onDrop={(e) => {
                e.preventDefault();
                const id = e.dataTransfer?.getData("text/plain") || draggedId();
                setDragOverCol(null);
                setDraggedId(null);
                if (id) moveToColumn(id, col);
              }}
            >
              <h2 class="column-title">{col}</h2>
              <div class="cards">
                <For each={cardsIn(col)}>
                  {(card) => (
                    <article
                      classList={{
                        card: true,
                        done: card.done,
                        dragging: draggedId() === card.id,
                      }}
                      draggable={true}
                      onDragStart={(e) => {
                        setDraggedId(card.id);
                        e.dataTransfer?.setData("text/plain", card.id);
                        if (e.dataTransfer) e.dataTransfer.effectAllowed = "move";
                      }}
                      onDragEnd={() => {
                        setDraggedId(null);
                        setDragOverCol(null);
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={card.done}
                        onChange={() => toggleDone(card)}
                      />
                      <span
                        class="title title-link"
                        onClick={() => setOpenId(card.id)}
                      >
                        {card.title}
                      </span>
                    </article>
                  )}
                </For>
              </div>
              <AddCard col={col} />
            </section>
          )}
        </For>
      </main>
      <Show when={openId()}>
        <CardDetail
          id={openId()!}
          onClose={() => setOpenId(null)}
          refreshSignal={() => detailTick()}
        />
      </Show>
    </Show>
  );
}

function AddCard(props: { col: string }) {
  const [text, setText] = createSignal("");

  const submit = async (e: Event) => {
    e.preventDefault();
    const title = text().trim();
    if (!title) return;
    // The new card arrives via SSE; no manual refresh needed.
    await addCard(props.col, title);
    setText("");
  };

  return (
    <form class="add-card" onSubmit={submit}>
      <input
        type="text"
        placeholder="Add a card…"
        value={text()}
        onInput={(e) => setText(e.currentTarget.value)}
      />
      <button type="submit">+</button>
    </form>
  );
}
