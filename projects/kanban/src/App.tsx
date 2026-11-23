import { createMemo, createSignal, onMount, For, Show } from "solid-js";
import { createStore, reconcile } from "solid-js/store";
import { getBoard, addCard, patchCard, flushPendingEdits, connectPeer, newBoard, watch, storageSupported, auth, meshStatus, onMeshStatus, historyEvents, knownTitles, type AuthState, type PendingRequest, type Member, type Card, type MeshStatus, type Viewer } from "./api.ts";
import { CardDetail } from "./CardDetail.tsx";
import { ActivityPanel } from "./ActivityPanel.tsx";

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
    {
      locked: false, device: "", authorized: false, isAuthor: false, author: "", requested: false, canRead: true,
      epoch: 0, isAdmin: false, passkeys: 0, passkeyHere: false,
    },
  );
  // The author's inbox: who has asked for write access (device id + safety code).
  const [pending, setPending] = createSignal<PendingRequest[]>([]);
  // Every authorized device (restricted boards): lets the owner remove one.
  const [members, setMembers] = createSignal<Member[]>([]);
  // Whether this page can use passkey device-linking (secure domain origin;
  // wasm is only initialized by the time refreshAuth first runs).
  const [canPasskey, setCanPasskey] = createSignal(false);
  const [passkeyMsg, setPasskeyMsg] = createSignal("");
  // Live peer-to-peer connection status (waiting/connected/reconnecting/offline),
  // from the lib: a status pill in the header.
  const [mesh, setMesh] = createSignal<MeshStatus>({ state: "idle", peers: 0 });
  const meshLabel = (s: MeshStatus) =>
    ({
      waiting: "waiting for peers…",
      connected: `● connected · ${s.peers}`,
      reconnecting: "reconnecting…",
      offline: "offline — edits are saved and sync when you're back",
      idle: "",
    })[s.state];
  const refreshAuth = () => {
    setAuthState(auth.state());
    setPending(auth.pending());
    setMembers(auth.members());
    setCanPasskey(auth.passkeySupported());
  };
  const lock = () => { auth.lockBoard(); refreshAuth(); };
  const askForAccess = () => { auth.requestAccess(); refreshAuth(); };
  const approve = (device: string) => { auth.grantDevice(device); refreshAuth(); };
  const revoke = (device: string) => { auth.revokeDevice(device); refreshAuth(); };
  const claimPasskey = async () => {
    setPasskeyMsg("");
    if (!(await auth.claimWithPasskey())) setPasskeyMsg("passkey not set up (cancelled or unsupported)");
    refreshAuth();
  };
  const linkPasskey = async () => {
    setPasskeyMsg("");
    if (!(await auth.linkWithPasskey())) setPasskeyMsg("couldn't link with a passkey (cancelled, or not this board's passkey)");
    refreshAuth();
  };

  // A mesh peer announced a different riftpipe protocol version (or predates
  // versioning). Detection lives in the library; we only render its event.
  const [versionMismatch, setVersionMismatch] = createSignal<{ theirs: number; ours: number } | null>(null);
  // Drag-and-drop: which card is being dragged, and which column is hovered.
  const [draggedId, setDraggedId] = createSignal<string | null>(null);
  const [dragOverCol, setDragOverCol] = createSignal<string | null>(null);
  // The board-wide Activity panel, and this device's id (for "You" labels).
  const [showActivity, setShowActivity] = createSignal(false);
  const [myId, setMyId] = createSignal("");

  // Board reloads so far — exposed for e2e to prove events/* changes don't
  // trigger them.
  let refreshCount = 0;

  // Reload the whole board (cheap — it's local OPFS) and nudge an open card.
  const refresh = async () => {
    try {
      refreshCount++;
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
    (globalThis as any).riftpipe.boardRefreshes = () => refreshCount;

    // One subscription for every change to the synced tree — our own edits, a
    // peer's merged edit, or another tab in this profile. All funnel through the
    // file API's `watch`; each re-reads the board. The event log lives under
    // events/ and doesn't affect the board view, so skip those. No SSE, no server.
    watch((path) => { if (!path.startsWith("events/")) void refresh(); });

    // Live connection status for the header pill.
    onMeshStatus(setMesh);

    // Connect P2P when the URL carries a connection id / ticket. No id =>
    // single-player, no server. Merges arrive via `watch` above.
    let lastHash: string | null = null; // null => never connected (empty hash is valid)
    const connect = async () => {
      if (location.hash === lastHash) return; // e.g. the host writing its own ticket
      lastHash = location.hash;
      // A board switch is coming: settle pending card.edit events on the board
      // they were typed on (they're dropped if it has already switched).
      await flushPendingEdits();
      try {
        await connectPeer();
      } catch (_e) {
        // no peer / signaling unavailable — runs fine solo
      }
      lastHash = location.hash; // a host may have just set the hash; don't re-trigger
    };

    (async () => {
      await refresh();
      setMyId(auth.deviceId());
      await connect();
      setMesh(meshStatus());
      refreshAuth();
    })();

    // Poll the lib's auth state — it changes as the board is locked or as
    // grants/genesis arrive from peers over the mesh.
    setInterval(refreshAuth, 2000);

    // Pasting a share link into this tab changes the hash — reconnect to it.
    // The new board's status replaces the old one's on the pill straight away.
    window.addEventListener("hashchange", () => void connect().then(() => setMesh(meshStatus())));

    // The library fires this when a peer speaks a different protocol version.
    window.addEventListener("riftpipe:protocol-mismatch", (e) => {
      try {
        const d = JSON.parse((e as CustomEvent<string>).detail);
        setVersionMismatch({ theirs: d.theirs, ours: d.ours });
      } catch (_e) {
        setVersionMismatch({ theirs: -1, ours: -1 });
      }
    });

    // Escape closes the Activity panel (not while a card drawer is on top).
    window.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && showActivity() && !openId()) closeActivity();
    });
  });

  // Closing the panel hands focus back to its toggle.
  let activityToggle: HTMLButtonElement | undefined;
  const closeActivity = () => {
    setShowActivity(false);
    activityToggle?.focus();
  };

  // History: titles of cards that no longer exist (or were renamed) come from
  // the log itself; live cards use their current title.
  const pastTitles = createMemo(() => knownTitles(historyEvents()));
  const titleOf = (id: string) => board.cards.find((c) => c.id === id)?.title ?? pastTitles().get(id) ?? id;
  const viewer = (): Viewer => ({ me: myId(), owner: authState().author });

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
      <header classList={{ topbar: true, "with-activity": showActivity() }}>
        <h1>{board.title}</h1>
        <Show when={mesh().state !== "idle"}>
          <span class={`mesh-status ${mesh().state}`} title="peer-to-peer connection">
            {meshLabel(mesh())}
          </span>
        </Show>
        <div class="topbar-actions">
          <button
            ref={activityToggle}
            classList={{ "activity-toggle": true, active: showActivity() }}
            title="Board history"
            aria-expanded={showActivity()}
            aria-controls="activity-panel"
            onClick={() => setShowActivity((v) => !v)}
          >
            Activity
          </button>
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
        </div>
      </header>
      <div classList={{ "auth-bar": true, "with-activity": showActivity() }}>
        <Show when={!authState().locked}>
          <button
            class="lock-btn"
            title="Become this board's owner — only you (and people you approve) can read or edit"
            onClick={lock}
          >
            🔓 Claim board (you become the owner)
          </button>
          <Show when={canPasskey()}>
            <button class="passkey-claim-btn" title="Claim this board and register a passkey (Face ID / Touch ID / security key) — your other devices can then join with it, no approval needed" onClick={claimPasskey}>🔑 Claim with passkey</button>
          </Show>
          <span class="auth-hint">anyone with the link can edit until you claim it</span>
        </Show>
        <Show when={authState().locked && authState().isAdmin && authState().canRead}>
          <span class="auth-status owner">{authState().isAuthor ? "🔒 You own this board" : "🔒 Your device · board admin"}</span>
          <Show when={authState().passkeys > 0} fallback={<Show when={canPasskey()}><button class="passkey-add-btn" title="Register a passkey so your other devices can join with it" onClick={claimPasskey}>🔑 Add passkey</button></Show>}>
            <span class="auth-hint passkey-on">🔑 passkey on — your other devices can join with it</span>
          </Show>
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
          <Show when={members().some((m) => !m.me)}>
            <span class="auth-hint">members:</span>
            <For each={members().filter((m) => !m.me)}>
              {(m) => (
                <span class="member">
                  <span class="req-id" title={`device ${m.device}`}>{m.device.slice(0, 8)}… · {m.role}</span>
                  <Show when={!m.founder}>
                    <button class="revoke-btn" title="Remove this device — it can't read anything new" onClick={() => revoke(m.device)}>Remove</button>
                  </Show>
                </span>
              )}
            </For>
          </Show>
        </Show>
        <Show when={authState().locked && authState().authorized && !authState().isAdmin && authState().canRead}>
          <span class="auth-status editor">🔒 Claimed · you can edit</span>
        </Show>
        <Show when={authState().locked && authState().authorized && !authState().canRead}>
          <span class="auth-status pending-key">🔒 Claimed · approved — waiting for the board key</span>
        </Show>
        <Show when={authState().locked && !authState().authorized}>
          <span class="auth-status no-access">🔒 Claimed · no access</span>
          <Show
            when={!authState().requested}
            fallback={<span class="auth-hint">✓ access requested — waiting for the owner to approve</span>}
          >
            <button class="ask-btn" onClick={askForAccess}>Ask to join</button>
          </Show>
          <Show when={canPasskey() && authState().passkeyHere}>
            <button class="passkey-link-btn" title="This is my board — join with my passkey (no approval needed)" onClick={linkPasskey}>🔑 Link with my passkey</button>
          </Show>
        </Show>
        <Show when={passkeyMsg()}><span class="auth-hint passkey-msg">{passkeyMsg()}</span></Show>
        <span class="device-id" title="this browser's device id">
          id: {authState().device.slice(0, 10)}…
        </span>
      </div>
      <main classList={{ columns: true, "with-activity": showActivity() }}>
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
      <Show when={showActivity()}>
        <ActivityPanel
          viewer={viewer}
          titleOf={titleOf}
          onOpenCard={(id) => setOpenId(id)}
          onClose={closeActivity}
        />
      </Show>
      <Show when={openId()}>
        <CardDetail
          id={openId()!}
          onClose={() => setOpenId(null)}
          refreshSignal={() => detailTick()}
          viewer={viewer}
          titleOf={titleOf}
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
