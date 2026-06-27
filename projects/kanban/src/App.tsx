import { createSignal, onMount, onCleanup, For, Show } from "solid-js";
import { createStore, reconcile } from "solid-js/store";
import { getBoard, getCard, addCard, patchCard, type Card } from "./api.ts";
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
  // Drag-and-drop: which card is being dragged, and which column is hovered.
  const [draggedId, setDraggedId] = createSignal<string | null>(null);
  const [dragOverCol, setDragOverCol] = createSignal<string | null>(null);

  onMount(() => {
    let es: EventSource | undefined;

    (async () => {
      try {
        const data = await getBoard();
        setBoard(reconcile({ ...data, loaded: true }));
      } catch (_e) {
        // initial load failed; SSE updates may still arrive
      }

      es = new EventSource("/api/events");
      es.onmessage = async (e) => {
        let msg: { type: string; id?: string };
        try {
          msg = JSON.parse(e.data);
        } catch {
          return;
        }

        if (msg.type === "ticket" && msg.id) {
          // If the open ticket changed, nudge the drawer to re-fetch.
          if (msg.id === openId()) setDetailTick((n) => n + 1);
          const card = await getCard(msg.id);
          if (card) {
            const i = board.cards.findIndex((c) => c.id === card.id);
            if (i >= 0) setBoard("cards", i, reconcile(card));
            else setBoard("cards", board.cards.length, card);
          } else {
            // Deleted ticket — drop it from the board.
            setBoard("cards", (cs) => cs.filter((c) => c.id !== msg.id));
          }
        } else if (msg.type === "board") {
          const b = await getBoard();
          setBoard("title", b.title);
          setBoard("columns", reconcile(b.columns));
          setBoard("cards", reconcile(b.cards, { key: "id" }));
        }
      };
    })();

    onCleanup(() => es?.close());
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
      <header class="topbar">
        <h1>{board.title}</h1>
      </header>
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
