// Coalesces per-keystroke card edits into ONE `card.edit` per field once typing
// settles (final value), and nothing if it ended where it started. Each pending
// edit remembers the board it was typed on: if the page switches boards before
// it fires, it is dropped — it must never land in (and sync to) another board's
// log. `flush` writes everything pending now (board switch, tab hidden/closing).
// No imports, timers injected — unit-testable under `deno test`.

export const EDIT_SETTLE_MS = 1500;

export type EditField = "title" | "description";

export type EditSettlerDeps = {
  settleMs: number;
  /** The board writes currently land on. */
  board: () => string;
  /** Write one card.edit event's data. */
  log: (data: Record<string, unknown>) => Promise<void>;
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
};

type PendingEdit = { timer: unknown; board: string; id: string; field: EditField; from: string; to: string };

export function createEditSettler(deps: EditSettlerDeps) {
  const setTimer = deps.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = deps.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  const pending = new Map<string, PendingEdit>();

  async function emit(p: PendingEdit): Promise<void> {
    if (p.to === p.from || deps.board() !== p.board) return;
    await deps.log(p.field === "title" ? { id: p.id, field: p.field, value: p.to } : { id: p.id, field: p.field });
  }

  return {
    /** Record that `field` of card `id` went `from` → `to`; (re)starts its timer. */
    queue(id: string, field: EditField, from: string, to: string): void {
      const key = `${id}\u0000${field}`;
      const board = deps.board();
      const prev = pending.get(key);
      if (prev) clearTimer(prev.timer);
      // A pending edit from another board is stale: start over on this one.
      const base = prev && prev.board === board ? prev.from : from;
      const entry: PendingEdit = { timer: undefined, board, id, field, from: base, to };
      entry.timer = setTimer(() => {
        if (pending.get(key) === entry) pending.delete(key);
        void emit(entry);
      }, deps.settleMs);
      pending.set(key, entry);
    },
    /** Write every pending edit now (dropping any typed on another board). */
    async flush(): Promise<void> {
      const all = [...pending.values()];
      pending.clear();
      for (const p of all) {
        clearTimer(p.timer);
        await emit(p);
      }
    },
    /** How many edits are waiting to settle. */
    pendingCount(): number {
      return pending.size;
    },
  };
}
