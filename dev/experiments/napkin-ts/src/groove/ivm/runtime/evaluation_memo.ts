// One owner for evaluated batches: dense frame slots for ordinary execution,
// keyed overflow for recursive scopes and alternate contexts. A slot is an
// address, never a validity proof.
import { KeyedMap } from "../../../support/collections.js";
import {
  ScopeId,
  evalMemoKeyHash,
  type EvalMemoEntry,
  type EvalMemoKey,
  type ExecutionLayout,
} from "../../../_extern/groove-ivm-ops.js";

type Slot = [EvalMemoKey, EvalMemoEntry] | undefined;

export class EvaluationMemo {
  private layout?: ExecutionLayout;
  private slots: Slot[] = [];
  private occupied = 0;
  private overflow = new KeyedMap<EvalMemoKey, EvalMemoEntry>(evalMemoKeyHash);
  /** Entries keyed to one tick (`tickEpoch !== undefined`). */
  private tickEntryCount = 0;

  static forLayout(layout: ExecutionLayout): EvaluationMemo {
    const memo = new EvaluationMemo();
    memo.slots = new Array(layout.nodes.length).fill(undefined);
    memo.layout = layout;
    return memo;
  }

  /** Move results into a newly discovered layout without copying batches. */
  setLayout(layout: ExecutionLayout): void {
    const entries = [...this.entries()];
    const next = EvaluationMemo.forLayout(layout);
    Object.assign(this, next);
    this.extend(entries);
  }

  sameKey(a: EvalMemoKey, b: EvalMemoKey): boolean {
    return evalMemoKeyHash(a) === evalMemoKeyHash(b);
  }

  private slotFor(key: EvalMemoKey): number | undefined {
    if (key.scope !== ScopeId.root()) return undefined;
    return this.layout?.slots.get(key.node);
  }

  slot(slot: number): Slot {
    return this.slots[slot];
  }

  get(key: EvalMemoKey): EvalMemoEntry | undefined {
    const slot = this.slotFor(key);
    if (slot !== undefined) {
      const stored = this.slots[slot];
      if (stored && this.sameKey(stored[0], key)) return stored[1];
    }
    return this.overflow.get(key);
  }

  insert(key: EvalMemoKey, entry: EvalMemoEntry): EvalMemoEntry | undefined {
    const previous = this.insertEntry(key, entry);
    if (key.tickEpoch !== undefined && previous === undefined) this.tickEntryCount++;
    return previous;
  }

  private insertEntry(key: EvalMemoKey, entry: EvalMemoEntry): EvalMemoEntry | undefined {
    const slot = this.slotFor(key);
    if (slot === undefined) {
      const previous = this.overflow.get(key);
      this.overflow.set(key, entry);
      return previous;
    }
    const stored = this.slots[slot];
    if (stored && this.sameKey(stored[0], key)) {
      const previous = stored[1];
      stored[1] = entry;
      return previous;
    }
    // A different context must not overwrite a still-reusable result: spill it.
    const replaced = this.overflow.get(key);
    this.overflow.delete(key);
    this.slots[slot] = [key, entry];
    if (stored) this.overflow.set(stored[0], stored[1]);
    else this.occupied++;
    return replaced;
  }

  remove(key: EvalMemoKey): EvalMemoEntry | undefined {
    const removed = this.removeEntry(key);
    if (removed && key.tickEpoch !== undefined) this.tickEntryCount--;
    return removed;
  }

  private removeEntry(key: EvalMemoKey): EvalMemoEntry | undefined {
    const slot = this.slotFor(key);
    if (slot !== undefined) {
      const stored = this.slots[slot];
      if (stored && this.sameKey(stored[0], key)) {
        this.occupied--;
        this.slots[slot] = undefined;
        return stored[1];
      }
    }
    const entry = this.overflow.get(key);
    this.overflow.delete(key);
    return entry;
  }

  retain(keep: (key: EvalMemoKey, entry: EvalMemoEntry) => boolean): void {
    let removedTickEntries = 0;
    this.slots.forEach((slot, i) => {
      if (slot && !keep(slot[0], slot[1])) {
        if (slot[0].tickEpoch !== undefined) removedTickEntries++;
        this.slots[i] = undefined;
        this.occupied--;
      }
    });
    for (const [key, entry] of [...this.overflow]) {
      if (!keep(key, entry)) {
        if (key.tickEpoch !== undefined) removedTickEntries++;
        this.overflow.delete(key);
      }
    }
    this.tickEntryCount -= removedTickEntries;
  }

  tickEntries(): number {
    return this.tickEntryCount;
  }

  *entries(): IterableIterator<[EvalMemoKey, EvalMemoEntry]> {
    for (const slot of this.slots) if (slot) yield slot;
    yield* this.overflow;
  }

  *keys(): IterableIterator<EvalMemoKey> {
    for (const [key] of this.entries()) yield key;
  }

  *values(): IterableIterator<EvalMemoEntry> {
    for (const [, entry] of this.entries()) yield entry;
  }

  get size(): number {
    return this.occupied + this.overflow.size;
  }

  extend(entries: Iterable<[EvalMemoKey, EvalMemoEntry]>): void {
    for (const [key, entry] of entries) this.insert(key, entry);
  }
}
