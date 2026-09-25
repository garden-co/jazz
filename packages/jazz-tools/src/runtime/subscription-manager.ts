import { bytesToHex, formatUuidAt } from "./hex.js";
import { Utf8Decoder } from "./utf8.js";
/**
 * Manage subscription state and compute deltas.
 *
 * Tracks the current result set for a subscription and transforms
 * WASM row deltas into typed object deltas with full state tracking.
 */

import type {
  RuntimeSubscriptionDelta,
  RuntimeTerminalOperation,
  Value,
  WasmRow,
} from "../drivers/types.js";

const fatalUtf8Decoder = new Utf8Decoder({ fatal: true });

export const RowChangeKind = {
  Added: 0 as const,
  Removed: 1 as const,
  Updated: 2 as const,
} as const;
export type RowChangeKind = typeof RowChangeKind;
export type RowChangeKindValue = (typeof RowChangeKind)[keyof typeof RowChangeKind];

export type RowDelta<T> =
  | { kind: RowChangeKind["Added"]; id: string; index: number; item: T }
  | { kind: RowChangeKind["Removed"]; id: string; index: number }
  | { kind: RowChangeKind["Updated"]; id: string; index: number; item?: T };

type DecodedRowDelta =
  | { kind: RowChangeKind["Added"]; id: string; index: number; row: WasmRow }
  | { kind: RowChangeKind["Removed"]; id: string; index: number }
  | { kind: RowChangeKind["Updated"]; id: string; index: number; row?: WasmRow | null };

export type SubscriptionDelta<T> =
  | {
      /** Complete result after applying this delta, when available. */
      all?: T[];
      /** Ordered list of changes for this delta. */
      delta: RowDelta<T>[];
      reset?: false;
    }
  | {
      /** Complete replacement result after applying this reset delta. */
      all: T[];
      /** Ordered list of changes for this delta. */
      delta: RowDelta<T>[];
      /** True when this delta replaces all previously observed state. */
      reset: true;
    };

const ABSENT: unique symbol = Symbol("jazz.subscriptionManager.absent");

/**
 * A Map that can record the first prior value of every key it changes, so a
 * failed delta can be undone in time proportional to what it touched rather
 * than to the size of the result.
 */
class JournaledMap<K, V> extends Map<K, V> {
  private journal: Map<K, V | typeof ABSENT> | null = null;

  beginJournal(): void {
    this.journal = new Map();
  }

  endJournal(): void {
    this.journal = null;
  }

  /** Put back every key changed since `beginJournal` and stop journaling. */
  rollBack(): void {
    const journal = this.journal;
    this.journal = null;
    if (!journal) return;
    for (const [key, previous] of journal) {
      if (previous === ABSENT) super.delete(key);
      else super.set(key, previous);
    }
  }

  override set(key: K, value: V): this {
    const journal = this.journal;
    if (journal && !journal.has(key)) {
      journal.set(key, super.has(key) ? super.get(key)! : ABSENT);
    }
    return super.set(key, value);
  }

  override delete(key: K): boolean {
    const journal = this.journal;
    if (journal && !journal.has(key) && super.has(key)) journal.set(key, super.get(key)!);
    return super.delete(key);
  }

  override clear(): void {
    if (this.journal) throw new Error("journaled subscription state must be replaced, not cleared");
    super.clear();
  }
}

/**
 * Rollback state for one `handleDelta` call. Field references are restored
 * as they were; maps that existed at the start undo their own journals, and
 * `orderedIds` is copied before its first in-place change.
 */
type SubscriptionManagerTransaction<T> = {
  currentResults: JournaledMap<string, T>;
  terminalRows: JournaledMap<string, WasmRow>;
  terminalOccurrenceAddresses: JournaledMap<string, string>;
  orderedIds: string[];
  orderedIdIndex: JournaledMap<string, number>;
  deferredTerminalOperations: RuntimeTerminalOperation[];
};

/**
 * Canonical reducer for subscription streams. Consumers own the materialized
 * result set; the stream only guarantees that reducing deltas in order yields
 * the current view. Fresh subscriptions start with a reset delta.
 */
export function applySubscriptionDelta<T extends { id: string }>(
  current: T[],
  delta: SubscriptionDelta<T>,
): T[] {
  if (delta.reset || delta.all !== undefined) {
    const all = delta.all!;
    current.length = all.length;
    for (let index = 0; index < all.length; index++) {
      current[index] = all[index]!;
    }
    return current;
  }

  if (shouldApplyDeltaInBulk(delta.delta)) {
    return applyBulkSubscriptionDelta(current, delta.delta);
  }

  return applySubscriptionDeltaSequentially(current, delta.delta);
}

function applySubscriptionDeltaSequentially<T extends { id: string }>(
  current: T[],
  delta: RowDelta<T>[],
): T[] {
  const changes = normalizeRowDelta(delta);
  for (let position = 0; position < changes.length; position++) {
    const change = changes[position]!;
    if (change.kind === RowChangeKind.Removed) {
      // Removals by id commute, so a run of them is applied in one pass.
      const end = removedRunEnd(changes, position);
      if (end - position > 1) {
        removeIdsOnce(current, changes, position, end);
        position = end - 1;
        continue;
      }
    }
    switch (change.kind) {
      case RowChangeKind.Added:
        removeById(current, change.id);
        current.splice(Math.max(0, Math.min(change.index, current.length)), 0, change.item);
        break;
      case RowChangeKind.Removed:
        removeById(current, change.id);
        break;
      case RowChangeKind.Updated: {
        const existing = current.find((item) => resultIdentity(item) === change.id);
        removeById(current, change.id);
        const next = change.item ?? existing;
        if (next) {
          current.splice(Math.max(0, Math.min(change.index, current.length)), 0, next);
        }
        break;
      }
    }
  }

  return current;
}

function applyBulkSubscriptionDelta<T extends { id: string }>(
  current: T[],
  delta: RowDelta<T>[],
): T[] {
  delta = normalizeRowDelta(delta);
  const changedIds = new Set(delta.map((change) => change.id));
  const existingById = new Map(current.map((item) => [resultIdentity(item), item]));
  const base = current.filter((item) => !changedIds.has(resultIdentity(item)));
  const placements: Array<{ id: string; index: number; item: T }> = [];

  for (const change of delta) {
    switch (change.kind) {
      case RowChangeKind.Added:
        placements.push({ id: change.id, index: change.index, item: change.item });
        break;
      case RowChangeKind.Removed:
        break;
      case RowChangeKind.Updated: {
        const item = change.item ?? existingById.get(change.id);
        if (item) placements.push({ id: change.id, index: change.index, item });
        break;
      }
    }
  }

  const ordered = mergeIndexedPlacements(base, placements);
  current.length = ordered.length;
  for (let index = 0; index < ordered.length; index++) {
    current[index] = ordered[index]!;
  }
  return current;
}

function shouldApplyDeltaInBulk<T extends { id: string }>(delta: RowDelta<T>[]): boolean {
  if (delta.length < 32) return false;
  const ids = new Set<string>();
  const indexes = new Set<number>();
  let previousIndex = -Infinity;
  for (const change of delta) {
    if (ids.has(change.id) || indexes.has(change.index) || change.index < previousIndex) {
      return false;
    }
    ids.add(change.id);
    indexes.add(change.index);
    previousIndex = change.index;
  }
  return true;
}

function normalizeRowDelta<T extends { id: string }>(delta: RowDelta<T>[]): RowDelta<T>[] {
  if (delta.length < 2) return delta;
  const materializedIds = new Set<string>();
  for (const change of delta) {
    if (change.kind === RowChangeKind.Added || change.kind === RowChangeKind.Updated) {
      materializedIds.add(change.id);
    }
  }
  if (materializedIds.size === 0) return delta;
  return delta.filter(
    (change) => change.kind !== RowChangeKind.Removed || !materializedIds.has(change.id),
  );
}

function mergeIndexedPlacements<T>(base: T[], placements: Array<{ index: number; item: T }>): T[] {
  if (placements.length === 0) return base;
  const byIndex = new Map<number, T>();
  let inserted = 0;
  for (const placement of placements) {
    const index = Math.max(0, Math.min(placement.index, base.length + inserted));
    byIndex.set(index, placement.item);
    inserted += 1;
  }

  const next: T[] = [];
  next.length = base.length + placements.length;
  let baseIndex = 0;
  let nextIndex = 0;
  while (nextIndex < next.length) {
    const placed = byIndex.get(nextIndex);
    if (placed !== undefined) {
      next[nextIndex++] = placed;
    } else {
      next[nextIndex++] = base[baseIndex++]!;
    }
  }
  return next;
}

/** Index just past the run of consecutive removals starting at `start`. */
function removedRunEnd(changes: readonly RowDelta<unknown>[], start: number): number {
  let end = start;
  while (end < changes.length && changes[end]!.kind === RowChangeKind.Removed) end++;
  return end;
}

/**
 * Apply `removeById` for each removal in `changes[start, end)` in one pass:
 * each removal drops the first remaining item with its identity.
 */
function removeIdsOnce<T extends { id: string }>(
  current: T[],
  changes: readonly RowDelta<T>[],
  start: number,
  end: number,
): void {
  const pending = new Map<string, number>();
  for (let index = start; index < end; index++) {
    const id = changes[index]!.id;
    pending.set(id, (pending.get(id) ?? 0) + 1);
  }
  let write = 0;
  for (let read = 0; read < current.length; read++) {
    const item = current[read]!;
    const id = resultIdentity(item);
    const remaining = pending.get(id);
    if (remaining !== undefined && remaining > 0) {
      pending.set(id, remaining - 1);
      continue;
    }
    if (write !== read) current[write] = item;
    write++;
  }
  if (write !== current.length) current.splice(write);
}

function removeById<T extends { id: string }>(current: T[], id: string): void {
  const index = current.findIndex((item) => resultIdentity(item) === id);
  if (index !== -1) current.splice(index, 1);
}

const RESULT_KEY_PROPERTY = "__jazzResultKey";
const MAX_DEFERRED_TERMINAL_OPERATIONS = 1024;

function withResultIdentity<T extends { id: string }>(item: T, key: string): T {
  Object.defineProperty(item, RESULT_KEY_PROPERTY, {
    value: key,
    enumerable: false,
    configurable: true,
  });
  return item;
}

function resultIdentity(item: { id: string }): string {
  return (item as { __jazzResultKey?: string }).__jazzResultKey ?? item.id;
}

/**
 * Manages subscription state for a single query.
 *
 * Tracks the current result set by ID and transforms incoming
 * row-level deltas into typed object deltas.
 *
 * @typeParam T - The typed object type (must have `id: string`)
 */
export class SubscriptionManager<T extends { id: string }> {
  private currentResults = new JournaledMap<string, T>();
  private terminalRows = new JournaledMap<string, WasmRow>();
  /** Exact ordered Groove root key -> opaque ResultKey V1 sidecar address. */
  private terminalOccurrenceAddresses = new JournaledMap<string, string>();
  private orderedIds: string[] = [];
  private orderedIdIndex = new JournaledMap<string, number>();
  /** Child edits received before a non-durable browser root hydration. */
  private deferredTerminalOperations: RuntimeTerminalOperation[] = [];
  /** Rollback state while `handleDelta` is applying a delta. */
  private transaction: SubscriptionManagerTransaction<T> | null = null;

  /** `orderedIds`, copied first if a rollback may still need the original. */
  private writableOrderedIds(): string[] {
    if (this.transaction && this.orderedIds === this.transaction.orderedIds) {
      this.orderedIds = this.orderedIds.slice();
    }
    return this.orderedIds;
  }

  /**
   * Lowest position whose `orderedIdIndex` entry may be stale, while a
   * sequential delta is being applied; null when the index is exact.
   * Positions below it are untouched since the index was last exact.
   */
  private staleFrom: number | null = null;
  /**
   * Splices since the index was last exact. Each moves any id by at most one
   * position, so a stale entry is within this distance of its true position.
   */
  private staleSplices = 0;

  private markStaleFrom(position: number, splices = 1): void {
    if (this.staleFrom === null || position < this.staleFrom) this.staleFrom = position;
    this.staleSplices += splices;
  }

  /** Current position of `id`, correct even while the index is stale. */
  private positionOf(id: string): number | undefined {
    const recorded = this.orderedIdIndex.get(id);
    if (recorded === undefined) return undefined;
    if (this.staleFrom === null || recorded < this.staleFrom) return recorded;
    const orderedIds = this.orderedIds;
    if (orderedIds[recorded] === id) return recorded;
    // Everything before `staleFrom` is exact, so a stale id lies after it,
    // within `staleSplices` of where it was recorded.
    const low = Math.max(this.staleFrom, recorded - this.staleSplices);
    const high = Math.min(orderedIds.length - 1, recorded + this.staleSplices);
    for (let distance = 1; recorded - distance >= low || recorded + distance <= high; distance++) {
      if (recorded + distance <= high && orderedIds[recorded + distance] === id) {
        return recorded + distance;
      }
      if (recorded - distance >= low && orderedIds[recorded - distance] === id) {
        return recorded - distance;
      }
    }
    return undefined;
  }

  /** Bring `orderedIdIndex` back in line after a sequential delta. */
  private refreshOrderedIdIndex(): void {
    if (this.staleFrom === null) return;
    const start = this.staleFrom;
    this.staleFrom = null;
    this.staleSplices = 0;
    this.reindexOrderedIds(start);
  }

  private removeId(id: string): void {
    const index = this.positionOf(id);
    if (index === undefined) return;
    this.writableOrderedIds().splice(index, 1);
    this.orderedIdIndex.delete(id);
    this.markStaleFrom(index);
  }

  /** Remove every id in `changes[start, end)` from the result in one pass. */
  private removeIds(changes: readonly RowDelta<T>[], start: number, end: number): void {
    let first = this.orderedIds.length;
    for (let index = start; index < end; index++) {
      const id = changes[index]!.id;
      this.currentResults.delete(id);
      const position = this.positionOf(id);
      if (position === undefined) continue;
      this.orderedIdIndex.delete(id);
      if (position < first) first = position;
    }
    if (first === this.orderedIds.length) return;
    const orderedIds = this.writableOrderedIds();
    let write = first;
    for (let read = first; read < orderedIds.length; read++) {
      const id = orderedIds[read]!;
      if (!this.orderedIdIndex.has(id)) continue;
      orderedIds[write] = id;
      write++;
    }
    const removed = orderedIds.length - write;
    orderedIds.length = write;
    this.markStaleFrom(first, removed);
  }

  private insertIdAt(id: string, index: number): void {
    const clamped = Math.max(0, Math.min(index, this.orderedIds.length));
    this.writableOrderedIds().splice(clamped, 0, id);
    this.orderedIdIndex.set(id, clamped);
    this.markStaleFrom(clamped);
  }

  /**
   * Whether removing `id` and reinserting it at `index` would put it back
   * where it is: an in-place change needs no splice or reindex.
   */
  private staysInPlace(id: string, index: number): boolean {
    const position = this.positionOf(id);
    if (position === undefined) return false;
    return position === Math.max(0, Math.min(index, this.orderedIds.length - 1));
  }

  private reindexOrderedIds(start = 0): void {
    for (let index = start; index < this.orderedIds.length; index++) {
      this.orderedIdIndex.set(this.orderedIds[index]!, index);
    }
  }

  /**
   * Process a row delta and return typed object delta.
   *
   * @param delta Structured root delta from the runtime adapter
   * @param transform Function to convert WasmRow to typed object T
   * @returns Typed delta with full state and changes
   */
  handleDelta(
    delta: RuntimeSubscriptionDelta,
    transform: (row: WasmRow) => T,
  ): SubscriptionDelta<T> {
    const reset = delta.reset === true;
    this.beginTransaction();
    try {
      if (reset) {
        this.clearRows();
        this.deferredTerminalOperations = [];
      }
      // Validate and register each occurrence once, keeping its public result
      // key and exact ordered-root address for the rest of this frame.
      const registerOccurrence = (sidecar: Uint8Array) => {
        const orderedKey = orderedTerminalKeyForTypedOccurrence(sidecar);
        if (!orderedKey) {
          throw new Error("malformed or noncanonical ResultKey V1 terminal occurrence key");
        }
        const id = publicResultKey(sidecar);
        return { id, address: this.registerTerminalOccurrenceAddress(orderedKey, id) };
      };
      const addedKeys = delta.added.map((change) => registerOccurrence(change.occurrenceKey));
      const updatedKeys = delta.updated.map((change) => registerOccurrence(change.occurrenceKey));
      const removedKeys = delta.removed.map((change) => registerOccurrence(change.occurrenceKey));
      const decoded: DecodedRowDelta[] = [
        ...delta.updated.map((change, index) => ({
          kind: RowChangeKind.Updated,
          id: updatedKeys[index]!.id,
          index: change.index,
          row: change.row,
        })),
        ...delta.added.map((change, index) => ({
          kind: RowChangeKind.Added,
          id: addedKeys[index]!.id,
          index: change.index,
          row: change.row,
        })),
        ...delta.removed.map((change, index) => ({
          kind: RowChangeKind.Removed,
          id: removedKeys[index]!.id,
          index: change.index,
        })),
      ];
      // A root that this frame both removes and adds or updates stays in the
      // result (see normalizeRowDelta), so it keeps its retained terminal row.
      const materializedRoots = new Set(
        decoded
          .filter((change) => change.kind !== RowChangeKind.Removed)
          .map((change) => change.id),
      );
      // Root removals are applied before terminal operations. Keep their
      // full public occurrence identities so a later descendant teardown in
      // this frame can be recognized as subsumed by its root removal. The
      // registered address of a removed root is its public result key.
      const removedRoots = new Set<string>();
      for (const key of removedKeys) {
        if (!materializedRoots.has(key.id)) removedRoots.add(key.id);
      }
      for (const change of decoded) {
        if (change.kind !== RowChangeKind.Removed && change.row) {
          // Retained roots are immutable. The first descendant edit in a
          // later frame makes a private writable copy of the whole root.
          this.terminalRows.set(change.id, change.row);
        }
      }
      // Removals follow every set in `decoded`, so dropping the removed roots
      // once afterwards is equivalent to dropping them at each removal.
      for (const rootId of removedRoots) this.terminalRows.delete(rootId);
      const wireResult = this.handleDecodedDelta(decoded, transform, reset);
      // Complete roots already include this frame's descendant edits. Replaying
      // those edits would remove children twice or apply moves to the new order.
      // Earlier deferred edits still replay when their root hydration arrives.
      const completeRoots = new Set(
        decoded
          .filter((change) => change.kind !== RowChangeKind.Removed)
          .map((change) => change.id),
      );
      const terminalOperations = this.readyTerminalOperations(
        (delta.terminalOperations ?? []).filter(
          (operation) =>
            operation.path.length === 0 ||
            !completeRoots.has(this.terminalAddress(operation.root_key)),
        ),
        removedRoots,
      );
      let result = wireResult;
      if (terminalOperations.length > 0) {
        const terminalResult = this.handleTerminalOperations(terminalOperations, transform);
        const combined = normalizeRowDelta([...wireResult.delta, ...terminalResult.delta]);
        result = reset
          ? { delta: combined, all: this.all(), reset: true }
          : { delta: combined, all: this.all() };
      }
      // A removed root's ordered-key address is only needed while the root is
      // part of the result; re-adding it registers the address again. Pruning
      // keeps the map bounded by the live result instead of every key seen.
      for (const { id, address } of removedKeys) {
        if (!this.currentResults.has(id)) this.terminalOccurrenceAddresses.delete(address);
      }
      this.commitTransaction();
      return result;
    } catch (error) {
      this.rollBackTransaction();
      throw error;
    }
  }

  /**
   * Start journaling so a throw can restore the state before this delta.
   * This costs O(1) up front and O(touched keys) afterwards, instead of
   * copying every map on every delta.
   */
  private beginTransaction(): void {
    this.transaction = {
      currentResults: this.currentResults,
      // Terminal application is copy-on-write, so retained roots remain safe
      // to share with the rollback state.
      terminalRows: this.terminalRows,
      terminalOccurrenceAddresses: this.terminalOccurrenceAddresses,
      orderedIds: this.orderedIds,
      orderedIdIndex: this.orderedIdIndex,
      // Never mutated in place: readyTerminalOperations replaces the array.
      deferredTerminalOperations: this.deferredTerminalOperations,
    };
    this.currentResults.beginJournal();
    this.terminalRows.beginJournal();
    this.terminalOccurrenceAddresses.beginJournal();
    this.orderedIdIndex.beginJournal();
  }

  private commitTransaction(): void {
    this.refreshOrderedIdIndex();
    const transaction = this.transaction!;
    this.transaction = null;
    transaction.currentResults.endJournal();
    transaction.terminalRows.endJournal();
    transaction.terminalOccurrenceAddresses.endJournal();
    transaction.orderedIdIndex.endJournal();
  }

  private rollBackTransaction(): void {
    const transaction = this.transaction;
    this.transaction = null;
    this.staleFrom = null;
    this.staleSplices = 0;
    if (!transaction) return;
    transaction.currentResults.rollBack();
    transaction.terminalRows.rollBack();
    transaction.terminalOccurrenceAddresses.rollBack();
    transaction.orderedIdIndex.rollBack();
    this.currentResults = transaction.currentResults;
    this.terminalRows = transaction.terminalRows;
    this.terminalOccurrenceAddresses = transaction.terminalOccurrenceAddresses;
    this.orderedIds = transaction.orderedIds;
    this.orderedIdIndex = transaction.orderedIdIndex;
    this.deferredTerminalOperations = transaction.deferredTerminalOperations;
  }

  /** Preserve a child splice that raced ahead of its root hydration. */
  private readyTerminalOperations(
    incoming: RuntimeTerminalOperation[],
    removedRoots: ReadonlySet<string>,
  ): RuntimeTerminalOperation[] {
    const operations = [...this.deferredTerminalOperations, ...incoming];
    this.deferredTerminalOperations = [];
    const ready: RuntimeTerminalOperation[] = [];
    for (const operation of operations) {
      if (operation.path.length === 0) {
        throw new Error("native producer emitted a root terminal operation");
      }
      const rootAddress = this.terminalAddress(operation.root_key);
      if (removedRoots.has(rootAddress)) {
        if ("Remove" in operation.edit) continue;
        throw new Error("terminal child edit addressed a root removed in the same frame");
      }
      if (!this.terminalRows.has(rootAddress)) {
        this.deferredTerminalOperations.push(operation);
        continue;
      }
      ready.push(operation);
    }
    if (this.deferredTerminalOperations.length > MAX_DEFERRED_TERMINAL_OPERATIONS) {
      throw new Error("terminal child edits arrived before their root beyond bounded limits");
    }
    return ready;
  }

  private handleTerminalOperations(
    operations: RuntimeTerminalOperation[],
    transform: (row: WasmRow) => T,
  ): SubscriptionDelta<T> {
    // Descendant edits never add, remove or reorder roots, so a root's index
    // before these operations is its current `orderedIdIndex` entry.
    const affectedRoots = new Set<string>();
    const writableRoots = new Set<string>();

    for (const operation of operations) {
      const rootId = this.terminalAddress(operation.root_key);
      const edit = operation.edit;
      const root = this.writableTerminalRoot(rootId, writableRoots);
      assertTerminalPathEditKey(operation.path, edit);
      const target = terminalCollection(root, operation.path);
      if (!target) throw new Error(`terminal child edit addressed an unresolved path on ${rootId}`);
      const values = target;
      if ("Insert" in edit) {
        const id = terminalPayloadRowId(edit.Insert.key);
        const key = terminalChildAddress(edit.Insert.key);
        if (edit.Insert.row.id !== id) {
          throw new Error("terminal insert row key does not match its edit key");
        }
        const value: Value = { type: "Row", value: cloneTerminalRow(edit.Insert.row) };
        setTerminalChildAddress(value, key);
        removeTerminalValue(values, key);
        values.splice(Math.max(0, Math.min(edit.Insert.index, values.length)), 0, value);
      } else if ("Update" in edit) {
        const id = terminalPayloadRowId(edit.Update.key);
        const key = terminalChildAddress(edit.Update.key);
        const index = terminalValueIndex(values, key);
        if (index === -1) throw new Error(`terminal child update addressed missing key ${id}`);
        if (edit.Update.row.id !== id) {
          throw new Error("terminal update row key does not match its edit key");
        }
        const value: Value = { type: "Row", value: cloneTerminalRow(edit.Update.row) };
        setTerminalChildAddress(value, key);
        values[index] = value;
      } else if ("Remove" in edit) {
        const id = terminalPayloadRowId(edit.Remove.key);
        const key = terminalChildAddress(edit.Remove.key);
        if (!removeTerminalValue(values, key)) {
          throw new Error(`terminal child removal addressed missing key ${id}`);
        }
      } else if ("Move" in edit) {
        const id = terminalPayloadRowId(edit.Move.key);
        const key = terminalChildAddress(edit.Move.key);
        const index = terminalValueIndex(values, key);
        if (index === -1) throw new Error(`terminal child move addressed missing key ${id}`);
        const [value] = values.splice(index, 1);
        values.splice(Math.max(0, Math.min(edit.Move.index, values.length)), 0, value!);
      }
      affectedRoots.add(rootId);
    }

    const delta = Array.from(affectedRoots).flatMap<RowDelta<T>>((id) => {
      const index = this.positionOf(id);
      if (index === undefined) return [];
      const row = this.terminalRows.get(id);
      if (row === undefined) return [{ kind: RowChangeKind.Removed, id, index }];
      const item = transform(row);
      this.currentResults.set(id, withResultIdentity(item, id));
      return [{ kind: RowChangeKind.Updated, id, index, item }];
    });
    return { delta, all: this.all() } as SubscriptionDelta<T>;
  }

  private writableTerminalRoot(rootId: string, writableRoots: Set<string>): WasmRow {
    const retained = this.terminalRows.get(rootId);
    if (!retained) throw new Error(`terminal child edit addressed missing root ${rootId}`);
    if (writableRoots.has(rootId)) return retained;

    const writable = cloneTerminalRow(retained);
    this.terminalRows.set(rootId, writable);
    writableRoots.add(rootId);
    return writable;
  }

  private terminalAddress(encoded: readonly number[]): string {
    return this.terminalOccurrenceAddresses.get(bytesToHex(encoded)) ?? terminalKeyId(encoded);
  }

  /** Register an ordered root key's occurrence and return its address key. */
  private registerTerminalOccurrenceAddress(
    orderedKey: Uint8Array,
    occurrenceAddress: string,
  ): string {
    const address = bytesToHex(orderedKey);
    const existing = this.terminalOccurrenceAddresses.get(address);
    if (existing !== undefined && existing !== occurrenceAddress) {
      throw new Error("conflicting typed terminal occurrence keys share an ordered root key");
    }
    this.terminalOccurrenceAddresses.set(address, occurrenceAddress);
    return address;
  }

  seed(rows: T[]): SubscriptionDelta<T> {
    return this.handleTypedDelta(
      rows.map((item, index) => ({
        kind: RowChangeKind.Added,
        id: item.id,
        index,
        item,
      })),
    );
  }

  private handleDecodedDelta(
    delta: DecodedRowDelta[],
    transform: (row: WasmRow) => T,
    reset = false,
  ): SubscriptionDelta<T> {
    return this.handleTypedDelta(
      delta.map((change) => {
        switch (change.kind) {
          case RowChangeKind.Added: {
            const addedItem = transform(change.row);
            return {
              kind: RowChangeKind.Added,
              id: change.id,
              index: change.index,
              item: withResultIdentity(addedItem, change.id),
            };
          }
          case RowChangeKind.Removed:
            return change;
          case RowChangeKind.Updated: {
            const updatedItem = change.row ? transform(change.row) : undefined;
            return {
              kind: RowChangeKind.Updated,
              id: change.id,
              index: change.index,
              item: updatedItem ? withResultIdentity(updatedItem, change.id) : undefined,
            };
          }
        }
      }),
      reset,
    );
  }

  private handleTypedDelta(delta: RowDelta<T>[], reset = false): SubscriptionDelta<T> {
    delta.sort((a, b) => a.index - b.index);
    delta = normalizeRowDelta(delta);

    if (reset) {
      return this.replaceWithResetDelta(delta);
    }

    if (shouldApplyDeltaInBulk(delta)) {
      this.applyBulkTypedDelta(delta);
      return { delta, all: this.all() } as SubscriptionDelta<T>;
    }

    // Positions are looked up through `positionOf` and the index is rebuilt
    // once at the end, instead of after every splice.
    for (let position = 0; position < delta.length; position++) {
      const change = delta[position]!;
      if (change.kind === RowChangeKind.Removed) {
        // Removals by id commute, so a run of them is applied in one pass.
        const end = removedRunEnd(delta, position);
        if (end - position > 1) {
          this.removeIds(delta, position, end);
          position = end - 1;
          continue;
        }
      }
      switch (change.kind) {
        case RowChangeKind.Added: {
          const alreadyPresent = this.currentResults.has(change.id);
          this.currentResults.set(change.id, change.item);
          if (alreadyPresent && this.staysInPlace(change.id, change.index)) break;
          if (alreadyPresent) {
            this.removeId(change.id);
          }
          this.insertIdAt(change.id, change.index);
          break;
        }
        case RowChangeKind.Removed:
          this.currentResults.delete(change.id);
          this.removeId(change.id);
          break;
        case RowChangeKind.Updated:
          if (!this.staysInPlace(change.id, change.index)) {
            this.removeId(change.id);
            this.insertIdAt(change.id, change.index);
          }
          if (change.item !== undefined) {
            this.currentResults.set(change.id, change.item);
          }
          break;
      }
    }
    this.refreshOrderedIdIndex();

    return {
      delta,
      all: this.all(),
    } as SubscriptionDelta<T>;
  }

  private replaceWithResetDelta(delta: RowDelta<T>[]): SubscriptionDelta<T> {
    this.currentResults = new JournaledMap();
    const placements: Array<{ id: string; index: number; item: T }> = [];
    for (const change of delta) {
      if (change.kind === RowChangeKind.Removed) continue;
      const item =
        change.kind === RowChangeKind.Added || change.item !== undefined
          ? change.item
          : this.currentResults.get(change.id);
      if (!item) continue;
      this.currentResults.set(change.id, item);
      placements.push({ id: change.id, index: change.index, item });
    }

    this.orderedIds = mergeIndexedPlacements(
      [],
      placements.map((placement) => ({ index: placement.index, item: placement.id })),
    );
    this.orderedIdIndex = new JournaledMap();
    this.reindexOrderedIds();
    const all = this.orderedIds
      .map((id) => this.currentResults.get(id))
      .filter((item): item is T => item !== undefined);
    return { delta, reset: true as const, all };
  }

  private applyBulkTypedDelta(delta: RowDelta<T>[]): void {
    const changedIds = new Set(delta.map((change) => change.id));
    const baseIds = this.orderedIds.filter((id) => !changedIds.has(id));
    const placements: Array<{ id: string; index: number }> = [];

    for (const change of delta) {
      switch (change.kind) {
        case RowChangeKind.Added:
          this.currentResults.set(change.id, change.item);
          placements.push({ id: change.id, index: change.index });
          break;
        case RowChangeKind.Removed:
          this.currentResults.delete(change.id);
          break;
        case RowChangeKind.Updated:
          if (change.item !== undefined) {
            this.currentResults.set(change.id, change.item);
          }
          if (this.currentResults.has(change.id)) {
            placements.push({ id: change.id, index: change.index });
          }
          break;
      }
    }

    this.orderedIds = mergeIndexedPlacements(
      baseIds,
      placements.map((placement) => ({ index: placement.index, item: placement.id })),
    );
    this.orderedIdIndex = new JournaledMap();
    this.reindexOrderedIds();
  }

  /**
   * Clear all tracked state.
   *
   * Called when unsubscribing to free memory.
   */
  clear(): void {
    this.clearRows();
  }

  private clearRows(): void {
    // Replace rather than clear, so an in-flight rollback keeps the old state.
    this.currentResults = new JournaledMap();
    this.terminalRows = new JournaledMap();
    this.terminalOccurrenceAddresses = new JournaledMap();
    this.orderedIds = [];
    this.orderedIdIndex = new JournaledMap();
  }

  all(): T[] {
    return this.orderedIds
      .map((id) => this.currentResults.get(id))
      .filter((item): item is T => item !== undefined);
  }

  /**
   * Get the current number of tracked items.
   */
  get size(): number {
    return this.currentResults.size;
  }
}

/**
 * Reconstruct the exact Groove ordered root key from a complete ResultKey V1.
 * This is metadata-driven: a terminal key is accepted as typed only when it
 * byte-for-byte matches the sidecar's root, joined UUIDs, and union-arm
 * discriminators. It intentionally does not infer meaning from a string in an
 * arbitrary ordered key.
 */
function orderedTerminalKeyForTypedOccurrence(sidecar: Uint8Array): Uint8Array | undefined {
  if (sidecar[0] !== 1 || sidecar.byteLength < 25) return undefined;
  let cursor = 1;
  const root = sidecar.subarray(cursor, (cursor += 16));
  const joinedCount = readU32Be(sidecar, cursor);
  cursor += 4;
  if (joinedCount > 256 || cursor + joinedCount * 16 + 4 > sidecar.byteLength) return undefined;
  const joined = Array.from({ length: joinedCount }, () => {
    const value = sidecar.subarray(cursor, (cursor += 16));
    return value;
  });
  const discriminatorCount = readU32Be(sidecar, cursor);
  cursor += 4;
  if (discriminatorCount > joinedCount + 1) return undefined;
  const arms = new Map<number, Uint8Array>();
  let previousPosition = -1;
  for (let index = 0; index < discriminatorCount; index += 1) {
    if (cursor + 8 > sidecar.byteLength) return undefined;
    const position = readU32Be(sidecar, cursor);
    const length = readU32Be(sidecar, cursor + 4);
    cursor += 8;
    if (
      position > joinedCount ||
      position <= previousPosition ||
      length === 0 ||
      length > 4096 ||
      cursor + length > sidecar.byteLength ||
      !isValidUtf8(sidecar.subarray(cursor, cursor + length)) ||
      arms.has(position)
    ) {
      return undefined;
    }
    previousPosition = position;
    arms.set(position, sidecar.subarray(cursor, (cursor += length)));
  }
  if (cursor !== sidecar.byteLength) return undefined;

  const ordered: number[] = [];
  const rootArm = arms.get(0);
  if (rootArm) ordered.push(6, ...orderedBytes(rootArm));
  ordered.push(10, ...root);
  for (const [index, uuid] of joined.entries()) {
    const arm = arms.get(index + 1);
    if (arm) {
      ordered.push(6, ...orderedBytes(arm));
    }
    ordered.push(10, ...uuid);
  }
  return Uint8Array.from(ordered);
}

function readU32Be(bytes: Uint8Array, offset: number): number {
  return (
    (((bytes[offset] ?? 0) << 24) |
      ((bytes[offset + 1] ?? 0) << 16) |
      ((bytes[offset + 2] ?? 0) << 8) |
      (bytes[offset + 3] ?? 0)) >>>
    0
  );
}

function orderedBytes(value: Uint8Array): number[] {
  const encoded: number[] = [];
  for (const byte of value) {
    if (byte === 0) encoded.push(0, 0xff);
    else encoded.push(byte);
  }
  encoded.push(0, 0);
  return encoded;
}

function isValidUtf8(bytes: Uint8Array): boolean {
  try {
    fatalUtf8Decoder.decode(bytes);
    return true;
  } catch {
    return false;
  }
}

function terminalKeyId(encoded: readonly number[]): string {
  const bytes = Uint8Array.from(encoded);
  if (bytes.length === 17 && bytes[0] === 10) {
    return readUuid(bytes, 1);
  }
  // Groove terminal operations address roots by an ordered Record key. A
  // multi-source row is therefore `Uuid, Uuid, …`, while the packed stream's
  // occurrence sidecar uses the equivalent V1 ResultKey (`1, root UUID,
  // joined count, joined UUIDs, label count`). Normalize only this exact
  // physical form so terminal patches meet the same full occurrence identity
  // that seeded the subscription state.
  if (bytes.length > 17 && isUuidOnlyTerminalKey(bytes)) {
    const uuids: number[][] = [];
    for (let offset = 0; offset < bytes.length; offset += 17) {
      if (bytes[offset] !== 10) {
        return bytesToHex(bytes);
      }
      uuids.push(Array.from(bytes.subarray(offset + 1, offset + 17)));
    }
    const occurrence = new Uint8Array(25 + (uuids.length - 1) * 16);
    occurrence[0] = 1;
    occurrence.set(uuids[0]!, 1);
    new DataView(occurrence.buffer).setUint32(17, uuids.length - 1);
    let cursor = 21;
    for (const uuid of uuids.slice(1)) {
      occurrence.set(uuid, cursor);
      cursor += 16;
    }
    return publicResultKey(occurrence);
  }
  return bytesToHex(bytes);
}

function isUuidOnlyTerminalKey(encoded: ArrayLike<number>): boolean {
  const bytes = Uint8Array.from(encoded);
  return (
    bytes.length >= 17 &&
    bytes.length % 17 === 0 &&
    Array.from({ length: bytes.length / 17 }, (_, index) => bytes[index * 17] === 10).every(Boolean)
  );
}

/** Decode the leading UUID key field from Groove's ordered record-key carrier. */
function terminalPayloadRowId(encoded: readonly number[]): string {
  const bytes = Uint8Array.from(encoded);
  if (bytes.length < 17 || bytes[0] !== 10) {
    throw new Error("terminal key must begin with a UUID row key");
  }
  return readUuid(bytes, 1);
}

function assertTerminalPathEditKey(
  path: RuntimeTerminalOperation["path"],
  edit: RuntimeTerminalOperation["edit"],
): void {
  const last = path.at(-1);
  if (!last || !("Key" in last)) return;
  const editKey = terminalEditKey(edit);
  if (
    last.Key.length !== editKey.length ||
    last.Key.some((byte, index) => byte !== editKey[index])
  ) {
    throw new Error("terminal path key does not match its edit key");
  }
}

function terminalEditKey(edit: RuntimeTerminalOperation["edit"]): readonly number[] {
  return "Insert" in edit
    ? edit.Insert.key
    : "Update" in edit
      ? edit.Update.key
      : "Remove" in edit
        ? edit.Remove.key
        : edit.Move.key;
}

// Internal identity survives copy-on-write but is not a public row field.
const terminalChildOccurrence = Symbol("jazz.terminalChildOccurrence.v1");
type OccurrenceValue = Value & { [terminalChildOccurrence]?: string };

function setTerminalChildAddress(value: Value, key: string): void {
  Object.defineProperty(value, terminalChildOccurrence, { value: key, configurable: true });
}

/** Child occurrence key v1: UUID key, optionally ff + nonzero u64 BE ordinal. */
function terminalChildAddress(encoded: readonly number[]): string {
  const id = terminalPayloadRowId(encoded);
  if (encoded.length === 17) return `${id}/0`;
  if (encoded.length !== 26 || encoded[17] !== 0xff) {
    throw new Error("invalid terminal child occurrence key v1");
  }
  const ordinal = new DataView(Uint8Array.from(encoded).buffer).getBigUint64(18, false);
  if (ordinal === 0n) throw new Error("noncanonical zero terminal occurrence ordinal");
  return `${id}/${ordinal}`;
}

function terminalValueIndex(values: Value[], key: string): number {
  const counts = new Map<string, number>();
  for (const value of values) {
    if (value.type !== "Row") continue;
    const id = value.value.id;
    if (typeof id !== "string") throw new Error("terminal child must have a row id");
    const ordinal = counts.get(id) ?? 0;
    counts.set(id, ordinal + 1);
    if ((value as OccurrenceValue)[terminalChildOccurrence] === undefined) {
      setTerminalChildAddress(value, `${id}/${ordinal}`);
    }
  }
  return values.findIndex((value) => (value as OccurrenceValue)[terminalChildOccurrence] === key);
}

function cloneTerminalRow(row: WasmRow): WasmRow {
  return cloneTerminalGraph(row, new WeakMap<object, unknown>());
}

/** Clone the positional tree while preserving hidden named-value maps and aliases. */
function cloneTerminalGraph<T>(value: T, seen: WeakMap<object, unknown>): T {
  if (typeof value !== "object" || value === null) return value;
  const existing = seen.get(value);
  if (existing !== undefined) return existing as T;
  if (value instanceof Uint8Array) {
    const clone = value.slice();
    seen.set(value, clone);
    return clone as T;
  }
  if (value instanceof Map) {
    const clone = new Map<unknown, unknown>();
    seen.set(value, clone);
    for (const [key, entry] of value) {
      clone.set(cloneTerminalGraph(key, seen), cloneTerminalGraph(entry, seen));
    }
    return clone as T;
  }
  if (Array.isArray(value)) {
    const clone: unknown[] = [];
    seen.set(value, clone);
    for (const entry of value) clone.push(cloneTerminalGraph(entry, seen));
    return clone as T;
  }

  const clone = Object.create(Object.getPrototypeOf(value)) as object;
  seen.set(value, clone);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor) continue;
    if ("value" in descriptor) {
      descriptor.value = cloneTerminalGraph(descriptor.value, seen);
    }
    Object.defineProperty(clone, key, descriptor);
  }
  return clone as T;
}

function removeTerminalValue(values: Value[], id: string): boolean {
  const index = terminalValueIndex(values, id);
  if (index === -1) return false;
  values.splice(index, 1);
  return true;
}

function terminalCollection(
  root: WasmRow,
  path: RuntimeTerminalOperation["path"],
): Value[] | undefined {
  let ownerValues = root.values;
  for (let index = 0; index < path.length; index += 1) {
    const segment = path[index]!;
    if (!("Collection" in segment)) return undefined;
    if (!Number.isSafeInteger(segment.Collection) || segment.Collection < 0) return undefined;
    const collection = ownerValues[segment.Collection];
    if (collection?.type !== "Array") return undefined;
    const values = collection.value;
    if (index === path.length - 1) return values;
    const keySegment = path[++index];
    if (!keySegment || !("Key" in keySegment)) return undefined;
    const childKey = terminalChildAddress(keySegment.Key);
    if (index === path.length - 1) return values;
    const child = values[terminalValueIndex(values, childKey)];
    if (child?.type !== "Row") return undefined;
    ownerValues = child.value.values;
  }
  return undefined;
}

function readUuid(bytes: Uint8Array, offset: number): string {
  return formatUuidAt(bytes, offset);
}

function publicResultKey(bytes: Uint8Array): string {
  if (bytes.length === 25 && bytes[0] === 1 && isZeroFrom(bytes, 17)) return readUuid(bytes, 1);
  return `result:${bytesToHex(bytes)}`;
}

function isZeroFrom(bytes: Uint8Array, start: number): boolean {
  for (let index = start; index < bytes.length; index++) {
    if (bytes[index] !== 0) return false;
  }
  return true;
}
