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

const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true });

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

type SubscriptionManagerSnapshot<T> = {
  currentResults: Map<string, T>;
  terminalRows: Map<string, WasmRow>;
  terminalOccurrenceAddresses: Map<string, string>;
  orderedIds: string[];
  orderedIdIndex: Map<string, number>;
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
  for (const change of normalizeRowDelta(delta)) {
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
  private currentResults = new Map<string, T>();
  private terminalRows = new Map<string, WasmRow>();
  /** Exact ordered Groove root key -> opaque ResultKey V1 sidecar address. */
  private terminalOccurrenceAddresses = new Map<string, string>();
  private orderedIds: string[] = [];
  private orderedIdIndex = new Map<string, number>();
  /** Child edits received before a non-durable browser root hydration. */
  private deferredTerminalOperations: RuntimeTerminalOperation[] = [];

  private removeId(id: string): void {
    const index = this.orderedIdIndex.get(id);
    if (index === undefined) return;
    this.orderedIds.splice(index, 1);
    this.orderedIdIndex.delete(id);
    this.reindexOrderedIds(index);
  }

  private insertIdAt(id: string, index: number): void {
    const clamped = Math.max(0, Math.min(index, this.orderedIds.length));
    this.orderedIds.splice(clamped, 0, id);
    this.reindexOrderedIds(clamped);
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
    const snapshot = this.snapshot();
    try {
      if (reset) {
        this.clearRows();
        this.deferredTerminalOperations = [];
      }
      for (const key of [...delta.added, ...delta.updated, ...delta.removed].map(
        (change) => change.occurrenceKey,
      )) {
        const orderedKey = orderedTerminalKeyForTypedOccurrence(key);
        if (orderedKey) {
          this.registerTerminalOccurrenceAddress(orderedKey, publicResultKey(key));
        } else {
          throw new Error("malformed or noncanonical ResultKey V1 terminal occurrence key");
        }
      }
      const decoded: DecodedRowDelta[] = [
        ...delta.updated.map((change) => ({
          kind: RowChangeKind.Updated,
          id: publicResultKey(change.occurrenceKey),
          index: change.index,
          row: change.row,
        })),
        ...delta.added.map((change) => ({
          kind: RowChangeKind.Added,
          id: publicResultKey(change.occurrenceKey),
          index: change.index,
          row: change.row,
        })),
        ...delta.removed.map((change) => ({
          kind: RowChangeKind.Removed,
          id: publicResultKey(change.occurrenceKey),
          index: change.index,
        })),
      ];
      // Root removals are applied before terminal operations. Keep their
      // full public occurrence identities so a later descendant teardown in
      // this frame can be recognized as subsumed by its root removal.
      const removedRoots = new Set<string>();
      for (const [index, change] of decoded
        .filter((change) => change.kind === RowChangeKind.Removed)
        .entries()) {
        removedRoots.add(change.id);
        const terminalKey = terminalKeyForOccurrence(delta.removed[index]?.occurrenceKey);
        if (!terminalKey) continue;
        const rootId = this.terminalAddress(Array.from(terminalKey));
        removedRoots.add(rootId);
        change.id = rootId;
      }
      for (const change of decoded) {
        if (change.kind === RowChangeKind.Removed) {
          for (const rootId of removedRoots) this.terminalRows.delete(rootId);
        } else if (change.row) {
          // Retained roots are immutable. The first descendant edit in a
          // later frame makes a private writable copy of the whole root.
          this.terminalRows.set(change.id, change.row);
        }
      }
      const wireResult = this.handleDecodedDelta(decoded, transform, reset);
      const terminalOperations = this.readyTerminalOperations(
        delta.terminalOperations ?? [],
        removedRoots,
      );
      if (terminalOperations.length > 0) {
        const terminalResult = this.handleTerminalOperations(terminalOperations, transform);
        const combined = normalizeRowDelta([...wireResult.delta, ...terminalResult.delta]);
        return reset
          ? { delta: combined, all: this.all(), reset: true }
          : { delta: combined, all: this.all() };
      }
      return wireResult;
    } catch (error) {
      this.restore(snapshot);
      throw error;
    }
  }

  private snapshot(): SubscriptionManagerSnapshot<T> {
    return {
      currentResults: new Map(this.currentResults),
      // Terminal application is copy-on-write, so retained roots remain safe
      // to share with this rollback snapshot.
      terminalRows: new Map(this.terminalRows),
      terminalOccurrenceAddresses: new Map(this.terminalOccurrenceAddresses),
      orderedIds: [...this.orderedIds],
      orderedIdIndex: new Map(this.orderedIdIndex),
      deferredTerminalOperations: [...this.deferredTerminalOperations],
    };
  }

  private restore(snapshot: SubscriptionManagerSnapshot<T>): void {
    this.currentResults = snapshot.currentResults;
    this.terminalRows = snapshot.terminalRows;
    this.terminalOccurrenceAddresses = snapshot.terminalOccurrenceAddresses;
    this.orderedIds = snapshot.orderedIds;
    this.orderedIdIndex = snapshot.orderedIdIndex;
    this.deferredTerminalOperations = snapshot.deferredTerminalOperations;
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
    const beforeIndices = new Map(this.orderedIdIndex);
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
        if (edit.Insert.row.id !== id) {
          throw new Error("terminal insert row key does not match its edit key");
        }
        const value: Value = { type: "Row", value: cloneTerminalRow(edit.Insert.row) };
        removeTerminalValue(values, id);
        values.splice(Math.max(0, Math.min(edit.Insert.index, values.length)), 0, value);
      } else if ("Update" in edit) {
        const id = terminalPayloadRowId(edit.Update.key);
        const index = terminalValueIndex(values, id);
        if (index === -1) throw new Error(`terminal child update addressed missing key ${id}`);
        if (edit.Update.row.id !== id) {
          throw new Error("terminal update row key does not match its edit key");
        }
        values[index] = { type: "Row", value: cloneTerminalRow(edit.Update.row) };
      } else if ("Remove" in edit) {
        const id = terminalPayloadRowId(edit.Remove.key);
        if (!removeTerminalValue(values, id)) {
          throw new Error(`terminal child removal addressed missing key ${id}`);
        }
      } else if ("Move" in edit) {
        const id = terminalPayloadRowId(edit.Move.key);
        const index = terminalValueIndex(values, id);
        if (index === -1) throw new Error(`terminal child move addressed missing key ${id}`);
        const [value] = values.splice(index, 1);
        values.splice(Math.max(0, Math.min(edit.Move.index, values.length)), 0, value!);
      }
      affectedRoots.add(rootId);
    }

    const delta = Array.from(affectedRoots).flatMap<RowDelta<T>>((id) => {
      const beforeIndex = beforeIndices.get(id);
      const index = this.orderedIdIndex.get(id);
      const row = this.terminalRows.get(id);
      if (beforeIndex !== undefined && (index === undefined || row === undefined)) {
        return [{ kind: RowChangeKind.Removed, id, index: beforeIndex }];
      }
      if (index === undefined || row === undefined) return [];
      const item = transform(row);
      this.currentResults.set(id, withResultIdentity(item, id));
      return [
        beforeIndex === undefined
          ? { kind: RowChangeKind.Added, id, index, item }
          : { kind: RowChangeKind.Updated, id, index, item },
      ];
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
    return this.terminalOccurrenceAddresses.get(bytesKey(encoded)) ?? terminalKeyId(encoded);
  }

  private registerTerminalOccurrenceAddress(
    orderedKey: Uint8Array,
    occurrenceAddress: string,
  ): void {
    const address = bytesKey(orderedKey);
    const existing = this.terminalOccurrenceAddresses.get(address);
    if (existing !== undefined && existing !== occurrenceAddress) {
      throw new Error("conflicting typed terminal occurrence keys share an ordered root key");
    }
    this.terminalOccurrenceAddresses.set(address, occurrenceAddress);
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

    for (const change of delta) {
      switch (change.kind) {
        case RowChangeKind.Added:
          const alreadyPresent = this.currentResults.has(change.id);
          this.currentResults.set(change.id, change.item);
          if (alreadyPresent) {
            this.removeId(change.id);
          }
          this.insertIdAt(change.id, change.index);
          break;
        case RowChangeKind.Removed:
          this.currentResults.delete(change.id);
          this.removeId(change.id);
          break;
        case RowChangeKind.Updated:
          this.removeId(change.id);
          this.insertIdAt(change.id, change.index);
          if (change.item !== undefined) {
            this.currentResults.set(change.id, change.item);
          }
          break;
      }
    }

    return {
      delta,
      all: this.all(),
    } as SubscriptionDelta<T>;
  }

  private replaceWithResetDelta(delta: RowDelta<T>[]): SubscriptionDelta<T> {
    this.currentResults = new Map();
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
    this.orderedIdIndex = new Map();
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
    this.orderedIdIndex = new Map();
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
    this.currentResults.clear();
    this.terminalRows.clear();
    this.terminalOccurrenceAddresses.clear();
    this.orderedIds = [];
    this.orderedIdIndex.clear();
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
  if (discriminatorCount > joinedCount) return undefined;
  const arms = new Map<number, Uint8Array>();
  let previousPosition = -1;
  for (let index = 0; index < discriminatorCount; index += 1) {
    if (cursor + 8 > sidecar.byteLength) return undefined;
    const position = readU32Be(sidecar, cursor);
    const length = readU32Be(sidecar, cursor + 4);
    cursor += 8;
    if (
      position >= joinedCount ||
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

  const ordered: number[] = [10, ...root];
  for (const [index, uuid] of joined.entries()) {
    const arm = arms.get(index);
    if (arm) {
      ordered.push(6, ...orderedBytes(arm));
    }
    ordered.push(10, ...uuid);
  }
  return Uint8Array.from(ordered);
}

/** Reconstruct a terminal key from the sole ResultKey V1 carrier. */
function terminalKeyForOccurrence(sidecar: Uint8Array | undefined): Uint8Array | undefined {
  if (!sidecar) return undefined;
  return orderedTerminalKeyForTypedOccurrence(sidecar);
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

function bytesKey(bytes: ArrayLike<number>): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function terminalKeyId(encoded: readonly number[]): string {
  const bytes = Uint8Array.from(encoded);
  if (bytes.length === 17 && bytes[0] === 10) {
    return readUuid(bytes, 1);
  }
  // Groove terminal operations address roots by an ordered Record key. A
  // multi-source row is therefore `Uuid, Uuid, …`, while the packed stream's
  // occurrence sidecar uses the equivalent v1 ResultKey (`1, uuid, uuid,
  // …`). Normalize only this exact physical form so terminal patches meet the
  // same full occurrence identity that seeded the subscription state.
  if (bytes.length > 17 && isUuidOnlyTerminalKey(bytes)) {
    const occurrence = [1];
    for (let offset = 0; offset < bytes.length; offset += 17) {
      if (bytes[offset] !== 10) {
        return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
      }
      occurrence.push(...bytes.subarray(offset + 1, offset + 17));
    }
    return publicResultKey(Uint8Array.from(occurrence));
  }
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
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

function terminalValueIndex(values: Value[], id: string): number {
  return values.findIndex((value) => value.type === "Row" && value.value.id === id);
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
    const childId = terminalPayloadRowId(keySegment.Key);
    if (index === path.length - 1) return values;
    const child = values.find((value) => value.type === "Row" && value.value.id === childId);
    if (child?.type !== "Row") return undefined;
    ownerValues = child.value.values;
  }
  return undefined;
}

function readUuid(bytes: Uint8Array, offset: number): string {
  const hex = Array.from(bytes.subarray(offset, offset + 16), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(
    16,
    20,
  )}-${hex.slice(20)}`;
}

function publicResultKey(bytes: Uint8Array): string {
  if (
    bytes.length === 25 &&
    bytes[0] === 1 &&
    readU32Be(bytes, 17) === 0 &&
    readU32Be(bytes, 21) === 0
  )
    return readUuid(bytes, 1);
  return `result:${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}
