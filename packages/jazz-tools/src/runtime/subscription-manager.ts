/**
 * Manage subscription state and compute deltas.
 *
 * Tracks the current result set for a subscription and transforms
 * WASM row deltas into typed object deltas with full state tracking.
 */

import type {
  ColumnDescriptor,
  NativeRowDelta,
  NativeTerminalOperation,
  SubscriptionWireDelta,
  Value,
  WasmRow,
  RowDelta as WireRowDelta,
} from "../drivers/types.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "./select-projection.js";
import { decodeNativeRow, decodeNativeTerminalRow } from "./native-runtime/native-row-codec.js";

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
  orderedIds: string[];
  orderedIdIndex: Map<string, number>;
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
        const existing = current.find((item) => item.id === change.id);
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
  const existingById = new Map(current.map((item) => [item.id, item]));
  const base = current.filter((item) => !changedIds.has(item.id));
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
  const index = current.findIndex((item) => item.id === id);
  if (index !== -1) current.splice(index, 1);
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
  private orderedIds: string[] = [];
  private orderedIdIndex = new Map<string, number>();

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
   * @param delta Raw row delta from WASM runtime
   * @param transform Function to convert WasmRow to typed object T
   * @returns Typed delta with full state and changes
   */
  handleDelta(
    delta: SubscriptionWireDelta,
    transform: (row: WasmRow) => T,
    nativeColumns?: readonly ColumnDescriptor[],
  ): SubscriptionDelta<T> {
    if (isNativeRowDelta(delta)) {
      const reset = delta.reset === true;
      if (!nativeColumns) {
        throw new Error("Native subscription delta requires output columns for decoding");
      }
      const snapshot = this.snapshot(nativeColumns);
      try {
        if (reset) {
          this.clear();
        }
        const decoded = decodeNativeDelta(delta, nativeColumns);
        for (const change of decoded) {
          if (change.kind === RowChangeKind.Removed) {
            this.terminalRows.delete(change.id);
          } else if (change.row) {
            this.terminalRows.set(change.id, change.row);
          }
        }
        const wireResult = this.handleWireDelta(decoded, transform, reset);
        if (delta.terminalOperations && delta.terminalOperations.length > 0) {
          const terminalResult = this.handleTerminalOperations(
            delta.terminalOperations,
            transform,
            nativeColumns,
          );
          return reset
            ? { delta: terminalResult.delta, all: terminalResult.all ?? this.all(), reset: true }
            : terminalResult;
        }
        return wireResult;
      } catch (error) {
        this.restore(snapshot);
        throw error;
      }
    }

    return this.handleWireDelta(delta, transform);
  }

  private snapshot(columns: readonly ColumnDescriptor[]): SubscriptionManagerSnapshot<T> {
    return {
      currentResults: new Map(this.currentResults),
      terminalRows: new Map(
        Array.from(this.terminalRows, ([id, row]) => [id, cloneTerminalRow(row, columns)]),
      ),
      orderedIds: [...this.orderedIds],
      orderedIdIndex: new Map(this.orderedIdIndex),
    };
  }

  private restore(snapshot: SubscriptionManagerSnapshot<T>): void {
    this.currentResults = snapshot.currentResults;
    this.terminalRows = snapshot.terminalRows;
    this.orderedIds = snapshot.orderedIds;
    this.orderedIdIndex = snapshot.orderedIdIndex;
  }

  private handleTerminalOperations(
    operations: NativeTerminalOperation[],
    transform: (row: WasmRow) => T,
    rootColumns: readonly ColumnDescriptor[],
  ): SubscriptionDelta<T> {
    const beforeIndices = new Map(this.orderedIdIndex);
    const affectedRoots = new Set<string>();
    const rootInserts = operations.filter(
      (operation) => operation.path.length === 0 && "Insert" in operation.edit,
    );

    // Pre-establish only newly inserted roots so child-before-root batches are
    // addressable. Updates remain in producer order: Groove deliberately emits
    // nested diffs before the final full root update.
    for (const operation of rootInserts) {
      const rootId = terminalKeyId(operation.root_key);
      const edit = operation.edit;
      assertTerminalRootEditKey(operation.root_key, edit);
      if (!("Insert" in edit)) throw new Error("terminal root insert partition is invalid");
      this.terminalRows.set(
        rootId,
        decodeNativeTerminalRow(rootId, rootColumns, Uint8Array.from(edit.Insert.value)),
      );
      this.removeId(rootId);
      this.insertIdAt(rootId, edit.Insert.index);
      affectedRoots.add(rootId);
    }

    for (const operation of operations) {
      const rootId = terminalKeyId(operation.root_key);
      const edit = operation.edit;
      if (operation.path.length === 0) {
        assertTerminalRootEditKey(operation.root_key, edit);
        if ("Insert" in edit) continue;
        if ("Update" in edit) {
          if (!this.terminalRows.has(rootId)) {
            throw new Error(`terminal root update addressed missing root ${rootId}`);
          }
          this.terminalRows.set(
            rootId,
            decodeNativeTerminalRow(rootId, rootColumns, Uint8Array.from(edit.Update.value)),
          );
        } else if ("Remove" in edit) {
          if (!this.terminalRows.delete(rootId)) {
            throw new Error(`terminal root removal addressed missing root ${rootId}`);
          }
          this.currentResults.delete(rootId);
          this.removeId(rootId);
        } else if ("Move" in edit) {
          if (!this.terminalRows.has(rootId)) {
            throw new Error(`terminal root move addressed missing root ${rootId}`);
          }
          this.removeId(rootId);
          this.insertIdAt(rootId, edit.Move.index);
        }
        affectedRoots.add(rootId);
        continue;
      }

      const root = this.terminalRows.get(rootId);
      if (!root) throw new Error(`terminal child edit addressed missing root ${rootId}`);
      const target = terminalCollection(root, rootColumns, operation.path);
      if (!target) throw new Error(`terminal child edit addressed an unresolved path on ${rootId}`);
      const { values, columns } = target;
      if ("Insert" in edit) {
        const id = terminalKeyId(edit.Insert.key);
        const row = decodeNativeTerminalRow(id, columns, Uint8Array.from(edit.Insert.value));
        const value: Value = { type: "Row", value: { id, values: row.values } };
        removeTerminalValue(values, id);
        values.splice(Math.max(0, Math.min(edit.Insert.index, values.length)), 0, value);
      } else if ("Update" in edit) {
        const id = terminalKeyId(edit.Update.key);
        const index = terminalValueIndex(values, id);
        if (index === -1) throw new Error(`terminal child update addressed missing key ${id}`);
        const row = decodeNativeTerminalRow(id, columns, Uint8Array.from(edit.Update.value));
        values[index] = { type: "Row", value: { id, values: row.values } };
      } else if ("Remove" in edit) {
        const id = terminalKeyId(edit.Remove.key);
        if (!removeTerminalValue(values, id)) {
          throw new Error(`terminal child removal addressed missing key ${id}`);
        }
      } else if ("Move" in edit) {
        const id = terminalKeyId(edit.Move.key);
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
      this.currentResults.set(id, item);
      return [
        beforeIndex === undefined
          ? { kind: RowChangeKind.Added, id, index, item }
          : { kind: RowChangeKind.Updated, id, index, item },
      ];
    });
    return { delta, all: this.all() } as SubscriptionDelta<T>;
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

  private handleWireDelta(
    delta: WireRowDelta,
    transform: (row: WasmRow) => T,
    reset = false,
  ): SubscriptionDelta<T> {
    return this.handleTypedDelta(
      delta.map((change) => {
        switch (change.kind) {
          case RowChangeKind.Added:
            return {
              kind: RowChangeKind.Added,
              id: change.id,
              index: change.index,
              item: transform(change.row),
            };
          case RowChangeKind.Removed:
            return change;
          case RowChangeKind.Updated:
            return {
              kind: RowChangeKind.Updated,
              id: change.id,
              index: change.index,
              item: change.row ? transform(change.row) : undefined,
            };
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
    this.currentResults.clear();
    this.terminalRows.clear();
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

export function isNativeRowDelta(delta: SubscriptionWireDelta): delta is NativeRowDelta {
  return !Array.isArray(delta) && delta.__jazzNativeRowDelta === true;
}

function terminalKeyId(encoded: readonly number[]): string {
  const bytes = Uint8Array.from(encoded);
  if (bytes.length === 17 && bytes[0] === 10) {
    return readUuid(bytes, 1);
  }
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function assertTerminalRootEditKey(
  rootKey: readonly number[],
  edit: NativeTerminalOperation["edit"],
): void {
  const editKey =
    "Insert" in edit
      ? edit.Insert.key
      : "Update" in edit
        ? edit.Update.key
        : "Remove" in edit
          ? edit.Remove.key
          : edit.Move.key;
  if (rootKey.length !== editKey.length || rootKey.some((byte, index) => byte !== editKey[index])) {
    throw new Error("terminal root edit key does not match its addressed root key");
  }
}

function terminalValueIndex(values: Value[], id: string): number {
  return values.findIndex((value) => value.type === "Row" && value.value.id === id);
}

function cloneTerminalRow(row: WasmRow, columns: readonly ColumnDescriptor[]): WasmRow {
  const values = structuredClone(row.values) as Value[];
  const clone = { id: row.id, values };
  Object.defineProperty(clone, "valuesByColumn", {
    value: new Map(columns.map((column, index) => [column.name, values[index]!])),
  });
  return clone;
}

function removeTerminalValue(values: Value[], id: string): boolean {
  const index = terminalValueIndex(values, id);
  if (index === -1) return false;
  values.splice(index, 1);
  return true;
}

function terminalCollection(
  root: WasmRow,
  rootColumns: readonly ColumnDescriptor[],
  path: NativeTerminalOperation["path"],
): { values: Value[]; columns: readonly ColumnDescriptor[] } | undefined {
  let ownerValues = root.values;
  let columns = rootColumns;
  for (let index = 0; index < path.length; index += 1) {
    const segment = path[index]!;
    if (!("Collection" in segment)) return undefined;
    const collectionName = segment.Collection.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX)
      ? segment.Collection.slice(HIDDEN_INCLUDE_COLUMN_PREFIX.length)
      : segment.Collection;
    const columnIndex = columns.findIndex((candidate) => candidate.name === collectionName);
    const column = columns[columnIndex];
    const columnType = column?.column_type;
    if (columnType?.type !== "Array" || columnType.element.type !== "Row") return undefined;
    const collection = ownerValues[columnIndex];
    if (collection?.type !== "Array") return undefined;
    const values = collection.value;
    const childColumns = columnType.element.columns;
    if (index === path.length - 1) return { values, columns: childColumns };
    const keySegment = path[++index];
    if (!keySegment || !("Key" in keySegment)) return undefined;
    const childId = terminalKeyId(keySegment.Key);
    const child = values.find((value) => value.type === "Row" && value.value.id === childId);
    if (child?.type !== "Row") return undefined;
    ownerValues = child.value.values;
    columns = childColumns;
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

export function decodeNativeDelta(
  native: NativeRowDelta,
  columns: readonly ColumnDescriptor[],
): WireRowDelta {
  const delta: WireRowDelta = [];

  {
    const bytes = native.updated;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let offset = 0;
    for (let i = 0; i < native.updatedCount; i++) {
      const id = readUuid(bytes, offset);
      offset += 16;
      const index = view.getUint32(offset, true);
      offset += 4;
      const flags = bytes[offset] ?? 0;
      offset += 1;
      if (flags & 1) {
        const len = view.getUint32(offset, true);
        offset += 4;
        const data = bytes.subarray(offset, offset + len);
        offset += len;
        delta.push({
          kind: RowChangeKind.Updated,
          id,
          index,
          row: decodeNativeRow(id, columns, data),
        });
      } else {
        delta.push({ kind: RowChangeKind.Updated, id, index });
      }
    }
  }

  {
    const bytes = native.added;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let offset = 0;
    for (let i = 0; i < native.addedCount; i++) {
      const id = readUuid(bytes, offset);
      offset += 16;
      const index = view.getUint32(offset, true);
      offset += 4;
      const len = view.getUint32(offset, true);
      offset += 4;
      const data = bytes.subarray(offset, offset + len);
      offset += len;
      delta.push({
        kind: RowChangeKind.Added,
        id,
        index,
        row: decodeNativeRow(id, columns, data),
      });
    }
  }

  {
    const bytes = native.removed;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let offset = 0;
    for (let i = 0; i < native.removedCount; i++) {
      const id = readUuid(bytes, offset);
      offset += 16;
      const index = view.getUint32(offset, true);
      offset += 4;
      delta.push({ kind: RowChangeKind.Removed, id, index });
    }
  }

  return delta;
}
