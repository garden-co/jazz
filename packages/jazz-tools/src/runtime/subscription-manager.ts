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
  NativeTerminalRootLayout,
  SubscriptionWireDelta,
  Value,
  WasmRow,
  RowDelta as WireRowDelta,
} from "../drivers/types.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "./select-projection.js";
import {
  decodeNativeRow,
  decodeNativeTerminalRow,
  decodeNativeTerminalRowWithDescriptor,
  compileNativeTerminalRootDecoder,
  logicalStorageColumns,
  readDescriptor,
} from "./native-runtime/native-row-codec.js";
import { PostcardReader } from "./native-runtime/native-codec.js";

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
  terminalRootDecoders: Map<string, TerminalRootDecoder>;
  deferredTerminalOperations: NativeTerminalOperation[];
};

type TerminalRootDecoder = {
  signature: string;
  decode: (id: string, raw: Uint8Array) => WasmRow;
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
  /** Exact ordered Groove root key -> opaque v2 occurrence sidecar address. */
  private terminalOccurrenceAddresses = new Map<string, string>();
  private orderedIds: string[] = [];
  private orderedIdIndex = new Map<string, number>();
  private terminalRootDecoders = new Map<string, TerminalRootDecoder>();
  /** Child edits received before a non-durable browser root hydration. */
  private deferredTerminalOperations: NativeTerminalOperation[] = [];

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
          this.clearRows();
          this.deferredTerminalOperations = [];
        }
        this.registerTerminalRootLayouts(delta.terminalLayouts, nativeColumns);
        for (const key of [
          ...(delta.addedOccurrenceKeys ?? []),
          ...(delta.updatedOccurrenceKeys ?? []),
          ...(delta.removedOccurrenceKeys ?? []),
        ]) {
          const orderedKey = orderedTerminalKeyForTypedOccurrence(key);
          if (orderedKey) {
            this.registerTerminalOccurrenceAddress(orderedKey, publicResultKey(key));
          } else if (key[0] === 2) {
            throw new Error("malformed or noncanonical typed terminal occurrence key");
          }
        }
        // Packed row deltas have already been normalized to the public logical
        // record layout. Only Groove terminal edit payloads retain the outer
        // sparse current-row carrier described by `sparse`.
        const decoded = decodeNativeDelta(delta, logicalStorageColumns(nativeColumns));
        for (const change of decoded) {
          if (change.kind === RowChangeKind.Removed) {
            this.terminalRows.delete(change.id);
          } else if (change.row) {
            // The plain native decoder exposes both positional values and a
            // name map, but decodes them independently. Terminal edits mutate
            // the positional tree, so normalize the retained terminal copy to
            // one shared value graph before accepting descendant operations.
            this.terminalRows.set(change.id, cloneTerminalRow(change.row, nativeColumns));
          }
        }
        const wireResult = this.handleWireDelta(decoded, transform, reset);
        const terminalOperations = this.readyTerminalOperations(delta.terminalOperations ?? []);
        if (terminalOperations.length > 0) {
          const terminalResult = this.handleTerminalOperations(
            terminalOperations,
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
      terminalOccurrenceAddresses: new Map(this.terminalOccurrenceAddresses),
      orderedIds: [...this.orderedIds],
      orderedIdIndex: new Map(this.orderedIdIndex),
      terminalRootDecoders: new Map(this.terminalRootDecoders),
      deferredTerminalOperations: [...this.deferredTerminalOperations],
    };
  }

  private restore(snapshot: SubscriptionManagerSnapshot<T>): void {
    this.currentResults = snapshot.currentResults;
    this.terminalRows = snapshot.terminalRows;
    this.terminalOccurrenceAddresses = snapshot.terminalOccurrenceAddresses;
    this.orderedIds = snapshot.orderedIds;
    this.orderedIdIndex = snapshot.orderedIdIndex;
    this.terminalRootDecoders = snapshot.terminalRootDecoders;
    this.deferredTerminalOperations = snapshot.deferredTerminalOperations;
  }

  private registerTerminalRootLayouts(
    layouts: readonly NativeTerminalRootLayout[] | undefined,
    columns: readonly ColumnDescriptor[],
  ): void {
    for (const layout of layouts ?? []) {
      const signature = JSON.stringify(layout);
      const existing = this.terminalRootDecoders.get(layout.id);
      if (existing) {
        if (existing.signature !== signature) {
          throw new Error(`terminal root layout ${layout.id} was redefined`);
        }
        continue;
      }
      const reader = new PostcardReader(Uint8Array.from(layout.rootDescriptor));
      const descriptor = readDescriptor(reader);
      if (!reader.done()) throw new Error("terminal root layout descriptor has trailing bytes");
      this.terminalRootDecoders.set(layout.id, {
        signature,
        decode: compileNativeTerminalRootDecoder(layout, descriptor, columns),
      });
    }
  }

  private decodeNativeTerminalRoot(
    id: string,
    operation: NativeTerminalOperation,
    columns: readonly ColumnDescriptor[],
    raw: Uint8Array,
  ): WasmRow {
    if (operation.rootLayoutId) {
      const decoder = this.terminalRootDecoders.get(operation.rootLayoutId);
      if (!decoder) {
        throw new Error(
          `terminal operation references unknown root layout ${operation.rootLayoutId}`,
        );
      }
      return decoder.decode(id, raw);
    }
    // Kept solely for older test fixtures and a mixed-version native addon.
    // Current Rust producers publish a layout before emitting any operation.
    if (!operation.rootDescriptor) {
      throw new Error("terminal operation is missing its root descriptor or layout ID");
    }
    const reader = new PostcardReader(Uint8Array.from(operation.rootDescriptor));
    const descriptor = readDescriptor(reader);
    if (!reader.done()) throw new Error("terminal root descriptor has trailing bytes");
    return decodeNativeTerminalRowWithDescriptor(id, descriptor, columns, raw);
  }

  /**
   * Preserve a child splice that raced ahead of its native root hydration.
   * A root insert in this same frame satisfies the dependency because the
   * terminal reducer pre-establishes such roots before applying children.
   */
  private readyTerminalOperations(incoming: NativeTerminalOperation[]): NativeTerminalOperation[] {
    const operations = [...this.deferredTerminalOperations, ...incoming];
    this.deferredTerminalOperations = [];
    const insertedRoots = new Set(
      operations
        .filter((operation) => operation.path.length === 0 && "Insert" in operation.edit)
        .map((operation) => this.terminalAddress(operation.root_key)),
    );
    const ready: NativeTerminalOperation[] = [];
    for (const operation of operations) {
      if (
        operation.path.length > 0 &&
        !insertedRoots.has(this.terminalAddress(operation.root_key)) &&
        !this.terminalRows.has(this.terminalRootId(operation.root_key))
      ) {
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
    operations: NativeTerminalOperation[],
    transform: (row: WasmRow) => T,
    rootColumns: readonly ColumnDescriptor[],
  ): SubscriptionDelta<T> {
    const beforeIndices = new Map(this.orderedIdIndex);
    const affectedRoots = new Set<string>();
    // Pre-establish only newly inserted root payloads so child-before-root
    // batches are addressable. Positional insertion remains in producer order:
    // applying its index before an earlier root Remove makes the outcome depend
    // on operation-key ordering.
    for (const operation of operations) {
      if (operation.path.length !== 0 || !("Insert" in operation.edit)) continue;
      const rootId = this.terminalAddress(operation.root_key);
      const rootRowId = terminalPayloadRowId(operation.root_key);
      const edit = operation.edit;
      assertTerminalRootEditKey(operation.root_key, edit);
      if (!("Insert" in edit)) throw new Error("terminal root insert partition is invalid");
      this.terminalRows.set(
        rootId,
        this.decodeNativeTerminalRoot(
          rootRowId,
          operation,
          rootColumns,
          Uint8Array.from(edit.Insert.value),
        ),
      );
    }

    for (const operation of operations) {
      const rootId = this.terminalRootId(operation.root_key);
      const edit = operation.edit;
      if (operation.path.length === 0) {
        assertTerminalRootEditKey(operation.root_key, edit);
        if ("Insert" in edit) {
          if (!this.terminalRows.has(rootId)) {
            throw new Error(`terminal root insert payload is missing for ${rootId}`);
          }
          this.removeId(rootId);
          this.insertIdAt(rootId, edit.Insert.index);
        } else if ("Update" in edit) {
          if (!this.terminalRows.has(rootId)) {
            if (isUuidOnlyTerminalKey(operation.root_key)) continue;
            throw new Error(`terminal root update addressed missing root ${rootId}`);
          }
          this.terminalRows.set(
            rootId,
            this.decodeNativeTerminalRoot(
              terminalPayloadRowId(operation.root_key),
              operation,
              rootColumns,
              Uint8Array.from(edit.Update.value),
            ),
          );
        } else if ("Remove" in edit) {
          if (!this.terminalRows.delete(rootId)) {
            if (isUuidOnlyTerminalKey(operation.root_key)) continue;
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
      assertTerminalPathEditKey(operation.path, edit);
      const target = terminalCollection(root, rootColumns, operation.path);
      if (!target) throw new Error(`terminal child edit addressed an unresolved path on ${rootId}`);
      const { values, columns } = target;
      if ("Insert" in edit) {
        const id = terminalPayloadRowId(edit.Insert.key);
        const row = decodeNativeTerminalRow(id, columns, Uint8Array.from(edit.Insert.value));
        const value: Value = { type: "Row", value: { id, values: row.values } };
        removeTerminalValue(values, id);
        values.splice(Math.max(0, Math.min(edit.Insert.index, values.length)), 0, value);
      } else if ("Update" in edit) {
        const id = terminalPayloadRowId(edit.Update.key);
        const index = terminalValueIndex(values, id);
        if (index === -1) throw new Error(`terminal child update addressed missing key ${id}`);
        const row = decodeNativeTerminalRow(id, columns, Uint8Array.from(edit.Update.value));
        values[index] = { type: "Row", value: { id, values: row.values } };
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

  /**
   * Snapshots predating occurrence sidecars seed terminal state by physical
   * row UUID. Prefer the full ResultKey address, but bridge that legacy
   * snapshot only when exactly one retained root has the addressed UUID.
   */
  private terminalRootId(encoded: readonly number[]): string {
    const address = this.terminalAddress(encoded);
    if (this.terminalRows.has(address)) return address;
    // Only UUID-only legacy composite keys have a lossless correspondence to
    // an occurrence sidecar. Typed values and v2 identities must remain
    // opaque: matching either by physical UUID would erase a discriminator.
    if (!address.startsWith("result:01")) return address;
    const physicalId = terminalPayloadRowId(encoded);
    const matches = Array.from(this.terminalRows, ([id, row]) =>
      row.id === physicalId && !id.startsWith("result:") ? id : null,
    ).filter((id): id is string => id !== null);
    return matches.length === 1 ? matches[0]! : address;
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

  private handleWireDelta(
    delta: WireRowDelta,
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

    // A new row version may render exactly the same public item. It is still
    // useful to publish that update, but it cannot legitimately change query
    // order. Preserve the retained position when a provisional source carries
    // an older index while reconciling its worker baseline.
    const inertEqualUpdates = new Set<string>();
    for (const change of delta) {
      if (change.kind !== RowChangeKind.Updated || change.item === undefined) continue;
      const previous = this.currentResults.get(change.id);
      if (previous !== undefined && sameSubscriptionItem(previous, change.item)) {
        inertEqualUpdates.add(change.id);
      }
    }

    const changesToApply = delta.filter(
      (change) => change.kind !== RowChangeKind.Updated || !inertEqualUpdates.has(change.id),
    );

    if (shouldApplyDeltaInBulk(changesToApply)) {
      this.applyBulkTypedDelta(changesToApply);
      for (const change of delta) {
        if (inertEqualUpdates.has(change.id)) change.index = this.orderedIdIndex.get(change.id)!;
      }
      return { delta, all: this.all() } as SubscriptionDelta<T>;
    }

    for (const change of changesToApply) {
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

    for (const change of delta) {
      if (inertEqualUpdates.has(change.id)) change.index = this.orderedIdIndex.get(change.id)!;
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
    this.terminalRootDecoders.clear();
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

function sameSubscriptionItem(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (left instanceof Uint8Array && right instanceof Uint8Array) {
    return (
      left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index])
    );
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length &&
      left.every((value, index) => sameSubscriptionItem(value, right[index]))
    );
  }
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object") {
    return false;
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftKeys = Object.keys(leftRecord);
  const rightKeys = Object.keys(rightRecord);
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      (key) =>
        Object.prototype.hasOwnProperty.call(rightRecord, key) &&
        sameSubscriptionItem(leftRecord[key], rightRecord[key]),
    )
  );
}

export function isNativeRowDelta(delta: SubscriptionWireDelta): delta is NativeRowDelta {
  return !Array.isArray(delta) && delta.__jazzNativeRowDelta === true;
}

/**
 * Reconstruct the exact Groove ordered root key from a typed ResultKey v2.
 * This is metadata-driven: a terminal key is accepted as typed only when it
 * byte-for-byte matches the sidecar's root, joined UUIDs, and union-arm
 * discriminators. It intentionally does not infer meaning from a string in an
 * arbitrary ordered key.
 */
function orderedTerminalKeyForTypedOccurrence(sidecar: Uint8Array): Uint8Array | undefined {
  if (sidecar[0] !== 2 || sidecar.byteLength < 25) return undefined;
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
  // ResultKey v2 exists only for union-discriminated output. Allowing no arms
  // aliases the UUID-only v1 ordered key, so it is deliberately noncanonical.
  if (discriminatorCount === 0 || discriminatorCount > joinedCount) return undefined;
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

function assertTerminalPathEditKey(
  path: NativeTerminalOperation["path"],
  edit: NativeTerminalOperation["edit"],
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

function terminalEditKey(edit: NativeTerminalOperation["edit"]): readonly number[] {
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
    const childId = terminalPayloadRowId(keySegment.Key);
    if (index === path.length - 1) return { values, columns: childColumns };
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
      const sourceId = readUuid(bytes, offset);
      const id = native.updatedOccurrenceKeys?.[i]
        ? publicResultKey(native.updatedOccurrenceKeys[i]!)
        : sourceId;
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
          row: decodeNativeRow(sourceId, columns, data),
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
      const sourceId = readUuid(bytes, offset);
      const id = native.addedOccurrenceKeys?.[i]
        ? publicResultKey(native.addedOccurrenceKeys[i]!)
        : sourceId;
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
        row: decodeNativeRow(sourceId, columns, data),
      });
    }
  }

  {
    const bytes = native.removed;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let offset = 0;
    for (let i = 0; i < native.removedCount; i++) {
      const sourceId = readUuid(bytes, offset);
      const id = native.removedOccurrenceKeys?.[i]
        ? publicResultKey(native.removedOccurrenceKeys[i]!)
        : sourceId;
      offset += 16;
      const index = view.getUint32(offset, true);
      offset += 4;
      delta.push({ kind: RowChangeKind.Removed, id, index });
    }
  }

  return delta;
}

function publicResultKey(bytes: Uint8Array): string {
  if (bytes.length === 17 && bytes[0] === 1) return readUuid(bytes, 1);
  return `result:${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}
