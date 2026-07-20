/**
 * Manage subscription state and compute deltas.
 *
 * Tracks the current result set for a subscription and transforms
 * WASM row deltas into typed object deltas with full state tracking.
 */

import type {
  ColumnDescriptor,
  NativeRowDelta,
  SubscriptionWireDelta,
  WasmRow,
  RowDelta as WireRowDelta,
} from "../drivers/types.js";
import { decodeNativeRow } from "./native-runtime/native-row-codec.js";

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
      if (reset) {
        this.clear();
      }
      return this.handleWireDelta(decodeNativeDelta(delta, nativeColumns), transform, reset);
    }

    return this.handleWireDelta(delta, transform);
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
