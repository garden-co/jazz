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
import { decodeNativeRow, decodeNativeRowObject } from "./native-row-format.js";

const RowChangeKind = {
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

/**
 * Delta result from a subscription callback.
 *
 * Contains the full current state (`all`) plus an ordered row-change stream.
 */
export interface SubscriptionDelta<T> {
  /** Current full result set after applying this delta */
  all: T[];
  /** Ordered list of changes for this delta */
  delta: RowDelta<T>[];
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

  private removeId(id: string): void {
    const index = this.orderedIds.indexOf(id);
    if (index !== -1) {
      this.orderedIds.splice(index, 1);
    }
  }

  private insertIdAt(id: string, index: number): void {
    const clamped = Math.max(0, Math.min(index, this.orderedIds.length));
    this.orderedIds.splice(clamped, 0, id);
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
    nativeTransform?: (row: Record<string, unknown>) => T,
  ): SubscriptionDelta<T> {
    if (isNativeRowDelta(delta)) {
      if (!nativeColumns) {
        throw new Error("Native subscription delta requires output columns for decoding");
      }
      if (nativeTransform) {
        return this.handleTypedDelta(decodeNativeTypedDelta(delta, nativeColumns, nativeTransform));
      }
      return this.handleWireDelta(decodeNativeDelta(delta, nativeColumns), transform);
    }

    return this.handleWireDelta(delta, transform);
  }

  private handleWireDelta(
    delta: WireRowDelta,
    transform: (row: WasmRow) => T,
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
    );
  }

  private handleTypedDelta(delta: RowDelta<T>[]): SubscriptionDelta<T> {
    delta.sort((a, b) => a.index - b.index);

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
      all: this.orderedIds
        .map((id) => this.currentResults.get(id))
        .filter((item): item is T => item !== undefined),
      delta,
    };
  }

  /**
   * Clear all tracked state.
   *
   * Called when unsubscribing to free memory.
   */
  clear(): void {
    this.currentResults.clear();
    this.orderedIds = [];
  }

  /**
   * Get the current number of tracked items.
   */
  get size(): number {
    return this.currentResults.size;
  }
}

function decodeNativeTypedDelta<T extends { id: string }>(
  native: NativeRowDelta,
  columns: readonly ColumnDescriptor[],
  transform: (row: Record<string, unknown>) => T,
): RowDelta<T>[] {
  const delta: RowDelta<T>[] = [];

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
        item: transform(decodeNativeRowObject(id, columns, data)),
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
          item: transform(decodeNativeRowObject(id, columns, data)),
        });
      } else {
        delta.push({ kind: RowChangeKind.Updated, id, index });
      }
    }
  }

  return delta;
}

function isNativeRowDelta(delta: SubscriptionWireDelta): delta is NativeRowDelta {
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

function decodeNativeDelta(
  native: NativeRowDelta,
  columns: readonly ColumnDescriptor[],
): WireRowDelta {
  const delta: WireRowDelta = [];

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

  return delta;
}
