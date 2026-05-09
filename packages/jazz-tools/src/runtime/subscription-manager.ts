/**
 * Manage subscription state and compute deltas.
 *
 * Tracks the current result set for a subscription and transforms
 * WASM row deltas into typed object deltas with full state tracking.
 */

import type { WasmRow, RowDelta as WireRowDelta } from "../drivers/types.js";

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
  handleDelta(delta: WireRowDelta, transform: (row: WasmRow) => T): SubscriptionDelta<T> {
    delta.sort((a, b) => a.index - b.index);

    for (const change of delta) {
      switch (change.kind) {
        case RowChangeKind.Added:
          const alreadyPresent = this.currentResults.has(change.id);
          this.currentResults.set(change.id, transform(change.row));
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
          if (change.row) {
            this.currentResults.set(change.id, transform(change.row));
          }
          break;
      }
    }

    return {
      all: this.orderedIds
        .map((id) => this.currentResults.get(id))
        .filter((item): item is T => item !== undefined),
      delta: delta as RowDelta<T>[],
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
