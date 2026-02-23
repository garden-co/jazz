/**
 * Manage subscription state and compute deltas.
 *
 * Tracks the current result set for a subscription and transforms
 * WASM row deltas into typed object deltas with full state tracking.
 */

import type { WasmRow, RowDelta } from "../drivers/types.js";

/**
 * Delta result from a subscription callback.
 *
 * Contains the full current state (`all`) plus granular changes
 * (`added`, `updated`, `removed`) for efficient UI updates.
 */
export interface SubscriptionDelta<T> {
  /** Current full result set after applying this delta */
  all: T[];
  /** Items added in this delta */
  added: T[];
  /** Items updated in this delta (new values) */
  updated: T[];
  /** Items removed in this delta */
  removed: T[];
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

  /**
   * Process a row delta and return typed object delta.
   *
   * @param delta Raw row delta from WASM runtime
   * @param transform Function to convert WasmRow to typed object T
   * @returns Typed delta with full state and changes
   */
  handleDelta(delta: RowDelta, transform: (row: WasmRow) => T): SubscriptionDelta<T> {
    const added: T[] = [];
    const updated: T[] = [];
    const removed: T[] = [];

    // Process additions
    for (const row of delta.added) {
      const item = transform(row);
      this.currentResults.set(item.id, item);
      added.push(item);
    }

    // Process updates - delta.updated is array of [oldRow, newRow] tuples
    for (const [_oldRow, newRow] of delta.updated) {
      const newItem = transform(newRow);
      this.currentResults.set(newItem.id, newItem);
      updated.push(newItem);
    }

    // Process removals
    for (const row of delta.removed) {
      const item = this.currentResults.get(row.id);
      if (item) {
        this.currentResults.delete(row.id);
        removed.push(item);
      }
    }

    return {
      all: Array.from(this.currentResults.values()),
      added,
      updated,
      removed,
    };
  }

  /**
   * Clear all tracked state.
   *
   * Called when unsubscribing to free memory.
   */
  clear(): void {
    this.currentResults.clear();
  }

  /**
   * Get the current number of tracked items.
   */
  get size(): number {
    return this.currentResults.size;
  }
}
