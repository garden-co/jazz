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
  handleDelta(delta: RowDelta, transform: (row: WasmRow) => T): SubscriptionDelta<T> {
    const added: T[] = [];
    const updated: T[] = [];
    const removed: T[] = [];

    // Process removals first (indices refer to pre-window).
    for (const row of delta.removed) {
      const item = this.currentResults.get(row.id);
      if (item) {
        this.currentResults.delete(row.id);
        this.removeId(row.id);
        removed.push(item);
      }
    }

    // Apply updates and moves.
    for (const change of delta.updated) {
      this.removeId(change.id);

      if (change.row) {
        const item = transform(change.row);
        this.currentResults.set(change.id, item);
      }

      const existing = this.currentResults.get(change.id);
      if (!existing) continue;

      this.insertIdAt(change.id, change.newIndex);
      updated.push(existing);
    }

    // Process additions last (indices refer to post-window).
    for (const change of delta.added) {
      const item = transform(change.row);
      this.currentResults.set(change.id, item);
      this.removeId(change.id);
      this.insertIdAt(change.id, change.index);
      added.push(item);
    }

    return {
      all: this.orderedIds
        .map((id) => this.currentResults.get(id))
        .filter((item): item is T => item !== undefined),
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
    this.orderedIds = [];
  }

  /**
   * Get the current number of tracked items.
   */
  get size(): number {
    return this.currentResults.size;
  }
}
