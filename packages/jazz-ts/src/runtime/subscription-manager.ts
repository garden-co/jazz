/**
 * Manage subscription state and compute deltas.
 *
 * Tracks the current result set for a subscription and transforms
 * WASM row deltas into typed object deltas with full state tracking.
 */

import type { WasmRow, RowDelta } from "../drivers/types.js";

export interface IndexedItem<T> {
  item: T;
  index: number;
}

export interface UpdatedIndexedItem<T> {
  oldItem: T;
  newItem: T;
  oldIndex: number;
  newIndex: number;
}

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
  added: IndexedItem<T>[];
  /** Items updated in this delta */
  updated: UpdatedIndexedItem<T>[];
  /** Items removed in this delta */
  removed: IndexedItem<T>[];
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
  private currentResults: T[] = [];

  /**
   * Process a row delta and return typed object delta.
   *
   * @param delta Raw row delta from WASM runtime
   * @param transform Function to convert WasmRow to typed object T
   * @returns Typed delta with full state and changes
   */
  handleDelta(delta: RowDelta, transform: (row: WasmRow) => T): SubscriptionDelta<T> {
    const added: IndexedItem<T>[] = [];
    const updated: UpdatedIndexedItem<T>[] = [];
    const removed: IndexedItem<T>[] = [];

    for (const { row, index } of delta.added) {
      added.push({ item: transform(row), index });
    }

    for (const { old_row, new_row, old_index, new_index } of delta.updated) {
      const oldItem = transform(old_row);
      const newItem = transform(new_row);
      if (oldItem.id === newItem.id) {
        updated.push({
          oldItem,
          newItem,
          oldIndex: old_index,
          newIndex: new_index,
        });
      } else {
        // Identity changes are represented as remove+add for deterministic patching.
        removed.push({ item: oldItem, index: old_index });
        added.push({ item: newItem, index: new_index });
      }
    }

    for (const { row, index } of delta.removed) {
      removed.push({ item: transform(row), index });
    }

    this.currentResults = this.buildPostState(this.currentResults, added, updated, removed);

    return {
      all: [...this.currentResults],
      added,
      updated,
      removed,
    };
  }

  private buildPostState(
    previous: T[],
    added: IndexedItem<T>[],
    updated: UpdatedIndexedItem<T>[],
    removed: IndexedItem<T>[],
  ): T[] {
    const removedIds = new Set(removed.map(({ item }) => item.id));
    const updatedOldIds = new Set(updated.map(({ oldItem }) => oldItem.id));

    const unchanged = previous.filter(
      (item) => !removedIds.has(item.id) && !updatedOldIds.has(item.id),
    );
    const expectedLength = previous.length - removed.length + added.length;
    const post: Array<T | undefined> = new Array(Math.max(expectedLength, 0));

    for (const { newItem, newIndex } of updated) {
      if (newIndex >= 0) {
        post[newIndex] = newItem;
      }
    }

    for (const { item, index } of added) {
      if (index >= 0) {
        post[index] = item;
      }
    }

    let unchangedCursor = 0;
    for (let i = 0; i < post.length; i += 1) {
      if (post[i] === undefined && unchangedCursor < unchanged.length) {
        post[i] = unchanged[unchangedCursor++];
      }
    }

    // Fallback for sparse / out-of-range index payloads.
    while (unchangedCursor < unchanged.length) {
      post.push(unchanged[unchangedCursor++]);
    }

    return post.filter((item): item is T => item !== undefined);
  }

  /**
   * Clear all tracked state.
   *
   * Called when unsubscribing to free memory.
   */
  clear(): void {
    this.currentResults = [];
  }

  /**
   * Get the current number of tracked items.
   */
  get size(): number {
    return this.currentResults.length;
  }
}
