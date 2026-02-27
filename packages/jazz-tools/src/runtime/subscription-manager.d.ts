/**
 * Manage subscription state and compute deltas.
 *
 * Tracks the current result set for a subscription and transforms
 * WASM row deltas into typed object deltas with full state tracking.
 */
import type { WasmRow, RowDelta as WireRowDelta } from "../drivers/types.js";
declare const RowChangeKind: {
  readonly Added: 0;
  readonly Removed: 1;
  readonly Updated: 2;
};
export type RowChangeKind = typeof RowChangeKind;
export type RowChangeKindValue = (typeof RowChangeKind)[keyof typeof RowChangeKind];
export type RowDelta<T> =
  | {
      kind: RowChangeKind["Added"];
      id: string;
      index: number;
      item: T;
    }
  | {
      kind: RowChangeKind["Removed"];
      id: string;
      index: number;
    }
  | {
      kind: RowChangeKind["Updated"];
      id: string;
      index: number;
      item?: T;
    };
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
export declare class SubscriptionManager<
  T extends {
    id: string;
  },
> {
  private currentResults;
  private orderedIds;
  private removeId;
  private insertIdAt;
  /**
   * Process a row delta and return typed object delta.
   *
   * @param delta Raw row delta from WASM runtime
   * @param transform Function to convert WasmRow to typed object T
   * @returns Typed delta with full state and changes
   */
  handleDelta(delta: WireRowDelta, transform: (row: WasmRow) => T): SubscriptionDelta<T>;
  /**
   * Clear all tracked state.
   *
   * Called when unsubscribing to free memory.
   */
  clear(): void;
  /**
   * Get the current number of tracked items.
   */
  get size(): number;
}

//# sourceMappingURL=subscription-manager.d.ts.map
