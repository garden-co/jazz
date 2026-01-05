/**
 * Shared types for @jazz/client
 */

// Re-export schema runtime types for convenience
export {
  type TableMeta,
  type SchemaMeta,
  type ColumnMeta,
  type ColumnType,
  type RefMeta,
  type ReverseRefMeta,
  type BaseWhereInput,
  type IncludeSpec,
  type SubscribeAllOptions,
  type SubscribeOptions,
  type Unsubscribe,
  buildQuery,
  buildQueryById,
} from "@jazz/schema/runtime";

/**
 * Delta types for incremental updates
 */
export const DELTA_ADDED = 1;
export const DELTA_UPDATED = 2;
export const DELTA_REMOVED = 3;

export type DeltaType =
  | typeof DELTA_ADDED
  | typeof DELTA_UPDATED
  | typeof DELTA_REMOVED;

/**
 * Decoded delta from binary format
 */
export type Delta<T> =
  | { type: "added"; row: T }
  | { type: "updated"; row: T }
  | { type: "removed"; id: string };

/**
 * Row decoder function type
 */
export type RowDecoder<T> = (buffer: ArrayBufferLike) => T[];

/**
 * Delta decoder function type
 */
export type DeltaDecoder<T> = (buffer: ArrayBufferLike) => Delta<T>;

/**
 * Decoder pair for a table
 */
export interface TableDecoder<T> {
  rows: RowDecoder<T>;
  delta: DeltaDecoder<T>;
}

/**
 * Minimal WASM database interface
 * This is the subset of WasmDatabase methods we need
 */
export interface WasmDatabaseLike {
  execute(sql: string): unknown;
  subscribe_delta(
    sql: string,
    callback: (deltas: Uint8Array[]) => void
  ): WasmQueryHandleLike;
  update_row(
    table: string,
    rowId: string,
    column: string,
    value: string | bigint
  ): boolean;
}

/**
 * Minimal WASM query handle interface
 */
export interface WasmQueryHandleLike {
  unsubscribe(): void;
  free(): void;
  /** Get a text diagram of the query graph */
  diagram(): string;
}
