/**
 * Shared types for @jazz/client
 */

// Import Unsubscribe for internal use
import type { Unsubscribe as UnsubscribeType } from "@jazz/schema/runtime";

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
    value: string
  ): boolean;
  update_row_i64(
    table: string,
    rowId: string,
    column: string,
    value: bigint
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

/**
 * Interface for subscribing to all rows with db passed at subscription time.
 * Used by TableDescriptor and QueryBuilder classes.
 *
 * @typeParam T - The row type returned by subscriptions
 * @typeParam CreateInput - The input type for creating new rows
 * @typeParam UpdateInput - The input type for updating existing rows
 */
export interface SubscribableAllWithDb<T, CreateInput, UpdateInput> {
  /** Subscribe to all matching rows */
  subscribeAll(
    db: WasmDatabaseLike,
    callback: (rows: T[]) => void
  ): UnsubscribeType;
  /** Create a new row */
  create(db: WasmDatabaseLike, values: CreateInput): string;
  /** Update an existing row */
  update(db: WasmDatabaseLike, id: string, values: UpdateInput): void;
  /** Delete a row */
  delete(db: WasmDatabaseLike, id: string): void;
  /** Optional query key for structural equality comparison in React hooks */
  _queryKey?: string;
}

/**
 * Interface for subscribing to a single row with db passed at subscription time.
 * Used by TableDescriptor and QueryBuilder classes.
 *
 * @typeParam T - The row type returned by subscriptions
 * @typeParam UpdateInput - The input type for updating existing rows
 */
export interface SubscribableOneWithDb<T, UpdateInput> {
  /** Subscribe to a single row by ID */
  subscribe(
    db: WasmDatabaseLike,
    id: string,
    callback: (row: T | null) => void
  ): UnsubscribeType;
  /** Update an existing row */
  update(db: WasmDatabaseLike, id: string, values: UpdateInput): void;
  /** Delete a row */
  delete(db: WasmDatabaseLike, id: string): void;
  /** Optional query key for structural equality comparison in React hooks */
  _queryKey?: string;
}

/**
 * Interface for mutating rows with db passed at call time.
 * Used by useMutate hook.
 */
export interface MutableWithDb<CreateInput, UpdateInput> {
  /** Create a new row */
  create(db: WasmDatabaseLike, values: CreateInput): string;
  /** Update an existing row */
  update(db: WasmDatabaseLike, id: string, values: UpdateInput): void;
  /** Delete a row */
  delete(db: WasmDatabaseLike, id: string): void;
}

/**
 * Mutation helpers returned by useAll hook
 */
export interface MutateAll<CreateInput, UpdateInput> {
  /** Create a new row */
  create(values: CreateInput): string;
  /** Update a row by id */
  update(id: string, values: UpdateInput): void;
  /** Delete a row by id */
  delete(id: string): void;
}

/**
 * Mutation helpers returned by useOne hook (id is captured)
 */
export interface MutateOne<UpdateInput> {
  /** Update the subscribed row */
  update(values: UpdateInput): void;
  /** Delete the subscribed row */
  delete(): void;
}
