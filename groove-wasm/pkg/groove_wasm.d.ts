/* tslint:disable */
/* eslint-disable */

export class WasmDatabase {
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Create a new in-memory database.
   */
  constructor();
  /**
   * Execute a SQL statement (legacy string-based results).
   */
  execute(sql: string): any;
  /**
   * Execute a SELECT query and return results as binary Uint8Array.
   *
   * Binary format:
   * - u32: row_count
   * - Per row:
   *   - 26 bytes: ObjectId (Base32 UTF-8)
   *   - Column values in schema order
   */
  select_binary(sql: string): Uint8Array;
  /**
   * Update a specific row's column with a string value.
   * row_id should be a Base32 ObjectId string.
   */
  update_row(table: string, row_id: string, column: string, value: string): boolean;
  /**
   * Update a specific row's column with an i64 value.
   * row_id should be a Base32 ObjectId string.
   */
  update_row_i64(table: string, row_id: string, column: string, value: bigint): boolean;
  /**
   * Initialize the database schema from a SQL string containing CREATE TABLE statements.
   * Statements are separated by semicolons.
   */
  init_schema(schema: string): void;
  /**
   * Create an incremental query that calls back on changes (legacy string-based).
   * Returns a handle that must be kept alive to maintain the subscription.
   */
  subscribe(sql: string, callback: Function): WasmQueryHandle;
  /**
   * Create an incremental query that calls back with binary data on changes.
   * The callback receives a Uint8Array in the binary row format.
   * Returns a handle that must be kept alive to maintain the subscription.
   */
  subscribe_binary(sql: string, callback: Function): WasmQueryHandleBinary;
  /**
   * Create an incremental query that calls back with individual delta buffers.
   * The callback receives an Array of Uint8Array, one per delta.
   * Each delta is: u8 type (1=add, 2=update, 3=remove) + row data (or just id for removes).
   * Returns a handle that must be kept alive to maintain the subscription.
   */
  subscribe_delta(sql: string, callback: Function): WasmQueryHandleDelta;
}

export class WasmQueryHandle {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Unsubscribe from updates.
   */
  unsubscribe(): void;
}

export class WasmQueryHandleBinary {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Unsubscribe from updates.
   */
  unsubscribe(): void;
}

export class WasmQueryHandleDelta {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Unsubscribe from updates.
   */
  unsubscribe(): void;
  /**
   * Get a text diagram of the query graph.
   *
   * Returns a human-readable representation of the computation DAG
   * showing node types, predicates, and current cache states.
   */
  diagram(): string;
}

/**
 * Initialize panic hook for better error messages.
 */
export function init(): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_wasmdatabase_free: (a: number, b: number) => void;
  readonly wasmdatabase_new: () => number;
  readonly wasmdatabase_execute: (a: number, b: number, c: number) => [number, number, number];
  readonly wasmdatabase_select_binary: (a: number, b: number, c: number) => [number, number, number];
  readonly wasmdatabase_update_row: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => [number, number, number];
  readonly wasmdatabase_update_row_i64: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: bigint) => [number, number, number];
  readonly wasmdatabase_init_schema: (a: number, b: number, c: number) => [number, number];
  readonly wasmdatabase_subscribe: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly wasmdatabase_subscribe_binary: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly wasmdatabase_subscribe_delta: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly __wbg_wasmqueryhandle_free: (a: number, b: number) => void;
  readonly wasmqueryhandle_unsubscribe: (a: number) => void;
  readonly __wbg_wasmqueryhandlebinary_free: (a: number, b: number) => void;
  readonly wasmqueryhandlebinary_unsubscribe: (a: number) => void;
  readonly __wbg_wasmqueryhandledelta_free: (a: number, b: number) => void;
  readonly wasmqueryhandledelta_unsubscribe: (a: number) => void;
  readonly wasmqueryhandledelta_diagram: (a: number) => [number, number];
  readonly init: () => void;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_externrefs: WebAssembly.Table;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
