/* tslint:disable */
/* eslint-disable */

/**
 * Connection state for the sync client.
 */
export enum ConnectionState {
  Disconnected = 0,
  Connecting = 1,
  Connected = 2,
}

/**
 * Connection state for the synced node (JS-compatible).
 */
export enum SyncState {
  Disconnected = 0,
  Connecting = 1,
  Connected = 2,
  Reconnecting = 3,
}

export class SyncedQueryHandle {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Unsubscribe from updates.
   */
  unsubscribe(): void;
  /**
   * Get a text diagram of the query graph.
   */
  diagram(): string;
  /**
   * Free resources (no-op, but required by WasmQueryHandleLike interface).
   */
  free(): void;
}

export class WasmBlobWriter {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Write a chunk of data to the blob.
   * Can be called multiple times before finish().
   */
  write(data: Uint8Array): void;
  /**
   * Get the current total size of data written.
   */
  size(): number;
  /**
   * Finish writing and get a blob handle.
   * Stores chunks in the Environment's ChunkStore for persistence.
   * The writer cannot be used after this.
   */
  finish(): bigint;
  /**
   * Abort the blob creation, discarding all written data.
   */
  abort(): void;
}

export class WasmDatabase {
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Create a new in-memory database.
   */
  constructor();
  /**
   * Create or open a persistent IndexedDB-backed database.
   *
   * If a database already exists in IndexedDB, it will be loaded.
   * Otherwise, a new database will be created.
   *
   * @param db_name - Optional database name (defaults to "groove")
   * @returns Promise that resolves to WasmDatabase
   */
  static withIndexedDb(db_name?: string | null): Promise<any>;
  /**
   * Check if a persistent database exists in IndexedDB.
   *
   * @param db_name - Optional database name (defaults to "groove")
   * @returns Promise that resolves to boolean
   */
  static hasPersistedDatabase(db_name?: string | null): Promise<any>;
  /**
   * Delete a persistent database from IndexedDB.
   *
   * @param db_name - Optional database name (defaults to "groove")
   * @returns Promise that resolves when deleted
   */
  static deletePersistedDatabase(db_name?: string | null): Promise<any>;
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
  updateRow(table: string, row_id: string, column: string, value: string): boolean;
  /**
   * Update a specific row's column with an i64 value.
   * row_id should be a Base32 ObjectId string.
   */
  updateRowI64(table: string, row_id: string, column: string, value: bigint): boolean;
  /**
   * Initialize the database schema from a SQL string containing CREATE TABLE statements.
   * Statements are separated by semicolons.
   */
  init_schema(schema: string): void;
  /**
   * List all tables in the database.
   * Returns an array of table names.
   */
  list_tables(): any;
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
  subscribeDelta(sql: string, callback: Function): WasmQueryHandleDelta;
  /**
   * Create a blob from raw bytes.
   * Returns a blob handle ID that can be used in insert/update operations.
   * Chunks are stored in the Environment's ChunkStore for persistence.
   */
  create_blob(data: Uint8Array): bigint;
  /**
   * Create a blob writer for streaming blob creation.
   * Call write_blob_chunk() to add data, then finish_blob() to get the handle.
   */
  create_blob_writer(): WasmBlobWriter;
  /**
   * Read all bytes from a blob handle.
   * For small blobs this returns the inline data directly.
   * For large chunked blobs, this reads and concatenates all chunks from Environment.
   * Use read_blob_chunk() for streaming reads of large blobs.
   */
  read_blob(handle_id: bigint): Uint8Array;
  /**
   * Get information about a blob.
   * Returns a JS object with: { isInline: bool, chunkCount: number, size?: number }
   */
  get_blob_info(handle_id: bigint): any;
  /**
   * Read a specific chunk of a blob by index.
   * For inline blobs, index 0 returns all data.
   * For chunked blobs, returns the chunk at the given index from Environment.
   */
  read_blob_chunk(handle_id: bigint, chunk_index: number): Uint8Array;
  /**
   * Release a blob handle, freeing the associated memory.
   * Call this when you're done with a blob to prevent memory leaks.
   */
  release_blob(handle_id: bigint): void;
  /**
   * Insert a row with blob values.
   * string_columns is an array of [column_name, value] pairs for string columns.
   * blob_columns is an array of [column_name, blob_handle_id] pairs.
   */
  insert_with_blobs(table: string, string_columns: any, blob_columns: any): string;
  /**
   * Update a row's blob column.
   */
  update_row_blob(table: string, row_id: string, column: string, blob_handle_id: bigint): boolean;
  /**
   * Get the current schema for a table.
   * Returns a JS object with column definitions.
   */
  getTableSchema(table: string): any;
  /**
   * Get the descriptor ID for a table.
   * Returns the ObjectId string (Crockford Base32 encoded).
   */
  getDescriptorId(table: string): string;
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

export class WasmSyncClient {
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Create a new sync client.
   *
   * # Arguments
   * * `base_url` - The sync server URL (e.g., "http://localhost:8080")
   * * `auth_token` - Bearer token for authentication
   */
  constructor(base_url: string, auth_token: string);
  /**
   * Set callback for commit events.
   *
   * Callback receives: (object_id: string, commits: Uint8Array[], frontier: string[])
   */
  setOnCommits(callback: Function): void;
  /**
   * Set callback for excluded events.
   *
   * Callback receives: (object_id: string)
   */
  setOnExcluded(callback: Function): void;
  /**
   * Set callback for error events.
   *
   * Callback receives: (code: number, message: string)
   */
  setOnError(callback: Function): void;
  /**
   * Set callback for connection state changes.
   *
   * Callback receives: (state: string)
   */
  setOnStateChange(callback: Function): void;
  /**
   * Subscribe to a query and start receiving updates.
   *
   * This opens an SSE connection to receive real-time updates.
   */
  subscribe(query: string): Promise<number>;
  /**
   * Push commits for an object to the server.
   */
  push(object_id: string, commits_data: Array<any>): Promise<any>;
  /**
   * Request reconciliation for an object.
   */
  reconcile(object_id: string, local_frontier: Array<any>): Promise<any>;
  /**
   * Disconnect from the server.
   */
  disconnect(): void;
  /**
   * Get current connection state.
   */
  readonly connectionState: ConnectionState;
}

export class WasmSyncedLocalNode {
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Create a new synced local node with in-memory storage.
   *
   * @param server_url - The sync server URL (e.g., "http://localhost:8080")
   * @param auth_token - Bearer token for authentication
   * @param catalog_id - Optional shared catalog ID (for sync between multiple clients)
   */
  constructor(server_url: string, auth_token: string, catalog_id?: string | null);
  /**
   * Create a synced local node with IndexedDB persistence.
   *
   * @param server_url - The sync server URL
   * @param auth_token - Bearer token for authentication
   * @param db_name - Optional database name (defaults to "groove")
   */
  static withIndexedDb(server_url: string, auth_token: string, db_name?: string | null): Promise<any>;
  /**
   * Set callback for sync state changes.
   *
   * Callback signature: (state: string) => void
   * States: "Disconnected", "Connecting", "Connected", "Reconnecting"
   */
  setOnStateChange(callback: Function): void;
  /**
   * Set callback for sync errors.
   *
   * Callback signature: (message: string) => void
   */
  setOnError(callback: Function): void;
  /**
   * Connect to the sync server and start receiving updates.
   *
   * This subscribes to the given query and starts an SSE stream
   * to receive real-time updates from other clients. The connection
   * automatically reconnects with exponential backoff on disconnection.
   *
   * The promise resolves once the initial connection is established.
   * The event loop continues running in the background.
   */
  connect(query: string): Promise<any>;
  /**
   * Execute a SQL statement.
   *
   * For INSERT/UPDATE operations, this automatically pushes the affected
   * objects to upstream servers.
   */
  execute(sql: string): any;
  /**
   * Provision or find the viewer (current user) and set it on the database.
   *
   * This method looks up or creates a user row in the "users" table with the
   * given external_id and name, then sets that user as the @viewer for
   * subsequent INSERT/UPDATE statements.
   *
   * Note: This does NOT push the user row to the server immediately. The user
   * row will be synced when connect() is called and the sync subscription
   * includes the users table.
   *
   * @param external_id - The external user ID (e.g., from JWT sub claim)
   * @param name - The user's display name
   * @returns The user's ObjectId as a string
   */
  provisionViewer(external_id: string, name: string): string;
  /**
   * Execute a SELECT query and return results as binary Uint8Array.
   */
  selectBinary(sql: string): Uint8Array;
  /**
   * Initialize the database schema from a SQL string.
   */
  initSchema(schema: string): void;
  /**
   * List all tables in the database.
   */
  listTables(): any;
  /**
   * Create an incremental query subscription (delta-based).
   */
  subscribeDelta(sql: string, callback: Function): SyncedQueryHandle;
  /**
   * Create an incremental query subscription that returns full row objects.
   *
   * The callback receives an Array of objects with column names as keys.
   * This maintains an internal row map and provides the complete result set on each change.
   */
  subscribeRows(sql: string, callback: Function): SyncedQueryHandle;
  /**
   * Get current sync state.
   */
  readonly syncState: SyncState;
}

/**
 * Create a ReadableStream that reads from a blob.
 * This is a convenience wrapper for JS interop.
 */
export function create_blob_readable_stream(db: WasmDatabase, handle_id: bigint): any;

/**
 * Initialize panic hook for better error messages.
 */
export function init(): void;

/**
 * Convert a Base32 string ObjectId to 16-byte binary.
 *
 * Returns a Uint8Array containing the u128 little-endian bytes.
 */
export function object_id_from_string(s: string): Uint8Array;

/**
 * Convert a 16-byte binary ObjectId to a Base32 string.
 *
 * This is useful for displaying ObjectIds or using them as string keys.
 * The binary format is u128 little-endian.
 */
export function object_id_to_string(bytes: Uint8Array): string;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_wasmdatabase_free: (a: number, b: number) => void;
  readonly wasmdatabase_new: () => number;
  readonly wasmdatabase_withIndexedDb: (a: number, b: number) => any;
  readonly wasmdatabase_hasPersistedDatabase: (a: number, b: number) => any;
  readonly wasmdatabase_deletePersistedDatabase: (a: number, b: number) => any;
  readonly wasmdatabase_execute: (a: number, b: number, c: number) => [number, number, number];
  readonly wasmdatabase_select_binary: (a: number, b: number, c: number) => [number, number, number];
  readonly wasmdatabase_updateRow: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => [number, number, number];
  readonly wasmdatabase_updateRowI64: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: bigint) => [number, number, number];
  readonly wasmdatabase_init_schema: (a: number, b: number, c: number) => [number, number];
  readonly wasmdatabase_list_tables: (a: number) => any;
  readonly wasmdatabase_subscribe: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly wasmdatabase_subscribe_binary: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly wasmdatabase_subscribeDelta: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly wasmdatabase_create_blob: (a: number, b: number, c: number) => bigint;
  readonly wasmdatabase_create_blob_writer: (a: number) => number;
  readonly wasmdatabase_read_blob: (a: number, b: bigint) => [number, number, number];
  readonly wasmdatabase_get_blob_info: (a: number, b: bigint) => [number, number, number];
  readonly wasmdatabase_read_blob_chunk: (a: number, b: bigint, c: number) => [number, number, number];
  readonly wasmdatabase_release_blob: (a: number, b: bigint) => void;
  readonly wasmdatabase_insert_with_blobs: (a: number, b: number, c: number, d: any, e: any) => [number, number, number, number];
  readonly wasmdatabase_update_row_blob: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: bigint) => [number, number, number];
  readonly wasmdatabase_getTableSchema: (a: number, b: number, c: number) => [number, number, number];
  readonly wasmdatabase_getDescriptorId: (a: number, b: number, c: number) => [number, number, number, number];
  readonly __wbg_wasmqueryhandle_free: (a: number, b: number) => void;
  readonly wasmqueryhandle_unsubscribe: (a: number) => void;
  readonly __wbg_wasmqueryhandlebinary_free: (a: number, b: number) => void;
  readonly wasmqueryhandlebinary_unsubscribe: (a: number) => void;
  readonly __wbg_wasmqueryhandledelta_free: (a: number, b: number) => void;
  readonly wasmqueryhandledelta_unsubscribe: (a: number) => void;
  readonly wasmqueryhandledelta_diagram: (a: number) => [number, number];
  readonly __wbg_wasmblobwriter_free: (a: number, b: number) => void;
  readonly wasmblobwriter_write: (a: number, b: number, c: number) => [number, number];
  readonly wasmblobwriter_size: (a: number) => [number, number, number];
  readonly wasmblobwriter_finish: (a: number) => [bigint, number, number];
  readonly wasmblobwriter_abort: (a: number) => void;
  readonly create_blob_readable_stream: (a: number, b: bigint) => [number, number, number];
  readonly init: () => void;
  readonly object_id_to_string: (a: number, b: number) => [number, number, number, number];
  readonly object_id_from_string: (a: number, b: number) => [number, number, number];
  readonly __wbg_wasmsyncclient_free: (a: number, b: number) => void;
  readonly wasmsyncclient_new: (a: number, b: number, c: number, d: number) => number;
  readonly wasmsyncclient_setOnCommits: (a: number, b: any) => void;
  readonly wasmsyncclient_setOnExcluded: (a: number, b: any) => void;
  readonly wasmsyncclient_setOnError: (a: number, b: any) => void;
  readonly wasmsyncclient_setOnStateChange: (a: number, b: any) => void;
  readonly wasmsyncclient_connectionState: (a: number) => number;
  readonly wasmsyncclient_subscribe: (a: number, b: number, c: number) => any;
  readonly wasmsyncclient_push: (a: number, b: number, c: number, d: any) => any;
  readonly wasmsyncclient_reconcile: (a: number, b: number, c: number, d: any) => any;
  readonly wasmsyncclient_disconnect: (a: number) => void;
  readonly __wbg_wasmsyncedlocalnode_free: (a: number, b: number) => void;
  readonly wasmsyncedlocalnode_new: (a: number, b: number, c: number, d: number, e: number, f: number) => number;
  readonly wasmsyncedlocalnode_withIndexedDb: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
  readonly wasmsyncedlocalnode_setOnStateChange: (a: number, b: any) => void;
  readonly wasmsyncedlocalnode_setOnError: (a: number, b: any) => void;
  readonly wasmsyncedlocalnode_syncState: (a: number) => number;
  readonly wasmsyncedlocalnode_connect: (a: number, b: number, c: number) => any;
  readonly wasmsyncedlocalnode_execute: (a: number, b: number, c: number) => [number, number, number];
  readonly wasmsyncedlocalnode_provisionViewer: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
  readonly wasmsyncedlocalnode_selectBinary: (a: number, b: number, c: number) => [number, number, number];
  readonly wasmsyncedlocalnode_initSchema: (a: number, b: number, c: number) => [number, number];
  readonly wasmsyncedlocalnode_listTables: (a: number) => any;
  readonly wasmsyncedlocalnode_subscribeDelta: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly wasmsyncedlocalnode_subscribeRows: (a: number, b: number, c: number, d: any) => [number, number, number];
  readonly __wbg_syncedqueryhandle_free: (a: number, b: number) => void;
  readonly syncedqueryhandle_unsubscribe: (a: number) => void;
  readonly syncedqueryhandle_diagram: (a: number) => [number, number];
  readonly syncedqueryhandle_free: (a: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__h2fda84b10f91d04a: (a: number, b: number, c: any) => void;
  readonly wasm_bindgen__closure__destroy__h145730200f177735: (a: number, b: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__h76dfe62a3b69c085: (a: number, b: number, c: any) => void;
  readonly wasm_bindgen__closure__destroy__h3d685ebc6ca20542: (a: number, b: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__ha88fd06ec3374ffb: (a: number, b: number) => void;
  readonly wasm_bindgen__closure__destroy__hd30a2cb8baf489cb: (a: number, b: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__h3097c68d921a6b39: (a: number, b: number, c: any, d: any) => void;
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
