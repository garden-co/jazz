/**
 * Types for Jazz WASM runtime.
 *
 * Types are generated from Rust via tsify and re-exported here with
 * friendlier names (without the "Wasm" prefix). This ensures compile-time
 * type safety across the Rust/TypeScript boundary.
 */

// Re-export generated types from WASM with friendlier names
export type {
  WasmValue as Value,
  WasmRow,
  WasmRowDelta as RowDelta,
  WasmColumnType as ColumnType,
  WasmColumnDescriptor as ColumnDescriptor,
  WasmTableSchema as TableSchema,
  WasmSchema,
} from "jazz-wasm";

// ============================================================================
// Storage Driver Interface
// ============================================================================

/**
 * Interface for storage backend implementations.
 *
 * With synchronous in-memory storage (MemoryIoHandler), the driver
 * interface is minimal — just an optional close hook.
 */
export interface StorageDriver {
  /**
   * Close the driver and release resources.
   * Optional - not all drivers need cleanup.
   */
  close?(): Promise<void>;
}
