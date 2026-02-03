/**
 * Storage driver types for Jazz WASM runtime.
 *
 * Types are generated from Rust via tsify and re-exported here with
 * friendlier names (without the "Wasm" prefix). This ensures compile-time
 * type safety across the Rust/TypeScript boundary.
 */

// Re-export generated types from WASM with friendlier names
export type {
  WasmStorageRequest as StorageRequest,
  WasmStorageResponse as StorageResponse,
  WasmCommit as Commit,
  WasmLoadedBranch as LoadedBranch,
  WasmBlobAssociation as BlobAssociation,
  WasmValue as Value,
  WasmRow,
  WasmRowDelta as RowDelta,
  WasmColumnType as ColumnType,
  WasmColumnDescriptor as ColumnDescriptor,
  WasmTableSchema as TableSchema,
  WasmSchema,
} from "groove-wasm";

// ============================================================================
// Storage Driver Interface (not generated, defined here)
// ============================================================================

// Import for use in interface definition
import type { WasmStorageRequest, WasmStorageResponse } from "groove-wasm";

/**
 * Interface for storage backend implementations.
 *
 * The driver processes batches of storage requests and returns responses
 * in the same order. Requests within a batch can be processed in parallel
 * or sequentially depending on the implementation.
 */
export interface StorageDriver {
  /**
   * Process a batch of storage requests.
   *
   * @param requests Array of storage requests to process
   * @returns Array of responses in the same order as requests
   */
  process(requests: WasmStorageRequest[]): Promise<WasmStorageResponse[]>;

  /**
   * Close the driver and release resources.
   * Optional - not all drivers need cleanup.
   */
  close?(): Promise<void>;
}
