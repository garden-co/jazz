/**
 * Storage driver interface for Jazz WASM runtime.
 *
 * Implementations handle persistent storage of objects, commits, blobs, and indexes.
 * The runtime calls process() with batches of requests and expects responses in order.
 */

// ============================================================================
// Value Types (matching groove::query_manager::types::Value)
// ============================================================================

export type Value =
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: bigint | string }
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Timestamp"; value: number }
  | { type: "Uuid"; value: string }
  | { type: "Array"; value: Value[] }
  | { type: "Row"; value: Value[] }
  | { type: "Null" };

// ============================================================================
// Commit Types
// ============================================================================

export interface Commit {
  parents: string[]; // CommitIds as hex strings
  content: Uint8Array;
  timestamp: number;
  author: string; // ObjectId as UUID string
  metadata?: Record<string, string>;
}

export interface LoadedBranch {
  tips: string[];
  tails?: string[];
  commits: Record<string, Commit>;
}

export interface BlobAssociation {
  object_id: string;
  branch_name: string;
  commit_id: string;
}

// ============================================================================
// Storage Requests
// ============================================================================

export type StorageRequest =
  | { type: "CreateObject"; id: string; metadata: Record<string, string> }
  | {
      type: "AppendCommit";
      object_id: string;
      branch_name: string;
      commit: Commit;
    }
  | {
      type: "LoadObjectBranch";
      object_id: string;
      branch_name: string;
      depth: "TipIdsOnly" | "TipsOnly" | "AllCommits";
    }
  | { type: "StoreBlob"; content_hash: string; data: Uint8Array }
  | { type: "LoadBlob"; content_hash: string }
  | {
      type: "AssociateBlob";
      content_hash: string;
      object_id: string;
      branch_name: string;
      commit_id: string;
    }
  | { type: "LoadBlobAssociations"; content_hash: string }
  | {
      type: "DeleteCommit";
      object_id: string;
      branch_name: string;
      commit_id: string;
    }
  | {
      type: "DissociateAndMaybeDeleteBlob";
      content_hash: string;
      object_id: string;
      branch_name: string;
      commit_id: string;
    }
  | {
      type: "SetBranchTails";
      object_id: string;
      branch_name: string;
      tails?: string[];
    }
  | { type: "LoadIndexPage"; table: string; column: string; page_id: number }
  | {
      type: "StoreIndexPage";
      table: string;
      column: string;
      page_id: number;
      data: Uint8Array;
    }
  | { type: "DeleteIndexPage"; table: string; column: string; page_id: number }
  | { type: "LoadIndexMeta"; table: string; column: string }
  | {
      type: "StoreIndexMeta";
      table: string;
      column: string;
      data: Uint8Array;
    };

// ============================================================================
// Storage Responses
// ============================================================================

export type StorageResponse =
  | { type: "CreateObject"; id: string; success: boolean; error?: string }
  | {
      type: "AppendCommit";
      object_id: string;
      commit_id: string;
      success: boolean;
      error?: string;
    }
  | {
      type: "LoadObjectBranch";
      object_id: string;
      branch_name: string;
      branch?: LoadedBranch;
      error?: string;
    }
  | {
      type: "StoreBlob";
      content_hash: string;
      success: boolean;
      error?: string;
    }
  | {
      type: "LoadBlob";
      content_hash: string;
      data?: Uint8Array;
      error?: string;
    }
  | {
      type: "AssociateBlob";
      content_hash: string;
      success: boolean;
      error?: string;
    }
  | {
      type: "LoadBlobAssociations";
      content_hash: string;
      associations?: BlobAssociation[];
      error?: string;
    }
  | {
      type: "DeleteCommit";
      object_id: string;
      branch_name: string;
      commit_id: string;
      success: boolean;
      error?: string;
    }
  | {
      type: "DissociateAndMaybeDeleteBlob";
      content_hash: string;
      object_id: string;
      branch_name: string;
      commit_id: string;
      blob_deleted?: boolean;
      error?: string;
    }
  | {
      type: "SetBranchTails";
      object_id: string;
      branch_name: string;
      success: boolean;
      error?: string;
    }
  | {
      type: "LoadIndexPage";
      table: string;
      column: string;
      page_id: number;
      data?: Uint8Array;
      error?: string;
    }
  | {
      type: "StoreIndexPage";
      table: string;
      column: string;
      page_id: number;
      success: boolean;
      error?: string;
    }
  | {
      type: "DeleteIndexPage";
      table: string;
      column: string;
      page_id: number;
      success: boolean;
      error?: string;
    }
  | {
      type: "LoadIndexMeta";
      table: string;
      column: string;
      data?: Uint8Array;
      error?: string;
    }
  | {
      type: "StoreIndexMeta";
      table: string;
      column: string;
      success: boolean;
      error?: string;
    };

// ============================================================================
// Storage Driver Interface
// ============================================================================

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
  process(requests: StorageRequest[]): Promise<StorageResponse[]>;

  /**
   * Close the driver and release resources.
   * Optional - not all drivers need cleanup.
   */
  close?(): Promise<void>;
}

// ============================================================================
// Schema Types (matching WasmSchema)
// ============================================================================

export type ColumnType =
  | { type: "Integer" }
  | { type: "BigInt" }
  | { type: "Boolean" }
  | { type: "Text" }
  | { type: "Timestamp" }
  | { type: "Uuid" }
  | { type: "Array"; element: ColumnType }
  | { type: "Row"; columns: ColumnDescriptor[] };

export interface ColumnDescriptor {
  name: string;
  column_type: ColumnType;
  nullable: boolean;
  references?: string;
}

export interface TableSchema {
  columns: ColumnDescriptor[];
}

export interface Schema {
  tables: Record<string, TableSchema>;
}

// ============================================================================
// Row Delta Types (for subscriptions)
// ============================================================================

export interface WasmRow {
  id: string;
  values: Value[];
}

export interface RowDelta {
  added: WasmRow[];
  removed: WasmRow[];
  updated: [WasmRow, WasmRow][];
  pending: boolean;
}
