/**
 * Types for Jazz WASM runtime.
 *
 * Types are generated from Rust via tsify and re-exported here with
 * friendlier names (without the "Wasm" prefix). This ensures compile-time
 * type safety across the Rust/TypeScript boundary.
 */

import type {
  WasmColumnDescriptor as GrooveWasmColumnDescriptor,
  WasmColumnType as GrooveWasmColumnType,
  WasmRow as GrooveWasmRow,
  WasmRowDelta as GrooveWasmRowDelta,
  WasmTableSchema as GrooveWasmTableSchema,
  WasmValue as GrooveWasmValue,
} from "jazz-wasm";

export type Value = GrooveWasmValue;
export type WasmRow = GrooveWasmRow;
export type RowDelta = GrooveWasmRowDelta;
export type ColumnType = GrooveWasmColumnType;
export type ColumnDescriptor = GrooveWasmColumnDescriptor;

export type PolicyOperation = "Select" | "Insert" | "Update" | "Delete";
export type PolicyCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

export type PolicyValue =
  | {
      type: "Literal";
      value: Value;
    }
  | {
      type: "SessionRef";
      path: string[];
    };

export type PolicyExpr =
  | {
      type: "Cmp";
      column: string;
      op: PolicyCmpOp;
      value: PolicyValue;
    }
  | {
      type: "IsNull";
      column: string;
    }
  | {
      type: "IsNotNull";
      column: string;
    }
  | {
      type: "In";
      column: string;
      session_path: string[];
    }
  | {
      type: "Exists";
      table: string;
      condition: PolicyExpr;
    }
  | {
      type: "Inherits";
      operation: PolicyOperation;
      via_column: string;
    }
  | {
      type: "And";
      exprs: PolicyExpr[];
    }
  | {
      type: "Or";
      exprs: PolicyExpr[];
    }
  | {
      type: "Not";
      expr: PolicyExpr;
    }
  | {
      type: "True";
    }
  | {
      type: "False";
    };

export interface OperationPolicy {
  using?: PolicyExpr;
  with_check?: PolicyExpr;
}

export interface TablePolicies {
  select?: OperationPolicy;
  insert?: OperationPolicy;
  update?: OperationPolicy;
  delete?: OperationPolicy;
}

export interface TableSchema extends GrooveWasmTableSchema {
  policies?: TablePolicies;
}

export interface WasmSchema {
  tables: Record<string, TableSchema>;
}

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
