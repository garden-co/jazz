/**
 * Types for Jazz WASM runtime.
 *
 * Types are generated from Rust via tsify and re-exported here with
 * friendlier names (without the "Wasm" prefix). This ensures compile-time
 * type safety across the Rust/TypeScript boundary.
 */

import type {
  WasmColumnDescriptor as JazzWasmColumnDescriptor,
  WasmColumnType as JazzWasmColumnType,
  WasmCmpOp as JazzWasmCmpOp,
  WasmOperationPolicy as JazzWasmOperationPolicy,
  WasmPolicyExpr as JazzWasmPolicyExpr,
  WasmPolicyOperation as JazzWasmPolicyOperation,
  WasmPolicyValue as JazzWasmPolicyValue,
  WasmRow as JazzWasmRow,
  WasmTableSchema as JazzWasmTableSchema,
  WasmTablePolicies as JazzWasmTablePolicies,
  WasmValue as JazzWasmValue,
} from "jazz-wasm";

export type Value = JazzWasmValue;
export type WasmRow = JazzWasmRow;
export type RowAdded = 0;
export type RowRemoved = 1;
export type RowUpdated = 2;
export type RowChangeKind = RowAdded | RowRemoved | RowUpdated;

export interface WireRowDeltaAdded {
  kind: RowAdded;
  id: string;
  index: number;
  row: JazzWasmRow;
}

export interface WireRowDeltaRemoved {
  kind: RowRemoved;
  id: string;
  index: number;
}

export interface WireRowDeltaUpdated {
  kind: RowUpdated;
  id: string;
  index: number;
  row?: JazzWasmRow | null;
}

export type WireRowChange = WireRowDeltaAdded | WireRowDeltaRemoved | WireRowDeltaUpdated;

export type RowDelta = WireRowChange[];
export type ColumnType = JazzWasmColumnType;
export type ColumnDescriptor = JazzWasmColumnDescriptor;

export type PolicyOperation = JazzWasmPolicyOperation;
export type PolicyCmpOp = JazzWasmCmpOp;
export type PolicyValue = JazzWasmPolicyValue;
export type PolicyExpr = JazzWasmPolicyExpr;
export type OperationPolicy = JazzWasmOperationPolicy;
export type TablePolicies = JazzWasmTablePolicies;

export interface TableSchema extends JazzWasmTableSchema {
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
