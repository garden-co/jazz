/**
 * Shared TS value and FFI boundary types used by the Jazz runtimes.
 *
 * `Value` is the logical runtime-facing value shape used throughout the TS client.
 * `FFIValue` names that same shape when values are crossing into a specific runtime
 * adapter or native binding. These are naming aliases only; runtime adapters can
 * translate them at transport boundaries without forcing client-side copies.
 */

export type Value =
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: bigint | number }
  | { type: "Double"; value: number }
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Timestamp"; value: number }
  | { type: "Uuid"; value: string }
  | { type: "Bytea"; value: Uint8Array }
  | { type: "Array"; value: Value[] }
  | {
      type: "Row";
      value: { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> };
    }
  | { type: "Enum"; value: { case: string; values: Value[] } }
  | { type: "Null" };

export type InsertValues = Record<string, Value>;
export type FFIValue = Value;
export type FFIRecord = InsertValues;

export interface WasmRow {
  id: string;
  values: Value[];
  valuesByColumn?: Map<string, Value>;
}

export type FFIRow = WasmRow;

export type NativeTerminalPathSegment = { Collection: string } | { Key: number[] };
export type NativeTerminalEdit =
  | { Insert: { index: number; key: number[]; value: number[] } }
  | { Update: { key: number[]; value: number[] } }
  | { Remove: { key: number[] } }
  | { Move: { key: number[]; index: number } };
/** Immutable producer-owned root descriptor contract retained for codec compatibility. */
export interface NativeTerminalRootLayout {
  id: string;
  rootDescriptor: number[];
  rootKeySlot: number;
  rootKeyFieldName: string;
  publicFields: Array<{
    name: string;
    descriptorFieldName: string;
    slot: number;
    carrier?: "CurrentRow" | "Logical";
  }>;
  carrier: "CurrentRow" | "Logical";
}
export interface NativeTerminalOperation {
  rootLayoutId?: string;
  rootDescriptor?: number[];
  root_key: number[];
  path: NativeTerminalPathSegment[];
  edit: NativeTerminalEdit;
}

/** Logical terminal-tree path consumed by the TypeScript materializer. */
export type RuntimeTerminalPathSegment = { Collection: number } | { Key: number[] };
export type RuntimeTerminalEdit =
  | { Insert: { index: number; key: number[]; row: WasmRow } }
  | { Update: { key: number[]; row: WasmRow } }
  | { Remove: { key: number[] } }
  | { Move: { key: number[]; index: number } };
export interface RuntimeTerminalOperation {
  root_key: number[];
  path: RuntimeTerminalPathSegment[];
  edit: RuntimeTerminalEdit;
}

export interface RuntimeSubscriptionAddedRow {
  sourceId: string;
  occurrenceKey: Uint8Array;
  index: number;
  row: WasmRow;
}

export interface RuntimeSubscriptionUpdatedRow {
  sourceId: string;
  occurrenceKey: Uint8Array;
  index: number;
  row?: WasmRow;
}

export interface RuntimeSubscriptionRemovedRow {
  sourceId: string;
  occurrenceKey: Uint8Array;
  index: number;
}

/** Structured root changes published by a runtime adapter to the TS facade. */
export interface RuntimeSubscriptionDelta {
  reset?: boolean;
  added: RuntimeSubscriptionAddedRow[];
  removed: RuntimeSubscriptionRemovedRow[];
  updated: RuntimeSubscriptionUpdatedRow[];
  terminalOperations?: RuntimeTerminalOperation[];
}

export type ColumnType =
  | { type: "Integer" }
  | { type: "BigInt" }
  | { type: "Double" }
  | { type: "Boolean" }
  | { type: "Text" }
  | { type: "Json"; schema?: Record<string, unknown> }
  | { type: "Enum"; variants: string[] }
  | { type: "EnumPayload"; cases: Array<{ name: string; fields: ColumnDescriptor[] }> }
  | { type: "Timestamp" }
  | { type: "Uuid" }
  | { type: "Bytea" }
  | { type: "Array"; element: ColumnType }
  | { type: "Row"; columns: ColumnDescriptor[] };

export type ColumnMergeStrategy = "Counter" | "GSet";

export interface ColumnDescriptor {
  name: string;
  column_type: ColumnType;
  nullable: boolean;
  /** Physical current-row carriers may omit this wildcard field. */
  sparse?: boolean;
  default?: Value;
  references?: string;
  merge_strategy?: ColumnMergeStrategy;
}

export type PolicyOperation = "Select" | "Insert" | "Update" | "Delete";
export type PolicyCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

export type PolicyValue =
  | { type: "Literal"; value: Value }
  | { type: "SessionRef"; path: string[] };

export type PolicyLiteralValue = Value;

export type PolicyExpr =
  | { type: "Cmp"; column: string; op: PolicyCmpOp; value: PolicyValue }
  | { type: "SessionCmp"; path: string[]; op: PolicyCmpOp; value: PolicyLiteralValue }
  | { type: "IsNull"; column: string }
  | { type: "SessionIsNull"; path: string[] }
  | { type: "IsNotNull"; column: string }
  | { type: "SessionIsNotNull"; path: string[] }
  | { type: "Contains"; column: string; value: PolicyValue }
  | { type: "SessionContains"; path: string[]; value: PolicyLiteralValue }
  | { type: "In"; column: string; session_path: string[] }
  | { type: "InList"; column: string; values: PolicyValue[] }
  | { type: "SessionInList"; path: string[]; values: PolicyLiteralValue[] }
  | { type: "Exists"; table: string; condition: PolicyExpr }
  | { type: "ExistsRel"; rel: unknown }
  | { type: "Inherits"; operation: PolicyOperation; via_column: string; max_depth?: number }
  | {
      type: "InheritsReferencing";
      operation: PolicyOperation;
      source_table: string;
      via_column: string;
      max_depth?: number;
    }
  | { type: "And"; exprs: PolicyExpr[] }
  | { type: "Or"; exprs: PolicyExpr[] }
  | { type: "Not"; expr: PolicyExpr }
  | { type: "True" }
  | { type: "False" };

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

export interface TableSchema {
  columns: ColumnDescriptor[];
  indexed_columns?: string[];
  policies?: TablePolicies;
  /** Ordinary immutable columns that form this table's branch key. */
  branchBy?: string[];
}

export type Schema = Record<string, TableSchema>;

export type WasmSchema = Schema;

// ============================================================================
// Storage Driver Interface
// ============================================================================

/**
 * Interface for storage backend implementations.
 *
 * - `persistent`: local persistence enabled (IndexedDB in browser, Fjall in backend)
 * - `memory`: non-persistent in-memory runtime only
 */
export type StorageDriver =
  | {
      type: "persistent";
      /** Browser IndexedDB namespace when persistence is enabled (default: appId). */
      dbName?: string;
    }
  | {
      type: "memory";
    };
