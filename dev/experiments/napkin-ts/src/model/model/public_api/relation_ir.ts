import type { TableName } from "./types/schema.js";
import type { Value } from "./types/value.js";

export type ColumnRef = { scope?: string; column: string };
export const unscopedColumn = (column: string): ColumnRef => ({ column });
export const toColumnRef = (c: string | ColumnRef): ColumnRef =>
  typeof c === "string" ? { column: c } : c;

export type RowIdRef = "Current" | "Outer" | "Frontier";

export type ValueRef =
  | { kind: "Literal"; value: Value }
  | { kind: "SessionRef"; path: string[] }
  | { kind: "OuterColumn"; column: ColumnRef }
  | { kind: "RowId"; ref: RowIdRef };

export type PredicateCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

export type PredicateExpr =
  | { kind: "Cmp"; left: ColumnRef; op: PredicateCmpOp; right: ValueRef }
  | { kind: "IsNull"; column: ColumnRef }
  | { kind: "IsNotNull"; column: ColumnRef }
  | { kind: "In"; left: ColumnRef; values: ValueRef[] }
  | { kind: "Contains"; left: ColumnRef; right: ValueRef }
  | { kind: "EnumMatch"; column: ColumnRef; case: string; payload: PredicateExpr }
  | { kind: "And"; exprs: PredicateExpr[] }
  | { kind: "Or"; exprs: PredicateExpr[] }
  | { kind: "Not"; expr: PredicateExpr }
  | { kind: "True" }
  | { kind: "False" };

export type KeyRef = { kind: "Column"; column: ColumnRef } | { kind: "RowId"; ref: RowIdRef };
export type ProjectExpr = KeyRef;
export type ProjectColumn = { alias: string; expr: ProjectExpr };
export type JoinCondition = { left: ColumnRef; right: ColumnRef };
export type JoinKind = "Inner" | "Left";
export type RecursionBound = { kind: "Fixpoint" } | { kind: "MaxDepth"; depth: number };

export type RelExpr =
  | { kind: "TableScan"; table: TableName; alias?: string }
  | { kind: "Filter"; input: RelExpr; predicate: PredicateExpr }
  | { kind: "Union"; inputs: UnionArm[] }
  | { kind: "Join"; left: RelExpr; right: RelExpr; on: JoinCondition[]; join_kind: JoinKind }
  | { kind: "Project"; input: RelExpr; columns: ProjectColumn[] }
  | {
      kind: "Gather";
      seed: RelExpr;
      step: RelExpr;
      frontier_key: KeyRef;
      bound: RecursionBound;
      dedupe_key: KeyRef[];
    };

export type UnionArm = { label: string; input: RelExpr };
