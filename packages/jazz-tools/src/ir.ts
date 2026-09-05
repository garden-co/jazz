export type RelColumnRef = {
  scope?: string;
  column: string;
};

export type RelRowIdRef = "Current" | "Outer" | "Frontier";

export type RelValueRef =
  | { Literal: unknown }
  | { Param: string }
  | { SessionRef: string[] }
  | { OuterColumn: RelColumnRef }
  | { FrontierColumn: RelColumnRef }
  | { RowId: RelRowIdRef };

export type RelPredicateCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

export type RelPredicateExpr =
  | {
      Cmp: {
        left: RelColumnRef;
        op: RelPredicateCmpOp;
        right: RelValueRef;
      };
    }
  | { IsNull: { column: RelColumnRef } }
  | { IsNotNull: { column: RelColumnRef } }
  | { In: { left: RelColumnRef; values: RelValueRef[] } }
  | { Contains: { left: RelColumnRef; right: RelValueRef } }
  | { EnumMatch: { column: RelColumnRef; case: string; payload: RelPredicateExpr } }
  | { And: RelPredicateExpr[] }
  | { Or: RelPredicateExpr[] }
  | { Not: RelPredicateExpr }
  | "True"
  | "False";

export type RelJoinKind = "Inner" | "Left";

export type RelJoinCondition = {
  left: RelColumnRef;
  right: RelColumnRef;
};

export type RelKeyRef = { Column: RelColumnRef } | { RowId: RelRowIdRef };

export type RelProjectExpr = { Column: RelColumnRef } | { RowId: RelRowIdRef };

export type RelProjectColumn = {
  alias: string;
  expr: RelProjectExpr;
};

export type RelOrderDirection = "Asc" | "Desc";

export type RelOrderByExpr = {
  column: RelColumnRef;
  direction: RelOrderDirection;
};

/**
 * A semantic recursion bound. `MaxDepth: 0` includes the seed and no recursive hop.
 */
export type RelRecursionBound = "Fixpoint" | { MaxDepth: number };

export type RelExpr =
  | { TableScan: { table: string; alias?: string } }
  | { Filter: { input: RelExpr; predicate: RelPredicateExpr } }
  | { Union: { inputs: RelExpr[] } }
  | { Join: { left: RelExpr; right: RelExpr; on: RelJoinCondition[]; join_kind: RelJoinKind } }
  | { Project: { input: RelExpr; columns: RelProjectColumn[] } }
  | {
      Gather: {
        seed: RelExpr;
        step: RelExpr;
        frontier_key: RelKeyRef;
        bound: RelRecursionBound;
        dedupe_key: RelKeyRef[];
      };
    }
  | { Distinct: { input: RelExpr; key: RelKeyRef[] } }
  | { OrderBy: { input: RelExpr; terms: RelOrderByExpr[] } }
  | { Offset: { input: RelExpr; offset: number } }
  | { Limit: { input: RelExpr; limit: number } };

export type PolicyOperation = "Select" | "Insert" | "Update" | "Delete";

export type PolicyExpr =
  | { Predicate: RelPredicateExpr }
  | { ExistsRel: { rel: RelExpr } }
  | {
      Inherits: {
        operation: PolicyOperation;
        via_column: string;
        max_depth?: number;
      };
    }
  | { And: PolicyExpr[] }
  | { Or: PolicyExpr[] }
  | { Not: PolicyExpr }
  | "True"
  | "False";
