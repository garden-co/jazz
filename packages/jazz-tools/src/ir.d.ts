export type RelColumnRef = {
  scope?: string;
  column: string;
};
export type RelRowIdRef = "Current" | "Outer" | "Frontier";
export type RelValueRef =
  | {
      type: "Literal";
      value: unknown;
    }
  | {
      type: "SessionRef";
      path: string[];
    }
  | {
      type: "OuterColumn";
      column: RelColumnRef;
    }
  | {
      type: "FrontierColumn";
      column: RelColumnRef;
    }
  | {
      type: "RowId";
      source: RelRowIdRef;
    };
export type RelPredicateCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";
export type RelPredicateExpr =
  | {
      type: "Cmp";
      left: RelColumnRef;
      op: RelPredicateCmpOp;
      right: RelValueRef;
    }
  | {
      type: "IsNull";
      column: RelColumnRef;
    }
  | {
      type: "IsNotNull";
      column: RelColumnRef;
    }
  | {
      type: "In";
      left: RelColumnRef;
      values: RelValueRef[];
    }
  | {
      type: "Contains";
      left: RelColumnRef;
      value: RelValueRef;
    }
  | {
      type: "And";
      exprs: RelPredicateExpr[];
    }
  | {
      type: "Or";
      exprs: RelPredicateExpr[];
    }
  | {
      type: "Not";
      expr: RelPredicateExpr;
    }
  | {
      type: "True";
    }
  | {
      type: "False";
    };
export type RelJoinKind = "Inner" | "Left";
export type RelJoinCondition = {
  left: RelColumnRef;
  right: RelColumnRef;
};
export type RelKeyRef =
  | {
      type: "Column";
      column: RelColumnRef;
    }
  | {
      type: "RowId";
      source: RelRowIdRef;
    };
export type RelProjectExpr =
  | {
      type: "Column";
      column: RelColumnRef;
    }
  | {
      type: "RowId";
      source: RelRowIdRef;
    };
export type RelProjectColumn = {
  alias: string;
  expr: RelProjectExpr;
};
export type RelOrderDirection = "Asc" | "Desc";
export type RelOrderByExpr = {
  column: RelColumnRef;
  direction: RelOrderDirection;
};
export type RelExpr =
  | {
      type: "TableScan";
      table: string;
    }
  | {
      type: "Filter";
      input: RelExpr;
      predicate: RelPredicateExpr;
    }
  | {
      type: "Join";
      left: RelExpr;
      right: RelExpr;
      on: RelJoinCondition[];
      joinKind: RelJoinKind;
    }
  | {
      type: "Project";
      input: RelExpr;
      columns: RelProjectColumn[];
    }
  | {
      type: "Gather";
      seed: RelExpr;
      step: RelExpr;
      frontierKey: RelKeyRef;
      maxDepth: number;
      dedupeKey: RelKeyRef[];
    }
  | {
      type: "Distinct";
      input: RelExpr;
      key: RelKeyRef[];
    }
  | {
      type: "OrderBy";
      input: RelExpr;
      terms: RelOrderByExpr[];
    }
  | {
      type: "Offset";
      input: RelExpr;
      offset: number;
    }
  | {
      type: "Limit";
      input: RelExpr;
      limit: number;
    };
export type PolicyOperationV2 = "Select" | "Insert" | "Update" | "Delete";
export type PolicyExprV2 =
  | {
      type: "Predicate";
      predicate: RelPredicateExpr;
    }
  | {
      type: "ExistsRel";
      rel: RelExpr;
    }
  | {
      type: "Inherits";
      operation: PolicyOperationV2;
      viaColumn: string;
      maxDepth?: number;
    }
  | {
      type: "And";
      exprs: PolicyExprV2[];
    }
  | {
      type: "Or";
      exprs: PolicyExprV2[];
    }
  | {
      type: "Not";
      expr: PolicyExprV2;
    }
  | {
      type: "True";
    }
  | {
      type: "False";
    };
//# sourceMappingURL=ir.d.ts.map
