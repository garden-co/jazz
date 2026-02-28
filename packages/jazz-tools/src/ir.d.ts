export type RelColumnRef = {
  scope?: string;
  column: string;
};
export type RelRowIdRef = "Current" | "Outer" | "Frontier";
export type RelValueRef =
  | {
      Literal: unknown;
    }
  | {
      SessionRef: string[];
    }
  | {
      OuterColumn: RelColumnRef;
    }
  | {
      FrontierColumn: RelColumnRef;
    }
  | {
      RowId: RelRowIdRef;
    };
export type RelPredicateCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";
export type RelPredicateExpr =
  | {
      Cmp: {
        left: RelColumnRef;
        op: RelPredicateCmpOp;
        right: RelValueRef;
      };
    }
  | {
      IsNull: {
        column: RelColumnRef;
      };
    }
  | {
      IsNotNull: {
        column: RelColumnRef;
      };
    }
  | {
      In: {
        left: RelColumnRef;
        values: RelValueRef[];
      };
    }
  | {
      Contains: {
        left: RelColumnRef;
        right: RelValueRef;
      };
    }
  | {
      And: RelPredicateExpr[];
    }
  | {
      Or: RelPredicateExpr[];
    }
  | {
      Not: RelPredicateExpr;
    }
  | "True"
  | "False";
export type RelJoinKind = "Inner" | "Left";
export type RelJoinCondition = {
  left: RelColumnRef;
  right: RelColumnRef;
};
export type RelKeyRef =
  | {
      Column: RelColumnRef;
    }
  | {
      RowId: RelRowIdRef;
    };
export type RelProjectExpr =
  | {
      Column: RelColumnRef;
    }
  | {
      RowId: RelRowIdRef;
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
      TableScan: {
        table: string;
      };
    }
  | {
      Filter: {
        input: RelExpr;
        predicate: RelPredicateExpr;
      };
    }
  | {
      Join: {
        left: RelExpr;
        right: RelExpr;
        on: RelJoinCondition[];
        join_kind: RelJoinKind;
      };
    }
  | {
      Project: {
        input: RelExpr;
        columns: RelProjectColumn[];
      };
    }
  | {
      Gather: {
        seed: RelExpr;
        step: RelExpr;
        frontier_key: RelKeyRef;
        max_depth: number;
        dedupe_key: RelKeyRef[];
      };
    }
  | {
      Distinct: {
        input: RelExpr;
        key: RelKeyRef[];
      };
    }
  | {
      OrderBy: {
        input: RelExpr;
        terms: RelOrderByExpr[];
      };
    }
  | {
      Offset: {
        input: RelExpr;
        offset: number;
      };
    }
  | {
      Limit: {
        input: RelExpr;
        limit: number;
      };
    };
export type PolicyOperationV2 = "Select" | "Insert" | "Update" | "Delete";
export type PolicyExprV2 =
  | {
      Predicate: RelPredicateExpr;
    }
  | {
      ExistsRel: {
        rel: RelExpr;
      };
    }
  | {
      Inherits: {
        operation: PolicyOperationV2;
        via_column: string;
        max_depth?: number;
      };
    }
  | {
      And: PolicyExprV2[];
    }
  | {
      Or: PolicyExprV2[];
    }
  | {
      Not: PolicyExprV2;
    }
  | "True"
  | "False";
//# sourceMappingURL=ir.d.ts.map
