// SQL-ish query AST accepted by the Groove facade. Builders only; the planner lowers it.
import type { Value } from "./records/index.js";

export type Query =
  | { kind: "Select"; select: Select }
  | { kind: "Set"; set: SetQuery }
  | { kind: "With"; with: WithQuery };

export type WithQuery = { recursive: boolean; ctes: Cte[]; query: Query };
export const withQuery = (ctes: Iterable<Cte>, query: Query): WithQuery => ({
  recursive: false,
  ctes: [...ctes],
  query,
});

export type CteMaterialization = "Unspecified" | "Materialized" | "NotMaterialized";
export type Cte = {
  name: string;
  columns: string[];
  query: Query;
  materialized: CteMaterialization;
};
export const cte = (name: string, query: Query, columns: string[] = []): Cte => ({
  name,
  columns,
  query,
  materialized: "Unspecified",
});

export type SelectQuantifier = "All" | "Distinct";

export class Select {
  quantifier: SelectQuantifier = "All";
  from: TableRef[] = [];
  selection: Expr | null = null;
  groupBy: Expr[] = [];
  having: Expr | null = null;
  orderBy: OrderByExpr[] = [];
  limit: Expr | null = null;
  offset: Expr | null = null;
  constructor(public projection: SelectItem[]) {}
  withFrom(from: TableRef[]): this {
    this.from = from;
    return this;
  }
  where(predicate: Expr): this {
    this.selection = predicate;
    return this;
  }
  withGroupBy(exprs: Expr[]): this {
    this.groupBy = exprs;
    return this;
  }
  withHaving(predicate: Expr): this {
    this.having = predicate;
    return this;
  }
  withOrderBy(exprs: OrderByExpr[]): this {
    this.orderBy = exprs;
    return this;
  }
  withLimit(limit: Expr): this {
    this.limit = limit;
    return this;
  }
  withOffset(offset: Expr): this {
    this.offset = offset;
    return this;
  }
  distinct(): this {
    this.quantifier = "Distinct";
    return this;
  }
}

export type SelectItem =
  | { kind: "Expr"; expr: Expr; alias: string | null }
  | { kind: "Wildcard" }
  | { kind: "QualifiedWildcard"; qualifier: string[] };
export const selectExpr = (expr: Expr, alias: string | null = null): SelectItem => ({
  kind: "Expr",
  expr,
  alias,
});

export type TableAlias = { name: string; columns: string[] };
export type ObjectName = string[];

export type TableRef =
  | { kind: "Named"; name: ObjectName; alias: TableAlias | null }
  | { kind: "Derived"; lateral: boolean; query: Query; alias: TableAlias | null }
  | {
      kind: "Join";
      left: TableRef;
      right: TableRef;
      joinKind: JoinKind;
      constraint: JoinConstraint;
    };

export const namedTable = (name: string): TableRef => ({
  kind: "Named",
  name: [name],
  alias: null,
});
export function aliasTable(t: TableRef, alias: string): TableRef {
  if (t.kind === "Join") return t;
  return { ...t, alias: { name: alias, columns: [] } };
}

export type JoinKind = "Inner" | "Left" | "Right" | "Full" | "Cross" | "Semi" | "Anti";
export type JoinConstraint =
  | { kind: "On"; expr: Expr }
  | { kind: "Using"; columns: string[] }
  | { kind: "Natural" }
  | { kind: "None" };

export type SetOperator = "Union" | "Except" | "Intersect";
export type SetQuantifier = "All" | "Distinct";
export type SetQuery = { left: Query; op: SetOperator; right: Query; quantifier: SetQuantifier };

export type UnaryOp = "Not" | "Neg" | "Plus" | "IsNull" | "IsNotNull";
export type BinaryOp =
  | "Eq"
  | "NotEq"
  | "Lt"
  | "LtEq"
  | "Gt"
  | "GtEq"
  | "And"
  | "Or"
  | "Add"
  | "Sub"
  | "Mul"
  | "Div"
  | "Mod"
  | "Like"
  | "NotLike"
  | "IsDistinctFrom"
  | "IsNotDistinctFrom";

export type ColumnRef = { qualifier: string[]; name: string };
export type QueryDataType = "U8" | "U16" | "U32" | "U64" | "F64" | "Bool" | "String" | "Bytes";
export type FunctionArg = { kind: "Expr"; expr: Expr } | { kind: "Wildcard" };
export type WindowSpec = { partitionBy: Expr[]; orderBy: OrderByExpr[] };
export type FunctionCall = {
  name: ObjectName;
  args: FunctionArg[];
  distinct: boolean;
  filter: Expr | null;
  over: WindowSpec | null;
};
export const functionCall = (name: string, args: FunctionArg[]): FunctionCall => ({
  name: [name],
  args,
  distinct: false,
  filter: null,
  over: null,
});
export type CorrelatedSubquery = { query: Query; outerRefs: ColumnRef[] };

export type Expr =
  | { kind: "Literal"; value: Value }
  | { kind: "Null" }
  | { kind: "Column"; column: ColumnRef }
  | { kind: "Parameter"; name: string }
  | { kind: "Unary"; op: UnaryOp; expr: Expr }
  | { kind: "Binary"; left: Expr; op: BinaryOp; right: Expr }
  | { kind: "Between"; expr: Expr; negated: boolean; low: Expr; high: Expr }
  | { kind: "InList"; expr: Expr; list: Expr[]; negated: boolean }
  | { kind: "InSubquery"; expr: Expr; query: Query; negated: boolean }
  | { kind: "Exists"; query: Query; negated: boolean }
  | { kind: "Function"; call: FunctionCall }
  | { kind: "Case"; operand: Expr | null; whenThen: [Expr, Expr][]; elseExpr: Expr | null }
  | { kind: "Cast"; expr: Expr; dataType: QueryDataType }
  | { kind: "Subquery"; query: Query }
  | { kind: "CorrelatedSubquery"; subquery: CorrelatedSubquery };

export const Expr = {
  column: (name: string, qualifier: string[] = []): Expr => ({
    kind: "Column",
    column: { qualifier, name },
  }),
  parameter: (name: string): Expr => ({ kind: "Parameter", name }),
  binary: (left: Expr, op: BinaryOp, right: Expr): Expr => ({ kind: "Binary", left, op, right }),
};

export type SortDirection = "Asc" | "Desc";
export type NullsOrder = "First" | "Last" | "Unspecified";
export type OrderByExpr = { expr: Expr; direction: SortDirection; nulls: NullsOrder };
export const orderBy = (expr: Expr, direction: SortDirection = "Asc"): OrderByExpr => ({
  expr,
  direction,
  nulls: "Unspecified",
});
