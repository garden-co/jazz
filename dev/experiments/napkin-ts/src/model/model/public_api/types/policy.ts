import {
  type PolicyExpr,
  type PolicyValue,
  type CmpOp,
  type Operation,
  PolicyExprs,
  policyExprEquals,
} from "../policy.js";
import {
  type ColumnRef,
  type PredicateExpr,
  type PredicateCmpOp,
  type RelExpr,
  type ValueRef,
  toColumnRef,
} from "../relation_ir.js";
import { type Value, type ValueInput, toValue } from "./value.js";
import type { TableName } from "./schema.js";

export type OperationPolicy = { using?: PolicyExpr; with_check?: PolicyExpr };

export type TablePolicies = {
  select: OperationPolicy;
  insert: OperationPolicy;
  update: OperationPolicy;
  delete: OperationPolicy;
};

export const defaultTablePolicies = (): TablePolicies => ({
  select: {},
  insert: {},
  update: {},
  delete: {},
});
export const tablePoliciesEqual = (a: TablePolicies, b: TablePolicies) => policyExprEquals(a, b);
export const tablePoliciesAreDefault = (p: TablePolicies) =>
  policyExprEquals(p, defaultTablePolicies());

export const TablePoliciesOps = {
  withSelect: (p: TablePolicies, using: PolicyExpr): TablePolicies => ({ ...p, select: { using } }),
  withInsert: (p: TablePolicies, with_check: PolicyExpr): TablePolicies => ({
    ...p,
    insert: { with_check },
  }),
  withUpdate: (
    p: TablePolicies,
    using: PolicyExpr | undefined,
    with_check: PolicyExpr,
  ): TablePolicies => ({
    ...p,
    update: { using, with_check },
  }),
  withDelete: (p: TablePolicies, using: PolicyExpr): TablePolicies => ({ ...p, delete: { using } }),
  hasAnyExplicitPolicy: (p: TablePolicies) =>
    !!(
      p.select.using ||
      p.insert.with_check ||
      p.update.using ||
      p.update.with_check ||
      p.delete.using
    ),
  hasExplicitUpdatePolicy: (p: TablePolicies) => !!(p.update.using || p.update.with_check),
};

function mergeExpr(target: OperationPolicy, key: "using" | "with_check", expr: PolicyExpr) {
  const existing = target[key];
  target[key] = existing ? PolicyExprs.or([existing, expr]) : expr;
}

/** Build table permissions with a TypeScript-DSL-like API. */
export function permissions(build: (p: TablePolicyBuilder) => void): TablePolicies {
  const b = new TablePolicyBuilder();
  build(b);
  for (const u of b.pendingUpdates) u.finish();
  return b.policies;
}

export class TablePolicyBuilder {
  policies = defaultTablePolicies();
  pendingUpdates: UpdatePolicyBuilder[] = [];
  allowRead() {
    return new ActionPolicyBuilder(this.policies.select, "using");
  }
  allowInsert() {
    return new ActionPolicyBuilder(this.policies.insert, "with_check");
  }
  allowDelete() {
    return new ActionPolicyBuilder(this.policies.delete, "using");
  }
  allowUpdate() {
    const u = new UpdatePolicyBuilder(this.policies);
    this.pendingUpdates.push(u);
    return u;
  }
}

export class ActionPolicyBuilder {
  constructor(
    private readonly target: OperationPolicy,
    private readonly key: "using" | "with_check",
  ) {}
  where(expr: PolicyExpr) {
    mergeExpr(this.target, this.key, expr);
  }
  always() {
    this.where({ type: "True" });
  }
  never() {
    this.where({ type: "False" });
  }
}

export class UpdatePolicyBuilder {
  private using?: PolicyExpr;
  private withCheck?: PolicyExpr;
  private done = false;
  constructor(private readonly policies: TablePolicies) {}
  where(expr: PolicyExpr) {
    this.using = expr;
    this.withCheck = expr;
  }
  always() {
    this.where({ type: "True" });
  }
  never() {
    this.where({ type: "False" });
  }
  whereOld(expr: PolicyExpr): this {
    this.using = expr;
    return this;
  }
  whereNew(expr: PolicyExpr): this {
    this.withCheck = expr;
    return this;
  }
  /** Rust's Drop impl. */
  finish() {
    if (this.done) return;
    this.done = true;
    const using = this.using ?? this.withCheck;
    const withCheck = this.withCheck ?? using;
    if (using) mergeExpr(this.policies.update, "using", using);
    if (withCheck) mergeExpr(this.policies.update, "with_check", withCheck);
  }
}

// ---- policy_expr DSL ----

export type SessionPath = string | string[];
const sessionPath = (p: SessionPath) => (typeof p === "string" ? p.split(".") : p);

export type PolicyValueInput =
  | { kind: "Literal"; value: Value }
  | { kind: "Session"; path: string[] };
const toPolicyValueInput = (v: PolicyValueInput | ValueInput): PolicyValueInput =>
  typeof v === "object" && v !== null && "kind" in v ? v : { kind: "Literal", value: toValue(v) };
const toPolicyValue = (v: PolicyValueInput | ValueInput): PolicyValue => {
  const i = toPolicyValueInput(v);
  return i.kind === "Literal"
    ? { type: "Literal", value: i.value }
    : { type: "SessionRef", path: i.path };
};

export type SessionWhere =
  | { kind: "Cmp"; op: CmpOp; value: Value }
  | { kind: "IsNull"; isNull: boolean }
  | { kind: "Contains"; value: Value }
  | { kind: "InList"; values: Value[] };

export const SessionWheres = {
  cmp(op: CmpOp, input: ValueInput): SessionWhere {
    const value = toValue(input);
    if (value.type === "Null" && op === "Eq") return { kind: "IsNull", isNull: true };
    if (value.type === "Null" && op === "Ne") return { kind: "IsNull", isNull: false };
    return { kind: "Cmp", op, value };
  },
  eq: (v: ValueInput) => SessionWheres.cmp("Eq", v),
  ne: (v: ValueInput) => SessionWheres.cmp("Ne", v),
  isNull: (isNull: boolean): SessionWhere => ({ kind: "IsNull", isNull }),
  contains: (v: ValueInput): SessionWhere => ({ kind: "Contains", value: toValue(v) }),
  inList: (vs: Iterable<ValueInput>): SessionWhere => ({
    kind: "InList",
    values: [...vs].map(toValue),
  }),
};

export class RelationBuilder {
  constructor(readonly rel: RelExpr) {}
  gather(step: RelationBuilder, maxDepth: number): RelationBuilder {
    return new RelationBuilder({
      kind: "Gather",
      seed: this.rel,
      step: step.rel,
      frontier_key: { kind: "RowId", ref: "Current" },
      bound: { kind: "MaxDepth", depth: maxDepth },
      dedupe_key: [{ kind: "RowId", ref: "Current" }],
    });
  }
  select(columns: Iterable<[string, string | ColumnRef]>): RelationBuilder {
    return new RelationBuilder({
      kind: "Project",
      input: this.rel,
      columns: [...columns].map(([alias, c]) => ({
        alias,
        expr: { kind: "Column", column: toColumnRef(c) },
      })),
    });
  }
  join(
    right: RelationBuilder,
    left: string | ColumnRef,
    rightColumn: string | ColumnRef,
  ): RelationBuilder {
    return new RelationBuilder({
      kind: "Join",
      left: this.rel,
      right: right.rel,
      on: [{ left: toColumnRef(left), right: toColumnRef(rightColumn) }],
      join_kind: "Inner",
    });
  }
  where(predicate: PredicateExpr): RelationBuilder {
    return new RelationBuilder({ kind: "Filter", input: this.rel, predicate });
  }
}

export type TableExists = { table: TableName; condition: PolicyExpr };

const isPolicyExpr = (x: PolicyExpr | PredicateExpr): x is PolicyExpr => "type" in x;

export class TableRef {
  constructor(readonly table: TableName) {}
  alias(alias: string): RelationBuilder {
    return new RelationBuilder({ kind: "TableScan", table: this.table, alias });
  }
  where(condition: PolicyExpr): TableExists;
  where(condition: PredicateExpr): RelationBuilder;
  where(condition: PolicyExpr | PredicateExpr): TableExists | RelationBuilder {
    if (isPolicyExpr(condition)) return { table: this.table, condition };
    return new RelationBuilder({
      kind: "Filter",
      input: { kind: "TableScan", table: this.table },
      predicate: condition,
    });
  }
}

const allowedTo = (operation: Operation, via_column: string, max_depth?: number): PolicyExpr => ({
  type: "Inherits",
  operation,
  via_column,
  max_depth,
});
const allowedToReferencing = (
  operation: Operation,
  source_table: string,
  via_column: string,
): PolicyExpr => ({
  type: "InheritsReferencing",
  operation,
  source_table,
  via_column,
});

const relCmp = (
  column: string | ColumnRef,
  op: PredicateCmpOp,
  right: ValueRef,
): PredicateExpr => ({
  kind: "Cmp",
  left: toColumnRef(column),
  op,
  right,
});

export const policyExpr = {
  session: (path: SessionPath): PolicyValueInput => ({ kind: "Session", path: sessionPath(path) }),
  literal: (v: ValueInput): PolicyValueInput => ({ kind: "Literal", value: toValue(v) }),
  null: (): PolicyValueInput => ({ kind: "Literal", value: { type: "Null" } }),
  sessionWhere(path: SessionPath, condition: SessionWhere | ValueInput): PolicyExpr {
    const p = sessionPath(path);
    const c: SessionWhere =
      typeof condition === "object" && condition !== null && "kind" in condition
        ? condition
        : SessionWheres.eq(condition as ValueInput);
    switch (c.kind) {
      case "Cmp":
        return { type: "SessionCmp", path: p, op: c.op, value: c.value };
      case "IsNull":
        return c.isNull
          ? { type: "SessionIsNull", path: p }
          : { type: "SessionIsNotNull", path: p };
      case "Contains":
        return { type: "SessionContains", path: p, value: c.value };
      case "InList":
        return c.values.length
          ? { type: "SessionInList", path: p, values: c.values }
          : { type: "False" };
    }
  },
  always: (): PolicyExpr => ({ type: "True" }),
  never: (): PolicyExpr => ({ type: "False" }),
  cmp: (column: string, op: CmpOp, value: PolicyValueInput | ValueInput): PolicyExpr => ({
    type: "Cmp",
    column,
    op,
    value: toPolicyValue(value),
  }),
  eq: (column: string, value: PolicyValueInput | ValueInput) => policyExpr.cmp(column, "Eq", value),
  ne: (column: string, value: PolicyValueInput | ValueInput) => policyExpr.cmp(column, "Ne", value),
  isNull: (column: string): PolicyExpr => ({ type: "IsNull", column }),
  isNotNull: (column: string): PolicyExpr => ({ type: "IsNotNull", column }),
  contains: (column: string, value: PolicyValueInput | ValueInput): PolicyExpr => ({
    type: "Contains",
    column,
    value: toPolicyValue(value),
  }),
  inSession: (column: string, path: SessionPath): PolicyExpr => ({
    type: "In",
    column,
    session_path: sessionPath(path),
  }),
  inList: (column: string, values: Iterable<PolicyValueInput | ValueInput>): PolicyExpr => ({
    type: "InList",
    column,
    values: [...values].map(toPolicyValue),
  }),
  allOf: (exprs: Iterable<PolicyExpr>) => PolicyExprs.and([...exprs]),
  anyOf: (exprs: Iterable<PolicyExpr>) => PolicyExprs.or([...exprs]),
  not: (expr: PolicyExpr) => PolicyExprs.not(expr),
  table: (table: TableName) => new TableRef(table),
  exists(e: TableExists | RelationBuilder): PolicyExpr {
    if (e instanceof RelationBuilder) return { type: "ExistsRel", rel: e.rel };
    return { type: "Exists", table: e.table, condition: e.condition };
  },
  allowedToRead: (via: string) => allowedTo("Select", via),
  allowedToInsert: (via: string) => allowedTo("Insert", via),
  allowedToUpdate: (via: string) => allowedTo("Update", via),
  allowedToDelete: (via: string) => allowedTo("Delete", via),
  allowedToReadWithDepth: (via: string, d: number) => allowedTo("Select", via, d),
  allowedToUpdateWithDepth: (via: string, d: number) => allowedTo("Update", via, d),
  allowedTo,
  allowedToWithDepth: allowedTo,
  allowedToReadReferencing: (src: string, via: string) => allowedToReferencing("Select", src, via),
  allowedToUpdateReferencing: (src: string, via: string) =>
    allowedToReferencing("Update", src, via),
  allowedToReferencing,
  rel: {
    eqFrontier: (column: string | ColumnRef) =>
      relCmp(column, "Eq", { kind: "RowId", ref: "Frontier" }),
    column: (scope: string, column: string): ColumnRef => ({ scope, column }),
    eqSession: (column: string | ColumnRef, path: SessionPath) =>
      relCmp(column, "Eq", { kind: "SessionRef", path: sessionPath(path) }),
    eqOuter: (column: string | ColumnRef, outer: string) =>
      relCmp(column, "Eq", { kind: "OuterColumn", column: { column: outer } }),
    eqLiteral: (column: string | ColumnRef, v: ValueInput) =>
      relCmp(column, "Eq", { kind: "Literal", value: toValue(v) }),
    isNull: (column: string | ColumnRef): PredicateExpr => ({
      kind: "IsNull",
      column: toColumnRef(column),
    }),
    allOf(exprs: Iterable<PredicateExpr>): PredicateExpr {
      const e = [...exprs];
      return e.length === 0 ? { kind: "True" } : e.length === 1 ? e[0] : { kind: "And", exprs: e };
    },
    anyOf(exprs: Iterable<PredicateExpr>): PredicateExpr {
      const e = [...exprs];
      return e.length === 0 ? { kind: "False" } : e.length === 1 ? e[0] : { kind: "Or", exprs: e };
    },
    cmp: relCmp,
  },
};
