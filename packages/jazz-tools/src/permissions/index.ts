import type {
  OperationPolicy,
  PolicyCmpOp,
  PolicyExpr,
  PolicyValue,
  TablePolicies,
} from "../schema.js";

type QueryBuilderLike = {
  _rowType: unknown;
  where(input: unknown): unknown;
};

type AppLike = Record<string, QueryBuilderLike | unknown> & {
  wasmSchema?: unknown;
};

type TableKey<TApp extends AppLike> = Exclude<keyof TApp, "wasmSchema">;
type QueryBuilderFor<TApp extends AppLike, K extends TableKey<TApp>> = Extract<
  TApp[K],
  QueryBuilderLike
>;
type RowFor<QB> = QB extends { _rowType: infer R } ? R : never;
type WhereFor<QB> = QB extends { where(input: infer W): unknown } ? W : never;

type PolicyAction = "read" | "insert" | "update" | "delete";

interface SessionRefValue {
  readonly __jazzPermissionKind: "session-ref";
  readonly path: string[];
}

interface RowRefValue {
  readonly __jazzPermissionKind: "row-ref";
  readonly column: string;
}

interface ExistsCondition {
  readonly __jazzPermissionKind: "exists";
  readonly table: string;
  readonly where: Record<string, unknown>;
}

interface CompoundCondition {
  readonly __jazzPermissionKind: "compound";
  readonly op: "And" | "Or";
  readonly conditions: Condition[];
}

type Condition = PolicyExpr | CompoundCondition | ExistsCondition;

interface Rule {
  table: string;
  action: PolicyAction;
  using?: Condition;
  withCheck?: Condition;
}

type RuleLike = Rule | UpdateRuleBuilder<unknown, unknown>;

type RowContext<Row> = {
  [K in keyof Row & string]: RowRefValue;
};

export type WhereInputOrCallback<WhereInput, Row> =
  | WhereInput
  | ((row: RowContext<Row>) => WhereInput | ConditionBuilder);

export interface ConditionBuilder {
  and(input: unknown): ConditionBuilder;
  or(input: unknown): ConditionBuilder;
  _condition(): Condition;
}

export type SessionContext = Record<string, SessionRefValue>;

interface ExistsBuilder<WhereInput> {
  where(input: PermissionWhereInput<WhereInput>): ExistsCondition;
}

interface ActionBuilder<WhereInput, Row> {
  where(
    input:
      | Condition
      | PermissionWhereInput<WhereInput>
      | ((row: RowContext<Row>) => unknown)
      | ConditionBuilder,
  ): Rule;
}

interface TablePolicyBuilder<WhereInput, Row> {
  allowRead: ActionBuilder<WhereInput, Row>;
  allowReads: ActionBuilder<WhereInput, Row>;
  allowInsert: ActionBuilder<WhereInput, Row>;
  allowInserts: ActionBuilder<WhereInput, Row>;
  allowDelete: ActionBuilder<WhereInput, Row>;
  allowDeletes: ActionBuilder<WhereInput, Row>;
  allowUpdate: UpdateRuleBuilder<WhereInput, Row>;
  allowUpdates: UpdateRuleBuilder<WhereInput, Row>;
  exists: ExistsBuilder<WhereInput>;
}

export type PolicyContext<TApp extends AppLike> = {
  policy: {
    [K in TableKey<TApp>]: TablePolicyBuilder<
      WhereFor<QueryBuilderFor<TApp, K>>,
      RowFor<QueryBuilderFor<TApp, K>>
    >;
  };
  either: (input: unknown) => ConditionBuilder;
  both: (input: unknown) => ConditionBuilder;
  session: SessionContext;
};

export type CompiledPermissions = Record<string, TablePolicies>;

type PermissionWhereInput<T> =
  T extends Array<infer U>
    ? Array<PermissionWhereInput<U>>
    : T extends object
      ? { [K in keyof T]?: PermissionWhereInput<T[K]> | SessionRefValue | RowRefValue }
      : T | SessionRefValue | RowRefValue;

class UpdateRuleBuilder<WhereInput, Row> {
  private oldCondition?: Condition;
  private newCondition?: Condition;

  constructor(private readonly table: string) {}

  where(
    input:
      | Condition
      | PermissionWhereInput<WhereInput>
      | ((row: RowContext<Row>) => unknown)
      | ConditionBuilder,
  ): Rule {
    const condition = resolveWhereInput(input);
    return {
      table: this.table,
      action: "update",
      using: condition,
      withCheck: condition,
    };
  }

  whereOld(
    input:
      | Condition
      | PermissionWhereInput<WhereInput>
      | ((row: RowContext<Row>) => unknown)
      | ConditionBuilder,
  ): this {
    this.oldCondition = resolveWhereInput(input);
    return this;
  }

  whereNew(
    input:
      | Condition
      | PermissionWhereInput<WhereInput>
      | ((row: RowContext<Row>) => unknown)
      | ConditionBuilder,
  ): this {
    this.newCondition = resolveWhereInput(input);
    return this;
  }

  toRule(): Rule {
    if (!this.oldCondition && !this.newCondition) {
      throw new Error(`Missing update policy conditions for table "${this.table}"`);
    }
    return {
      table: this.table,
      action: "update",
      using: this.oldCondition ?? this.newCondition,
      withCheck: this.newCondition ?? this.oldCondition,
    };
  }
}

export function definePermissions<TApp extends AppLike>(
  app: TApp,
  factory: (ctx: PolicyContext<TApp>) => RuleLike[] | RuleLike,
): CompiledPermissions {
  const tableNames = Object.keys(app).filter((key) => key !== "wasmSchema");
  const ctx = {
    policy: buildPolicyContext(tableNames),
    either,
    both,
    session: createSessionContext(),
  } as unknown as PolicyContext<TApp>;
  const output = factory(ctx);
  const rules = Array.isArray(output) ? output : [output];
  return compileRules(rules);
}

function buildPolicyContext(tableNames: string[]): Record<string, unknown> {
  const context: Record<string, unknown> = {};
  for (const table of tableNames) {
    context[table] = buildTablePolicyBuilder(table);
  }
  return context;
}

function buildTablePolicyBuilder(table: string): Record<string, unknown> {
  const read: ActionBuilder<unknown, unknown> = {
    where: (input) => ({ table, action: "read", using: resolveWhereInput(input) }),
  };
  const insert: ActionBuilder<unknown, unknown> = {
    where: (input) => ({ table, action: "insert", withCheck: resolveWhereInput(input) }),
  };
  const del: ActionBuilder<unknown, unknown> = {
    where: (input) => ({ table, action: "delete", using: resolveWhereInput(input) }),
  };
  const updateFactory = (): UpdateRuleBuilder<unknown, unknown> => new UpdateRuleBuilder(table);
  const exists: ExistsBuilder<unknown> = {
    where: (input) => ({
      __jazzPermissionKind: "exists",
      table,
      where: normalizeWhereObject(input),
    }),
  };

  return {
    allowRead: read,
    allowReads: read,
    allowInsert: insert,
    allowInserts: insert,
    allowDelete: del,
    allowDeletes: del,
    get allowUpdate() {
      return updateFactory();
    },
    get allowUpdates() {
      return updateFactory();
    },
    exists,
  };
}

function createSessionContext(): SessionContext {
  const claimRef = (path: string): SessionRefValue => ({
    __jazzPermissionKind: "session-ref",
    path: normalizeSessionPath(path),
  });
  return new Proxy({} as SessionContext, {
    get(_target, prop, _receiver) {
      if (typeof prop === "string") {
        return claimRef(prop);
      }
      return undefined;
    },
  });
}

function normalizeSessionPath(path: string | string[]): string[] {
  const parts = Array.isArray(path) ? path : path.split(".");
  return parts.map((part) => part.trim()).filter((part) => part.length > 0);
}

function createRowContext(): RowContext<Record<string, unknown>> {
  return new Proxy({} as RowContext<Record<string, unknown>>, {
    get(_target, prop) {
      if (typeof prop === "string") {
        return {
          __jazzPermissionKind: "row-ref",
          column: prop,
        } satisfies RowRefValue;
      }
      return undefined;
    },
  });
}

function normalizeWhereObject(input: unknown): Record<string, unknown> {
  if (!isPlainObject(input)) {
    throw new Error("Expected a where-object condition.");
  }
  return input;
}

function resolveWhereInput(input: unknown): Condition {
  if (isConditionBuilder(input)) {
    return input._condition();
  }
  if (typeof input === "function") {
    const result = input(createRowContext());
    return resolveWhereInput(result);
  }
  if (isExistsCondition(input)) {
    return input;
  }
  if (isPolicyExpr(input)) {
    return input;
  }
  if (isPlainObject(input)) {
    return whereObjectToCondition(input);
  }
  throw new Error("Unsupported permission condition input.");
}

function whereObjectToCondition(where: Record<string, unknown>): Condition {
  const exprs: PolicyExpr[] = [];
  for (const [column, raw] of Object.entries(where)) {
    if (raw === undefined) {
      continue;
    }
    exprs.push(...columnFilterToExprs(column, raw));
  }
  return andExpr(exprs);
}

function columnFilterToExprs(column: string, raw: unknown): PolicyExpr[] {
  if (raw === null) {
    return [{ type: "IsNull", column }];
  }
  if (isSessionRefValue(raw)) {
    return [cmpExpr(column, "Eq", raw)];
  }
  if (isRowRefValue(raw)) {
    throw new Error(
      `Correlated row references in exists(...) are not yet supported ("${column}").`,
    );
  }
  if (isPlainObject(raw)) {
    const exprs: PolicyExpr[] = [];
    for (const [op, value] of Object.entries(raw)) {
      if (value === undefined) {
        continue;
      }
      switch (op) {
        case "eq":
          if (value === null) {
            exprs.push({ type: "IsNull", column });
          } else {
            exprs.push(cmpExpr(column, "Eq", value));
          }
          break;
        case "ne":
          if (value === null) {
            exprs.push({ type: "IsNotNull", column });
          } else {
            exprs.push(cmpExpr(column, "Ne", value));
          }
          break;
        case "gt":
          exprs.push(cmpExpr(column, "Gt", value));
          break;
        case "gte":
          exprs.push(cmpExpr(column, "Ge", value));
          break;
        case "lt":
          exprs.push(cmpExpr(column, "Lt", value));
          break;
        case "lte":
          exprs.push(cmpExpr(column, "Le", value));
          break;
        case "isNull":
          if (typeof value !== "boolean") {
            throw new Error(`"${column}.isNull" expects a boolean value.`);
          }
          exprs.push(value ? { type: "IsNull", column } : { type: "IsNotNull", column });
          break;
        case "contains":
        case "in":
          throw new Error(
            `Where operator "${op}" is not yet supported in permissions DSL for "${column}".`,
          );
        default:
          throw new Error(`Unsupported where operator "${op}" in permissions DSL.`);
      }
    }
    return exprs.length === 0 ? [{ type: "True" }] : exprs;
  }
  return [cmpExpr(column, "Eq", raw)];
}

function cmpExpr(column: string, op: PolicyCmpOp, value: unknown): PolicyExpr {
  return {
    type: "Cmp",
    column,
    op,
    value: toPolicyValue(value),
  };
}

function toPolicyValue(value: unknown): PolicyValue {
  if (isSessionRefValue(value)) {
    return { type: "SessionRef", path: value.path };
  }
  if (isRowRefValue(value)) {
    throw new Error("Row references are only valid inside exists() clauses.");
  }
  return { type: "Literal", value };
}

function andExpr(exprs: PolicyExpr[]): PolicyExpr {
  if (exprs.length === 0) {
    return { type: "True" };
  }
  if (exprs.length === 1) {
    return exprs[0];
  }
  return { type: "And", exprs };
}

export function either(input: unknown): ConditionBuilder {
  return compoundBuilder("Or", input);
}

export function both(input: unknown): ConditionBuilder {
  return compoundBuilder("And", input);
}

function compoundBuilder(op: "And" | "Or", input: unknown): ConditionBuilder {
  const conditions: Condition[] = [resolveWhereInput(input)];
  return {
    and(next) {
      if (op !== "And") {
        throw new Error('Use "both(...)" for AND chains.');
      }
      conditions.push(resolveWhereInput(next));
      return this;
    },
    or(next) {
      if (op !== "Or") {
        throw new Error('Use "either(...)" for OR chains.');
      }
      conditions.push(resolveWhereInput(next));
      return this;
    },
    _condition() {
      return {
        __jazzPermissionKind: "compound",
        op,
        conditions: [...conditions],
      };
    },
  };
}

function compileRules(rules: RuleLike[]): CompiledPermissions {
  const compiled: CompiledPermissions = {};
  for (const ruleLike of rules) {
    const rule = isUpdateRuleBuilder(ruleLike) ? ruleLike.toRule() : ruleLike;
    if (!compiled[rule.table]) {
      compiled[rule.table] = {};
    }
    const tablePolicies = compiled[rule.table];
    switch (rule.action) {
      case "read":
        tablePolicies.select = mergeOperationPolicy(tablePolicies.select, {
          using: compileCondition(rule.using),
        });
        break;
      case "insert":
        tablePolicies.insert = mergeOperationPolicy(tablePolicies.insert, {
          with_check: compileCondition(rule.withCheck),
        });
        break;
      case "update":
        tablePolicies.update = mergeOperationPolicy(tablePolicies.update, {
          using: compileCondition(rule.using),
          with_check: compileCondition(rule.withCheck),
        });
        break;
      case "delete":
        tablePolicies.delete = mergeOperationPolicy(tablePolicies.delete, {
          using: compileCondition(rule.using),
        });
        break;
      default:
        throw new Error(`Unsupported action ${(rule as { action: string }).action}`);
    }
  }
  return compiled;
}

function mergeOperationPolicy(
  existing: OperationPolicy | undefined,
  incoming: OperationPolicy,
): OperationPolicy {
  return {
    using: mergeExprWithOr(existing?.using, incoming.using),
    with_check: mergeExprWithOr(existing?.with_check, incoming.with_check),
  };
}

function mergeExprWithOr(left?: PolicyExpr, right?: PolicyExpr): PolicyExpr | undefined {
  if (!left) {
    return right;
  }
  if (!right) {
    return left;
  }
  const exprs: PolicyExpr[] = [];
  if (left.type === "Or") {
    exprs.push(...left.exprs);
  } else {
    exprs.push(left);
  }
  if (right.type === "Or") {
    exprs.push(...right.exprs);
  } else {
    exprs.push(right);
  }
  return { type: "Or", exprs };
}

function compileCondition(condition: Condition | undefined): PolicyExpr | undefined {
  if (!condition) {
    return undefined;
  }
  if (isPolicyExpr(condition)) {
    return condition;
  }
  if (isExistsCondition(condition)) {
    const compiledCondition = compileCondition(resolveWhereInput(condition.where));
    if (!compiledCondition) {
      throw new Error(
        `Failed to compile exists(...) condition for table "${condition.table}" in permissions.ts`,
      );
    }
    return {
      type: "Exists",
      table: condition.table,
      condition: compiledCondition,
    };
  }
  if (isCompoundCondition(condition)) {
    const compiledChildren = condition.conditions.map((child) => compileCondition(child));
    const exprs = compiledChildren.filter((expr): expr is PolicyExpr => Boolean(expr));
    if (exprs.length === 0) {
      return condition.op === "And" ? { type: "True" } : { type: "False" };
    }
    if (exprs.length === 1) {
      return exprs[0];
    }
    return condition.op === "And" ? { type: "And", exprs } : { type: "Or", exprs };
  }
  throw new Error("Unsupported condition in permissions compiler.");
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Object.prototype.toString.call(value) === "[object Object]";
}

function isPolicyExpr(input: unknown): input is PolicyExpr {
  return isPlainObject(input) && typeof input.type === "string";
}

function isSessionRefValue(input: unknown): input is SessionRefValue {
  return (
    isPlainObject(input) &&
    input.__jazzPermissionKind === "session-ref" &&
    Array.isArray(input.path)
  );
}

function isRowRefValue(input: unknown): input is RowRefValue {
  return (
    isPlainObject(input) &&
    input.__jazzPermissionKind === "row-ref" &&
    typeof input.column === "string"
  );
}

function isExistsCondition(input: unknown): input is ExistsCondition {
  return (
    isPlainObject(input) &&
    input.__jazzPermissionKind === "exists" &&
    typeof input.table === "string" &&
    isPlainObject(input.where)
  );
}

function isCompoundCondition(input: unknown): input is CompoundCondition {
  return (
    isPlainObject(input) &&
    input.__jazzPermissionKind === "compound" &&
    (input.op === "And" || input.op === "Or") &&
    Array.isArray(input.conditions)
  );
}

function isConditionBuilder(input: unknown): input is ConditionBuilder {
  return isPlainObject(input) && typeof input._condition === "function";
}

function isUpdateRuleBuilder(input: unknown): input is UpdateRuleBuilder<unknown, unknown> {
  return isPlainObject(input) && typeof input.toRule === "function";
}
