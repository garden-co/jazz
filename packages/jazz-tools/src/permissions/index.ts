import type {
  OperationPolicy,
  PolicyCmpOp,
  PolicyExpr,
  PolicyValue,
  TablePolicies,
} from "../schema.js";
import type { WasmSchema } from "../drivers/types.js";

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

const OUTER_ROW_SESSION_PREFIX = "__jazz_outer_row";

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
  | ((row: RowContext<Row>) => WhereInput | Condition);

export type SessionContext = Record<string, SessionRefValue>;

export interface AllowedToContext {
  read(fkColumn: string): PolicyExpr;
  insert(fkColumn: string): PolicyExpr;
  update(fkColumn: string): PolicyExpr;
  delete(fkColumn: string): PolicyExpr;
}

interface ExistsBuilder<WhereInput> {
  where(input: PermissionWhereInput<WhereInput>): ExistsCondition;
}

interface ActionBuilder<WhereInput, Row> {
  where(
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
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
  anyOf: (conditions: readonly unknown[]) => Condition;
  allOf: (conditions: readonly unknown[]) => Condition;
  allowedTo: AllowedToContext;
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
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
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
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
  ): this {
    this.oldCondition = resolveWhereInput(input);
    return this;
  }

  whereNew(
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
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
  const fkColumnsByTable = collectFkColumnsByTable(app);
  const tableNames = Object.keys(app).filter((key) => key !== "wasmSchema");
  const ctx = {
    policy: buildPolicyContext(tableNames),
    anyOf,
    allOf,
    allowedTo: createAllowedToContext(),
    session: createSessionContext(),
  } as unknown as PolicyContext<TApp>;
  const output = factory(ctx);
  const rules = Array.isArray(output) ? output : [output];
  return compileRules(rules, fkColumnsByTable);
}

function collectFkColumnsByTable(app: AppLike): Map<string, Set<string>> {
  const result = new Map<string, Set<string>>();
  const schema = (app as { wasmSchema?: unknown }).wasmSchema;
  if (!schema || typeof schema !== "object") {
    return result;
  }

  const typedSchema = schema as WasmSchema;
  if (!typedSchema.tables || typeof typedSchema.tables !== "object") {
    return result;
  }

  for (const [tableName, table] of Object.entries(typedSchema.tables)) {
    const fkColumns = new Set<string>();
    for (const column of table.columns ?? []) {
      if (column.references) {
        fkColumns.add(column.name);
      }
    }
    result.set(tableName, fkColumns);
  }

  return result;
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

function createAllowedToContext(): AllowedToContext {
  return {
    read(fkColumn: string): PolicyExpr {
      return {
        type: "Inherits",
        operation: "Select",
        via_column: fkColumn,
      };
    },
    insert(fkColumn: string): PolicyExpr {
      return {
        type: "Inherits",
        operation: "Insert",
        via_column: fkColumn,
      };
    },
    update(fkColumn: string): PolicyExpr {
      return {
        type: "Inherits",
        operation: "Update",
        via_column: fkColumn,
      };
    },
    delete(fkColumn: string): PolicyExpr {
      return {
        type: "Inherits",
        operation: "Delete",
        via_column: fkColumn,
      };
    },
  };
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
  if (typeof input === "function") {
    const result = input(createRowContext());
    return resolveWhereInput(result);
  }
  if (isExistsCondition(input)) {
    return input;
  }
  if (isCompoundCondition(input)) {
    return input;
  }
  if (isPolicyExpr(input)) {
    return input;
  }
  if (isPlainObject(input)) {
    return whereObjectToCondition(input, { allowRowRefs: false });
  }
  throw new Error("Unsupported permission condition input.");
}

function whereObjectToCondition(
  where: Record<string, unknown>,
  options: { allowRowRefs: boolean },
): PolicyExpr {
  const exprs: PolicyExpr[] = [];
  for (const [column, raw] of Object.entries(where)) {
    if (raw === undefined) {
      continue;
    }
    exprs.push(...columnFilterToExprs(column, raw, options));
  }
  return andExpr(exprs);
}

function columnFilterToExprs(
  column: string,
  raw: unknown,
  options: { allowRowRefs: boolean },
): PolicyExpr[] {
  if (raw === null) {
    return [{ type: "IsNull", column }];
  }
  if (isSessionRefValue(raw)) {
    return [cmpExpr(column, "Eq", raw, options)];
  }
  if (isRowRefValue(raw)) {
    if (!options.allowRowRefs) {
      throw new Error("Row references are only valid inside exists() clauses.");
    }
    return [cmpExpr(column, "Eq", raw, options)];
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
            exprs.push(cmpExpr(column, "Eq", value, options));
          }
          break;
        case "ne":
          if (value === null) {
            exprs.push({ type: "IsNotNull", column });
          } else {
            exprs.push(cmpExpr(column, "Ne", value, options));
          }
          break;
        case "gt":
          exprs.push(cmpExpr(column, "Gt", value, options));
          break;
        case "gte":
          exprs.push(cmpExpr(column, "Ge", value, options));
          break;
        case "lt":
          exprs.push(cmpExpr(column, "Lt", value, options));
          break;
        case "lte":
          exprs.push(cmpExpr(column, "Le", value, options));
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
  return [cmpExpr(column, "Eq", raw, options)];
}

function cmpExpr(
  column: string,
  op: PolicyCmpOp,
  value: unknown,
  options: { allowRowRefs: boolean },
): PolicyExpr {
  return {
    type: "Cmp",
    column,
    op,
    value: toPolicyValue(value, options),
  };
}

function toPolicyValue(value: unknown, options: { allowRowRefs: boolean }): PolicyValue {
  if (isSessionRefValue(value)) {
    return { type: "SessionRef", path: value.path };
  }
  if (isRowRefValue(value)) {
    if (!options.allowRowRefs) {
      throw new Error("Row references are only valid inside exists() clauses.");
    }
    return {
      type: "SessionRef",
      path: [OUTER_ROW_SESSION_PREFIX, value.column],
    };
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

export function anyOf(conditions: readonly unknown[]): Condition {
  return compoundCondition("Or", conditions);
}

export function allOf(conditions: readonly unknown[]): Condition {
  return compoundCondition("And", conditions);
}

function compoundCondition(op: "And" | "Or", inputs: readonly unknown[]): CompoundCondition {
  if (!Array.isArray(inputs)) {
    const fnName = op === "And" ? "allOf" : "anyOf";
    throw new Error(`"${fnName}(...)" expects an array of conditions.`);
  }

  return {
    __jazzPermissionKind: "compound",
    op,
    conditions: inputs.map((input) => resolveWhereInput(input)),
  };
}

function compileRules(
  rules: RuleLike[],
  fkColumnsByTable: Map<string, Set<string>>,
): CompiledPermissions {
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
          using: compileCondition(rule.using, rule.table, fkColumnsByTable),
        });
        break;
      case "insert":
        tablePolicies.insert = mergeOperationPolicy(tablePolicies.insert, {
          with_check: compileCondition(rule.withCheck, rule.table, fkColumnsByTable),
        });
        break;
      case "update":
        tablePolicies.update = mergeOperationPolicy(tablePolicies.update, {
          using: compileCondition(rule.using, rule.table, fkColumnsByTable),
          with_check: compileCondition(rule.withCheck, rule.table, fkColumnsByTable),
        });
        break;
      case "delete":
        tablePolicies.delete = mergeOperationPolicy(tablePolicies.delete, {
          using: compileCondition(rule.using, rule.table, fkColumnsByTable),
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

function compileCondition(
  condition: Condition | undefined,
  table: string,
  fkColumnsByTable: Map<string, Set<string>>,
): PolicyExpr | undefined {
  if (!condition) {
    return undefined;
  }
  if (isPolicyExpr(condition)) {
    assertInheritsColumns(condition, table, fkColumnsByTable);
    return condition;
  }
  if (isExistsCondition(condition)) {
    const compiledCondition = whereObjectToCondition(condition.where, { allowRowRefs: true });
    assertInheritsColumns(compiledCondition, table, fkColumnsByTable);
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
    const compiledChildren = condition.conditions.map((child) =>
      compileCondition(child, table, fkColumnsByTable),
    );
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

function assertInheritsColumns(
  expr: PolicyExpr,
  table: string,
  fkColumnsByTable: Map<string, Set<string>>,
): void {
  const check = (node: PolicyExpr, currentTable: string): void => {
    switch (node.type) {
      case "Inherits": {
        const fkColumns = fkColumnsByTable.get(currentTable);
        if (!fkColumns) {
          throw new Error(
            `allowedTo.${node.operation.toLowerCase()}("${node.via_column}") is invalid for table "${currentTable}": ` +
              `table metadata is missing in app.wasmSchema.`,
          );
        }
        if (!fkColumns.has(node.via_column)) {
          const fkList = [...fkColumns].sort();
          const available = fkList.length > 0 ? fkList.join(", ") : "(none)";
          throw new Error(
            `allowedTo.${node.operation.toLowerCase()}("${node.via_column}") is invalid for table "${currentTable}": ` +
              `column is not a foreign key reference. Available FK columns: ${available}.`,
          );
        }
        break;
      }
      case "And":
      case "Or":
        for (const child of node.exprs) {
          check(child, currentTable);
        }
        break;
      case "Not":
        check(node.expr, currentTable);
        break;
      case "Exists":
        check(node.condition, node.table);
        break;
      default:
        break;
    }
  };

  check(expr, table);
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

function isUpdateRuleBuilder(input: unknown): input is UpdateRuleBuilder<unknown, unknown> {
  return isPlainObject(input) && typeof input.toRule === "function";
}
