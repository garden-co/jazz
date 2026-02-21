import type {
  OperationPolicy,
  PolicyCmpOp,
  PolicyExpr,
  PolicyValue,
  TablePolicies,
} from "../schema.js";
import type { WasmSchema } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";
import type {
  PolicyExprV2,
  RelColumnRef,
  RelExpr,
  RelJoinCondition,
  RelPredicateExpr,
  RelProjectColumn,
  RelValueRef,
} from "../ir.js";

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
const RECURSIVE_POLICY_MAX_DEPTH_DEFAULT = 10;
const RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP = 64;

interface SessionRefValue {
  readonly __jazzPermissionKind: "session-ref";
  readonly path: string[];
}

interface RecursiveDepthOptions {
  maxDepth?: number;
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

interface RelationJoinSpec {
  table: string;
  left: string;
  right: string;
  viaHop?: boolean;
}

interface RelationFilterEntry {
  column: string;
  raw: unknown;
}

interface TableRelationPlan {
  kind: "table";
  table: string;
  filters: RelationFilterEntry[];
  joins: RelationJoinSpec[];
  selectMap?: Record<string, string>;
}

interface RecursiveRelationPlan {
  kind: "recursive";
  alias: string;
  startTable: string;
  startColumn: string;
  startFilters: RelationFilterEntry[];
  stepTable: string;
  stepInputColumn: string;
  stepOutputColumn: string;
  stepFilters: RelationFilterEntry[];
  maxDepth: number;
  filters: RelationFilterEntry[];
  joins: RelationJoinSpec[];
}

type RelationPlan = TableRelationPlan | RecursiveRelationPlan;

interface TableJoinTarget {
  readonly __jazzPermissionKind: "table-builder";
  readonly __jazzPermissionTable: string;
}

type RelationJoinTarget = string | TableJoinTarget;

export interface PermissionRelation {
  where(input: unknown): PermissionRelation;
  join(target: RelationJoinTarget, on: { left: string; right: string }): PermissionRelation;
  select(columns: Record<string, string>): PermissionRelation;
  hopTo(relation: string): PermissionRelation;
  gather(options: {
    start: Record<string, unknown>;
    step: (ctx: { current: unknown }) => PermissionRelation;
    maxDepth?: number;
  }): PermissionRelation;
}

interface RecursiveCurrentValue {
  readonly __jazzPermissionKind: "recursive-current";
}

class PermissionRelationBuilder implements PermissionRelation {
  constructor(
    private readonly plan: RelationPlan,
    private readonly relations: Map<string, Relation[]>,
  ) {}

  where(input: unknown): PermissionRelation {
    const where = resolveRelationWhereInput(input);
    const filters = [...this.plan.filters, ...extractRelationFilters(where)];
    return new PermissionRelationBuilder(
      {
        ...this.plan,
        filters,
      },
      this.relations,
    );
  }

  join(target: RelationJoinTarget, on: { left: string; right: string }): PermissionRelation {
    const table = relationJoinTargetToTable(target);
    const joins = [
      ...this.plan.joins,
      {
        table,
        left: on.left,
        right: on.right,
      },
    ];
    return new PermissionRelationBuilder(
      {
        ...this.plan,
        joins,
      },
      this.relations,
    );
  }

  select(columns: Record<string, string>): PermissionRelation {
    return new PermissionRelationBuilder(
      {
        ...this.plan,
        selectMap: normalizeRelationSelectMap(columns),
      },
      this.relations,
    );
  }

  hopTo(relation: string): PermissionRelation {
    const relationName = relation.trim();
    if (!relationName) {
      throw new Error("hopTo(...) requires a non-empty relation name.");
    }

    if (this.plan.kind === "table") {
      if (this.plan.joins.length > 0) {
        throw new Error("hopTo(...) currently supports a single hop per relation in MVP.");
      }
      if (this.plan.selectMap && Object.keys(this.plan.selectMap).length > 0) {
        throw new Error("hopTo(...) cannot be composed after select(...).");
      }
      const rel = resolveNamedRelation(this.relations, this.plan.table, relationName);
      const join: RelationJoinSpec =
        rel.type === "forward"
          ? {
              table: rel.toTable,
              left: rel.fromColumn,
              right: "id",
              viaHop: true,
            }
          : {
              table: rel.toTable,
              left: "id",
              right: rel.toColumn,
              viaHop: true,
            };
      return new PermissionRelationBuilder(
        {
          ...this.plan,
          joins: [...this.plan.joins, join],
        },
        this.relations,
      );
    }

    // Recursive relation hop: anchored against the recursive row identity.
    if (this.plan.joins.length > 0) {
      throw new Error("hopTo(...) currently supports a single hop per relation in MVP.");
    }

    const rel = resolveNamedRelation(this.relations, this.plan.startTable, relationName);
    if (rel.type !== "reverse") {
      throw new Error(
        `Recursive hopTo("${relationName}") currently requires a reverse relation from "${this.plan.startTable}".`,
      );
    }

    return new PermissionRelationBuilder(
      {
        ...this.plan,
        joins: [
          ...this.plan.joins,
          {
            table: rel.toTable,
            left: this.plan.alias,
            right: rel.toColumn,
            viaHop: true,
          },
        ],
      },
      this.relations,
    );
  }

  gather(options: {
    start: Record<string, unknown>;
    step: (ctx: { current: unknown }) => PermissionRelation;
    maxDepth?: number;
  }): PermissionRelation {
    if (this.plan.kind !== "table") {
      throw new Error("gather(...) must start from policy.<table>.");
    }
    if (this.plan.joins.length > 0) {
      throw new Error("gather(...) does not support pre-joined start relations in MVP.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires a step callback.");
    }

    const startWhere = resolveRelationWhereInput(options.start);
    const startFilters = [...this.plan.filters, ...extractRelationFilters(startWhere)];

    const currentToken: RecursiveCurrentValue = {
      __jazzPermissionKind: "recursive-current",
    };
    const stepPlan = getRelationPlan(options.step({ current: currentToken }));
    if (stepPlan.kind !== "table") {
      throw new Error("gather(...) step must return a relation built from policy.<table>.");
    }
    if (stepPlan.joins.length !== 1 || !stepPlan.joins[0]?.viaHop) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }
    if (stepPlan.selectMap && Object.keys(stepPlan.selectMap).length > 0) {
      throw new Error("gather(...) step does not support select(...).");
    }

    const currentFilters = stepPlan.filters.filter((filter) =>
      isRecursiveCurrentFilter(filter.raw, currentToken),
    );
    if (currentFilters.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const currentFilter = currentFilters[0];
    const stepFilters = stepPlan.filters.filter((filter) => filter !== currentFilter);
    const stepJoin = stepPlan.joins[0];

    if (stepJoin.table !== this.plan.table || stripQualifier(stepJoin.right) !== "id") {
      throw new Error(
        `gather(...) step must hop back to "${this.plan.table}" rows via hopTo(...).`,
      );
    }

    const maxDepth = normalizeRecursiveRelationDepth(options.maxDepth);
    return new PermissionRelationBuilder(
      {
        kind: "recursive",
        alias: "__recursive_current",
        startTable: this.plan.table,
        startColumn: "id",
        startFilters,
        stepTable: stepPlan.table,
        stepInputColumn: stripQualifier(currentFilter.column),
        stepOutputColumn: stripQualifier(stepJoin.left),
        stepFilters,
        maxDepth,
        filters: [],
        joins: [],
      },
      this.relations,
    );
  }

  toPlan(): RelationPlan {
    return this.plan;
  }
}

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
  read(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
  insert(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
  update(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
  delete(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
}

interface ExistsBuilder<WhereInput> {
  where(input: PermissionWhereInput<WhereInput>): ExistsCondition;
}

interface ActionBuilder<WhereInput, Row> {
  where(
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
  ): Rule;
}

interface TableRelationBuilder<WhereInput, Row> extends TableJoinTarget, PermissionRelation {
  where(
    input: PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
  ): PermissionRelation;
  select(columns: Record<string, string>): PermissionRelation;
}

interface TablePolicyBuilder<WhereInput, Row> extends TableRelationBuilder<WhereInput, Row> {
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
  } & {
    exists(relation: PermissionRelation): PolicyExpr;
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
  const relationsByTable = collectRelationsByTable(app);
  const tableNames = Object.keys(app).filter((key) => key !== "wasmSchema");
  const ctx = {
    policy: buildPolicyContext(tableNames, relationsByTable),
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

function collectRelationsByTable(app: AppLike): Map<string, Relation[]> {
  const schema = (app as { wasmSchema?: unknown }).wasmSchema;
  if (!schema || typeof schema !== "object") {
    return new Map();
  }

  const typedSchema = schema as WasmSchema;
  if (!typedSchema.tables || typeof typedSchema.tables !== "object") {
    return new Map();
  }

  try {
    return analyzeRelations(typedSchema);
  } catch {
    // Preserve legacy behavior for partially-specified schemas used in tests/tooling.
    // hopTo/gather callers will still receive explicit unknown-relation errors.
    return new Map();
  }
}

function buildPolicyContext(
  tableNames: string[],
  relationsByTable: Map<string, Relation[]>,
): Record<string, unknown> {
  const context: Record<string, unknown> = {};
  for (const table of tableNames) {
    context[table] = buildTablePolicyBuilder(table, relationsByTable);
  }
  context.exists = (relation: PermissionRelation): PolicyExpr => compileRelationExists(relation);
  return context;
}

function buildTablePolicyBuilder(
  table: string,
  relationsByTable: Map<string, Relation[]>,
): Record<string, unknown> {
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
    __jazzPermissionKind: "table-builder",
    __jazzPermissionTable: table,
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
    where(input: unknown): PermissionRelation {
      return createTableRelation(table, relationsByTable).where(input);
    },
    select(columns: Record<string, string>): PermissionRelation {
      return createTableRelation(table, relationsByTable).select(columns);
    },
    hopTo(relation: string): PermissionRelation {
      return createTableRelation(table, relationsByTable).hopTo(relation);
    },
    gather(options: {
      start: Record<string, unknown>;
      step: (ctx: { current: unknown }) => PermissionRelation;
      maxDepth?: number;
    }): PermissionRelation {
      return createTableRelation(table, relationsByTable).gather(options);
    },
  };
}

function createTableRelation(
  table: string,
  relationsByTable: Map<string, Relation[]>,
): PermissionRelation {
  return new PermissionRelationBuilder(
    {
      kind: "table",
      table,
      filters: [],
      joins: [],
    },
    relationsByTable,
  );
}

function relationJoinTargetToTable(target: RelationJoinTarget): string {
  if (typeof target === "string") {
    return target;
  }
  if (
    isPlainObject(target) &&
    target.__jazzPermissionKind === "table-builder" &&
    typeof target.__jazzPermissionTable === "string"
  ) {
    return target.__jazzPermissionTable;
  }
  throw new Error("join(...) expects a table builder (policy.<table>) or table name string.");
}

function resolveNamedRelation(
  relationsByTable: Map<string, Relation[]>,
  table: string,
  relationName: string,
): Relation {
  const relations = relationsByTable.get(table) ?? [];
  const relation = relations.find((candidate) => candidate.name === relationName);
  if (!relation) {
    throw new Error(`Unknown relation "${relationName}" on table "${table}".`);
  }
  return relation;
}

function isRecursiveCurrentFilter(raw: unknown, token: RecursiveCurrentValue): boolean {
  if (raw === token) {
    return true;
  }
  if (!isPlainObject(raw)) {
    return false;
  }
  const keys = Object.keys(raw).filter((key) => raw[key] !== undefined);
  return keys.length === 1 && keys[0] === "eq" && raw.eq === token;
}

function resolveRelationWhereInput(input: unknown): Record<string, unknown> {
  if (typeof input === "function") {
    return resolveRelationWhereInput(input(createRowContext()));
  }
  return normalizeWhereObject(input);
}

function extractRelationFilters(where: Record<string, unknown>): RelationFilterEntry[] {
  const filters: RelationFilterEntry[] = [];
  for (const [column, raw] of Object.entries(where)) {
    if (raw === undefined) {
      continue;
    }
    filters.push({ column, raw });
  }
  return filters;
}

function normalizeRelationSelectMap(columns: Record<string, string>): Record<string, string> {
  if (!isPlainObject(columns)) {
    throw new Error("select(...) expects an object map: { alias: column }.");
  }
  const entries = Object.entries(columns);
  if (entries.length === 0) {
    throw new Error("select(...) requires at least one projected column.");
  }

  const selectMap: Record<string, string> = {};
  for (const [alias, column] of entries) {
    const normalizedAlias = alias.trim();
    if (!normalizedAlias) {
      throw new Error("select(...) alias names must be non-empty strings.");
    }
    if (typeof column !== "string" || !column.trim()) {
      throw new Error(`select(...) column for alias "${alias}" must be a non-empty string.`);
    }
    selectMap[normalizedAlias] = stripQualifier(column);
  }

  return selectMap;
}

function normalizeRecursiveRelationDepth(maxDepth?: number): number {
  if (maxDepth === undefined) {
    return RECURSIVE_POLICY_MAX_DEPTH_DEFAULT;
  }
  if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
    throw new Error("gather(...) maxDepth must be a positive integer.");
  }
  if (maxDepth > RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP) {
    throw new Error(
      `gather(...) maxDepth ${maxDepth} exceeds hard cap ${RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP}.`,
    );
  }
  return maxDepth;
}

function getRelationPlan(relation: PermissionRelation): RelationPlan {
  if (relation instanceof PermissionRelationBuilder) {
    return relation.toPlan();
  }
  throw new Error("Expected a relation built from policy.<table> with where/join/hopTo/gather.");
}

function relationColumnRef(column: string, defaultScope: string): RelColumnRef {
  const [prefix, bare] = splitQualifiedColumn(column);
  if (prefix) {
    return { scope: prefix, column: bare };
  }
  return { scope: defaultScope, column: bare };
}

function toRelValueRef(value: unknown, options: { allowRowRefs: boolean }): RelValueRef {
  if (isSessionRefValue(value)) {
    return { type: "SessionRef", path: value.path };
  }
  if (isRowRefValue(value)) {
    if (!options.allowRowRefs) {
      throw new Error("Row references are only valid inside exists() clauses.");
    }
    return {
      type: "OuterColumn",
      column: { column: value.column },
    };
  }
  return { type: "Literal", value };
}

function relationFilterToPredicates(
  filter: RelationFilterEntry,
  defaultScope: string,
): RelPredicateExpr[] {
  const left = relationColumnRef(filter.column, defaultScope);
  const raw = filter.raw;

  if (raw === null) {
    return [{ type: "IsNull", column: left }];
  }
  if (isSessionRefValue(raw) || isRowRefValue(raw)) {
    return [
      {
        type: "Cmp",
        left,
        op: "Eq",
        right: toRelValueRef(raw, { allowRowRefs: true }),
      },
    ];
  }
  if (!isPlainObject(raw)) {
    return [
      {
        type: "Cmp",
        left,
        op: "Eq",
        right: { type: "Literal", value: raw },
      },
    ];
  }

  const predicates: RelPredicateExpr[] = [];
  for (const [op, value] of Object.entries(raw)) {
    if (value === undefined) {
      continue;
    }
    switch (op) {
      case "eq":
        if (value === null) {
          predicates.push({ type: "IsNull", column: left });
        } else {
          predicates.push({
            type: "Cmp",
            left,
            op: "Eq",
            right: toRelValueRef(value, { allowRowRefs: true }),
          });
        }
        break;
      case "ne":
        if (value === null) {
          predicates.push({ type: "IsNotNull", column: left });
        } else {
          predicates.push({
            type: "Cmp",
            left,
            op: "Ne",
            right: toRelValueRef(value, { allowRowRefs: true }),
          });
        }
        break;
      case "gt":
        predicates.push({
          type: "Cmp",
          left,
          op: "Gt",
          right: toRelValueRef(value, { allowRowRefs: true }),
        });
        break;
      case "gte":
        predicates.push({
          type: "Cmp",
          left,
          op: "Ge",
          right: toRelValueRef(value, { allowRowRefs: true }),
        });
        break;
      case "lt":
        predicates.push({
          type: "Cmp",
          left,
          op: "Lt",
          right: toRelValueRef(value, { allowRowRefs: true }),
        });
        break;
      case "lte":
        predicates.push({
          type: "Cmp",
          left,
          op: "Le",
          right: toRelValueRef(value, { allowRowRefs: true }),
        });
        break;
      case "isNull":
        if (typeof value !== "boolean") {
          throw new Error(`"${filter.column}.isNull" expects a boolean value.`);
        }
        predicates.push(
          value ? { type: "IsNull", column: left } : { type: "IsNotNull", column: left },
        );
        break;
      case "in":
        if (!Array.isArray(value)) {
          throw new Error(`"${filter.column}.in" expects an array value.`);
        }
        predicates.push({
          type: "In",
          left,
          values: value.map((entry) => toRelValueRef(entry, { allowRowRefs: true })),
        });
        break;
      case "contains":
        predicates.push({
          type: "Contains",
          left,
          value: toRelValueRef(value, { allowRowRefs: true }),
        });
        break;
      default:
        throw new Error(`Unsupported where operator "${op}" in relation IR lowering.`);
    }
  }

  return predicates.length > 0 ? predicates : [{ type: "True" }];
}

function andRelPredicates(predicates: RelPredicateExpr[]): RelPredicateExpr {
  if (predicates.length === 0) {
    return { type: "True" };
  }
  if (predicates.length === 1) {
    return predicates[0];
  }
  return { type: "And", exprs: predicates };
}

function applyRelFilter(input: RelExpr, predicates: RelPredicateExpr[]): RelExpr {
  const predicate = andRelPredicates(predicates);
  if (predicate.type === "True") {
    return input;
  }
  return {
    type: "Filter",
    input,
    predicate,
  };
}

function joinConditionFromSpec(
  join: RelationJoinSpec,
  leftScope: string,
  rightScope: string,
): RelJoinCondition {
  return {
    left: relationColumnRef(join.left, leftScope),
    right: relationColumnRef(join.right, rightScope),
  };
}

function projectHopResult(scope: string): RelProjectColumn[] {
  return [
    {
      alias: "id",
      expr: {
        type: "Column",
        column: { scope, column: "id" },
      },
    },
  ];
}

function compileTableRelationToRelExpr(plan: TableRelationPlan): RelExpr {
  let relation: RelExpr = {
    type: "TableScan",
    table: plan.table,
  };
  let defaultScope = plan.table;
  let hasHopJoin = false;

  for (let i = 0; i < plan.joins.length; i += 1) {
    const join = plan.joins[i];
    const rightScope = join.viaHop ? `__hop_${i}` : `__join_${i}`;
    relation = {
      type: "Join",
      left: relation,
      right: {
        type: "TableScan",
        table: join.table,
      },
      on: [joinConditionFromSpec(join, defaultScope, rightScope)],
      joinKind: "Inner",
    };
    defaultScope = rightScope;
    hasHopJoin ||= Boolean(join.viaHop);
  }

  const predicates = plan.filters.flatMap((filter) =>
    relationFilterToPredicates(filter, defaultScope),
  );
  relation = applyRelFilter(relation, predicates);

  if (plan.selectMap && Object.keys(plan.selectMap).length > 0) {
    const columns: RelProjectColumn[] = Object.entries(plan.selectMap).map(([alias, column]) => ({
      alias,
      expr: {
        type: "Column",
        column: relationColumnRef(column, defaultScope),
      },
    }));
    relation = {
      type: "Project",
      input: relation,
      columns,
    };
  } else if (hasHopJoin) {
    relation = {
      type: "Project",
      input: relation,
      columns: projectHopResult(defaultScope),
    };
  }

  return relation;
}

function compileRecursiveRelationToRelExpr(plan: RecursiveRelationPlan): RelExpr {
  const seedPredicates = plan.startFilters.flatMap((filter) =>
    relationFilterToPredicates(filter, plan.startTable),
  );
  const seed = applyRelFilter(
    {
      type: "TableScan",
      table: plan.startTable,
    },
    seedPredicates,
  );

  const stepPredicates = [
    ...plan.stepFilters.flatMap((filter) => relationFilterToPredicates(filter, plan.stepTable)),
    {
      type: "Cmp",
      left: {
        scope: plan.stepTable,
        column: plan.stepInputColumn,
      },
      op: "Eq",
      right: {
        type: "RowId",
        source: "Frontier",
      },
    } satisfies RelPredicateExpr,
  ];
  const stepFiltered = applyRelFilter(
    {
      type: "TableScan",
      table: plan.stepTable,
    },
    stepPredicates,
  );

  const recursiveHopScope = "__recursive_hop_0";
  const stepJoined: RelExpr = {
    type: "Join",
    left: stepFiltered,
    right: {
      type: "TableScan",
      table: plan.startTable,
    },
    on: [
      {
        left: { scope: plan.stepTable, column: plan.stepOutputColumn },
        right: { scope: recursiveHopScope, column: "id" },
      },
    ],
    joinKind: "Inner",
  };
  const stepProjected: RelExpr = {
    type: "Project",
    input: stepJoined,
    columns: projectHopResult(recursiveHopScope),
  };

  let relation: RelExpr = {
    type: "Gather",
    seed,
    step: stepProjected,
    frontierKey: { type: "RowId", source: "Current" },
    maxDepth: plan.maxDepth,
    dedupeKey: [{ type: "RowId", source: "Current" }],
  };

  let defaultScope = plan.alias;
  let hasHopJoin = false;
  for (let i = 0; i < plan.joins.length; i += 1) {
    const join = plan.joins[i];
    const rightScope = join.viaHop ? `__recursive_join_${i}` : `__recursive_join_${i}`;
    relation = {
      type: "Join",
      left: relation,
      right: {
        type: "TableScan",
        table: join.table,
      },
      on: [joinConditionFromSpec(join, defaultScope, rightScope)],
      joinKind: "Inner",
    };
    defaultScope = rightScope;
    hasHopJoin ||= Boolean(join.viaHop);
  }

  const postPredicates = plan.filters.flatMap((filter) =>
    relationFilterToPredicates(filter, defaultScope),
  );
  relation = applyRelFilter(relation, postPredicates);

  if (hasHopJoin) {
    relation = {
      type: "Project",
      input: relation,
      columns: projectHopResult(defaultScope),
    };
  }

  return relation;
}

function compileRelationPlanToRelExpr(plan: RelationPlan): RelExpr {
  switch (plan.kind) {
    case "table":
      return compileTableRelationToRelExpr(plan);
    case "recursive":
      return compileRecursiveRelationToRelExpr(plan);
    default:
      throw new Error("Unsupported relation shape in relation IR compiler.");
  }
}

export function relationToIr(relation: PermissionRelation): RelExpr {
  return compileRelationPlanToRelExpr(getRelationPlan(relation));
}

export function relationExistsToPolicyV2(relation: PermissionRelation): PolicyExprV2 {
  return {
    type: "ExistsRel",
    rel: relationToIr(relation),
  };
}

function compileRelationExists(relation: PermissionRelation): PolicyExpr {
  const plan = getRelationPlan(relation);
  switch (plan.kind) {
    case "table":
      return compileTableRelationExists(plan);
    case "recursive":
      return compileRecursiveRelationExists(plan);
    default:
      throw new Error("Unsupported relation shape in policy.exists(...).");
  }
}

function compileTableRelationExists(plan: TableRelationPlan): PolicyExpr {
  if (plan.joins.length === 0) {
    return {
      type: "Exists",
      table: plan.table,
      condition: andExpr(compileFilterExprsForTable(plan.filters, plan.table, plan.selectMap)),
    };
  }

  if (plan.joins.length > 1) {
    throw new Error("policy.exists(...) currently supports at most one join for table relations.");
  }

  const join = plan.joins[0];
  const leftColumn = resolveJoinLeftColumn(join.left, plan.selectMap);
  const rightColumn = stripQualifier(join.right);

  const rootFilters: RelationFilterEntry[] = [];
  const joinFilters: RelationFilterEntry[] = [];
  for (const filter of plan.filters) {
    const target = classifyTableFilterTarget(filter.column, plan.table, join.table);
    if (target === "root") {
      rootFilters.push(filter);
    } else {
      joinFilters.push(filter);
    }
  }

  const joinCondition = andExpr([
    ...compileFilterExprsForTable(joinFilters, join.table),
    {
      type: "Cmp",
      column: rightColumn,
      op: "Eq",
      value: outerRowRefValue(leftColumn),
    },
  ]);

  return {
    type: "Exists",
    table: plan.table,
    condition: andExpr([
      ...compileFilterExprsForTable(rootFilters, plan.table, plan.selectMap),
      {
        type: "Exists",
        table: join.table,
        condition: joinCondition,
      },
    ]),
  };
}

function compileRecursiveRelationExists(plan: RecursiveRelationPlan): PolicyExpr {
  if (plan.joins.length > 1) {
    throw new Error(
      "policy.exists(...) currently supports at most one join after gather(...) in MVP.",
    );
  }

  if (plan.joins.length === 0) {
    const aliasFilters: RelationFilterEntry[] = [];
    for (const filter of plan.filters) {
      const target = classifyRecursiveFilterTarget(filter.column, plan.alias, "");
      if (target !== "alias") {
        throw new Error(
          `Filter "${filter.column}" is not valid without a join on recursive relation "${plan.alias}".`,
        );
      }
      aliasFilters.push(filter);
    }

    if (aliasFilters.length === 0) {
      return {
        type: "Exists",
        table: plan.startTable,
        condition: andExpr(compileFilterExprsForTable(plan.startFilters, plan.startTable)),
      };
    }

    const aliasPredicates = aliasFilters.flatMap((filter) =>
      compileRecursiveAliasFilter(filter, plan.alias, plan),
    );
    return andExpr(aliasPredicates);
  }

  const join = plan.joins[0];
  const joinLeft = stripQualifier(join.left);
  if (joinLeft !== plan.alias) {
    throw new Error(`First join after gather(...) must join from recursive alias "${plan.alias}".`);
  }

  const anchorTable = join.table;
  const anchorColumn = stripQualifier(join.right);

  const anchorFilters: RelationFilterEntry[] = [];
  const aliasFilters: RelationFilterEntry[] = [];
  for (const filter of plan.filters) {
    const target = classifyRecursiveFilterTarget(filter.column, plan.alias, anchorTable);
    if (target === "alias") {
      aliasFilters.push(filter);
    } else {
      anchorFilters.push(filter);
    }
  }

  const aliasExprs = aliasFilters.flatMap((filter) =>
    compileRecursiveAliasFilterAgainstAnchor(filter, plan.alias, anchorColumn),
  );

  return {
    type: "Exists",
    table: anchorTable,
    condition: andExpr([
      ...compileFilterExprsForTable(anchorFilters, anchorTable),
      ...aliasExprs,
      buildRecursiveReachableExpr(plan, outerRowRefValue(anchorColumn)),
    ]),
  };
}

function buildRecursiveReachableExpr(plan: RecursiveRelationPlan, value: PolicyValue): PolicyExpr {
  const startFilterExprs = compileFilterExprsForTable(plan.startFilters, plan.startTable);
  const stepFilterExprs = compileFilterExprsForTable(plan.stepFilters, plan.stepTable);
  const depthExprs: PolicyExpr[] = [];
  for (let depth = 0; depth <= plan.maxDepth; depth += 1) {
    depthExprs.push(buildRecursivePathExpr(plan, depth, value, startFilterExprs, stepFilterExprs));
  }
  return depthExprs.length === 1 ? depthExprs[0] : { type: "Or", exprs: depthExprs };
}

function buildRecursivePathExpr(
  plan: RecursiveRelationPlan,
  depth: number,
  value: PolicyValue,
  startFilterExprs: PolicyExpr[],
  stepFilterExprs: PolicyExpr[],
): PolicyExpr {
  if (depth === 0) {
    return {
      type: "Exists",
      table: plan.startTable,
      condition: andExpr([
        ...startFilterExprs,
        {
          type: "Cmp",
          column: plan.startColumn,
          op: "Eq",
          value,
        },
      ]),
    };
  }

  return {
    type: "Exists",
    table: plan.stepTable,
    condition: andExpr([
      ...stepFilterExprs,
      {
        type: "Cmp",
        column: plan.stepOutputColumn,
        op: "Eq",
        value,
      },
      buildRecursivePathExpr(
        plan,
        depth - 1,
        outerRowRefValue(plan.stepInputColumn),
        startFilterExprs,
        stepFilterExprs,
      ),
    ]),
  };
}

function compileRecursiveAliasFilter(
  filter: RelationFilterEntry,
  alias: string,
  plan: RecursiveRelationPlan,
): PolicyExpr[] {
  const column = stripQualifier(filter.column);
  if (column !== alias) {
    throw new Error(
      `Recursive filter "${filter.column}" must target alias "${alias}" when no join is present.`,
    );
  }

  return extractEqPolicyValues(filter.raw).map((value) => buildRecursiveReachableExpr(plan, value));
}

function compileRecursiveAliasFilterAgainstAnchor(
  filter: RelationFilterEntry,
  alias: string,
  anchorColumn: string,
): PolicyExpr[] {
  const column = stripQualifier(filter.column);
  if (column !== alias) {
    throw new Error(`Recursive alias filter "${filter.column}" must target "${alias}".`);
  }

  return extractEqPolicyValues(filter.raw).map((value) => ({
    type: "Cmp",
    column: anchorColumn,
    op: "Eq",
    value,
  }));
}

function extractEqPolicyValues(raw: unknown): PolicyValue[] {
  if (raw === null) {
    throw new Error("Recursive alias filters do not support null values.");
  }
  if (isPlainObject(raw)) {
    const keys = Object.keys(raw).filter((key) => raw[key] !== undefined);
    if (keys.length !== 1 || keys[0] !== "eq") {
      throw new Error('Recursive alias filters currently only support "eq".');
    }
    const eqValue = raw.eq;
    if (eqValue === null || eqValue === undefined) {
      throw new Error('Recursive alias filter "eq" must be a non-null value.');
    }
    return [toPolicyValue(eqValue, { allowRowRefs: true })];
  }
  return [toPolicyValue(raw, { allowRowRefs: true })];
}

function classifyTableFilterTarget(
  column: string,
  rootTable: string,
  joinTable: string,
): "root" | "join" {
  const [prefix] = splitQualifiedColumn(column);
  if (!prefix) {
    // With joins present, unqualified filters default to joined table.
    return "join";
  }
  if (prefix === rootTable) {
    return "root";
  }
  if (prefix === joinTable) {
    return "join";
  }
  throw new Error(`Unknown filter table prefix "${prefix}" in relation where("${column}").`);
}

function classifyRecursiveFilterTarget(
  column: string,
  alias: string,
  anchorTable: string,
): "alias" | "anchor" {
  const [prefix] = splitQualifiedColumn(column);
  if (!prefix) {
    return stripQualifier(column) === alias ? "alias" : "anchor";
  }
  if (prefix === alias) {
    return "alias";
  }
  if (anchorTable && prefix === anchorTable) {
    return "anchor";
  }
  throw new Error(
    `Unknown filter table prefix "${prefix}" in recursive relation where("${column}").`,
  );
}

function compileFilterExprsForTable(
  filters: RelationFilterEntry[],
  table: string,
  aliasMap?: Record<string, string>,
): PolicyExpr[] {
  const exprs: PolicyExpr[] = [];
  for (const filter of filters) {
    const column = resolveFilterColumnForTable(filter.column, table, aliasMap);
    exprs.push(...columnFilterToExprs(column, filter.raw, { allowRowRefs: true }));
  }
  return exprs;
}

function resolveFilterColumnForTable(
  column: string,
  table: string,
  aliasMap?: Record<string, string>,
): string {
  const [prefix, bare] = splitQualifiedColumn(column);
  if (!prefix) {
    if (aliasMap && aliasMap[column]) {
      return aliasMap[column];
    }
    return bare;
  }
  if (prefix !== table) {
    throw new Error(`Filter "${column}" does not target table "${table}".`);
  }
  return bare;
}

function resolveJoinLeftColumn(left: string, aliasMap?: Record<string, string>): string {
  const [prefix, bare] = splitQualifiedColumn(left);
  if (!prefix && aliasMap && aliasMap[left]) {
    return aliasMap[left];
  }
  return bare;
}

function splitQualifiedColumn(column: string): [string | undefined, string] {
  const dotIndex = column.indexOf(".");
  if (dotIndex < 0) {
    return [undefined, column];
  }
  return [column.slice(0, dotIndex), column.slice(dotIndex + 1)];
}

function stripQualifier(column: string): string {
  const [, bare] = splitQualifiedColumn(column);
  return bare;
}

function outerRowRefValue(column: string): PolicyValue {
  return {
    type: "SessionRef",
    path: [OUTER_ROW_SESSION_PREFIX, column],
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
  const inheritsExpr = (
    operation: "Select" | "Insert" | "Update" | "Delete",
    fkColumn: string,
    options?: RecursiveDepthOptions,
  ): PolicyExpr => {
    const maxDepth = options?.maxDepth;
    if (maxDepth !== undefined) {
      if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
        throw new Error(`allowedTo.*("${fkColumn}") maxDepth must be a positive integer.`);
      }
    }
    const expr: PolicyExpr = {
      type: "Inherits",
      operation,
      via_column: fkColumn,
    };
    if (maxDepth !== undefined) {
      expr.max_depth = maxDepth;
    }
    return expr;
  };

  return {
    read(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr {
      return inheritsExpr("Select", fkColumn, options);
    },
    insert(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr {
      return inheritsExpr("Insert", fkColumn, options);
    },
    update(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr {
      return inheritsExpr("Update", fkColumn, options);
    },
    delete(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr {
      return inheritsExpr("Delete", fkColumn, options);
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
