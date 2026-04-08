import type {
  OperationPolicy,
  PolicyCmpOp,
  PolicyExpr,
  PolicyLiteralValue,
  PolicyValue,
  TablePolicies,
} from "../schema.js";
import type { WasmSchema } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";
import type {
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

type AppLike = object;

type TableKey<TApp extends AppLike> = Extract<
  {
    [K in keyof TApp]-?: K extends "wasmSchema"
      ? never
      : TApp[K] extends QueryBuilderLike
        ? K
        : never;
  }[keyof TApp],
  string
>;
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

interface SessionWhereCondition {
  readonly __jazzPermissionKind: "session-where";
  readonly where: Record<string, unknown>;
}

type SessionWhereBuilder = SessionRefValue &
  ((input: Record<string, unknown>) => SessionWhereCondition);

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

interface ExistsRelationCondition {
  readonly __jazzPermissionKind: "exists-relation";
  readonly relation: PermissionRelation;
}

interface CompoundCondition {
  readonly __jazzPermissionKind: "compound";
  readonly op: "And" | "Or";
  readonly conditions: Condition[];
}

type Condition =
  | PolicyExpr
  | CompoundCondition
  | ExistsCondition
  | ExistsRelationCondition
  | SessionWhereCondition;

interface RelationJoinSpec {
  table: string;
  left: string;
  right: string;
  viaHop?: boolean;
}

interface RelationFilterEntry {
  column: string;
  raw: unknown;
  scope: string;
}

interface RelationExprState {
  kind: "table" | "recursive";
  outputTable: string;
  base: RelExpr;
  initialScope: string;
  filters: RelationFilterEntry[];
  joins: RelationJoinSpec[];
  selectMap?: Record<string, string>;
}

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
    step: (ctx: { current: RecursiveCurrentValue }) => PermissionRelation;
    maxDepth?: number;
  }): PermissionRelation;
}

interface RecursiveCurrentValue {
  readonly __jazzPermissionKind: "recursive-current";
}

class PermissionRelationBuilder implements PermissionRelation {
  constructor(
    private readonly state: RelationExprState,
    private readonly relations: Map<string, Relation[]>,
  ) {}

  where(input: unknown): PermissionRelation {
    const where = resolveRelationWhereInput(input);
    const filters = [
      ...this.state.filters,
      ...extractRelationFilters(where, currentRelationScope(this.state)),
    ];
    return new PermissionRelationBuilder(
      {
        ...this.state,
        filters,
      },
      this.relations,
    );
  }

  join(target: RelationJoinTarget, on: { left: string; right: string }): PermissionRelation {
    const table = relationJoinTargetToTable(target);
    const joins = [
      ...this.state.joins,
      {
        table,
        left: on.left,
        right: on.right,
      },
    ];
    return new PermissionRelationBuilder(
      {
        ...this.state,
        joins,
      },
      this.relations,
    );
  }

  select(columns: Record<string, string>): PermissionRelation {
    return new PermissionRelationBuilder(
      {
        ...this.state,
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

    if (this.state.kind === "table") {
      if (this.state.joins.length > 0) {
        throw new Error("hopTo(...) currently supports a single hop per relation in MVP.");
      }
      if (this.state.selectMap && Object.keys(this.state.selectMap).length > 0) {
        throw new Error("hopTo(...) cannot be composed after select(...).");
      }
      const rel = resolveNamedRelation(this.relations, this.state.outputTable, relationName);
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
          ...this.state,
          joins: [...this.state.joins, join],
        },
        this.relations,
      );
    }

    // Recursive relation hop: anchored against the recursive row identity.
    if (this.state.joins.length > 0) {
      throw new Error("hopTo(...) currently supports a single hop per relation in MVP.");
    }

    const rel = resolveNamedRelation(this.relations, this.state.outputTable, relationName);
    if (rel.type !== "reverse") {
      throw new Error(
        `Recursive hopTo("${relationName}") currently requires a reverse relation from "${this.state.outputTable}".`,
      );
    }

    return new PermissionRelationBuilder(
      {
        ...this.state,
        joins: [
          ...this.state.joins,
          {
            table: rel.toTable,
            left: "id",
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
    step: (ctx: { current: RecursiveCurrentValue }) => PermissionRelation;
    maxDepth?: number;
  }): PermissionRelation {
    if (this.state.kind !== "table") {
      throw new Error("gather(...) must start from policy.<table>.");
    }
    if (this.state.joins.length > 0) {
      throw new Error("gather(...) does not support pre-joined start relations in MVP.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires a step callback.");
    }

    const startWhere = resolveRelationWhereInput(options.start);
    const startFilters = [
      ...this.state.filters,
      ...extractRelationFilters(startWhere, currentRelationScope(this.state)),
    ];

    const currentToken: RecursiveCurrentValue = {
      __jazzPermissionKind: "recursive-current",
    };
    const stepState = getRelationState(options.step({ current: currentToken }));
    if (stepState.kind !== "table") {
      throw new Error("gather(...) step must return a relation built from policy.<table>.");
    }
    if (stepState.joins.length !== 1 || !stepState.joins[0]?.viaHop) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }
    if (stepState.selectMap && Object.keys(stepState.selectMap).length > 0) {
      throw new Error("gather(...) step does not support select(...).");
    }

    const currentFilters = stepState.filters.filter((filter) =>
      isRecursiveCurrentFilter(filter.raw, currentToken),
    );
    if (currentFilters.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const currentFilter = currentFilters[0];
    if (!currentFilter) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const stepFilters = stepState.filters.filter((filter) => filter !== currentFilter);
    const stepJoin = stepState.joins[0];

    if (stepJoin.table !== this.state.outputTable || stripQualifier(stepJoin.right) !== "id") {
      throw new Error(
        `gather(...) step must hop back to "${this.state.outputTable}" rows via hopTo(...).`,
      );
    }

    const seedPredicates = startFilters.flatMap((filter) => relationFilterToPredicates(filter));
    const seed = applyRelFilter(this.state.base, seedPredicates);

    const stepPredicates = [
      ...stepFilters.flatMap((filter) => relationFilterToPredicates(filter)),
      {
        Cmp: {
          left: {
            scope: stepState.outputTable,
            column: stripQualifier(currentFilter.column),
          },
          op: "Eq",
          right: {
            RowId: "Frontier",
          },
        },
      } satisfies RelPredicateExpr,
    ];
    const stepFiltered = applyRelFilter(stepState.base, stepPredicates);

    const recursiveHopScope = "__recursive_hop_0";
    const stepProjected: RelExpr = {
      Project: {
        input: {
          Join: {
            left: stepFiltered,
            right: {
              TableScan: {
                table: this.state.outputTable,
              },
            },
            on: [
              {
                left: {
                  scope: stepState.outputTable,
                  column: stripQualifier(stepJoin.left),
                },
                right: { scope: recursiveHopScope, column: "id" },
              },
            ],
            join_kind: "Inner",
          },
        },
        columns: projectHopResult(recursiveHopScope),
      },
    };

    const maxDepth = normalizeRecursiveRelationDepth(options.maxDepth);
    return new PermissionRelationBuilder(
      {
        kind: "recursive",
        outputTable: this.state.outputTable,
        base: {
          Gather: {
            seed,
            step: stepProjected,
            frontier_key: { RowId: "Current" },
            max_depth: maxDepth,
            dedupe_key: [{ RowId: "Current" }],
          },
        },
        initialScope: this.state.outputTable,
        filters: [],
        joins: [],
        selectMap: undefined,
      },
      this.relations,
    );
  }

  toState(): RelationExprState {
    return this.state;
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

export type SessionContext = Record<string, SessionRefValue> & {
  readonly user_id: SessionRefValue;
  readonly userId: SessionRefValue;
  where: SessionWhereBuilder;
};

export interface AllowedToContext {
  read(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
  insert(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
  update(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
  delete(fkColumn: string, options?: RecursiveDepthOptions): PolicyExpr;
  readReferencing(
    sourceTable: RelationJoinTarget,
    fkColumn: string,
    options?: RecursiveDepthOptions,
  ): PolicyExpr;
  insertReferencing(
    sourceTable: RelationJoinTarget,
    fkColumn: string,
    options?: RecursiveDepthOptions,
  ): PolicyExpr;
  updateReferencing(
    sourceTable: RelationJoinTarget,
    fkColumn: string,
    options?: RecursiveDepthOptions,
  ): PolicyExpr;
  deleteReferencing(
    sourceTable: RelationJoinTarget,
    fkColumn: string,
    options?: RecursiveDepthOptions,
  ): PolicyExpr;
}

interface ExistsBuilder<WhereInput> {
  where(input: PermissionWhereInput<WhereInput>): ExistsCondition;
}

interface ActionBuilder<WhereInput, Row> {
  where(
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
  ): Rule;
  always(): Rule;
  never(): Rule;
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
    exists(relation: PermissionRelation): ExistsRelationCondition;
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
      ? {
          [K in keyof T]?:
            | PermissionWhereInput<T[K]>
            | SessionRefValue
            | RowRefValue
            | RecursiveCurrentValue;
        }
      : T | SessionRefValue | RowRefValue | RecursiveCurrentValue;

class UpdateRuleBuilder<WhereInput, Row> {
  private oldCondition?: Condition;
  private newCondition?: Condition;
  private isRegistered = false;

  constructor(
    private readonly table: string,
    private readonly registerRule?: (ruleLike: RuleLike) => void,
  ) {}

  where(
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
  ): Rule {
    const condition = resolveWhereInput(input);
    const rule: Rule = {
      table: this.table,
      action: "update",
      using: condition,
      withCheck: condition,
    };
    this.registerRule?.(rule);
    return rule;
  }

  never(): Rule {
    return this.where(neverCondition());
  }

  always(): Rule {
    return this.where(alwaysCondition());
  }

  whereOld(
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
  ): this {
    this.oldCondition = resolveWhereInput(input);
    this.registerBuilder();
    return this;
  }

  whereNew(
    input: Condition | PermissionWhereInput<WhereInput> | ((row: RowContext<Row>) => unknown),
  ): this {
    this.newCondition = resolveWhereInput(input);
    this.registerBuilder();
    return this;
  }

  private registerBuilder(): void {
    if (this.isRegistered) {
      return;
    }
    this.isRegistered = true;
    this.registerRule?.(this as unknown as RuleLike);
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
  factory: (ctx: PolicyContext<TApp>) => void,
): CompiledPermissions {
  const fkReferencesByTable = collectFkReferencesByTable(app);
  const relationsByTable = collectRelationsByTable(app);
  const tableNames = Object.keys(app).filter((key) => key !== "wasmSchema");
  const rules: RuleLike[] = [];
  const seenRules = new Set<RuleLike>();
  const collectRule = (ruleLike: RuleLike): void => {
    if (seenRules.has(ruleLike)) {
      return;
    }
    seenRules.add(ruleLike);
    rules.push(ruleLike);
  };
  const ctx = {
    policy: buildPolicyContext(tableNames, relationsByTable, collectRule),
    anyOf,
    allOf,
    allowedTo: createAllowedToContext(),
    session: createSessionContext(),
  } as unknown as PolicyContext<TApp>;
  factory(ctx);
  return compileRules(rules, fkReferencesByTable);
}

function collectFkReferencesByTable(app: AppLike): Map<string, Map<string, string>> {
  const result = new Map<string, Map<string, string>>();
  const schema = (app as { wasmSchema?: unknown }).wasmSchema;
  if (!schema || typeof schema !== "object") {
    return result;
  }

  const typedSchema = schema as WasmSchema;
  for (const [tableName, table] of Object.entries(typedSchema)) {
    if (!table || typeof table !== "object" || !Array.isArray(table.columns)) {
      continue;
    }
    const fkColumns = new Map<string, string>();
    for (const column of table.columns) {
      if (column.references) {
        fkColumns.set(column.name, column.references);
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
  try {
    return analyzeRelations(typedSchema);
  } catch {
    // Keep permissive behavior for partially-specified schemas used in tests/tooling.
    // hopTo/gather callers still receive explicit unknown-relation errors.
    return new Map();
  }
}

function buildPolicyContext(
  tableNames: string[],
  relationsByTable: Map<string, Relation[]>,
  collectRule: (ruleLike: RuleLike) => void,
): Record<string, unknown> {
  const context: Record<string, unknown> = {};
  for (const table of tableNames) {
    context[table] = buildTablePolicyBuilder(table, relationsByTable, collectRule);
  }
  context.exists = (relation: PermissionRelation): ExistsRelationCondition => ({
    __jazzPermissionKind: "exists-relation",
    relation,
  });
  return context;
}

function buildTablePolicyBuilder(
  table: string,
  relationsByTable: Map<string, Relation[]>,
  collectRule: (ruleLike: RuleLike) => void,
): Record<string, unknown> {
  const registerRule = (rule: Rule): Rule => {
    collectRule(rule);
    return rule;
  };
  const read: ActionBuilder<unknown, unknown> = {
    where: (input) => registerRule({ table, action: "read", using: resolveWhereInput(input) }),
    always: () => read.where(alwaysCondition()),
    never: () => read.where(neverCondition()),
  };
  const insert: ActionBuilder<unknown, unknown> = {
    where: (input) =>
      registerRule({ table, action: "insert", withCheck: resolveWhereInput(input) }),
    always: () => insert.where(alwaysCondition()),
    never: () => insert.where(neverCondition()),
  };
  const del: ActionBuilder<unknown, unknown> = {
    where: (input) => registerRule({ table, action: "delete", using: resolveWhereInput(input) }),
    always: () => del.where(alwaysCondition()),
    never: () => del.where(neverCondition()),
  };
  const updateFactory = (): UpdateRuleBuilder<unknown, unknown> =>
    new UpdateRuleBuilder(table, collectRule);
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
      outputTable: table,
      base: {
        TableScan: {
          table,
        },
      },
      initialScope: table,
      filters: [],
      joins: [],
      selectMap: undefined,
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

function currentRelationScope(state: RelationExprState): string {
  if (state.joins.length === 0) {
    return state.initialScope;
  }

  const joinIndex = state.joins.length - 1;
  const join = state.joins[joinIndex]!;
  if (state.kind === "recursive") {
    return `__recursive_join_${joinIndex}`;
  }
  return join.viaHop ? `__hop_${joinIndex}` : `__join_${joinIndex}`;
}

function extractRelationFilters(
  where: Record<string, unknown>,
  scope: string,
): RelationFilterEntry[] {
  const filters: RelationFilterEntry[] = [];
  for (const [column, raw] of Object.entries(where)) {
    if (raw === undefined) {
      continue;
    }
    filters.push({ column, raw, scope });
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

function getRelationState(relation: PermissionRelation): RelationExprState {
  if (relation instanceof PermissionRelationBuilder) {
    return relation.toState();
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
    return { SessionRef: value.path };
  }
  if (isRowRefValue(value)) {
    if (!options.allowRowRefs) {
      throw new Error("Row references are only valid inside exists() clauses.");
    }
    return {
      OuterColumn: { column: value.column },
    };
  }
  return { Literal: value };
}

function relationFilterToPredicates(filter: RelationFilterEntry): RelPredicateExpr[] {
  const left = relationColumnRef(filter.column, filter.scope);
  const raw = filter.raw;

  if (raw === null) {
    return [{ IsNull: { column: left } }];
  }
  if (isSessionRefValue(raw) || isRowRefValue(raw)) {
    return [
      {
        Cmp: {
          left,
          op: "Eq",
          right: toRelValueRef(raw, { allowRowRefs: true }),
        },
      },
    ];
  }
  if (!isPlainObject(raw)) {
    return [
      {
        Cmp: {
          left,
          op: "Eq",
          right: { Literal: raw },
        },
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
          predicates.push({ IsNull: { column: left } });
        } else {
          predicates.push({
            Cmp: {
              left,
              op: "Eq",
              right: toRelValueRef(value, { allowRowRefs: true }),
            },
          });
        }
        break;
      case "ne":
        if (value === null) {
          predicates.push({ IsNotNull: { column: left } });
        } else {
          predicates.push({
            Cmp: {
              left,
              op: "Ne",
              right: toRelValueRef(value, { allowRowRefs: true }),
            },
          });
        }
        break;
      case "gt":
        predicates.push({
          Cmp: {
            left,
            op: "Gt",
            right: toRelValueRef(value, { allowRowRefs: true }),
          },
        });
        break;
      case "gte":
        predicates.push({
          Cmp: {
            left,
            op: "Ge",
            right: toRelValueRef(value, { allowRowRefs: true }),
          },
        });
        break;
      case "lt":
        predicates.push({
          Cmp: {
            left,
            op: "Lt",
            right: toRelValueRef(value, { allowRowRefs: true }),
          },
        });
        break;
      case "lte":
        predicates.push({
          Cmp: {
            left,
            op: "Le",
            right: toRelValueRef(value, { allowRowRefs: true }),
          },
        });
        break;
      case "isNull":
        if (typeof value !== "boolean") {
          throw new Error(`"${filter.column}.isNull" expects a boolean value.`);
        }
        predicates.push(value ? { IsNull: { column: left } } : { IsNotNull: { column: left } });
        break;
      case "in":
        if (!Array.isArray(value)) {
          throw new Error(`"${filter.column}.in" expects an array value.`);
        }
        predicates.push({
          In: {
            left,
            values: value.map((entry) => toRelValueRef(entry, { allowRowRefs: true })),
          },
        });
        break;
      case "contains":
        predicates.push({
          Contains: {
            left,
            right: toRelValueRef(value, { allowRowRefs: true }),
          },
        });
        break;
      default:
        throw new Error(`Unsupported where operator "${op}" in relation IR lowering.`);
    }
  }

  return predicates.length > 0 ? predicates : ["True"];
}

function andRelPredicates(predicates: RelPredicateExpr[]): RelPredicateExpr {
  if (predicates.length === 0) {
    return "True";
  }
  if (predicates.length === 1) {
    return predicates[0]!;
  }
  return { And: predicates };
}

function applyRelFilter(input: RelExpr, predicates: RelPredicateExpr[]): RelExpr {
  const predicate = andRelPredicates(predicates);
  if (predicate === "True") {
    return input;
  }
  return {
    Filter: {
      input,
      predicate,
    },
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
        Column: { scope, column: "id" },
      },
    },
  ];
}

function applyRelationTail(options: {
  base: RelExpr;
  initialScope: string;
  joins: RelationJoinSpec[];
  filters: RelationFilterEntry[];
  selectMap?: Record<string, string>;
  joinAlias: (join: RelationJoinSpec, index: number) => string;
}): RelExpr {
  let relation = options.base;
  let defaultScope = options.initialScope;
  let hasHopJoin = false;

  for (let i = 0; i < options.joins.length; i += 1) {
    const join = options.joins[i]!;
    const rightScope = options.joinAlias(join, i);
    relation = {
      Join: {
        left: relation,
        right: {
          TableScan: {
            table: join.table,
          },
        },
        on: [joinConditionFromSpec(join, defaultScope, rightScope)],
        join_kind: "Inner",
      },
    };
    defaultScope = rightScope;
    hasHopJoin ||= Boolean(join.viaHop);
  }

  const predicates = options.filters.flatMap((filter) => relationFilterToPredicates(filter));
  relation = applyRelFilter(relation, predicates);

  if (options.selectMap && Object.keys(options.selectMap).length > 0) {
    const columns: RelProjectColumn[] = Object.entries(options.selectMap).map(
      ([alias, column]) => ({
        alias,
        expr: {
          Column: relationColumnRef(column, defaultScope),
        },
      }),
    );
    relation = {
      Project: {
        input: relation,
        columns,
      },
    };
  } else if (hasHopJoin) {
    relation = {
      Project: {
        input: relation,
        columns: projectHopResult(defaultScope),
      },
    };
  }

  return relation;
}

function relationStateToRelExpr(state: RelationExprState): RelExpr {
  return applyRelationTail({
    base: state.base,
    initialScope: state.initialScope,
    joins: state.joins,
    filters: state.filters,
    selectMap: state.selectMap,
    joinAlias: (join, index) =>
      state.kind === "recursive"
        ? `__recursive_join_${index}`
        : join.viaHop
          ? `__hop_${index}`
          : `__join_${index}`,
  });
}

export function relationToIr(relation: PermissionRelation): RelExpr {
  return relationStateToRelExpr(getRelationState(relation));
}

export function relationExistsToPolicy(relation: PermissionRelation): PolicyExpr {
  return {
    type: "ExistsRel",
    rel: relationToIr(relation),
  };
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

function createSessionContext(): SessionContext {
  const claimRef = (path: string): SessionRefValue => ({
    __jazzPermissionKind: "session-ref",
    path: normalizeSessionPath(path),
  });
  const whereBuilder = ((input: Record<string, unknown>): SessionWhereCondition => ({
    __jazzPermissionKind: "session-where",
    where: normalizeWhereObject(input),
  })) as SessionWhereBuilder;
  return new Proxy({} as SessionContext, {
    get(_target, prop, _receiver) {
      if (typeof prop === "string") {
        if (prop === "where") {
          return whereBuilder;
        }
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

  const inheritsReferencingExpr = (
    operation: "Select" | "Insert" | "Update" | "Delete",
    sourceTable: RelationJoinTarget,
    fkColumn: string,
    options?: RecursiveDepthOptions,
  ): PolicyExpr => {
    const maxDepth = options?.maxDepth;
    if (maxDepth !== undefined) {
      if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
        throw new Error(
          `allowedTo.*Referencing(..., "${fkColumn}") maxDepth must be a positive integer.`,
        );
      }
    }
    const expr: PolicyExpr = {
      type: "InheritsReferencing",
      operation,
      source_table: relationJoinTargetToTable(sourceTable),
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
    readReferencing(
      sourceTable: RelationJoinTarget,
      fkColumn: string,
      options?: RecursiveDepthOptions,
    ): PolicyExpr {
      return inheritsReferencingExpr("Select", sourceTable, fkColumn, options);
    },
    insertReferencing(
      sourceTable: RelationJoinTarget,
      fkColumn: string,
      options?: RecursiveDepthOptions,
    ): PolicyExpr {
      return inheritsReferencingExpr("Insert", sourceTable, fkColumn, options);
    },
    updateReferencing(
      sourceTable: RelationJoinTarget,
      fkColumn: string,
      options?: RecursiveDepthOptions,
    ): PolicyExpr {
      return inheritsReferencingExpr("Update", sourceTable, fkColumn, options);
    },
    deleteReferencing(
      sourceTable: RelationJoinTarget,
      fkColumn: string,
      options?: RecursiveDepthOptions,
    ): PolicyExpr {
      return inheritsReferencingExpr("Delete", sourceTable, fkColumn, options);
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
  if (isSessionWhereCondition(input)) {
    return input;
  }
  if (isExistsCondition(input)) {
    return input;
  }
  if (isExistsRelationCondition(input)) {
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

function sessionWhereObjectToCondition(where: Record<string, unknown>): PolicyExpr {
  const exprs: PolicyExpr[] = [];
  for (const [path, raw] of Object.entries(where)) {
    if (raw === undefined) {
      continue;
    }
    exprs.push(...sessionPathFilterToExprs(path, raw));
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
          exprs.push({
            type: "Contains",
            column,
            value: toPolicyValue(value, options),
          });
          break;
        case "in":
          if (isSessionRefValue(value)) {
            exprs.push({
              type: "In",
              column,
              session_path: value.path,
            });
            break;
          }
          if (!Array.isArray(value)) {
            throw new Error(`"${column}.in" expects an array or session reference.`);
          }
          if (value.length === 0) {
            exprs.push({ type: "False" });
            break;
          }
          exprs.push({
            type: "InList",
            column,
            values: value.map((entry) => toPolicyValue(entry, options)),
          });
          break;
        default:
          throw new Error(`Unsupported where operator "${op}" in permissions DSL.`);
      }
    }
    return exprs.length === 0 ? [{ type: "True" }] : exprs;
  }
  return [cmpExpr(column, "Eq", raw, options)];
}

function sessionPathFilterToExprs(path: string, raw: unknown): PolicyExpr[] {
  const sessionPath = normalizeSessionPath(path);
  if (sessionPath.length === 0) {
    throw new Error("session.where(...) requires non-empty session path keys.");
  }

  if (raw === null) {
    return [{ type: "SessionIsNull", path: sessionPath }];
  }
  if (
    !isPlainObject(raw) ||
    isSessionRefValue(raw) ||
    isRowRefValue(raw) ||
    isRecursiveCurrentValue(raw) ||
    isExistsCondition(raw) ||
    isExistsRelationCondition(raw) ||
    isCompoundCondition(raw) ||
    isPolicyExpr(raw)
  ) {
    return [sessionCmpExpr(sessionPath, "Eq", raw, path)];
  }

  const exprs: PolicyExpr[] = [];
  for (const [op, value] of Object.entries(raw)) {
    if (value === undefined) {
      continue;
    }
    switch (op) {
      case "eq":
        if (value === null) {
          exprs.push({ type: "SessionIsNull", path: sessionPath });
        } else {
          exprs.push(sessionCmpExpr(sessionPath, "Eq", value, path));
        }
        break;
      case "ne":
        if (value === null) {
          exprs.push({ type: "SessionIsNotNull", path: sessionPath });
        } else {
          exprs.push(sessionCmpExpr(sessionPath, "Ne", value, path));
        }
        break;
      case "gt":
        exprs.push(sessionCmpExpr(sessionPath, "Gt", value, path));
        break;
      case "gte":
        exprs.push(sessionCmpExpr(sessionPath, "Ge", value, path));
        break;
      case "lt":
        exprs.push(sessionCmpExpr(sessionPath, "Lt", value, path));
        break;
      case "lte":
        exprs.push(sessionCmpExpr(sessionPath, "Le", value, path));
        break;
      case "isNull":
        if (typeof value !== "boolean") {
          throw new Error(`session.where("${path}.isNull") expects a boolean value.`);
        }
        exprs.push(
          value
            ? { type: "SessionIsNull", path: sessionPath }
            : { type: "SessionIsNotNull", path: sessionPath },
        );
        break;
      case "contains":
        exprs.push({
          type: "SessionContains",
          path: sessionPath,
          value: toPolicyLiteralValue(value, `session.where("${path}.contains")`),
        });
        break;
      case "in":
        if (!Array.isArray(value)) {
          throw new Error(`session.where("${path}.in") expects an array of literal values.`);
        }
        if (value.length === 0) {
          exprs.push({ type: "False" });
          break;
        }
        exprs.push({
          type: "SessionInList",
          path: sessionPath,
          values: value.map((entry) => toPolicyLiteralValue(entry, `session.where("${path}.in")`)),
        });
        break;
      default:
        throw new Error(
          `Unsupported session.where operator "${op}" in permissions DSL. Nested object claim syntax is not supported; use dotted path keys instead.`,
        );
    }
  }

  return exprs.length === 0 ? [{ type: "True" }] : exprs;
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

function sessionCmpExpr(
  path: string[],
  op: PolicyCmpOp,
  value: unknown,
  originalPath: string,
): PolicyExpr {
  return {
    type: "SessionCmp",
    path,
    op,
    value: toPolicyLiteralValue(value, `session.where("${originalPath}")`),
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

function toPolicyLiteralValue(value: unknown, context: string): PolicyLiteralValue {
  assertSessionWhereLiteralValue(value, context);
  return { type: "Literal", value };
}

function assertSessionWhereLiteralValue(value: unknown, context: string): void {
  if (isSessionRefValue(value)) {
    throw new Error(
      `${context} only accepts literal values; session references are not supported.`,
    );
  }
  if (isRowRefValue(value)) {
    throw new Error(`${context} only accepts literal values; row references are not supported.`);
  }
  if (isRecursiveCurrentValue(value)) {
    throw new Error(
      `${context} only accepts literal values; recursive current refs are not supported.`,
    );
  }
  if (
    isExistsCondition(value) ||
    isExistsRelationCondition(value) ||
    isCompoundCondition(value) ||
    isPolicyExpr(value)
  ) {
    throw new Error(
      `${context} only accepts literal values; relation and policy expressions are not supported.`,
    );
  }
  if (typeof value === "function" || value === undefined) {
    throw new Error(`${context} only accepts literal values.`);
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      assertSessionWhereLiteralValue(entry, context);
    }
    return;
  }
  if (isPlainObject(value)) {
    throw new Error(`${context} only accepts literal values; nested objects are not supported.`);
  }
}

function andExpr(exprs: PolicyExpr[]): PolicyExpr {
  if (exprs.length === 0) {
    return { type: "True" };
  }
  if (exprs.length === 1) {
    return exprs[0]!;
  }
  return { type: "And", exprs };
}

export function anyOf(conditions: readonly unknown[]): Condition {
  return compoundCondition("Or", conditions);
}

export function allOf(conditions: readonly unknown[]): Condition {
  return compoundCondition("And", conditions);
}

function alwaysCondition(): Condition {
  return allOf([]);
}

function neverCondition(): Condition {
  return anyOf([]);
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
  fkReferencesByTable: Map<string, Map<string, string>>,
): CompiledPermissions {
  const compiled: CompiledPermissions = {};
  for (const ruleLike of rules) {
    const rule = isUpdateRuleBuilder(ruleLike) ? ruleLike.toRule() : ruleLike;
    if (!compiled[rule.table]) {
      compiled[rule.table] = emptyTablePolicies();
    }
    const tablePolicies = compiled[rule.table]!;
    switch (rule.action) {
      case "read":
        tablePolicies.select = mergeOperationPolicy(tablePolicies.select, {
          using: compileCondition(rule.using, rule.table, fkReferencesByTable),
        });
        break;
      case "insert":
        tablePolicies.insert = mergeOperationPolicy(tablePolicies.insert, {
          with_check: compileCondition(rule.withCheck, rule.table, fkReferencesByTable),
        });
        break;
      case "update":
        tablePolicies.update = mergeOperationPolicy(tablePolicies.update, {
          using: compileCondition(rule.using, rule.table, fkReferencesByTable),
          with_check: compileCondition(rule.withCheck, rule.table, fkReferencesByTable),
        });
        break;
      case "delete":
        tablePolicies.delete = mergeOperationPolicy(tablePolicies.delete, {
          using: compileCondition(rule.using, rule.table, fkReferencesByTable),
        });
        break;
      default:
        throw new Error(`Unsupported action ${(rule as { action: string }).action}`);
    }
  }
  return compiled;
}

function emptyOperationPolicy(): OperationPolicy {
  return {};
}

function emptyTablePolicies(): TablePolicies {
  return {
    select: emptyOperationPolicy(),
    insert: emptyOperationPolicy(),
    update: emptyOperationPolicy(),
    delete: emptyOperationPolicy(),
  };
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
  fkReferencesByTable: Map<string, Map<string, string>>,
): PolicyExpr | undefined {
  if (!condition) {
    return undefined;
  }
  if (isPolicyExpr(condition)) {
    resolveAndAssertInheritsColumns(condition, table, fkReferencesByTable);
    return condition;
  }
  if (isSessionWhereCondition(condition)) {
    return sessionWhereObjectToCondition(condition.where);
  }
  if (isExistsRelationCondition(condition)) {
    return {
      type: "ExistsRel",
      rel: relationToIr(condition.relation),
    };
  }
  if (isExistsCondition(condition)) {
    const compiledCondition = whereObjectToCondition(condition.where, { allowRowRefs: true });
    resolveAndAssertInheritsColumns(compiledCondition, table, fkReferencesByTable);
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
      compileCondition(child, table, fkReferencesByTable),
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

function resolveFkColumn(name: string, fkColumns: Map<string, string>): string | undefined {
  if (fkColumns.has(name)) return name;
  const withId = name + "Id";
  if (fkColumns.has(withId)) return withId;
  const withUnderId = name + "_id";
  if (fkColumns.has(withUnderId)) return withUnderId;
  return undefined;
}

function resolveAndAssertInheritsColumns(
  expr: PolicyExpr,
  table: string,
  fkReferencesByTable: Map<string, Map<string, string>>,
): void {
  const check = (node: PolicyExpr, currentTable: string): void => {
    switch (node.type) {
      case "Inherits": {
        const fkColumns = fkReferencesByTable.get(currentTable);
        if (!fkColumns) {
          throw new Error(
            `allowedTo.${node.operation.toLowerCase()}("${node.via_column}") is invalid for table "${currentTable}": ` +
              `table metadata is missing in app.wasmSchema.`,
          );
        }
        const resolved = resolveFkColumn(node.via_column, fkColumns);
        if (!resolved) {
          const fkList = [...fkColumns.keys()].sort();
          const available = fkList.length > 0 ? fkList.join(", ") : "(none)";
          throw new Error(
            `allowedTo.${node.operation.toLowerCase()}("${node.via_column}") is invalid for table "${currentTable}": ` +
              `column is not a foreign key reference. Available FK columns: ${available}.`,
          );
        }
        node.via_column = resolved;
        break;
      }
      case "InheritsReferencing": {
        const originalColumn = node.via_column;
        const sourceFks = fkReferencesByTable.get(node.source_table);
        if (!sourceFks) {
          throw new Error(
            `allowedTo.${node.operation.toLowerCase()}Referencing(policy.${node.source_table}, "${originalColumn}") is invalid for table "${currentTable}": ` +
              `source table metadata is missing in app.wasmSchema.`,
          );
        }
        const resolved = resolveFkColumn(originalColumn, sourceFks);
        if (!resolved) {
          const fkList = [...sourceFks.keys()].sort();
          const available = fkList.length > 0 ? fkList.join(", ") : "(none)";
          throw new Error(
            `allowedTo.${node.operation.toLowerCase()}Referencing(policy.${node.source_table}, "${originalColumn}") is invalid for table "${currentTable}": ` +
              `column is not a foreign key reference on source table. Available FK columns: ${available}.`,
          );
        }
        node.via_column = resolved;
        const referenced = sourceFks.get(resolved);
        if (referenced !== currentTable) {
          throw new Error(
            `allowedTo.${node.operation.toLowerCase()}Referencing(policy.${node.source_table}, "${originalColumn}") is invalid for table "${currentTable}": ` +
              `source FK references "${referenced}" but this rule is for "${currentTable}".`,
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

function isSessionWhereCondition(input: unknown): input is SessionWhereCondition {
  return (
    isPlainObject(input) &&
    input.__jazzPermissionKind === "session-where" &&
    isPlainObject(input.where)
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

function isExistsRelationCondition(input: unknown): input is ExistsRelationCondition {
  return (
    isPlainObject(input) &&
    input.__jazzPermissionKind === "exists-relation" &&
    isPlainObject(input.relation)
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

function isRecursiveCurrentValue(input: unknown): input is RecursiveCurrentValue {
  return isPlainObject(input) && input.__jazzPermissionKind === "recursive-current";
}

function isUpdateRuleBuilder(input: unknown): input is UpdateRuleBuilder<unknown, unknown> {
  return isPlainObject(input) && typeof input.toRule === "function";
}
