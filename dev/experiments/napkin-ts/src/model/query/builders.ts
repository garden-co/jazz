import type { JazzSchema, RuntimeSchema, SchemaVersionId } from "../../_extern/model-query.js";
import { jazzSchemaRuntime } from "../../_extern/model-query.js";
import { cloneQuery, type Query } from "./ast.js";
import {
  Aggregate,
  defaultMaxDepth,
  newAggregateQuery,
  newInclude,
  type ArraySubquery,
  type Include,
  type InheritsOperation,
  type JoinCorrelation,
  type JoinSourceLookup,
  type JoinTarget,
  type JoinVia,
  type Operand,
  type OrderDirection,
  type PolicyBranch,
  type Predicate,
  type RecursionBound,
} from "./policy_and_request_contracts.js";
import {
  validateQuery,
  validateQueryWithSchemaVersion,
  type ValidatedQuery,
} from "./validation.js";

/** A table participating in a query, optionally under a public alias. */
export interface TableRef {
  table: string;
  alias?: string;
}
export const table = (name: string, alias?: string): TableRef => ({ table: name, alias });
const toTableRef = (source: string | TableRef): TableRef =>
  typeof source === "string" ? { table: source } : source;

/** Configuration for recursively gathering rows through an edge relation. */
export class Gather {
  currentColumn?: string;
  hopColumn?: string;
  frontierColumnName?: string;
  filters: Predicate[] = [];
  bound: RecursionBound = defaultMaxDepth();
  constructor(readonly stepTable: string) {}
  static from(table: string): Gather {
    return new Gather(table);
  }
  whereCurrent(column: string): this {
    this.currentColumn = column;
    return this;
  }
  hopTo(column: string): this {
    this.hopColumn = column;
    return this;
  }
  frontierColumn(column: string): this {
    this.frontierColumnName = column;
    return this;
  }
  filter(predicate: Predicate): this {
    this.filters.push(predicate);
    return this;
  }
  maxDepth(depth: number): this {
    this.bound = { kind: "MaxDepth", depth };
    return this;
  }
  untilFixpoint(): this {
    this.bound = { kind: "Fixpoint" };
    return this;
  }
}

export function queryFrom(source: string | TableRef): Query {
  const { table, alias } = toTableRef(source);
  return {
    table,
    filters: [],
    joins: [],
    flat_join: alias !== undefined ? { root_alias: alias, sources: [] } : undefined,
    policy_branches: [],
    reachable: [],
    inherits: [],
    includes: [],
    array_subqueries: [],
    order_by: [],
    offset: 0,
  };
}

/** The row query an aggregate consumes; undefined for a non-aggregate query. */
export function aggregateInput(q: Query): Query | undefined {
  if (!q.aggregate) return undefined;
  return {
    ...cloneQuery(q),
    aggregate: undefined,
    select: undefined,
    order_by: [],
    limit: undefined,
    offset: 0,
  };
}

export function policyBranch(q: Query, branch: PolicyBranch): Query {
  q.policy_branches.push(branch);
  return q;
}

export function filter(q: Query, predicate: Predicate): Query {
  q.filters.push(predicate);
  return q;
}

export function flatJoin(q: Query, source: string | TableRef, left: string, right: string): Query {
  const { table, alias } = toTableRef(source);
  (q.flat_join ??= { sources: [] }).sources.push({ table, alias, on: { left, right } });
  return q;
}

export function gather(q: Query, g: Gather): Query {
  const rootTable = q.table;
  const [frontierColumn, accessTeamTarget]: [string, JoinTarget] =
    g.frontierColumnName !== undefined ? [g.frontierColumnName, "Column"] : ["id", "RowId"];
  if (g.currentColumn === undefined)
    throw new Error("Gather::where_current must be called before Query::gather");
  if (g.hopColumn === undefined)
    throw new Error("Gather::hop_to must be called before Query::gather");
  const seedFilters = q.filters;
  q.filters = [];
  q.reachable.push({
    access_table: rootTable,
    access_row_column: frontierColumn,
    access_team_column: frontierColumn,
    access_team_target: accessTeamTarget,
    from: { kind: "Literal", value: { kind: "Uuid", value: new Uint8Array(16) } },
    access_filters: [],
    edge_table: g.stepTable,
    edge_member_column: g.currentColumn,
    edge_parent_column: g.hopColumn,
    edge_filters: g.filters,
    bound: g.bound,
    seed: { table: rootTable, team_column: frontierColumn, filters: seedFilters },
  });
  return q;
}

/** Every `join_via*` builder variant collapses to this one with optional fields. */
export function joinVia(
  q: Query,
  table: string,
  onColumn: string,
  filters: Iterable<Predicate>,
  opts: {
    target?: JoinTarget;
    sourceColumn?: string;
    sourceLookup?: JoinSourceLookup;
    correlatedFilters?: Iterable<JoinCorrelation>;
    nestedJoins?: Iterable<JoinVia>;
  } = {},
): Query {
  q.joins.push({
    table,
    on_column: onColumn,
    target: opts.target ?? "Column",
    source_column: opts.sourceLookup ? opts.sourceLookup.value_column : opts.sourceColumn,
    source_lookup: opts.sourceLookup,
    correlated_filters: [...(opts.correlatedFilters ?? [])],
    filters: [...filters],
    nested_joins: [...(opts.nestedJoins ?? [])],
  });
  return q;
}

export const joinViaRowId = (
  q: Query,
  table: string,
  sourceColumn: string,
  filters: Iterable<Predicate>,
  correlatedFilters?: Iterable<JoinCorrelation>,
): Query => joinVia(q, table, "id", filters, { target: "RowId", sourceColumn, correlatedFilters });

export function reachableVia(
  q: Query,
  accessTable: string,
  accessRowColumn: string,
  accessTeamColumn: string,
  from: Operand,
  accessFilters: Iterable<Predicate>,
  edgeTable: string,
  edgeMemberColumn: string,
  edgeParentColumn: string,
  edgeFilters: Iterable<Predicate>,
): Query {
  q.reachable.push({
    access_table: accessTable,
    access_row_column: accessRowColumn,
    access_team_column: accessTeamColumn,
    access_team_target: "Column",
    from,
    access_filters: [...accessFilters],
    edge_table: edgeTable,
    edge_member_column: edgeMemberColumn,
    edge_parent_column: edgeParentColumn,
    edge_filters: [...edgeFilters],
    bound: defaultMaxDepth(),
  });
  return q;
}

export function seededBy(
  q: Query,
  seedTable: string,
  userColumn: string,
  claimPath: string,
  teamColumn: string,
): Query {
  const reachable = q.reachable[q.reachable.length - 1];
  if (!reachable) throw new Error("seeded_by requires a preceding reachable_via traversal");
  reachable.seed = {
    table: seedTable,
    user_column: userColumn,
    user_claim: claimPath,
    team_column: teamColumn,
    filters: [],
  };
  return q;
}

export function inherits(
  q: Query,
  parentColumn: string,
  operation: InheritsOperation = "Select",
  maxDepth?: number,
): Query {
  q.inherits.push({ parent_column: parentColumn, operation, max_depth: maxDepth });
  return q;
}

export function include(q: Query, path: string | Include): Query {
  q.includes.push(typeof path === "string" ? newInclude(path) : path);
  return q;
}

export function arraySubquery(q: Query, subquery: ArraySubquery): Query {
  q.array_subqueries.push(subquery);
  return q;
}

export function select(q: Query, columns: Iterable<string>): Query {
  q.select = [...columns];
  return q;
}

export function orderBy(q: Query, column: string, direction: OrderDirection): Query {
  q.order_by.push({ column, direction });
  return q;
}

export function aggregate(q: Query, aggregates: Iterable<Aggregate>): Query {
  q.aggregate = newAggregateQuery(aggregates);
  return q;
}
export const count = (q: Query) => aggregate(q, [Aggregate.count()]);
export const sum = (q: Query, column: string) => aggregate(q, [Aggregate.sum(column)]);
export const avg = (q: Query, column: string) => aggregate(q, [Aggregate.avg(column)]);
export const min = (q: Query, column: string) => aggregate(q, [Aggregate.min(column)]);
export const max = (q: Query, column: string) => aggregate(q, [Aggregate.max(column)]);

export function groupBy(q: Query, column: string): Query {
  (q.aggregate ??= newAggregateQuery([Aggregate.count()])).group_by = column;
  return q;
}

export function limit(q: Query, n: number): Query {
  q.limit = n;
  return q;
}

export function offset(q: Query, n: number): Query {
  q.offset = n;
  return q;
}

export const validate = (q: Query, schema: JazzSchema): ValidatedQuery =>
  validateQuery(q, jazzSchemaRuntime(schema));
export const validateRuntime = (q: Query, schema: RuntimeSchema): ValidatedQuery =>
  validateQuery(q, schema);
export const validateWithSchemaVersion = (
  q: Query,
  schema: RuntimeSchema,
  schemaVersion: SchemaVersionId,
): ValidatedQuery => validateQueryWithSchemaVersion(q, schema, schemaVersion);
