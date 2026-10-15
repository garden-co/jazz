// Narrowed exclusive reads of the sources a query reads beyond its root.
import { KeyedMap } from "../../../support/index.js";
import {
  type NodeState,
  type SourceId,
  nodeError,
  sourceIdKey,
  JazzQuery,
  ArraySubquery,
  relationUnionParts,
} from "../../../_extern/node-qe-a.js";
import {
  normalizedRowSetShape,
  includeAuxiliarySourceId,
  nestedJoinSourceId,
  correlatedChildSourceId,
  rootSourceId,
} from "./normalization.js";

export interface NarrowedSourceRead {
  shape: any;
  binding: any;
}

export interface ExclusiveSourceReads {
  reads: KeyedMap<SourceId, NarrowedSourceRead>;
  payload: KeyedMap<SourceId, true>;
}

class NarrowedSources {
  reads = new KeyedMap<SourceId, NarrowedSourceRead>(sourceIdKey);
  unsupported = new KeyedMap<SourceId, string>(sourceIdKey);
  unsupportedQuery: string | undefined;

  static unsupportedQuery(pattern: string): NarrowedSources {
    const n = new NarrowedSources();
    n.unsupportedQuery = pattern;
    return n;
  }

  unsupportedPattern(source: SourceId, root: string): string {
    return (
      this.unsupported.get(source) ??
      this.unsupportedQuery ??
      `\`${source.table}\` as source \`${describeSourcePath(source)}\` of a query on \`${root}\``
    );
  }
}

function describeSourcePath(source: SourceId): string {
  return source.path.components.map((c: any) => (c.kind === "Root" ? "root" : c.name)).join("/");
}

interface Correlation {
  childKey: string;
  parentKey: string;
  extra: [string, string][];
  membership: boolean;
}

interface Link {
  parentTable: string;
  parentFilters: any[];
  correlation: Correlation;
}

class CorrelationParent {
  constructor(
    readonly table: string,
    readonly filters: any[],
    readonly links: Link[],
  ) {}

  child(
    table: string,
    filters: any[],
    correlation: Correlation | undefined,
  ): [any, CorrelationParent] {
    const links = correlation
      ? [{ parentTable: this.table, parentFilters: this.filters, correlation }, ...this.links]
      : [];
    const parent = new CorrelationParent(table, filters, links);
    return [parent.narrowedRead(), parent];
  }

  narrowedRead(): any {
    const query = JazzQuery.from(this.table);
    query.filters = this.filters.slice();
    if (this.links.some((l) => l.correlation.membership)) {
      let up: any;
      for (const link of [...this.links].reverse()) {
        const c = link.correlation;
        if (c.extra.length) return undefined;
        const hop = ArraySubquery.new("narrowed_parent", link.parentTable, c.parentKey, c.childKey);
        hop.filters = link.parentFilters.slice();
        hop.requirement = "AtLeastOne";
        hop.nestedArrays = up ? [up] : [];
        up = hop;
      }
      query.arraySubqueries = up ? [up] : [];
    } else {
      let up: any;
      for (const link of [...this.links].reverse()) {
        const c = link.correlation;
        up = {
          table: link.parentTable,
          target: c.parentKey === "id" ? "RowId" : "Column",
          onColumn: c.parentKey,
          sourceColumn: c.childKey,
          sourceLookup: undefined,
          correlatedFilters: c.extra.map(([child, parent]) => ({
            joinColumn: parent,
            sourceColumn: child,
          })),
          filters: link.parentFilters.slice(),
          nestedJoins: up ? [up] : [],
        };
      }
      query.joins = up ? [up] : [];
    }
    return query;
  }
}

function bindPredicate(predicate: any, values: Map<string, any>): any {
  const bound = structuredClone(predicate);
  let complete = true;
  bound.forEachOperandMut((operand: any) => {
    if (operand.kind === "Param") {
      const v = values.get(operand.name);
      if (v !== undefined) Object.assign(operand, { kind: "Literal", value: v });
      else complete = false;
    }
  });
  return complete ? bound : undefined;
}

function bindPredicates(predicates: any[], values: Map<string, any>): any[] | undefined {
  const out: any[] = [];
  for (const p of predicates) {
    const b = bindPredicate(p, values);
    if (b === undefined) return undefined;
    out.push(b);
  }
  return out;
}

function isArray(columnType: any): boolean {
  if (columnType.kind === "Array") return true;
  if (columnType.kind === "Nullable") return isArray(columnType.inner);
  return false;
}

export function exclusiveSourceReads(
  st: NodeState,
  shape: any,
  binding: any,
  includeDeleted: boolean,
): ExclusiveSourceReads {
  const normalized = normalizedRowSetShape(st, shape, binding);
  const payload = new KeyedMap<SourceId, true>(sourceIdKey);
  for (const path of normalized.closurePaths) {
    if (path.kind === "ImplicitRootReference") payload.set(path.segment.target, true);
  }
  const sources = new KeyedMap<SourceId, SourceId>(sourceIdKey);
  const candidates: SourceId[] = [];
  for (const node of normalized.nodes.values())
    if (node.kind === "Source") candidates.push(node.source);
  candidates.push(...normalized.auxiliarySources);
  for (const s of candidates) {
    const isRoot = s.path.components.length === 1 && s.path.components[0].kind === "Root";
    if (!isRoot && !payload.has(s)) sources.set(s, s);
  }
  const ordered = [...sources]
    .map(([, s]) => s)
    .sort((a, b) => (sourceIdKey(a) < sourceIdKey(b) ? -1 : 1));
  const first = ordered[0];
  if (!first) return { reads: new KeyedMap(sourceIdKey), payload };
  const root = shape.query().table;
  const narrowed = includeDeleted
    ? NarrowedSources.unsupportedQuery(`deleted \`${root}\` rows together with \`${first.table}\``)
    : narrowedSourceReads(st, shape, binding);
  const missing = ordered.find((s) => !narrowed.reads.has(s));
  if (missing)
    throw nodeError("UnsupportedExclusiveRead", narrowed.unsupportedPattern(missing, root));
  return { reads: narrowed.reads, payload };
}

function narrowedSourceReads(st: NodeState, shape: any, binding: any): NarrowedSources {
  const query = shape.query();
  const rootTable = query.table;
  if (query.relation) {
    return NarrowedSources.unsupportedQuery(
      relationUnionParts(query.relation.rel) !== undefined
        ? `a union of relations over \`${rootTable}\``
        : `a relation over \`${rootTable}\` that projects selected columns`,
    );
  }
  if (query.flatJoin) {
    const tables = query.flatJoin.sources.map((s: any) => `\`${s.table}\``).join(", ");
    return NarrowedSources.unsupportedQuery(`a flat join of \`${rootTable}\` with ${tables}`);
  }
  if (query.policyBranches.length) {
    return NarrowedSources.unsupportedQuery(`\`${rootTable}\` through policy branches`);
  }
  const reachable = query.reachable[0];
  if (reachable) {
    return NarrowedSources.unsupportedQuery(
      `\`${rootTable}\` through a recursive traversal of \`${reachable.edgeTable}\``,
    );
  }
  const inherits = query.inherits[0];
  if (inherits) {
    const parent =
      st.tableInSchemaRef(rootTable, shape.schemaVersion()).references.get(inherits.parentColumn) ??
      inherits.parentColumn;
    const access = (
      { Select: "read", Insert: "insert", Update: "update", Delete: "delete" } as any
    )[inherits.operation];
    return NarrowedSources.unsupportedQuery(
      `\`${rootTable}\` inheriting ${access} access from \`${parent}\``,
    );
  }
  const values: Map<string, any> = binding.values();
  const rootFilters = bindPredicates(query.filters, values);
  if (!rootFilters) {
    return NarrowedSources.unsupportedQuery(
      `\`${rootTable}\` with a filter whose parameter is not bound`,
    );
  }
  const root = new CorrelationParent(query.table, rootFilters, []);
  const candidates: [SourceId, any][] = [];
  const narrowed = new NarrowedSources();
  const schemaVersion = shape.schemaVersion();
  query.joins.forEach((join: any, index: number) =>
    narrowJoin(
      st,
      root,
      join,
      `join_via:${index}`,
      schemaVersion,
      values,
      candidates,
      narrowed.unsupported,
    ),
  );
  query.includes.forEach((include: any, includeIndex: number) => {
    let parent = root;
    const segments: string[] = include.path.split(".");
    for (let segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
      const segment = segments[segmentIndex];
      const target = st.tableInSchemaRef(parent.table, schemaVersion).references.get(segment);
      if (target === undefined) break;
      const c = correlation(st, target, "id", parent.table, segment, schemaVersion);
      const [read, child] = parent.child(target, [], c);
      pushCandidate(
        includeAuxiliarySourceId(target, includeIndex, segmentIndex),
        read,
        candidates,
        narrowed.unsupported,
      );
      parent = child;
    }
  });
  const rootSource = rootSourceId(query.table);
  query.arraySubqueries.forEach((sub: any, index: number) =>
    narrowArraySubquery(
      st,
      root,
      rootSource,
      sub,
      [index],
      schemaVersion,
      values,
      candidates,
      narrowed.unsupported,
    ),
  );
  const entry = st.catalogue.catalogueSchemas.get(schemaVersion);
  if (!entry) throw nodeError("InvalidStoredValue", "query schema version is unknown");
  for (const [source, read] of candidates) {
    try {
      const shape = read.validate(entry.schema);
      const binding = shape.bind(new Map());
      narrowed.reads.set(source, { shape, binding });
    } catch (error) {
      narrowed.unsupported.set(
        source,
        `\`${source.table}\` correlated with \`${rootTable}\` (${error})`,
      );
    }
  }
  return narrowed;
}

function correlation(
  st: NodeState,
  childTable: string,
  childKey: string,
  parentTable: string,
  parentKey: string,
  schemaVersion: any,
): Correlation {
  return {
    childKey,
    parentKey,
    extra: [],
    membership:
      columnIsArray(st, childTable, childKey, schemaVersion) ||
      columnIsArray(st, parentTable, parentKey, schemaVersion),
  };
}

function columnIsArray(st: NodeState, table: string, column: string, schemaVersion: any): boolean {
  try {
    const t = st.tableInSchemaRef(table, schemaVersion);
    const c = t.columns.find((c: any) => c.name === column);
    return !!c && isArray(c.columnType);
  } catch {
    return false;
  }
}

function narrowJoin(
  st: NodeState,
  parent: CorrelationParent,
  join: any,
  path: string,
  schemaVersion: any,
  values: Map<string, any>,
  candidates: [SourceId, any][],
  unsupported: KeyedMap<SourceId, string>,
): void {
  if (join.sourceLookup !== undefined) {
    declineJoin(
      join,
      path,
      `\`${join.table}\` through a lookup join from \`${parent.table}\``,
      unsupported,
    );
    return;
  }
  const filters = bindPredicates(join.filters, values);
  if (!filters) {
    declineJoin(
      join,
      path,
      `\`${join.table}\` joined to \`${parent.table}\` with a filter whose parameter is not bound`,
      unsupported,
    );
    return;
  }
  let c: Correlation | undefined;
  if (join.target !== "Uncorrelated") {
    const childKey = join.target === "RowId" ? "id" : join.onColumn;
    c = correlation(
      st,
      join.table,
      childKey,
      parent.table,
      join.sourceColumn ?? "id",
      schemaVersion,
    );
    c.extra = join.correlatedFilters.map((cf: any) => [cf.joinColumn, cf.sourceColumn]);
  }
  const [read, child] = parent.child(join.table, filters, c);
  pushCandidate(nestedJoinSourceId(join, path), read, candidates, unsupported);
  join.nestedJoins.forEach((nested: any, index: number) =>
    narrowJoin(
      st,
      child,
      nested,
      `${path}:nested:${index}`,
      schemaVersion,
      values,
      candidates,
      unsupported,
    ),
  );
}

function declineJoin(
  join: any,
  path: string,
  pattern: string,
  unsupported: KeyedMap<SourceId, string>,
): void {
  unsupported.set(nestedJoinSourceId(join, path), pattern);
  join.nestedJoins.forEach((nested: any, index: number) =>
    declineJoin(nested, `${path}:nested:${index}`, pattern, unsupported),
  );
}

function narrowArraySubquery(
  st: NodeState,
  parent: CorrelationParent,
  owner: SourceId,
  subquery: any,
  path: number[],
  schemaVersion: any,
  values: Map<string, any>,
  candidates: [SourceId, any][],
  unsupported: KeyedMap<SourceId, string>,
): void {
  const source = correlatedChildSourceId(owner, subquery, path);
  const filters = bindPredicates(subquery.filters, values);
  if (!filters) {
    unsupported.set(
      source,
      `\`${subquery.table}\` related to \`${parent.table}\` with a filter whose parameter is not bound`,
    );
    return;
  }
  const c = correlation(
    st,
    subquery.table,
    subquery.innerColumn,
    parent.table,
    subquery.outerColumn,
    schemaVersion,
  );
  const [read, child] = parent.child(subquery.table, filters, c);
  pushCandidate(source, read, candidates, unsupported);
  subquery.nestedArrays.forEach((nested: any, index: number) =>
    narrowArraySubquery(
      st,
      child,
      source,
      nested,
      [...path, index],
      schemaVersion,
      values,
      candidates,
      unsupported,
    ),
  );
}

function pushCandidate(
  source: SourceId,
  read: any,
  candidates: [SourceId, any][],
  unsupported: KeyedMap<SourceId, string>,
): void {
  if (read !== undefined) candidates.push([source, read]);
  else
    unsupported.set(
      source,
      `\`${source.table}\` through a reference array together with additional join keys`,
    );
}
