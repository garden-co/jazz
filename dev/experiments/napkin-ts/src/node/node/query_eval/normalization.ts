// Normalize validated Jazz queries and policy clauses into the query engine's
// logical row-set representation.
import {
  SortedMap,
  SortedSet,
  cmpStr,
  Writer,
  uuidToString,
  uuidFromString,
  blake3,
  toHex,
} from "../../../support/index.js";
import {
  type NodeState,
  type SourceId,
  nodeError,
  cmpSourceId,
  encodeValue,
  Value,
  ColumnType,
  RowAuthor,
  relationUnionParts,
  relationOutputProjection,
  relationOutputProjectionIfPresent,
  relationScope,
  flatJoinPredicateSources,
  unqualifyFlatJoinPredicate,
  operandClaimPath,
  claimParamField,
  routeParamField,
  predicateParams,
  RecursionBound,
  columnTypeEq,
  debugFmt,
  queryError,
  LiteralValue,
  encodePredicate,
  encodeJoinVia,
  encodeReachableVia,
  encodeInheritsVia,
  normalizedSourceTables,
  authorizationQueryFromReadPolicy,
  bindScopeClaimOperands,
  disambiguatePolicyClaimParams,
  claimPathEq,
  PENDING_BINDING_SOURCE_SHAPE,
  bindingClaimParamsForShape,
  queryBindingSourceShapeForParts,
  retargetBindingValueSources,
  relationQueryToQuery,
  validateUnionArmWithSchemaVersion,
} from "../../../_extern/node-qe-a.js";
import { factorPolicyBranches } from "./policy_factoring.js";

export type RowSetExpr = any;
export type NodeMap = SortedMap<string, RowSetExpr>;
type Pred = any;
type ValueRef = any;

const Root = { kind: "Root" } as const;
const alias = (name: string) => ({ kind: "Alias", name });
const sid = (table: string, ...components: any[]): SourceId => ({ table, path: { components } });
const rowIdOf = (source: SourceId): ValueRef => ({
  kind: "RowId",
  rowId: { kind: "Source", source },
});
const sourceField = (source: SourceId, field: string): ValueRef => ({
  kind: "SourceField",
  source,
  field,
});
const frontierColumn = (frontier: string, field: string): ValueRef => ({
  kind: "FrontierColumn",
  frontier,
  field,
});
const compare = (left: ValueRef, op: string, right: ValueRef): Pred => ({
  kind: "Compare",
  left,
  op,
  right,
});
const sourceNode = (source: SourceId): RowSetExpr => ({
  kind: "Source",
  source,
  visibility: "Visible",
});
const lowering = (message: string) => nodeError("QueryLowering", message);
const capability = (message: string) => nodeError("QueryCapability", message);

export function rootSourceId(table: string): SourceId {
  return sid(table, Root);
}

/** Accumulators shared by every normalization step of one shape. */
export interface NormalizeAcc {
  nodes: NodeMap;
  auxiliarySources: SortedSet<SourceId>;
  closurePaths: any[];
  joinContributions: any[];
  inheritedContributions: any[];
  reachableContributions: any[];
}

export function newAcc(): NormalizeAcc {
  return {
    nodes: new SortedMap(cmpStr),
    auxiliarySources: new SortedSet(cmpSourceId),
    closurePaths: [],
    joinContributions: [],
    inheritedContributions: [],
    reachableContributions: [],
  };
}

const NODE_INPUT_FIELDS: Record<string, string[]> = {
  Filter: ["input"],
  Join: ["left", "right"],
  RecursiveRelation: ["seed", "step"],
  Distinct: ["input"],
  Project: ["input"],
  CorrelatedPathProjection: ["input", "childInput"],
  OrderBy: ["input"],
  Slice: ["input"],
  Aggregate: ["input"],
};

/** Move one independently-normalized relation arm into the union program. */
function prefixNormalizedRelationArm(normalized: any, prefix: string, dest: NormalizeAcc): void {
  const ids = new Map<string, string>();
  for (const id of normalized.nodes.keysArray()) ids.set(id, `${prefix}:${id}`);
  const map = (id: string) => {
    const m = ids.get(id);
    if (m === undefined) throw new Error("normal relation arm references a node in its own DAG");
    return m;
  };
  const rewritten = new SortedMap<string, RowSetExpr>(cmpStr);
  for (const [id, expression] of normalized.nodes) {
    let e = { ...expression };
    if (e.kind === "Union")
      e.inputs = e.inputs.map((i: any) => ({ node: map(i.node), label: i.label }));
    for (const f of NODE_INPUT_FIELDS[e.kind] ?? []) e[f] = map(e[f]);
    rewritten.set(map(id), e);
  }
  normalized.nodes = new SortedMap(cmpStr);
  for (const id of dest.nodes.keysArray()) {
    if (rewritten.has(id)) throw capability("relation UNION arm node ids collide after prefixing");
  }
  for (const [k, v] of rewritten) dest.nodes.set(k, v);
  normalized.root = map(normalized.root);
  for (const s of normalized.auxiliarySources) dest.auxiliarySources.add(s);
  for (const path of normalized.closurePaths) path.id = `${prefix}:${path.id}`;
  dest.closurePaths.push(...normalized.closurePaths);
  for (const c of normalized.joinContributions) {
    c.id = `${prefix}:${c.id}`;
    c.input = map(c.input);
  }
  dest.joinContributions.push(...normalized.joinContributions);
  for (const c of normalized.inheritedContributions) {
    c.id = `${prefix}:${c.id}`;
    c.input = map(c.input);
  }
  dest.inheritedContributions.push(...normalized.inheritedContributions);
  for (const c of normalized.reachableContributions) {
    c.id = `${prefix}:${c.id}`;
    c.accessInput = map(c.accessInput);
  }
  dest.reachableContributions.push(...normalized.reachableContributions);
}

export function nestedJoinSourceId(join: any, path: string): SourceId {
  return sid(join.table, alias(path));
}

function joinLookupSourceId(lookup: any, path: string): SourceId {
  return sid(lookup.table, alias(`${path}:source_lookup`));
}

export function currentQueryOutputRequest(output: string, query: any): any {
  const hasArrays = query.arraySubqueries.length > 0;
  const coverage = { kind: "ProgramSourceCoverage", scope: "Program" };
  let facts: any[];
  switch (output) {
    case "AuthorizedRows":
      facts = [{ kind: "AuthorizedRows" }];
      break;
    case "RelationSnapshot":
      facts = hasArrays ? [{ kind: "RelationEdges" }, { kind: "PathCorrelationCoverage" }] : [];
      break;
    case "MaintainedView":
      if (query.aggregate)
        facts = [{ kind: "VersionWitnesses" }, { kind: "ReplacementWitnesses" }, coverage];
      else if (hasArrays)
        facts = [
          { kind: "ResultMembership" },
          { kind: "VersionWitnesses" },
          { kind: "ReplacementWitnesses" },
          { kind: "RelationEdges" },
          coverage,
        ];
      else
        facts = [
          { kind: "ResultMembership" },
          { kind: "VersionWitnesses" },
          { kind: "ReplacementWitnesses" },
          coverage,
        ];
      break;
    default:
      facts = [];
  }
  const appRows = ["AppRows", "PolicyPredicate", "RelationSnapshot", "MaintainedView"].includes(
    output,
  )
    ? {
        publicTerminal: output !== "PolicyPredicate",
        projection: appRowPayloadProjection(
          query,
          output === "MaintainedView" || hasArrays,
          output === "AppRows" && !query.flatJoin && !hasArrays,
        ),
      }
    : undefined;
  return { appRows, facts };
}

/** Whether a maintained current-read can retain only its delivered result members. */
export function storageBackedMaintainedViewEligible(
  query: any,
  tier: string,
  readView: any,
  normalized: any,
): boolean {
  return (
    tier !== "None" &&
    readView.isDefault() &&
    !query.joins.length &&
    !query.flatJoin &&
    !query.reachable.length &&
    !query.inherits.length &&
    !query.includes.length &&
    !query.arraySubqueries.length &&
    !query.aggregate &&
    !query.relation &&
    !query.policyBranches.length &&
    !normalized.closurePaths.length &&
    !normalized.auxiliarySources.size &&
    !normalized.joinContributions.length &&
    !normalized.inheritedContributions.length &&
    !normalized.reachableContributions.length
  );
}

function appRowPayloadProjection(
  query: any,
  collectRelations: boolean,
  retainOrderKeys: boolean,
): any {
  let relationProjection: any;
  if (query.relation) {
    relationProjection =
      relationUnionParts(query.relation.rel) !== undefined
        ? relationOutputProjection(query.relation)[1]
        : relationOutputProjectionIfPresent(query.relation);
  }
  if (relationProjection !== undefined) {
    if (query.arraySubqueries.length || query.select) {
      throw capability("a relation output projection cannot carry include or select presentation");
    }
    return { kind: "Relation", columns: relationProjection };
  }
  const paths = collectRelations
    ? appRowPathProjections(rootSourceId(query.table), query.arraySubqueries, [])
    : [];
  if (!query.select && !paths.length) return { kind: "ShapeDefault" };
  let fields: any = { kind: "All" };
  if (query.select) {
    const set = new Set<string>(query.select.filter((f: string) => f !== "id"));
    for (const include of query.includes) set.add(include.path.split(".")[0]);
    if (retainOrderKeys)
      for (const order of query.orderBy) if (order.column !== "id") set.add(order.column);
    fields = { kind: "Fields", fields: [...set].sort() };
  }
  return { kind: "Tree", tree: { fields, paths } };
}

function appRowPathProjections(owner: SourceId, subqueries: any[], path: number[]): any[] {
  return subqueries.map((subquery, index) => {
    const childPath = [...path, index];
    const child = correlatedChildSourceId(owner, subquery, childPath);
    return {
      path: { owner, child },
      field: subquery.columnName,
      cardinality: "Many",
      fields: subquery.select
        ? {
            kind: "Fields",
            fields: [...new Set<string>(subquery.select.filter((f: string) => f !== "id"))].sort(),
          }
        : { kind: "All" },
      children: appRowPathProjections(child, subquery.nestedArrays, childPath),
      holePolicy: "KeepParentWithHoles",
    };
  });
}

export function requiredFieldIdx(descriptor: any, field: string): number {
  const idx = descriptor.fieldIndex(field);
  if (idx === undefined)
    throw lowering(`query-engine relation snapshot sink did not emit field '${field}'`);
  return idx;
}

function normalizePredicates(
  schema: any,
  source: SourceId,
  predicates: any[],
  flatAlias: boolean,
): Pred {
  if (!predicates.length) return { kind: "True" };
  if (predicates.length === 1) return normalizePredicate(schema, source, predicates[0], flatAlias);
  return {
    kind: "And",
    items: predicates.map((p) => normalizePredicate(schema, source, p, flatAlias)),
  };
}

function routeFlatJoinFilters(predicate: any, rootScope: string, routed: Map<string, any[]>): void {
  if (predicate.kind === "All") {
    for (const p of predicate.items) routeFlatJoinFilters(p, rootScope, routed);
    return;
  }
  const sources: string[] = [...flatJoinPredicateSources(predicate)].sort();
  const scope = sources[0] ?? rootScope;
  let list = routed.get(scope);
  if (!list) routed.set(scope, (list = []));
  list.push(unqualifyFlatJoinPredicate(predicate, scope));
}

function flatJoinFiltersBySource(filters: any[], rootScope: string): Map<string, any[]> {
  const routed = new Map<string, any[]>();
  for (const p of filters) routeFlatJoinFilters(p, rootScope, routed);
  return routed;
}

export function rootLiteralEqualities(query: any, binding: any): Map<string, any> {
  return literalEqualitiesForFilters(query.filters, binding);
}

export function literalEqualitiesForFilters(filters: any[], binding: any): Map<string, any> {
  const eq = new Map<string, any>();
  for (const p of filters) collectRootLiteralEqualities(p, binding, eq);
  return eq;
}

function collectRootLiteralEqualities(predicate: any, binding: any, eq: Map<string, any>): void {
  if (predicate.kind === "All") {
    for (const p of predicate.items) collectRootLiteralEqualities(p, binding, eq);
  } else if (predicate.kind === "Eq") {
    const hit =
      rootEqualityLiteral(predicate.left, predicate.right, binding) ??
      rootEqualityLiteral(predicate.right, predicate.left, binding);
    if (hit && !eq.has(hit[0])) eq.set(hit[0], hit[1]);
  }
}

function rootEqualityLiteral(field: any, value: any, binding: any): [string, any] | undefined {
  if (field.kind !== "Column") return undefined;
  let v;
  switch (value.kind) {
    case "Literal":
      v = value.value;
      break;
    case "Param":
      v = binding.values().get(value.name);
      if (v === undefined) throw queryError("MissingParam", value.name);
      break;
    default:
      return undefined;
  }
  return [field.name, v];
}

export function selectCurrentAccessPath(table: any, equalities: Map<string, any>): any {
  const id = equalities.get("id");
  if (id !== undefined) return { kind: "PrimaryKey", values: [id] };
  const probes: [string, any[]][] = [];
  for (const column of table.globalCurrentIndexedColumns()) {
    const v = equalities.get(column);
    if (v !== undefined) probes.push([column, [physicalCurrentIndexValue(table, column, v)]]);
  }
  if (!probes.length) return undefined;
  const [column, prefix] = probes[0];
  return {
    kind: "Index",
    column,
    orderColumn: undefined,
    reverse: false,
    prefix,
    intersections: probes.slice(1),
    maintained: false,
    candidateFilter: undefined,
    sourceLimit: undefined,
  };
}

/** Select a declared two-column composite index `[first, covered]` by an equality on `first`. */
export function selectCompositeLeadingEqualityAccessPath(
  table: any,
  equalities: Map<string, any>,
  covered: string,
): any {
  for (const columns of table.compositeIndexes) {
    if (columns.length !== 2) continue;
    const [first, second] = columns;
    if (second !== covered || first === "id") continue;
    const value = equalities.get(first);
    if (value === undefined) continue;
    return {
      kind: "Index",
      column: first,
      orderColumn: second,
      reverse: false,
      prefix: [physicalCurrentIndexValue(table, first, value)],
      intersections: [],
      maintained: false,
      candidateFilter: undefined,
      sourceLimit: undefined,
    };
  }
  return undefined;
}

export function physicalCurrentIndexValue(table: any, column: string, value: any): any {
  const candidate = table.columns.find((c: any) => c.name === column);
  const logical = candidate ? coerceLiteralPreservingNullable(value, candidate.columnType) : value;
  return Value.Nullable(logical);
}

function coerceLiteralPreservingNullable(value: any, columnType: any): any {
  if (columnType.kind === "Nullable") {
    if (value.kind === "Nullable") {
      return Value.Nullable(
        value.value === undefined
          ? undefined
          : coerceLiteralPreservingNullable(value.value, columnType.inner),
      );
    }
    return Value.Nullable(coerceLiteralPreservingNullable(value, columnType.inner));
  }
  return coerceLiteralForColumnType(value, columnType);
}

export function staticScanForPrefix(prefix: any[], fullKeyLen: number): any {
  const values = prefix.map((v) => LiteralValue.from(v));
  return values.length === fullKeyLen ? { kind: "Point", values } : { kind: "Prefix", values };
}

const COMPARE_OPS: Record<string, string> = {
  Eq: "Eq",
  Ne: "Ne",
  Gt: "Gt",
  Gte: "Gte",
  Lt: "Lt",
  Lte: "Lte",
};

function normalizePredicate(
  schema: any,
  source: SourceId,
  predicate: any,
  flatAlias: boolean,
): Pred {
  switch (predicate.kind) {
    case "All":
      return {
        kind: "And",
        items: predicate.items.map((p: any) => normalizePredicate(schema, source, p, flatAlias)),
      };
    case "Any":
      return {
        kind: "Or",
        items: predicate.items.map((p: any) => normalizePredicate(schema, source, p, flatAlias)),
      };
    case "Not":
      return { kind: "Not", inner: normalizePredicate(schema, source, predicate.inner, flatAlias) };
    case "Eq":
    case "Ne":
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte":
      return normalizeCompare(
        schema,
        source,
        predicate.left,
        COMPARE_OPS[predicate.kind],
        predicate.right,
        flatAlias,
      );
    case "In": {
      return {
        kind: "In",
        value: normalizeOperandFull(source, predicate.value, undefined, flatAlias),
        options: predicate.options.map((o: any) =>
          normalizeOperandFull(
            source,
            o,
            predicateOperandColumnType(schema, source, predicate.value, flatAlias),
            flatAlias,
          ),
        ),
      };
    }
    case "Contains":
      return {
        kind: "ArrayContains",
        value: normalizeOperandFull(source, predicate.value, undefined, flatAlias),
        needle: normalizeOperandFull(
          source,
          predicate.needle,
          containsNeedleType(schema, source, predicate.value),
          flatAlias,
        ),
      };
    case "IsNull":
      return {
        kind: "IsNull",
        value: normalizeOperandFull(source, predicate.value, undefined, flatAlias),
      };
    case "EnumMatch": {
      let columnType = operandColumnType(schema, source, {
        kind: "Column",
        name: predicate.column,
      });
      if (!columnType) throw lowering("enum match column has no type");
      if (columnType.kind === "Nullable") columnType = columnType.inner;
      if (columnType.kind !== "Enum") throw lowering("enum match requires a payload enum column");
      let caseTag, enumCase;
      try {
        caseTag = columnType.schema.tag(predicate.case);
        enumCase = columnType.schema.case(caseTag);
      } catch {
        throw lowering(`unknown payload enum case ${predicate.case}`);
      }
      return {
        kind: "EnumMatch",
        value: normalizeOperand(source, { kind: "Column", name: predicate.column }),
        caseTag,
        payload: normalizeEnumPayloadPredicate(enumCase.payload, source, predicate.payload),
      };
    }
  }
  throw lowering(`unsupported predicate ${predicate.kind}`);
}

/** Normalize a predicate evaluated inside one selected payload-enum case. */
export function normalizeEnumPayloadPredicate(
  descriptor: any,
  source: SourceId,
  predicate: any,
): Pred {
  switch (predicate.kind) {
    case "All":
      return {
        kind: "And",
        items: predicate.items.map((p: any) =>
          normalizeEnumPayloadPredicate(descriptor, source, p),
        ),
      };
    case "Any":
      return {
        kind: "Or",
        items: predicate.items.map((p: any) =>
          normalizeEnumPayloadPredicate(descriptor, source, p),
        ),
      };
    case "Not":
      return {
        kind: "Not",
        inner: normalizeEnumPayloadPredicate(descriptor, source, predicate.inner),
      };
    case "Eq":
    case "Ne":
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte": {
      const lt = enumPayloadOperandType(descriptor, predicate.left);
      const rt = enumPayloadOperandType(descriptor, predicate.right);
      return compare(
        normalizeEnumPayloadOperand(descriptor, source, predicate.left, rt),
        COMPARE_OPS[predicate.kind],
        normalizeEnumPayloadOperand(descriptor, source, predicate.right, lt),
      );
    }
    case "In": {
      const target = enumPayloadOperandType(descriptor, predicate.value);
      return {
        kind: "In",
        value: normalizeEnumPayloadOperand(descriptor, source, predicate.value, undefined),
        options: predicate.options.map((o: any) =>
          normalizeEnumPayloadOperand(descriptor, source, o, target),
        ),
      };
    }
    case "Contains": {
      const needleType = needleTypeOf(enumPayloadOperandType(descriptor, predicate.value));
      return {
        kind: "ArrayContains",
        value: normalizeEnumPayloadOperand(descriptor, source, predicate.value, undefined),
        needle: normalizeEnumPayloadOperand(descriptor, source, predicate.needle, needleType),
      };
    }
    case "IsNull":
      return {
        kind: "IsNull",
        value: normalizeEnumPayloadOperand(descriptor, source, predicate.value, undefined),
      };
    case "EnumMatch":
      throw lowering("nested payload enum matches are not supported");
  }
  throw lowering(`unsupported predicate ${predicate.kind}`);
}

function normalizeEnumPayloadOperand(
  descriptor: any,
  source: SourceId,
  operand: any,
  target: any,
): ValueRef {
  switch (operand.kind) {
    case "Column":
      if (enumPayloadFieldType(descriptor, operand.name) === undefined) {
        throw lowering(`unknown payload enum field ${operand.name}`);
      }
      return sourceField(source, operand.name);
    case "Param":
      return { kind: "Param", name: operand.name };
    case "Claim":
      return { kind: "Claim", path: operandClaimPath(operand.claim) };
    default:
      return literalValueRef(
        target ? coerceLiteralForColumnType(operand.value, target) : operand.value,
      );
  }
}

function enumPayloadOperandType(descriptor: any, operand: any): any {
  if (operand.kind !== "Column") return undefined;
  const t = enumPayloadFieldType(descriptor, operand.name);
  if (t === undefined) throw lowering(`unknown payload enum field ${operand.name}`);
  return t;
}

function enumPayloadFieldType(descriptor: any, field: string): any {
  return descriptor.fields().find((c: any) => c.name === field)?.valueType;
}

function needleTypeOf(t: any): any {
  if (!t) return undefined;
  if (t.kind === "Array") return t.member;
  if (t.kind === "Nullable") {
    if (t.inner.kind === "Array") return t.inner.member;
    if (t.inner.kind === "String") return ColumnType.String;
    return undefined;
  }
  if (t.kind === "String") return ColumnType.String;
  return undefined;
}

function normalizeCompare(
  schema: any,
  source: SourceId,
  left: any,
  op: string,
  right: any,
  flatAlias: boolean,
): Pred {
  const lt = predicateOperandColumnType(schema, source, left, flatAlias);
  const rt = predicateOperandColumnType(schema, source, right, flatAlias);
  return compare(
    normalizeOperandFull(source, left, rt, flatAlias),
    op,
    normalizeOperandFull(source, right, lt, flatAlias),
  );
}

function normalizeOperand(source: SourceId, operand: any): ValueRef {
  return normalizeOperandFull(source, operand, undefined, false);
}

function normalizeOperandFull(
  source: SourceId,
  operand: any,
  target: any,
  flatAlias: boolean,
): ValueRef {
  switch (operand.kind) {
    case "Column": {
      const column: string = operand.name;
      if ((flatAlias && column === "_id") || column === "id") return rowIdOf(source);
      const prov = provenanceField(column);
      return prov ? { kind: "Provenance", source, field: prov } : sourceField(source, column);
    }
    case "Param":
      return { kind: "Param", name: operand.name };
    case "Claim":
      return { kind: "Claim", path: operandClaimPath(operand.claim) };
    default:
      return literalValueRef(
        target ? coerceLiteralForColumnType(operand.value, target) : operand.value,
      );
  }
}

function predicateOperandColumnType(
  schema: any,
  source: SourceId,
  operand: any,
  flatAlias: boolean,
): any {
  if (flatAlias && operand.kind === "Column" && operand.name === "_id") return ColumnType.Uuid;
  return operandColumnType(schema, source, operand);
}

export function operandColumnType(schema: any, source: SourceId, operand: any): any {
  if (operand.kind !== "Column") return undefined;
  const column: string = operand.name;
  const prov = provenanceField(column);
  if (prov)
    return prov === "CreatedAt" || prov === "UpdatedAt" ? ColumnType.U64 : RowAuthor.valueType();
  if (column === "id") return ColumnType.Uuid;
  return tableSchema(schema, source.table).columns.find((c: any) => c.name === column)?.columnType;
}

export function containsNeedleType(schema: any, source: SourceId, value: any): any {
  return needleTypeOf(operandColumnType(schema, source, value));
}

export function coerceLiteralForColumnType(value: any, columnType: any): any {
  if (value.kind === "Uuid" && columnType.kind === "String")
    return Value.String(uuidToString(value.value));
  if (value.kind === "String" && columnType.kind === "Uuid") {
    try {
      return Value.Uuid(uuidFromString(value.value));
    } catch {
      return value;
    }
  }
  if (value.kind === "Nullable" && value.value !== undefined && columnType.kind === "Nullable") {
    return Value.Nullable(coerceLiteralForColumnType(value.value, columnType.inner));
  }
  if (value.kind === "Array" && columnType.kind === "Array") {
    return Value.Array(
      value.value.map((v: any) => coerceLiteralForColumnType(v, columnType.member)),
    );
  }
  if (
    value.kind === "Tuple" &&
    columnType.kind === "Tuple" &&
    value.value.length === columnType.types.length
  ) {
    return Value.Tuple(
      value.value.map((v: any, i: number) => coerceLiteralForColumnType(v, columnType.types[i])),
    );
  }
  if (value.kind === "Nullable" && value.value !== undefined) {
    return Value.Nullable(coerceLiteralForColumnType(value.value, columnType));
  }
  if (columnType.kind === "Nullable") return coerceLiteralForColumnType(value, columnType.inner);
  return value;
}

function provenanceField(column: string): string | undefined {
  switch (column) {
    case "$createdAt":
      return "CreatedAt";
    case "$createdBy":
      return "CreatedBy";
    case "$updatedAt":
      return "UpdatedAt";
    case "$updatedBy":
      return "UpdatedBy";
  }
  return undefined;
}

function normalizeOrderKey(source: SourceId, order: any): any {
  return {
    value: normalizeOperand(source, { kind: "Column", name: order.column }),
    direction: order.direction === "Desc" ? "Desc" : "Asc",
  };
}

function normalizedAggregateGroupBy(source: SourceId, aggregate: any): ValueRef[] {
  return aggregate.groupBy.map((c: string) =>
    normalizeOperand(source, { kind: "Column", name: c }),
  );
}

function normalizedAggregateOutputs(source: SourceId, aggregate: any): any[] {
  return aggregate.aggregates.map((a: any) => ({
    output: typedOutputField(a.alias, normalizedAggregateOutputType(a)),
    function: a.function,
    input:
      a.column !== undefined
        ? normalizeOperand(source, { kind: "Column", name: a.column })
        : undefined,
  }));
}

function normalizedAggregateOutputType(aggregate: any): any {
  switch (aggregate.function) {
    case "Count":
      return ColumnType.U64;
    case "Avg":
      return ColumnType.Nullable(ColumnType.F64);
    default:
      return ColumnType.Nullable(ColumnType.Bytes);
  }
}

const normalizationGap = (message: string) => lowering(message);

export function correlatedChildSourceId(owner: SourceId, subquery: any, path: number[]): SourceId {
  return sid(subquery.table, ...owner.path.components, {
    kind: "CorrelatedChild",
    name: `${path.join(".")}:${subquery.columnName}`,
  });
}

/** The source an implicit root reference `column` reads from `table`. */
export function implicitReferenceSourceId(table: string, column: string): SourceId {
  return sid(table, Root, alias(`reference:${column}`));
}

export function includeAuxiliarySourceId(
  table: string,
  includeIndex: number,
  segmentIndex: number,
): SourceId {
  return sid(table, Root, alias(`include:${includeIndex}:${segmentIndex}`));
}

function collectClosurePaths(
  st: NodeState,
  rootTable: string,
  schemaVersion: any,
  includes: any[],
): [SourceId[], any[]] {
  const sources: SourceId[] = [];
  const paths: any[] = [];
  const rootSource = rootSourceId(rootTable);
  const rootSchema = st.tableInSchemaRef(rootTable, schemaVersion);
  const explicit = new Set(includes.map((i: any) => i.path.split(".")[0]));
  for (const [column, targetTable] of [...rootSchema.references].sort(([a], [b]) => cmpStr(a, b))) {
    if (explicit.has(column)) continue;
    const target = implicitReferenceSourceId(targetTable, column);
    sources.push(target);
    paths.push({
      kind: "ImplicitRootReference",
      id: `reference:${column}`,
      segment: { parent: rootSource, target, sourceField: column },
    });
  }
  includes.forEach((include: any, includeIndex: number) => {
    let currentTable = rootTable;
    let parent = rootSource;
    const segments: any[] = [];
    include.path.split(".").forEach((segment: string, segmentIndex: number) => {
      const targetTable = st.tableInSchemaRef(currentTable, schemaVersion).references.get(segment);
      if (targetTable === undefined)
        throw nodeError("InvalidStoredValue", "include path was not validated");
      const target = includeAuxiliarySourceId(targetTable, includeIndex, segmentIndex);
      sources.push(target);
      segments.push({ parent, target, sourceField: segment });
      parent = target;
      currentTable = targetTable;
    });
    paths.push({
      kind: "ExplicitInclude",
      id: `include:${includeIndex}:${include.path}`,
      segments,
      rootGate: include.require ? "Required" : include.joinMode === "Inner" ? "Inner" : undefined,
    });
  });
  return [sources, paths];
}

function normalizeArraySubquery(
  nodes: NodeMap,
  current: string,
  schema: any,
  ownerSource: SourceId,
  subquery: any,
  path: number[],
): string {
  const pathId = path.join(".");
  const childSource = correlatedChildSourceId(ownerSource, subquery, path);
  const gap = <T>(f: () => T): T => {
    try {
      return f();
    } catch (err) {
      throw normalizationGap(String(err));
    }
  };
  let child = `array_subquery:${pathId}:source`;
  nodes.set(child, sourceNode(childSource));
  if (subquery.filters.length) {
    const id = `array_subquery:${pathId}:filter`;
    nodes.set(id, {
      kind: "Filter",
      input: child,
      predicate: gap(() => normalizePredicates(schema, childSource, subquery.filters, false)),
    });
    child = id;
  }
  if (subquery.orderBy.length) {
    const id = `array_subquery:${pathId}:order`;
    nodes.set(id, {
      kind: "OrderBy",
      input: child,
      keys: gap(() => subquery.orderBy.map((o: any) => normalizeOrderKey(childSource, o))),
    });
    child = id;
  }
  if (subquery.limit !== undefined || subquery.offset !== 0) {
    const id = `array_subquery:${pathId}:slice`;
    nodes.set(id, {
      kind: "Slice",
      input: child,
      partitionBy: [sourceColumnValue(childSource, subquery.innerColumn, "Column")],
      limit: subquery.limit === undefined ? undefined : Math.min(subquery.limit, 0xffffffff),
      offset: Math.min(subquery.offset, 0xffffffff),
      tieBreaker: [rowIdOf(childSource)],
      rankOutput: undefined,
    });
    child = id;
  }
  const nestedParentInput = child;
  const pathNode = `array_subquery:${pathId}:path`;
  nodes.set(pathNode, {
    kind: "CorrelatedPathProjection",
    input: current,
    childInput: child,
    path: { owner: ownerSource, child: childSource },
    correlation: compare(
      sourceColumnValue(childSource, subquery.innerColumn, "Column"),
      "Eq",
      gap(() => normalizeOperand(ownerSource, { kind: "Column", name: subquery.outerColumn })),
    ),
    requirement: subquery.requirement,
  });
  subquery.nestedArrays.forEach((nested: any, i: number) =>
    normalizeArraySubquery(nodes, nestedParentInput, schema, childSource, nested, [...path, i]),
  );
  return pathNode;
}

function normalizeReachable(
  nodes: NodeMap,
  current: string,
  schema: any,
  rootSource: SourceId,
  reachable: any,
  index: number,
  prefix: string,
  bindingSourceShape: string,
  paramTypes: Map<string, any>,
): [string, any] {
  const rid = prefix ? `${prefix}:reachable:${index}` : `reachable:${index}`;
  const frontier = `${rid}:frontier`;
  const [seedNode, columns] = normalizeReachableSeed(
    nodes,
    schema,
    reachable,
    rid,
    bindingSourceShape,
    paramTypes,
  );
  const frontierNode = `${rid}:frontier`;
  nodes.set(frontierNode, { kind: "FrontierSource", frontier, columns });
  const edgeSource = reachableEdgeSourceId(reachable, rid);
  let edge = `${rid}:edge_source`;
  nodes.set(edge, sourceNode(edgeSource));
  if (reachable.edgeFilters.length) {
    const id = `${rid}:edge_filter`;
    nodes.set(id, {
      kind: "Filter",
      input: edge,
      predicate: normalizePredicates(schema, edgeSource, reachable.edgeFilters, false),
    });
    edge = id;
  }
  const stepJoin = `${rid}:step_join`;
  nodes.set(stepJoin, {
    kind: "Join",
    left: frontierNode,
    right: edge,
    mode: "Inner",
    on: compare(
      frontierColumn(frontier, "reachable_team"),
      "Eq",
      sourceColumnValue(edgeSource, reachable.edgeMemberColumn, "Column"),
    ),
  });
  const stepProject = `${rid}:step_project`;
  const stepColumns = [
    { output: typedOutputField("team", ColumnType.Uuid), value: frontierColumn(frontier, "team") },
    {
      output: typedOutputField("reachable_team", ColumnType.Uuid),
      value: sourceColumnValue(edgeSource, reachable.edgeParentColumn, "Column"),
    },
    ...columns
      .filter((c) => c.name !== "team" && c.name !== "reachable_team")
      .map((c) => ({
        output: typedOutputField(c.name, c.ty),
        value: frontierColumn(frontier, c.name),
      })),
  ];
  nodes.set(stepProject, { kind: "Project", input: stepJoin, columns: stepColumns });
  const closure = `${rid}:closure`;
  nodes.set(closure, {
    kind: "RecursiveRelation",
    seed: seedNode,
    step: stepProject,
    frontier,
    frontierKey: frontierColumn(frontier, "reachable_team"),
    dedupeKeys: reachableDedupeKeys(frontier, columns),
    bound: reachable.bound,
  });
  const accessSource = reachableAccessSourceId(reachable, rid);
  let access = `${rid}:access_source`;
  nodes.set(access, sourceNode(accessSource));
  if (reachable.accessFilters.length) {
    const id = `${rid}:access_filter`;
    nodes.set(id, {
      kind: "Filter",
      input: access,
      predicate: normalizePredicates(schema, accessSource, reachable.accessFilters, false),
    });
    access = id;
  }
  const accessJoin = `${rid}:access_join`;
  nodes.set(accessJoin, {
    kind: "Join",
    left: access,
    right: closure,
    mode: "Inner",
    on: compare(
      sourceColumnValue(accessSource, reachable.accessTeamColumn, reachable.accessTeamTarget),
      "Eq",
      frontierColumn(frontier, "reachable_team"),
    ),
  });
  const rootJoin = `${rid}:root_join`;
  nodes.set(rootJoin, {
    kind: "Join",
    left: current,
    right: accessJoin,
    mode: "Inner",
    on: compare(
      sourceColumnValue(rootSource, "id", "Column"),
      "Eq",
      sourceColumnValue(accessSource, reachable.accessRowColumn, "Column"),
    ),
  });
  return [
    rootJoin,
    {
      id: rid,
      accessSource,
      edgeSource,
      accessInput: accessJoin,
      rootRefField: reachable.accessRowColumn,
    },
  ];
}

export function sourceColumnValue(source: SourceId, column: string, target: string): ValueRef {
  return target === "RowId" || column === "id" ? rowIdOf(source) : sourceField(source, column);
}

function normalizeJoinViaRight(
  nodes: NodeMap,
  auxiliarySources: SortedSet<SourceId>,
  nestedContributions: any[],
  schema: any,
  join: any,
  path: string,
): [string, SourceId] {
  const joinSource = nestedJoinSourceId(join, path);
  auxiliarySources.add(joinSource);
  const table = tableSchema(schema, join.table);
  let current = `${path}:source`;
  nodes.set(current, sourceNode(joinSource));
  if (join.filters.length) {
    const id = `${path}:filter`;
    nodes.set(id, {
      kind: "Filter",
      input: current,
      predicate: normalizePredicates(schema, joinSource, join.filters, false),
    });
    current = id;
  }
  const lookup = join.sourceLookup;
  if (lookup) {
    const lookupSource = joinLookupSourceId(lookup, path);
    auxiliarySources.add(lookupSource);
    const lookupSourceNode = `${path}:lookup_source`;
    nodes.set(lookupSourceNode, sourceNode(lookupSource));
    const lookupJoin = `${path}:lookup_join`;
    nodes.set(lookupJoin, {
      kind: "Join",
      left: current,
      right: lookupSourceNode,
      mode: "Inner",
      on: compare(
        joinViaTargetKey(joinSource, join),
        "Eq",
        sourceColumnValue(lookupSource, lookup.valueColumn, "Column"),
      ),
    });
    const lookupProject = `${path}:lookup_project`;
    const columns = sourcePublicFieldProjections(table, joinSource);
    columns.push({
      output: typedOutputField(lookup.rowIdSourceColumn, ColumnType.Uuid),
      value: rowIdOf(lookupSource),
    });
    nodes.set(lookupProject, { kind: "Project", input: lookupJoin, columns });
    current = lookupProject;
  }
  join.nestedJoins.forEach((nested: any, i: number) => {
    const nestedPath = `${path}:nested:${i}`;
    const descendants: any[] = [];
    const [nestedRight, nestedSource] = normalizeJoinViaRight(
      nodes,
      auxiliarySources,
      descendants,
      schema,
      nested,
      nestedPath,
    );
    nestedContributions.push({
      parent: joinSource,
      id: nestedPath,
      source: nestedSource,
      input: nestedRight,
      membership: joinViaPredicate(joinSource, nestedSource, nested),
    });
    nestedContributions.push(...descendants);
    const nestedJoin = `${nestedPath}:join`;
    nodes.set(nestedJoin, {
      kind: "Join",
      left: current,
      right: nestedRight,
      mode: "Inner",
      on: joinViaPredicate(joinSource, nestedSource, nested),
    });
    const project = `${nestedPath}:parent_project`;
    nodes.set(project, {
      kind: "Project",
      input: nestedJoin,
      columns: sourcePublicFieldProjections(table, joinSource),
    });
    current = project;
  });
  return [current, joinSource];
}

function reachableDedupeKeys(frontier: string, columns: any[]): ValueRef[] {
  return [
    "reachable_team",
    ...columns.map((c) => c.name).filter((n) => n !== "team" && n !== "reachable_team"),
  ].map((f) => frontierColumn(frontier, f));
}

function requireUuidSeed(schema: any, seed: any): void {
  const ty = schemaColumnType(schema, seed.table, seed.teamColumn);
  if (!columnTypeEq(ty, ColumnType.Uuid)) {
    throw lowering(
      `reachable_via seed ${seed.table}.${seed.teamColumn} must be uuid, found ${debugFmt(ty)}`,
    );
  }
}

function normalizeReachableSeed(
  nodes: NodeMap,
  schema: any,
  reachable: any,
  rid: string,
  bindingSourceShape: string,
  paramTypes: Map<string, any>,
): [string, any[]] {
  const seed = reachable.seed;
  if (seed) {
    const seedSource = reachableSeedSourceId(seed, rid);
    const columns = reachableSeedFrontierColumns(schema, seedSource, seed);
    const edgeRouteColumns = reachableEdgeRouteColumns(reachable, paramTypes);
    for (const c of edgeRouteColumns) if (!columns.some((e) => e.name === c.name)) columns.push(c);
    const userColumnTy =
      seed.userColumn !== undefined
        ? schemaColumnType(schema, seed.table, seed.userColumn)
        : undefined;
    requireUuidSeed(schema, seed);
    let cur = `${rid}:seed_source`;
    nodes.set(cur, sourceNode(seedSource));
    const claimField =
      seed.userClaim !== undefined ? claimParamField(operandClaimPath(seed.userClaim)) : undefined;
    if (seed.userColumn !== undefined && claimField !== undefined) {
      const id = `${rid}:seed_claim_filter`;
      nodes.set(id, {
        kind: "Filter",
        input: cur,
        predicate: compare(sourceColumnValue(seedSource, seed.userColumn, "Column"), "Eq", {
          kind: "Param",
          name: claimField,
        }),
      });
      cur = id;
    }
    if (seed.filters.length) {
      const id = `${rid}:seed_filter`;
      nodes.set(id, {
        kind: "Filter",
        input: cur,
        predicate: normalizePredicates(schema, seedSource, seed.filters, false),
      });
      cur = id;
    }
    const project = `${rid}:seed_project`;
    const team = sourceColumnValue(seedSource, seed.teamColumn, "Column");
    const seedColumns: any[] = [
      { output: typedOutputField("team", ColumnType.Uuid), value: team },
      { output: typedOutputField("reachable_team", ColumnType.Uuid), value: team },
    ];
    if (claimField !== undefined) {
      seedColumns.push({
        output: typedOutputField(claimField, userColumnTy ?? ColumnType.Uuid),
        value: { kind: "Param", name: claimField },
      });
    }
    for (const c of edgeRouteColumns)
      seedColumns.push({ output: typedOutputField(c.name, c.ty), value: c.value });
    nodes.set(project, { kind: "Project", input: cur, columns: seedColumns });
    return [project, columns];
  }
  const columns = reachableFrontierColumns(reachable.from, paramTypes);
  for (const c of reachableEdgeRouteColumns(reachable, paramTypes)) {
    if (!columns.some((e) => e.name === c.name)) columns.push(c);
  }
  const seedNode = `${rid}:seed`;
  nodes.set(seedNode, {
    kind: "ValueSource",
    shape: bindingSourceShape,
    columns: [...columns],
    mode: reachableSeedValueSourceMode(reachable.from),
  });
  return [seedNode, columns];
}

function reachableEdgeRouteColumns(reachable: any, paramTypes: Map<string, any>): any[] {
  return [...predicateParams(reachable.edgeFilters)].map((param: string) => {
    const ty = paramTypes.get(param);
    if (ty === undefined) throw lowering(`unknown reachable edge parameter ${param}`);
    return { name: routeParamField(param), value: { kind: "Param", name: param }, ty };
  });
}

function reachableSeedFrontierColumns(schema: any, source: SourceId, seed: any): any[] {
  requireUuidSeed(schema, seed);
  const value = sourceColumnValue(source, seed.teamColumn, "Column");
  const columns: any[] = [
    { name: "team", value, ty: ColumnType.Uuid },
    { name: "reachable_team", value, ty: ColumnType.Uuid },
  ];
  if (seed.userClaim !== undefined) {
    if (seed.userColumn === undefined)
      throw lowering("reachable_via relation seed user_claim requires user_column");
    const ty = schemaColumnType(schema, seed.table, seed.userColumn);
    const path = operandClaimPath(seed.userClaim);
    columns.push({ name: claimParamField(path), value: { kind: "Claim", path }, ty });
  }
  return columns;
}

const SEED_GAP = "reachable_via currently supports uuid parameter/claim/literal seeds only";

function reachableFrontierColumns(seed: any, paramTypes: Map<string, any>): any[] {
  const value = reachableSeedValueRef(seed);
  let ty;
  if (seed.kind === "Param") ty = paramTypes.get(seed.name) ?? ColumnType.Uuid;
  else if ((seed.kind === "Literal" && seed.value.kind === "Uuid") || seed.kind === "Claim")
    ty = ColumnType.Uuid;
  else throw normalizationGap(SEED_GAP);
  const columns: any[] = [
    { name: "team", value, ty },
    { name: "reachable_team", value, ty },
  ];
  if (seed.kind === "Param") {
    const pty = paramTypes.get(seed.name) ?? ColumnType.Uuid;
    columns.push({
      name: routeParamField(seed.name),
      value: { kind: "Param", name: seed.name },
      ty: pty,
    });
    if (seed.name !== "team" && seed.name !== "reachable_team") {
      columns.push({ name: seed.name, value: { kind: "Param", name: seed.name }, ty: pty });
    }
  }
  if (seed.kind === "Claim") {
    const path = operandClaimPath(seed.claim);
    columns.push({
      name: claimParamField(path),
      value: { kind: "Claim", path },
      ty: ColumnType.Uuid,
    });
  }
  return columns;
}

function reachableSeedValueRef(seed: any): ValueRef {
  if (seed.kind === "Param") return { kind: "Param", name: seed.name };
  if (seed.kind === "Literal" && seed.value.kind === "Uuid") return literalValueRef(seed.value);
  if (seed.kind === "Claim") return { kind: "Claim", path: operandClaimPath(seed.claim) };
  throw normalizationGap(SEED_GAP);
}

function reachableSeedValueSourceMode(seed: any): string {
  if (seed.kind === "Param" || seed.kind === "Claim") return "Binding";
  if (seed.kind === "Literal" && seed.value.kind === "Uuid") return "Inline";
  throw normalizationGap(SEED_GAP);
}

function literalValueRef(value: any): ValueRef {
  try {
    const w = new Writer();
    encodeValue(w, value);
    return { kind: "Literal", bytes: w.finish() };
  } catch (err) {
    throw lowering(`literal encoding failed: ${err}`);
  }
}

const typedOutputField = (name: string, ty: any) => ({ name, ty });

export function tableSchema(schema: any, table: string): any {
  const t = schema.tables.find((c: any) => c.name === table);
  if (!t) throw lowering(`unknown query table ${table}`);
  return t;
}

export function schemaColumnType(schema: any, table: string, column: string): any {
  const t = tableSchema(schema, table);
  if (column === "id") return ColumnType.Uuid;
  const c = t.columns.find((c: any) => c.name === column);
  if (c) return c.columnType;
  throw lowering(`unknown query column ${table}.${column}`);
}

function sourcePublicFieldProjections(table: any, source: SourceId): any[] {
  return [
    { output: typedOutputField("id", ColumnType.Uuid), value: rowIdOf(source) },
    ...table.columns.map((c: any) => ({
      output: typedOutputField(c.name, c.columnType),
      value: sourceField(source, c.name),
    })),
  ];
}

function joinViaRootKey(rootSource: SourceId, join: any): ValueRef {
  const f = join.sourceColumn;
  return f === undefined || f === "id" ? rowIdOf(rootSource) : sourceField(rootSource, f);
}

function relationRowProjection(schema: any, query: any, rootSource: SourceId): any[] {
  const relation = query.relation;
  if (!relation) throw lowering("relation projection is missing its relation tree");
  let outputScope, columns;
  try {
    [outputScope, columns] = relationOutputProjection(relation);
  } catch (error) {
    throw capability(String(error));
  }
  const out: any[] = [
    { output: typedOutputField("row_uuid", ColumnType.Uuid), value: rowIdOf(rootSource) },
  ];
  for (const column of columns) {
    let value, ty;
    const e = column.expr;
    if (e.kind === "RowId" && e.ref === "Current") {
      value = rowIdOf(rootSource);
      ty = ColumnType.Uuid;
    } else if (e.kind === "Column") {
      let scope;
      try {
        scope = relationScope(e.reference);
      } catch (error) {
        throw capability(String(error));
      }
      if (scope !== outputScope)
        throw capability("relation projection must select from its output scope");
      ty = schemaColumnType(schema, query.table, e.reference.column);
      value = sourceColumnValue(rootSource, e.reference.column, "Column");
    } else {
      throw capability("outer/frontier row-id relation projections are not unified yet");
    }
    out.push({ output: typedOutputField(column.alias, ty), value });
  }
  return out;
}

function joinViaTargetKey(joinSource: SourceId, join: any): ValueRef {
  return sourceColumnValue(joinSource, join.onColumn, join.target);
}

function joinViaPredicate(left: SourceId, right: SourceId, join: any): Pred {
  if (join.target === "Uncorrelated") return { kind: "True" };
  const lookup = join.sourceLookup;
  const pairs: [ValueRef, ValueRef][] = [
    lookup
      ? [sourceField(left, lookup.rowIdSourceColumn), sourceField(right, lookup.rowIdSourceColumn)]
      : [joinViaRootKey(left, join), joinViaTargetKey(right, join)],
  ];
  for (const c of join.correlatedFilters) {
    pairs.push([
      sourceColumnValue(left, c.sourceColumn, "Column"),
      sourceColumnValue(right, c.joinColumn, "Column"),
    ]);
  }
  if (pairs.length === 1) return compare(pairs[0][0], "Eq", pairs[0][1]);
  return { kind: "And", items: pairs.map(([l, r]) => compare(l, "Eq", r)) };
}

export function reachableEdgeSourceId(reachable: any, rid: string): SourceId {
  return sid(reachable.edgeTable, Root, {
    kind: "RecursiveStep",
    name: `${rid}:${reachable.edgeTable}`,
  });
}

export function reachableAccessSourceId(reachable: any, rid: string): SourceId {
  return sid(reachable.accessTable, alias(`${rid}:${reachable.accessTable}`));
}

export function reachableSeedSourceId(seed: any, rid: string): SourceId {
  return sid(seed.table, Root, { kind: "RecursiveSeed", name: `${rid}:${seed.table}` });
}

function inheritedParentSourceId(table: string, prefix: string): SourceId {
  return sid(table, alias(prefix));
}

/** The inheritance atoms expanded on the current policy-composition path. */
class InheritanceExpansionPath {
  constructor(readonly uses = new Map<string, number>()) {}
  descend(childTable: string, inherits: any): InheritanceExpansionPath | undefined {
    const key = `${childTable}\0${inherits.parentColumn}\0${inherits.operation}`;
    const used = this.uses.get(key) ?? 0;
    const limit = inherits.maxDepth ?? RecursionBound.defaultMaxDepth().depthSteps();
    if (used >= limit) return undefined;
    const next = new InheritanceExpansionPath(new Map(this.uses));
    next.uses.set(key, used + 1);
    return next;
  }
}

function normalizeFalsePolicyBranch(nodes: NodeMap, input: string, prefix: string): string {
  const node = `${prefix}:max_depth`;
  nodes.set(node, { kind: "Filter", input, predicate: { kind: "Or", items: [] } });
  return node;
}

function normalizeFilterJoinChain(
  acc: NormalizeAcc,
  schema: any,
  rootSource: SourceId,
  start: string,
  prefix: string,
  filters: any[],
  joins: any[],
  recordJoinContributions: boolean,
): string {
  let current = start;
  if (filters.length) {
    const id = `${prefix}:filter`;
    acc.nodes.set(id, {
      kind: "Filter",
      input: current,
      predicate: normalizePredicates(schema, rootSource, filters, false),
    });
    current = id;
  }
  joins.forEach((join: any, index: number) => {
    const path = prefix === "query" ? `join_via:${index}` : `${prefix}:join_via:${index}`;
    const nested: any[] = [];
    const [right, joinSource] = normalizeJoinViaRight(
      acc.nodes,
      acc.auxiliarySources,
      nested,
      schema,
      join,
      path,
    );
    const pred = joinViaPredicate(rootSource, joinSource, join);
    if (recordJoinContributions) {
      acc.joinContributions.push({
        parent: undefined,
        id: path,
        source: joinSource,
        input: right,
        membership: pred,
      });
      acc.joinContributions.push(...nested);
    }
    const joinNode = `${path}:join`;
    acc.nodes.set(joinNode, {
      kind: "Join",
      left: current,
      right,
      mode: join.target === "Uncorrelated" ? "Semi" : "Inner",
      on: pred,
    });
    current = joinNode;
  });
  return current;
}

interface PolicyAtomChain {
  filters: any[];
  joins: any[];
  inherits: any[];
  reachable: any[];
}

function normalizePolicyAtomChain(
  acc: NormalizeAcc,
  schema: any,
  rootSource: SourceId,
  start: string,
  prefix: string,
  chain: PolicyAtomChain,
  bindingSourceShape: string,
  paramTypes: Map<string, any>,
  recordJoinContributions: boolean,
  recordInheritedContributions: boolean,
  inheritancePath: InheritanceExpansionPath,
): string {
  let current = normalizeFilterJoinChain(
    acc,
    schema,
    rootSource,
    start,
    prefix,
    chain.filters,
    chain.joins,
    recordJoinContributions,
  );
  chain.inherits.forEach((inherits, index) => {
    current = normalizeInheritedParentPolicy(
      acc,
      schema,
      rootSource,
      current,
      inherits,
      `${prefix}:inherits:${index}`,
      bindingSourceShape,
      paramTypes,
      recordInheritedContributions,
      inheritancePath,
    );
  });
  chain.reachable.forEach((reachable, index) => {
    const [next, contribution] = normalizeReachable(
      acc.nodes,
      current,
      schema,
      rootSource,
      reachable,
      index,
      prefix === "query" ? "" : prefix,
      bindingSourceShape,
      paramTypes,
    );
    current = next;
    acc.reachableContributions.push(contribution);
  });
  return current;
}

function normalizeInheritedParentPolicy(
  acc: NormalizeAcc,
  schema: any,
  childSource: SourceId,
  childCurrent: string,
  inherits: any,
  prefix: string,
  bindingSourceShape: string,
  paramTypes: Map<string, any>,
  recordInheritedContributions: boolean,
  inheritancePath: InheritanceExpansionPath,
): string {
  const childTable = tableSchema(schema, childSource.table);
  const parentTableName = childTable.references.get(inherits.parentColumn);
  if (parentTableName === undefined) {
    throw lowering(`${childSource.table}.${inherits.parentColumn} is not a parent reference`);
  }
  const parentTable = tableSchema(schema, parentTableName);
  const parentPath = inheritancePath.descend(childSource.table, inherits);
  if (!parentPath) return normalizeFalsePolicyBranch(acc.nodes, childCurrent, prefix);
  const parentSource = inheritedParentSourceId(parentTableName, prefix);
  acc.auxiliarySources.add(parentSource);
  let parentCurrent = `${prefix}:source`;
  acc.nodes.set(parentCurrent, sourceNode(parentSource));
  const wp = parentTable.writePolicies;
  const parentPolicy = (
    {
      Select: parentTable.readPolicy,
      Insert: wp.insertCheck,
      Update: wp.updateUsing,
      Delete: wp.deleteUsing,
    } as any
  )[inherits.operation];
  if (parentPolicy) {
    parentCurrent = parentPolicy.policyBranches.length
      ? normalizePolicyBranchAuthorization(
          acc,
          schema,
          parentSource,
          parentCurrent,
          `${prefix}:parent_policy`,
          parentPolicy,
          bindingSourceShape,
          paramTypes,
          parentPath,
        )
      : normalizePolicyAtomChain(
          acc,
          schema,
          parentSource,
          parentCurrent,
          `${prefix}:parent_policy`,
          parentPolicy,
          bindingSourceShape,
          paramTypes,
          false,
          false,
          parentPath,
        );
  } else if (inherits.operation !== "Update" || wp.updateCheck === undefined) {
    return normalizeFalsePolicyBranch(acc.nodes, childCurrent, prefix);
  }
  const joinNode = `${prefix}:join`;
  const membership = compare(
    sourceField(childSource, inherits.parentColumn),
    "Eq",
    rowIdOf(parentSource),
  );
  if (recordInheritedContributions) {
    acc.inheritedContributions.push({
      id: prefix,
      source: parentSource,
      input: parentCurrent,
      membership,
    });
  }
  acc.nodes.set(joinNode, {
    kind: "Join",
    left: childCurrent,
    right: parentCurrent,
    mode: "Semi",
    on: membership,
  });
  return joinNode;
}

function branchUnionInput(
  acc: NormalizeAcc,
  schema: any,
  rootSource: SourceId,
  prefix: string,
  chain: PolicyAtomChain,
  bindingSourceShape: string,
  paramTypes: Map<string, any>,
  recordInherited: boolean,
  inheritancePath: InheritanceExpansionPath,
): any {
  const root = `${prefix}:root`;
  acc.nodes.set(root, sourceNode(rootSource));
  const current = normalizePolicyAtomChain(
    acc,
    schema,
    rootSource,
    root,
    prefix,
    chain,
    bindingSourceShape,
    paramTypes,
    false,
    recordInherited,
    inheritancePath,
  );
  return {
    node: normalizeRowIdProjection(acc.nodes, current, rootSource, `${prefix}:row_id`),
    label: policyBranchSemanticLabel(chain),
  };
}

function normalizePolicyBranchAuthorization(
  acc: NormalizeAcc,
  schema: any,
  rootSource: SourceId,
  current: string,
  prefix: string,
  policy: any,
  bindingSourceShape: string,
  paramTypes: Map<string, any>,
  inheritancePath: InheritanceExpansionPath,
): string {
  const inputs: any[] = [];
  if (!policyBranchBaseIsConverterFalse(policy)) {
    inputs.push(
      branchUnionInput(
        acc,
        schema,
        rootSource,
        `${prefix}:base`,
        policy,
        bindingSourceShape,
        paramTypes,
        false,
        inheritancePath,
      ),
    );
  }
  policy.policyBranches.forEach((branch: any, index: number) =>
    inputs.push(
      branchUnionInput(
        acc,
        schema,
        rootSource,
        `${prefix}:${index}`,
        branch,
        bindingSourceShape,
        paramTypes,
        false,
        inheritancePath,
      ),
    ),
  );
  return authorizeByUnion(acc.nodes, current, inputs, rootSource, prefix);
}

/** Keep the rows of `current` whose root row id appears in any authorized branch. */
function authorizeByUnion(
  nodes: NodeMap,
  current: string,
  inputs: any[],
  rootSource: SourceId,
  prefix: string,
): string {
  const unionNode = `${prefix}:authorized_rows`;
  nodes.set(unionNode, { kind: "Union", inputs });
  const joinNode = `${prefix}:authorize`;
  nodes.set(joinNode, {
    kind: "Join",
    left: current,
    right: unionNode,
    mode: "Inner",
    on: compare(rowIdOf(rootSource), "Eq", sourceField(rootSource, "row_uuid")),
  });
  return joinNode;
}

function policyBranchSemanticLabel(chain: PolicyAtomChain): string {
  let bytes;
  try {
    const w = new Writer();
    w.varint(chain.filters.length);
    for (const f of chain.filters) encodePredicate(w, f);
    w.varint(chain.joins.length);
    for (const j of chain.joins) encodeJoinVia(w, j);
    w.varint(chain.reachable.length);
    for (const r of chain.reachable) encodeReachableVia(w, r);
    w.varint(chain.inherits.length);
    for (const i of chain.inherits) encodeInheritsVia(w, i);
    bytes = w.finish();
  } catch (error) {
    throw lowering(`policy branch fingerprint encoding failed: ${error}`);
  }
  return `policy:${toHex(blake3(bytes))}`;
}

function normalizeRowIdProjection(
  nodes: NodeMap,
  input: string,
  rootSource: SourceId,
  nodeId: string,
): string {
  nodes.set(nodeId, {
    kind: "Project",
    input,
    columns: [
      { output: typedOutputField("row_uuid", ColumnType.Uuid), value: rowIdOf(rootSource) },
    ],
  });
  return nodeId;
}

function policyBranchBaseIsConverterFalse(query: any): boolean {
  return (
    query.filters.length === 1 &&
    query.filters[0].kind === "Any" &&
    query.filters[0].items.length === 0 &&
    !query.joins.length &&
    !query.reachable.length &&
    !query.inherits.length
  );
}

export function collectPolicyDependencyClaimParams(
  st: NodeState,
  schema: any,
  policy: any,
  input: any,
  params: SortedMap<string, any>,
): void {
  if (policy.kind === "System") return;
  const claims = policy.claims;
  for (const tableName of normalizedSourceTables(input)) {
    const table = schema.tables.find((t: any) => t.name === tableName);
    if (!table) throw nodeError("TableNotFound", tableName);
    const query = authorizationQueryFromReadPolicy(table);
    const values = new Map();
    bindScopeClaimOperands(query, claims, values);
    for (const [name, claim] of disambiguatePolicyClaimParams(query, schema, values)) {
      if (
        [...params.valuesArray()].some(
          (e) => claimPathEq(e.path, claim.path) && columnTypeEq(e.ty, claim.ty),
        )
      )
        continue;
      params.set(name, claim);
    }
  }
}

export function normalizedRowSetShape(st: NodeState, shape: any, binding: any): any {
  const schema = st.catalogue.schemaForVersion(shape.schemaVersion());
  if (!schema) throw nodeError("InvalidStoredValue", "query schema version is unknown");
  const query = shape.query();
  if (query.relation && relationUnionParts(query.relation.rel) !== undefined) {
    return normalizedRelationUnionRowSetShape(st, shape, query.relation, binding, schema);
  }
  const rootSource = rootSourceId(query.table);
  const acc = newAcc();
  const [aux, closurePaths] = collectClosurePaths(
    st,
    query.table,
    shape.schemaVersion(),
    query.includes,
  );
  for (const s of aux) acc.auxiliarySources.add(s);
  acc.closurePaths.push(...closurePaths);
  acc.nodes.set("root", sourceNode(rootSource));
  let current = "root";
  const inheritancePath = new InheritanceExpansionPath();
  const bindingSourceShape = PENDING_BINDING_SOURCE_SHAPE;
  const queryChainFilters = query.flatJoin ? [] : query.filters;
  const params = shape.params();
  const factors = policyBranchBaseIsConverterFalse(query)
    ? factorPolicyBranches(query.policyBranches)
    : undefined;
  if (factors) {
    factors.forEach((alternatives, factorIndex) => {
      const inputs = alternatives.map((alt, index) =>
        branchUnionInput(
          acc,
          schema,
          rootSource,
          `policy_factor:${factorIndex}:${index}`,
          alt,
          bindingSourceShape,
          params,
          false,
          inheritancePath,
        ),
      );
      current = authorizeByUnion(
        acc.nodes,
        current,
        inputs,
        rootSource,
        `policy_factor:${factorIndex}`,
      );
    });
  } else if (query.policyBranches.length) {
    const inputs: any[] = [];
    if (!policyBranchBaseIsConverterFalse(query)) {
      const base = branchUnionInput(
        acc,
        schema,
        rootSource,
        "policy_branch:base",
        {
          filters: queryChainFilters,
          joins: query.joins,
          inherits: query.inherits,
          reachable: query.reachable,
        },
        bindingSourceShape,
        params,
        true,
        inheritancePath,
      );
      base.label = policyBranchSemanticLabel(query);
      inputs.push(base);
    }
    query.policyBranches.forEach((branch: any, index: number) =>
      inputs.push(
        branchUnionInput(
          acc,
          schema,
          rootSource,
          `policy_branch:${index}`,
          branch,
          bindingSourceShape,
          params,
          false,
          inheritancePath,
        ),
      ),
    );
    current = authorizeByUnion(acc.nodes, current, inputs, rootSource, "policy_branch");
  } else {
    current = normalizePolicyAtomChain(
      acc,
      schema,
      rootSource,
      current,
      "query",
      {
        filters: queryChainFilters,
        joins: query.joins,
        inherits: query.inherits,
        reachable: query.reachable,
      },
      bindingSourceShape,
      params,
      true,
      true,
      inheritancePath,
    );
  }

  const flatJoin = query.flatJoin;
  if (flatJoin) {
    const rootName: string = flatJoin.rootAlias ?? query.table;
    const routed = flatJoinFiltersBySource(query.filters, rootName);
    const rootFilters = routed.get(rootName);
    if (rootFilters) {
      routed.delete(rootName);
      acc.nodes.set("flat_join:root_filter", {
        kind: "Filter",
        input: current,
        predicate: normalizePredicates(schema, rootSource, rootFilters, true),
      });
      current = "flat_join:root_filter";
    }
    const sources = new Map<string, SourceId>([[rootName, rootSource]]);
    const tupleSources = [rootSource];
    const outputSources: [string, SourceId][] = [[rootName, rootSource]];
    flatJoin.sources.forEach((join: any, index: number) => {
      const name: string = join.alias ?? join.table;
      const source = sid(join.table, alias(`flat_join:${index}:${name}`));
      const srcNode = `flat_join:${index}:source`;
      acc.nodes.set(srcNode, sourceNode(source));
      let rightInput = srcNode;
      const filters = routed.get(name);
      if (filters) {
        routed.delete(name);
        rightInput = `flat_join:${index}:filter`;
        acc.nodes.set(rightInput, {
          kind: "Filter",
          input: srcNode,
          predicate: normalizePredicates(schema, source, filters, true),
        });
      }
      acc.auxiliarySources.add(source);
      const split = (field: string): [string, string] => {
        const i = field.lastIndexOf(".");
        if (i < 0) throw capability(`flat join field must be qualified: ${field}`);
        return [field.slice(0, i), field.slice(i + 1)];
      };
      const valueRef = (field: string): ValueRef => {
        const [scope, column] = split(field);
        const s = sources.get(scope);
        if (!s) throw capability(`unknown flat join source ${scope}`);
        return column === "_id" ? rowIdOf(s) : sourceColumnValue(s, column, "Column");
      };
      const [, rightColumn] = split(join.on.right);
      const membership = compare(
        valueRef(join.on.left),
        "Eq",
        rightColumn === "_id" ? rowIdOf(source) : sourceColumnValue(source, rightColumn, "Column"),
      );
      const joinNode = `flat_join:${index}:join`;
      acc.nodes.set(joinNode, {
        kind: "Join",
        left: current,
        right: rightInput,
        mode: "Inner",
        on: membership,
      });
      acc.joinContributions.push({
        parent: undefined,
        id: `flat_join:${index}`,
        source,
        input: rightInput,
        membership,
      });
      current = joinNode;
      sources.set(name, source);
      outputSources.push([name, source]);
      tupleSources.push(source);
    });
    const leftover = [...routed.keys()].sort()[0];
    if (leftover !== undefined)
      throw lowering(`flat join filter references unknown source ${leftover}`);
    const columns: any[] = tupleSources.map((source, position) => ({
      output: typedOutputField(
        position === 0 ? "row_uuid" : `__flat_join_row_${position}`,
        ColumnType.Uuid,
      ),
      value: rowIdOf(source),
    }));
    for (const name of ["tx_time", "tx_node_id"]) {
      columns.push({
        output: typedOutputField(name, ColumnType.U64),
        value: sourceField(rootSource, name),
      });
    }
    for (const [name, source] of outputSources) {
      const s = schema.tables.find((t: any) => t.name === source.table);
      if (!s) throw capability(`unknown flat join table ${source.table}`);
      for (const c of s.columns) {
        columns.push({
          output: typedOutputField(`${name}.${c.name}`, c.columnType),
          value: sourceField(source, c.name),
        });
      }
    }
    acc.nodes.set("flat_join:output", { kind: "Project", input: current, columns });
    current = "flat_join:output";
  }

  query.arraySubqueries.forEach((sub: any, index: number) => {
    current = normalizeArraySubquery(acc.nodes, current, schema, rootSource, sub, [index]);
  });
  if (!query.aggregate && query.orderBy.length) {
    acc.nodes.set("order", {
      kind: "OrderBy",
      input: current,
      keys: query.orderBy.map((o: any) => normalizeOrderKey(rootSource, o)),
    });
    current = "order";
  }
  if (!query.aggregate && (query.limit !== undefined || query.offset !== 0)) {
    acc.nodes.set("slice", {
      kind: "Slice",
      input: current,
      partitionBy: [],
      limit: query.limit === undefined ? undefined : Math.min(query.limit, 0xffffffff),
      offset: Math.min(query.offset, 0xffffffff),
      tieBreaker: [rowIdOf(rootSource)],
      rankOutput: undefined,
    });
    current = "slice";
  }
  if (query.relation && relationOutputProjectionIfPresent(query.relation) !== undefined) {
    acc.nodes.set("relation:output", {
      kind: "Project",
      input: current,
      columns: relationRowProjection(schema, query, rootSource),
    });
    current = "relation:output";
  }
  if (query.aggregate) {
    acc.nodes.set("aggregate", {
      kind: "Aggregate",
      input: current,
      groupBy: normalizedAggregateGroupBy(rootSource, query.aggregate),
      outputs: normalizedAggregateOutputs(rootSource, query.aggregate),
    });
    current = "aggregate";
  }
  const normalized = {
    identity: { shapeId: shape.shapeId(), canonical: shape.canonicalBytes() },
    root: current,
    result: { kind: "RealRow", table: query.table, row: { kind: "Source", source: rootSource } },
    ...acc,
  };
  const claimParams = bindingClaimParamsForShape(normalized, params);
  retargetBindingValueSources(normalized, queryBindingSourceShapeForParts(params, claimParams));
  return normalized;
}

/** Normalize a retained public `UNION ALL` by reusing the single-relation facade for each arm. */
function normalizedRelationUnionRowSetShape(
  st: NodeState,
  shape: any,
  relation: any,
  binding: any,
  schema: any,
): any {
  const parts = relationUnionParts(relation.rel);
  if (!parts) {
    throw capability(
      "retained relation body must be a UNION ALL with global terminal operators in builder order",
    );
  }
  const acc = newAcc();
  const unionInputs: any[] = [];
  for (const arm of parts.inputs) {
    const armRelation = { rel: arm.input };
    const armQuery = relationQueryToQuery(armRelation);
    if (armQuery.table !== shape.query().table)
      throw capability("UNION ALL arms must emit the same output table");
    armQuery.relation = armRelation;
    const armShape = validateUnionArmWithSchemaVersion(armQuery, schema, shape.schemaVersion());
    const normalized = normalizedRowSetShape(st, armShape, binding);
    prefixNormalizedRelationArm(normalized, `relation_union:${arm.label}`, acc);
    unionInputs.push({ node: normalized.root, label: arm.label });
  }
  acc.nodes.set("relation_union:root", { kind: "Union", inputs: unionInputs });
  const rootSource = rootSourceId(shape.query().table);
  let terminal = "relation_union:root";
  if (parts.orderBy) {
    acc.nodes.set("relation_union:order", {
      kind: "OrderBy",
      input: terminal,
      keys: parts.orderBy.map((term: any) => ({
        value: normalizeOperand(rootSource, { kind: "Column", name: term.column.column }),
        direction: term.direction === "Desc" ? "Desc" : "Asc",
      })),
    });
    terminal = "relation_union:order";
  }
  if (parts.limit !== undefined || parts.offset !== undefined) {
    acc.nodes.set("relation_union:slice", {
      kind: "Slice",
      input: terminal,
      partitionBy: [],
      limit: parts.limit === undefined ? undefined : Math.min(parts.limit, 0xffffffff),
      offset: Math.min(parts.offset ?? 0, 0xffffffff),
      tieBreaker: [rowIdOf(rootSource), sourceField(rootSource, "__root_union_arm")],
      rankOutput: undefined,
    });
    terminal = "relation_union:slice";
  }
  return {
    identity: { shapeId: shape.shapeId(), canonical: shape.canonicalBytes() },
    root: terminal,
    result: {
      kind: "RealRow",
      table: shape.query().table,
      row: { kind: "Source", source: rootSource },
    },
    ...acc,
  };
}

export function normalizedIncludeDeletedRowSetShape(st: NodeState, shape: any, binding: any): any {
  const normalized = normalizedRowSetShape(st, shape, binding);
  const rootSource = rootSourceId(shape.query().table);
  for (const node of normalized.nodes.valuesArray()) {
    if (node.kind === "Source" && cmpSourceId(node.source, rootSource) === 0)
      node.visibility = "IncludeDeleted";
  }
  return normalized;
}
