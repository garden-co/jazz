// Hash-consed IVM graph IR and builder API.
import {
  type FieldIdentity,
  type RecordDescriptor,
  type Value,
  type ValueType,
  collectByOrderedScalar,
  valueTypeContainsRecord,
} from "../../_extern/groove-ivm-plan.js";
import { JazzError } from "../../support/runtime.js";
import { utf8 } from "../../support/bytes.js";
import type { TypedGraphTemplate, TemplateScalarArgument } from "./template.js";
import { ActivationCache, type ActivationPlan } from "./activation.js";
import { ExecutionLayoutCache, type ExecutionLayout } from "./execution_layout.js";
import { RouteIndex } from "./routes.js";
import {
  type AggregateExpr,
  type AggregateOp,
  type ArgMaxByOp,
  type ArgMinByOp,
  type BindingSourceKey,
  type BindingSourceOp,
  type CollectByMode,
  type CollectByOp,
  type CollectByProjection,
  type FilterOp,
  type FrontierName,
  type FrontierSourceOp,
  type IndexByOp,
  type IndexSourceOp,
  type InlineRecordsOp,
  type JoinOp,
  type LiteralValue,
  type LookupOp,
  type MapProjectOp,
  type PersistOp,
  type PredicateExpr,
  type RecursiveEnumRemaps,
  type RecursiveOp,
  type StaticScanSpec,
  type StreamingChecksumOp,
  type TableSourceOp,
  type TopByDirection,
  type TopByLimit,
  type TopByOp,
  type UnnestOp,
  type UnwrapNullableOp,
  type ValueComparison,
  type VariantProjectOp,
  BindingSourceKey as BindingKey,
  structEq,
  structKey,
} from "./op_types.js";

export { narrowProjectedJoinChain } from "./graph/join_chains.js";

/** Snapshot-only index-key filter. */
export interface IndexCandidateFilter {
  table: string;
  index: string;
  scan: StaticScanSpec;
  sourceColumn: string;
  candidateColumn: string;
}

export type FieldRef =
  | { kind: "Name"; name: string }
  | { kind: "StoredName"; name: string }
  | { kind: "Resolved"; index: number };

export const FieldRef = {
  name: (name: string): FieldRef => ({ kind: "Name", name }),
  resolved: (index: number): FieldRef => ({ kind: "Resolved", index }),
  storedName: (name: string): FieldRef => ({ kind: "StoredName", name }),
  displayName(f: FieldRef): string {
    switch (f.kind) {
      case "Name":
        return f.name;
      case "StoredName":
        return `stored:${f.name}`;
      case "Resolved":
        return `#${f.index}`;
    }
  },
};

export interface TopByOrder {
  field: FieldRef;
  direction: TopByDirection;
}
export const TopByOrder = {
  asc: (field: string): TopByOrder => ({ field: FieldRef.name(field), direction: "Asc" }),
  desc: (field: string): TopByOrder => ({ field: FieldRef.name(field), direction: "Desc" }),
};

export interface CollectByField {
  field: FieldRef;
  outputName: string;
  unwrapNullable: boolean;
}
export const CollectByField = {
  named: (field: string): CollectByField => ({
    field: FieldRef.name(field),
    outputName: field,
    unwrapNullable: false,
  }),
  renamed: (field: string, outputName: string): CollectByField => ({
    field: FieldRef.name(field),
    outputName,
    unwrapNullable: false,
  }),
  renamedUnwrapNullable: (field: string, outputName: string): CollectByField => ({
    field: FieldRef.name(field),
    outputName,
    unwrapNullable: true,
  }),
};

export interface CollectByBuilder {
  mode: CollectByMode;
  groupCols: FieldRef[];
  parentFields: CollectByField[];
  childFields: CollectByField[];
  collectionField: string;
  slots: CollectBySlotBuilder[];
  tupleFields: CollectByField[];
  occurrenceIdCols: FieldRef[];
  orderCols: TopByOrder[];
  tieCols: FieldRef[];
  offset: number;
  limit: TopByLimit;
}

export interface CollectBySlotBuilder {
  groupCols: FieldRef[];
  ownerKeyCols: FieldRef[];
  childFields: CollectByField[];
  collectionField: string;
  slots: CollectBySlotBuilder[];
  orderCols: TopByOrder[];
  tieCols: FieldRef[];
  presenceCol?: FieldRef;
  referenceArrayCol?: FieldRef;
  referenceOrder: boolean;
  offset: number;
  limit: TopByLimit;
}

export const MAX_COLLECT_BY_TREE_DEPTH = 16;

export const CollectBySlotBuilder = {
  new: (
    groupCols: string[],
    childFields: CollectByField[],
    collectionField: string,
    slots: CollectBySlotBuilder[],
    orderCols: TopByOrder[],
    tieCols: string[],
    offset: number,
    limit: TopByLimit,
  ): CollectBySlotBuilder => ({
    groupCols: groupCols.map(FieldRef.name),
    ownerKeyCols: [],
    childFields,
    collectionField,
    slots,
    orderCols,
    tieCols: tieCols.map(FieldRef.name),
    referenceOrder: false,
    offset,
    limit,
  }),
  withPresenceCol: (s: CollectBySlotBuilder, col: string): CollectBySlotBuilder => ({
    ...s,
    presenceCol: FieldRef.name(col),
  }),
  withOwnerKeyCols: (s: CollectBySlotBuilder, cols: string[]): CollectBySlotBuilder => ({
    ...s,
    ownerKeyCols: cols.map(FieldRef.name),
  }),
};

/** Opaque identity of one mutable input owned by a single IVM runtime. */
export class InputSourceId {
  constructor(
    readonly runtimeNamespace: number,
    readonly local: number,
  ) {}
  belongsTo(ns: number): boolean {
    return this.runtimeNamespace === ns;
  }
  wasAllocatedBy(ns: number, nextLocal: number): boolean {
    return this.belongsTo(ns) && this.local < nextLocal;
  }
  bindingKey(): BindingSourceKey {
    return { kind: "RuntimeInput", runtimeNamespace: this.runtimeNamespace, local: this.local };
  }
  diagnosticName(): string {
    return `runtime input source ${this.runtimeNamespace}:${this.local}`;
  }
  structKey(): string {
    return `in${this.runtimeNamespace}:${this.local}`;
  }
}

type JoinLike = {
  left: GraphBuilder;
  right: GraphBuilder;
  leftOn: FieldRef[];
  rightOn: FieldRef[];
  comparison: ValueComparison;
};

export type GraphBuilder =
  | {
      kind: "TypedTemplate";
      program: TypedGraphTemplate;
      inputs: GraphBuilder[];
      predicates: PredicateExpr[];
      scalars: TemplateScalarArgument[];
    }
  | { kind: "TemplateInput"; slot: number; output: RecordDescriptor; input?: GraphBuilder }
  | { kind: "Table"; table: string; scan?: StaticScanSpec; variantProjection?: string }
  | { kind: "InlineRecords"; output: RecordDescriptor; records: Uint8Array[] }
  | { kind: "InputSource"; id: InputSourceId; output: RecordDescriptor }
  | {
      kind: "Index";
      table: string;
      index: string;
      scan?: StaticScanSpec;
      intersections: [string, StaticScanSpec][];
      candidateFilter?: IndexCandidateFilter;
      rowProjection?: string;
    }
  | { kind: "FrontierSource"; binding: FrontierName; output: RecordDescriptor }
  | { kind: "BindingSource"; shape: string; output: RecordDescriptor }
  | {
      kind: "Recursive";
      seed: GraphBuilder;
      step: GraphBuilder;
      stepWitness?: GraphBuilder;
      frontier: FrontierName;
      maxIters: number;
      truncateAtMaxIters: boolean;
    }
  | { kind: "RecursiveStepWitness"; recursive: GraphBuilder }
  | { kind: "Filter"; input: GraphBuilder; predicate: PredicateExpr; comparison: ValueComparison }
  | { kind: "UnwrapNullable"; input: GraphBuilder; field: FieldRef }
  | { kind: "Unnest"; input: GraphBuilder; arrayField: FieldRef; elementField: string }
  | { kind: "VariantProject"; input: GraphBuilder; field: FieldRef; case: string }
  | { kind: "Project"; input: GraphBuilder; fields: ProjectField[] }
  | {
      kind: "StreamingChecksum";
      input: GraphBuilder;
      field: FieldRef;
      outputField: string;
      windowBytes: number;
      maxBytesPerTurn: number;
    }
  | { kind: "Union"; inputs: GraphBuilder[] }
  | ({ kind: "Join" | "SemiJoin" | "AntiJoin" } & JoinLike)
  | {
      kind: "Lookup";
      keys: GraphBuilder;
      keysOn: FieldRef[];
      table: string;
      scan?: StaticScanSpec;
      variantProjection?: string;
    }
  | {
      kind: "ArgMaxBy" | "ArgMinBy";
      input: GraphBuilder;
      groupCols: FieldRef[];
      orderCols: FieldRef[];
    }
  | {
      kind: "TopBy";
      input: GraphBuilder;
      groupCols: FieldRef[];
      orderCols: TopByOrder[];
      tieCols: FieldRef[];
      offset: number;
      limit: TopByLimit;
    }
  | { kind: "CollectBy"; input: GraphBuilder; collect: CollectByBuilder }
  | { kind: "Aggregate"; input: GraphBuilder; groupCols: FieldRef[]; aggregates: AggregateExpr[] };

const names = (xs: string[]) => xs.map(FieldRef.name);
const unbounded: TopByLimit = { kind: "Unbounded" };

function collectBuilder(
  mode: CollectByMode,
  groupCols: string[],
  over: Partial<CollectByBuilder>,
): CollectByBuilder {
  return {
    mode,
    groupCols: names(groupCols),
    parentFields: [],
    childFields: [],
    collectionField: "",
    slots: [],
    tupleFields: [],
    occurrenceIdCols: [],
    orderCols: [],
    tieCols: [],
    offset: 0,
    limit: unbounded,
    ...over,
  };
}

export const GraphBuilder = {
  table: (table: string): GraphBuilder => ({ kind: "Table", table }),
  tableScan: (table: string, scan: StaticScanSpec): GraphBuilder => ({
    kind: "Table",
    table,
    scan,
  }),
  variantSource: (table: string, target: string): GraphBuilder => ({
    kind: "Table",
    table,
    variantProjection: target,
  }),
  variantSourceScan: (table: string, target: string, scan: StaticScanSpec): GraphBuilder => ({
    kind: "Table",
    table,
    scan,
    variantProjection: target,
  }),
  inlineRecords: (output: RecordDescriptor, records: Uint8Array[]): GraphBuilder => ({
    kind: "InlineRecords",
    output,
    records,
  }),
  inputSource: (id: InputSourceId, output: RecordDescriptor): GraphBuilder => ({
    kind: "InputSource",
    id,
    output,
  }),
  values(output: RecordDescriptor, rows: Value[][]): GraphBuilder {
    return GraphBuilder.inlineRecords(
      output,
      rows.map((row) => output.create(row)),
    );
  },
  index: (table: string, index: string, scan?: StaticScanSpec): GraphBuilder => ({
    kind: "Index",
    table,
    index,
    scan,
    intersections: [],
  }),
  variantIndexScan: (
    table: string,
    index: string,
    target: string,
    scan: StaticScanSpec,
  ): GraphBuilder => ({
    kind: "Index",
    table,
    index,
    scan,
    intersections: [],
    rowProjection: target,
  }),
  variantIndexIntersectionScan: (
    table: string,
    index: string,
    scan: StaticScanSpec,
    intersections: [string, StaticScanSpec][],
    target: string,
  ): GraphBuilder => ({ kind: "Index", table, index, scan, intersections, rowProjection: target }),
  variantIndexCandidateScan: (
    table: string,
    index: string,
    scan: StaticScanSpec,
    candidateFilter: IndexCandidateFilter,
    target: string,
  ): GraphBuilder => ({
    kind: "Index",
    table,
    index,
    scan,
    intersections: [],
    candidateFilter,
    rowProjection: target,
  }),
  frontierSource: (binding: string, output: RecordDescriptor): GraphBuilder => ({
    kind: "FrontierSource",
    binding,
    output,
  }),
  bindingSource: (shape: string, output: RecordDescriptor): GraphBuilder => ({
    kind: "BindingSource",
    shape,
    output,
  }),
  recursive: (
    seed: GraphBuilder,
    step: GraphBuilder,
    frontier: string,
    maxIters: number,
    truncate = false,
  ): GraphBuilder => ({
    kind: "Recursive",
    seed,
    step,
    frontier,
    maxIters,
    truncateAtMaxIters: truncate,
  }),
  recursiveBounded: (seed: GraphBuilder, step: GraphBuilder, frontier: string, maxIters: number) =>
    GraphBuilder.recursive(seed, step, frontier, maxIters, true),
  recursiveWithStepWitness: (
    seed: GraphBuilder,
    step: GraphBuilder,
    stepWitness: GraphBuilder,
    frontier: string,
    maxIters: number,
    truncateAtMaxIters: boolean,
  ): GraphBuilder => ({
    kind: "Recursive",
    seed,
    step,
    stepWitness,
    frontier,
    maxIters,
    truncateAtMaxIters,
  }),
  recursiveStepWitness: (recursive: GraphBuilder): GraphBuilder => ({
    kind: "RecursiveStepWitness",
    recursive,
  }),
  union: (inputs: GraphBuilder[]): GraphBuilder => ({ kind: "Union", inputs }),
  join: (
    left: GraphBuilder,
    right: GraphBuilder,
    leftOn: string[],
    rightOn: string[],
  ): GraphBuilder => ({
    kind: "Join",
    left,
    right,
    leftOn: names(leftOn),
    rightOn: names(rightOn),
    comparison: "Exact",
  }),
  policyJoin: (
    left: GraphBuilder,
    right: GraphBuilder,
    leftOn: string[],
    rightOn: string[],
  ): GraphBuilder => ({
    kind: "Join",
    left,
    right,
    leftOn: names(leftOn),
    rightOn: names(rightOn),
    comparison: "Policy",
  }),
  semiJoin: (
    left: GraphBuilder,
    right: GraphBuilder,
    leftOn: string[],
    rightOn: string[],
  ): GraphBuilder => ({
    kind: "SemiJoin",
    left,
    right,
    leftOn: names(leftOn),
    rightOn: names(rightOn),
    comparison: "Exact",
  }),
  antiJoin: (
    left: GraphBuilder,
    right: GraphBuilder,
    leftOn: string[],
    rightOn: string[],
  ): GraphBuilder => ({
    kind: "AntiJoin",
    left,
    right,
    leftOn: names(leftOn),
    rightOn: names(rightOn),
    comparison: "Exact",
  }),
  /** Probe a stored table by the keys of another relation. */
  lookup(right: GraphBuilder, keys: GraphBuilder, keysOn: string[]): GraphBuilder | undefined {
    if (right.kind !== "Table") return undefined;
    return {
      kind: "Lookup",
      keys,
      keysOn: names(keysOn),
      table: right.table,
      scan: right.scan,
      variantProjection: right.variantProjection,
    };
  },
  argMaxBy: (input: GraphBuilder, groupCols: string[], orderCols: string[]): GraphBuilder => ({
    kind: "ArgMaxBy",
    input,
    groupCols: names(groupCols),
    orderCols: names(orderCols),
  }),
  argMinBy: (input: GraphBuilder, groupCols: string[], orderCols: string[]): GraphBuilder => ({
    kind: "ArgMinBy",
    input,
    groupCols: names(groupCols),
    orderCols: names(orderCols),
  }),
  topBy: (
    input: GraphBuilder,
    groupCols: string[],
    orderCols: TopByOrder[],
    tieCols: string[],
    offset: number,
    limit: TopByLimit,
  ): GraphBuilder => ({
    kind: "TopBy",
    input,
    groupCols: names(groupCols),
    orderCols,
    tieCols: names(tieCols),
    offset,
    limit,
  }),
  collectBy: (
    input: GraphBuilder,
    groupCols: string[],
    parentFields: CollectByField[],
    childFields: CollectByField[],
    collectionField: string,
    orderCols: TopByOrder[],
    tieCols: string[],
    offset: number,
    limit: TopByLimit,
  ): GraphBuilder => ({
    kind: "CollectBy",
    input,
    collect: collectBuilder("Collect", groupCols, {
      parentFields,
      childFields,
      collectionField,
      orderCols,
      tieCols: names(tieCols),
      offset,
      limit,
    }),
  }),
  collectByTree: (
    input: GraphBuilder,
    groupCols: string[],
    parentFields: CollectByField[],
    slots: CollectBySlotBuilder[],
  ) =>
    GraphBuilder.collectByTreeOrdered(input, groupCols, parentFields, slots, [], [], 0, unbounded),
  collectByTreeOrdered: (
    input: GraphBuilder,
    groupCols: string[],
    parentFields: CollectByField[],
    slots: CollectBySlotBuilder[],
    orderCols: TopByOrder[],
    tieCols: string[],
    offset: number,
    limit: TopByLimit,
  ): GraphBuilder => ({
    kind: "CollectBy",
    input,
    collect: collectBuilder("Collect", groupCols, {
      parentFields,
      slots,
      orderCols,
      tieCols: names(tieCols),
      offset,
      limit,
    }),
  }),
  collectRootOrdered: (
    input: GraphBuilder,
    groupCols: string[],
    parentFields: CollectByField[],
    orderCols: TopByOrder[],
    tieCols: string[],
    offset: number,
    limit: TopByLimit,
  ): GraphBuilder => ({
    kind: "CollectBy",
    input,
    collect: collectBuilder("Root", groupCols, {
      parentFields,
      orderCols,
      tieCols: names(tieCols),
      offset,
      limit,
    }),
  }),
  collectByExpand: (
    input: GraphBuilder,
    groupCols: string[],
    tupleFields: CollectByField[],
    occurrenceIdCols: string[],
    orderCols: TopByOrder[],
    tieCols: string[],
    offset: number,
    limit: TopByLimit,
  ): GraphBuilder => ({
    kind: "CollectBy",
    input,
    collect: collectBuilder("Expand", groupCols, {
      tupleFields,
      occurrenceIdCols: names(occurrenceIdCols),
      orderCols,
      tieCols: names(tieCols),
      offset,
      limit,
    }),
  }),
  aggregate: (
    input: GraphBuilder,
    groupCols: string[],
    aggregates: AggregateExpr[],
  ): GraphBuilder => ({
    kind: "Aggregate",
    input,
    groupCols: names(groupCols),
    aggregates,
  }),
  filter: (input: GraphBuilder, predicate: PredicateExpr): GraphBuilder => ({
    kind: "Filter",
    input,
    predicate,
    comparison: "Exact",
  }),
  policyFilter: (input: GraphBuilder, predicate: PredicateExpr): GraphBuilder => ({
    kind: "Filter",
    input,
    predicate,
    comparison: "Policy",
  }),
  unwrapNullable: (input: GraphBuilder, field: string): GraphBuilder => ({
    kind: "UnwrapNullable",
    input,
    field: FieldRef.name(field),
  }),
  unnest: (input: GraphBuilder, arrayField: string, elementField: string): GraphBuilder => ({
    kind: "Unnest",
    input,
    arrayField: FieldRef.name(arrayField),
    elementField,
  }),
  variantProject: (input: GraphBuilder, field: string, c: string): GraphBuilder => ({
    kind: "VariantProject",
    input,
    field: FieldRef.name(field),
    case: c,
  }),
  project: (input: GraphBuilder, fields: string[]): GraphBuilder => ({
    kind: "Project",
    input,
    fields: fields.map(ProjectField.named),
  }),
  projectFields: (input: GraphBuilder, fields: ProjectField[]): GraphBuilder => ({
    kind: "Project",
    input,
    fields,
  }),
  streamingChecksum: (
    input: GraphBuilder,
    field: string,
    outputField: string,
    windowBytes: number,
    maxBytesPerTurn: number,
  ): GraphBuilder => ({
    kind: "StreamingChecksum",
    input,
    field: FieldRef.name(field),
    outputField,
    windowBytes,
    maxBytesPerTurn,
  }),
};

/** Immediate inputs, in evaluation order. */
export function graphInputs(g: GraphBuilder): GraphBuilder[] {
  switch (g.kind) {
    case "TypedTemplate":
      return g.inputs;
    case "TemplateInput":
      return g.input ? [g.input] : [];
    case "Filter":
    case "Project":
    case "StreamingChecksum":
    case "UnwrapNullable":
    case "Unnest":
    case "VariantProject":
    case "ArgMaxBy":
    case "ArgMinBy":
    case "TopBy":
    case "CollectBy":
    case "Aggregate":
      return [g.input];
    case "Lookup":
      return [g.keys];
    case "Union":
      return g.inputs;
    case "Join":
    case "SemiJoin":
    case "AntiJoin":
      return [g.left, g.right];
    case "Recursive":
      return g.stepWitness ? [g.seed, g.step, g.stepWitness] : [g.seed, g.step];
    case "RecursiveStepWitness":
      return [g.recursive];
    default:
      return [];
  }
}

/** All builder fragments in child-before-parent order, without recursion. */
export function graphPostorder(root: GraphBuilder): GraphBuilder[] {
  const pending: [GraphBuilder, boolean][] = [[root, false]];
  const ordered: GraphBuilder[] = [];
  while (pending.length) {
    const [graph, visited] = pending.pop()!;
    if (visited) {
      ordered.push(graph);
      continue;
    }
    pending.push([graph, true]);
    const inputs = graphInputs(graph);
    // TypedTemplate pushes inputs in order (popped reversed), every other
    // multi-input node pushes reversed so the first input is visited first.
    const order = graph.kind === "TypedTemplate" ? inputs : inputs.slice().reverse();
    for (const input of order) pending.push([input, false]);
  }
  return ordered;
}

/** Rewrite immediate inputs only; the caller owns traversal and sharing. */
export function graphMapInputs(
  g: GraphBuilder,
  map: (input: GraphBuilder) => GraphBuilder,
): GraphBuilder {
  switch (g.kind) {
    case "TypedTemplate":
    case "Union":
      return { ...g, inputs: g.inputs.map(map) } as GraphBuilder;
    case "TemplateInput":
      return g.input ? { ...g, input: map(g.input) } : g;
    case "Filter":
    case "Project":
    case "StreamingChecksum":
    case "UnwrapNullable":
    case "Unnest":
    case "VariantProject":
    case "ArgMaxBy":
    case "ArgMinBy":
    case "TopBy":
    case "CollectBy":
    case "Aggregate":
      return { ...g, input: map(g.input) } as GraphBuilder;
    case "Lookup":
      return { ...g, keys: map(g.keys) };
    case "Join":
    case "SemiJoin":
    case "AntiJoin":
      return { ...g, left: map(g.left), right: map(g.right) };
    case "Recursive":
      return {
        ...g,
        seed: map(g.seed),
        step: map(g.step),
        stepWitness: g.stepWitness && map(g.stepWitness),
      };
    case "RecursiveStepWitness":
      return { ...g, recursive: map(g.recursive) };
    default:
      return g;
  }
}

export type ProjectExpr =
  | { kind: "TemplateArgument"; slot: number; valueType: ValueType }
  | { kind: "Field"; source: FieldRef }
  | { kind: "RecordField"; source: FieldRef; path: string[] }
  | { kind: "Literal"; value: LiteralValue }
  | { kind: "TypedLiteral"; value: LiteralValue; valueType: ValueType }
  | { kind: "Null"; valueType: ValueType }
  | { kind: "Nullable"; source: FieldRef }
  | { kind: "NullableFlat"; source: FieldRef }
  | { kind: "EnumTagRemap"; source: FieldRef; tags: (number | undefined)[] }
  | { kind: "EnumRemap"; source: FieldRef; tags: (number | undefined)[] }
  | {
      kind: "RecursiveEnumRemap";
      source: FieldRef;
      target: ValueType;
      remaps: RecursiveEnumRemaps;
      omitUnrepresentable: boolean;
    };

export function projectExprSource(e: ProjectExpr): FieldRef | undefined {
  return "source" in e ? e.source : undefined;
}

export interface ProjectField {
  expression: ProjectExpr;
  outputName: string;
  outputIdentity: FieldIdentity;
}

const nameId = (n: string): FieldIdentity => ({ kind: "Name", name: n });
const pf = (
  expression: ProjectExpr,
  outputName: string,
  outputIdentity = nameId(outputName),
): ProjectField => ({
  expression,
  outputName,
  outputIdentity,
});

export const ProjectField = {
  recordField: (source: string, path: string[], outputName: string) =>
    pf({ kind: "RecordField", source: FieldRef.name(source), path }, outputName),
  named: (name: string) => pf({ kind: "Field", source: FieldRef.name(name) }, name),
  namedWithIdentity: (name: string, identity: FieldIdentity) =>
    pf({ kind: "Field", source: FieldRef.name(name) }, name, identity),
  renamed: (source: string, outputName: string) =>
    pf({ kind: "Field", source: FieldRef.name(source) }, outputName),
  renamedResolved: (idx: number, outputName: string) =>
    pf({ kind: "Field", source: FieldRef.resolved(idx) }, outputName),
  nullableFlatResolved: (idx: number, outputName: string) =>
    pf({ kind: "NullableFlat", source: FieldRef.resolved(idx) }, outputName),
  renamedWithIdentity: (source: string, outputName: string, identity: FieldIdentity) =>
    pf({ kind: "Field", source: FieldRef.name(source) }, outputName, identity),
  literal: (outputName: string, value: LiteralValue) => pf({ kind: "Literal", value }, outputName),
  literalWithIdentity: (outputName: string, value: LiteralValue, identity: FieldIdentity) =>
    pf({ kind: "Literal", value }, outputName, identity),
  literalTyped: (outputName: string, value: LiteralValue, valueType: ValueType) =>
    pf({ kind: "TypedLiteral", value, valueType }, outputName),
  null: (outputName: string) =>
    ProjectField.nullTyped(outputName, { kind: "Nullable", inner: { kind: "Bytes" } } as ValueType),
  nullTyped: (outputName: string, valueType: ValueType) =>
    pf({ kind: "Null", valueType }, outputName),
  nullable: (source: string, outputName: string) =>
    pf({ kind: "Nullable", source: FieldRef.name(source) }, outputName),
  nullableResolved: (idx: number, outputName: string) =>
    pf({ kind: "Nullable", source: FieldRef.resolved(idx) }, outputName),
  nullableFlat: (source: string, outputName: string) =>
    pf({ kind: "NullableFlat", source: FieldRef.name(source) }, outputName),
  enumTagRemap: (source: string, outputName: string, tags: (number | undefined)[]) =>
    pf({ kind: "EnumTagRemap", source: FieldRef.name(source), tags }, outputName),
  enumRemap: (source: string, outputName: string, tags: (number | undefined)[]) =>
    pf({ kind: "EnumRemap", source: FieldRef.name(source), tags }, outputName),
  recursiveEnumRemap: (
    source: string,
    outputName: string,
    target: ValueType,
    remaps: RecursiveEnumRemaps,
    omit = false,
  ) =>
    pf(
      {
        kind: "RecursiveEnumRemap",
        source: FieldRef.name(source),
        target,
        remaps,
        omitUnrepresentable: omit,
      },
      outputName,
    ),
  recursiveEnumRemapOmittingUnrepresentable: (
    source: string,
    outputName: string,
    target: ValueType,
    remaps: RecursiveEnumRemaps,
  ) => ProjectField.recursiveEnumRemap(source, outputName, target, remaps, true),
  source: (f: ProjectField) => projectExprSource(f.expression),
};

function collectProjectionOutputType(input: ValueType, projection: CollectByProjection): ValueType {
  if (projection.unwrapNullable && input.kind === "Nullable") return input.inner;
  return input;
}

// Operators.
export type ArrangeOp = { fields: string[]; comparison: ValueComparison };
export type RecursiveStepWitnessOp = Record<string, never>;

export type OpType =
  | { kind: "TableSource"; op: TableSourceOp }
  | { kind: "IndexSource"; op: IndexSourceOp }
  | { kind: "InlineRecords"; op: InlineRecordsOp }
  | { kind: "FrontierSource"; op: FrontierSourceOp }
  | { kind: "BindingSource"; op: BindingSourceOp }
  | { kind: "Arrange"; op: ArrangeOp }
  | { kind: "ArgMaxBy"; op: ArgMaxByOp }
  | { kind: "ArgMinBy"; op: ArgMinByOp }
  | { kind: "TopBy"; op: TopByOp }
  | { kind: "CollectBy"; op: CollectByOp }
  | { kind: "Recursive"; op: RecursiveOp }
  | { kind: "RecursiveStepWitness"; op: RecursiveStepWitnessOp }
  | { kind: "Persist"; op: PersistOp }
  | { kind: "Filter"; op: FilterOp }
  | { kind: "MapProject"; op: MapProjectOp }
  | { kind: "StreamingChecksum"; op: StreamingChecksumOp }
  | { kind: "UnwrapNullable"; op: UnwrapNullableOp }
  | { kind: "Unnest"; op: UnnestOp }
  | { kind: "VariantProject"; op: VariantProjectOp }
  | { kind: "IndexBy"; op: IndexByOp }
  | { kind: "Join" | "SemiJoin" | "AntiJoin"; op: JoinOp }
  | { kind: "Lookup"; op: LookupOp }
  | { kind: "Union" | "Negate" | "Distinct" }
  | { kind: "Aggregate"; op: AggregateOp };

/** Identity of a deduplicated graph node (64-bit FNV-1a of the descriptor). */
export type NodeId = bigint;

export type NodeOutput =
  | { kind: "Records"; records: RecordDescriptor }
  | { kind: "Arrangement"; records: RecordDescriptor };
export const NodeOutput = {
  records: (records: RecordDescriptor): NodeOutput => ({ kind: "Records", records }),
  arrangement: (records: RecordDescriptor): NodeOutput => ({ kind: "Arrangement", records }),
};

export type NodeDurability = { kind: "Ephemeral" } | { kind: "Durable"; storage: DurableStorage };

export interface DurableStorage {
  columnFamily: string;
  keyPrefix: Uint8Array;
  name: string;
}

export type Retainer =
  | { kind: "Subscription"; id: string }
  | { kind: "Hydration"; id: string }
  | { kind: "PreparedShape"; id: string }
  | { kind: "DurableSchemaObject"; id: string };

const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK64 = 0xffffffffffffffffn;

function stableHash(bytes: Uint8Array): bigint {
  let hash = FNV_OFFSET;
  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & MASK64;
  }
  return hash;
}

export class NodeDescriptor {
  constructor(
    readonly operator: OpType,
    readonly inputs: NodeId[],
    readonly output: NodeOutput,
  ) {}

  nodeId(): NodeId {
    return NodeDescriptor.nodeIdOf(this.operator, this.inputs, this.output);
  }

  static nodeIdOf(operator: OpType, inputs: NodeId[], output: NodeOutput): NodeId {
    return stableHash(utf8(structKey([operator, inputs, output])));
  }

  structKey(): string {
    return structKey([this.operator, this.inputs, this.output]);
  }

  validate(inputOutputs: NodeOutput[]): void {
    if (this.inputs.length !== inputOutputs.length) {
      throw GraphValidationError.inputDescriptorArityMismatch(
        this.inputs.length,
        inputOutputs.length,
      );
    }
    const op = this.operator;
    const arrangementConsumer = [
      "Join",
      "SemiJoin",
      "AntiJoin",
      "ArgMaxBy",
      "ArgMinBy",
      "TopBy",
      "CollectBy",
      "Aggregate",
    ].includes(op.kind);
    if (arrangementConsumer) expectArrangementInputs(inputOutputs);
    else if (inputOutputs.some((i) => i.kind === "Arrangement"))
      throw GraphValidationError.invalidNodeOutput();
    const inputs = inputOutputs.map((o) => o.records);
    const output = this.output.records;
    if (op.kind !== "Arrange" && this.output.kind === "Arrangement")
      throw GraphValidationError.invalidNodeOutput();
    const inFields = () => inputs[0].fields();
    const checkIdx = (idx: number, len = inFields().length) => {
      if (idx >= len) throw GraphValidationError.fieldIndexOutOfBounds(idx, len);
    };
    switch (op.kind) {
      case "TableSource":
      case "IndexSource":
      case "InlineRecords":
      case "FrontierSource":
      case "BindingSource":
        return expectArity(this.inputs, 0);
      case "Arrange":
        expectArity(this.inputs, 1);
        if (inputOutputs[0].kind !== "Records" || this.output.kind !== "Arrangement")
          throw GraphValidationError.invalidNodeOutput();
        return expectSameOutput(output, inputs[0]);
      case "Filter":
      case "Distinct":
      case "Negate":
        expectArity(this.inputs, 1);
        return expectSameOutput(output, inputs[0]);
      case "ArgMaxBy":
      case "ArgMinBy":
        expectArity(this.inputs, 1);
        expectArrangementInputs(inputOutputs);
        expectSameOutput(output, inputs[0]);
        [...op.op.groupFieldIndices, ...op.op.comparisonFieldIndices].forEach((i) => checkIdx(i));
        return;
      case "TopBy":
        expectArity(this.inputs, 1);
        expectArrangementInputs(inputOutputs);
        expectSameOutput(output, inputs[0]);
        [...op.op.groupFieldIndices, ...op.op.sortFieldIndices].forEach((i) => checkIdx(i));
        for (const i of op.op.sortFieldIndices) {
          if (!collectByOrderedScalar(inFields()[i].valueType))
            throw GraphValidationError.topBySortFieldMustBeOrderable();
        }
        return;
      case "CollectBy":
        expectArity(this.inputs, 1);
        expectArrangementInputs(inputOutputs);
        return validateCollectBy(op.op, inputs[0], output);
      case "UnwrapNullable":
        expectArity(this.inputs, 1);
        return checkIdx(op.op.fieldIdx);
      case "Unnest":
        expectArity(this.inputs, 1);
        return checkIdx(op.op.arrayFieldIdx);
      case "VariantProject":
        expectArity(this.inputs, 1);
        return checkIdx(op.op.fieldIdx);
      case "MapProject": {
        expectArity(this.inputs, 1);
        for (const [, idx] of op.op.mapping) checkIdx(idx);
        const outLen = output.fields().length;
        if (op.op.expressions.length && op.op.expressions.length !== outLen)
          throw GraphValidationError.outputFieldCountMismatch(op.op.expressions.length, outLen);
        if (!op.op.expressions.length && op.op.mapping.length !== outLen)
          throw GraphValidationError.outputFieldCountMismatch(op.op.mapping.length, outLen);
        return;
      }
      case "StreamingChecksum":
        expectArity(this.inputs, 1);
        checkIdx(op.op.fieldIdx);
        if (op.op.windowBytes === 0 || op.op.maxBytesPerTurn === 0)
          throw GraphValidationError.outputDescriptorMismatch();
        return;
      case "IndexBy":
        expectArity(this.inputs, 1);
        [...op.op.keyFields, ...op.op.valueFields].forEach((i) => checkIdx(i));
        return;
      case "Persist":
        expectArity(this.inputs, 1);
        expectSameOutput(output, inputs[0]);
        op.op.keyFields.forEach((i) => checkIdx(i, output.fields().length));
        return;
      case "Join":
      case "SemiJoin":
      case "AntiJoin":
        expectArity(this.inputs, 2);
        expectArrangementInputs(inputOutputs);
        if (op.op.leftDescriptor !== inputs[0] || op.op.rightDescriptor !== inputs[1])
          throw GraphValidationError.joinInputDescriptorMismatch();
        if (op.op.leftKey.length !== op.op.rightKey.length)
          throw GraphValidationError.joinKeyArityMismatch(
            op.op.leftKey.length,
            op.op.rightKey.length,
          );
        return;
      case "Union":
        for (const i of inputs) expectSameOutput(output, i);
        return;
      case "Aggregate":
        expectArity(this.inputs, 1);
        expectArrangementInputs(inputOutputs);
        op.op.groupFieldIndices.forEach((i) => checkIdx(i));
        return;
      case "Recursive":
        return expectArity(this.inputs, op.op.stepWitnessOutput ? 3 : 2);
      case "RecursiveStepWitness":
        return expectArity(this.inputs, 1);
      case "Lookup":
        expectArity(this.inputs, 1);
        if (op.op.keyDescriptor !== inputs[0])
          throw GraphValidationError.joinInputDescriptorMismatch();
        op.op.keyFields.forEach((i) => checkIdx(i));
        return;
    }
  }
}

function validateCollectBy(
  c: CollectByOp,
  input: RecordDescriptor,
  output: RecordDescriptor,
): void {
  const inFields = input.fields();
  const outFields = output.fields();
  const mismatch = () => GraphValidationError.collectByOutputDescriptorMismatch();
  const inField = (idx: number) => {
    const f = inFields[idx];
    if (!f) throw GraphValidationError.fieldIndexOutOfBounds(idx, inFields.length);
    return f;
  };
  const checkProjections = (targets: any[], projections: CollectByProjection[]) => {
    projections.forEach((projection, i) => {
      const target = targets[i];
      if (!target) return;
      if (
        target.name !== projection.outputName ||
        !structEq(
          collectProjectionOutputType(inField(projection.fieldIdx).valueType, projection),
          target.valueType,
        )
      )
        throw mismatch();
    });
  };
  const checkScalarKeys = (idxs: number[]) => {
    for (const idx of idxs) {
      const t = inField(idx).valueType;
      if (!collectByOrderedScalar(t) || valueTypeContainsRecord(t))
        throw GraphValidationError.collectByKeyFieldMustBeScalar();
    }
  };
  const windowMismatch = () =>
    c.sortFieldIndices.length !== c.sortDirections.length ||
    c.sortFieldIndices.length !== c.orderFields.length + c.tieFields.length ||
    !c.orderFields.length ||
    !c.tieFields.length;
  const collectionType = (d: RecordDescriptor) => ({
    kind: "Array",
    inner: { kind: "Record", descriptor: d },
  });

  if (c.mode === "Expand") {
    if (
      !c.tupleFields.length ||
      !c.occurrenceIdFieldIndices.length ||
      windowMismatch() ||
      outFields.length !== c.tupleFields.length
    )
      throw mismatch();
    for (const idx of [
      ...c.groupFieldIndices,
      ...c.sortFieldIndices,
      ...c.occurrenceIdFieldIndices,
      ...c.tupleFields.map((f) => f.fieldIdx),
    ])
      inField(idx);
    checkProjections(outFields, c.tupleFields);
    return checkScalarKeys([
      ...c.groupFieldIndices,
      ...c.sortFieldIndices,
      ...c.occurrenceIdFieldIndices,
    ]);
  }
  if (c.mode === "Root") {
    if (
      c.slots.length ||
      c.childFields.length ||
      c.collectionField ||
      outFields.length !== c.parentFields.length ||
      windowMismatch()
    )
      throw mismatch();
    checkProjections(outFields, c.parentFields);
    return checkScalarKeys([...c.groupFieldIndices, ...c.sortFieldIndices]);
  }
  if (c.slots.length) {
    if (outFields.length !== c.parentFields.length + c.slots.length) throw mismatch();
    checkProjections(outFields.slice(0, c.parentFields.length), c.parentFields);
    c.slots.forEach((slot, i) => {
      const f = outFields[c.parentFields.length + i];
      if (
        f.name !== slot.collectionField ||
        !structEq(f.valueType, collectionType(slot.childDescriptor))
      )
        throw mismatch();
    });
    return;
  }
  if (c.collectionFieldIndex >= outFields.length)
    throw GraphValidationError.fieldIndexOutOfBounds(c.collectionFieldIndex, outFields.length);
  if (!structEq(outFields[c.collectionFieldIndex].valueType, collectionType(c.childDescriptor)))
    throw mismatch();
  if (
    outFields.length !== c.parentFields.length + 1 ||
    outFields[c.collectionFieldIndex].name !== c.collectionField
  )
    throw mismatch();
  for (const idx of [
    ...c.groupFieldIndices,
    ...c.sortFieldIndices,
    ...c.parentFields.map((f) => f.fieldIdx),
    ...c.childFields.map((f) => f.fieldIdx),
  ])
    inField(idx);
  if (
    c.collectionFieldIndex !== c.parentFields.length ||
    c.childDescriptor.fields().length !== c.childFields.length ||
    windowMismatch()
  )
    throw mismatch();
  checkProjections(outFields.slice(0, c.parentFields.length), c.parentFields);
  checkProjections(c.childDescriptor.fields(), c.childFields);
  checkScalarKeys([...c.groupFieldIndices, ...c.sortFieldIndices]);
}

function expectArity(inputs: NodeId[], expected: number): void {
  if (inputs.length !== expected)
    throw GraphValidationError.inputArityMismatch(expected, inputs.length);
}
function expectSameOutput(expected: RecordDescriptor, actual: RecordDescriptor): void {
  if (!expected.registryCompatibleWith(actual))
    throw GraphValidationError.outputDescriptorMismatch();
}
function expectArrangementInputs(inputs: NodeOutput[]): void {
  if (!inputs.every((i) => i.kind === "Arrangement"))
    throw GraphValidationError.invalidNodeOutput();
}

export class GraphValidationError extends JazzError {
  static invalidNodeOutput = () =>
    new GraphValidationError("InvalidNodeOutput", "operator has an incompatible typed output");
  static fieldIndexOutOfBounds = (index: number, len: number) =>
    new GraphValidationError(
      "FieldIndexOutOfBounds",
      `field index ${index} out of bounds for ${len} fields`,
    );
  static inputArityMismatch = (expected: number, actual: number) =>
    new GraphValidationError("InputArityMismatch", `expected ${expected} inputs, got ${actual}`);
  static inputDescriptorArityMismatch = (inputs: number, descriptors: number) =>
    new GraphValidationError(
      "InputDescriptorArityMismatch",
      `input count ${inputs} does not match descriptor count ${descriptors}`,
    );
  static joinInputDescriptorMismatch = () =>
    new GraphValidationError("JoinInputDescriptorMismatch", "join input descriptors do not match");
  static joinKeyArityMismatch = (left: number, right: number) =>
    new GraphValidationError(
      "JoinKeyArityMismatch",
      `join key arity mismatch: ${left} vs ${right}`,
    );
  static outputDescriptorMismatch = () =>
    new GraphValidationError("OutputDescriptorMismatch", "output descriptor mismatch");
  static outputFieldCountMismatch = (expected: number, actual: number) =>
    new GraphValidationError(
      "OutputFieldCountMismatch",
      `expected ${expected} output fields, got ${actual}`,
    );
  static collectByOutputDescriptorMismatch = () =>
    new GraphValidationError(
      "CollectByOutputDescriptorMismatch",
      "collect_by output descriptor does not match its projection and collection slot",
    );
  static collectByKeyFieldMustBeScalar = () =>
    new GraphValidationError(
      "CollectByKeyFieldMustBeScalar",
      "collect_by group, order, and tie fields must be scalar and non-record-valued",
    );
  static topBySortFieldMustBeOrderable = () =>
    new GraphValidationError(
      "TopBySortFieldMustBeOrderable",
      "top_by order and tie fields must be orderable scalar values",
    );
  static collectByInputIsTerminal = () =>
    new GraphValidationError(
      "CollectByInputIsTerminal",
      "collect_by is terminal-only and cannot be an input node",
    );
}

export class MissingNodeError extends Error {
  constructor(readonly node: NodeId) {
    super(`missing IVM node ${node}`);
  }
}

/** One deduplicated node plus reverse edges for graph maintenance. */
export class GraphNode {
  readonly id: NodeId;
  children = new Set<NodeId>();
  constructor(
    readonly descriptor: NodeDescriptor,
    readonly durability: NodeDurability,
  ) {
    this.id = descriptor.nodeId();
  }
  isDurable(): boolean {
    return this.durability.kind === "Durable";
  }
}

function addSource<K>(map: Map<K, Set<NodeId>>, key: K, id: NodeId) {
  let set = map.get(key);
  if (!set) map.set(key, (set = new Set()));
  set.add(id);
}
function removeSource<K>(map: Map<K, Set<NodeId>>, key: K, id: NodeId) {
  const set = map.get(key);
  if (set && (set.delete(id), set.size === 0)) map.delete(key);
}

function sourceKeyOf(
  op: OpType,
): [kind: "table" | "binding" | "frontier", key: string] | undefined {
  switch (op.kind) {
    case "TableSource":
    case "IndexSource":
      return ["table", op.op.table];
    case "Lookup":
      return ["table", op.op.source.table];
    case "BindingSource":
      return ["binding", structKey(op.op.key)];
    case "FrontierSource":
      return ["frontier", op.op.binding];
    default:
      return undefined;
  }
}

/** Deduplicated DAG of IVM node descriptors. */
export class IvmGraph {
  private activations = new ActivationCache();
  private executionLayouts = new ExecutionLayoutCache();
  private nodesById = new Map<NodeId, GraphNode>();
  private tableSources = new Map<string, Set<NodeId>>();
  private bindingSources = new Map<string, Set<NodeId>>();
  private frontierSources = new Map<string, Set<NodeId>>();
  private routeIndex = new RouteIndex();
  private addedSinceDrain: NodeId[] = [];

  routes(): RouteIndex {
    return this.routeIndex;
  }

  addRouteBarrier(
    terminal: NodeId,
    barrier: NodeId,
    fieldIndices: number[],
    fieldTypes: ValueType[],
    key: Uint8Array,
    rootOrderingNode?: NodeId,
  ): void {
    if (this.routeIndex.add(terminal, barrier, fieldIndices, fieldTypes, key, rootOrderingNode))
      this.activations.clear();
  }

  releaseRouteBarrier(barrier: NodeId): void {
    if (this.routeIndex.release(barrier)) this.activations.clear();
  }

  /** Every descendant of `roots`, including those behind route barriers. */
  downstreamThroughRoutes(roots: Iterable<NodeId>): Set<NodeId> {
    const reached = new Set<NodeId>();
    const pending = [...roots];
    while (pending.length) {
      const id = pending.pop()!;
      if (reached.has(id)) continue;
      reached.add(id);
      const node = this.nodesById.get(id);
      if (node) pending.push(...node.children);
    }
    return reached;
  }

  affectedNodesThroughRoutes(
    tables: Iterable<string>,
    bindings: Iterable<BindingSourceKey>,
  ): Set<NodeId> {
    return this.downstreamThroughRoutes(this.sourceNodes(tables, bindings));
  }

  executionLayout(roots: Iterable<NodeId>): ExecutionLayout {
    return this.executionLayouts.get(this, roots);
  }

  executionLayoutCounters(): [number, number] {
    return this.executionLayouts.counters();
  }

  dedupNode(descriptor: NodeDescriptor, durability: NodeDurability): NodeId {
    const id = descriptor.nodeId();
    const existing = this.nodesById.get(id);
    if (existing) {
      if (existing.descriptor.structKey() !== descriptor.structKey())
        throw new JazzError("Invariant", "IVM node id collision for incompatible descriptors");
      return id;
    }
    this.validateNode(descriptor);
    return this.insertNewNode(id, descriptor, durability);
  }

  dedupNodeParts(
    operator: OpType,
    inputs: NodeId[],
    output: NodeOutput,
    durability: NodeDurability,
  ): NodeId {
    return this.dedupNode(new NodeDescriptor(operator, inputs, output), durability);
  }

  private insertNewNode(
    id: NodeId,
    descriptor: NodeDescriptor,
    durability: NodeDurability,
  ): NodeId {
    this.addedSinceDrain.push(id);
    if (durability.kind === "Durable") this.activations.clear();
    else this.activations.added(descriptor.inputs);
    for (const input of descriptor.inputs) this.nodesById.get(input)?.children.add(id);
    const source = sourceKeyOf(descriptor.operator);
    if (source) addSource(this.sourceMap(source[0]), source[1], id);
    this.nodesById.set(id, new GraphNode(descriptor, durability));
    return id;
  }

  private sourceMap(kind: "table" | "binding" | "frontier") {
    return kind === "table"
      ? this.tableSources
      : kind === "binding"
        ? this.bindingSources
        : this.frontierSources;
  }

  /** Validate a descriptor against the graph's already-built inputs. */
  validateNode(descriptor: NodeDescriptor): void {
    const inputOutputs = descriptor.inputs.flatMap((i) => {
      const n = this.nodesById.get(i);
      return n ? [n.descriptor.output] : [];
    });
    descriptor.validate(inputOutputs);
    if (
      descriptor.inputs.some((i) => this.nodesById.get(i)?.descriptor.operator.kind === "CollectBy")
    )
      throw GraphValidationError.collectByInputIsTerminal();
  }

  node(id: NodeId): GraphNode | undefined {
    return this.nodesById.get(id);
  }

  nodeOrThrow(id: NodeId): GraphNode {
    const node = this.nodesById.get(id);
    if (!node) throw new MissingNodeError(id);
    return node;
  }

  nodes(): Map<NodeId, GraphNode> {
    return this.nodesById;
  }

  affectedNodes(tables: Iterable<string>, bindings: Iterable<BindingSourceKey>): Set<NodeId> {
    return this.activationPlan(tables, bindings).affected;
  }

  activationPlan(tables: Iterable<string>, bindings: Iterable<BindingSourceKey>): ActivationPlan {
    return this.activations.get(this, this.sourceNodes(tables, bindings));
  }

  private sourceNodes(tables: Iterable<string>, bindings: Iterable<BindingSourceKey>): NodeId[] {
    const out: NodeId[] = [];
    for (const t of tables) out.push(...(this.tableSources.get(t) ?? []));
    for (const b of bindings) {
      out.push(...(this.bindingSources.get(structKey(b)) ?? []));
      const name = BindingKey.preparedName(b);
      if (name !== undefined) out.push(...(this.frontierSources.get(name) ?? []));
    }
    return out;
  }

  takeAddedNodes(): NodeId[] {
    const out = this.addedSinceDrain;
    this.addedSinceDrain = [];
    return out;
  }

  markAncestors(id: NodeId, retained: Set<NodeId>): void {
    const pending = [id];
    while (pending.length) {
      const next = pending.pop()!;
      if (retained.has(next)) continue;
      retained.add(next);
      const node = this.nodesById.get(next);
      if (node) pending.push(...node.descriptor.inputs);
    }
  }

  removeNode(id: NodeId): void {
    this.activations.removed(id);
    if (this.routeIndex.remove(id)) this.activations.clear();
    const node = this.nodesById.get(id);
    if (!node) return;
    this.nodesById.delete(id);
    this.executionLayouts.invalidate(id);
    for (const input of node.descriptor.inputs) this.nodesById.get(input)?.children.delete(id);
    const source = sourceKeyOf(node.descriptor.operator);
    if (source) removeSource(this.sourceMap(source[0]), source[1], id);
  }

  clone(): IvmGraph {
    const g = new IvmGraph();
    for (const [id, n] of this.nodesById) {
      const c = new GraphNode(n.descriptor, n.durability);
      c.children = new Set(n.children);
      g.nodesById.set(id, c);
    }
    for (const [src, dst] of [
      [this.tableSources, g.tableSources],
      [this.bindingSources, g.bindingSources],
      [this.frontierSources, g.frontierSources],
    ] as const)
      for (const [k, v] of src) dst.set(k, new Set(v));
    g.routeIndex = this.routeIndex.clone();
    g.addedSinceDrain = this.addedSinceDrain.slice();
    return g;
  }
}
