// Translation from analyzed plans to executable Groove operators.
import { SortedMap, SortedSet, cmpStr } from "../../../support/collections.js";
import { uuidFromString, uuidToString } from "../../../support/index.js";
import {
  authorClaimPathKey,
  authorPolicyClaims,
  composeUnionArmPath,
  decodeValue,
  FieldIdentity,
  FieldRef,
  GraphBuilder,
  GrooveAggregateExpr,
  GroovePlanExpr,
  GroovePredicateExpr,
  LiteralValue,
  policyClaimAtPath,
  PredicateKind,
  ProjectExpr,
  ProjectField,
  RecordDescriptor,
  TopByLimit,
  TopByOrder,
  ValueComparison,
  type ColumnType,
  type Value,
  type ValueType,
} from "../../../_extern/engine.js";
import {
  aggregateOutputField,
  claimParamField,
  claimPathFromParamField,
  LEFT_JOIN_PREFIX,
  leftField,
  rightField,
  routeParamField,
  routeParamFromField,
  USER_COLUMN_PREFIX,
  userColumnField,
} from "../fields.js";
import {
  dbg,
  structEq,
  structMap,
  type AggregateExpr,
  type ClaimPath,
  type ComparisonOp,
  type NormalizedRowSetShape,
  type NormalizedValueRef,
  type OrderKey,
  type PredicateExpr,
  type ProvenanceField,
  type RowProjection,
  type SourceId,
  type ValueSourceColumn,
  type ValueSourceMode,
} from "../input.js";
import type { PolicyContext } from "../policy.js";
import {
  operatorGap,
  runtimeGap,
  UnsupportedError,
  type ResolvedSource,
  type UnsupportedReason,
} from "../resolver.js";
import { claimBindingSlot } from "../lowering.js";
import type { LoweringContext } from "./arguments.js";
import {
  linearRootSource,
  recursiveSeedSource,
  recursiveStepSource,
  relationRootSource,
  type AnalyzedQueryPlan,
  type CorrelatedPathPlan,
  type LinearCurrentRoot,
  type LinearRoot,
  type LinearStep,
  type RecursiveRelationPlan,
  type RelationInputPlan,
  type UnionPlan,
} from "./planning.js";
import {
  collectWindowSourceField,
  projectSourceFieldsFromPrefix,
  projectSourceFieldsFromPrefixRewrappingNullable,
  projectSourceFieldsWithRoutes,
  routeLiteralProjectField,
} from "./terminals.js";

export type Sources = SortedMap<SourceId, ResolvedSource>;
export type LoweredCache = Map<RelationInputPlan, LoweredRelationInput>;
export const strSet = (items?: Iterable<string>) => new SortedSet<string>(cmpStr, items);
export const strMap = <V>(entries?: Iterable<[string, V]>) =>
  new SortedMap<string, V>(cmpStr, entries);

// Groove returns RecursiveIterationLimit instead of silently truncating at this bound.
const FIXPOINT_MAX_ITERS = 128;

export type LoweredRelationInput = {
  graph: GraphBuilder;
  rootSource?: ResolvedSource;
  fields: SortedSet<string>;
  nullableFields: SortedSet<string>;
  nullableFieldDepths: SortedMap<string, number>;
  /** Hidden `(stable union-arm label, source row UUID)` identity of UNION ALL inputs. */
  unionOccurrenceCarrier?: [string, string];
};

const sourceRelation = (graph: GraphBuilder, source: ResolvedSource): LoweredRelationInput => ({
  graph,
  rootSource: source,
  fields: strSet(sourceFields(source)),
  nullableFields: sourceNullableFields(source),
  nullableFieldDepths: sourceNullableFieldDepths(source),
});

export function lowerPlanSteps(
  graph: GraphBuilder,
  plan: AnalyzedQueryPlan,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  switch (plan.kind) {
    case "Linear":
      return lowerLinearPlanStepsCached(
        graph,
        plan.plan,
        rootSource,
        resolvedSources,
        request,
        undefined,
        undefined,
        request.output.appRows?.projection.kind === "Relation",
      );
    case "Union":
      return lowerUnionPlan(plan.plan, graph, rootSource, resolvedSources, request);
    case "CorrelatedPath":
      return lowerCorrelatedPathPlan(graph, plan.plan, rootSource, resolvedSources, request);
    case "RecursiveRelation":
      return lowerRecursiveRelationCached(
        graph,
        plan.plan,
        rootSource,
        resolvedSources,
        request,
        undefined,
      );
  }
}

function pathChild(path: CorrelatedPathPlan, resolvedSources: Sources): [SourceId, ResolvedSource] {
  const childRoot = linearRootSource(path.child.root);
  if (!childRoot) throw operatorGap("path child must be a source");
  const childSource = resolvedSources.get(childRoot);
  if (!childSource) throw runtimeGap(`path child source ${dbg(childRoot)} was not resolved`);
  return [childRoot, childSource];
}

function pathParentSource(path: CorrelatedPathPlan): SourceId {
  const source = linearRootSource(path.parent.root);
  if (!source) throw operatorGap("path parent must be a source");
  return source;
}

/** Public child projections are applied by the collector after the path. */
const withoutWindowAndProject = (steps: LinearStep[]) =>
  steps.filter((s) => s.kind !== "OrderBy" && s.kind !== "Slice" && s.kind !== "Project");

function lowerCorrelatedPathPlan(
  graph: GraphBuilder,
  path: CorrelatedPathPlan,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  const parent = lowerLinearPlanSteps(graph, path.parent, rootSource, resolvedSources, request);
  const [childRoot, childSource] = pathChild(path, resolvedSources);
  const child = lowerLinearPlanSteps(
    childSource.graph,
    { root: path.child.root, steps: withoutWindowAndProject(path.child.steps) },
    childSource,
    resolvedSources,
    request,
  );
  // Route is part of existence identity whenever both sides carry it.
  const sharedRouteFields = strSet(
    [...rootSource.routingFields].filter((f) => child.fields.has(f)),
  );
  let childGraph = lowerRequiredNestedParentGraph(
    child.graph,
    path.nested,
    childSource,
    resolvedSources,
    request,
  );
  const [parentKey, childKey] = lowerPathKeyPair(
    path.correlation,
    pathParentSource(path),
    rootSource,
    childRoot,
    childSource,
    request,
  );
  const parentDepth = sourceFieldNullableDepth(rootSource, parentKey);
  childGraph = unwrapNullableJoinKey(
    childGraph,
    childKey,
    sourceFieldNullableDepth(childSource, childKey),
  );

  let lowered: LoweredRelationInput;
  switch (path.requirement) {
    case "Optional":
      lowered = sourceRelation(parent.graph, rootSource);
      break;
    case "AtLeastOne": {
      const parentGraph = unwrapNullableJoinKey(parent.graph, parentKey, parentDepth);
      const [parentKeys, childKeys] = correlationKeysWithRoutes(
        parentKey,
        childKey,
        sharedRouteFields,
      );
      lowered = sourceRelation(
        restoreCorrelationParentShape(
          GraphBuilder.semiJoin(parentGraph, childGraph, parentKeys, childKeys),
          rootSource,
          parentKey,
          parentDepth,
        ),
        rootSource,
      );
      break;
    }
    case "MatchCorrelationCardinality": {
      const parentGraph = unwrapNullableJoinKey(parent.graph, parentKey, parentDepth);
      const complete = lowerCardinalityCompleteParentGraph(
        parentGraph,
        childGraph,
        rootSource,
        parentKey,
        childKey,
        sharedRouteFields,
      );
      lowered = sourceRelation(
        restoreCorrelationParentShape(complete, rootSource, parentKey, parentDepth),
        rootSource,
      );
    }
  }
  if (path.outputSteps.length === 0) return lowered;
  return lowerLinearPlanSteps(
    lowered.graph,
    { root: path.parent.root, steps: path.outputSteps },
    rootSource,
    resolvedSources,
    request,
  );
}

/** Requirements of nested relation builders gate their immediate parent rows. */
function lowerRequiredNestedParentGraph(
  parent: GraphBuilder,
  nested: CorrelatedPathPlan[],
  parentSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): GraphBuilder {
  for (const path of nested) {
    if (path.requirement === "Optional") continue;
    parent = lowerCorrelatedPathPlan(parent, path, parentSource, resolvedSources, request).graph;
  }
  return parent;
}

/** Required-match joins unwrap nullable keys; restore the original parent row shape. */
function restoreCorrelationParentShape(
  graph: GraphBuilder,
  source: ResolvedSource,
  key: string,
  nullableDepth: number,
): GraphBuilder {
  if (nullableDepth === 0) {
    return graph.projectFields(projectSourceFieldsWithRoutes(source, source.routingFields));
  }
  for (let i = 0; i < nullableDepth; i++) {
    const fields = projectSourceFieldsWithRoutes(source, source.routingFields);
    for (const field of fields) {
      if (field.outputName === key)
        field.expression = ProjectExpr.Nullable(FieldRef.storedName(key));
    }
    graph = graph.projectFields(fields);
  }
  return graph;
}

function correlationKeysWithRoutes(
  parentKey: string,
  childKey: string,
  sharedRouteFields: SortedSet<string>,
): [string[], string[]] {
  const parentKeys = [parentKey];
  const childKeys = [childKey];
  for (const field of sharedRouteFields) {
    if (!parentKeys.some((p, i) => p === field && childKeys[i] === field)) {
      parentKeys.push(field);
      childKeys.push(field);
    }
  }
  return [parentKeys, childKeys];
}

function lowerCardinalityCompleteParentGraph(
  parent: GraphBuilder,
  child: GraphBuilder,
  rootSource: ResolvedSource,
  parentKey: string,
  childKey: string,
  sharedRouteFields: SortedSet<string>,
): GraphBuilder {
  const parentKeyType = sourceFieldType(rootSource, parentKey);
  if (parentKeyType === undefined) {
    throw operatorGap(
      `match-correlation-cardinality parent key ${dbg(parentKey)} is not projected`,
    );
  }
  const isArrayKey =
    parentKeyType.kind === "Array" ||
    (parentKeyType.kind === "Nullable" && parentKeyType.inner.kind === "Array");
  if (!isArrayKey) {
    const [parentKeys, childKeys] = correlationKeysWithRoutes(
      parentKey,
      childKey,
      sharedRouteFields,
    );
    return GraphBuilder.semiJoin(parent, child, parentKeys, childKeys).projectFields(
      projectSourceFieldsWithRoutes(rootSource, rootSource.routingFields),
    );
  }
  const element = "__jazz_required_correlation_element";
  const rowUuid = rootSource.rowShape.rowUuidField;
  const required = parent.unnest(parentKey, element);
  const coveredFields = projectSourceFieldsFromPrefix(rootSource, "left.");
  coveredFields.push(ProjectField.renamed(leftField(element), element));
  const covered = GraphBuilder.join(required, child, [element], [childKey]).projectFields(
    coveredFields,
  );
  const missing = GraphBuilder.antiJoin(
    required,
    covered,
    [rowUuid, element],
    [rowUuid, element],
  ).projectFields(projectSourceFieldsFromPrefix(rootSource, ""));
  return GraphBuilder.antiJoin(parent, missing, [rowUuid], [rowUuid]);
}

export function lowerCorrelatedPathRelationGraph(
  path: CorrelatedPathPlan,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  const parent = lowerLinearPlanSteps(
    rootSource.graph,
    path.parent,
    rootSource,
    resolvedSources,
    request,
  );
  return lowerCorrelatedPathRelationGraphFromParent(
    path,
    parent.graph,
    rootSource,
    resolvedSources,
    request,
    true,
  );
}

export function lowerCorrelatedPathRelationGraphFromParent(
  path: CorrelatedPathPlan,
  parent: GraphBuilder,
  parentSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  retainChildWindow: boolean,
): LoweredRelationInput {
  const [childRoot, childSource] = pathChild(path, resolvedSources);
  const child = lowerLinearPlanSteps(
    childSource.graph,
    {
      root: path.child.root,
      steps: retainChildWindow
        ? childStepsForRelationEdges(path.child.steps)
        : withoutWindowAndProject(path.child.steps),
    },
    childSource,
    resolvedSources,
    request,
  );
  const childGraph = lowerRequiredNestedParentGraph(
    child.graph,
    path.nested,
    childSource,
    resolvedSources,
    request,
  );
  const [parentKey, childKey] = lowerPathKeyPair(
    path.correlation,
    pathParentSource(path),
    parentSource,
    childRoot,
    childSource,
    request,
  );
  return {
    graph: GraphBuilder.join(
      unwrapNullableJoinKey(parent, parentKey, sourceFieldNullableDepth(parentSource, parentKey)),
      unwrapNullableJoinKey(childGraph, childKey, sourceFieldNullableDepth(childSource, childKey)),
      [parentKey],
      [childKey],
    ),
    fields: strSet(),
    nullableFields: strSet(),
    nullableFieldDepths: strMap(),
  };
}

/** Relation-edge terminals keep the source layout; drop public projections and unordered slices. */
function childStepsForRelationEdges(steps: LinearStep[]): LinearStep[] {
  let previousWasOrderBy = false;
  const filtered: LinearStep[] = [];
  for (const step of steps) {
    if (step.kind === "Project" || (step.kind === "Slice" && !previousWasOrderBy)) {
      previousWasOrderBy = false;
      continue;
    }
    previousWasOrderBy = step.kind === "OrderBy";
    filtered.push(step);
  }
  return filtered;
}

export function unwrapNullableJoinKey(
  graph: GraphBuilder,
  field: string,
  nullableDepth: number,
): GraphBuilder {
  for (let i = 0; i < nullableDepth; i++) graph = graph.unwrapNullable(field);
  return graph;
}

export function lowerRelationInput(
  plan: RelationInputPlan,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  return lowerRelationInputWithRetainedFinalFields(plan, resolvedSources, request, false);
}

function lowerRelationInputWithRetainedFinalFields(
  plan: RelationInputPlan,
  resolvedSources: Sources,
  request: LoweringContext,
  retainFinalProjectInputFields: boolean,
): LoweredRelationInput {
  const lowered: LoweredCache = new Map();
  for (const input of relationInputPostorder(plan)) {
    lowered.set(
      input,
      lowerRelationInputCached(
        input,
        resolvedSources,
        request,
        lowered,
        retainFinalProjectInputFields && input === plan,
      ),
    );
  }
  const result = lowered.get(plan);
  if (!result) throw runtimeGap("relation input was not lowered");
  return result;
}

/** Contributor terminals retain physical witness fields alongside the facade projection. */
export function lowerRelationInputForContributor(
  plan: RelationInputPlan,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  return lowerRelationInputWithRetainedFinalFields(plan, resolvedSources, request, true);
}

/** Lower nested relation inputs iteratively in postorder. */
function relationInputPostorder(root: RelationInputPlan): RelationInputPlan[] {
  const pending: [RelationInputPlan, boolean][] = [[root, false]];
  const ordered: RelationInputPlan[] = [];
  while (pending.length > 0) {
    const [plan, visited] = pending.pop()!;
    if (visited) {
      ordered.push(plan);
      continue;
    }
    pending.push([plan, true]);
    const pushJoins = (steps: LinearStep[]) => {
      for (const step of [...steps].reverse())
        if (step.kind === "Join") pending.push([step.right, false]);
    };
    if (plan.kind === "Linear") pushJoins(plan.plan.steps);
    else if (plan.kind === "Union") {
      for (const branch of [...plan.plan.branches].reverse()) pending.push([branch.plan, false]);
    } else {
      pushJoins(plan.plan.step.steps);
      pushJoins(plan.plan.seed.steps);
    }
  }
  return ordered;
}

function lowerRelationInputCached(
  plan: RelationInputPlan,
  resolvedSources: Sources,
  request: LoweringContext,
  lowered: LoweredCache,
  retainFinalProjectInputFields: boolean,
): LoweredRelationInput {
  switch (plan.kind) {
    case "Linear": {
      const sourceId = linearRootSource(plan.plan.root);
      if (!sourceId) throw operatorGap("linear join input must have a source");
      const source = resolvedSources.get(sourceId);
      if (!source) throw runtimeGap(`join source ${dbg(sourceId)} was not resolved`);
      return lowerLinearPlanStepsCached(
        source.graph,
        plan.plan,
        source,
        resolvedSources,
        request,
        lowered,
        plan.plan,
        retainFinalProjectInputFields,
      );
    }
    case "Union":
      return lowerUnionRelationInputWithPrefixCached(
        plan.plan,
        resolvedSources,
        request,
        undefined,
        lowered,
      );
    case "Recursive": {
      const sourceId = relationRootSource(plan);
      if (!sourceId) throw operatorGap("recursive join input must include a table source");
      const source = resolvedSources.get(sourceId);
      if (!source) throw runtimeGap(`recursive join source ${dbg(sourceId)} was not resolved`);
      return lowerRecursiveRelationCached(
        undefined,
        plan.plan,
        source,
        resolvedSources,
        request,
        lowered,
      );
    }
  }
}

export function lowerUnionRelationInputWithPrefixCached(
  union: UnionPlan,
  resolvedSources: Sources,
  request: LoweringContext,
  prefix: string | undefined,
  lowered: LoweredCache | undefined,
): LoweredRelationInput {
  const outputs: LoweredRelationInput[] = [];
  for (const [plan, label] of unionBranchPlansWithLabels(union, prefix)) {
    if (plan.kind === "Recursive") {
      throw operatorGap("recursive UNION ALL join arms lack a stable finite occurrence carrier");
    }
    if (plan.kind !== "Linear") throw new Error("nested unions are flattened above");
    const linear = plan.plan;
    const sourceId = linearRootSource(linear.root);
    if (!sourceId)
      throw operatorGap("UNION ALL join occurrence identity requires a stable source row");
    const source = resolvedSources.get(sourceId);
    if (!source) throw runtimeGap(`union source ${dbg(sourceId)} was not resolved`);
    const retained: LinearCurrentRoot = { root: linear.root, steps: [...linear.steps] };
    const last = retained.steps[retained.steps.length - 1];
    if (last?.kind === "Project") {
      retained.steps[retained.steps.length - 1] = {
        kind: "Project",
        columns: [
          ...last.columns,
          {
            output: { name: "__union_occurrence_row", ty: { kind: "Uuid" } },
            value: { kind: "RowId", row: { kind: "Source", source: sourceId } },
          },
        ],
      };
    }
    const output = lowerLinearPlanStepsCached(
      source.graph,
      retained,
      source,
      resolvedSources,
      request,
      lowered,
      linear,
      false,
    );
    if (!output.fields.has("__union_occurrence_row")) {
      const rowField = source.rowShape.rowUuidField;
      if (!output.fields.has(rowField)) {
        throw operatorGap("UNION ALL arm projection discarded its stable source row identity");
      }
      output.graph = output.graph.projectFields([
        ...[...output.fields].map((f) => ProjectField.named(f)),
        ProjectField.renamed(rowField, "__union_occurrence_row"),
      ]);
      output.fields.add("__union_occurrence_row");
    }
    output.graph = output.graph.projectFields([
      ...[...output.fields].map((f) => ProjectField.named(f)),
      ProjectField.literal("__union_occurrence_arm", { kind: "String", value: label }),
    ]);
    output.fields.add("__union_occurrence_arm");
    output.unionOccurrenceCarrier = ["__union_occurrence_arm", "__union_occurrence_row"];
    outputs.push(output);
  }
  return lowerUnionInputs(outputs, request);
}

/** Flatten nested UNION ALL inputs without recursion; the full path label is the carrier. */
function unionBranchPlansWithLabels(
  union: UnionPlan,
  prefix: string | undefined,
): [RelationInputPlan, string][] {
  const leaves: [RelationInputPlan, string][] = [];
  const pending: [RelationInputPlan, string][] = [...union.branches]
    .reverse()
    .map((b) => [b.plan, prefix === undefined ? b.label : composeUnionArmPath(prefix, b.label)]);
  while (pending.length > 0) {
    const [plan, label] = pending.pop()!;
    if (plan.kind === "Union") {
      for (const b of [...plan.plan.branches].reverse())
        pending.push([b.plan, composeUnionArmPath(label, b.label)]);
    } else leaves.push([plan, label]);
  }
  return leaves;
}

function lowerUnionPlan(
  union: UnionPlan,
  rootGraph: GraphBuilder | undefined,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  const lowered: LoweredRelationInput[] = [];
  for (const [branchPlan, label] of unionBranchPlansWithLabels(union, undefined)) {
    let input: LoweredRelationInput;
    if (branchPlan.kind === "Linear") {
      const sourceId = linearRootSource(branchPlan.plan.root);
      if (!sourceId) throw operatorGap("union branch must have a source");
      if (!structEq(sourceId, rootSource.rowShape.source)) {
        throw operatorGap("root union branches must share the query result source");
      }
      let graph = rootGraph;
      if (graph === undefined) {
        const resolved = resolvedSources.get(sourceId);
        if (!resolved) throw runtimeGap(`union branch source ${dbg(sourceId)} was not resolved`);
        graph = resolved.graph;
      }
      input = lowerLinearPlanStepsCached(
        graph,
        branchPlan.plan,
        rootSource,
        resolvedSources,
        request,
        undefined,
        undefined,
        true,
      );
    } else input = lowerRelationInputForContributor(branchPlan, resolvedSources, request);
    // Retain an explicit typed `(arm, source-row)` carrier beside the real root.
    const sourceId = relationRootSource(branchPlan);
    if (!sourceId) throw operatorGap("UNION ALL root arm lacks a stable source row");
    const source = resolvedSources.get(sourceId);
    if (!source) throw runtimeGap(`union source ${dbg(sourceId)} was not resolved`);
    const rowField = source.rowShape.rowUuidField;
    if (!input.fields.has(rowField)) {
      throw operatorGap("UNION ALL root arm projection discarded its stable source row identity");
    }
    // Keep the source descriptor's order ahead of public aliases.
    const names = [...sourceFields(source)];
    const nameSet = new Set(names);
    input.graph = input.graph.projectFields([
      ...[...names, ...[...input.fields].filter((f) => !nameSet.has(f))].map((f) =>
        ProjectField.named(f),
      ),
      ProjectField.renamed(rowField, "__root_union_row"),
      ProjectField.literal("__root_union_arm", { kind: "String", value: label }),
    ]);
    input.fields.add("__root_union_row");
    input.fields.add("__root_union_arm");
    input.unionOccurrenceCarrier = ["__root_union_arm", "__root_union_row"];
    lowered.push(input);
  }
  return lowerUnionTerminalSteps(
    lowerUnionInputs(lowered, request),
    union.terminalSteps,
    rootSource,
    request,
  );
}

function lowerUnionTerminalSteps(
  input: LoweredRelationInput,
  steps: LinearStep[],
  rootSource: ResolvedSource,
  request: LoweringContext,
): LoweredRelationInput {
  if (steps.length === 0) return input;
  const root = input.rootSource ?? rootSource;
  const plan: LinearCurrentRoot = {
    root: { kind: "Source", source: root.rowShape.source, visibility: "Visible" },
    steps: [],
  };
  let pendingOrder: any[] | undefined;
  for (const step of steps) {
    if (step.kind === "OrderBy") pendingOrder = step.keys;
    else if (step.kind === "Slice") {
      input.graph = lowerWindow(
        input.graph,
        pendingOrder ?? [],
        step.partitionBy,
        root.routingFields,
        step.limit,
        step.offset,
        step.tieBreaker,
        plan,
        root,
        request,
      );
      pendingOrder = undefined;
    } else throw operatorGap("only global order/window operators may wrap a public UNION ALL");
  }
  if (pendingOrder) {
    input.graph = lowerWindow(
      input.graph,
      pendingOrder,
      [],
      root.routingFields,
      undefined,
      0,
      [{ kind: "RowId", row: { kind: "Source", source: root.rowShape.source } }],
      plan,
      root,
      request,
    );
  }
  return input;
}

function lowerUnionInputs(
  lowered: LoweredRelationInput[],
  request: LoweringContext,
): LoweredRelationInput {
  const unionFields = strSet(lowered.flatMap((b) => [...b.fields]));
  if (lowered.some((b) => !structEq(b.fields, unionFields))) {
    lowered = lowered.map((b) => alignUnionRouteFields(b, unionFields, request));
  }
  const [first, ...rest] = lowered;
  if (!first) throw operatorGap("union row-set nodes require at least one input");
  const graphs = [first.graph];
  let rootSource = first.rootSource;
  const nullableFields = first.nullableFields;
  const nullableFieldDepths = first.nullableFieldDepths;
  for (const branch of rest) {
    if (!structEq(branch.fields, first.fields))
      throw operatorGap("union branches must output the same fields");
    if (!structEq(branch.unionOccurrenceCarrier, first.unionOccurrenceCarrier)) {
      throw operatorGap("union branches must use the same typed occurrence carrier");
    }
    for (const f of branch.nullableFields) nullableFields.add(f);
    for (const [field, depth] of branch.nullableFieldDepths) {
      nullableFieldDepths.set(field, Math.max(nullableFieldDepths.get(field) ?? 0, depth));
    }
    if (!structEq(rootSource?.rowShape.source, branch.rootSource?.rowShape.source))
      rootSource = undefined;
    graphs.push(branch.graph);
  }
  return {
    graph: GraphBuilder.union(graphs),
    rootSource,
    fields: first.fields,
    nullableFields,
    nullableFieldDepths,
    unionOccurrenceCarrier: first.unionOccurrenceCarrier,
  };
}

function alignUnionRouteFields(
  branch: LoweredRelationInput,
  fields: SortedSet<string>,
  request: LoweringContext,
): LoweredRelationInput {
  const routeFields = request.parameterDomain().routingParams;
  const missing = [...fields].filter((f) => !branch.fields.has(f));
  if (missing.some((f) => !routeFields.has(f)))
    throw operatorGap("union branches must output the same fields");
  const bindingShape = request.input.binding.sourceShape;
  if (bindingShape !== undefined) {
    const binding = GraphBuilder.bindingSource(
      bindingShape,
      bindingSourceDescriptorWithUserParams(request, []),
    );
    const projectFields = [...fields].map((field) =>
      branch.fields.has(field)
        ? ProjectField.renamed(leftField(field), field)
        : ProjectField.renamed(rightField(routeParamFromField(field) ?? field), field),
    );
    const existingRouteFields = [...branch.fields].filter((f) => routeFields.has(f));
    const bindingRouteFields = existingRouteFields.map((f) => routeParamFromField(f) ?? f);
    branch.graph = policyJoinIfNeeded(
      branch.graph,
      binding,
      existingRouteFields,
      bindingRouteFields,
      request,
    ).projectFields(projectFields);
  } else {
    branch.graph = branch.graph.projectFields(
      [...fields].map((field) =>
        branch.fields.has(field)
          ? ProjectField.named(field)
          : routeLiteralProjectField(field, request),
      ),
    );
  }
  branch.fields = strSet(fields);
  return branch;
}

function linearRootFields(root: LinearRoot): SortedSet<string> {
  return root.kind === "Source" ? strSet() : strSet(root.columns.map((c) => c.name));
}

export function* sourceFields(source: ResolvedSource): IterableIterator<string> {
  for (const field of source.rowShape.descriptor.fields())
    if (field.name !== undefined) yield field.name;
  yield* source.routingFields;
}

function sourceNullableFields(source: ResolvedSource): SortedSet<string> {
  return strSet(sourceNullableFieldDepths(source).keysArray());
}

function sourceNullableFieldDepths(source: ResolvedSource): SortedMap<string, number> {
  const depths = strMap<number>();
  for (const name of sourceFields(source)) {
    const depth = sourceFieldNullableDepth(source, name);
    if (depth === 0) continue;
    if (name.startsWith(USER_COLUMN_PREFIX))
      depths.set(name.slice(USER_COLUMN_PREFIX.length), depth);
    depths.set(name, depth);
  }
  return depths;
}

export function lowerRecursiveRelationCached(
  rootGraph: GraphBuilder | undefined,
  relation: RecursiveRelationPlan,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  lowered: LoweredCache | undefined,
): LoweredRelationInput {
  // Closure terminals lower the recursion directly; build the same immediate child cache.
  const ownedChildren: LoweredCache = new Map(lowered ?? []);
  for (const linear of [relation.seed, relation.step]) {
    for (const step of linear.steps) {
      if (step.kind === "Join" && !ownedChildren.has(step.right)) {
        ownedChildren.set(step.right, lowerRelationInput(step.right, resolvedSources, request));
      }
    }
  }
  const seedId = recursiveSeedSource(relation);
  const seedRootSource = seedId && resolvedSources.get(seedId);
  const seedGraph = seedRootSource?.graph ?? rootGraph ?? rootSource.graph;
  const seed = lowerLinearPlanStepsCached(
    seedGraph,
    relation.seed,
    seedRootSource ?? rootSource,
    resolvedSources,
    request,
    ownedChildren,
    undefined,
    false,
  );
  const stepSourceId = recursiveStepSource(relation);
  if (!stepSourceId) throw operatorGap("recursive step must include a table source");
  const stepSource = resolvedSources.get(stepSourceId);
  if (!stepSource) throw runtimeGap(`recursive step source ${dbg(stepSourceId)} was not resolved`);
  const step = lowerLinearPlanStepsCached(
    stepSource.graph,
    relation.step,
    stepSource,
    resolvedSources,
    request,
    ownedChildren,
    undefined,
    false,
  );
  // A recursion-owned view of the physical step rows before the final projection.
  const lastStep = relation.step.steps[relation.step.steps.length - 1];
  if (lastStep?.kind !== "Project")
    throw operatorGap("recursive step witness requires a final projection");
  const witnessPlan: LinearCurrentRoot = {
    root: relation.step.root,
    steps: relation.step.steps.slice(0, -1),
  };
  const witnessChildren: LoweredCache = new Map();
  for (const s of witnessPlan.steps) {
    if (s.kind === "Join")
      witnessChildren.set(s.right, lowerRelationInput(s.right, resolvedSources, request));
  }
  const stepWitness = lowerLinearPlanStepsCached(
    stepSource.graph,
    witnessPlan,
    stepSource,
    resolvedSources,
    request,
    witnessChildren,
    witnessPlan,
    false,
  );
  const bound = relation.bound;
  const truncateAtMaxIters = bound.kind === "MaxDepth";
  const maxIters = bound.kind === "Fixpoint" ? FIXPOINT_MAX_ITERS : bound.depth;
  if (!structEq(seed.fields, step.fields)) {
    throw operatorGap("recursive seed and step outputs must have the same fields");
  }
  return {
    graph: GraphBuilder.recursiveWithStepWitness(
      seed.graph,
      step.graph,
      stepWitness.graph,
      relation.frontier,
      maxIters,
      truncateAtMaxIters,
    ),
    rootSource,
    fields: seed.fields,
    nullableFields: strSet(),
    nullableFieldDepths: strMap(),
  };
}

export function lowerLinearPlanSteps(
  graph: GraphBuilder,
  plan: LinearCurrentRoot,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  return lowerLinearPlanStepsCached(
    graph,
    plan,
    rootSource,
    resolvedSources,
    request,
    undefined,
    undefined,
    false,
  );
}

/** Lower a recursive seed for a covered receiver input. */
export function lowerRecursiveSeedMembership(
  relation: RecursiveRelationPlan,
  seedSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): LoweredRelationInput {
  return lowerLinearPlanStepsCached(
    seedSource.graph,
    relation.seed,
    seedSource,
    resolvedSources,
    request,
    undefined,
    undefined,
    true,
  );
}

type LastJoinRight = {
  plan: RelationInputPlan;
  nullableFields: SortedSet<string>;
  nullableFieldDepths: SortedMap<string, number>;
  fields: SortedSet<string>;
};
type AccumulatedJoinFields = SortedMap<[SourceId, string], [string, number]>;

const isBindingRoute = (field: string) =>
  routeParamFromField(field) !== undefined || claimPathFromParamField(field) !== undefined;

function orderFallback(plan: LinearCurrentRoot): NormalizedValueRef[] {
  const source = linearRootSource(plan.root);
  if (!source) throw operatorGap("order fallback must be a source");
  return [{ kind: "RowId", row: { kind: "Source", source } }];
}

export function lowerLinearPlanStepsCached(
  graph: GraphBuilder,
  plan: LinearCurrentRoot,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  lowered: LoweredCache | undefined,
  cachePlan: LinearCurrentRoot | undefined,
  retainFinalProjectInputFields: boolean,
): LoweredRelationInput {
  const isSourceRoot = plan.root.kind === "Source";
  if (plan.root.kind === "Value") {
    graph = lowerValueSource(plan.root.shape, plan.root.columns, plan.root.mode, request);
  } else if (plan.root.kind === "Frontier") {
    graph = GraphBuilder.frontierSource(
      plan.root.frontier,
      valueSourceDescriptor(plan.root.columns),
    );
  }
  let fields =
    plan.root.kind === "Source" ? strSet(sourceFields(rootSource)) : linearRootFields(plan.root);
  let nullableFields = isSourceRoot ? sourceNullableFields(rootSource) : strSet();
  let nullableFieldDepths = isSourceRoot ? sourceNullableFieldDepths(rootSource) : strMap<number>();
  let pendingOrder: OrderKey[] | undefined;
  let lastJoinRight: LastJoinRight | undefined;
  // Earlier inner-join inputs flattened into the left record keep source-qualified addresses.
  const accumulatedJoinFields: AccumulatedJoinFields = structMap();
  // UNION input occurrence identity is the complete `(arm, row)` pair.
  const accumulatedUnionOccurrenceFields = strSet();
  let availableRouteFields = isSourceRoot ? strSet(rootSource.routingFields) : strSet();
  const routeFields = request.parameterDomain().routingParams;

  for (let stepIndex = 0; stepIndex < plan.steps.length; stepIndex++) {
    const step = plan.steps[stepIndex];
    switch (step.kind) {
      case "Filter": {
        lastJoinRight = undefined;
        const source = linearRootSource(plan.root);
        if (!source) throw operatorGap("filters on value/frontier sources are not lowered yet");
        const [joined, residual, introduced] = lowerEqualityParamFilterJoins(
          graph,
          step.predicate,
          source,
          rootSource,
          availableRouteFields,
          request,
        );
        graph = joined;
        for (const f of introduced) {
          fields.add(f);
          availableRouteFields.add(f);
        }
        if (residual.kind !== "True") {
          const predicate = lowerPredicate(residual, source, rootSource, request);
          graph = usesPolicyValueComparison(request)
            ? graph.policyFilter(predicate)
            : graph.filter(predicate);
        }
        break;
      }
      case "Join": {
        const { right, mode, on } = step;
        if (mode !== "Inner" && mode !== "Semi")
          throw operatorGap("join_via only lowers inner/semi joins");
        let loweredRight: LoweredRelationInput;
        if (lowered) {
          const cacheStep = cachePlan?.steps[stepIndex];
          const key = cacheStep?.kind === "Join" ? cacheStep.right : right;
          const hit = lowered.get(key);
          if (!hit) {
            throw runtimeGap(
              `join relation input was not lowered at step ${stepIndex}: right=${dbg(right)}`,
            );
          }
          loweredRight = { ...hit, fields: strSet(hit.fields) };
        } else loweredRight = lowerRelationInput(right, resolvedSources, request);
        const [leftKeys, rightKeys] = lowerLinearJoinKeyPairs(
          on,
          plan.root,
          rootSource,
          right,
          loweredRight,
          accumulatedJoinFields,
          request,
        );
        const unwrappedLeft = new Set<string>();
        for (const key of leftKeys) {
          if (nullableFields.has(key) && !unwrappedLeft.has(key)) {
            unwrappedLeft.add(key);
            graph = unwrapNullableJoinKey(graph, key, nullableFieldDepths.get(key) ?? 1);
          }
        }
        for (const key of unwrappedLeft) {
          nullableFields.delete(key);
          nullableFieldDepths.delete(key);
        }
        const rightNullable = strSet(loweredRight.nullableFields);
        const rightDepths = loweredRight.nullableFieldDepths.clone();
        let rightGraph = loweredRight.graph;
        const unwrappedRight = new Set<string>();
        for (const key of rightKeys) {
          if (loweredRight.nullableFields.has(key) && !unwrappedRight.has(key)) {
            unwrappedRight.add(key);
            rightGraph = unwrapNullableJoinKey(
              rightGraph,
              key,
              loweredRight.nullableFieldDepths.get(key) ?? 1,
            );
          }
        }
        for (const key of unwrappedRight) {
          rightNullable.delete(key);
          rightDepths.delete(key);
        }
        if (mode === "Semi") {
          // Route each left occurrence before applying the existence gate.
          const rightRouteFields = [...routeFields].filter((f) => loweredRight.fields.has(f));
          const missingRouteFields = rightRouteFields.filter((f) => !availableRouteFields.has(f));
          if (missingRouteFields.length > 0) {
            const bindingShape = request.input.binding.sourceShape;
            if (bindingShape !== undefined) {
              const binding = GraphBuilder.bindingSource(
                bindingShape,
                bindingSourceDescriptorWithUserParams(request, []),
              );
              const existing = [...availableRouteFields].filter((f) =>
                rightRouteFields.includes(f),
              );
              const projection = [
                ...[...fields].map((f) => ProjectField.renamed(leftField(f), f)),
                ...missingRouteFields.map((f) =>
                  ProjectField.renamed(rightField(routeParamFromField(f) ?? f), f),
                ),
              ];
              graph = policyJoinIfNeeded(
                graph,
                binding,
                existing,
                existing.map((f) => routeParamFromField(f) ?? f),
                request,
              ).projectFields(projection);
            } else {
              graph = graph.projectFields([
                ...[...fields].map((f) => ProjectField.named(f)),
                ...missingRouteFields.map((f) => routeLiteralProjectField(f, request)),
              ]);
            }
            for (const f of missingRouteFields) {
              fields.add(f);
              availableRouteFields.add(f);
            }
          }
          const rightFields = [...rightKeys];
          const semiLeftKeys = [...leftKeys];
          const semiRightKeys = [...rightKeys];
          for (const field of rightRouteFields) {
            if (!semiRightKeys.includes(field)) {
              rightFields.push(field);
              semiLeftKeys.push(field);
              semiRightKeys.push(field);
            }
          }
          graph = semiJoinIfNeeded(
            graph,
            rightGraph.projectFields(rightFields.map((f) => ProjectField.named(f))),
            semiLeftKeys,
            semiRightKeys,
            request,
          );
          lastJoinRight = undefined;
        } else {
          // A left row must only meet right rows derived for its own binding (#4017).
          const joinLeft = [...leftKeys];
          const joinRight = [...rightKeys];
          for (const field of availableRouteFields) {
            if (
              isBindingRoute(field) &&
              loweredRight.fields.has(field) &&
              !joinRight.includes(field)
            ) {
              joinLeft.push(field);
              joinRight.push(field);
            }
          }
          graph = policyJoinIfNeeded(graph, rightGraph, joinLeft, joinRight, request);
          lastJoinRight = {
            plan: right,
            nullableFields: rightNullable,
            nullableFieldDepths: rightDepths,
            fields: strSet(loweredRight.fields),
          };
        }
        const nextKind = plan.steps[stepIndex + 1]?.kind;
        const nextIsProject = nextKind === "Project";
        const nextIsJoin = nextKind === "Join";
        if (mode === "Inner" && nextIsJoin) {
          // Consecutive joins retain the right input for the next predicate.
          const policySubplan = omitsPublicOccurrenceCarriers(request);
          const last = lastJoinRight!;
          lastJoinRight = undefined;
          const rightSource = relationRootSource(right);
          if (!rightSource)
            throw operatorGap("multi-source flat join requires a source-rooted right input");
          const unionOutputs = new Map<string, string>();
          if (!policySubplan && right.kind === "Union" && loweredRight.unionOccurrenceCarrier) {
            const [armField, rowField] = loweredRight.unionOccurrenceCarrier;
            unionOutputs.set(armField, `__root_join_arm_${stepIndex}`);
            unionOutputs.set(rowField, `__root_join_row_${stepIndex}`);
          }
          const projection = [...fields].map((f) => ProjectField.renamed(leftField(f), f));
          const nextFields = strSet(fields);
          const nextNullable = strSet(nullableFields);
          const nextDepths = nullableFieldDepths.clone();
          // A binding route the right input introduces stays addressable under its own name (#4017).
          for (const field of last.fields) {
            if (isBindingRoute(field) && !fields.has(field)) {
              projection.push(ProjectField.renamed(rightField(field), field));
              nextFields.add(field);
              availableRouteFields.add(field);
            }
          }
          for (const field of last.fields) {
            const output =
              unionOutputs.get(field) ??
              (policySubplan
                ? `__policy_join_source_${stepIndex}_${field}`
                : `__flat_join_source_${stepIndex}_${field}`);
            projection.push(ProjectField.renamed(rightField(field), output));
            if (unionOutputs.has(field)) accumulatedUnionOccurrenceFields.add(output);
            const depth = last.nullableFieldDepths.get(field) ?? 0;
            accumulatedJoinFields.set([rightSource, field], [output, depth]);
            nextFields.add(output);
            if (last.nullableFields.has(field)) {
              nextNullable.add(output);
              nextDepths.set(output, Math.max(depth, 1));
            }
          }
          graph = graph.projectFields(projection);
          fields = nextFields;
          nullableFields = nextNullable;
          nullableFieldDepths = nextDepths;
        }
        if (isSourceRoot && !nextIsProject && !nextIsJoin) {
          const introduced = strSet([...routeFields].filter((f) => loweredRight.fields.has(f)));
          const retainedRoutes = strSet([...availableRouteFields, ...introduced]);
          const projection =
            mode === "Semi"
              ? projectSourceFieldsWithRoutes(rootSource, retainedRoutes)
              : projectLeftSourceFieldsWithJoinRoutes(rootSource, availableRouteFields, introduced);
          const retainedLeft = (field: string) => (mode === "Semi" ? field : leftField(field));
          const occurrenceFields = strSet();
          if (!omitsPublicOccurrenceCarriers(request)) {
            // Earlier UNION joins flattened their occurrence pair into the left input.
            for (const output of accumulatedUnionOccurrenceFields) {
              projection.push(ProjectField.renamed(retainedLeft(output), output));
              occurrenceFields.add(output);
            }
            // Preserve earlier inner-join row ids: terminals distinguish public root occurrences.
            for (const [[sourceId, field], [output]] of accumulatedJoinFields) {
              if (resolvedSources.get(sourceId)?.rowShape.rowUuidField === field) {
                projection.push(ProjectField.renamed(retainedLeft(output), output));
                occurrenceFields.add(output);
              }
            }
          }
          if (mode === "Inner" && !omitsPublicOccurrenceCarriers(request)) {
            // A union's public occurrence is the `(arm, row)` pair.
            if (right.kind === "Union") {
              if (loweredRight.unionOccurrenceCarrier) {
                const [armField, rowField] = loweredRight.unionOccurrenceCarrier;
                const armOutput = `__root_join_arm_${stepIndex}`;
                const rowOutput = `__root_join_row_${stepIndex}`;
                projection.push(ProjectField.renamed(rightField(armField), armOutput));
                projection.push(ProjectField.renamed(rightField(rowField), rowOutput));
                occurrenceFields.add(armOutput);
                occurrenceFields.add(rowOutput);
              }
            } else {
              const rightSourceId = relationRootSource(right);
              if (rightSourceId) {
                const rightSource = resolvedSources.get(rightSourceId);
                if (!rightSource) {
                  throw operatorGap(
                    `inner join occurrence source ${dbg(rightSourceId)} was not resolved`,
                  );
                }
                const components = rightSourceId.path.components;
                const appJoinSource = components[components.length - 1]?.kind === "Alias";
                const rightRow = rightSource.rowShape.rowUuidField;
                if (appJoinSource && lastJoinRight?.fields.has(rightRow)) {
                  const output = `__root_join_row_${stepIndex}`;
                  projection.push(ProjectField.renamed(rightField(rightRow), output));
                  occurrenceFields.add(output);
                }
              }
            }
          }
          // Drop the flattened chain fields this projection did not name.
          graph = graph.projectFields(projection).narrowProjectedJoinChain();
          fields = strSet([
            ...sourceFields(rootSource),
            ...availableRouteFields,
            ...introduced,
            ...occurrenceFields,
          ]);
          nullableFields = sourceNullableFields(rootSource);
          nullableFieldDepths = sourceNullableFieldDepths(rootSource);
          for (const f of introduced) availableRouteFields.add(f);
          lastJoinRight = undefined;
        }
        break;
      }
      case "Project": {
        // Bind source order while the graph still has the source descriptor.
        if (pendingOrder) {
          graph = lowerWindow(
            graph,
            pendingOrder,
            [],
            availableRouteFields,
            undefined,
            0,
            orderFallback(plan),
            plan,
            rootSource,
            request,
          );
          pendingOrder = undefined;
        }
        const retainedContributorFields =
          retainFinalProjectInputFields && stepIndex + 1 === plan.steps.length
            ? strSet(fields)
            : undefined;
        const retainedContributorDepths = retainedContributorFields && nullableFieldDepths.clone();
        const unwrapFields = new Map<string, number>();
        const projectedDepths = strMap<number>();
        const wrapAfterProject = new Map<string, number>();
        const fieldPlans = step.columns.map((column) =>
          lowerProjectionField(
            column,
            plan,
            rootSource,
            fields,
            nullableFieldDepths,
            lastJoinRight,
            accumulatedJoinFields,
            request,
          ),
        );
        const retainedPrefix = lastJoinRight ? LEFT_JOIN_PREFIX : "";
        if (retainedContributorFields) {
          // Retained source fields keep their pre-projection meaning; outputs may only copy them.
          for (const field of fieldPlans) {
            const output = field.project.outputName;
            const source = field.sourceNullability?.[0];
            const isIdentity = source === output || source === `${retainedPrefix}${output}`;
            if (retainedContributorFields.has(output) && !isIdentity) {
              throw operatorGap(
                `projection output ${dbg(output)} would replace the retained source field of the same name`,
              );
            }
          }
        }
        for (const field of fieldPlans) {
          for (const [source, depth] of field.unwrapBeforeProject) {
            unwrapFields.set(source, Math.max(unwrapFields.get(source) ?? 0, depth));
          }
        }
        const projectFields = fieldPlans.map((field) => {
          const outputDepth = field.nullableAfterProject?.[1] ?? 0;
          if (field.sourceNullability) {
            // Every alias sees the same transformed input.
            const [source, sourceDepth] = field.sourceNullability;
            const remaining = Math.max(0, sourceDepth - (unwrapFields.get(source) ?? 0));
            const wrappers = Math.max(0, outputDepth - remaining);
            if (wrappers > 0) {
              field.project = ProjectField.nullable(source, field.project.outputName);
              if (wrappers > 1) wrapAfterProject.set(field.project.outputName, wrappers - 1);
            }
          }
          if (field.nullableAfterProject) projectedDepths.set(...field.nullableAfterProject);
          return field.project;
        });
        for (const [field, depth] of unwrapFields) {
          for (let i = 0; i < depth; i++) graph = graph.unwrapNullable(field);
        }
        const retainedRouteFields = strSet();
        const projectedOutputs = new Set<string>(projectFields.map((f: any) => f.outputName));
        if (lastJoinRight) {
          for (const field of availableRouteFields) {
            if (!projectedOutputs.has(field))
              projectFields.push(ProjectField.renamed(leftField(field), field));
            retainedRouteFields.add(field);
          }
          for (const field of routeFields) {
            if (!lastJoinRight.fields.has(field)) continue;
            if (!projectedOutputs.has(field))
              projectFields.push(ProjectField.renamed(rightField(field), field));
            retainedRouteFields.add(field);
          }
        } else {
          for (const field of availableRouteFields) {
            if (!projectedOutputs.has(field)) projectFields.push(ProjectField.named(field));
            retainedRouteFields.add(field);
          }
        }
        if (retainedContributorFields) {
          // Every retained root descriptor field still lives on the left side of a joined graph.
          for (const field of retainedContributorFields) {
            if (projectedOutputs.has(field)) continue;
            projectedOutputs.add(field);
            const sourceField = `${retainedPrefix}${field}`;
            const declared = retainedContributorDepths!.get(field) ?? 0;
            const removed = Math.min(unwrapFields.get(sourceField) ?? 0, declared);
            // Restore wrappers removed by shared source unwraps.
            if (removed > 0) {
              projectFields.push(ProjectField.nullable(sourceField, field));
              if (removed > 1) wrapAfterProject.set(field, removed - 1);
            } else projectFields.push(ProjectField.renamed(sourceField, field));
          }
        }
        graph = graph.projectFields(projectFields).narrowProjectedJoinChain();
        fields = strSet([
          ...step.columns.map((c) => c.output.name),
          ...retainedRouteFields,
          ...(retainedContributorFields ?? []),
        ]);
        const maxWrap = Math.max(0, ...wrapAfterProject.values());
        for (let layer = 0; layer < maxWrap; layer++) {
          graph = graph.projectFields(
            [...fields].map((name) =>
              (wrapAfterProject.get(name) ?? 0) > layer
                ? ProjectField.nullable(name, name)
                : ProjectField.named(name),
            ),
          );
        }
        nullableFields = strSet(projectedDepths.keysArray());
        nullableFieldDepths = projectedDepths;
        if (retainedContributorDepths) {
          for (const [field, depth] of retainedContributorDepths) {
            if (!fields.has(field)) continue;
            nullableFields.add(field);
            nullableFieldDepths.set(field, Math.max(nullableFieldDepths.get(field) ?? 0, depth));
          }
        }
        availableRouteFields = retainedRouteFields;
        lastJoinRight = undefined;
        break;
      }
      case "OrderBy":
        lastJoinRight = undefined;
        pendingOrder = step.keys;
        break;
      case "Slice":
        lastJoinRight = undefined;
        graph = lowerWindow(
          graph,
          pendingOrder ?? [],
          step.partitionBy,
          availableRouteFields,
          step.limit,
          step.offset,
          step.tieBreaker,
          plan,
          rootSource,
          request,
        );
        pendingOrder = undefined;
        break;
      case "Aggregate": {
        lastJoinRight = undefined;
        if (pendingOrder) throw operatorGap("order-by before aggregate is not lowered yet");
        const aggregate = lowerAggregate(
          graph,
          step.groupBy,
          step.outputs,
          plan,
          rootSource,
          request,
        );
        graph = aggregate.graph;
        fields = aggregate.fields;
        nullableFields = strSet();
        nullableFieldDepths = strMap();
        availableRouteFields = strSet();
      }
    }
  }
  if (pendingOrder) {
    graph = lowerWindow(
      graph,
      pendingOrder,
      [],
      availableRouteFields,
      undefined,
      0,
      orderFallback(plan),
      plan,
      rootSource,
      request,
    );
  }
  return { graph, rootSource, fields, nullableFields, nullableFieldDepths };
}

const usesPolicyValueComparison = (request: LoweringContext) =>
  request.policy.kind === "AuthorizationSubplan";

/** Policy-predicate programs never publish rows, so intermediate join values stay internal. */
const omitsPublicOccurrenceCarriers = (request: LoweringContext) =>
  usesPolicyValueComparison(request) || request.output.appRows?.publicTerminal === false;

export function policyJoinIfNeeded(
  left: GraphBuilder,
  right: GraphBuilder,
  leftOn: string[],
  rightOn: string[],
  request: LoweringContext,
): GraphBuilder {
  return usesPolicyValueComparison(request)
    ? GraphBuilder.policyJoin(left, right, leftOn, rightOn)
    : GraphBuilder.join(left, right, leftOn, rightOn);
}

function semiJoinIfNeeded(
  left: GraphBuilder,
  right: GraphBuilder,
  leftOn: string[],
  rightOn: string[],
  request: LoweringContext,
): GraphBuilder {
  return GraphBuilder.SemiJoin({
    left,
    right,
    leftOn: leftOn.map((f) => FieldRef.name(f)),
    rightOn: rightOn.map((f) => FieldRef.name(f)),
    comparison: usesPolicyValueComparison(request) ? ValueComparison.Policy : ValueComparison.Exact,
  });
}

const valueSourceDescriptor = (columns: ValueSourceColumn[]) =>
  new RecordDescriptor(columns.map((c) => [c.name, c.ty]));

function bindingDescriptorParamsWithUserParams(
  request: LoweringContext,
  additionalUserParams: [string, ColumnType][],
): [string, ColumnType][] {
  const domain = request.parameterDomain();
  const userParams = request.input.binding.extraUserParams.clone();
  for (const [name, ty] of domain.userParams) userParams.set(name, ty);
  // A source's physical type is only a fallback; it never replaces a validated binding type.
  for (const [name, ty] of additionalUserParams) userParams.getOrInsert(name, () => ty);
  return [
    ...userParams,
    ...[...domain.claimParams].map(([name, p]): [string, ColumnType] => [name, p.ty]),
  ];
}

export function bindingSourceDescriptorWithUserParams(
  request: LoweringContext,
  additionalUserParams: [string, ColumnType][],
): RecordDescriptor {
  return new RecordDescriptor(bindingDescriptorParamsWithUserParams(request, additionalUserParams));
}

function decodeLiteral(bytes: Uint8Array, what: string): Value {
  try {
    return decodeValue(bytes);
  } catch (err) {
    throw operatorGap(`${what}: ${err}`);
  }
}

function lowerValueSource(
  shape: string,
  columns: ValueSourceColumn[],
  mode: ValueSourceMode,
  request: LoweringContext,
): GraphBuilder {
  const descriptor = valueSourceDescriptor(columns);
  const inlineRow = (message: string) => {
    const row = columns.map((c) => lowerValueSourceColumn(c, request));
    try {
      return GraphBuilder.values(descriptor, [row]);
    } catch (err) {
      throw operatorGap(`${message}: ${err}`);
    }
  };
  if (mode === "Inline") return inlineRow("inline value source could not encode");
  const domain = request.parameterDomain();
  const params = bindingDescriptorParamsWithUserParams(request, []);
  for (const column of columns) {
    const value = column.value;
    if (value.kind === "Param") {
      const existing = params.find(([name]) => name === value.name);
      if (!existing) {
        throw operatorGap(
          `binding parameter '${value.name}' is not part of the program parameter domain`,
        );
      }
      if (!structEq(existing[1], column.ty)) {
        throw operatorGap(`binding parameter '${value.name}' has conflicting value-source types`);
      }
    } else if (value.kind === "Claim") {
      const param = claimParamField(value.path);
      const existing = domain.claimParams.get(param);
      if (!existing)
        throw operatorGap(`claim parameter '${param}' is not part of the program parameter domain`);
      if (!structEq(existing.ty, column.ty)) {
        throw operatorGap(`claim parameter '${param}' has conflicting value-source types`);
      }
    } else if (value.kind !== "Literal") {
      throw operatorGap(
        "binding value source columns must reference binding params, claims, or literals",
      );
    }
  }
  if (params.length === 0) return inlineRow("binding value source could not encode constant row");
  const projected = new Set(columns.map((c) => c.name));
  const sourceUserParams = new Set<string>();
  for (const column of columns) {
    if (column.value.kind === "Param" && domain.userParams.has(column.value.name)) {
      sourceUserParams.add(column.value.name);
    } else if (column.value.kind === "Claim") {
      const param = claimParamField(column.value.path);
      if (domain.claimParams.has(param)) sourceUserParams.add(param);
    }
  }
  const retainedRoutes = [
    ...domain.userParams
      .keysArray()
      .filter((p) => sourceUserParams.has(p) && !projected.has(routeParamField(p)))
      .map((p) => ProjectField.renamed(p, routeParamField(p))),
    // The shared binding descriptor keeps enclosing claim slots for downstream joins.
    ...domain.claimParams
      .keysArray()
      .filter((p) => !projected.has(p))
      .map((p) => ProjectField.named(p)),
  ];
  const columnFields = columns.map((column) => {
    const value = column.value;
    if (value.kind === "Param") return ProjectField.renamed(value.name, column.name);
    if (value.kind === "Claim")
      return ProjectField.renamed(claimParamField(value.path), column.name);
    if (value.kind === "Literal") {
      return ProjectField.literal(
        column.name,
        decodeLiteral(value.bytes, "literal value could not be decoded"),
      );
    }
    throw new Error("checked above");
  });
  return GraphBuilder.bindingSource(shape, new RecordDescriptor(params)).projectFields([
    ...columnFields,
    ...retainedRoutes,
  ]);
}

function lowerValueSourceColumn(column: ValueSourceColumn, request: LoweringContext): Value {
  const value = column.value;
  switch (value.kind) {
    case "Param": {
      const bound = request.input.binding.values.get(value.name);
      if (bound === undefined) throw operatorGap(`binding parameter '${value.name}' is not bound`);
      return bound;
    }
    case "Literal":
      return decodeLiteral(value.bytes, "literal value could not be decoded");
    case "Claim":
      return claimValue(value.path, request.policy);
    default:
      throw operatorGap("value source columns must be binding params, literals, or claims");
  }
}

type KeyResolver = (value: NormalizedValueRef) => string;

export function lowerPathKeyPair(
  predicate: PredicateExpr,
  parentSourceId: SourceId,
  parentSource: ResolvedSource,
  childSourceId: SourceId,
  childSource: ResolvedSource,
  request: LoweringContext,
): [string, string] {
  return lowerBidirectionalKeyPair(
    predicate,
    "correlated path projection only lowers equality correlations",
    "correlated path projection correlation must compare parent and child fields",
    (v) => lowerJoinKeyRef(v, parentSourceId, parentSource, request),
    (v) => lowerJoinKeyRef(v, childSourceId, childSource, request),
  );
}

export function lowerJoinKeyPairs(
  predicate: PredicateExpr,
  leftSourceId: SourceId,
  leftSource: ResolvedSource,
  rightSourceId: SourceId,
  rightSource: ResolvedSource,
  request: LoweringContext,
): [string[], string[]] {
  return lowerKeyPairs(predicate, "join_via requires at least one equality join predicate", (p) =>
    lowerBidirectionalKeyPair(
      p,
      "join_via only lowers equality join predicates",
      "join_via join predicate must compare the root row id to one join source field",
      (v) => lowerJoinKeyRef(v, leftSourceId, leftSource, request),
      (v) => lowerJoinKeyRef(v, rightSourceId, rightSource, request),
    ),
  );
}

function lowerLinearJoinKeyPairs(
  predicate: PredicateExpr,
  leftRoot: LinearRoot,
  leftSource: ResolvedSource,
  rightPlan: RelationInputPlan,
  rightOutput: LoweredRelationInput,
  accumulated: AccumulatedJoinFields,
  request: LoweringContext,
): [string[], string[]] {
  return lowerKeyPairs(predicate, "join_via requires at least one equality join predicate", (p) =>
    lowerBidirectionalKeyPair(
      p,
      "join_via only lowers equality join predicates",
      "join_via join predicate must compare left root and right relation fields",
      (v) => {
        try {
          return lowerLinearRootKeyRef(v, leftRoot, leftSource, request);
        } catch (err) {
          if (!(err instanceof UnsupportedError)) throw err;
          const field = accumulatedJoinField(v, accumulated);
          if (!field)
            throw operatorGap(
              "join left key must reference the root or an accumulated join source",
            );
          return field[0];
        }
      },
      (v) => lowerRelationKeyRef(v, rightPlan, rightOutput, request),
    ),
  );
}

export function lowerRootToRelationKeyPairs(
  predicate: PredicateExpr,
  rootSource: ResolvedSource,
  rightPlan: RelationInputPlan,
  rightOutput: LoweredRelationInput,
  request: LoweringContext,
): [string[], string[]] {
  return lowerKeyPairs(
    predicate,
    "join contribution membership requires at least one equality predicate",
    (p) =>
      lowerBidirectionalKeyPair(
        p,
        "join contribution membership only lowers equality predicates",
        "join contribution membership must compare root fields to relation output fields",
        (v) => lowerJoinKeyRef(v, rootSource.rowShape.source, rootSource, request),
        (v) => lowerRelationKeyRef(v, rightPlan, rightOutput, request),
      ),
  );
}

/** A conjunction of equalities lowers into parallel key lists, one pair per conjunct. */
function lowerKeyPairs(
  predicate: PredicateExpr,
  emptyMessage: string,
  lowerPair: (p: PredicateExpr) => [string, string],
): [string[], string[]] {
  // A true join condition is an uncorrelated existence gate.
  if (predicate.kind === "True") return [[], []];
  const pairs =
    predicate.kind === "And" ? predicate.children.map(lowerPair) : [lowerPair(predicate)];
  if (pairs.length === 0) throw operatorGap(emptyMessage);
  return [pairs.map((p) => p[0]), pairs.map((p) => p[1])];
}

function attempt(f: () => string): { ok: string } | { err: UnsupportedReason } {
  try {
    return { ok: f() };
  } catch (err) {
    if (err instanceof UnsupportedError) return { err: err.gap };
    throw err;
  }
}

const keyPairError = (r: { ok: string } | { err: UnsupportedReason }) =>
  "ok" in r ? `accepted ${dbg(r.ok)}` : dbg(r.err);

function lowerBidirectionalKeyPair(
  predicate: PredicateExpr,
  nonEqualityMessage: string,
  mismatchMessage: string,
  leftResolver: KeyResolver,
  rightResolver: KeyResolver,
): [string, string] {
  if (predicate.kind !== "Compare" || predicate.op !== "Eq") throw operatorGap(nonEqualityMessage);
  const directLeft = attempt(() => leftResolver(predicate.left));
  const directRight = attempt(() => rightResolver(predicate.right));
  if ("ok" in directLeft && "ok" in directRight) return [directLeft.ok, directRight.ok];
  const swappedLeft = attempt(() => leftResolver(predicate.right));
  const swappedRight = attempt(() => rightResolver(predicate.left));
  if ("ok" in swappedLeft && "ok" in swappedRight) return [swappedLeft.ok, swappedRight.ok];
  throw operatorGap(
    `${mismatchMessage}; direct errors: ${keyPairError(directLeft)}, ${keyPairError(directRight)}; swapped errors: ${keyPairError(swappedLeft)}, ${keyPairError(swappedRight)}`,
  );
}

export function lowerRelationKeyRef(
  value: NormalizedValueRef,
  plan: RelationInputPlan,
  output: LoweredRelationInput,
  request: LoweringContext,
): string {
  if (plan.kind !== "Linear") return lowerNamedRelationField(value, output.fields);
  const linear = plan.plan;
  const last = linear.steps[linear.steps.length - 1];
  if (last?.kind === "Project") {
    const named = attempt(() => lowerNamedRelationField(value, output.fields));
    if ("ok" in named) return named.ok;
    // Relation-backed policies commonly project their source row id as public `id`.
    if (
      value.kind === "RowId" &&
      value.row.kind === "Source" &&
      structEq(linearRootSource(linear.root), value.row.source)
    ) {
      const valueSource = value.row.source;
      const column = last.columns.find(
        (c) =>
          c.value.kind === "RowId" &&
          c.value.row.kind === "Source" &&
          structEq(c.value.row.source, valueSource),
      );
      if (column) return column.output.name;
    }
  }
  const sourceId = linearRootSource(linear.root);
  if (output.rootSource && sourceId) {
    const key = attempt(() => lowerJoinKeyRef(value, sourceId, output.rootSource!, request));
    if ("ok" in key) return key.ok;
  }
  return lowerNamedRelationField(value, output.fields);
}

function lowerNamedRelationField(value: NormalizedValueRef, fields: SortedSet<string>): string {
  let field: string;
  switch (value.kind) {
    case "FrontierColumn":
    case "SourceField":
      field = value.field;
      break;
    case "Param":
      field = value.name;
      break;
    case "RowId":
      if (value.row.kind === "Frontier") {
        field = "row_uuid";
        break;
      }
    // falls through
    default:
      throw operatorGap("join relation key must be an output field");
  }
  if (!fields.has(field)) throw operatorGap(`join relation does not output field '${field}'`);
  return field;
}

function lowerLinearRootKeyRef(
  value: NormalizedValueRef,
  root: LinearRoot,
  source: ResolvedSource,
  request: LoweringContext,
): string {
  if (root.kind === "Source") return lowerJoinKeyRef(value, root.source, source, request);
  const hasColumn = (name: string) => root.columns.some((c) => c.name === name);
  if (root.kind === "Frontier") {
    if (
      value.kind === "FrontierColumn" &&
      value.frontier === root.frontier &&
      hasColumn(value.field)
    ) {
      return value.field;
    }
    if (
      value.kind === "RowId" &&
      value.row.kind === "Frontier" &&
      value.row.frontier === root.frontier &&
      hasColumn("row_uuid")
    ) {
      return "row_uuid";
    }
    throw operatorGap("join left key must be a frontier column");
  }
  const name =
    value.kind === "Param" ? value.name : value.kind === "FrontierColumn" ? value.field : undefined;
  if (name !== undefined && hasColumn(name)) return name;
  throw operatorGap("join left key must be a value-source column");
}

function accumulatedJoinField(
  value: NormalizedValueRef,
  fields: AccumulatedJoinFields,
): [string, number] | undefined {
  if (value.kind === "SourceField") return fields.get([value.source, userColumnField(value.field)]);
  if (value.kind === "RowId" && value.row.kind === "Source")
    return fields.get([value.row.source, "row_uuid"]);
  return undefined;
}

type ProjectionSource =
  | { kind: "Argument"; expression: any }
  | { kind: "Field"; field: string; nullableDepth: number }
  | { kind: "Literal"; value: any };

type ProjectionFieldPlan = {
  project: any;
  unwrapBeforeProject: Map<string, number>;
  nullableAfterProject?: [string, number];
  sourceNullability?: [string, number];
};

function lowerProjectionField(
  column: RowProjection,
  plan: LinearCurrentRoot,
  source: ResolvedSource,
  fields: SortedSet<string>,
  depths: SortedMap<string, number>,
  lastJoinRight: LastJoinRight | undefined,
  accumulated: AccumulatedJoinFields,
  request: LoweringContext,
): ProjectionFieldPlan {
  const result: ProjectionFieldPlan = { project: undefined, unwrapBeforeProject: new Map() };
  const name = column.output.name;
  const projection = lowerProjectionSource(
    column.value,
    plan,
    source,
    fields,
    depths,
    lastJoinRight,
    accumulated,
    request,
  );
  switch (projection.kind) {
    case "Field": {
      const outputDepth = valueTypeNullableDepth(column.output.ty);
      const unwrapDepth = Math.max(0, projection.nullableDepth - outputDepth);
      if (unwrapDepth > 0) result.unwrapBeforeProject.set(projection.field, unwrapDepth);
      if (outputDepth > 0) result.nullableAfterProject = [name, outputDepth];
      result.sourceNullability = [projection.field, projection.nullableDepth];
      result.project = ProjectField.renamed(projection.field, name);
      break;
    }
    case "Literal":
      result.project = ProjectField.literal(name, projection.value);
      break;
    case "Argument":
      result.project = new ProjectField({
        expression: projection.expression,
        outputName: name,
        outputIdentity: FieldIdentity.Name(name),
      });
  }
  return result;
}

function lowerProjectionSource(
  value: NormalizedValueRef,
  plan: LinearCurrentRoot,
  source: ResolvedSource,
  fields: SortedSet<string>,
  depths: SortedMap<string, number>,
  lastJoinRight: LastJoinRight | undefined,
  accumulated: AccumulatedJoinFields,
  request: LoweringContext,
): ProjectionSource {
  const leftQualified = (field: string) => (lastJoinRight ? leftField(field) : field);
  const root = attempt(() => lowerLinearRootKeyRef(value, plan.root, source, request));
  if ("ok" in root)
    return {
      kind: "Field",
      field: leftQualified(root.ok),
      nullableDepth: depths.get(root.ok) ?? 0,
    };
  if (value.kind === "Param" && fields.has(value.name)) {
    return { kind: "Field", field: leftQualified(value.name), nullableDepth: 0 };
  }
  const joined = accumulatedJoinField(value, accumulated);
  if (joined)
    return {
      kind: "Field",
      field: leftQualified(joined[0]),
      nullableDepth: depths.get(joined[0]) ?? 0,
    };
  if (lastJoinRight) {
    const field = lowerRelationProjectionRef(value, lastJoinRight.plan);
    if (field !== undefined) {
      const nullableDepth = lastJoinRight.nullableFields.has(field)
        ? (lastJoinRight.nullableFieldDepths.get(field) ?? 1)
        : 0;
      return { kind: "Field", field: rightField(field), nullableDepth };
    }
  }
  if (value.kind === "Param") {
    const ty = request.input.binding.paramTypes.get(value.name);
    const expression = ty !== undefined ? request.scalarArgument(value, ty, false) : undefined;
    if (expression !== undefined) return { kind: "Argument", expression };
  }
  if (value.kind === "TemplateLiteral") {
    const ty = request.literalType(value.slot);
    const expression = ty !== undefined ? request.scalarArgument(value, ty, false) : undefined;
    if (expression !== undefined) return { kind: "Argument", expression };
  }
  const literal = lowerLiteralProjectionValue(value, request);
  if (literal === undefined) {
    throw operatorGap(
      "project value must reference the current root, last join input, or a literal",
    );
  }
  return { kind: "Literal", value: literal };
}

function lowerRelationProjectionRef(
  value: NormalizedValueRef,
  plan: RelationInputPlan,
): string | undefined {
  switch (plan.kind) {
    case "Linear": {
      const sourceId = linearRootSource(plan.plan.root);
      if (sourceId) {
        if (value.kind === "SourceField" && structEq(value.source, sourceId))
          return userColumnField(value.field);
        if (
          value.kind === "RowId" &&
          value.row.kind === "Source" &&
          structEq(value.row.source, sourceId)
        ) {
          return "row_uuid";
        }
      }
      if (value.kind === "Param") return value.name;
      if (value.kind === "FrontierColumn") return value.field;
      return undefined;
    }
    case "Recursive":
      if (value.kind === "FrontierColumn" && value.frontier === plan.plan.frontier)
        return value.field;
      if (value.kind === "Param") return value.name;
      return undefined;
    case "Union":
      if (value.kind === "Param") return value.name;
      if (value.kind === "FrontierColumn" || value.kind === "SourceField") return value.field;
      if (value.kind === "RowId") return "row_uuid";
      return undefined;
  }
}

export function lowerEqualityParamFilterJoins(
  graph: GraphBuilder,
  predicate: PredicateExpr,
  sourceId: SourceId,
  source: ResolvedSource,
  availableRouteFields: SortedSet<string>,
  request: LoweringContext,
): [GraphBuilder, PredicateExpr, SortedSet<string>] {
  const predicates = predicate.kind === "And" ? predicate.children : [predicate];
  const residual: PredicateExpr[] = [];
  const retainedRouteFields = strSet(availableRouteFields);
  for (const predicate of predicates) {
    const join = equalityParamJoin(predicate, sourceId, source, request);
    const bindingSourceShape = request.input.binding.sourceShape;
    if (!join || bindingSourceShape === undefined) {
      residual.push(predicate);
      continue;
    }
    const domain = request.parameterDomain();
    const isClaimParam = domain.claimParams.has(join.param);
    const bindingDescriptor = bindingSourceDescriptorWithUserParams(
      request,
      isClaimParam ? [] : [[join.param, join.valueType]],
    );
    const bindingFields: string[] = bindingDescriptor.fields().map((field: any) => {
      if (field.name === undefined) throw operatorGap("binding fields must be named");
      return field.name;
    });
    let binding = GraphBuilder.bindingSource(bindingSourceShape, bindingDescriptor);
    const routeField = isClaimParam ? join.param : routeParamField(join.param);
    const claimTy = domain.claimParams.get(join.param)?.ty;
    const userTy = domain.userParams.get(join.param);
    const routeIsNullable = (claimTy ?? userTy)?.kind === "Nullable";
    // Equality unwrapping must not become the route carrier's value
    // conversion. Keep a second, untouched copy of a nullable binding for
    // the policy-arm union and for the downstream result-membership
    // window, then unwrap only the copy used as the equality key.
    const routeCarrier =
      join.nullable && routeIsNullable ? `__jazz_route_carrier:${join.param}` : undefined;
    if (routeCarrier !== undefined) {
      binding = binding.projectFields([
        ...bindingFields.map((f) => ProjectField.named(f)),
        ProjectField.renamed(join.param, routeCarrier),
      ]);
    }
    const projection = projectSourceFieldsFromPrefixRewrappingNullable(
      source,
      LEFT_JOIN_PREFIX,
      join.nullable ? join.field : undefined,
    );
    for (const field of retainedRouteFields)
      projection.push(ProjectField.renamed(leftField(field), field));
    projection.push(ProjectField.renamed(rightField(routeCarrier ?? join.param), routeField));
    if (join.nullable) {
      graph = graph.unwrapNullable(join.field);
      binding = binding.unwrapNullable(join.param);
    }
    graph = policyJoinIfNeeded(graph, binding, [join.field], [join.param], request).projectFields(
      projection,
    );
    retainedRouteFields.add(routeField);
  }
  const out: PredicateExpr =
    residual.length === 0
      ? { kind: "True" }
      : residual.length === 1
        ? residual[0]
        : { kind: "And", children: residual };
  return [graph, out, retainedRouteFields];
}

type EqualityParamJoin = { field: string; param: string; valueType: ValueType; nullable: boolean };

function equalityParamJoin(
  predicate: PredicateExpr,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): EqualityParamJoin | undefined {
  if (predicate.kind !== "Compare" || predicate.op !== "Eq") return undefined;
  const l = sourceJoinField(predicate.left, sourceId, source);
  const r = sourceJoinField(predicate.right, sourceId, source);
  let field: [string, ValueType, boolean];
  let value: NormalizedValueRef;
  if (l && !r) [field, value] = [l, predicate.right];
  else if (!l && r) [field, value] = [r, predicate.left];
  else return undefined;
  const param = equalityBindingParam(value, request);
  if (param === undefined) return undefined;
  return { field: field[0], param, valueType: field[1], nullable: field[2] };
}

/**
 * The binding slot an equality operand reads, when it has one. A claim is a
 * slot only when the program carries it as a prepared routing parameter, so
 * every session with the same query shares one graph (#4017).
 */
function equalityBindingParam(
  value: NormalizedValueRef,
  request: LoweringContext,
): string | undefined {
  if (value.kind === "Param") return value.name;
  if (value.kind === "Claim" && request.input.binding.sourceShape !== undefined) {
    const domain = request.parameterDomain();
    const slot = claimBindingSlot(domain, value.path);
    return slot !== undefined && domain.routingParams.has(slot) ? slot : undefined;
  }
  return undefined;
}

function sourceJoinField(
  value: NormalizedValueRef,
  sourceId: SourceId,
  source: ResolvedSource,
): [string, ValueType, boolean] | undefined {
  let field: string;
  if (value.kind === "SourceField" && structEq(value.source, sourceId)) {
    try {
      field = requireSourceField(source, userColumnField(value.field));
    } catch {
      field = requireSourceField(source, value.field);
    }
  } else if (
    value.kind === "RowId" &&
    value.row.kind === "Source" &&
    structEq(value.row.source, sourceId)
  ) {
    field = requireSourceField(source, source.rowShape.rowUuidField);
  } else return undefined;
  const valueType = sourceFieldType(source, field);
  if (valueType === undefined)
    throw runtimeGap(`source field ${dbg(field)} is missing from resolved descriptor`);
  return valueType.kind === "Nullable" ? [field, valueType.inner, true] : [field, valueType, false];
}

export function lowerLiteralProjectionValue(
  value: NormalizedValueRef,
  request: LoweringContext,
): any {
  switch (value.kind) {
    case "TemplateLiteral":
      return LiteralValue.from(request.literal(value.slot));
    case "Literal":
      return LiteralValue.from(decodeLiteral(value.bytes, "literal value could not be decoded"));
    case "Param": {
      const bound = request.input.binding.values.get(value.name);
      if (bound === undefined) throw operatorGap(`binding parameter '${value.name}' is not bound`);
      return LiteralValue.from(bound);
    }
    default:
      return undefined;
  }
}

export function lowerJoinKeyRef(
  value: NormalizedValueRef,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): string {
  const lowered = lowerValueRef(value, sourceId, source, request);
  if (lowered.kind === "Field") return lowered.field;
  throw operatorGap("join_via join keys must be source fields");
}

const sourceFieldIsNullable = (source: ResolvedSource, field: string) =>
  sourceFieldNullableDepth(source, field) > 0;

export function valueTypeNullableDepth(valueType: ValueType): number {
  let depth = 0;
  while (valueType.kind === "Nullable") {
    depth++;
    valueType = valueType.inner;
  }
  return depth;
}

export function sourceFieldNullableDepth(source: ResolvedSource, field: string): number {
  const ty = sourceFieldType(source, field);
  return ty === undefined ? 0 : valueTypeNullableDepth(ty);
}

export function resolvedSourceDescriptorIndex(
  source: ResolvedSource,
  field: string,
): number | undefined {
  const descriptor = source.rowShape.descriptor;
  const index = descriptor.fieldIndex(field);
  if (index !== undefined) return index;
  const position = descriptor.fields().findIndex((c: any) => c.name === field);
  return position < 0 ? undefined : position;
}

export function sourceFieldType(source: ResolvedSource, field: string): ValueType | undefined {
  const index = resolvedSourceDescriptorIndex(source, field);
  return index === undefined ? undefined : source.rowShape.descriptor.fields()[index]?.valueType;
}

function projectLeftSourceFieldsWithJoinRoutes(
  source: ResolvedSource,
  existingRouteFields: SortedSet<string>,
  introducedRouteFields: SortedSet<string>,
): any[] {
  const fields = projectSourceFieldsFromPrefix(source, LEFT_JOIN_PREFIX);
  for (const f of existingRouteFields) fields.push(ProjectField.renamed(leftField(f), f));
  for (const f of introducedRouteFields) fields.push(ProjectField.renamed(rightField(f), f));
  return fields;
}

export function lowerWindow(
  graph: GraphBuilder,
  order: OrderKey[],
  partitionBy: NormalizedValueRef[],
  routeFields: SortedSet<string>,
  limit: number | undefined,
  offset: number,
  tieBreaker: NormalizedValueRef[],
  plan: LinearCurrentRoot,
  source: ResolvedSource,
  request: LoweringContext,
): GraphBuilder {
  const groupCols = partitionBy.map((v) =>
    lowerFieldRef(v, plan, source, request, "slice partition key"),
  );
  // One maintained graph serves every active binding. A window must therefore
  // be independent for each route tuple before the runtime filters its sinks.
  for (const routeField of routeFields)
    if (!groupCols.includes(routeField)) groupCols.push(routeField);
  const orderCols = order.map((key) => lowerOrderKey(key, plan, source, request));
  const tieCols = (
    tieBreaker.length === 0
      ? [source.rowShape.rowUuidField]
      : tieBreaker.map((v) => lowerFieldRef(v, plan, source, request, "slice tie-breaker"))
  ).filter((tie) => !orderCols.some((o) => structEq(o.field, FieldRef.name(tie))));
  const topByLimit = limit !== undefined ? TopByLimit.Finite(BigInt(limit)) : TopByLimit.Unbounded;
  if (order.length === 0 && offset === 0 && limit === 1)
    return GraphBuilder.argMinBy(graph, groupCols, tieCols);
  return GraphBuilder.topBy(
    graph,
    groupCols,
    order.length === 0 ? [] : orderCols,
    tieCols,
    BigInt(offset),
    topByLimit,
  );
}

export function lowerAggregate(
  graph: GraphBuilder,
  groupBy: NormalizedValueRef[],
  outputs: AggregateExpr[],
  plan: LinearCurrentRoot,
  source: ResolvedSource,
  request: LoweringContext,
): LoweredRelationInput {
  const groupCols = groupBy.map((v) =>
    lowerFieldRef(v, plan, source, request, "aggregate group key"),
  );
  const aggregates = outputs.map((a) => lowerAggregateExpr(a, plan, source, request));
  const unwrapFields = strSet(groupCols.filter((f) => sourceFieldIsNullable(source, f)));
  for (const aggregate of outputs) {
    if (aggregate.input === undefined) continue;
    const field = lowerFieldRef(aggregate.input, plan, source, request, "aggregate input");
    if (sourceFieldIsNullable(source, field)) unwrapFields.add(field);
  }
  for (const field of unwrapFields) {
    graph = GraphBuilder.UnwrapNullable({ input: graph, field: FieldRef.storedName(field) });
  }
  const fields = strSet(groupCols);
  for (const aggregate of outputs) fields.add(aggregateOutputField(aggregate.output.name));
  return {
    graph: GraphBuilder.Aggregate({
      input: graph,
      groupCols: groupCols.map((f) => FieldRef.storedName(f)),
      aggregates,
    }),
    rootSource: source,
    fields,
    nullableFields: strSet(),
    nullableFieldDepths: strMap(),
  };
}

function lowerAggregateExpr(
  aggregate: AggregateExpr,
  plan: LinearCurrentRoot,
  source: ResolvedSource,
  request: LoweringContext,
): any {
  let expression: any;
  if (aggregate.input !== undefined) {
    const carrier = lowerFieldRef(aggregate.input, plan, source, request, "aggregate input");
    const descriptorFields = source.rowShape.descriptor.fields();
    const index = descriptorFields.findIndex((f: any) => f.name === carrier);
    if (index < 0)
      throw operatorGap(`aggregate input carrier ${dbg(carrier)} is missing from resolved source`);
    const field = descriptorFields[index];
    // PlanExpr carries a name. Choose a spelling that its logical-then-carrier
    // resolver binds back to this exact slot, including user-prefix collisions.
    const name = [field.name, field.logicalName()]
      .filter((n): n is string => n !== undefined)
      .find((n) => resolvedSourceDescriptorIndex(source, n) === index);
    if (name === undefined) {
      throw operatorGap(
        `aggregate input carrier ${dbg(carrier)} has no unambiguous expression name`,
      );
    }
    expression = GroovePlanExpr.field(name);
  }
  const outputName = aggregateOutputField(aggregate.output.name);
  return new GrooveAggregateExpr({
    function: aggregate.function,
    expression,
    distinct: false,
    outputName,
    outputIdentity: FieldIdentity.Name(outputName),
  });
}

function lowerOrderKey(
  key: OrderKey,
  plan: LinearCurrentRoot,
  source: ResolvedSource,
  request: LoweringContext,
): any {
  const lowered = lowerFieldRef(key.value, plan, source, request, "order key");
  const output = request.output.appRows;
  const relationOutput =
    output !== undefined &&
    output.projection.kind === "Relation" &&
    plan.steps.some((step) => step.kind === "Project");
  const windowField = collectWindowSourceField(source, key.value);
  let field: any;
  if (windowField === undefined) field = FieldRef.name(lowered);
  else if (relationOutput) {
    const index = resolvedSourceDescriptorIndex(source, lowered);
    if (index === undefined) {
      throw operatorGap(
        `resolved order key field ${dbg(lowered)} is missing from the source descriptor`,
      );
    }
    field = FieldRef.resolved(index);
  } else field = FieldRef.storedName(windowField.name);
  return new TopByOrder({ field, direction: key.direction });
}

export function lowerPredicate(
  predicate: PredicateExpr,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  const argument = request.predicateArgument(predicate, sourceId, source);
  if (argument !== undefined) return argument;
  return orFalseOnUnboundClaim(() =>
    lowerPredicateInner(predicate, sourceId, source, request),
  ).canonicalize();
}

function orFalseOnUnboundClaim(f: () => GroovePredicateExpr): GroovePredicateExpr {
  try {
    return f();
  } catch (err) {
    if (err instanceof UnsupportedError && isUnboundClaimReason(err.gap))
      return constantPredicate(false);
    throw err;
  }
}

function lowerPredicateInner(
  predicate: PredicateExpr,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  switch (predicate.kind) {
    case "True":
      return GroovePredicateExpr.And([]);
    case "False":
      return GroovePredicateExpr.Or([]);
    case "Compare":
      return lowerCompare(
        predicate.left,
        predicate.op,
        predicate.right,
        false,
        sourceId,
        source,
        request,
      );
    case "In":
      return GroovePredicateExpr.Or(
        predicate.options.map((o) =>
          lowerCompare(predicate.value, "Eq", o, false, sourceId, source, request),
        ),
      );
    case "ArrayContains":
      return lowerContains(predicate.value, predicate.needle, sourceId, source, request);
    case "TextContains":
      throw operatorGap("text containment predicates are not lowered yet");
    case "IsNull":
      return lowerNullTest(predicate.value, true, sourceId, source, request);
    case "IsNotNull":
      return lowerNullTest(predicate.value, false, sourceId, source, request);
    case "EnumMatch": {
      const lowered = lowerValueRef(predicate.value, sourceId, source, request);
      if (lowered.kind !== "Field") throw operatorGap("enum match requires a source field");
      return GroovePredicateExpr.EnumMatch({
        field: lowered.field,
        caseTag: predicate.caseTag,
        payload: lowerEnumPayloadPredicate(predicate.payload),
      });
    }
    case "And":
      return GroovePredicateExpr.And(
        predicate.children.map((p) => lowerPredicate(p, sourceId, source, request)),
      );
    case "Or":
      return GroovePredicateExpr.Or(
        predicate.children.map((p) => lowerPredicate(p, sourceId, source, request)),
      );
    case "Not":
      return lowerNotPredicate(predicate.child, sourceId, source, request);
  }
}

function lowerEnumPayloadPredicate(predicate: PredicateExpr): GroovePredicateExpr {
  const decode = (bytes: Uint8Array) =>
    LiteralValue.from(decodeLiteral(bytes, "payload enum literal could not be decoded"));
  switch (predicate.kind) {
    case "True":
      return GroovePredicateExpr.And([]);
    case "False":
      return GroovePredicateExpr.Or([]);
    case "Compare": {
      const { left, op, right } = predicate;
      if (left.kind === "SourceField" && right.kind === "Literal") {
        return GroovePredicateExpr.fromFieldLiteral(
          predicateKind(op),
          left.field,
          decode(right.bytes),
        );
      }
      if (left.kind === "Literal" && right.kind === "SourceField") {
        return GroovePredicateExpr.fromFieldLiteral(
          predicateKind(invertComparison(op)),
          right.field,
          decode(left.bytes),
        );
      }
      throw operatorGap("payload enum predicates require field/literal comparisons");
    }
    case "IsNull":
    case "IsNotNull":
      if (predicate.value.kind !== "SourceField") break;
      return predicate.kind === "IsNull"
        ? GroovePredicateExpr.IsNull({ field: predicate.value.field })
        : GroovePredicateExpr.IsNotNull({ field: predicate.value.field });
    case "And":
      return GroovePredicateExpr.And(predicate.children.map(lowerEnumPayloadPredicate));
    case "Or":
      return GroovePredicateExpr.Or(predicate.children.map(lowerEnumPayloadPredicate));
    case "Not":
      throw operatorGap("negated payload enum predicates are not lowered yet");
  }
  throw operatorGap("unsupported payload enum predicate");
}

function lowerNotPredicate(
  predicate: PredicateExpr,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  return orFalseOnUnboundClaim(() =>
    lowerNotPredicateInner(predicate, sourceId, source, request),
  ).canonicalize();
}

function lowerNotPredicateInner(
  predicate: PredicateExpr,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  switch (predicate.kind) {
    case "True":
      return GroovePredicateExpr.Or([]);
    case "False":
      return GroovePredicateExpr.And([]);
    case "Compare":
      return lowerCompare(
        predicate.left,
        predicate.op,
        predicate.right,
        true,
        sourceId,
        source,
        request,
      );
    case "In":
      return GroovePredicateExpr.And(
        predicate.options.map((o) =>
          lowerTwoValuedNe(predicate.value, o, sourceId, source, request),
        ),
      );
    case "ArrayContains":
    case "TextContains":
      throw operatorGap("negated containment predicates are not lowered yet");
    case "IsNull":
      return lowerNullTest(predicate.value, false, sourceId, source, request);
    case "IsNotNull":
      return lowerNullTest(predicate.value, true, sourceId, source, request);
    case "And":
      return GroovePredicateExpr.Or(
        predicate.children.map((p) => lowerNotPredicate(p, sourceId, source, request)),
      );
    case "Or":
      return GroovePredicateExpr.And(
        predicate.children.map((p) => lowerNotPredicate(p, sourceId, source, request)),
      );
    case "Not":
      return lowerPredicate(predicate.child, sourceId, source, request);
    case "EnumMatch":
      throw operatorGap("negated enum match predicates are not lowered yet");
  }
}

function lowerTwoValuedNe(
  left: NormalizedValueRef,
  right: NormalizedValueRef,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  // Groove comparisons use SQL-null semantics; Jazz membership negation is
  // two-valued: exactly one operand is null, or equality is false.
  const isNull = (v: NormalizedValueRef, n: boolean) =>
    lowerNullTest(v, n, sourceId, source, request);
  return GroovePredicateExpr.Or([
    GroovePredicateExpr.And([isNull(left, true), isNull(right, false)]),
    GroovePredicateExpr.And([isNull(left, false), isNull(right, true)]),
    lowerCompare(left, "Eq", right, true, sourceId, source, request),
  ]).canonicalize();
}

const INVERTED: Record<ComparisonOp, ComparisonOp> = {
  Eq: "Ne",
  Ne: "Eq",
  Lt: "Gte",
  Lte: "Gt",
  Gt: "Lte",
  Gte: "Lt",
};
const invertComparison = (op: ComparisonOp): ComparisonOp => INVERTED[op];

function lowerCompare(
  left: NormalizedValueRef,
  op: ComparisonOp,
  right: NormalizedValueRef,
  negated: boolean,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  const l = lowerValueRef(left, sourceId, source, request);
  const r = lowerValueRef(right, sourceId, source, request);
  const effectiveOp = negated ? invertComparison(op) : op;
  const kind = predicateKind(effectiveOp);
  if (l.kind === "Field" && r.kind === "Literal") {
    return GroovePredicateExpr.fromFieldLiteral(
      kind,
      l.field,
      coerceLiteralForSourceField(r.value, source, l.field),
    );
  }
  if (l.kind === "Literal" && r.kind === "Field") {
    return GroovePredicateExpr.fromFieldLiteral(
      kind.reversed(),
      r.field,
      coerceLiteralForSourceField(l.value, source, r.field),
    );
  }
  if (l.kind === "Field" && r.kind === "Field") {
    if (effectiveOp === "Eq")
      return GroovePredicateExpr.EqField({ field: l.field, valueField: r.field });
    if (effectiveOp === "Ne")
      return GroovePredicateExpr.NeqField({ field: l.field, valueField: r.field });
    throw operatorGap(`field-to-field comparison ${effectiveOp} is not lowered yet`);
  }
  // Inverting an operator is not boolean negation for unordered floats.
  const matches = compareLiterals((l as any).value, op, (r as any).value);
  return constantPredicate(negated ? !matches : matches);
}

function lowerContains(
  value: NormalizedValueRef,
  needle: NormalizedValueRef,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  const v = lowerValueRef(value, sourceId, source, request);
  const n = lowerValueRef(needle, sourceId, source, request);
  if (v.kind === "Literal" && v.value.kind === "Array" && n.kind === "Literal") {
    return constantPredicate(v.value.value.some((item: any) => structEq(item, n.value)));
  }
  if (v.kind === "Field" && n.kind === "Literal") {
    return GroovePredicateExpr.Contains({
      field: v.field,
      value: coerceLiteralForSourceArrayElement(n.value, source, v.field),
    });
  }
  if (v.kind === "Field" && n.kind === "Field") {
    return GroovePredicateExpr.ContainsField({ field: v.field, needleField: n.field });
  }
  if (v.kind === "Literal" && v.value.kind === "Array" && n.kind === "Field") {
    const values: any[] = v.value.value;
    if (values.length === 0) return constantPredicate(false);
    return GroovePredicateExpr.Or(
      values.map((item) =>
        GroovePredicateExpr.Eq({
          field: n.field,
          value: coerceLiteralForSourceField(item, source, n.field),
        }),
      ),
    );
  }
  throw operatorGap("array contains requires a source field haystack");
}

function coerceLiteralForSourceArrayElement(
  value: any,
  source: ResolvedSource,
  field: string,
): any {
  const valueType = sourceFieldType(source, field);
  if (valueType === undefined) return value;
  const inner = valueType.nonNullable();
  return inner.kind === "Array" ? coerceLiteralForValueType(value, inner.inner) : value;
}

function coerceLiteralForSourceField(value: any, source: ResolvedSource, field: string): any {
  const valueType = sourceFieldType(source, field);
  return valueType === undefined ? value : coerceLiteralForValueType(value, valueType);
}

export function coerceLiteralForValueType(value: any, valueType: ValueType): any {
  if (value.kind === "String" && valueType.kind === "Uuid") {
    try {
      return LiteralValue.Uuid(uuidFromString(value.value));
    } catch {
      return value;
    }
  }
  if (value.kind === "Uuid" && valueType.kind === "String")
    return LiteralValue.String(uuidToString(value.value));
  if (value.kind === "String" && valueType.kind === "EnumTag") {
    const tag = valueType.inner.discriminant(value.value);
    return tag !== undefined ? LiteralValue.EnumTag(tag) : value;
  }
  if (value.kind === "Nullable" && value.value !== undefined) {
    return LiteralValue.Nullable(coerceLiteralForValueType(value.value, valueType));
  }
  if (valueType.kind === "Nullable")
    return LiteralValue.Nullable(coerceLiteralForValueType(value, valueType.inner));
  if (value.kind === "Array" && valueType.kind === "Array") {
    return LiteralValue.Array(
      value.value.map((v: any) => coerceLiteralForValueType(v, valueType.inner)),
    );
  }
  if (
    value.kind === "Tuple" &&
    valueType.kind === "Tuple" &&
    value.value.length === valueType.inner.length
  ) {
    return LiteralValue.Tuple(
      value.value.map((v: any, i: number) => coerceLiteralForValueType(v, valueType.inner[i])),
    );
  }
  return value;
}

function lowerNullTest(
  value: NormalizedValueRef,
  isNull: boolean,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): GroovePredicateExpr {
  const lowered = lowerValueRef(value, sourceId, source, request);
  if (lowered.kind === "Field") {
    return isNull
      ? GroovePredicateExpr.IsNull({ field: lowered.field })
      : GroovePredicateExpr.IsNotNull({ field: lowered.field });
  }
  const literalNull = lowered.value.kind === "Nullable" && lowered.value.value === undefined;
  return constantPredicate(literalNull ? isNull : !isNull);
}

const PREDICATE_KINDS: Record<ComparisonOp, string> = {
  Eq: "Eq",
  Ne: "Neq",
  Lt: "Lt",
  Lte: "LtEq",
  Gt: "Gt",
  Gte: "GtEq",
};
const predicateKind = (op: ComparisonOp) => PredicateKind[PREDICATE_KINDS[op]];

function literalFloat(value: any): number | undefined {
  while (value.kind === "Nullable" && value.value !== undefined) value = value.value;
  return value.kind === "F64" ? value.float() : undefined;
}

function compareLiterals(left: any, op: ComparisonOp, right: any): boolean {
  const lf = literalFloat(left);
  const rf = literalFloat(right);
  // Predicate semantics are numeric; unordered comparisons are false for every operator, including Ne.
  const ordering =
    lf !== undefined && rf !== undefined
      ? Number.isNaN(lf) || Number.isNaN(rf)
        ? undefined
        : Math.sign(lf - rf)
      : LiteralValue.cmp(left, right);
  if (ordering === undefined) return false;
  switch (op) {
    case "Eq":
      return ordering === 0;
    case "Ne":
      return ordering !== 0;
    case "Lt":
      return ordering < 0;
    case "Lte":
      return ordering <= 0;
    case "Gt":
      return ordering > 0;
    case "Gte":
      return ordering >= 0;
  }
}

const constantPredicate = (value: boolean): GroovePredicateExpr =>
  value ? GroovePredicateExpr.And([]) : GroovePredicateExpr.Or([]);

type LoweredValueRef = { kind: "Field"; field: string } | { kind: "Literal"; value: any };

function lowerFieldRef(
  value: NormalizedValueRef,
  plan: LinearCurrentRoot,
  source: ResolvedSource,
  request: LoweringContext,
  context: string,
): string {
  const sourceId = linearRootSource(plan.root);
  if (sourceId === undefined) throw operatorGap(`${context} must be a root source field`);
  const lowered = lowerValueRef(value, sourceId, source, request);
  if (lowered.kind === "Field") return lowered.field;
  throw operatorGap(`${context} must be a root source field`);
}

function lowerValueRef(
  value: NormalizedValueRef,
  sourceId: SourceId,
  source: ResolvedSource,
  request: LoweringContext,
): LoweredValueRef {
  const field = (f: string): LoweredValueRef => ({ kind: "Field", field: f });
  const literal = (v: Value): LoweredValueRef => ({ kind: "Literal", value: LiteralValue.from(v) });
  switch (value.kind) {
    case "TemplateLiteral":
      return literal(request.literal(value.slot));
    case "SourceField":
      if (!structEq(value.source, sourceId)) {
        throw operatorGap(`predicate references unsupported source ${dbg(value.source)}`);
      }
      if (value.field === "__root_union_arm") return field(value.field);
      try {
        return field(requireSourceField(source, value.field));
      } catch {
        return field(requireSourceField(source, userColumnField(value.field)));
      }
    case "Param": {
      const bound = request.input.binding.values.get(value.name);
      if (bound === undefined) throw operatorGap(`binding parameter '${value.name}' is not bound`);
      return literal(bound);
    }
    case "Claim":
      return literal(claimValue(value.path, request.policy));
    case "FrontierColumn":
      throw operatorGap("frontier values are not valid in root source predicates");
    case "RowId":
      if (value.row.kind === "Frontier") {
        throw operatorGap("frontier row ids are not valid in root source predicates");
      }
      if (!structEq(value.row.source, sourceId)) {
        throw operatorGap(
          `predicate references unsupported row id source ${dbg(value.row.source)}`,
        );
      }
      return field(requireSourceField(source, source.rowShape.rowUuidField));
    case "Provenance":
      if (!structEq(value.source, sourceId)) {
        throw operatorGap(
          `predicate references unsupported provenance source ${dbg(value.source)}`,
        );
      }
      return field(requireSourceField(source, provenanceSourceField(value.field)));
    case "Literal":
      return literal(decodeLiteral(value.bytes, "literal value could not be decoded"));
  }
}

export function claimValue(path: ClaimPath, policy: PolicyContext): Value {
  if (policy.kind === "System")
    throw operatorGap("claim values require an identity policy context");
  const { permissionSubject, claims } = policy;
  const unbound = () => new UnsupportedError([{ kind: "UnboundClaim", path }]);
  const authorKey = authorClaimPathKey(path);
  if (authorKey !== undefined) {
    const value = authorPolicyClaims(permissionSubject).get(authorKey);
    if (value === undefined) throw unbound();
    return value;
  }
  const value = policyClaimAtPath(claims, path);
  if (value !== undefined) return value;
  if (path.length === 1 && path[0] === "user") return permissionSubject.toValue();
  throw unbound();
}

const isUnboundClaimReason = (reason: UnsupportedReason) => reason.kind === "UnboundClaim";

export function requireSourceField(source: ResolvedSource, field: string): string {
  const index = resolvedSourceDescriptorIndex(source, field);
  if (index === undefined) {
    throw operatorGap(
      `resolved source ${dbg(source.rowShape.source)} does not provide field '${field}'`,
    );
  }
  const name = source.rowShape.descriptor.fields()[index]?.name;
  if (name === undefined) {
    throw operatorGap(
      `resolved source ${dbg(source.rowShape.source)} field '${field}' has no carrier name`,
    );
  }
  return name;
}

const PROVENANCE_FIELDS: Record<ProvenanceField, string> = {
  CreatedAt: "$createdAt",
  CreatedBy: "$createdBy",
  UpdatedAt: "$updatedAt",
  UpdatedBy: "$updatedBy",
};
const provenanceSourceField = (field: ProvenanceField) => PROVENANCE_FIELDS[field];

export const hasExplicitClosurePath = (shape: NormalizedRowSetShape) =>
  shape.closurePaths.some((path) => path.kind === "ExplicitInclude");
