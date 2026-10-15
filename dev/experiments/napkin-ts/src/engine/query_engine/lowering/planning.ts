import type { SortedMap, SortedSet } from "../../../support/collections.js";
import type { DurabilityTier, RecursionBound } from "../../../_extern/engine.js";
import {
  dbg,
  structEq,
  structMap,
  structSet,
  type AggregateExpr,
  type CorrelationRequirement,
  type FrontierId,
  type JoinMode,
  type NormalizedValueRef,
  type OrderKey,
  type PredicateExpr,
  type ProgramPathId,
  type QueryProgramRequest,
  type RowProjection,
  type RowSetExpr,
  type RowSetNodeId,
  type RowVisibility,
  type SourceId,
  type UnionInput,
  type ValueSourceColumn,
  type ValueSourceMode,
} from "../input.js";
import { fromUtf8 } from "../../../support/bytes.js";
import { operatorGap, UnsupportedError, type UnsupportedReason } from "../resolver.js";
import { sourceExprCurrentTier, type SchemaProjection, type SourceExpr } from "../read.js";
import type { TypedOutputField } from "../schemas.js";

export const MAX_ROW_SET_NESTING_DEPTH = 256;

export type LinearCurrentRoot = { root: LinearRoot; steps: LinearStep[] };

export type LinearRoot =
  | { kind: "Source"; source: SourceId; visibility: RowVisibility }
  | { kind: "Value"; shape: string; columns: ValueSourceColumn[]; mode: ValueSourceMode }
  | { kind: "Frontier"; frontier: FrontierId; columns: ValueSourceColumn[] };

export const linearRootSource = (root: LinearRoot): SourceId | undefined =>
  root.kind === "Source" ? root.source : undefined;

export type AnalyzedQueryPlan =
  | { kind: "Linear"; plan: LinearCurrentRoot }
  | { kind: "Union"; plan: UnionPlan }
  | { kind: "CorrelatedPath"; plan: CorrelatedPathPlan }
  | { kind: "RecursiveRelation"; plan: RecursiveRelationPlan };

export function planRootSource(plan: AnalyzedQueryPlan): SourceId {
  let source: SourceId | undefined;
  switch (plan.kind) {
    case "Linear":
      source = linearRootSource(plan.plan.root);
      break;
    case "Union":
      source = unionRootSource(plan.plan);
      break;
    case "CorrelatedPath":
      source = linearRootSource(plan.plan.parent.root);
      break;
    case "RecursiveRelation":
      source = recursiveRootSource(plan.plan);
      break;
  }
  if (!source) throw new Error(`${plan.kind} root source`);
  return source;
}

export function planCapabilityLabel(plan: AnalyzedQueryPlan): string {
  switch (plan.kind) {
    case "Linear":
      return "table-rooted current lowering";
    case "Union":
      return "union current lowering";
    case "CorrelatedPath":
      return "correlated path projection analysis";
    case "RecursiveRelation":
      return "recursive relation analysis";
  }
}

export function firstStepSource(steps: LinearStep[]): SourceId | undefined {
  for (const step of steps) {
    if (step.kind === "Join") {
      const source = relationRootSource(step.right);
      if (source) return source;
    }
  }
  return undefined;
}

export type CorrelatedPathPlan = {
  parent: LinearCurrentRoot;
  child: LinearCurrentRoot;
  path: ProgramPathId;
  correlation: PredicateExpr;
  requirement: CorrelationRequirement;
  outputSteps: LinearStep[];
  siblings: CorrelatedPathPlan[];
  nested: CorrelatedPathPlan[];
};

export type RecursiveRelationPlan = {
  seed: LinearCurrentRoot;
  step: LinearCurrentRoot;
  frontier: FrontierId;
  frontierKey: NormalizedValueRef;
  dedupeKeys: NormalizedValueRef[];
  bound: RecursionBound;
};

export type UnionPlan = { branches: UnionBranchPlan[]; terminalSteps: LinearStep[] };
export type UnionBranchPlan = { label: string; plan: RelationInputPlan };

export function unionRootSource(union: UnionPlan): SourceId | undefined {
  const sources = union.branches
    .map((b) => relationRootSource(b.plan))
    .filter((s) => s !== undefined);
  if (sources.length === 0) return undefined;
  return sources.every((s) => structEq(s, sources[0])) ? sources[0] : undefined;
}

export const recursiveSeedSource = (r: RecursiveRelationPlan): SourceId | undefined =>
  linearRootSource(r.seed.root) ?? firstStepSource(r.seed.steps);
export const recursiveStepSource = (r: RecursiveRelationPlan): SourceId | undefined =>
  linearRootSource(r.step.root) ?? firstStepSource(r.step.steps);
export const recursiveRootSource = (r: RecursiveRelationPlan): SourceId | undefined =>
  recursiveSeedSource(r) ?? recursiveStepSource(r);

export type RelationInputPlan =
  | { kind: "Linear"; plan: LinearCurrentRoot }
  | { kind: "Union"; plan: UnionPlan }
  | { kind: "Recursive"; plan: RecursiveRelationPlan };

export function relationRootSource(plan: RelationInputPlan): SourceId | undefined {
  switch (plan.kind) {
    case "Linear":
      return linearRootSource(plan.plan.root);
    case "Union":
      return unionRootSource(plan.plan);
    case "Recursive":
      return recursiveRootSource(plan.plan);
  }
}

export type LinearStep =
  | { kind: "Filter"; predicate: PredicateExpr }
  | { kind: "Join"; right: RelationInputPlan; mode: JoinMode; on: PredicateExpr }
  | { kind: "Project"; columns: RowProjection[] }
  | { kind: "OrderBy"; keys: OrderKey[] }
  | {
      kind: "Slice";
      partitionBy: NormalizedValueRef[];
      limit?: number;
      offset: number;
      tieBreaker: NormalizedValueRef[];
      rankOutput?: TypedOutputField;
    }
  | { kind: "Aggregate"; groupBy: NormalizedValueRef[]; outputs: AggregateExpr[] };

/** Throws UnsupportedError carrying every gap. */
export function analyzeQueryPlan(request: QueryProgramRequest): AnalyzedQueryPlan {
  const gaps: UnsupportedReason[] = [];
  validateRowSetNesting(request.input.shape.root, request.input.shape.nodes);
  if (request.reads.factReads.size > 0) {
    gaps.push({ kind: "Source", gap: { kind: "TransactionReadOverlay" } });
  }
  let plan: AnalyzedQueryPlan;
  try {
    plan = analyzeRootNode(request);
  } catch (err) {
    if (err instanceof UnsupportedError) throw new UnsupportedError([...gaps, err.gap]);
    throw err;
  }
  plan = planWithDefaultResultOrder(plan, request);
  validateOutputCapabilities(request, plan, gaps);
  validateRecursiveArgByCapabilities(plan, gaps);
  for (const planSource of analyzedPlanSources(plan)) {
    const projection = supportedCurrentStorageProjection(
      request.reads.primary.sources.get(planSource),
    );
    if (!projection) {
      gaps.push({ kind: "Source", gap: { kind: "HistoricalStorageCut" } });
      continue;
    }
    if (
      projection.schemaFamily.kind !== "Current" ||
      !(
        projection.storage.kind === "Single" || projection.storage.kind === "CompatiblePartitions"
      ) ||
      projection.lens.kind !== "Canonical"
    ) {
      gaps.push({ kind: "Source", gap: { kind: "SchemaProjection" } });
    }
  }
  if (gaps.length > 0) throw new UnsupportedError(gaps);
  return plan;
}

function rowSetChildren(node: RowSetExpr): RowSetNodeId[] {
  switch (node.kind) {
    case "Source":
    case "ValueSource":
    case "FrontierSource":
      return [];
    case "Filter":
    case "Distinct":
    case "Project":
    case "OrderBy":
    case "Slice":
    case "Aggregate":
      return [node.input];
    case "Join":
      return [node.left, node.right];
    case "RecursiveRelation":
      return [node.seed, node.step];
    case "Union":
      return node.inputs.map((input) => input.node);
    case "CorrelatedPathProjection":
      return [node.input, node.childInput];
  }
}

function validateRowSetNesting(
  root: RowSetNodeId,
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
): void {
  const pending: [RowSetNodeId, boolean][] = [[root, false]];
  const active = new Set<RowSetNodeId>();
  const heights = new Map<RowSetNodeId, number>();
  while (pending.length > 0) {
    const [nodeId, expanded] = pending.pop()!;
    if (heights.has(nodeId)) continue;
    const node = nodes.get(nodeId);
    if (!node) throw operatorGap(`row-set node ${dbg(nodeId)} is missing`);
    const children = rowSetChildren(node);
    if (expanded) {
      active.delete(nodeId);
      let max = 0;
      for (const child of children) max = Math.max(max, heights.get(child) ?? 0);
      const height = 1 + max;
      if (height > MAX_ROW_SET_NESTING_DEPTH) {
        throw operatorGap(
          `row-set nesting depth exceeds MAX_ROW_SET_NESTING_DEPTH (${MAX_ROW_SET_NESTING_DEPTH})`,
        );
      }
      heights.set(nodeId, height);
      continue;
    }
    if (active.has(nodeId))
      throw operatorGap(`row-set nesting contains a cycle at node ${dbg(nodeId)}`);
    active.add(nodeId);
    pending.push([nodeId, true]);
    for (const child of children.reverse()) {
      if (active.has(child))
        throw operatorGap(`row-set nesting contains a cycle at node ${dbg(child)}`);
      if (!heights.has(child)) pending.push([child, false]);
    }
  }
}

const hasFact = (request: QueryProgramRequest, kind: string) =>
  [...request.output.facts].some((fact) => fact.kind === kind);

export function validateOutputCapabilities(
  request: QueryProgramRequest,
  plan: AnalyzedQueryPlan,
  gaps: UnsupportedReason[],
): void {
  if (!hasFact(request, "ResultMembership")) return;
  if (planContainsAggregate(plan)) return;
  if (maintainedResultMembershipWindowSupported(plan)) return;
  gaps.push({
    kind: "Operator",
    message: "maintained subscription view window shape is not lowered yet",
  });
}

function validateRecursiveArgByCapabilities(
  plan: AnalyzedQueryPlan,
  gaps: UnsupportedReason[],
): void {
  if (collectPlanFragments(plan).recursives.some(logicalRecursivePlanContainsArgBy)) {
    gaps.push({
      kind: "Operator",
      message: "arg_max_by and arg_min_by are not supported inside recursive seed or step graphs",
    });
  }
}

function logicalRecursivePlanContainsArgBy(relation: RecursiveRelationPlan): boolean {
  const linearPlans = [relation.seed, relation.step];
  const relationInputs: RelationInputPlan[] = [];
  for (;;) {
    const plan = linearPlans.pop();
    if (plan) {
      let pendingOrderIsEmpty = true;
      for (const step of plan.steps) {
        switch (step.kind) {
          case "OrderBy":
            pendingOrderIsEmpty = step.keys.length === 0;
            break;
          case "Slice":
            if (pendingOrderIsEmpty && step.offset === 0 && step.limit === 1) return true;
            pendingOrderIsEmpty = true;
            break;
          case "Aggregate":
            pendingOrderIsEmpty = true;
            break;
          case "Join":
            relationInputs.push(step.right);
            break;
        }
      }
      continue;
    }
    const input = relationInputs.pop();
    if (!input) return false;
    if (input.kind === "Linear") linearPlans.push(input.plan);
    else if (input.kind === "Union") relationInputs.push(...input.plan.branches.map((b) => b.plan));
    else linearPlans.push(input.plan.seed, input.plan.step);
  }
}

function planWithDefaultResultOrder(
  plan: AnalyzedQueryPlan,
  request: QueryProgramRequest,
): AnalyzedQueryPlan {
  const producesRootRows =
    request.output.appRows !== undefined || hasFact(request, "ResultMembership");
  if (!producesRootRows) return plan;
  if (plan.kind === "Linear") injectDefaultRootOrder(plan.plan.root, plan.plan.steps);
  else if (plan.kind === "CorrelatedPath")
    injectDefaultRootOrder(plan.plan.parent.root, plan.plan.outputSteps);
  return plan;
}

function injectDefaultRootOrder(root: LinearRoot, steps: LinearStep[]): void {
  const source = linearRootSource(root);
  if (!source) return;
  const firstTerminal = steps.findIndex(
    (step) => step.kind === "OrderBy" || step.kind === "Slice" || step.kind === "Aggregate",
  );
  if (firstTerminal < 0) return;
  const kind = steps[firstTerminal].kind;
  if (kind === "OrderBy" || kind === "Aggregate") return;
  steps.splice(firstTerminal, 0, defaultRootOrder(source));
}

const defaultRootOrder = (source: SourceId): LinearStep => ({
  kind: "OrderBy",
  keys: [{ value: { kind: "RowId", row: { kind: "Source", source } }, direction: "Asc" }],
});

function maintainedResultMembershipWindowSupported(plan: AnalyzedQueryPlan): boolean {
  // Relation-local slices use their declared row-id tie-breaker; root slices have an injected order.
  const fragments = collectPlanFragments(plan);
  return !fragments.recursives.some(
    (r) =>
      r.seed.steps.some((s) => s.kind === "Slice") || r.step.steps.some((s) => s.kind === "Slice"),
  );
}

function planContainsAggregate(plan: AnalyzedQueryPlan): boolean {
  return collectPlanFragments(plan).linears.some((f) =>
    f.steps.some((s) => s.kind === "Aggregate"),
  );
}

export function rootAggregateStep(
  plan: AnalyzedQueryPlan,
): [NormalizedValueRef[], AggregateExpr[]] | undefined {
  if (plan.kind !== "Linear") return undefined;
  const last = plan.plan.steps[plan.plan.steps.length - 1];
  return last?.kind === "Aggregate" ? [last.groupBy, last.outputs] : undefined;
}

function rootUnionTerminalSteps(
  root: RowSetNodeId,
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
): [UnionInput[], LinearStep[]] | undefined {
  let current = root;
  const outerToInner: LinearStep[] = [];
  for (;;) {
    const node = nodes.get(current);
    if (!node) return undefined;
    switch (node.kind) {
      case "OrderBy":
        outerToInner.push({ kind: "OrderBy", keys: node.keys });
        current = node.input;
        break;
      case "Slice":
        outerToInner.push(sliceStep(node));
        current = node.input;
        break;
      case "Union":
        return [node.inputs, outerToInner.reverse()];
      default:
        return undefined;
    }
  }
}

const sliceStep = (node: Extract<RowSetExpr, { kind: "Slice" }>): LinearStep => ({
  kind: "Slice",
  partitionBy: node.partitionBy,
  limit: node.limit,
  offset: node.offset,
  tieBreaker: node.tieBreaker,
  rankOutput: node.rankOutput,
});

const pathParentMustBeSource = () => operatorGap("path parent must be a source");

function analyzeRootNode(request: QueryProgramRequest): AnalyzedQueryPlan {
  let visited = new Set<RowSetNodeId>();
  const { root, nodes } = request.input.shape;
  const rootNode = nodes.get(root);
  if (!rootNode) throw operatorGap(`row-set root node ${dbg(root)} is missing`);

  const unionRoot = rootUnionTerminalSteps(root, nodes);
  if (unionRoot) {
    visited.add(root);
    const union = analyzeUnion(unionRoot[0], nodes, visited);
    union.terminalSteps = unionRoot[1];
    const source = unionRootSource(union);
    if (!source) throw operatorGap("union result branches must share one root source");
    validateResultSource(request, source);
    return { kind: "Union", plan: union };
  }

  let plan: AnalyzedQueryPlan;
  switch (rootNode.kind) {
    case "CorrelatedPathProjection": {
      visited.add(root);
      const parent = analyzeLinearSubplan(rootNode.input, nodes, visited);
      const child = analyzeCorrelatedChildSubplan(
        rootNode.childInput,
        rootNode.path,
        nodes,
        visited,
      );
      const parentSource = linearRootSource(parent.root);
      if (!parentSource) throw pathParentMustBeSource();
      validateResultSource(request, parentSource);
      plan = {
        kind: "CorrelatedPath",
        plan: {
          path: rootNode.path,
          correlation: rootNode.correlation,
          requirement: rootNode.requirement,
          outputSteps: [],
          siblings: collectCorrelatedPaths(parentSource, rootNode.path.child, nodes, visited),
          nested: collectCorrelatedPaths(rootNode.path.child, undefined, nodes, visited),
          parent,
          child,
        },
      };
      break;
    }
    case "RecursiveRelation": {
      visited.add(root);
      const seed = analyzeLinearSubplan(rootNode.seed, nodes, visited);
      const step = analyzeLinearSubplan(rootNode.step, nodes, visited);
      const result = request.input.shape.result;
      const ok =
        (result.kind === "RealRow" &&
          result.row.kind === "Source" &&
          (structEq(linearRootSource(seed.root), result.row.source) ||
            structEq(linearRootSource(step.root), result.row.source))) ||
        result.kind === "PathTuple";
      if (!ok)
        throw operatorGap("recursive relation result must be a seed/step real row or path tuple");
      plan = {
        kind: "RecursiveRelation",
        plan: {
          seed,
          step,
          frontier: rootNode.frontier,
          frontierKey: rootNode.frontierKey,
          dedupeKeys: rootNode.dedupeKeys,
          bound: rootNode.bound,
        },
      };
      break;
    }
    case "Union": {
      visited.add(root);
      const union = analyzeUnion(rootNode.inputs, nodes, visited);
      const source = unionRootSource(union);
      if (!source) throw operatorGap("union result branches must share one root source");
      validateResultSource(request, source);
      plan = { kind: "Union", plan: union };
      break;
    }
    default: {
      const pathVisited = new Set(visited);
      let pathPlan: CorrelatedPathPlan | undefined;
      try {
        pathPlan = analyzeCorrelatedPathRoot(root, request, pathVisited);
      } catch (err) {
        if (!(err instanceof UnsupportedError)) throw err;
      }
      if (pathPlan) {
        pathPlan.nested = collectCorrelatedPaths(
          pathPlan.path.child,
          undefined,
          nodes,
          pathVisited,
        );
        const parentSource = linearRootSource(pathPlan.parent.root);
        if (!parentSource) throw pathParentMustBeSource();
        pathPlan.siblings = collectCorrelatedPaths(
          parentSource,
          pathPlan.path.child,
          nodes,
          pathVisited,
        );
        validateResultSource(request, parentSource);
        visited = pathVisited;
        plan = { kind: "CorrelatedPath", plan: pathPlan };
      } else {
        const linear = analyzeLinearSubplan(root, nodes, visited);
        const source = linearRootSource(linear.root);
        if (!source) throw operatorGap("result must be the root source row");
        validateResultSource(request, source);
        plan = { kind: "Linear", plan: linear };
      }
    }
  }
  if (visited.size !== nodes.size) {
    throw operatorGap(
      "only connected current source/filter/join/order/slice/path/relation plans are lowered yet",
    );
  }
  return plan;
}

function analyzeCorrelatedPathRoot(
  nodeId: RowSetNodeId,
  request: QueryProgramRequest,
  visited: Set<RowSetNodeId>,
): CorrelatedPathPlan {
  const nodes = request.input.shape.nodes;
  const node = nodes.get(nodeId);
  if (!node) throw operatorGap(`row-set node ${dbg(nodeId)} is missing`);
  visited.add(nodeId);
  switch (node.kind) {
    case "CorrelatedPathProjection":
      return {
        parent: analyzeLinearSubplan(node.input, nodes, visited),
        child: analyzeCorrelatedChildSubplan(node.childInput, node.path, nodes, visited),
        path: node.path,
        correlation: node.correlation,
        requirement: node.requirement,
        outputSteps: [],
        siblings: [],
        nested: collectCorrelatedPaths(node.path.child, undefined, nodes, visited),
      };
    case "OrderBy": {
      const plan = analyzeCorrelatedPathRoot(node.input, request, visited);
      plan.outputSteps.push({ kind: "OrderBy", keys: node.keys });
      return plan;
    }
    case "Slice": {
      const plan = analyzeCorrelatedPathRoot(node.input, request, visited);
      plan.outputSteps.push(sliceStep(node));
      return plan;
    }
    case "Project": {
      const plan = analyzeCorrelatedPathRoot(node.input, request, visited);
      plan.outputSteps.push({ kind: "Project", columns: node.columns });
      return plan;
    }
    default:
      throw operatorGap("root is not a correlated path plan");
  }
}

function collectCorrelatedPaths(
  owner: SourceId,
  excludedChild: SourceId | undefined,
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
  visited: Set<RowSetNodeId>,
): CorrelatedPathPlan[] {
  const paths: CorrelatedPathPlan[] = [];
  for (const [nodeId, node] of nodes) {
    if (node.kind !== "CorrelatedPathProjection") continue;
    if (
      !structEq(node.path.owner, owner) ||
      (excludedChild && structEq(node.path.child, excludedChild))
    ) {
      continue;
    }
    visited.add(nodeId);
    const parentVisited = new Set<RowSetNodeId>();
    const parent = analyzeLinearSubplan(node.input, nodes, parentVisited);
    parentVisited.forEach((v) => visited.add(v));
    const childVisited = new Set<RowSetNodeId>();
    const child = analyzeCorrelatedChildSubplan(node.childInput, node.path, nodes, childVisited);
    childVisited.forEach((v) => visited.add(v));
    paths.push({
      parent,
      child,
      path: node.path,
      correlation: node.correlation,
      requirement: node.requirement,
      outputSteps: [],
      siblings: [],
      nested: collectCorrelatedPaths(node.path.child, undefined, nodes, visited),
    });
  }
  return paths;
}

function analyzeCorrelatedChildSubplan(
  childInput: RowSetNodeId,
  path: ProgramPathId,
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
  visited: Set<RowSetNodeId>,
): LinearCurrentRoot {
  const node = nodes.get(childInput);
  if (node?.kind === "CorrelatedPathProjection" && structEq(node.path.owner, path.child)) {
    return analyzeLinearSubplan(node.input, nodes, visited);
  }
  return analyzeLinearSubplan(childInput, nodes, visited);
}

function analyzeLinearSubplan(
  nodeId: RowSetNodeId,
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
  visited: Set<RowSetNodeId>,
): LinearCurrentRoot {
  const [root, steps] = analyzeCurrentNode(nodeId, nodes, visited);
  const gaps: UnsupportedReason[] = [];
  validateStepOrder(steps, gaps);
  if (gaps.length > 0) throw new UnsupportedError([gaps[0]]);
  return { root, steps };
}

function validateResultSource(request: QueryProgramRequest, source: SourceId): void {
  const result = request.input.shape.result;
  if (
    !(
      result.kind === "RealRow" &&
      result.row.kind === "Source" &&
      structEq(result.row.source, source)
    )
  ) {
    throw operatorGap("result must be the root source row");
  }
}

export type LinearFragment = { root?: LinearRoot; steps: LinearStep[] };
export type PlanFragments = {
  linears: LinearFragment[];
  correlations: PredicateExpr[];
  recursives: RecursiveRelationPlan[];
};

export function collectPlanFragments(plan: AnalyzedQueryPlan): PlanFragments {
  const fragments: PlanFragments = { linears: [], correlations: [], recursives: [] };
  switch (plan.kind) {
    case "Linear":
      collectLinearFragments(plan.plan, fragments);
      break;
    case "Union":
      collectUnionFragments(plan.plan, fragments);
      break;
    case "CorrelatedPath":
      collectCorrelatedPathFragments(plan.plan, fragments);
      break;
    case "RecursiveRelation":
      collectRecursiveFragments(plan.plan, fragments);
  }
  return fragments;
}

function collectCorrelatedPathFragments(path: CorrelatedPathPlan, fragments: PlanFragments): void {
  collectLinearFragments(path.parent, fragments);
  collectLinearFragments(path.child, fragments);
  fragments.correlations.push(path.correlation);
  if (path.outputSteps.length > 0) {
    fragments.linears.push({ steps: path.outputSteps });
    collectStepRelationFragments(path.outputSteps, fragments);
  }
  for (const sibling of path.siblings) collectCorrelatedPathFragments(sibling, fragments);
  for (const nested of path.nested) collectCorrelatedPathFragments(nested, fragments);
}

function collectRelationFragments(plan: RelationInputPlan, fragments: PlanFragments): void {
  if (plan.kind === "Linear") collectLinearFragments(plan.plan, fragments);
  else if (plan.kind === "Union") collectUnionFragments(plan.plan, fragments);
  else collectRecursiveFragments(plan.plan, fragments);
}

function collectUnionFragments(union: UnionPlan, fragments: PlanFragments): void {
  for (const branch of union.branches) collectRelationFragments(branch.plan, fragments);
}

function collectRecursiveFragments(
  relation: RecursiveRelationPlan,
  fragments: PlanFragments,
): void {
  fragments.recursives.push(relation);
  collectLinearFragments(relation.seed, fragments);
  collectLinearFragments(relation.step, fragments);
}

function collectLinearFragments(linear: LinearCurrentRoot, fragments: PlanFragments): void {
  fragments.linears.push({ root: linear.root, steps: linear.steps });
  collectStepRelationFragments(linear.steps, fragments);
}

function collectStepRelationFragments(steps: LinearStep[], fragments: PlanFragments): void {
  for (const step of steps)
    if (step.kind === "Join") collectRelationFragments(step.right, fragments);
}

function analyzedPlanSources(plan: AnalyzedQueryPlan): SortedSet<SourceId> {
  const sources = structSet<SourceId>();
  for (const fragment of collectPlanFragments(plan).linears) {
    const source = fragment.root && linearRootSource(fragment.root);
    if (source) sources.add(source);
  }
  return sources;
}

export function programSources(
  request: QueryProgramRequest,
  plan: AnalyzedQueryPlan,
): SortedSet<SourceId> {
  const sources = analyzedPlanSources(plan);
  for (const source of request.input.shape.auxiliarySources) sources.add(source);
  return sources;
}

export function sourceVisibilities(plan: AnalyzedQueryPlan): SortedMap<SourceId, RowVisibility> {
  const visibilities = structMap<SourceId, RowVisibility>();
  for (const fragment of collectPlanFragments(plan).linears) {
    if (fragment.root?.kind !== "Source") continue;
    const entry = visibilities.getOrInsert(fragment.root.source, () => "Visible");
    if (fragment.root.visibility === "IncludeDeleted" && entry === "Visible") {
      visibilities.set(fragment.root.source, "IncludeDeleted");
    }
  }
  return visibilities;
}

export function sourceCurrentTier(
  request: QueryProgramRequest,
  source: SourceId,
): DurabilityTier | undefined {
  const expr = request.reads.primary.sources.get(source);
  return expr && sourceExprCurrentTier(expr);
}

function supportedCurrentStorageProjection(
  source: SourceExpr | undefined,
): SchemaProjection | undefined {
  if (!source) return undefined;
  switch (source.kind) {
    case "VisibleCurrent":
    case "BranchView":
    case "SettledBindingView":
      return source.projection;
    case "HistoryCut":
    case "SnapshotRef":
      return source.data.kind === "Current" ? source.projection : undefined;
    case "WithOverlays": {
      const entries = source.overlays.entries as { kind: string }[];
      const pendingOverSettled =
        entries.length === 1 &&
        entries[0].kind === "PendingLocal" &&
        source.input.kind === "SettledBindingView";
      return pendingOverSettled || entries.every((o) => o.kind === "OpenTransaction")
        ? supportedCurrentStorageProjection(source.input)
        : undefined;
    }
    default:
      return undefined;
  }
}

function analyzeCurrentNode(
  nodeId: RowSetNodeId,
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
  visited: Set<RowSetNodeId>,
): [LinearRoot, LinearStep[]] {
  visited.add(nodeId);
  const node = nodes.get(nodeId);
  if (!node) throw operatorGap(`row-set node ${dbg(nodeId)} is missing`);
  const chain = (input: RowSetNodeId, step: LinearStep): [LinearRoot, LinearStep[]] => {
    const [root, steps] = analyzeCurrentNode(input, nodes, visited);
    steps.push(step);
    return [root, steps];
  };
  switch (node.kind) {
    case "Source":
      return [{ kind: "Source", source: node.source, visibility: node.visibility }, []];
    case "ValueSource":
      return [{ kind: "Value", shape: node.shape, columns: node.columns, mode: node.mode }, []];
    case "FrontierSource":
      return [{ kind: "Frontier", frontier: node.frontier, columns: node.columns }, []];
    case "Filter":
      return chain(node.input, { kind: "Filter", predicate: node.predicate });
    case "OrderBy":
      return chain(node.input, { kind: "OrderBy", keys: node.keys });
    case "Slice":
      return chain(node.input, sliceStep(node));
    case "Join": {
      const [root, steps] = analyzeCurrentNode(node.left, nodes, visited);
      const right = analyzeRelationInputNode(node.right, nodes, visited);
      steps.push({ kind: "Join", right, mode: node.mode, on: node.on });
      return [root, steps];
    }
    case "Project":
      return chain(node.input, { kind: "Project", columns: node.columns });
    case "RecursiveRelation":
      throw operatorGap("recursive relation row-set nodes are not lowered yet");
    case "Union":
      throw operatorGap("union row-set nodes are not lowered yet");
    case "Distinct":
      throw operatorGap(
        unsupportedMarkerMessage(node.keys) ?? "distinct row-set nodes are not lowered yet",
      );
    case "CorrelatedPathProjection":
      return analyzeCurrentNode(node.input, nodes, visited);
    case "Aggregate":
      return chain(node.input, { kind: "Aggregate", groupBy: node.groupBy, outputs: node.outputs });
  }
}

export function analyzeRelationInputNode(
  nodeId: RowSetNodeId,
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
  visited: Set<RowSetNodeId>,
): RelationInputPlan {
  validateRowSetNesting(nodeId, nodes);
  const node = nodes.get(nodeId);
  if (!node) throw operatorGap(`row-set node ${dbg(nodeId)} is missing`);
  if (node.kind === "Union") {
    visited.add(nodeId);
    return { kind: "Union", plan: analyzeUnion(node.inputs, nodes, visited) };
  }
  if (node.kind === "RecursiveRelation") {
    visited.add(nodeId);
    return {
      kind: "Recursive",
      plan: {
        seed: analyzeLinearSubplan(node.seed, nodes, visited),
        step: analyzeLinearSubplan(node.step, nodes, visited),
        frontier: node.frontier,
        frontierKey: node.frontierKey,
        dedupeKeys: node.dedupeKeys,
        bound: node.bound,
      },
    };
  }
  const linear = analyzeLinearSubplan(nodeId, nodes, visited);
  validateJoinRelation(linear);
  return { kind: "Linear", plan: linear };
}

function analyzeUnion(
  inputs: UnionInput[],
  nodes: SortedMap<RowSetNodeId, RowSetExpr>,
  visited: Set<RowSetNodeId>,
): UnionPlan {
  if (inputs.length === 0) throw operatorGap("union row-set nodes require at least one input");
  const labels = new Set<string>();
  const branches: UnionBranchPlan[] = [];
  for (const input of inputs) {
    if (input.label.length === 0 || input.label.includes("\0")) {
      throw operatorGap("union arm labels must be non-empty, NUL-free stable semantic identities");
    }
    if (labels.has(input.label)) {
      throw operatorGap(
        `union arm label ${dbg(input.label)} is duplicated; occurrence identity requires unique stable semantic arm labels`,
      );
    }
    labels.add(input.label);
    branches.push({
      label: input.label,
      plan: analyzeRelationInputNode(input.node, nodes, visited),
    });
  }
  return { branches, terminalSteps: [] };
}

function validateJoinRelation(plan: LinearCurrentRoot): void {
  for (const step of plan.steps) {
    if (step.kind === "OrderBy" || step.kind === "Slice" || step.kind === "Aggregate") {
      throw operatorGap("join inputs do not support order/slice/aggregate operators yet");
    }
  }
}

function unsupportedMarkerMessage(keys: NormalizedValueRef[]): string | undefined {
  if (keys.length !== 1 || keys[0].kind !== "Literal") return undefined;
  try {
    return fromUtf8(keys[0].bytes);
  } catch {
    return undefined;
  }
}

function validateStepOrder(steps: LinearStep[], gaps: UnsupportedReason[]): void {
  let seenOrder = false;
  let seenSlice = false;
  let seenAggregate = false;
  const gap = (message: string) => gaps.push({ kind: "Operator", message });
  for (const step of steps) {
    switch (step.kind) {
      case "Filter":
      case "Join":
        if (seenOrder || seenSlice || seenAggregate) {
          gap("filters/joins after order/slice/aggregate are not lowered yet");
        }
        break;
      case "Project":
        if (seenAggregate) gap("projects after aggregate are not lowered yet");
        break;
      case "OrderBy":
        if (seenAggregate) gap("order/slice after aggregate is not lowered yet");
        else if (seenSlice) gap("order-by after slice is not lowered yet");
        else if (seenOrder) gap("multiple order-by nodes are not lowered yet");
        else seenOrder = true;
        break;
      case "Slice":
        if (seenAggregate) {
          gap("order/slice after aggregate is not lowered yet");
          break;
        }
        if (seenSlice) gap("multiple slice nodes are not lowered yet");
        if (step.rankOutput) gap("slice rank outputs are not lowered yet");
        seenSlice = true;
        break;
      case "Aggregate":
        if (seenOrder || seenSlice) gap("aggregate over ordered/windowed input is not lowered yet");
        seenAggregate = true;
    }
  }
}
