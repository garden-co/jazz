import { SortedSet, cmpStr, type SortedMap } from "../../../support/collections.js";
import { GraphBuilder, ProjectField, type ValueType } from "../../../_extern/engine.js";
import {
  CLOSURE_REQUIRED_ELEMENT,
  LEFT_JOIN_PREFIX,
  RIGHT_JOIN_PREFIX,
  leftField,
  rightField,
  userColumnField,
} from "../fields.js";
import {
  dbg,
  structEq,
  structMap,
  type ClosurePath,
  type ClosurePathSegment,
  type ClosureRootGate,
  type InheritedContribution,
  type JoinContribution,
  type NormalizedValueRef,
  type PredicateExpr,
  type ReachableContribution,
  type RowSetExpr,
  type RowSetNodeId,
  type SourceId,
} from "../input.js";
import {
  CapabilityError,
  lookupResolvedSource,
  operatorGap,
  orReport,
  UnsupportedError,
  type ResolvedSource,
} from "../resolver.js";
import type { TypedOutputField } from "../schemas.js";
import { unionMember } from "../lowering.js";
import type { LoweringContext } from "./arguments.js";
import {
  analyzeRelationInputNode,
  recursiveSeedSource,
  recursiveRootSource,
  recursiveStepSource,
  type AnalyzedQueryPlan,
  type RecursiveRelationPlan,
  type RelationInputPlan,
} from "./planning.js";
import {
  lowerRecursiveRelationCached,
  lowerRecursiveSeedMembership,
  lowerRelationInputForContributor,
  lowerRelationKeyRef,
  lowerRootToRelationKeyPairs,
  sourceFieldType,
  unwrapNullableJoinKey,
} from "./graph_lowering.js";
import {
  flatJoinPayloadFields,
  projectSourceFieldsFromPrefix,
  projectSourceFieldsWithRoutes,
  projectSourceFieldsWithRoutesFromPrefix,
  sourceRowsForVisibleGraph,
} from "./terminals.js";

type Sources = SortedMap<SourceId, ResolvedSource>;
type Nodes = SortedMap<RowSetNodeId, RowSetExpr>;
const stringSet = (items?: Iterable<string>) => new SortedSet(cmpStr, items);

export type ClosureLowering = {
  visibleRoot: GraphBuilder;
  resultMembers: SortedMap<SourceId, GraphBuilder>;
};

export function lowerClosureMembership(
  rootGraph: GraphBuilder,
  request: LoweringContext,
  plan: AnalyzedQueryPlan,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  routeFields: SortedSet<string>,
  rootCarrierFields: SortedSet<string>,
): ClosureLowering {
  let visibleRoot = rootGraph;
  for (const path of request.input.shape.closurePaths) {
    if (path.kind === "ExplicitInclude" && path.rootGate) {
      visibleRoot = requiredClosureParentGraph(
        visibleRoot,
        path.segments,
        0,
        path.rootGate,
        rootSource,
        resolvedSources,
        rootCarrierFields,
      );
    }
  }
  const resultMembers = structMap<SourceId, GraphBuilder>();
  for (const path of request.input.shape.closurePaths) {
    const base =
      flatJoinPayloadFields(plan).length === 0
        ? visibleRoot
        : sourceRowsForVisibleGraph(rootSource, visibleRoot, routeFields);
    for (const [, source, graph] of closureMembershipGraphForPath(
      base,
      path,
      rootSource,
      resolvedSources,
      routeFields,
    )) {
      const resolved = resolvedSources.get(source);
      if (!resolved) continue;
      // A member depends on the root's routes and on its own policy routes.
      const memberRoutes = stringSet([...routeFields, ...resolved.routingFields]);
      unionMember(
        resultMembers,
        source,
        graph.projectFields(projectSourceFieldsWithRoutes(resolved, memberRoutes)),
      );
    }
  }
  return { visibleRoot, resultMembers };
}

function analyzeAndLower(
  input: RowSetNodeId,
  nodes: Nodes,
  sources: Sources,
  request: LoweringContext,
) {
  return orReport(() => {
    const plan = analyzeRelationInputNode(input, nodes, new Set());
    return { plan, lowered: lowerRelationInputForContributor(plan, sources, request) };
  });
}

export function reachableContributionMembershipGraph(
  visibleRoot: GraphBuilder,
  contribution: ReachableContribution,
  rootSource: ResolvedSource,
  contributionSource: ResolvedSource,
  nodes: Nodes,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
): GraphBuilder {
  const { lowered } = analyzeAndLower(contribution.accessInput, nodes, resolvedSources, request);
  const joinField = reachableRootReferenceField(contributionSource, contribution.rootRefField);
  if (!lowered.fields.has(joinField)) {
    throw CapabilityError.explained(
      {
        kind: "Operator",
        message: `reachable contribution ${contribution.id} does not provide root reference field ${joinField}`,
      },
      "reachable contribution payload requires root reference field",
    );
  }
  let graph = lowered.graph;
  if (lowered.nullableFields.has(joinField)) graph = unwrapNullableJoinKey(graph, joinField, 1);
  return GraphBuilder.join(
    visibleRoot,
    graph,
    [rootSource.rowShape.rowUuidField],
    [joinField],
  ).projectFields(projectJoinContributionFieldsWithRootRoutes(contributionSource, routeFields));
}

/** `ReachableVia` names `id` even when the table uses Jazz's implicit row id. */
function reachableRootReferenceField(source: ResolvedSource, field: string): string {
  const user = userColumnField(field);
  const declared = source.rowShape.descriptor.fields().some((c: any) => c.name === user);
  return declared || field !== "id" ? user : source.rowShape.rowUuidField;
}

/** The side stream owned by the one recursive step that physically reads `edge_source`. */
export function reachableStepWitnessMembershipGraph(
  contribution: ReachableContribution,
  nodes: Nodes,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
): GraphBuilder {
  const plan = orReport(() => analyzeRelationInputNode(contribution.accessInput, nodes, new Set()));
  const recursive = recursiveRelationForEdge(plan, contribution.edgeSource);
  if (!recursive) {
    throw CapabilityError.operator(
      `reachable contribution ${contribution.id} has no recursive step for edge source ${dbg(contribution.edgeSource)}`,
    );
  }
  const rootId = recursiveRootSource(recursive);
  if (!rootId) throw CapabilityError.operator("recursive step witness requires a source root");
  const rootSource = resolvedSources.get(rootId);
  if (!rootSource)
    throw CapabilityError.runtime(`recursive witness root ${dbg(rootId)} was not resolved`);
  const edgeSource = lookupResolvedSource(
    resolvedSources,
    contribution.edgeSource,
    "recursive edge witness",
  );
  const lowered = orReport(() =>
    lowerRecursiveRelationCached(
      undefined,
      recursive,
      rootSource,
      resolvedSources,
      request,
      undefined,
    ),
  );
  const routes = stringSet([...edgeSource.routingFields].filter((f) => routeFields.has(f)));
  return GraphBuilder.recursiveStepWitness(lowered.graph).projectFields(
    projectJoinContributionFieldsWithRootRoutes(edgeSource, routes),
  );
}

/** The exact recursive seed rows that participate in a reachable contributor. */
export function reachableSeedMembershipGraph(
  contribution: ReachableContribution,
  nodes: Nodes,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
): [SourceId, GraphBuilder] | undefined {
  const plan = orReport(() => analyzeRelationInputNode(contribution.accessInput, nodes, new Set()));
  const recursive = recursiveRelationForEdge(plan, contribution.edgeSource);
  if (!recursive) {
    throw CapabilityError.operator(
      `reachable contribution ${contribution.id} has no recursive seed for edge source ${dbg(contribution.edgeSource)}`,
    );
  }
  // A literal seed is already a bound input; only physical seed sources become CoveredInput facts.
  const seedId = recursiveSeedSource(recursive);
  if (!seedId) return undefined;
  const seedSource = lookupResolvedSource(resolvedSources, seedId, "recursive seed");
  const seed = orReport(() =>
    lowerRecursiveSeedMembership(recursive, seedSource, resolvedSources, request),
  );
  const routes = stringSet([...seedSource.routingFields].filter((f) => routeFields.has(f)));
  return [seedId, seed.graph.projectFields(projectSourceFieldsWithRoutes(seedSource, routes))];
}

function recursiveRelationForEdge(
  plan: RelationInputPlan,
  edgeSource: SourceId,
): RecursiveRelationPlan | undefined {
  const pending = [plan];
  while (pending.length > 0) {
    const current = pending.pop()!;
    if (current.kind === "Recursive") {
      if (structEq(recursiveStepSource(current.plan), edgeSource)) return current.plan;
    } else if (current.kind === "Linear") {
      for (const step of [...current.plan.steps].reverse())
        if (step.kind === "Join") pending.push(step.right);
    } else {
      pending.push(...[...current.plan.branches].reverse().map((b) => b.plan));
    }
  }
  return undefined;
}

export function joinContributionMembershipGraph(
  visibleRoot: GraphBuilder,
  contribution: JoinContribution,
  rootSource: ResolvedSource,
  contributionSource: ResolvedSource,
  nodes: Nodes,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
): GraphBuilder {
  return relationMembershipGraph(
    visibleRoot,
    contribution.input,
    contribution.membership,
    (joinKey) =>
      CapabilityError.explained(
        {
          kind: "Operator",
          message: `join contribution ${contribution.id} does not provide join key field ${joinKey}`,
        },
        "join contribution payload requires the normalized join key",
      ),
    rootSource,
    contributionSource,
    nodes,
    resolvedSources,
    request,
    routeFields,
  );
}

/** Parent rows consumed by a caller-requested `inherits` semi-join. */
export function inheritedContributionMembershipGraph(
  visibleRoot: GraphBuilder,
  contribution: InheritedContribution,
  rootSource: ResolvedSource,
  parentSource: ResolvedSource,
  nodes: Nodes,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
): GraphBuilder {
  return relationMembershipGraph(
    visibleRoot,
    contribution.input,
    contribution.membership,
    (joinKey) =>
      CapabilityError.operator(
        `inherited contribution ${contribution.id} does not provide parent key field ${joinKey}`,
      ),
    rootSource,
    parentSource,
    nodes,
    resolvedSources,
    request,
    routeFields,
  );
}

function relationMembershipGraph(
  visibleRoot: GraphBuilder,
  input: RowSetNodeId,
  membership: PredicateExpr,
  missingJoinKey: (key: string) => CapabilityError,
  rootSource: ResolvedSource,
  targetSource: ResolvedSource,
  nodes: Nodes,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
): GraphBuilder {
  const { plan, lowered } = analyzeAndLower(input, nodes, resolvedSources, request);
  const [rootKeys, joinKeys] = orReport(() =>
    lowerRootToRelationKeyPairs(membership, rootSource, plan, lowered, request),
  );
  const missing = joinKeys.find((key) => !lowered.fields.has(key));
  if (missing !== undefined) throw missingJoinKey(missing);
  let graph = lowered.graph;
  const unwrapped = new Set<string>();
  for (const key of joinKeys) {
    if (lowered.nullableFields.has(key) && !unwrapped.has(key)) {
      unwrapped.add(key);
      graph = unwrapNullableJoinKey(graph, key, 1);
    }
  }
  return GraphBuilder.join(visibleRoot, graph, rootKeys, joinKeys).projectFields(
    projectJoinContributionFieldsWithRootRoutes(targetSource, routeFields),
  );
}

/** Derive one flat-join source from the already-authorized rendered root. */
export function flatJoinContributionMembershipGraph(
  visibleRoot: GraphBuilder,
  contribution: JoinContribution,
  contributionSource: ResolvedSource,
  nodes: Nodes,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
): GraphBuilder {
  const { plan, lowered } = analyzeAndLower(contribution.input, nodes, resolvedSources, request);
  const membership = contribution.membership;
  if (membership.kind !== "Compare" || membership.op !== "Eq") {
    throw CapabilityError.operator("flat join contribution membership must be equality");
  }
  let visibleKey: TypedOutputField;
  let relationKey: string;
  try {
    visibleKey = flatJoinVisibleField(membership.left, nodes);
    relationKey = lowerRelationKeyRef(membership.right, plan, lowered, request);
  } catch (err) {
    if (!(err instanceof UnsupportedError)) throw err;
    visibleKey = orReport(() => flatJoinVisibleField(membership.right, nodes));
    relationKey = orReport(() => lowerRelationKeyRef(membership.left, plan, lowered, request));
  }
  let relationGraph = lowered.graph;
  if (lowered.nullableFields.has(relationKey)) {
    relationGraph = unwrapNullableJoinKey(
      relationGraph,
      relationKey,
      lowered.nullableFieldDepths.get(relationKey) ?? 1,
    );
  }
  // Public projections keep authored nullability; unwrap on this private frontier only.
  const [visibleGraph] = unwrapNullableLayers(visibleRoot, visibleKey.name, visibleKey.ty);
  return GraphBuilder.join(
    visibleGraph,
    relationGraph,
    [visibleKey.name],
    [relationKey],
  ).projectFields(projectJoinContributionFieldsWithRootRoutes(contributionSource, routeFields));
}

/** Row fields come from the right input, routing fields from the left root. */
export function projectJoinContributionFieldsWithRootRoutes(
  source: ResolvedSource,
  routeFields: SortedSet<string>,
): any[] {
  const fields = source.rowShape.descriptor
    .fields()
    .map((f: any) => f.name)
    .filter((name: string | undefined) => name !== undefined && !routeFields.has(name))
    .map((name: string) => ProjectField.renamed(`${RIGHT_JOIN_PREFIX}${name}`, name));
  for (const field of routeFields)
    fields.push(ProjectField.renamed(`${LEFT_JOIN_PREFIX}${field}`, field));
  return fields;
}

function flatJoinVisibleField(value: NormalizedValueRef, nodes: Nodes): TypedOutputField {
  // The rendered flat root is the compiler-owned `flat_join:output` projection.
  const node = nodes.get("flat_join:output");
  if (node?.kind !== "Project")
    throw operatorGap("flat join contribution is missing its output projection");
  const column = node.columns.find((c) => structEq(c.value, value));
  if (!column)
    throw operatorGap("flat join contribution key is not retained by the output projection");
  return column.output;
}

function requiredClosureParentGraph(
  parentGraph: GraphBuilder,
  segments: ClosurePathSegment[],
  index: number,
  rootGate: ClosureRootGate,
  parentSource: ResolvedSource,
  resolvedSources: Sources,
  routeFields: SortedSet<string>,
): GraphBuilder {
  const segment = segments[index];
  if (!segment) return parentGraph;
  const target = lookupResolvedSource(resolvedSources, segment.target, "closure target");
  const targetValid = requiredClosureParentGraph(
    target.graph,
    segments,
    index + 1,
    rootGate,
    target,
    resolvedSources,
    stringSet(),
  );
  const sourceKey = userColumnField(segment.sourceField);
  const sourceKeyType = sourceFieldType(parentSource, sourceKey);
  if (sourceKeyType === undefined) {
    throw CapabilityError.operator(`closure source field ${dbg(sourceKey)} is not projected`);
  }
  const parentRowUuid = parentSource.rowShape.rowUuidField;
  const targetRowUuid = target.rowShape.rowUuidField;
  const [requiredBase, requiredKeyType] = unwrapNullableLayers(
    parentGraph,
    sourceKey,
    sourceKeyType,
  );
  const isArray = requiredKeyType.kind === "Array";
  const required = isArray
    ? requiredBase.unnest(sourceKey, CLOSURE_REQUIRED_ELEMENT)
    : requiredBase;
  const leftKey = isArray ? CLOSURE_REQUIRED_ELEMENT : sourceKey;
  const coveredFields = projectSourceFieldsWithRoutesFromPrefix(
    parentSource,
    LEFT_JOIN_PREFIX,
    routeFields,
  );
  if (leftKey === CLOSURE_REQUIRED_ELEMENT) {
    coveredFields.push(
      ProjectField.renamed("left.__closure_required_element", CLOSURE_REQUIRED_ELEMENT),
    );
  }
  const covered = GraphBuilder.join(
    required,
    targetValid,
    [leftKey],
    [targetRowUuid],
  ).projectFields(coveredFields);
  const requiredKey = sourceKeyForRequired(requiredKeyType, sourceKey);
  if (rootGate === "Inner" && !isArray) {
    // Select authoritative parent rows by stable identity so the source carrier is kept exactly.
    const coveredRoots = covered.projectFields([ProjectField.named(parentRowUuid)]);
    return GraphBuilder.semiJoin(
      parentGraph,
      coveredRoots,
      [parentRowUuid],
      [parentRowUuid],
    ).projectFields(projectSourceFieldsWithRoutes(parentSource, routeFields));
  }
  const missing = (
    leftKey === CLOSURE_REQUIRED_ELEMENT
      ? GraphBuilder.antiJoin(
          required,
          covered,
          [parentRowUuid, leftKey],
          [parentRowUuid, requiredKey],
        )
      : GraphBuilder.antiJoin(required, covered, [leftKey], [requiredKey])
  ).projectFields(projectSourceFieldsWithRoutes(parentSource, routeFields));
  const allRequiredRefsResolve = GraphBuilder.antiJoin(
    parentGraph,
    missing,
    [parentRowUuid],
    [parentRowUuid],
  );
  if (rootGate === "Required") return allRequiredRefsResolve;
  return GraphBuilder.join(
    allRequiredRefsResolve,
    GraphBuilder.argMinBy(covered, [parentRowUuid], [parentRowUuid]),
    [parentRowUuid],
    [parentRowUuid],
  ).projectFields(
    projectSourceFieldsWithRoutesFromPrefix(parentSource, LEFT_JOIN_PREFIX, routeFields),
  );
}

const sourceKeyForRequired = (sourceKeyType: ValueType, sourceKey: string): string =>
  sourceKeyType.kind === "Array" ? CLOSURE_REQUIRED_ELEMENT : sourceKey;

function unwrapNullableLayers(
  graph: GraphBuilder,
  field: string,
  valueType: ValueType,
): [GraphBuilder, ValueType] {
  while (valueType.kind === "Nullable") {
    graph = graph.unwrapNullable(field);
    valueType = valueType.inner;
  }
  return [graph, valueType];
}

function closureMembershipGraphForPath(
  rootGraph: GraphBuilder,
  path: ClosurePath,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  routeFields: SortedSet<string>,
): [number, SourceId, GraphBuilder][] {
  const segments = closurePathSegments(path);
  const canLowerAsParentSemijoin = routeFields.size === 0 && path.kind === "ImplicitRootReference";
  let currentGraph = rootGraph.projectFields([
    ...projectSourceFieldsWithRoutes(rootSource, routeFields),
    ProjectField.renamed(rootSource.rowShape.rowUuidField, "__closure_root_row_uuid"),
  ]);
  let currentSource = rootSource;
  // Every later join must key on the routes both sides carry.
  const carriedRoutes = stringSet(routeFields);
  const outputs: [number, SourceId, GraphBuilder][] = [];
  for (let index = 0; index < segments.length; index++) {
    const segment = segments[index];
    const target = lookupResolvedSource(resolvedSources, segment.target, "closure target");
    const sourceKey = userColumnField(segment.sourceField);
    const sourceKeyType = sourceFieldType(currentSource, sourceKey);
    if (sourceKeyType === undefined) {
      throw CapabilityError.operator(`closure source field ${dbg(sourceKey)} is not projected`);
    }
    let joined: GraphBuilder;
    if (canLowerAsParentSemijoin) {
      const [base, keyType] = unwrapNullableLayers(currentGraph, sourceKey, sourceKeyType);
      const keys =
        keyType.kind === "Array" ? base.unnest(sourceKey, CLOSURE_REQUIRED_ELEMENT) : base;
      const key = sourceKeyForRequired(keyType, sourceKey);
      joined = GraphBuilder.semiJoin(
        target.graph,
        keys.projectFields([ProjectField.named(key)]),
        [target.rowShape.rowUuidField],
        [key],
      ).projectFields(projectSourceFieldsWithRoutes(target, target.routingFields));
    } else {
      const sharedRoutes = [...carriedRoutes].filter((f) => target.routingFields.has(f));
      const targetOnlyRoutes = [...target.routingFields].filter((f) => !carriedRoutes.has(f));
      joined = GraphBuilder.join(
        currentGraph.unwrapNullable(sourceKey),
        target.graph,
        [sourceKey, ...sharedRoutes],
        [target.rowShape.rowUuidField, ...sharedRoutes],
      ).projectFields([
        ...projectSourceFieldsFromPrefix(target, RIGHT_JOIN_PREFIX),
        ProjectField.renamed("left.__closure_root_row_uuid", "__closure_root_row_uuid"),
        ...[...carriedRoutes].map((f) => ProjectField.renamed(leftField(f), f)),
        ...targetOnlyRoutes.map((f) => ProjectField.renamed(rightField(f), f)),
      ]);
    }
    for (const f of target.routingFields) carriedRoutes.add(f);
    outputs.push([index, segment.target, joined]);
    currentGraph = joined;
    currentSource = target;
  }
  return outputs;
}

export function closurePathSegments(path: ClosurePath): ClosurePathSegment[] {
  return path.kind === "ImplicitRootReference" ? [path.segment] : path.segments;
}
