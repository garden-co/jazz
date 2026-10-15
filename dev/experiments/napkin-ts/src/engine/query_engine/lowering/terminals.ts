// Construction of typed public and maintained-query terminals: result
// membership, structured application rows, relation edges, version witnesses,
// aggregates, routing fields, and the schemas used to decode those outputs.
import { SortedMap, type SortedSet } from "../../../support/collections.js";
import {
  DescriptorField,
  FieldIdentity,
  GraphBuilder,
  GroovePredicateExpr,
  LiteralValue,
  PredicateKind,
  ProjectExpr,
  ProjectField,
  FieldRef,
  RecordDescriptor,
  TopByLimit,
  TopByOrder,
  Value,
  ValueComparison,
  type ColumnType,
  type RelationProjectColumn,
  type ValueType,
} from "../../../_extern/engine.js";
import { coercePreparedBindingValue } from "../binding_values.js";
import {
  aggregateOutputField,
  claimPathFromParamField,
  descriptorPublicName,
  LEFT_JOIN_PREFIX,
  leftField,
  RIGHT_JOIN_PREFIX,
  rightField,
  routeParamFromField,
  tableUserColumnField,
  userColumnField,
} from "../fields.js";
import {
  dbg,
  programSourceId,
  structEq,
  structMap,
  type AggregateExpr,
  type NormalizedValueRef,
  type OrderKey,
  type ProgramPathId,
  type RowProjection,
  type SourceId,
} from "../input.js";
import type { AppProjectionTree, ProgramFactKey } from "../output.js";
import { currentRowMetadataVisibility, type CurrentRowPublicationField } from "../publication.js";
import {
  CapabilityError,
  lookupResolvedSource,
  orReport,
  runtimeGap,
  UnsupportedError,
  type ResolvedSource,
  type SourceRowShape,
} from "../resolver.js";
import type {
  AggregateResultSchema,
  AppRowCarrier,
  AppRowSchema,
  AppRowTerminal,
  OutputTerminalSchema,
  PathCorrelationCoverageSchema,
  ProgramFactOutput,
  ProgramFactSchema,
  ProgramFactTerminal,
  RelationEdgeSchema,
  TypedOutputField,
  VersionedRowRefSchema,
  VersionWitnessSchema,
  VersionWitnessSchemas,
} from "../schemas.js";
import {
  collectBindingSourceParams,
  graphDeclaredOutputFields,
  unionMember,
  type LoweredTerminal,
  type ParameterDomain,
} from "../lowering.js";
import type { LoweringContext } from "./arguments.js";
import {
  flatJoinContributionMembershipGraph,
  inheritedContributionMembershipGraph,
  joinContributionMembershipGraph,
  lowerClosureMembership,
  projectJoinContributionFieldsWithRootRoutes,
  reachableContributionMembershipGraph,
  reachableSeedMembershipGraph,
  reachableStepWitnessMembershipGraph,
} from "./closure.js";
import {
  collectAnchorGraph,
  collectByFieldFor,
  collectLayout,
  collectOutputDescriptor,
  collectSlotBuilder,
  collectSlotInputType,
  collectUnwrappedOutputType,
  lowerCollectSlotGraphs,
  rootCollectContextGraph,
  type CollectFlatField,
  type CollectLayout,
  type CollectSlotLayout,
} from "./collect_layout.js";
import {
  claimValue,
  hasExplicitClosurePath,
  lowerCorrelatedPathRelationGraph,
  lowerCorrelatedPathRelationGraphFromParent,
  lowerPathKeyPair,
  lowerPlanSteps,
  policyJoinIfNeeded,
  requireSourceField,
  sourceFieldType,
  strMap,
  strSet,
  type Sources,
} from "./graph_lowering.js";
import {
  firstStepSource,
  linearRootSource,
  planRootSource,
  relationRootSource,
  rootAggregateStep,
  type AnalyzedQueryPlan,
  type CorrelatedPathPlan,
  type LinearStep,
} from "./planning.js";

/**
 * Share execution only after proving the complete typed source/routing schema
 * and graph equal, except for the known terminal role literal.
 */
export function sharedWitnessSinks(terminals: LoweredTerminal[]): SortedMap<string, string> {
  const shared = strMap<string>();
  for (const replacement of terminals) {
    const rOut = replacement.output;
    if (
      rOut.kind !== "Fact" ||
      rOut.fact.key.kind !== "ReplacementWitnesses" ||
      rOut.fact.schema.kind !== "ReplacementWitnesses"
    ) {
      continue;
    }
    const replacementSchema = rOut.fact.schema.schema;
    let layer: WitnessLayer;
    if (rOut.fact.terminal === "ReplacementWitnessContent") layer = "Content";
    else if (rOut.fact.terminal === "ReplacementWitnessDeletion") layer = "Deletion";
    else continue;
    const versionRole = witnessTerminal("Version", layer);
    const versionTag = witnessTag("Version", layer);
    const replacementTag = witnessTag("Replacement", layer);
    for (const version of terminals) {
      if (shared.valuesArray().includes(version.sink)) continue;
      const vOut = version.output;
      if (
        vOut.kind !== "Fact" ||
        vOut.fact.key.kind !== "VersionWitnesses" ||
        vOut.fact.schema.kind !== "VersionWitnesses"
      ) {
        continue;
      }
      const versionSchema = vOut.fact.schema.schema;
      if (vOut.fact.terminal !== versionRole || !structEq(versionSchema, replacementSchema))
        continue;
      const v = version.graph;
      const r = replacement.graph;
      if (v.kind !== "Project" || r.kind !== "Project") continue;
      if (!structEq(v.input, r.input) || v.fields.length !== r.fields.length) continue;
      let roleFields = 0;
      const equal = v.fields.every((left: any, i: number) => {
        const right = r.fields[i];
        if (left.outputName === versionSchema.roleField) {
          roleFields++;
          return (
            left.outputName === right.outputName &&
            structEq(left.outputIdentity, right.outputIdentity) &&
            structEq(
              left.expression,
              ProjectField.literal("event_kind", Value.String(versionTag)).expression,
            ) &&
            structEq(
              right.expression,
              ProjectField.literal("event_kind", Value.String(replacementTag)).expression,
            )
          );
        }
        return structEq(left, right);
      });
      if (equal && roleFields === 1) {
        shared.set(replacement.sink, version.sink);
        break;
      }
    }
  }
  return shared;
}

function resolvedSourcePublicName(source: ResolvedSource, field: string): string | undefined {
  const found = source.rowShape.descriptor.fields().find((c: any) => c.name === field);
  return found ? descriptorPublicName(found) : undefined;
}

function sourcePublicationField(source: ResolvedSource, name: string): CurrentRowPublicationField {
  const id = source.storedColumnIds.get(name);
  return id !== undefined
    ? { kind: "StoredColumn", id, outputName: name }
    : { kind: "ResultField", name, visibility: "ApplicationCell" };
}

function collectPublicationFields(
  source: ResolvedSource,
  layout: CollectLayout,
): SortedMap<string, CurrentRowPublicationField> {
  const fields = strMap<CurrentRowPublicationField>();
  for (const field of layout.rootFields) {
    if (!field.isOutput) continue;
    let binding: CurrentRowPublicationField;
    if (field.origin === "SourceRow" && field.sourcePublicName !== undefined && !field.isRowId) {
      binding = sourcePublicationField(source, field.sourcePublicName);
    } else if (field.origin === "SourceRow") {
      binding = {
        kind: "ResultField",
        name: field.output,
        visibility: currentRowMetadataVisibility(field.sourceField ?? field.output),
      };
    } else {
      binding = {
        kind: "ResultField",
        name: field.output,
        visibility: field.isRowId ? "HiddenMetadata" : "ApplicationCell",
      };
    }
    fields.set(field.output, binding);
  }
  for (const slot of layout.slots) {
    fields.set(slot.collectionField, {
      kind: "ResultField",
      name: slot.collectionField,
      visibility: "ApplicationCell",
    });
  }
  return fields;
}

export function validateAppRowPublicationSchema(rows: AppRowSchema): void {
  for (const field of rows.descriptor.fields()) {
    const name: string | undefined = field.name;
    if (name === undefined)
      throw CapabilityError.runtime("application row descriptor has an unnamed field");
    if (name !== "row_uuid" && !rows.hiddenFields.has(name) && !rows.publicationFields.has(name)) {
      throw CapabilityError.runtime(
        `application row field ${dbg(name)} has no publication binding`,
      );
    }
  }
}

const intersect = (a: SortedSet<string>, b: { has(x: string): boolean }) =>
  strSet([...a].filter((x) => b.has(x)));
const union = (a: Iterable<string>, b: Iterable<string>) => strSet([...a, ...b]);

export function loweredTerminals(
  graph: GraphBuilder,
  request: LoweringContext,
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  parameterDomain: ParameterDomain,
  routingParamFields: SortedSet<string>,
  availableFieldsIn: SortedSet<string>,
): LoweredTerminal[] {
  if (rootAggregateStep(plan) !== undefined) {
    return loweredAggregateTerminals(
      graph,
      request,
      plan,
      source,
      resolvedSources,
      parameterDomain,
      routingParamFields,
      availableFieldsIn,
    );
  }
  // Root-union occurrence carriers are introduced by graph lowering, not a
  // source descriptor, but are real fields on every branch and must survive
  // both the closure and collector boundaries.
  const availableFields = strSet(availableFieldsIn);
  if (plan.kind === "Union") {
    for (const [name] of rootJoinOccurrenceFields(plan, resolvedSources, request))
      availableFields.add(name);
  }
  const initialRootRouteFields = intersect(routingParamFields, availableFields);
  // Closure evidence is an authorization boundary only for policy-claim
  // routes; ordinary query parameters can be absent from provenance-only
  // closure graphs.
  const rootOccurrenceFields = strSet(
    rootJoinOccurrenceFields(plan, resolvedSources, request)
      .map(([name]) => name)
      .filter((name) => availableFields.has(name)),
  );
  const initialClosureRootCarrierFields = union(initialRootRouteFields, rootOccurrenceFields);
  const terminals: LoweredTerminal[] = [];
  const initialClosure = lowerClosureMembership(
    graph,
    request,
    plan,
    source,
    resolvedSources,
    initialRootRouteFields,
    initialClosureRootCarrierFields,
  );
  // The source graph's conservative field declaration can omit a route
  // carrier introduced by correlated/policy lowering. Discover it from the
  // post-policy residual root, then rebuild the closure with that carrier so
  // CoveredInput terminals are never accidentally unscoped.
  const initialVisibleRootFields = graphDeclaredOutputFields(initialClosure.visibleRoot);
  const rootRouteFields = initialVisibleRootFields
    ? intersect(routingParamFields, initialVisibleRootFields)
    : strSet(initialRootRouteFields);
  const closureRootCarrierFields = union(rootRouteFields, rootOccurrenceFields);
  let closure = initialClosure;
  let visibleRootFields = initialVisibleRootFields;
  if (!structEq(rootRouteFields, initialRootRouteFields)) {
    closure = lowerClosureMembership(
      graph,
      request,
      plan,
      source,
      resolvedSources,
      rootRouteFields,
      closureRootCarrierFields,
    );
    visibleRootFields = graphDeclaredOutputFields(closure.visibleRoot);
  }
  // Non-root covered members depend on the root's routes and on their own
  // policy routes. Deliver each by every route it carries.
  const memberRouteScope = union(routingParamFields, rootRouteFields);
  const claimRouteFields = strSet(
    orReport(() => request.parameterDomain())
      .claimParams.keysArray()
      .filter((f) => rootRouteFields.has(f)),
  );
  // Covered inputs are produced only by the complete authority closure for
  // the exact source occurrence. The root's post-closure graph is the
  // authority's admission boundary and can carry a user route introduced by
  // the query residual; keep that graph-local set on root-only terminals.
  const rootSourceRouteFields = visibleRootFields
    ? intersect(rootRouteFields, visibleRootFields)
    : sourceTerminalRouteFields(source, rootRouteFields);
  let visibleRootWithRoutes: GraphBuilder;
  if (flatJoinPayloadFields(plan).length > 0) {
    visibleRootWithRoutes = sourceRowsForVisibleGraph(
      source,
      closure.visibleRoot,
      rootSourceRouteFields,
    );
  } else if (rootSourceRouteFields.size === 0) {
    visibleRootWithRoutes = closure.visibleRoot;
  } else {
    visibleRootWithRoutes = closure.visibleRoot.projectFields(
      projectSourceFieldsWithRoutes(source, rootSourceRouteFields),
    );
  }
  // Source facts stay partitioned by every authority route, including policy
  // claims; dropping one would make independent bindings share one unscoped
  // source closure.
  const rootSourceId = planRootSource(plan);
  const coveredSourceMembers = structMap<SourceId, GraphBuilder>();
  for (const [sourceId, memberGraph] of closure.resultMembers) {
    const resolved = lookupResolvedSource(resolvedSources, sourceId, "closure");
    coveredSourceMembers.set(
      sourceId,
      memberGraph.projectFields(
        projectSourceFieldsWithRoutes(
          resolved,
          terminalRouteFieldsForSource(
            sourceId,
            resolved,
            rootSourceId,
            memberRouteScope,
            rootSourceRouteFields,
          ),
        ),
      ),
    );
  }
  unionMember(coveredSourceMembers, rootSourceId, visibleRootWithRoutes);
  // Every join-side row contributing to a public root is receiver input.
  // Derive each contributor from the post-policy visible root, not its raw
  // table scan, so the authority publishes the exact residual relation.
  const shape = request.input.shape;
  for (const contribution of shape.joinContributions) {
    const resolved = lookupResolvedSource(
      resolvedSources,
      contribution.source,
      "join contribution",
    );
    const contributionRouteFields = sourceTerminalRouteFields(resolved, memberRouteScope);
    let contributionGraph: GraphBuilder;
    if (contribution.id.startsWith("flat_join:")) {
      contributionGraph = flatJoinContributionMembershipGraph(
        closure.visibleRoot,
        contribution,
        resolved,
        shape.nodes,
        resolvedSources,
        request,
        contributionRouteFields,
      );
    } else {
      let visibleParent: GraphBuilder;
      let parentSource: ResolvedSource;
      if (contribution.parent !== undefined) {
        const parentGraph = coveredSourceMembers.get(contribution.parent);
        if (parentGraph === undefined) {
          throw CapabilityError.runtime("nested join contribution requires its admitted parent");
        }
        const resolvedParent = resolvedSources.get(contribution.parent);
        if (resolvedParent === undefined)
          throw CapabilityError.runtime("nested join parent source was not resolved");
        [visibleParent, parentSource] = [parentGraph, resolvedParent];
      } else {
        [visibleParent, parentSource] = [closure.visibleRoot, source];
      }
      contributionGraph = joinContributionMembershipGraph(
        visibleParent,
        contribution,
        parentSource,
        resolved,
        shape.nodes,
        resolvedSources,
        request,
        contributionRouteFields,
      );
    }
    unionMember(coveredSourceMembers, contribution.source, contributionGraph);
  }
  // A caller-requested `inherits` is a receiver-local semi-join; its parent
  // crosses the authority boundary only after the parent read policy.
  for (const contribution of shape.inheritedContributions) {
    const parentSource = lookupResolvedSource(
      resolvedSources,
      contribution.source,
      "inherited contribution",
    );
    unionMember(
      coveredSourceMembers,
      contribution.source,
      inheritedContributionMembershipGraph(
        visibleRootWithRoutes,
        contribution,
        source,
        parentSource,
        shape.nodes,
        resolvedSources,
        request,
        sourceTerminalRouteFields(parentSource, memberRouteScope),
      ),
    );
  }
  // Recursive reachability rows are exact source inputs; they keep policy
  // routes until multisink partitioning chooses the recipient.
  for (const contribution of shape.reachableContributions) {
    const accessSource = lookupResolvedSource(
      resolvedSources,
      contribution.accessSource,
      "reachable contribution",
    );
    unionMember(
      coveredSourceMembers,
      contribution.accessSource,
      reachableContributionMembershipGraph(
        visibleRootWithRoutes,
        contribution,
        source,
        accessSource,
        shape.nodes,
        resolvedSources,
        request,
        sourceTerminalRouteFields(accessSource, memberRouteScope),
      ),
    );
    unionMember(
      coveredSourceMembers,
      contribution.edgeSource,
      reachableStepWitnessMembershipGraph(
        contribution,
        shape.nodes,
        resolvedSources,
        request,
        rootRouteFields,
      ),
    );
    const seed = reachableSeedMembershipGraph(
      contribution,
      shape.nodes,
      resolvedSources,
      request,
      rootRouteFields,
    );
    if (seed) unionMember(coveredSourceMembers, seed[0], seed[1]);
  }
  // A correlated collector owns a distinct source occurrence per child path.
  // Derive every child from the admitted (routed) root frontier and publish it
  // under that occurrence's own descriptor.
  if (plan.kind === "CorrelatedPath") {
    collectCorrelatedCoveredSourceMembers(
      plan.plan,
      visibleRootWithRoutes,
      source,
      resolvedSources,
      request,
      rootRouteFields,
      coveredSourceMembers,
    );
  }
  const appRows = request.output.appRows;
  if (appRows !== undefined) {
    const projectedOutput = projectedMultisourceTerminal(plan, source);
    const collectAppRows = (projection: AppProjectionTree) =>
      lowerCollectByAppRows(
        closure.visibleRoot,
        projection,
        plan,
        source,
        resolvedSources,
        request,
        rootRouteFields,
        availableFields,
      );
    let lowered: LoweredAppRows;
    const projection = appRows.projection;
    if (!appRows.publicTerminal) {
      // Authorization consumes existence, not candidate payloads.
      const rowId = source.rowShape.rowUuidField;
      const rowFields = source.rowShape.descriptor.fields().filter((f: any) => f.name === rowId);
      const publicationFields = strMap<CurrentRowPublicationField>();
      for (const field of rowFields) {
        const carrier: string = field.name;
        const name = descriptorPublicName(field);
        publicationFields.set(
          carrier,
          name !== undefined
            ? sourcePublicationField(source, name)
            : {
                kind: "ResultField",
                name: carrier,
                visibility: currentRowMetadataVisibility(carrier),
              },
        );
      }
      lowered = {
        graph: visibleRootWithRoutes.project([rowId, ...rootRouteFields]),
        descriptor: RecordDescriptor.newWithFields(rowFields),
        hiddenFields: strSet(),
        carrier: "CurrentRow",
        fieldCarriers: strMap(),
        publicFieldNames: strMap(),
        publicationFields,
        terminal: { kind: "Direct" },
      };
    } else if (projection.kind === "Relation") {
      lowered = relationAppRows(graph, projection.columns, plan, source, rootRouteFields);
    } else if (projection.kind === "Tree") {
      lowered = collectAppRows(projection.tree);
    } else if (projectedOutput !== undefined) {
      const [outputSourceId, outputFields] = projectedOutput;
      const outputSource = lookupResolvedSource(
        resolvedSources,
        outputSourceId,
        "projected output",
      );
      const hiddenFields = strSet([
        ...hiddenSourceFields(outputSource.rowShape),
        ...outputFields.filter((f) => f.name.startsWith("__flat_join_row_")).map((f) => f.name),
      ]);
      const publicFields = outputFields.filter((f) => !hiddenFields.has(f.name));
      const descriptor = new RecordDescriptor(publicFields.map((f) => [f.name, f.ty]));
      // Keep the session/parameter routes that partition this terminal.
      lowered = {
        graph: graph.projectFields([
          ...publicFields.map((f) => ProjectField.named(f.name)),
          ...[...rootRouteFields].map((f) => ProjectField.named(f)),
        ]),
        descriptor,
        hiddenFields: strSet(),
        carrier: "Logical",
        fieldCarriers: strMap(),
        publicFieldNames: strMap(
          publicFields.map((f) => {
            const index = descriptor.fieldIndex(f.name);
            const field = index !== undefined ? descriptor.fields()[index] : undefined;
            return [f.name, (field && descriptorPublicName(field)) ?? f.name];
          }),
        ),
        publicationFields: strMap(
          publicFields.map((f) => [
            f.name,
            sourcePublicationField(
              outputSource,
              resolvedSourcePublicName(outputSource, f.name) ?? f.name,
            ),
          ]),
        ),
        terminal: { kind: "Direct" },
      };
    } else {
      lowered = collectAppRows({ fields: { kind: "All" }, paths: [] });
    }
    // Prepared multisinks bind route parameters at every terminal; keep the
    // compiler-owned route carriers hidden in the app-row schema.
    for (const f of rootRouteFields) lowered.hiddenFields.add(f);
    terminals.push({
      sink: "app_rows",
      graph: lowered.graph,
      output: {
        kind: "AppRows",
        schema: {
          descriptor: lowered.descriptor,
          hiddenFields: lowered.hiddenFields,
          carrier: lowered.carrier,
          fieldCarriers: lowered.fieldCarriers,
          publicFieldNames: lowered.publicFieldNames,
          publicationFields: lowered.publicationFields,
          terminal: lowered.terminal,
          rootUnionArm: plan.kind === "Union",
        },
      },
    });
  }

  for (const fact of request.output.facts) {
    if (fact.kind === "ResultMembership") {
      const output = factOutput(
        fact,
        plan,
        rootSourceId,
        source,
        resolvedSources,
        request,
        strSet(routingParamFields),
      );
      // A flat join needs its complete policy-filtered tuple here, including
      // the internal contributor ids used by the occurrence address; the
      // closure projection would lose `__flat_join_row_N` through a lens.
      const occurrenceAddressed =
        output.schema.kind === "ResultMembership" &&
        output.schema.schema.occurrenceIdFields.length > 1;
      const resultMembershipInput =
        flatJoinPayloadFields(plan).length === 0 && !occurrenceAddressed
          ? visibleRootWithRoutes
          : graph;
      terminals.push({
        sink: factSinkName(fact),
        graph: factTerminalGraph(
          fact,
          resultMembershipInput,
          plan,
          source,
          resolvedSources,
          request,
          outputRoutingFields(output),
        ),
        output: { kind: "Fact", fact: output },
      });
      // A flat join's wide primary terminal is its public output; source-only
      // closure terminals would ask a source graph for `__flat_join_row_N`.
      if (flatJoinPayloadFields(plan).length === 0) {
        for (const [sourceId, closureGraph] of closure.resultMembers) {
          const resolved = lookupResolvedSource(resolvedSources, sourceId, "closure member");
          const memberOutput = factOutputWithTerminal(
            fact,
            "Primary",
            plan,
            sourceId,
            resolved,
            resolvedSources,
            request,
            strSet(claimRouteFields),
          );
          terminals.push({
            sink: scopedFactSinkName(fact, sourceId),
            graph: factTerminalGraph(
              fact,
              closureGraph,
              plan,
              resolved,
              resolvedSources,
              request,
              outputRoutingFields(memberOutput),
            ),
            output: { kind: "Fact", fact: memberOutput },
          });
        }
      }
      if (hasExplicitClosurePath(shape)) {
        for (const contribution of shape.joinContributions) {
          const resolved = lookupResolvedSource(
            resolvedSources,
            contribution.source,
            "join contribution",
          );
          const contributionOutput = factOutputWithTerminal(
            fact,
            "Primary",
            plan,
            contribution.source,
            resolved,
            resolvedSources,
            request,
            strSet(claimRouteFields),
          );
          const contributionGraph = coveredSourceMembers.get(contribution.source);
          if (contributionGraph === undefined) {
            throw CapabilityError.runtime("join contribution has no admitted source rows");
          }
          terminals.push({
            sink: scopedFactSinkName(fact, contribution.source),
            graph: factTerminalGraph(
              fact,
              contributionGraph,
              plan,
              resolved,
              resolvedSources,
              request,
              outputRoutingFields(contributionOutput),
            ),
            output: { kind: "Fact", fact: contributionOutput },
          });
        }
      }
    } else if (witnessFlavourOf(fact) !== undefined) {
      const flavour = witnessFlavourOf(fact)!;
      for (const [sourceId, resolved] of resolvedSources) {
        // An exact source with no visible contribution has an empty input
        // relation, not an authorization to reopen its storage relation.
        const covered = coveredSourceMembers.get(sourceId);
        if (covered === undefined) continue;
        pushWitnessTerminals(
          terminals,
          flavour,
          fact,
          plan,
          sourceId,
          resolved,
          covered,
          terminalRouteFieldsForSource(
            sourceId,
            resolved,
            rootSourceId,
            memberRouteScope,
            rootSourceRouteFields,
          ),
          resolvedSources,
          request,
          parameterDomain,
        );
      }
    } else if (fact.kind === "ProgramSourceCoverage") {
      // Coverage is control-plane evidence: seed exactly one complete receipt
      // per compiled source occurrence so an empty table is distinguishable
      // from a partial or missing authority closure.
      for (const sourceId of coveredSourceMembers.keysArray()) {
        const resolved = resolvedSources.get(sourceId);
        if (resolved === undefined)
          throw CapabilityError.runtime("covered source has no resolved descriptor");
        const sourceRouteFields = terminalRouteFieldsForSource(
          sourceId,
          resolved,
          rootSourceId,
          memberRouteScope,
          rootSourceRouteFields,
        );
        const output = factOutputWithTerminal(
          fact,
          "Primary",
          plan,
          sourceId,
          resolved,
          resolvedSources,
          request,
          strSet(sourceRouteFields),
        );
        if (output.schema.kind !== "ProgramSourceCoverage") {
          throw new Error("program-source coverage key has matching schema");
        }
        terminals.push({
          sink: scopedFactSinkName(fact, sourceId),
          graph: programSourceCoverageGraph(
            request,
            parameterDomain,
            output.schema.schema.complete,
            sourceRouteFields,
          ),
          output: { kind: "Fact", fact: output },
        });
      }
    } else {
      const terminalRouteFields =
        fact.kind === "AuthorizedRows" ? strSet(rootRouteFields) : strSet();
      const output = factOutput(
        fact,
        plan,
        rootSourceId,
        source,
        resolvedSources,
        request,
        strSet(terminalRouteFields),
      );
      const terminalGraph = factInputGraph(fact, graph, plan, source, resolvedSources, request);
      terminals.push({
        sink: factSinkName(fact),
        graph: factTerminalGraph(
          fact,
          terminalGraph,
          plan,
          source,
          resolvedSources,
          request,
          terminalRouteFields,
        ),
        output: { kind: "Fact", fact: output },
      });
    }
  }
  return terminals;
}

/** App rows for an explicit relation projection (`select` columns). */
function relationAppRows(
  graph: GraphBuilder,
  requested: RelationProjectColumn[],
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  rootRouteFields: SortedSet<string>,
): LoweredAppRows {
  const rootSourceId = planRootSource(plan);
  const rowIdRef: NormalizedValueRef = {
    kind: "RowId",
    row: { kind: "Source", source: rootSourceId },
  };
  const steps = rootLinearSteps(plan);
  const last = steps?.[steps.length - 1];
  let columns: RowProjection[];
  if (last?.kind === "Project") {
    columns = last.columns;
  } else {
    columns = [{ output: { name: "row_uuid", ty: { kind: "Uuid" } }, value: rowIdRef }];
    for (const column of requested) {
      let ty: ColumnType;
      let value: NormalizedValueRef;
      const expr = column.expr;
      if (expr.kind === "RowId") {
        if (expr.row !== "Current") {
          throw CapabilityError.operator(
            "outer/frontier row-id relation projections are not unified yet",
          );
        }
        [ty, value] = [{ kind: "Uuid" }, rowIdRef];
      } else if (expr.reference.column === "id") {
        [ty, value] = [{ kind: "Uuid" }, rowIdRef];
      } else {
        const found = source.tableSchema.columns.find((c: any) => c.name === expr.reference.column);
        if (!found) {
          throw CapabilityError.operator(
            `relation output column ${dbg(expr.reference.column)} is absent from the source schema`,
          );
        }
        [ty, value] = [
          found.columnType,
          { kind: "SourceField", source: rootSourceId, field: expr.reference.column },
        ];
      }
      columns.push({ output: { name: column.alias, ty }, value });
    }
  }
  const requestedNames = strSet(requested.map((c) => c.alias));
  return {
    graph: graph.projectFields([
      ...columns.map((c) => ProjectField.named(c.output.name)),
      ...[...rootRouteFields].map((f) => ProjectField.named(f)),
    ]),
    descriptor: new RecordDescriptor(columns.map((c) => [c.output.name, c.output.ty])),
    hiddenFields: strSet(
      columns
        .map((c) => c.output.name)
        .filter((name) => name !== "row_uuid" && !requestedNames.has(name)),
    ),
    carrier: "Logical",
    fieldCarriers: strMap(),
    publicFieldNames: strMap([...requestedNames].map((n) => [n, n])),
    publicationFields: strMap(
      requested.map((c): [string, CurrentRowPublicationField] => [
        c.alias,
        { kind: "ResultField", name: c.alias, visibility: "ApplicationCell" },
      ]),
    ),
    terminal: { kind: "Direct" },
  };
}

/**
 * Add each correlated collector child as an exact post-policy covered input:
 * only children reachable from an already-authorized parent, still carrying
 * the child's own source policy.
 */
function collectCorrelatedCoveredSourceMembers(
  path: CorrelatedPathPlan,
  parentGraph: GraphBuilder,
  parentSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
  members: SortedMap<SourceId, GraphBuilder>,
): void {
  const childId = path.path.child;
  const childSource = lookupResolvedSource(resolvedSources, childId, "correlated covered");
  const joined = orReport(() =>
    lowerCorrelatedPathRelationGraphFromParent(
      path,
      parentGraph,
      parentSource,
      resolvedSources,
      request,
      false,
    ),
  ).graph;
  // After the parent/child join the route carrier is owned by the left
  // parent; reuse the closure boundary's explicit layout.
  const childMembers = joined.projectFields(
    projectJoinContributionFieldsWithRootRoutes(childSource, routeFields),
  );
  unionMember(members, childId, childMembers);
  for (const sibling of path.siblings) {
    collectCorrelatedCoveredSourceMembers(
      sibling,
      parentGraph,
      parentSource,
      resolvedSources,
      request,
      routeFields,
      members,
    );
  }
  // Nested paths use this child as their left parent and retain the same
  // root-owned route carrier.
  for (const nested of path.nested) {
    collectCorrelatedCoveredSourceMembers(
      nested,
      childMembers,
      childSource,
      resolvedSources,
      request,
      routeFields,
      members,
    );
  }
}

/** Root fields published as application columns; the row id stays internal. */
const publicRootFields = (layout: CollectLayout) =>
  layout.rootFields.filter((f) => f.isOutput && !f.isRowId);

/** Public root columns keep their CurrentRow wrapper unless the projection declared a logical type. */
const rootFieldCarriers = (layout: CollectLayout) =>
  strMap<AppRowCarrier>(
    publicRootFields(layout).map((f) => [
      f.output,
      structEq(f.valueType, f.outputValueType) ? "CurrentRow" : "Logical",
    ]),
  );

/** Layout construction binds the public identity before collector fields are renamed. */
const publicRootFieldName = (field: CollectFlatField) => field.sourcePublicName ?? field.output;

const rootPublicFieldNames = (layout: CollectLayout) =>
  strMap<string>(publicRootFields(layout).map((f) => [f.output, publicRootFieldName(f)]));

type LoweredAppRows = {
  publicationFields: SortedMap<string, CurrentRowPublicationField>;
  graph: GraphBuilder;
  descriptor: RecordDescriptor;
  hiddenFields: SortedSet<string>;
  carrier: AppRowCarrier;
  fieldCarriers: SortedMap<string, AppRowCarrier>;
  publicFieldNames: SortedMap<string, string>;
  terminal: AppRowTerminal;
};

function lowerCollectByAppRows(
  visibleRoot: GraphBuilder,
  projection: AppProjectionTree,
  plan: AnalyzedQueryPlan,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  routeFields: SortedSet<string>,
  availableFields: SortedSet<string>,
): LoweredAppRows {
  const base = orReport(() => request.parameterDomain());
  const parameterDomain: ParameterDomain = {
    ...base,
    userParams: base.userParams.clone(),
    claimParams: base.claimParams.clone(),
    routingParams: strSet(base.routingParams),
  };
  collectBindingSourceParams(visibleRoot, parameterDomain);
  const layout = collectLayout(
    projection,
    plan,
    rootSource,
    resolvedSources,
    request,
    routeFields,
    parameterDomain,
    availableFields,
  );
  alignCollectRootWindow(layout, plan, rootSource);
  alignCollectJoinKeyTypes(layout.slots, plan, resolvedSources, request);
  const publicationFields = collectPublicationFields(rootSource, layout);
  const stable = plan.kind !== "Union";
  const rootRowId = layout.rootFields.find((f) => f.isRowId);
  if (layout.slots.length === 0) {
    // A collect-all root preserves the source CurrentRow application-cell
    // wrappers; explicit projections unwrap them to their declared logical
    // types. Bind that distinction here, never from descriptor field names.
    const carrier: AppRowCarrier = publicRootFields(layout).every((f) =>
      structEq(f.valueType, f.outputValueType),
    )
      ? "CurrentRow"
      : "Logical";
    let anchor = collectAnchorGraph(visibleRoot, rootSource, layout, stable);
    const hasWindow = (rootLinearSteps(plan) ?? []).some(
      (s) => s.kind === "OrderBy" || s.kind === "Slice",
    );
    if (!hasWindow) {
      anchor = GraphBuilder.topBy(
        anchor,
        [],
        layout.rootOrderCols,
        layout.rootTieCols,
        0n,
        TopByLimit.Unbounded,
      );
    }
    if (!rootRowId) throw new Error("collector root retains row id");
    return {
      publicationFields,
      graph: GraphBuilder.collectRootOrdered(
        anchor,
        [...rootCollectGroupFields(rootRowId.input, layout.rootOccurrenceInputs), ...routeFields],
        layout.rootFields.filter((f) => f.isOutput).map(collectByFieldFor),
        layout.rootOrderCols,
        layout.rootTieCols,
        layout.rootOffset,
        layout.rootLimit,
      ),
      descriptor: collectOutputDescriptor(layout),
      // Route parameters partition results but are not part of the app projection.
      hiddenFields: strSet(routeFields),
      carrier,
      fieldCarriers: rootFieldCarriers(layout),
      publicFieldNames: rootPublicFieldNames(layout),
      terminal: { kind: "RootCollector" },
    };
  }
  const rootContext = rootCollectContextGraph(visibleRoot, rootSource, layout, stable);
  const fieldCarriers = rootFieldCarriers(layout);
  const publicFieldNames = rootPublicFieldNames(layout);
  for (const slot of layout.slots) {
    fieldCarriers.set(slot.collectionField, "Logical");
    // Collection field names are public query-path identities, carried verbatim.
    publicFieldNames.set(slot.collectionField, slot.collectionField);
  }
  const associationGraphs: GraphBuilder[] = [];
  for (const slot of layout.slots) {
    const path = findCorrelatedPath(plan, slot.path);
    if (!path) {
      throw CapabilityError.operator(
        `app projection path ${dbg(slot.path)} is not a correlated path in the query shape`,
      );
    }
    associationGraphs.push(
      ...lowerCollectSlotGraphs(
        slot,
        path,
        rootContext,
        rootSource,
        rootSource,
        layout,
        new Set(),
        resolvedSources,
        request,
      ),
    );
  }
  const anchor = collectAnchorGraph(visibleRoot, rootSource, layout, stable);
  const input = GraphBuilder.union([anchor, ...associationGraphs]);
  const descriptor = collectOutputDescriptor(layout);
  if (!rootRowId)
    throw CapabilityError.runtime("collector root projection did not retain the row id");
  const rootGroup = rootRowId.input;
  const graph = GraphBuilder.collectByTreeOrdered(
    input,
    [...rootCollectGroupFields(rootGroup, layout.rootOccurrenceInputs), ...routeFields],
    layout.rootFields.filter((f) => f.isOutput).map(collectByFieldFor),
    layout.slots.map((slot) => collectSlotBuilder(slot, rootGroup, routeFields)),
    layout.rootOrderCols,
    layout.rootTieCols,
    layout.rootOffset,
    layout.rootLimit,
  );
  const hiddenFields = hiddenSourceFields(rootSource.rowShape);
  for (const f of routeFields) hiddenFields.add(f);
  return {
    publicationFields,
    graph,
    descriptor,
    hiddenFields,
    carrier: "Logical",
    fieldCarriers,
    publicFieldNames,
    terminal: { kind: "RootCollector" },
  };
}

/**
 * Collectors require their physical root UUID as the first grouping field; for
 * root UNION ALL the following discriminator is source position zero.
 */
const rootCollectGroupFields = (root: string, occurrence: string[]) => [root, ...occurrence];

const topByOrder = (key: OrderKey, field: string) =>
  key.direction === "Asc" ? TopByOrder.asc(field) : TopByOrder.desc(field);
const sliceLimit = (limit: number | undefined) =>
  limit !== undefined ? TopByLimit.Finite(BigInt(limit)) : TopByLimit.Unbounded;

function alignCollectJoinKeyTypes(
  slots: CollectSlotLayout[],
  plan: AnalyzedQueryPlan,
  resolvedSources: Sources,
  request: LoweringContext,
): void {
  for (const slot of slots) {
    const path = findCorrelatedPath(plan, slot.path);
    if (!path) {
      throw CapabilityError.operator(
        `app projection path ${dbg(slot.path)} is not a correlated path in the query shape`,
      );
    }
    const parentId = linearRootSource(path.parent.root);
    if (!parentId) throw CapabilityError.operator("collector path parent must be a source");
    const childId = linearRootSource(path.child.root);
    if (!childId) throw CapabilityError.operator("collector path child must be a source");
    const parent = lookupResolvedSource(resolvedSources, parentId, "collector parent");
    const child = lookupResolvedSource(resolvedSources, childId, "collector child");
    const [parentKey, childKey] = orReport(() =>
      lowerPathKeyPair(path.correlation, parentId, parent, childId, child, request),
    );
    const keyField = slot.fields.find((f) => f.sourceField === childKey);
    if (keyField)
      keyField.valueType = { kind: "Nullable", inner: keyField.valueType.nonNullable() };
    const parentType = sourceFieldType(parent, parentKey)?.nonNullable();
    if (parentType?.isArrayFk() && childKey === child.rowShape.rowUuidField) {
      const input = `${slot.rowIdInput}_references`;
      const valueType: ValueType = {
        kind: "Nullable",
        inner: { kind: "Array", inner: { kind: "Uuid" } },
      };
      slot.fields.push({
        input,
        output: input,
        valueType,
        outputValueType: valueType,
        sourceField: parentKey,
        origin: "Derived",
        isRowId: false,
        isPresence: false,
        isOutput: false,
      });
      slot.referenceArrayInput = input;
      slot.referenceOrder = !path.child.steps.some((s) => s.kind === "OrderBy");
    }
    for (const step of path.child.steps) {
      if (step.kind === "OrderBy") {
        for (const key of step.keys) retainCollectSlotValue(slot, key.value, child);
        slot.orderCols = step.keys.map((key) =>
          topByOrder(key, collectSlotInputForValue(slot, key.value, child)),
        );
      } else if (step.kind === "Slice") {
        for (const value of step.tieBreaker) retainCollectSlotValue(slot, value, child);
        slot.offset = step.offset;
        slot.limit = sliceLimit(step.limit);
        slot.tieCols = step.tieBreaker.map((v) => collectSlotInputForValue(slot, v, child));
      }
    }
    if (slot.orderCols.length === 0) slot.orderCols = [TopByOrder.asc(slot.rowIdInput)];
    if (slot.tieCols.length === 0) slot.tieCols = [slot.rowIdInput];
    alignCollectJoinKeyTypes(slot.children, plan, resolvedSources, request);
  }
}

function alignCollectRootWindow(
  layout: CollectLayout,
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
): void {
  let steps: LinearStep[] = [];
  if (plan.kind === "Linear") steps = plan.plan.steps;
  else if (plan.kind === "CorrelatedPath")
    steps = [...plan.plan.parent.steps, ...plan.plan.outputSteps];
  else if (plan.kind === "Union") steps = plan.plan.terminalSteps;
  for (const step of steps) {
    if (step.kind === "OrderBy") {
      for (const key of step.keys) retainCollectRootValue(layout, key.value, source);
      layout.rootOrderCols = step.keys.map((key) =>
        topByOrder(key, collectRootInputForValue(layout, key.value, source)),
      );
    } else if (step.kind === "Slice") {
      for (const value of step.tieBreaker) retainCollectRootValue(layout, value, source);
      layout.rootOffset = step.offset;
      layout.rootLimit = sliceLimit(step.limit);
      layout.rootTieCols = step.tieBreaker.map((v) => collectRootInputForValue(layout, v, source));
    }
  }
  const rowId = layout.rootFields.find((f) => f.isRowId);
  if (!rowId) throw new Error("collector root retains row id");
  if (layout.rootOrderCols.length === 0) layout.rootOrderCols = [TopByOrder.asc(rowId.input)];
  if (layout.rootTieCols.length === 0) layout.rootTieCols = [rowId.input];
  for (const occurrence of layout.rootOccurrenceInputs) {
    if (!layout.rootTieCols.includes(occurrence)) layout.rootTieCols.push(occurrence);
  }
}

/**
 * Keep values used to order or slice a collector in its internal input row.
 * They are not public payload fields: a path can order by provenance even when
 * its projection selects only ordinary columns.
 */
function retainCollectRootValue(
  layout: CollectLayout,
  value: NormalizedValueRef,
  source: ResolvedSource,
): void {
  const descriptorField = collectWindowSourceField(source, value);
  if (!descriptorField) return;
  const sourceField: string = descriptorField.name;
  if (layout.rootFields.some((f) => f.sourceField === sourceField)) return;
  layout.rootFields.push({
    input: `__collect_root_${sourceField}`,
    output: sourceField,
    valueType: descriptorField.valueType,
    outputValueType: descriptorField.valueType,
    sourcePublicName: resolvedSourcePublicName(source, sourceField),
    sourceField,
    origin: "SourceRow",
    isRowId: false,
    isPresence: false,
    isOutput: false,
  });
}

function retainCollectSlotValue(
  slot: CollectSlotLayout,
  value: NormalizedValueRef,
  source: ResolvedSource,
): void {
  const descriptorField = collectWindowSourceField(source, value);
  if (!descriptorField) return;
  const sourceField: string = descriptorField.name;
  if (slot.fields.some((f) => f.sourceField === sourceField)) return;
  const suffix = `_${source.rowShape.rowUuidField}`;
  if (!slot.rowIdInput.endsWith(suffix)) {
    throw CapabilityError.runtime(
      `collector child row-id input ${dbg(slot.rowIdInput)} does not match source row-id field ${dbg(source.rowShape.rowUuidField)}`,
    );
  }
  const prefix = slot.rowIdInput.slice(0, -suffix.length);
  // Derive types exactly as selected slot fields do, so the anchor and
  // association arms agree on the outer wrapper.
  const outputValueType = collectUnwrappedOutputType(
    source,
    sourceField,
    descriptorField.valueType,
  );
  slot.fields.push({
    input: `${prefix}_${sourceField}`,
    output: sourceField,
    valueType: collectSlotInputType(outputValueType),
    outputValueType,
    sourcePublicName: resolvedSourcePublicName(source, sourceField),
    sourceField,
    origin: "SourceRow",
    isRowId: false,
    isPresence: false,
    isOutput: false,
  });
}

function collectRootInputForValue(
  layout: CollectLayout,
  value: NormalizedValueRef,
  source: ResolvedSource,
): string {
  if (value.kind === "SourceField" && value.field === "__root_union_arm") {
    const found = layout.rootFields.find((c) => c.sourceField === value.field);
    if (!found) throw CapabilityError.operator("collector root union-arm tie-breaker is missing");
    return found.input;
  }
  const field: string | undefined = collectWindowSourceField(source, value)?.name;
  let input: string | undefined;
  if (field !== undefined) input = layout.rootFields.find((c) => c.sourceField === field)?.input;
  else if (value.kind === "RowId" && value.row.kind === "Source")
    input = layout.rootFields.find((f) => f.isRowId)?.input;
  if (input === undefined) {
    throw CapabilityError.operator(
      `collector root window key ${dbg(value)} is not present in the root projection`,
    );
  }
  return input;
}

function collectSlotInputForValue(
  slot: CollectSlotLayout,
  value: NormalizedValueRef,
  source: ResolvedSource,
): string {
  const field: string | undefined = collectWindowSourceField(source, value)?.name;
  let input: string | undefined;
  if (field !== undefined) input = slot.fields.find((c) => c.sourceField === field)?.input;
  else if (value.kind === "RowId" && value.row.kind === "Source") input = slot.rowIdInput;
  if (input === undefined) {
    throw CapabilityError.operator(
      `collector window key ${dbg(value)} is not present in the child projection`,
    );
  }
  return input;
}

/**
 * Resolve a window reference at the application-to-source boundary. Declared
 * columns use their canonical CurrentRow carrier; derived logical fields keep
 * their descriptor identity; provenance uses its metadata carrier.
 */
export function collectWindowSourceField(
  source: ResolvedSource,
  value: NormalizedValueRef,
): DescriptorField | undefined {
  const descriptor = source.rowShape.descriptor;
  let carrier: string;
  if (value.kind === "SourceField") {
    if (source.tableSchema.columns.some((c: any) => c.name === value.field))
      carrier = userColumnField(value.field);
    else {
      const index = descriptor.fieldIndex(value.field);
      return index === undefined ? undefined : descriptor.fields()[index];
    }
  } else if (value.kind === "Provenance") {
    carrier = `$${value.field[0].toLowerCase()}${value.field.slice(1)}`;
  } else return undefined;
  return descriptor.fields().find((f: any) => f.name === carrier);
}

export function rootJoinOccurrenceFields(
  plan: AnalyzedQueryPlan,
  resolvedSources: Sources,
  request: LoweringContext,
): [string, ValueType][] {
  // Authorization subplans are decision programs, not public row sets.
  if (
    request.policy.kind === "AuthorizationSubplan" ||
    request.output.appRows?.publicTerminal === false
  )
    return [];
  if (plan.kind === "Union") {
    return [
      ["__root_union_arm", { kind: "String" }],
      ["__root_union_row", { kind: "Uuid" }],
    ];
  }
  const steps = rootLinearSteps(plan);
  if (!steps) return [];
  const fields: [string, ValueType][] = [];
  steps.forEach((step, stepIndex) => {
    if (step.kind !== "Join" || step.mode !== "Inner") return;
    const next = steps[stepIndex + 1];
    if (next?.kind === "Project") return;
    const right = step.right;
    if (right.kind === "Union") {
      fields.push([`__root_join_arm_${stepIndex}`, { kind: "String" }]);
      fields.push([`__root_join_row_${stepIndex}`, { kind: "Uuid" }]);
      return;
    }
    const sourceId = relationRootSource(right);
    if (!sourceId) return;
    const source = lookupResolvedSource(resolvedSources, sourceId, "inner join occurrence");
    if (sourceId.path.components[sourceId.path.components.length - 1]?.kind !== "Alias") return;
    let exposesRowId = false;
    if (right.kind === "Linear") {
      const last = right.plan.steps[right.plan.steps.length - 1];
      exposesRowId = !(
        last?.kind === "Project" &&
        !last.columns.some((c) => c.output.name === source.rowShape.rowUuidField)
      );
    }
    if (!exposesRowId) return;
    const name =
      next?.kind === "Join"
        ? `__flat_join_source_${stepIndex}_${source.rowShape.rowUuidField}`
        : `__root_join_row_${stepIndex}`;
    fields.push([name, { kind: "Uuid" }]);
  });
  return fields;
}

function findCorrelatedPath(
  plan: AnalyzedQueryPlan,
  path: ProgramPathId,
): CorrelatedPathPlan | undefined {
  return plan.kind === "CorrelatedPath" ? findCorrelatedPathInTree(plan.plan, path) : undefined;
}

export function findNestedCorrelatedPath(
  parent: CorrelatedPathPlan,
  path: ProgramPathId,
): CorrelatedPathPlan | undefined {
  for (const candidate of parent.nested) {
    const found = findCorrelatedPathInTree(candidate, path);
    if (found) return found;
  }
  return undefined;
}

function findCorrelatedPathInTree(
  path: CorrelatedPathPlan,
  target: ProgramPathId,
): CorrelatedPathPlan | undefined {
  if (structEq(path.path, target)) return path;
  for (const candidate of [...path.siblings, ...path.nested]) {
    const found = findCorrelatedPathInTree(candidate, target);
    if (found) return found;
  }
  return undefined;
}

function loweredAggregateTerminals(
  graph: GraphBuilder,
  request: LoweringContext,
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  parameterDomain: ParameterDomain,
  routingParamFields: SortedSet<string>,
  availableFields: SortedSet<string>,
): LoweredTerminal[] {
  const terminals: LoweredTerminal[] = [];
  // A maintained authority is an input publisher, not an aggregate-result
  // renderer; its receiver derives the synthetic row after installing the
  // admitted source closure. An authority ResultPayload would be a second
  // truth path for aggregates.
  const authorityPublishesCoveredInputs =
    request.request.authorizationMode !== "ClientLocal" &&
    request.output.facts.has({ kind: "VersionWitnesses" });
  // Rebuild the graph immediately before the aggregate so the contributor
  // witness is selected by the same filters and policy as the result; the
  // raw source graph would leak invisible rows into a client-local aggregate.
  const aggregateInput = aggregateInputGraph(plan, source, resolvedSources, request);
  // Only the root occurrence contributes; joined relations are existence
  // gates and must not cross into a receiver as raw inputs.
  const aggregateContributors = structMap<SourceId, GraphBuilder>([
    [planRootSource(plan), aggregateInput],
  ]);
  const rootRouteFields = intersect(routingParamFields, availableFields);
  const aggregateDescriptor = aggregateAppRowDescriptor(plan, source);
  const aggregateGraph =
    rootRouteFields.size === 0
      ? graph
      : graph.projectFields([
          ...aggregateDescriptor
            .fields()
            .map((f: any) => ProjectField.namedWithIdentity(f.name, f.identity)),
          ...[...rootRouteFields].map((f) => ProjectField.named(f)),
        ]);
  if (request.output.appRows !== undefined && !authorityPublishesCoveredInputs) {
    const aggregateSchema = aggregateResultSchema(plan, source, strSet(rootRouteFields));
    const [groups, outputs] = rootAggregateStep(plan)!;
    const publicationFields = strMap<CurrentRowPublicationField>();
    for (const value of groups) {
      const carrier = aggregateSourceFieldName(value, source);
      let name: string;
      if (value.kind === "RowId") name = "id";
      else {
        const found = resolvedSourcePublicName(source, carrier);
        if (found === undefined) {
          throw CapabilityError.runtime("aggregate source field has no explicit application name");
        }
        name = found;
      }
      publicationFields.set(carrier, sourcePublicationField(source, name));
    }
    for (const output of outputs) {
      publicationFields.set(aggregateOutputField(output.output.name), {
        kind: "ResultField",
        name: output.output.name,
        visibility: "ApplicationCell",
      });
    }
    terminals.push({
      sink: "app_rows",
      graph: aggregateGraph,
      output: {
        kind: "AppRows",
        schema: {
          publicationFields,
          descriptor: aggregateAppRowDescriptor(plan, source),
          hiddenFields: strSet(rootRouteFields),
          carrier: "Logical",
          fieldCarriers: strMap(),
          publicFieldNames: strMap(),
          terminal: { kind: "Aggregate", schema: aggregateSchema },
          rootUnionArm: false,
        },
      },
    });
  }
  const noSources = structMap<SourceId, ResolvedSource>();
  for (const fact of request.output.facts) {
    if (fact.kind === "ResultMembership") {
      if (authorityPublishesCoveredInputs) continue;
      const output = factOutput(
        fact,
        plan,
        planRootSource(plan),
        source,
        noSources,
        request,
        strSet(rootRouteFields),
      );
      terminals.push({
        sink: factSinkName(fact),
        graph: factTerminalGraph(
          fact,
          aggregateGraph,
          plan,
          source,
          noSources,
          request,
          outputRoutingFields(output),
        ),
        output: { kind: "Fact", fact: output },
      });
    } else if (fact.kind === "ProgramSourceCoverage") {
      for (const sourceId of aggregateContributors.keysArray()) {
        const resolved = resolvedSources.get(sourceId);
        if (resolved === undefined)
          throw CapabilityError.runtime("aggregate contributor has no resolved descriptor");
        const output = factOutputWithTerminal(
          fact,
          "Primary",
          plan,
          sourceId,
          resolved,
          resolvedSources,
          request,
          strSet(rootRouteFields),
        );
        if (output.schema.kind !== "ProgramSourceCoverage") {
          throw new Error("program-source coverage key has matching schema");
        }
        terminals.push({
          sink: scopedFactSinkName(fact, sourceId),
          graph: programSourceCoverageGraph(
            request,
            parameterDomain,
            output.schema.schema.complete,
            rootRouteFields,
          ),
          output: { kind: "Fact", fact: output },
        });
      }
    } else {
      const flavour = witnessFlavourOf(fact);
      if (flavour === undefined) continue;
      // Aggregate membership is derived output, not a replayable input; ship
      // the exact versioned contributors so a covered receiver rebuilds the
      // same aggregate graph locally.
      for (const [sourceId, contributorGraph] of aggregateContributors) {
        const resolved = resolvedSources.get(sourceId);
        if (resolved === undefined)
          throw CapabilityError.runtime("aggregate contributor has no resolved descriptor");
        pushWitnessTerminals(
          terminals,
          flavour,
          fact,
          plan,
          sourceId,
          resolved,
          contributorGraph,
          rootRouteFields,
          resolvedSources,
          request,
          parameterDomain,
        );
      }
    }
  }
  return terminals;
}

/** Version and replacement witnesses differ only in roles, sink names and the `event_kind` tag. */
type WitnessFlavour = "Version" | "Replacement";
type WitnessLayer = "Content" | "Deletion";

function witnessFlavourOf(key: ProgramFactKey): WitnessFlavour | undefined {
  if (key.kind === "VersionWitnesses") return "Version";
  if (key.kind === "ReplacementWitnesses") return "Replacement";
  return undefined;
}

const witnessTerminal = (flavour: WitnessFlavour, layer: WitnessLayer): ProgramFactTerminal =>
  `${flavour}Witness${layer}` as ProgramFactTerminal;

/** The `event_kind` literal; `maintained.{tag}` is also the sink prefix. */
const witnessTag = (flavour: WitnessFlavour, layer: WitnessLayer): string =>
  `${flavour.toLowerCase()}_${layer.toLowerCase()}`;

/** Emit one source's content witness terminal and, with a deletion register, its deletion terminal. */
function pushWitnessTerminals(
  terminals: LoweredTerminal[],
  flavour: WitnessFlavour,
  fact: ProgramFactKey,
  plan: AnalyzedQueryPlan,
  sourceId: SourceId,
  resolvedSource: ResolvedSource,
  visibleGraph: GraphBuilder,
  routeFields: SortedSet<string>,
  resolvedSources: Sources,
  request: LoweringContext,
  parameterDomain: ParameterDomain,
): void {
  const output = (layer: WitnessLayer): OutputTerminalSchema => ({
    kind: "Fact",
    fact: factOutputWithTerminal(
      fact,
      witnessTerminal(flavour, layer),
      plan,
      sourceId,
      resolvedSource,
      resolvedSources,
      request,
      strSet(routeFields),
    ),
  });
  const contentOutput = output("Content");
  terminals.push({
    sink: scopedFactSinkName(fact, sourceId),
    graph: contentVersionWitnessGraphFromVisibleGraph(
      resolvedSource,
      visibleGraph,
      witnessTag(flavour, "Content"),
      routeFields,
    ),
    output: contentOutput,
  });
  // A content retraction alone may be policy revocation and must never be
  // mistaken for a tombstone; ship an explicit deletion layer independently.
  if (resolvedSource.deletionRegister === undefined) return;
  const deletionOutput = output("Deletion");
  terminals.push({
    sink: scopedDeletionFactSinkName(fact, sourceId),
    graph: deletionWitnessGraphForCurrentRegister(
      resolvedSource,
      witnessTag(flavour, "Deletion"),
      request,
      parameterDomain,
      routeFields,
    ),
    output: deletionOutput,
  });
}

/**
 * Lower the authority-visible relation immediately before the root aggregate,
 * so policy filtering stays authoritative and no terminal reopens a physical
 * source relation.
 */
function aggregateInputGraph(
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): GraphBuilder {
  if (plan.kind !== "Linear") {
    throw CapabilityError.runtime("aggregate contributor input requires a linear root plan");
  }
  const steps = [...plan.plan.steps];
  if (steps.pop()?.kind !== "Aggregate") {
    throw CapabilityError.runtime(
      "aggregate contributor input is missing its terminal aggregate step",
    );
  }
  return orReport(
    () =>
      lowerPlanSteps(
        source.graph,
        { kind: "Linear", plan: { ...plan.plan, steps } },
        source,
        resolvedSources,
        request,
      ).graph,
  );
}

function factInputGraph(
  key: ProgramFactKey,
  graph: GraphBuilder,
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): GraphBuilder {
  if (
    plan.kind === "CorrelatedPath" &&
    (key.kind === "RelationEdges" || key.kind === "PathCorrelationCoverage")
  ) {
    try {
      return lowerCorrelatedPathRelationGraph(plan.plan, source, resolvedSources, request).graph;
    } catch (err) {
      if (err instanceof UnsupportedError) {
        throw CapabilityError.explained(
          err.gap,
          "correlated path relation facts lower from the parent-child path graph",
        );
      }
      throw err;
    }
  }
  return graph;
}

export const projectSourceFieldsFromPrefix = (source: ResolvedSource, prefix: string): any[] =>
  projectSourceFieldsFromPrefixRewrappingNullable(source, prefix, undefined);

export function projectSourceFieldsFromPrefixRewrappingNullable(
  source: ResolvedSource,
  prefix: string,
  nullableField: string | undefined,
): any[] {
  const out: any[] = [];
  for (const field of source.rowShape.descriptor.fields()) {
    const name: string | undefined = field.name;
    if (name === undefined) continue;
    const sourceField = FieldRef.storedName(`${prefix}${name}`);
    // Restoring a source row after joins or policy filtering retains its
    // source identity rather than minting a new logical application name.
    out.push(
      new ProjectField({
        expression:
          nullableField === name
            ? ProjectExpr.Nullable(sourceField)
            : ProjectExpr.Field(sourceField),
        outputName: name,
        outputIdentity: field.identity ?? FieldIdentity.Name(name),
      }),
    );
  }
  return out;
}

export const projectSourceFieldsWithRoutes = (
  source: ResolvedSource,
  routeFields: SortedSet<string>,
): any[] => projectSourceFieldsWithRoutesFromPrefix(source, "", routeFields);

const sourceTerminalRouteFields = (source: ResolvedSource, rootRouteFields: SortedSet<string>) =>
  intersect(source.routingFields, rootRouteFields);

function terminalRouteFieldsForSource(
  sourceId: SourceId,
  source: ResolvedSource,
  rootSource: SourceId,
  rootRouteFields: SortedSet<string>,
  rootTerminalRouteFields: SortedSet<string>,
): SortedSet<string> {
  return structEq(sourceId, rootSource)
    ? strSet(rootTerminalRouteFields)
    : sourceTerminalRouteFields(source, rootRouteFields);
}

export function projectSourceFieldsWithRoutesFromPrefix(
  source: ResolvedSource,
  prefix: string,
  routeFields: SortedSet<string>,
): any[] {
  const fields = projectSourceFieldsFromPrefix(source, prefix);
  for (const field of routeFields) fields.push(ProjectField.renamed(`${prefix}${field}`, field));
  return fields;
}

const factOutput = (
  key: ProgramFactKey,
  plan: AnalyzedQueryPlan,
  sourceId: SourceId,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  routingParamFields: SortedSet<string>,
) =>
  factOutputWithTerminal(
    key,
    "Primary",
    plan,
    sourceId,
    source,
    resolvedSources,
    request,
    routingParamFields,
  );

function factOutputWithTerminal(
  key: ProgramFactKey,
  terminal: ProgramFactTerminal,
  plan: AnalyzedQueryPlan,
  sourceId: SourceId,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  routingParamFields: SortedSet<string>,
): ProgramFactOutput {
  let schema: ProgramFactSchema;
  switch (key.kind) {
    case "AuthorizedRows":
      schema = {
        kind: "AuthorizedRows",
        schema: { rowField: source.rowShape.rowUuidField, routingParamFields },
      };
      break;
    case "ResultMembership": {
      if (rootAggregateStep(plan) !== undefined) {
        return {
          key,
          terminal,
          schema: {
            kind: "AggregateResult",
            schema: aggregateResultSchema(plan, source, routingParamFields),
          },
        };
      }
      const version = versionWitnessFields(source.rowShape);
      const flatJoinPayload = flatJoinPayloadFields(plan);
      const payloadFields = resultPayloadFields(plan, source);
      const payloadPublicationFields = strMap<CurrentRowPublicationField>();
      for (const field of payloadFields) {
        const name = resolvedSourcePublicName(source, field.name);
        payloadPublicationFields.set(
          field.name,
          flatJoinPayload.length > 0
            ? { kind: "ResultField", name: field.name, visibility: "ApplicationCell" }
            : name !== undefined
              ? sourcePublicationField(source, name)
              : {
                  kind: "ResultField",
                  name: field.name,
                  visibility: currentRowMetadataVisibility(field.name),
                },
        );
      }
      schema = {
        kind: "ResultMembership",
        schema: {
          tableField: "table_name",
          rowField: source.rowShape.rowUuidField,
          occurrenceIdFields: resultOccurrenceIdFields(plan, source, resolvedSources, request),
          occurrenceUnionArmFields: resultOccurrenceUnionArmFields(
            plan,
            source,
            resolvedSources,
            request,
          ),
          payloadPublicationFields,
          payloadFields,
          // Only view-relative sources use the branch/prefix field as public
          // result identity; ordinary rows would otherwise churn memberships
          // across schema projections.
          branchOrPrefixField: source.requiresResultPayload
            ? version.branchOrPrefixField
            : undefined,
          version: {
            kind: "Content",
            content: { txTimeField: "content_tx_time", txNodeField: "content_tx_node_id" },
          },
          settlePositionField:
            flatJoinPayload.length === 0 ? settlePositionField(source.rowShape) : undefined,
          routingParamFields,
        },
      };
      break;
    }
    case "ProgramSourceCoverage":
      schema = {
        kind: "ProgramSourceCoverage",
        schema: { source: programSourceId(sourceId), complete: true, routingParamFields },
      };
      break;
    case "VersionWitnesses":
    case "ReplacementWitnesses": {
      const witness = versionWitnessSchema(sourceId, source, versionWitnessFields(source.rowShape));
      const schemas: VersionWitnessSchemas = {
        roleField: "event_kind",
        content: witness,
        deletion: { ...witness },
        routingParamFields,
      };
      schema = { kind: key.kind, schema: schemas };
      break;
    }
    case "RelationEdges":
      schema = { kind: "RelationEdges", schema: relationEdgeSchema(plan, source, resolvedSources) };
      break;
    case "PathCorrelationCoverage":
      schema = {
        kind: "PathCorrelationCoverage",
        schema: pathCorrelationCoverageSchema(plan, source, resolvedSources),
      };
      break;
    default:
      throw CapabilityError.explained(
        { kind: "Output", fact: key },
        "requested fact is not lowered yet",
      );
  }
  return { key, terminal, schema };
}

function resultOccurrenceIdFields(
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): string[] {
  const fields = [source.rowShape.rowUuidField];
  if (!structEq(source.rowShape.source, planRootSource(plan))) return fields;
  const steps = rootLinearSteps(plan);
  const last = steps?.[steps.length - 1];
  if (last?.kind === "Project") {
    fields.push(
      ...last.columns.map((c) => c.output.name).filter((n) => n.startsWith("__flat_join_row_")),
    );
  } else {
    for (const [name, ty] of rootJoinOccurrenceFields(plan, resolvedSources, request)) {
      if (ty.kind === "Uuid") fields.push(name);
    }
  }
  return fields;
}

function resultOccurrenceUnionArmFields(
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): SortedMap<number, string> {
  const arms = new SortedMap<number, string>((a, b) => a - b);
  if (!structEq(source.rowShape.source, planRootSource(plan))) return arms;
  const fields = rootJoinOccurrenceFields(plan, resolvedSources, request);
  if (plan.kind === "Union") {
    arms.set(0, "__root_union_arm");
    return arms;
  }
  let joinedPosition = 1;
  let pendingArm: string | undefined;
  for (const [name, ty] of fields) {
    if (ty.kind === "String") pendingArm = name;
    else if (ty.kind === "Uuid") {
      if (pendingArm !== undefined) arms.set(joinedPosition, pendingArm);
      pendingArm = undefined;
      joinedPosition++;
    }
  }
  return arms;
}

export function flatJoinPayloadFields(plan: AnalyzedQueryPlan): TypedOutputField[] {
  const steps = rootLinearSteps(plan);
  const last = steps?.[steps.length - 1];
  if (
    last?.kind !== "Project" ||
    !last.columns.some((c) => c.output.name.startsWith("__flat_join_row_"))
  )
    return [];
  return last.columns.filter((c) => c.output.name.includes(".")).map((c) => c.output);
}

function resultPayloadFields(plan: AnalyzedQueryPlan, source: ResolvedSource): TypedOutputField[] {
  const flatJoin = flatJoinPayloadFields(plan);
  if (flatJoin.length > 0 || !source.requiresResultPayload) return flatJoin;
  const hidden = hiddenSourceFields(source.rowShape);
  const out: TypedOutputField[] = [];
  for (const field of source.rowShape.descriptor.fields()) {
    const name: string | undefined = field.name;
    if (
      name !== undefined &&
      name !== source.rowShape.rowUuidField &&
      !hidden.has(name) &&
      !source.routingFields.has(name)
    ) {
      out.push({ name, ty: field.valueType });
    }
  }
  return out;
}

function projectedMultisourceTerminal(
  plan: AnalyzedQueryPlan,
  rootSource: ResolvedSource,
): [SourceId, TypedOutputField[], boolean] | undefined {
  const steps = rootLinearSteps(plan);
  const last = steps?.[steps.length - 1];
  if (last?.kind !== "Project") return undefined;
  const outputColumn = last.columns.find(
    (c) =>
      c.value.kind === "RowId" &&
      c.value.row.kind === "Source" &&
      c.output.name === rootSource.rowShape.rowUuidField,
  );
  if (
    !outputColumn ||
    outputColumn.value.kind !== "RowId" ||
    outputColumn.value.row.kind !== "Source"
  )
    return undefined;
  const isFlat = last.columns.some((c) => c.output.name.startsWith("__flat_join_row_"));
  return isFlat
    ? [outputColumn.value.row.source, last.columns.map((c) => c.output), true]
    : undefined;
}

function rootLinearSteps(plan: AnalyzedQueryPlan): LinearStep[] | undefined {
  if (plan.kind === "Linear") return plan.plan.steps;
  if (plan.kind === "Union") return plan.plan.terminalSteps;
  return undefined;
}

export function outputRoutingFields(output: ProgramFactOutput): SortedSet<string> {
  const schema = output.schema;
  switch (schema.kind) {
    case "AuthorizedRows":
    case "ResultMembership":
    case "AggregateResult":
    case "ProgramSourceCoverage":
    case "VersionWitnesses":
    case "ReplacementWitnesses":
    case "ReadFrontierSettled":
      return strSet(schema.schema.routingParamFields);
    default:
      return strSet();
  }
}

const FACT_SINKS: Partial<Record<ProgramFactKey["kind"], string>> = {
  AuthorizedRows: "policy.authorized_rows",
  ResultMembership: "maintained.result_current",
  VersionWitnesses: "maintained.version_content",
  ReplacementWitnesses: "maintained.replacement_content",
  RelationEdges: "maintained.relation_edges",
  PathCorrelationCoverage: "maintained.path_coverage",
  ProgramSourceCoverage: "maintained.program_source_coverage",
};

const factSinkName = (key: ProgramFactKey): string => FACT_SINKS[key.kind] ?? `fact.${dbg(key)}`;

const scopedFactSinkName = (key: ProgramFactKey, source: SourceId) =>
  `${factSinkName(key)}.${source.table}.${sourcePathSinkFragment(source)}`;

function scopedDeletionFactSinkName(key: ProgramFactKey, source: SourceId): string {
  const flavour = witnessFlavourOf(key);
  if (flavour === undefined) return scopedFactSinkName(key, source);
  return `maintained.${witnessTag(flavour, "Deletion")}.${source.table}.${sourcePathSinkFragment(source)}`;
}

const sinkSafe = (name: string) => name.replace(/[^A-Za-z0-9]/g, "_");
const ROLE_SINK_PREFIX: Record<string, string> = {
  RecursiveSeed: "recursive_seed_",
  RecursiveStep: "recursive_step_",
  CorrelatedChild: "correlated_child_",
  Policy: "policy_",
  Alias: "",
};

function sourcePathSinkFragment(source: SourceId): string {
  return source.path.components
    .map((c) => (c.kind === "Root" ? "root" : `${ROLE_SINK_PREFIX[c.kind]}${sinkSafe(c.name)}`))
    .join(".");
}

function factTerminalGraph(
  key: ProgramFactKey,
  graph: GraphBuilder,
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
  routingParamFields: SortedSet<string>,
): GraphBuilder {
  switch (key.kind) {
    case "AuthorizedRows":
      return graph.projectFields([
        ProjectField.named(source.rowShape.rowUuidField),
        ...[...routingParamFields].map((f) => ProjectField.named(f)),
      ]);
    case "ResultMembership": {
      if (rootAggregateStep(plan) !== undefined) {
        // Groove models an ungrouped aggregate as one logical empty group,
        // including COUNT(*) = 0; filtering it here made maintained and
        // covered-input reads disagree with one-shot evaluation.
        return graph.projectFields(
          aggregateResultMembershipFields(plan, source, routingParamFields),
        );
      }
      const occurrenceFields = [
        ...resultOccurrenceIdFields(plan, source, resolvedSources, request),
        ...resultOccurrenceUnionArmFields(plan, source, resolvedSources, request).valuesArray(),
      ];
      return graph.projectFields(
        resultMembershipFields(
          source,
          routingParamFields,
          resultPayloadFields(plan, source),
          occurrenceFields,
          flatJoinPayloadFields(plan).length === 0,
        ),
      );
    }
    case "VersionWitnesses":
      return contentVersionWitnessGraph(source, "version_content");
    case "ReplacementWitnesses":
      return contentVersionWitnessGraph(source, "replacement_content");
    case "RelationEdges":
      relationEdgeSchema(plan, source, resolvedSources);
      return relationEdgeGraph(key, graph, plan, source, resolvedSources, request);
    case "PathCorrelationCoverage":
      pathCorrelationCoverageSchema(plan, source, resolvedSources);
      return graph;
    case "ProgramSourceCoverage":
      throw CapabilityError.explained(
        { kind: "Output", fact: key },
        "program source coverage is an independently seeded control-plane terminal",
      );
    default:
      throw CapabilityError.explained(
        { kind: "Output", fact: key },
        "requested fact graph is not lowered yet",
      );
  }
}

function seedValues(complete: boolean, message: string): GraphBuilder {
  try {
    return GraphBuilder.values(new RecordDescriptor([["complete", { kind: "Bool" }]]), [
      [Value.Bool(complete)],
    ]);
  } catch (error) {
    throw CapabilityError.runtime(`${message}: ${error}`);
  }
}

/**
 * Emit a source-completeness receipt for the bound program scope, even when
 * the admitted residual relation is empty. Prepared programs read routes from
 * their binding input so a reused program never retains the first binding's
 * coverage route.
 */
function programSourceCoverageGraph(
  request: LoweringContext,
  parameterDomain: ParameterDomain,
  complete: boolean,
  routingParamFields: SortedSet<string>,
): GraphBuilder {
  const shape = request.input.binding.sourceShape;
  if (routingParamFields.size > 0 && shape !== undefined) {
    const [binding, routes] = orReport(() =>
      bindingRouteProjection(
        shape,
        request.input.binding.extraUserParams,
        parameterDomain,
        routingParamFields,
      ),
    );
    return binding.projectFields([
      ProjectField.literal("complete", Value.Bool(complete)),
      ...routes,
    ]);
  }
  if (routingParamFields.size === 0)
    return seedValues(complete, "could not seed program-source coverage terminal");
  const graph = seedValues(complete, "could not seed routed program-source coverage terminal");
  return graph.projectFields([
    ProjectField.named("complete"),
    ...orReport(() =>
      [...routingParamFields].map((f) =>
        routeLiteralProjectFieldForDomain(f, request, parameterDomain),
      ),
    ),
  ]);
}

/**
 * A binding-dependent terminal consumes an explicit typed input; compilation
 * only resolves names against its descriptor.
 */
function bindingRouteProjection(
  shape: string,
  extraUserParams: SortedMap<string, ColumnType>,
  domain: ParameterDomain,
  routes: SortedSet<string>,
): [GraphBuilder, any[]] {
  const params = extraUserParams.clone();
  for (const [name, ty] of domain.userParams) params.set(name, ty);
  const descriptor = new RecordDescriptor([
    ...params,
    ...[...domain.claimParams].map(([name, claim]): [string, ColumnType] => [name, claim.ty]),
  ]);
  const fields = [...routes].map((route) => {
    const source = domain.claimParams.has(route) ? route : routeParamFromField(route);
    if (source === undefined) throw runtimeGap(`binding route '${route}' has no parameter slot`);
    const index = descriptor.fieldIndex(source);
    if (index === undefined)
      throw runtimeGap(`binding route '${route}' is absent from its input descriptor`);
    return ProjectField.renamedResolved(index, route);
  });
  return [GraphBuilder.bindingSource(shape, descriptor), fields];
}

export function routeLiteralProjectField(routeField: string, request: LoweringContext): any {
  return routeLiteralProjectFieldForDomain(routeField, request, request.parameterDomain());
}

/**
 * Build a literal route field using the descriptor domain that will bind the
 * prepared program, so literal-only terminals compare the same representation
 * as the binding source.
 */
function routeLiteralProjectFieldForDomain(
  routeField: string,
  request: LoweringContext,
  domain: ParameterDomain,
): any {
  const claimPath = claimPathFromParamField(routeField);
  let scalar: [NormalizedValueRef, ColumnType] | undefined;
  if (claimPath !== undefined) {
    const claim = domain.claimParams.get(routeField);
    if (claim) scalar = [{ kind: "Claim", path: claimPath }, claim.ty];
  } else {
    const param = routeParamFromField(routeField);
    const ty = param !== undefined ? domain.userParams.get(param) : undefined;
    if (param !== undefined && ty !== undefined) scalar = [{ kind: "Param", name: param }, ty];
  }
  if (scalar) {
    const expression = request.scalarArgument(scalar[0], scalar[1], true);
    if (expression !== undefined) {
      return new ProjectField({
        expression,
        outputName: routeField,
        outputIdentity: FieldIdentity.Name(routeField),
      });
    }
  }
  if (claimPath !== undefined) {
    const value = claimValue(claimPath, request.policy);
    const claim = domain.claimParams.get(routeField);
    // Prepared subscriptions compare routes against the descriptor-coerced
    // binding record (a UUID claim may be a string parameter); use the shared
    // coercion rather than the raw claim encoding.
    return claim
      ? ProjectField.literalTyped(routeField, coercePreparedBindingValue(value, claim.ty), claim.ty)
      : ProjectField.literal(routeField, value);
  }
  const param = routeParamFromField(routeField);
  if (param === undefined) {
    throw runtimeGap(
      `authorization route field '${routeField}' is neither a claim nor user parameter`,
    );
  }
  const value = request.input.binding.values.get(param);
  if (value === undefined) {
    throw runtimeGap(
      `authorization route field '${routeField}' refers to unbound parameter '${param}'`,
    );
  }
  // The binding descriptor owns the type, not the literal's payload: an enum
  // tag cannot reconstruct its registry, nor null its element type.
  const ty = domain.userParams.get(param);
  return ty !== undefined
    ? ProjectField.literalTyped(routeField, coercePreparedBindingValue(value, ty), ty)
    : ProjectField.literal(routeField, value);
}

function relationEdgeGraph(
  key: ProgramFactKey,
  graph: GraphBuilder,
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): GraphBuilder {
  switch (plan.kind) {
    case "CorrelatedPath": {
      const graphs = correlatedRelationEdgeGraphs(
        plan.plan,
        graph,
        source,
        resolvedSources,
        request,
      );
      return graphs.length === 1 ? graphs[0] : GraphBuilder.union(graphs);
    }
    case "RecursiveRelation":
      return graph;
    default:
      throw CapabilityError.explained(
        { kind: "Output", fact: key },
        "relation edge facts require a path or recursive relation node",
      );
  }
}

function explainedGap<T>(capability: string, f: () => T): T {
  try {
    return f();
  } catch (err) {
    if (err instanceof UnsupportedError) throw CapabilityError.explained(err.gap, capability);
    throw err;
  }
}

function correlatedRelationEdgeGraphs(
  path: CorrelatedPathPlan,
  graph: GraphBuilder,
  source: ResolvedSource,
  resolvedSources: Sources,
  request: LoweringContext,
): GraphBuilder[] {
  const target = lookupResolvedSource(resolvedSources, path.path.child, "path child");
  const graphs = [graph.projectFields(correlatedRelationEdgeFields(source, target, path))];
  for (const sibling of path.siblings) {
    const siblingGraph = explainedGap(
      "sibling correlated path relation facts lower from the shared root graph",
      () => lowerCorrelatedPathRelationGraph(sibling, source, resolvedSources, request).graph,
    );
    graphs.push(
      ...correlatedRelationEdgeGraphs(sibling, siblingGraph, source, resolvedSources, request),
    );
  }
  for (const nested of path.nested) {
    const nestedParent = graph.projectFields(
      projectSourceFieldsFromPrefix(target, RIGHT_JOIN_PREFIX),
    );
    const nestedGraph = explainedGap(
      "nested correlated path relation facts lower from parent-child path graphs",
      () =>
        lowerCorrelatedPathRelationGraphFromParent(
          nested,
          nestedParent,
          target,
          resolvedSources,
          request,
          true,
        ).graph,
    );
    graphs.push(
      ...correlatedRelationEdgeGraphs(nested, nestedGraph, target, resolvedSources, request),
    );
  }
  return graphs;
}

function correlatedRelationEdgeFields(
  source: ResolvedSource,
  target: ResolvedSource,
  path: CorrelatedPathPlan,
): any[] {
  const sv = versionWitnessFields(source.rowShape);
  const tv = versionWitnessFields(target.rowShape);
  const lit = (name: string, value: string) => ProjectField.literal(name, Value.String(value));
  const fields = [
    lit("source_source", source.rowShape.source.table),
    lit("source_table", source.tableSchema.name),
    ProjectField.renamed(leftField(source.rowShape.rowUuidField), "source_row"),
    ProjectField.renamed(leftField(sv.txTimeField), "source_tx_time"),
    ProjectField.renamed(leftField(sv.txNodeField), "source_tx_node_id"),
    lit("path", correlatedRelationName(path)),
    lit("kind", "array_subquery"),
    lit("role", "terminal"),
    lit("target_source", target.rowShape.source.table),
    lit("target_table", target.tableSchema.name),
    ProjectField.renamed(rightField(target.rowShape.rowUuidField), "target_row"),
    ProjectField.renamed(rightField(tv.txTimeField), "target_tx_time"),
    ProjectField.renamed(rightField(tv.txNodeField), "target_tx_node_id"),
  ];
  if (sv.branchOrPrefixField !== undefined) {
    fields.push(ProjectField.renamed(leftField(sv.branchOrPrefixField), "source_branch_or_prefix"));
  }
  if (tv.branchOrPrefixField !== undefined) {
    fields.push(
      ProjectField.renamed(rightField(tv.branchOrPrefixField), "target_branch_or_prefix"),
    );
  }
  return fields;
}

function correlatedRelationName(path: CorrelatedPathPlan): string {
  const components = path.path.child.path.components;
  for (let i = components.length - 1; i >= 0; i--) {
    const role = components[i];
    if (role.kind === "CorrelatedChild") {
      const colon = role.name.indexOf(":");
      return colon < 0 ? role.name : role.name.slice(colon + 1);
    }
  }
  return path.path.child.table;
}

const DELETION_KEYS = ["row_uuid", "tx_time", "tx_node_id"];

function deletionWitnessGraphForCurrentRegister(
  source: ResolvedSource,
  eventKind: string,
  request: LoweringContext,
  parameterDomain: ParameterDomain,
  routingParamFields: SortedSet<string>,
): GraphBuilder {
  const register = source.deletionRegister;
  if (!register)
    throw CapabilityError.runtime("resolved source did not provide deletion register source");
  const authorizedPreimage = source.authorizedDeletionPreimage;
  if (!authorizedPreimage)
    throw CapabilityError.runtime("resolved source did not provide authorized deletion preimage");
  // The raw register owns the exact deletion witness; the sibling (same
  // occurrence under IncludeDeleted policy) authorizes only the preimage the
  // receiver may learn is deleted. Match the register's exact winner, so a
  // later deletion never borrows authorization from an earlier one.
  const authorizedDeletedWinner = authorizedPreimage.graph.filter(
    GroovePredicateExpr.fromFieldLiteral(
      PredicateKind.Eq,
      "__jazz_deleted",
      LiteralValue.Bool(true),
    ),
  );
  const shape = request.input.binding.sourceShape;
  if (routingParamFields.size > 0 && shape !== undefined) {
    const witnessFields = deletionWitnessFieldsForTaggedRows(source, eventKind);
    const witnessNames: string[] = witnessFields.map((f: any) => f.outputName);
    const presentRoutes = intersect(authorizedPreimage.routingFields, routingParamFields);
    const witness = routedDeletionAuthorizationGraph(
      register.graph.projectFields(witnessFields),
      witnessNames,
      source.rowShape.rowUuidField,
      authorizedDeletedWinner,
      presentRoutes,
    );
    const missingRoutes = strSet([...routingParamFields].filter((f) => !presentRoutes.has(f)));
    if (missingRoutes.size === 0) return witness;
    const [binding, routes] = orReport(() =>
      bindingRouteProjection(
        shape,
        request.input.binding.extraUserParams,
        parameterDomain,
        routingParamFields,
      ),
    );
    return policyJoinIfNeeded(
      witness,
      binding.projectFields(routes),
      [...presentRoutes],
      [...presentRoutes],
      request,
    ).projectFields([
      ...[...witnessNames, ...presentRoutes].map((f) => ProjectField.renamed(leftField(f), f)),
      ...[...missingRoutes].map((f) => ProjectField.renamed(rightField(f), f)),
    ]);
  }
  const authorizedRegister = GraphBuilder.semiJoin(
    register.graph,
    authorizedDeletedWinner.project(DELETION_KEYS),
    [register.rowUuidField, "tx_time", "tx_node_id"],
    DELETION_KEYS,
  );
  return authorizedRegister.projectFields([
    ...deletionWitnessFieldsForTaggedRows(source, eventKind),
    ...orReport(() => [...routingParamFields].map((f) => routeLiteralProjectField(f, request))),
  ]);
}

/**
 * Transfer only routes that proved this exact deletion winner. Dedupe the
 * proof side first: several policy derivations must not multiply a witness.
 */
function routedDeletionAuthorizationGraph(
  witness: GraphBuilder,
  witnessFields: string[],
  rowUuidField: string,
  authorized: GraphBuilder,
  routes: SortedSet<string>,
): GraphBuilder {
  const left = [rowUuidField, "tx_time", "tx_node_id"];
  if (routes.size === 0)
    return GraphBuilder.semiJoin(witness, authorized.project(DELETION_KEYS), left, DELETION_KEYS);
  const proofFields = [...DELETION_KEYS, ...routes];
  const proof = GraphBuilder.argMaxBy(authorized.project(proofFields), proofFields, []);
  return GraphBuilder.join(witness, proof, left, DELETION_KEYS).projectFields([
    ...witnessFields.map((f) => ProjectField.renamed(leftField(f), f)),
    ...[...routes].map((f) => ProjectField.renamed(rightField(f), f)),
  ]);
}

const contentVersionWitnessGraph = (source: ResolvedSource, eventKind: string) =>
  contentVersionWitnessGraphFromVisibleGraph(source, source.graph, eventKind, strSet());

/**
 * Recover source-shaped rows from a visible (possibly flat-joined) relation,
 * keyed by exact version and prepared-binding route.
 */
export function sourceRowsForVisibleGraph(
  source: ResolvedSource,
  visible: GraphBuilder,
  routeFields: SortedSet<string>,
): GraphBuilder {
  const version = versionWitnessFields(source.rowShape);
  const keys = [
    source.rowShape.rowUuidField,
    version.txTimeField,
    version.txNodeField,
    ...intersect(source.routingFields, routeFields),
  ];
  if (routeFields.size === 0) return GraphBuilder.semiJoin(source.graph, visible, keys, keys);
  // A joined table's policy may introduce a route absent from the root
  // source; retain every authority route from the admitted visible relation.
  return GraphBuilder.join(source.graph, visible, keys, keys).projectFields([
    ...projectSourceFieldsFromPrefix(source, LEFT_JOIN_PREFIX),
    ...[...routeFields].map((f) => ProjectField.renamed(rightField(f), f)),
  ]);
}

/** Where a projection expression keeps its source FieldRef, if it has one. */
function projectExprKeySlot(expression: any): "field" | "source" | undefined {
  switch (expression.kind) {
    case "Field":
    case "Nullable":
    case "NullableFlat":
      return "field";
    case "RecordField":
    case "EnumTagRemap":
    case "EnumRemap":
    case "RecursiveEnumRemap":
      return "source";
    default:
      return undefined;
  }
}

/**
 * Attach immutable version evidence to rows that already passed the authority
 * program's visibility graph. A covered-input publication must not reopen the
 * physical source and bypass policy/filter selection.
 */
function contentVersionWitnessGraphFromVisibleGraph(
  source: ResolvedSource,
  visibleGraph: GraphBuilder,
  eventKind: string,
  routingParamFields: SortedSet<string>,
): GraphBuilder {
  // Visibility can be a wide join tuple; resolve its exact row/version keys
  // back to complete witnesses. The visibility join is required either way.
  const [witnessSource, witnessFields] = source.contentVersion
    ? [source.contentVersion.graph, unprefixedVersionWitnessFieldsForTaggedRows(source, eventKind)]
    : [source.graph, inlineVersionWitnessFieldsForTaggedRows(source, eventKind)];
  // Resolve selection against the raw carrier before expanding it into a witness.
  const witnessKeys = DELETION_KEYS.map((name) => {
    const field = witnessFields.find((f: any) => f.outputName === name);
    if (!field) throw new Error("witness projection declares its exact version key");
    if (field.expression.kind !== "Field") throw new Error("witness identity is a source field");
    return field.expression.field;
  });
  const version = versionWitnessFields(source.rowShape);
  const visibleKeys = [
    FieldRef.name(source.rowShape.rowUuidField),
    FieldRef.name(version.txTimeField),
    FieldRef.name(version.txNodeField),
  ];
  if (routingParamFields.size === 0) {
    return GraphBuilder.SemiJoin({
      left: witnessSource,
      right: visibleGraph,
      leftOn: witnessKeys,
      rightOn: visibleKeys,
      comparison: ValueComparison.Exact,
    }).projectFields(witnessFields);
  }
  // A routed publication keeps the visibility relation's bag multiplicity and
  // binding values; Groove prunes unused join inputs.
  for (const field of witnessFields) {
    const slot = projectExprKeySlot(field.expression);
    if (slot === undefined) continue;
    const key = field.expression[slot];
    if (key.kind === "Resolved") throw new Error("witness carriers are bound by name");
    field.expression[slot] = FieldRef.storedName(`right.${key.name}`);
  }
  for (const route of routingParamFields) {
    const projected = ProjectField.named(route);
    projected.expression = ProjectExpr.Field(FieldRef.storedName(`left.${route}`));
    witnessFields.push(projected);
  }
  return GraphBuilder.Join({
    left: visibleGraph,
    right: witnessSource,
    leftOn: visibleKeys,
    rightOn: witnessKeys,
    comparison: ValueComparison.Exact,
  }).projectFields(witnessFields);
}

function resultMembershipFields(
  source: ResolvedSource,
  routingParamFields: SortedSet<string>,
  payloadFields: TypedOutputField[],
  occurrenceIdFields: string[],
  includeSettlePosition: boolean,
): any[] {
  const version = versionWitnessFields(source.rowShape);
  const settlePosition = includeSettlePosition ? settlePositionField(source.rowShape) : undefined;
  const rowId = source.rowShape.rowUuidField;
  const fields = [
    ProjectField.literal("event_kind", Value.String("result_current")),
    ProjectField.literal("table_name", Value.String(source.tableSchema.name)),
    ProjectField.named(rowId),
    ProjectField.renamed(version.txTimeField, "content_tx_time"),
    ProjectField.renamed(version.txNodeField, "content_tx_node_id"),
  ];
  if (version.branchOrPrefixField !== undefined)
    fields.push(ProjectField.named(version.branchOrPrefixField));
  for (const f of occurrenceIdFields) if (f !== rowId) fields.push(ProjectField.named(f));
  fields.push(
    settlePosition !== undefined
      ? ProjectField.renamed(settlePosition, "settle_position")
      : ProjectField.nullTyped("settle_position", { kind: "Nullable", inner: { kind: "U64" } }),
  );
  for (const f of routingParamFields) fields.push(ProjectField.named(f));
  // The row identity is already the terminal's authoritative root carrier.
  for (const f of payloadFields) if (f.name !== rowId) fields.push(ProjectField.named(f.name));
  return fields;
}

function requireAggregateStep(
  plan: AnalyzedQueryPlan,
  what: string,
): [NormalizedValueRef[], AggregateExpr[]] {
  const step = rootAggregateStep(plan);
  if (!step) throw CapabilityError.runtime(`${what} requested for non-aggregate plan`);
  return step;
}

function aggregateAppRowDescriptor(
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
): RecordDescriptor {
  const [groupBy, outputs] = requireAggregateStep(plan, "aggregate app row descriptor");
  const fields: any[] = [];
  for (const value of groupBy) {
    const field = aggregateSourceFieldName(value, source);
    const descriptorField = source.rowShape.descriptor.fields().find((c: any) => c.name === field);
    if (!descriptorField) {
      throw CapabilityError.runtime(
        `aggregate group field ${dbg(field)} is missing from resolved descriptor`,
      );
    }
    // lower_aggregate removes the source cell-presence wrapper once before grouping.
    fields.push(
      new DescriptorField({
        name: field,
        valueType: aggregateUnwrappedInputType(descriptorField.valueType),
        identity: descriptorField.identity,
      }),
    );
  }
  for (const output of outputs) {
    const name = aggregateOutputField(output.output.name);
    fields.push(
      DescriptorField.new(name, aggregateOutputValueType(output, source)).withIdentity(
        FieldIdentity.Name(name),
      ),
    );
  }
  return RecordDescriptor.newWithFields(fields);
}

function aggregateResultSchema(
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  routingParamFields: SortedSet<string>,
): AggregateResultSchema {
  const [groupBy, outputs] = requireAggregateStep(plan, "aggregate result schema");
  // Use the emitted app-row descriptor's typed fields; carrier spellings are
  // never a lookup identity.
  const all = aggregateAppRowDescriptor(plan, source).fields();
  return {
    synthetic: {
      tableField: "table_name",
      rowField: "synthetic_row",
      replacementField: "synthetic_replacement",
      routingParamFields: strSet(routingParamFields),
    },
    groupKeyFields: all.slice(0, groupBy.length),
    groupNames: groupBy.map((group) => {
      const carrier = aggregateSourceFieldName(group, source);
      if (group.kind === "RowId") return "id";
      const name = resolvedSourcePublicName(source, carrier);
      if (name === undefined)
        throw CapabilityError.runtime("aggregate group has no declared logical name");
      return name;
    }),
    valueFields: all.slice(groupBy.length),
    valueNames: outputs.map((o) => o.output.name),
    routingParamFields,
  };
}

function storedProject(
  field: string,
  outputName: string,
  identity: any = FieldIdentity.Name(outputName),
): any {
  return new ProjectField({
    expression: ProjectExpr.Field(FieldRef.storedName(field)),
    outputName,
    outputIdentity: identity,
  });
}

function aggregateResultMembershipFields(
  plan: AnalyzedQueryPlan,
  source: ResolvedSource,
  routingParamFields: SortedSet<string>,
): any[] {
  const [groupBy, outputs] = requireAggregateStep(plan, "aggregate result fields");
  if (groupBy.length > 1) {
    throw CapabilityError.operator(
      "multi-column aggregate group result identity is not lowered yet",
    );
  }
  // The synthetic table is only a protocol label; identity is the group-key
  // value (or the fixed global token).
  const fields = [ProjectField.literal("table_name", Value.String("aggregate_result"))];
  fields.push(
    groupBy.length > 0
      ? storedProject(aggregateSourceFieldName(groupBy[0], source), "synthetic_row")
      : ProjectField.literal("synthetic_row", Value.String("global")),
  );
  for (const group of groupBy) {
    const field = aggregateSourceFieldName(group, source);
    const identity =
      source.rowShape.descriptor.fields().find((c: any) => c.name === field)?.identity ??
      FieldIdentity.Name(field);
    fields.push(storedProject(field, field, identity));
  }
  for (const output of outputs) {
    const name = aggregateOutputField(output.output.name);
    fields.push(storedProject(name, name));
  }
  for (const f of routingParamFields) fields.push(ProjectField.named(f));
  return fields;
}

const aggregateUnwrappedInputType = (valueType: ValueType): ValueType =>
  valueType.kind === "Nullable" ? valueType.inner : valueType;

function aggregateOutputValueType(output: AggregateExpr, source: ResolvedSource): ValueType {
  switch (output.function) {
    case "Count":
      return { kind: "U64" };
    case "Avg":
      return { kind: "Nullable", inner: { kind: "F64" } };
    default: {
      if (output.input === undefined)
        throw CapabilityError.operator("aggregate input is required for sum/min/max");
      const field = aggregateSourceFieldName(output.input, source);
      const valueType = aggregateCarrierFieldType(source, field);
      if (valueType === undefined) {
        throw CapabilityError.runtime(
          `aggregate input field ${dbg(field)} is missing from resolved descriptor`,
        );
      }
      const unwrapped = aggregateUnwrappedInputType(valueType);
      return unwrapped.kind === "Nullable" ? unwrapped : { kind: "Nullable", inner: unwrapped };
    }
  }
}

function aggregateSourceFieldName(value: NormalizedValueRef, source: ResolvedSource): string {
  if (value.kind === "SourceField" && structEq(value.source, source.rowShape.source)) {
    return orReport(() => requireSourceField(source, value.field));
  }
  if (
    value.kind === "RowId" &&
    value.row.kind === "Source" &&
    structEq(value.row.source, source.rowShape.source)
  ) {
    return source.rowShape.rowUuidField;
  }
  throw CapabilityError.operator("aggregate group keys must be root source fields");
}

const aggregateCarrierFieldType = (
  source: ResolvedSource,
  carrier: string,
): ValueType | undefined =>
  source.rowShape.descriptor.fields().find((f: any) => f.name === carrier)?.valueType;

const unprefixedVersionWitnessFieldsForTaggedRows = (source: ResolvedSource, eventKind: string) =>
  prefixedVersionWitnessFieldsForTaggedRows(source, eventKind, "");

/**
 * Witness field names identify compiler-owned source carriers, including
 * metadata whose spelling can collide with an application column identity.
 */
function bindWitnessCarrierFields(fields: any[]): any[] {
  for (const field of fields) {
    const slot = projectExprKeySlot(field.expression);
    if (slot === undefined || (slot === "source" && field.expression.kind === "RecordField"))
      continue;
    const key = field.expression[slot];
    if (key.kind === "Name") field.expression[slot] = FieldRef.storedName(key.name);
  }
  return fields;
}

function witnessHeader(source: ResolvedSource, eventKind: string): any[] {
  return [
    ProjectField.literal("event_kind", Value.String(eventKind)),
    ProjectField.literal("table_name", Value.String(source.tableSchema.name)),
  ];
}

const NULLABLE_U8: ValueType = { kind: "Nullable", inner: { kind: "U8" } };

function prefixedVersionWitnessFieldsForTaggedRows(
  source: ResolvedSource,
  eventKind: string,
  prefix: string,
): any[] {
  if (source.contentVersion === undefined) {
    throw CapabilityError.runtime("resolved source did not provide content version source");
  }
  const renamed: [string, string][] = [
    ["row_uuid", "row_uuid"],
    ["tx_time", "content_tx_time"],
    ["tx_node_id", "content_tx_node_id"],
    ["tx_time", "tx_time"],
    ["tx_node_id", "tx_node_id"],
    ["schema_version", "schema_version"],
    ["authored_columns", "authored_columns"],
    ["created_by", "created_by"],
    ["created_at", "created_at"],
    ["updated_by", "updated_by"],
    ["updated_at", "updated_at"],
  ];
  const fields = [
    ...witnessHeader(source, eventKind),
    ...renamed.map(([from, to]) => ProjectField.renamed(`${prefix}${from}`, to)),
    ProjectField.nullTyped("_deletion", NULLABLE_U8),
  ];
  // Every user field in a version witness is Nullable(column type) whichever
  // source realization supplied it: flatten an existing wrapper or add one.
  for (const column of source.tableSchema.columns) {
    fields.push(
      ProjectField.nullableFlat(
        `${prefix}${userColumnField(column.name)}`,
        tableUserColumnField(source.tableSchema.name, column.name),
      ),
    );
  }
  const branchOrPrefix = versionWitnessFields(source.rowShape).branchOrPrefixField;
  if (branchOrPrefix !== undefined)
    fields.push(ProjectField.renamed(`${prefix}${branchOrPrefix}`, branchOrPrefix));
  return bindWitnessCarrierFields(fields);
}

function inlineVersionWitnessFieldsForTaggedRows(source: ResolvedSource, eventKind: string): any[] {
  const version = versionWitnessFields(source.rowShape);
  const fields = [
    ...witnessHeader(source, eventKind),
    ProjectField.renamed(source.rowShape.rowUuidField, "row_uuid"),
    ProjectField.renamed(version.txTimeField, "content_tx_time"),
    ProjectField.renamed(version.txNodeField, "content_tx_node_id"),
    ProjectField.renamed(version.txTimeField, "tx_time"),
    ProjectField.renamed(version.txNodeField, "tx_node_id"),
    ProjectField.renamed(version.schemaVersionField, "schema_version"),
    ...["authored_columns", "created_by", "created_at", "updated_by", "updated_at"].map((f) =>
      ProjectField.named(f),
    ),
    ProjectField.nullTyped("_deletion", NULLABLE_U8),
  ];
  // Keep the witness contract identical to the physical-history path above.
  for (const column of source.tableSchema.columns) {
    fields.push(
      ProjectField.nullableFlat(
        userColumnField(column.name),
        tableUserColumnField(source.tableSchema.name, column.name),
      ),
    );
  }
  if (version.branchOrPrefixField !== undefined)
    fields.push(ProjectField.named(version.branchOrPrefixField));
  return bindWitnessCarrierFields(fields);
}

function deletionWitnessFieldsForTaggedRows(source: ResolvedSource, eventKind: string): any[] {
  const fields = [
    ...witnessHeader(source, eventKind),
    ProjectField.named(source.rowShape.rowUuidField),
    ProjectField.renamed("tx_time", "content_tx_time"),
    ProjectField.renamed("tx_node_id", "content_tx_node_id"),
    ...["tx_time", "tx_node_id", "schema_version"].map((f) => ProjectField.named(f)),
    ProjectField.nullTyped("authored_columns", {
      kind: "Nullable",
      inner: { kind: "Array", inner: { kind: "U64" } },
    }),
    ...["created_by", "created_at", "updated_by", "updated_at"].map((f) => ProjectField.named(f)),
    ProjectField.nullable("_deletion", "_deletion"),
  ];
  for (const column of source.tableSchema.columns) {
    fields.push(
      ProjectField.nullTyped(tableUserColumnField(source.tableSchema.name, column.name), {
        kind: "Nullable",
        inner: column.columnType,
      }),
    );
  }
  const branchOrPrefix = versionWitnessFields(source.rowShape).branchOrPrefixField;
  if (branchOrPrefix !== undefined) fields.push(ProjectField.named(branchOrPrefix));
  return bindWitnessCarrierFields(fields);
}

function relationEdgeSchema(
  plan: AnalyzedQueryPlan,
  rootSource: ResolvedSource,
  resolvedSources: Sources,
): RelationEdgeSchema {
  const base = { pathField: "path", kindField: "kind", roleField: "role" };
  switch (plan.kind) {
    case "CorrelatedPath": {
      const child = lookupResolvedSource(resolvedSources, plan.plan.path.child, "path child");
      return {
        ...base,
        source: prefixedVersionedRowRefSchema(rootSource, "source"),
        target: prefixedVersionedRowRefSchema(child, "target"),
      };
    }
    case "RecursiveRelation": {
      const relation = plan.plan;
      const stepSource =
        linearRootSource(relation.step.root) ?? firstStepSource(relation.step.steps);
      if (!stepSource) throw CapabilityError.runtime("recursive step source was not resolved");
      const step = lookupResolvedSource(resolvedSources, stepSource, "recursive step");
      return {
        ...base,
        source: versionedRowRefSchema(rootSource),
        target: versionedRowRefSchema(step),
        depthField: "depth",
      };
    }
    default:
      throw CapabilityError.explained(
        { kind: "Output", fact: { kind: "RelationEdges" } },
        "relation edge facts require a path or recursive relation node",
      );
  }
}

function pathCorrelationCoverageSchema(
  plan: AnalyzedQueryPlan,
  rootSource: ResolvedSource,
  _resolvedSources: Sources,
): PathCorrelationCoverageSchema {
  const base = {
    pathField: "path",
    readableCountField: "readable_count",
    coverageStateField: "coverage_state",
  };
  switch (plan.kind) {
    case "CorrelatedPath":
      return {
        ...base,
        parent: versionedRowRefSchema(rootSource),
        correlationField: "correlation",
        expectedCountField:
          plan.plan.requirement === "MatchCorrelationCardinality" ? "expected_count" : undefined,
      };
    case "RecursiveRelation":
      return { ...base, parent: versionedRowRefSchema(rootSource), correlationField: "frontier" };
    default:
      throw CapabilityError.explained(
        { kind: "Output", fact: { kind: "PathCorrelationCoverage" } },
        "path correlation coverage facts require a path or recursive relation node",
      );
  }
}

function versionedRowRefSchema(source: ResolvedSource): VersionedRowRefSchema {
  const version = versionWitnessFields(source.rowShape);
  return {
    row: { sourceField: "source", tableField: "table", rowField: source.rowShape.rowUuidField },
    version: {
      kind: "Content",
      content: { txTimeField: version.txTimeField, txNodeField: version.txNodeField },
    },
    branchOrPrefixField: version.branchOrPrefixField,
  };
}

function prefixedVersionedRowRefSchema(
  source: ResolvedSource,
  prefix: string,
): VersionedRowRefSchema {
  const version = versionWitnessFields(source.rowShape);
  return {
    row: {
      sourceField: `${prefix}_source`,
      tableField: `${prefix}_table`,
      rowField: `${prefix}_row`,
    },
    version: {
      kind: "Content",
      content: { txTimeField: `${prefix}_tx_time`, txNodeField: `${prefix}_tx_node_id` },
    },
    branchOrPrefixField:
      version.branchOrPrefixField !== undefined ? `${prefix}_branch_or_prefix` : undefined,
  };
}

function versionWitnessSchema(
  sourceId: SourceId,
  source: ResolvedSource,
  version: VersionWitnessFieldRefs,
): VersionWitnessSchema {
  const table = source.tableSchema.name;
  return {
    source: programSourceId(sourceId),
    descriptor: source.rowShape.descriptor,
    identity: {
      tableField: "table_name",
      rowField: source.rowShape.rowUuidField,
      txTimeField: "tx_time",
      txNodeField: "tx_node_id",
      branchOrPrefixField: version.branchOrPrefixField,
      schemaField: "schema_version",
      layerField: "layer",
    },
    createdByField: "created_by",
    createdAtField: "created_at",
    updatedByField: "updated_by",
    updatedAtField: "updated_at",
    authoredColumnsField: "authored_columns",
    deletionField: "_deletion",
    userFields: strMap(
      source.tableSchema.columns.map((c: any) => [c.name, tableUserColumnField(table, c.name)]),
    ),
  };
}

type VersionWitnessFieldRefs = {
  schemaVersionField: string;
  txTimeField: string;
  txNodeField: string;
  branchOrPrefixField?: string;
};

function versionWitnessFields(rowShape: SourceRowShape): VersionWitnessFieldRefs {
  const meta = rowShape.metadata.get({ kind: "VersionWitnesses" });
  if (meta?.kind !== "VersionWitnesses") {
    throw CapabilityError.runtime("resolved source did not provide version witness fields");
  }
  return {
    schemaVersionField: meta.schemaVersionField,
    txTimeField: meta.txTimeField,
    txNodeField: meta.txNodeField,
    branchOrPrefixField: meta.branchOrPrefixField,
  };
}

function settlePositionField(rowShape: SourceRowShape): string | undefined {
  const meta = rowShape.metadata.get({ kind: "SettlePosition" });
  return meta?.kind === "SettlePosition" ? meta.settlePositionField : undefined;
}

function hiddenSourceFields(rowShape: SourceRowShape): SortedSet<string> {
  const fields: (string | undefined)[] = [];
  for (const m of rowShape.metadata.valuesArray()) {
    switch (m.kind) {
      case "VersionWitnesses":
        fields.push(m.schemaVersionField, m.txTimeField, m.txNodeField, m.branchOrPrefixField);
        break;
      case "DeletionMarkers":
        fields.push(m.deletionStateField, m.deletionTxTimeField, m.deletionTxNodeField);
        break;
      case "BatchMembership":
        fields.push(m.batchIdField, m.branchOrPrefixField, m.rowDigestField, m.batchKindField);
        break;
      case "Coverage":
        fields.push(m.coverageField);
        break;
      case "SettlePosition":
        fields.push(m.settlePositionField);
        break;
      case "ValidationReads":
        fields.push(m.snapshotField);
        break;
      case "PolicyWitnesses":
        fields.push(m.policyPathField, m.edgeKindField);
        break;
      case "Provenance":
        fields.push(m.field);
    }
  }
  return strSet(fields.filter((f): f is string => f !== undefined));
}
