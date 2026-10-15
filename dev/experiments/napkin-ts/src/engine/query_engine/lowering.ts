import { SortedMap, SortedSet, cmpStr } from "../../support/collections.js";
import {
  collectByOrderedScalar,
  coveredInputTrace,
  type ColumnType,
  GraphBuilder,
  type ProgramSourceId,
  type RecordDescriptor,
  type TableSchema,
} from "../../_extern/engine.js";
import {
  claimParamField,
  claimPathFromParamField,
  leftField,
  rightField,
  routeParamField,
  routeParamFromField,
} from "./fields.js";
import {
  dbg,
  programSourceId,
  structEq,
  structMap,
  type ClaimPath,
  type NormalizedRowSetShape,
  type NormalizedValueRef,
  type PredicateExpr,
  type ProgramBinding,
  type QueryProgramRequest,
  type SourceId,
} from "./input.js";
import {
  CapabilityError,
  emptyExplain,
  runtimeGap,
  UnsupportedError,
  type CapabilityReport,
  type ExplainPlan,
  type ResolvedSource,
  type SourceAuthorizationRequest,
  type SourceGraphPreparer,
  SourceResolutionError,
  type SourceRequest,
  type UnsupportedReason,
} from "./resolver.js";
import type { OutputTerminalSchema, ProgramOutputSchemas } from "./schemas.js";
import { LoweringContext, type ProgramArgumentRecipes } from "./lowering/arguments.js";
import {
  analyzeQueryPlan,
  planCapabilityLabel,
  planRootSource,
  rootAggregateStep,
  sourceCurrentTier,
  sourceVisibilities,
  type AnalyzedQueryPlan,
} from "./lowering/planning.js";
import { sourceRequirements } from "./lowering/requirements.js";
import { lowerPlanSteps } from "./lowering/graph_lowering.js";
import {
  loweredTerminals,
  outputRoutingFields,
  sharedWitnessSinks,
  validateAppRowPublicationSchema,
} from "./lowering/terminals.js";

export { QueryProgramTemplateCache } from "./lowering/templates.js";

export type ParameterDomain = {
  userParams: SortedMap<string, ColumnType>;
  claimParams: SortedMap<string, ClaimParameter>;
  routingParams: SortedSet<string>;
};
export const emptyParameterDomain = (): ParameterDomain => ({
  userParams: new SortedMap(cmpStr),
  claimParams: new SortedMap(cmpStr),
  routingParams: new SortedSet(cmpStr),
});

export type ClaimParameter = { path: ClaimPath; ty: ColumnType };

export type ResolvedQuerySources = SortedMap<SourceId, ResolvedSource>;

const reportWith = (
  gaps: UnsupportedReason[],
  request: QueryProgramRequest,
  explain: ExplainPlan,
) => new CapabilityError({ gaps, explain: explainWithRequest(request, explain) });

/** One compilation's analyzed structure and exact source requirements. */
export class QueryProgramCompilation {
  private constructor(
    readonly request: QueryProgramRequest,
    readonly plan: AnalyzedQueryPlan,
    readonly sources: SourceRequest[],
  ) {}

  static analyze(request: QueryProgramRequest): QueryProgramCompilation {
    let plan: AnalyzedQueryPlan;
    try {
      plan = analyzeQueryPlan(request);
    } catch (err) {
      if (!(err instanceof UnsupportedError)) throw err;
      const explain = emptyExplain();
      explain.capabilities.push("only current-source row-set lowering is implemented");
      throw reportWith(err.gaps, request, explain);
    }
    return new QueryProgramCompilation(request, plan, sourceRequestsForPlan(request, plan));
  }
}

export function queryProgramSourceRequests(request: QueryProgramRequest): SourceRequest[] {
  let plan: AnalyzedQueryPlan;
  try {
    plan = analyzeQueryPlan(request);
  } catch (err) {
    if (!(err instanceof UnsupportedError)) throw err;
    throw reportWith(err.gaps, request, emptyExplain());
  }
  return sourceRequestsForPlan(request, plan);
}

function sourceRequestsForPlan(
  request: QueryProgramRequest,
  plan: AnalyzedQueryPlan,
): SourceRequest[] {
  const visibilities = sourceVisibilities(plan);
  return [...sourceRequirements(request, plan)].map(([source, requirements]) => ({
    visibility: visibilities.get(source) ?? "Visible",
    authorization: sourceAuthorizationForSource(request, source),
    source,
    requirements,
  }));
}

/** The policy-filtered include-deleted sibling for a source that asked for deletion witnesses. */
export function authorizedDeletionPreimageSourceRequest(
  request: SourceRequest,
): SourceRequest | undefined {
  if (
    request.visibility !== "Visible" ||
    !request.requirements.metadata.has({ kind: "DeletionMarkers" })
  ) {
    return undefined;
  }
  return {
    ...request,
    visibility: "IncludeDeleted",
    requirements: {
      appFields: request.requirements.appFields,
      metadata: new SortedSet(cmpStr as any),
    },
  };
}

export async function prepareAndLowerQueryProgram(
  compilation: QueryProgramCompilation,
  preparer: SourceGraphPreparer,
): Promise<QueryProgram> {
  const [sources, explain] = await prepareQueryProgramSources(compilation, preparer);
  return lowerResolvedQueryProgram(compilation, sources, explain);
}

export async function prepareQueryProgramSources(
  compilation: QueryProgramCompilation,
  preparer: SourceGraphPreparer,
): Promise<[ResolvedQuerySources, ExplainPlan]> {
  const explain = emptyExplain();
  const request = compilation.request;
  const resolvedSources = structMap<SourceId, ResolvedSource>();
  for (const sourceRequest of compilation.sources) {
    const source = sourceRequest.source;
    let resolved: ResolvedSource;
    try {
      resolved = await preparer.prepareSourceGraph(sourceRequest);
    } catch (err) {
      if (!(err instanceof SourceResolutionError)) throw err;
      const failure = {
        ...explain,
        read: [...explain.read, `failed source request: ${dbg(err.request)}`],
      };
      throw reportWith([{ kind: "Source", gap: err.gap }], request, failure);
    }
    const preimageRequest =
      resolved.deletionRegister && authorizedDeletionPreimageSourceRequest(sourceRequest);
    if (preimageRequest) {
      try {
        const preimage = await preparer.prepareSourceGraph(preimageRequest);
        resolved.authorizedDeletionPreimage = {
          graph: preimage.graph,
          routingFields: preimage.routingFields,
        };
      } catch (err) {
        if (!(err instanceof SourceResolutionError)) throw err;
        throw reportWith([{ kind: "Source", gap: err.gap }], request, {
          ...emptyExplain(),
          read: [`failed authorized deletion preimage request: ${dbg(err.request)}`],
        });
      }
    }
    explain.physical.push(
      `source ${dbg(source)} (${dbg(sourceCurrentTier(request, source))}) -> resolved table ${resolved.tableSchema.name}`,
    );
    resolvedSources.set(source, resolved);
  }
  return [resolvedSources, explain];
}

/** Purely lower a request whose Groove sources have already been prepared. */
export function lowerResolvedQueryProgram(
  compilation: QueryProgramCompilation,
  resolvedSources: ResolvedQuerySources,
  explain: ExplainPlan,
): QueryProgram {
  return lowerResolvedQueryProgramWithSourceParameters(
    compilation,
    resolvedSources,
    explain,
    new Map(),
  );
}

export function lowerResolvedQueryProgramWithSourceParameters(
  compilation: QueryProgramCompilation,
  resolvedSources: ResolvedQuerySources,
  explain: ExplainPlan,
  sourceParameters: Map<number, ParameterDomain>,
  args?: ProgramArgumentRecipes,
): QueryProgram {
  const { request, plan } = compilation;
  const context = args
    ? LoweringContext.compiling(request, args)
    : LoweringContext.concrete(request);
  const asReport = <T>(f: () => T): T => {
    try {
      return f();
    } catch (err) {
      if (err instanceof UnsupportedError) throw reportWith([err.gap], request, { ...explain });
      throw err;
    }
  };
  const resolvedRoot = resolvedSources.get(planRootSource(plan));
  if (!resolvedRoot) {
    throw reportWith([{ kind: "Runtime", message: "root source was not resolved" }], request, {
      ...explain,
    });
  }
  explain.capabilities.push(planCapabilityLabel(plan));
  const lowered = asReport(() =>
    lowerPlanSteps(resolvedRoot.graph, plan, resolvedRoot, resolvedSources, context),
  );
  const parameters = asReport(() => parameterDomainForRequest(request));
  collectBindingSourceParamsWithSlots(lowered.graph, parameters, sourceParameters);
  retainKnownRoutingParams(parameters);

  const internalAppRowsGraph =
    request.output.appRows !== undefined && rootAggregateStep(plan) === undefined
      ? lowered.graph
      : undefined;
  const terminals = loweredTerminals(
    lowered.graph,
    context,
    plan,
    resolvedRoot,
    resolvedSources,
    parameters,
    parameters.routingParams,
    lowered.fields,
  );
  for (const terminal of terminals) {
    if (terminal.output.kind === "AppRows") validateAppRowPublicationSchema(terminal.output.schema);
  }
  // Terminals share most of their graph, so both verification passes share one memo.
  const declaredFields = new DeclaredOutputFields();
  verifyRoutedTerminalOutputs(terminals, parameters, request, explain, declaredFields);
  const output: ProgramOutputSchemas = {
    kind: "RowSet",
    terminals: terminals.map((t) => t.output),
  };
  const coveredInputSourceDescriptors = coveredInputDescriptors(terminals);

  for (const terminal of terminals) {
    collectBindingSourceParamsWithSlots(terminal.graph, parameters, sourceParameters);
  }
  retainKnownRoutingParams(parameters);
  verifyRoutedTerminalOutputs(terminals, parameters, request, explain, declaredFields);

  const maintainedTerminalTables = new SortedMap<string, TableSchema>(cmpStr);
  for (const source of [...resolvedSources.valuesArray()].reverse()) {
    maintainedTerminalTables.getOrInsert(source.tableSchema.name, () => source.tableSchema);
  }
  const sourceDescriptors = new SortedMap<ProgramSourceId, RecordDescriptor>(cmpByKey);
  for (const [source, resolved] of resolvedSources) {
    sourceDescriptors.set(programSourceId(source), resolved.rowShape.descriptor);
  }
  return {
    lowered: {
      sharedWitnessSinks: sharedWitnessSinks(terminals),
      terminals,
      internalAppRowsGraph,
      parameters,
      output,
      maintainedTerminalTables,
      targetedRefreshTables: new SortedSet(cmpStr, maintainedTerminalTables.keysArray()),
      targetedRefreshUncertain: false,
    },
    sourceDescriptors,
    coveredInputSourceDescriptors,
    request,
    explain,
  };
}

const cmpByKey = (a: unknown, b: unknown) => cmpStr(dbg(a), dbg(b));

function retainKnownRoutingParams(parameters: ParameterDomain): void {
  const keep = [...parameters.routingParams].filter((field) => {
    const param = routeParamFromField(field);
    if (param !== undefined && parameters.userParams.has(param)) return true;
    return claimPathFromParamField(field) !== undefined && parameters.claimParams.has(field);
  });
  parameters.routingParams = new SortedSet(cmpStr, keep);
}

function coveredInputDescriptors(
  terminals: LoweredTerminal[],
): SortedMap<ProgramSourceId, RecordDescriptor> {
  const descriptors = new SortedMap<ProgramSourceId, RecordDescriptor>(cmpByKey);
  for (const terminal of terminals) {
    const output = terminal.output;
    if (
      output.kind !== "Fact" ||
      output.fact.key.kind !== "VersionWitnesses" ||
      output.fact.schema.kind !== "VersionWitnesses"
    ) {
      continue;
    }
    const witness = output.fact.schema.schema.content;
    if (!witness) continue;
    const existing = descriptors.set(witness.source, witness.descriptor);
    if (existing !== undefined && !structEq(existing, witness.descriptor)) {
      throw CapabilityError.runtime("covered input source has conflicting compiled descriptors");
    }
  }
  return descriptors;
}

function verifyRoutedTerminalOutputs(
  terminals: LoweredTerminal[],
  parameters: ParameterDomain,
  request: QueryProgramRequest,
  explain: ExplainPlan,
  declaredFields: DeclaredOutputFields,
): void {
  for (const terminal of terminals) {
    const expected = terminalSchemaRoutingFields(terminal.output, parameters.routingParams);
    if (expected.length === 0) continue;
    const actual = declaredFields.of(terminal.graph);
    if (!actual) {
      throw reportWith(
        [
          {
            kind: "Runtime",
            message: `routed terminal '${terminal.sink}' output fields could not be verified`,
          },
        ],
        request,
        { ...explain },
      );
    }
    for (const field of expected) {
      if (!actual.has(field)) {
        throw reportWith(
          [
            {
              kind: "Runtime",
              message: `routed terminal '${terminal.sink}' is missing route field '${field}'`,
            },
          ],
          request,
          { ...explain },
        );
      }
    }
  }
}

function terminalSchemaRoutingFields(
  output: OutputTerminalSchema,
  routingParams: SortedSet<string>,
): string[] {
  if (output.kind === "AppRows")
    return [...output.schema.hiddenFields].filter((f) => routingParams.has(f));
  return [...outputRoutingFields(output.fact)];
}

export function graphDeclaredOutputFields(graph: GraphBuilder): Set<string> | undefined {
  return new DeclaredOutputFields().of(graph);
}

/** Declared output fields per builder node, memoized by node identity. */
class DeclaredOutputFields {
  private outputs = new Map<GraphBuilder, Set<string> | undefined>();

  of(graph: GraphBuilder): Set<string> | undefined {
    // Iterative so deep policy graphs never exhaust the stack.
    if (!this.outputs.has(graph)) {
      for (const node of graphBuilderPostorder(graph)) {
        if (this.outputs.has(node)) continue;
        this.outputs.set(
          node,
          declaredNodeFields(node, (child) => {
            const fields = this.outputs.get(child);
            return fields && new Set(fields);
          }),
        );
      }
    }
    return this.outputs.get(graph);
  }
}

function declaredNodeFields(
  node: GraphBuilder,
  childOutput: (child: GraphBuilder) => Set<string> | undefined,
): Set<string> | undefined {
  switch (node.kind) {
    case "TemplateInput":
    case "InlineRecords":
    case "FrontierSource":
    case "BindingSource":
      return descriptorNamedFields(node.output);
    case "TypedTemplate":
      return descriptorNamedFields(node.program.outputDescriptor());
    case "InputSource":
    case "Table":
    case "Index":
    case "Lookup":
      return undefined;
    case "Project":
      return new Set(node.fields.map((f: any) => f.outputName));
    case "StreamingChecksum": {
      const fields = childOutput(node.input);
      if (!fields || node.field.kind === "Resolved") return undefined;
      fields.delete(node.field.name);
      fields.add(node.outputField);
      return fields;
    }
    case "Aggregate":
      return new Set([
        ...node.groupCols.map((f: any) => f.displayName()),
        ...node.aggregates.map((a: any, i: number) => a.outputName ?? `aggregate_${i}`),
      ]);
    case "CollectBy":
      return new Set([
        ...node.collect.parentFields.map((f: any) => f.outputName),
        node.collect.collectionField,
      ]);
    case "Filter":
    case "UnwrapNullable":
    case "VariantProject":
    case "ArgMaxBy":
    case "ArgMinBy":
    case "TopBy":
      return childOutput(node.input);
    case "SemiJoin":
    case "AntiJoin":
      return childOutput(node.left);
    case "Unnest": {
      const fields = childOutput(node.input);
      fields?.add(node.elementField);
      return fields;
    }
    case "Recursive":
      return childOutput(node.seed);
    case "RecursiveStepWitness":
      return node.recursive.kind === "Recursive" && node.recursive.stepWitness
        ? childOutput(node.recursive.stepWitness)
        : undefined;
    case "Union": {
      let fields: Set<string> | undefined;
      for (const input of node.inputs) {
        const child = childOutput(input);
        if (!child) return undefined;
        fields = fields ? new Set([...fields].filter((f) => child.has(f))) : child;
      }
      return fields;
    }
    case "Join": {
      const left = childOutput(node.left);
      const right = childOutput(node.right);
      if (!left || !right) return undefined;
      return new Set([...[...left].map(leftField), ...[...right].map(rightField)]);
    }
    default:
      return undefined;
  }
}

function descriptorNamedFields(descriptor: RecordDescriptor): Set<string> | undefined {
  const fields = new Set<string>();
  for (const field of descriptor.fields()) {
    if (field.name === undefined) return undefined;
    fields.add(field.name);
  }
  return fields;
}

export function explainWithRequest(
  request: QueryProgramRequest,
  explain: ExplainPlan,
): ExplainPlan {
  return {
    ...explain,
    input: dbg(request.input),
    read: [dbg(request.reads), ...explain.read],
    policy: [dbg(request.policy), ...explain.policy],
    output: [dbg(request.output), ...explain.output],
  };
}

function sourceAuthorizationForSource(
  request: QueryProgramRequest,
  source: SourceId,
): SourceAuthorizationRequest {
  // Client-local results are scoped by the upstream emission boundary.
  if (request.authorizationMode === "ClientLocal") {
    if (coveredInputTrace()) {
      console.error(
        `JAZZ_COVERED_INPUT_TRACE stage=source_authorization mode=client_local source=${dbg(source)} policy=${dbg(request.policy)} authorization=system`,
      );
    }
    return { kind: "System" };
  }
  const policy = request.policy;
  let authorization: SourceAuthorizationRequest;
  if (policy.kind === "Identity") {
    authorization = {
      kind: "PolicyFiltered",
      permissionSubject: policy.permissionSubject,
      plan: {
        protectedSource: source,
        role: "Read",
        protectedRowField: "row_uuid",
        bindingSourceShape: request.input.binding.sourceShape,
        bindingUserParams: bindingUserParamTypes(request.input.binding),
        bindingClaimParams: request.input.binding.claimParams,
      },
    };
  } else {
    // Auxiliary closure and payload sources are read under system authority.
    authorization = { kind: "System" };
  }
  if (coveredInputTrace()) {
    console.error(
      `JAZZ_COVERED_INPUT_TRACE stage=source_authorization mode=trusted_serving source=${dbg(source)} policy=${dbg(policy)} authorization=${dbg(authorization)}`,
    );
  }
  return authorization;
}

function bindingUserParamTypes(binding: ProgramBinding): SortedMap<string, ColumnType> {
  const params = binding.extraUserParams.clone();
  for (const name of binding.values.keysArray()) {
    if (binding.claimParams.has(name)) continue;
    const ty = binding.paramTypes.get(name);
    if (ty === undefined) {
      throw CapabilityError.runtime(`binding parameter '${name}' is missing a validated type`);
    }
    params.set(name, ty);
  }
  return params;
}

/** Adds `graph` to the membership of `source`, unioning with any graph already recorded. */
export function unionMember(
  members: SortedMap<SourceId, GraphBuilder>,
  source: SourceId,
  graph: GraphBuilder,
): void {
  const existing = members.get(source);
  members.set(source, existing === undefined ? graph : GraphBuilder.union([existing, graph]));
}

function parameterDomain(shape: NormalizedRowSetShape): ParameterDomain {
  const domain = emptyParameterDomain();
  for (const node of shape.nodes.valuesArray()) {
    if (node.kind === "ValueSource" && node.mode === "Binding") {
      for (const column of node.columns) {
        if (column.value.kind === "Param") {
          domain.userParams.set(column.value.name, column.ty);
          domain.routingParams.add(routeParamField(column.value.name));
        } else if (column.value.kind === "Claim") {
          const param = claimParamField(column.value.path);
          domain.claimParams.set(param, { path: column.value.path, ty: column.ty });
          if (collectByOrderedScalar(column.ty)) domain.routingParams.add(param);
        }
      }
    } else if (node.kind === "Filter") {
      collectEqualityFilterRouteParams(node.predicate, domain.routingParams);
    }
    // INV-LOWER-13: aggregation is node-side post-processing.
  }
  return domain;
}

export function parameterDomainForRequest(request: QueryProgramRequest): ParameterDomain {
  const domain = parameterDomain(request.input.shape);
  const binding = request.input.binding;
  // Inline-only programs consume concrete values and must not acquire a prepared-binding domain.
  if (binding.sourceShape !== undefined) {
    for (const [name, ty] of binding.paramTypes) domain.userParams.set(name, ty);
  }
  for (const [name, ty] of binding.extraUserParams) {
    const existing = domain.userParams.get(name);
    if (existing !== undefined && !structEq(existing, ty)) {
      throw runtimeGap(`binding parameter '${name}' has inconsistent validated types`);
    }
    domain.userParams.set(name, ty);
  }
  if (binding.claimParams.size === 0) return domain;

  for (const [name, param] of binding.claimParams) {
    const existing = domain.claimParams.get(name);
    if (existing !== undefined && !structEq(existing, { path: param.path, ty: param.ty })) {
      throw runtimeGap("pre-retarget claim parameter domain diverged from lowered binding sources");
    }
  }
  for (const [name, param] of binding.claimParams) {
    domain.userParams.delete(name);
    domain.claimParams.set(name, { path: param.path, ty: param.ty });
  }
  if (binding.sourceShape !== undefined) {
    // A filter equating a source field with a carried claim lowers to a binding-source join (#4017).
    const claimPaths: ClaimPath[] = [];
    for (const node of request.input.shape.nodes.valuesArray()) {
      if (node.kind === "Filter") collectEqualityFilterClaims(node.predicate, claimPaths);
    }
    for (const path of claimPaths) {
      const slot = claimBindingSlot(domain, path);
      if (slot !== undefined) domain.routingParams.add(slot);
    }
  }
  return domain;
}

/** The one binding slot carrying `path`, when a source equality against that claim can join the binding source. */
export function claimBindingSlot(domain: ParameterDomain, path: ClaimPath): string | undefined {
  const slots = [...domain.claimParams].filter(([, claim]) => structEq(claim.path, path));
  return slots.length === 1 && collectByOrderedScalar(slots[0][1].ty) ? slots[0][0] : undefined;
}

function equalitySides(predicate: PredicateExpr, each: (other: NormalizedValueRef) => void): void {
  if (predicate.kind === "And") {
    for (const child of predicate.children) equalitySides(child, each);
  } else if (predicate.kind === "Compare" && predicate.op === "Eq") {
    if (sourceValueRef(predicate.left)) each(predicate.right);
    else if (sourceValueRef(predicate.right)) each(predicate.left);
  }
}

function collectEqualityFilterClaims(predicate: PredicateExpr, claims: ClaimPath[]): void {
  equalitySides(predicate, (other) => {
    if (other.kind === "Claim") claims.push(other.path);
  });
}

function collectEqualityFilterRouteParams(
  predicate: PredicateExpr,
  routing: SortedSet<string>,
): void {
  equalitySides(predicate, (other) => {
    if (other.kind === "Param") routing.add(paramRouteField(other.name));
  });
}

const paramRouteField = (param: string): string =>
  claimPathFromParamField(param) !== undefined ? param : routeParamField(param);

export const sourceValueRef = (value: NormalizedValueRef): boolean =>
  value.kind === "SourceField" || (value.kind === "RowId" && value.row.kind === "Source");

export function collectBindingSourceParams(graph: GraphBuilder, domain: ParameterDomain): void {
  collectBindingSourceParamsWithSlots(graph, domain, new Map());
}

export function collectBindingSourceParamsWithSlots(
  graph: GraphBuilder,
  domain: ParameterDomain,
  slots: Map<number, ParameterDomain>,
): void {
  for (const node of graphBuilderPostorder(graph)) {
    if (node.kind === "TemplateInput") {
      const parameters = slots.get(node.slot);
      if (parameters) {
        for (const [name, ty] of parameters.userParams)
          domain.userParams.getOrInsert(name, () => ty);
        for (const [name, claim] of parameters.claimParams)
          domain.claimParams.getOrInsert(name, () => claim);
        for (const field of parameters.routingParams) domain.routingParams.add(field);
      }
    }
    if (node.kind !== "BindingSource") continue;
    for (const field of node.output.fields()) {
      const name: string | undefined = field.name;
      if (name === undefined) continue;
      const path = claimPathFromParamField(name);
      if (path) {
        domain.claimParams.getOrInsert(name, () => ({ path, ty: field.valueType }));
        if (collectByOrderedScalar(field.valueType)) domain.routingParams.add(name);
      } else {
        domain.userParams.getOrInsert(name, () => field.valueType);
        domain.routingParams.add(routeParamField(name));
      }
    }
  }
}

/** Lowering owns declarative sources only; mutable inputs are composed afterwards. */
export function graphContainsInputSource(graph: GraphBuilder): boolean {
  return graphBuilderPostorder(graph).some((node) => node.kind === "InputSource");
}

/** Builder nodes in child-before-parent order, each shared node once, without recursion. */
export function graphBuilderPostorder(graph: GraphBuilder): GraphBuilder[] {
  const pending: [GraphBuilder, boolean][] = [[graph, false]];
  const ordered: GraphBuilder[] = [];
  const expanded = new Set<GraphBuilder>();
  while (pending.length > 0) {
    const [node, visited] = pending.pop()!;
    if (visited) {
      ordered.push(node);
      continue;
    }
    if (expanded.has(node)) continue;
    expanded.add(node);
    pending.push([node, true]);
    const push = (child: GraphBuilder) => pending.push([child, false]);
    switch (node.kind) {
      case "TypedTemplate":
        [...node.inputs].reverse().forEach(push);
        break;
      case "TemplateInput":
        if (node.input) push(node.input);
        break;
      case "Recursive":
        if (node.stepWitness) push(node.stepWitness);
        push(node.step);
        push(node.seed);
        break;
      case "RecursiveStepWitness":
        push(node.recursive);
        break;
      case "Filter":
      case "UnwrapNullable":
      case "Unnest":
      case "VariantProject":
      case "Project":
      case "StreamingChecksum":
      case "ArgMaxBy":
      case "ArgMinBy":
      case "TopBy":
      case "CollectBy":
      case "Aggregate":
        push(node.input);
        break;
      case "Lookup":
        push(node.keys);
        break;
      case "Union":
        [...node.inputs].reverse().forEach(push);
        break;
      case "Join":
      case "SemiJoin":
      case "AntiJoin":
        push(node.right);
        push(node.left);
        break;
    }
  }
  return ordered;
}

export type QueryProgram = {
  request: QueryProgramRequest;
  lowered: LoweredGraph;
  sourceDescriptors: SortedMap<ProgramSourceId, RecordDescriptor>;
  coveredInputSourceDescriptors: SortedMap<ProgramSourceId, RecordDescriptor>;
  explain: ExplainPlan;
};

export type LoweredGraph = {
  terminals: LoweredTerminal[];
  sharedWitnessSinks: SortedMap<string, string>;
  internalAppRowsGraph?: GraphBuilder;
  parameters: ParameterDomain;
  output: ProgramOutputSchemas;
  maintainedTerminalTables: SortedMap<string, TableSchema>;
  targetedRefreshTables: SortedSet<string>;
  targetedRefreshUncertain: boolean;
};

export function executionTerminals(lowered: LoweredGraph): LoweredTerminal[] {
  return lowered.terminals.filter((t) => !lowered.sharedWitnessSinks.has(t.sink));
}

export type LoweredTerminal = { sink: string; graph: GraphBuilder; output: OutputTerminalSchema };

export type { CapabilityReport };
