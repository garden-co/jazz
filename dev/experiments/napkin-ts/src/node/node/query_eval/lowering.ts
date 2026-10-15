import { blake3, utf8, equalBytes, SortedMap, cmpStr } from "../../../support/index.js";
import type {
  NodeState,
  ProgramOutputSchemas,
  AppRowSchema,
  PreparedQueryPlan,
  QueryProgram,
  GraphBuilder,
  PreparedQueryParam,
  OutputTerminalSchema,
  RecordDescriptor,
  QueryProgramRequest,
  SourceId,
  SourceExpr,
  CurrentAccessPath,
  CurrentRow,
  SourceRequest,
  Binding,
  ValidatedQuery,
  MultisinkSubscription,
  RecordDeltas,
  PreparedShapeId,
  RootIndirectValues,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  installNodeMethods,
  debugFmt,
  routeParamField,
  cmpSourceId,
  RecordDescriptorNew,
  RoutedMultisinkTerminal,
  SubscriptionLifetime,
  TransactionWriteOverlay,
  QueryProgramCompilation,
  JazzSourceGraphPreparer,
  localUnavailablePolicyBinding,
  prepareQueryProgramSources,
  capabilityTraceEnabled,
  traceCapabilityCompile,
  coveredInputTrace,
  authorizedDeletionPreimageSourceRequest,
  emptyAuthorizedRowIdGraph,
  eprintln,
} from "../../../_extern/node-qe-b.js";
import { JAZZ_APP_ROWS_SINK, takeRequiredSinkDeltas } from "../query_eval.js";
import {
  bindingValuesForPlan,
  queryBindingSourceShapeForPreparedParams,
} from "./prepared_bindings.js";
import { PreparedClaimBindingMode } from "./bindings.js";

/** Owns an installing/first-result subscription and its prepared shape. */
class HydrationSubscription {
  constructor(
    readonly database: any,
    public subscription: MultisinkSubscription | undefined,
    public preparedShape: PreparedShapeId | undefined,
    readonly sharedShape: boolean,
  ) {}
  release(): void {
    const subscription = this.subscription;
    this.subscription = undefined;
    if (subscription) this.database.unsubscribe(subscription.id());
    const shape = this.preparedShape;
    this.preparedShape = undefined;
    if (shape !== undefined) {
      if (this.sharedShape) this.database.releaseSharedPreparedShape(shape);
      else this.database.retirePreparedShape(shape);
    }
  }
}

function appRowTerminalFields(output: ProgramOutputSchemas): string[] {
  return appRowTerminalSchema(output)
    .descriptor.fields()
    .map((field: any) => {
      if (field.name === undefined)
        throw Err.InvalidStoredValue("app row terminal field must be named");
      return field.name;
    });
}

export function appRowTerminalRouteEligibleFields(output: ProgramOutputSchemas): string[] {
  return [...appRowTerminalFields(output), ...appRowTerminalSchema(output).hiddenFields];
}

function appRowTerminalSchema(output: ProgramOutputSchemas): AppRowSchema {
  const found = output.terminals.find((t: OutputTerminalSchema) => t.kind === "AppRows");
  if (!found) throw Err.InvalidStoredValue("query program did not emit app row terminal");
  return found.rows;
}

export function materializationAppRowSchema(
  plan: PreparedQueryPlan | undefined,
  program: QueryProgram | undefined,
): AppRowSchema {
  if (plan) {
    const schema = plan.appRowSchema();
    if (schema === undefined) throw Err.InvalidStoredValue("prepared plan has no app-row schema");
    return schema;
  }
  if (!program) throw Err.InvalidStoredValue("materialization has no lowered program");
  return appRowTerminalSchema(program.lowered.output);
}

export function loweredTerminalGraph(program: QueryProgram, sink: string): GraphBuilder {
  const terminal = program.lowered.terminals.find((t: any) => t.sink === sink);
  if (!terminal) throw Err.QueryLowering(`query program did not emit sink ${sink}`);
  return terminal.graph;
}

export const loweredAppRowsGraph = (program: QueryProgram) =>
  loweredTerminalGraph(program, JAZZ_APP_ROWS_SINK);

function materializesInternalAppRowsGraph(program: QueryProgram): boolean {
  const appRows = program.request.output.appRows;
  const publishesStructuredTree =
    appRows?.projection.kind === "Tree" && appRows.projection.tree.paths.length > 0;
  const publicRootOwnsMembership = program.request.input.shape.closurePaths.some(
    (path: any) => path.kind === "ExplicitInclude" && path.rootGate !== undefined,
  );
  const existenceOnly = appRows !== undefined && !appRows.publicTerminal;
  return (
    !(existenceOnly || publishesStructuredTree || publicRootOwnsMembership) &&
    program.lowered.internalAppRowsGraph !== undefined
  );
}

export function loweredMaterializationAppRowsGraph(program: QueryProgram): GraphBuilder {
  const graph = program.lowered.internalAppRowsGraph;
  return graph !== undefined && materializesInternalAppRowsGraph(program)
    ? graph
    : loweredAppRowsGraph(program);
}

export function loweredProgramSinks(program: QueryProgram): [string, GraphBuilder][] {
  return [...program.lowered.executionTerminals()].map((t: any) => [t.sink, t.graph]);
}

export function preparedParamsFromDomain(parameters: any): PreparedQueryParam[] {
  const params: PreparedQueryParam[] = [];
  for (const [name, ty] of parameters.userParams)
    params.push({ name, ty, source: { kind: "User" } });
  for (const [name, claim] of parameters.claimParams)
    params.push({ name, ty: claim.ty, source: { kind: "Claim", path: claim.path } });
  return params;
}

const preparedParamRouteField = (param: PreparedQueryParam): string =>
  param.source.kind === "User" ? routeParamField(param.name) : param.name;

export function preparedRouteParamNames(parameters: any): string[] {
  return preparedParamsFromDomain(parameters)
    .map(preparedParamRouteField)
    .filter((field) => parameters.routingParams.has(field));
}

function preparedRouteValueIndices(params: PreparedQueryParam[], routeFields: string[]): number[] {
  return routeFields.map((routeField) => {
    const index = params.findIndex((p) => preparedParamRouteField(p) === routeField);
    if (index < 0)
      throw Err.InvalidStoredValue("terminal route fields come from the prepared parameter domain");
    return index;
  });
}

export function terminalRouteFields(
  routeParams: string[],
  routeEligibleFields: string[],
): string[] {
  const eligible = new Set(routeEligibleFields);
  return routeParams.filter((p) => eligible.has(p));
}

function terminalPublicFields(terminal: OutputTerminalSchema): string[] {
  return terminal.kind === "AppRows"
    ? descriptorFieldNames(terminal.rows.descriptor)
    : factPublicFields(terminal.fact.schema);
}

export function terminalRouteEligibleFields(terminal: OutputTerminalSchema): string[] {
  const fields = terminalPublicFields(terminal);
  if (terminal.kind === "AppRows") fields.push(...terminal.rows.hiddenFields);
  return fields;
}

const opt = (v: string | undefined): string[] => (v === undefined ? [] : [v]);

export function factPublicFields(schema: any): string[] {
  const s = schema.schema;
  switch (schema.kind) {
    case "AuthorizedRows":
      return [s.rowField, ...s.routingParamFields];
    case "ResultMembership":
      return [
        s.tableField,
        s.rowField,
        ...s.occurrenceIdFields.filter((f: string) => f !== s.rowField),
        ...s.occurrenceUnionArmFields.values(),
        ...opt(s.branchOrPrefixField),
        ...resultMembershipVersionFields(s.version),
        ...opt(s.settlePositionField),
        ...s.routingParamFields,
        ...s.payloadFields.map((f: any) => f.name),
      ];
    case "AggregateResult":
      return [
        s.synthetic.tableField,
        s.synthetic.rowField,
        ...[...s.groupKeyFields, ...s.valueFields].map((f: any) => {
          if (f.name === undefined)
            throw Err.InvalidStoredValue("lowered aggregate field is named");
          return f.name;
        }),
        ...s.routingParamFields,
      ];
    case "RelationEdges":
      return [
        ...versionedRowRefFields(s.source),
        s.pathField,
        ...versionedRowRefFields(s.target),
        s.kindField,
        ...opt(s.depthField),
        ...opt(s.edgeIdField),
        ...opt(s.branchField),
        ...opt(s.roleField),
        ...opt(s.orderField),
        ...opt(s.holeStateField),
      ];
    case "VersionWitnesses":
    case "ReplacementWitnesses": {
      const witness = s.content ?? s.deletion;
      if (!witness)
        throw Err.InvalidStoredValue("version witness fact schema has no terminal schema");
      return [...versionWitnessPublicFields(s.roleField, witness), ...s.routingParamFields];
    }
    case "ProgramSourceCoverage":
      return ["complete", ...s.routingParamFields];
    case "PathCorrelationCoverage":
      throw Err.InvalidStoredValue("path correlation coverage facts are not prepared yet");
    case "ReadFrontierSettled":
      throw Err.InvalidStoredValue("read frontier facts are not prepared yet");
    case "CompleteTxPayloadCoverage":
      throw Err.InvalidStoredValue("complete transaction coverage facts are not prepared yet");
    case "ViewCompleteExclusiveCoverage":
      throw Err.InvalidStoredValue("view-complete coverage facts are not prepared yet");
    case "PolicyDecision":
      throw Err.InvalidStoredValue("policy decision facts are not prepared yet");
    case "PolicyWitnesses":
      throw Err.InvalidStoredValue("policy witness facts are not prepared yet");
    case "ContributingMembers":
      throw Err.InvalidStoredValue("contributing member facts are not prepared yet");
    case "PredicateReads":
      throw Err.InvalidStoredValue("predicate-read facts are not prepared yet");
    case "PredicateOutputSet":
      throw Err.InvalidStoredValue("predicate output set facts are not prepared yet");
    default:
      throw Err.InvalidStoredValue("point-read facts are not prepared yet");
  }
}

export type PolicyAuthorizationGraph = {
  graph: GraphBuilder;
  routeFields: Set<string>;
  accessPaths: SortedMap<SourceId, CurrentAccessPath>;
};

type PolicyDependencyFootprint = { tables: Set<string>; uncertain: boolean };

function includeFootprint(fp: PolicyDependencyFootprint, dependency: QueryProgramRequest): void {
  for (const [source] of dependency.reads.primary.sources) fp.tables.add(source.table);
  for (const [, read] of dependency.reads.factReads)
    for (const [source] of read.sources) fp.tables.add(source.table);
}

export const policyAuthorizationGraphCacheKey = (request: QueryProgramRequest) => debugFmt(request);

export function outputRoutingFieldsForQueryEval(output: any): Set<string> {
  switch (output.schema.kind) {
    case "AuthorizedRows":
    case "ResultMembership":
    case "AggregateResult":
    case "ProgramSourceCoverage":
    case "VersionWitnesses":
    case "ReplacementWitnesses":
    case "ReadFrontierSettled":
      return new Set(output.schema.schema.routingParamFields);
  }
  return new Set();
}

function versionWitnessPublicFields(roleField: string, schema: any): string[] {
  return [
    roleField,
    schema.identity.tableField,
    schema.identity.rowField,
    "content_tx_time",
    "content_tx_node_id",
    schema.identity.txTimeField,
    schema.identity.txNodeField,
    schema.identity.schemaField,
    schema.authoredColumnsField,
    schema.createdByField,
    schema.createdAtField,
    schema.updatedByField,
    schema.updatedAtField,
    schema.deletionField,
    ...schema.userFields.values(),
    ...opt(schema.identity.branchOrPrefixField),
  ];
}

export function descriptorFieldNames(descriptor: RecordDescriptor): string[] {
  return descriptor.fields().map((field: any) => {
    if (field.name === undefined)
      throw Err.InvalidStoredValue("query-engine terminal field must be named");
    return field.name;
  });
}

export function versionedRowRefFields(schema: any): string[] {
  return [
    schema.row.sourceField,
    schema.row.tableField,
    schema.row.rowField,
    ...opt(schema.branchOrPrefixField),
    ...(schema.version ? resultMembershipVersionFields(schema.version) : []),
  ];
}

function resultMembershipVersionFields(schema: any): string[] {
  const content = [schema.content.txTimeField, schema.content.txNodeField];
  if (schema.kind === "Content") return content;
  return [...content, ...versionIdentityFields(schema.deletion), schema.deletionStateField];
}

function versionIdentityFields(schema: any): string[] {
  return [
    schema.tableField,
    schema.rowField,
    schema.txTimeField,
    schema.txNodeField,
    schema.schemaField,
    schema.layerField,
    ...opt(schema.batchIdField),
    ...opt(schema.branchOrPrefixField),
    ...opt(schema.rowDigestField),
  ];
}

const COMPILED_QUERY_PROGRAM_CACHE_MAX_ENTRIES = 32;
export const ADMISSION_HANDOFF_MAX_PROGRAMS = 128;
const MAX_ADMISSIONS = 256;

export type SupportedQueryProgram = { fingerprint: Uint8Array; program: QueryProgram | undefined };

function admissionProgramKey(
  request: QueryProgramRequest,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
): Uint8Array | undefined {
  return (request.authorizationMode === "TrustedServing" ||
    request.authorizationMode === "ClientLocal") &&
    queryProgramSourcesCacheSafe(request)
    ? blake3(utf8(queryProgramCacheKey(request, accessPaths)))
    : undefined;
}

function queryProgramSourceCacheSafe(source: SourceExpr): boolean {
  switch (source.kind) {
    case "VisibleCurrent":
      return source.data.kind === "Current";
    case "WithOverlays":
      return source.overlays.entries.length === 0 && queryProgramSourceCacheSafe(source.input);
    case "LensProject":
      return queryProgramSourceCacheSafe(source.input);
    case "Merge":
      return source.inputs.every(queryProgramSourceCacheSafe);
  }
  return false;
}

const queryProgramCacheSafe = (request: QueryProgramRequest) =>
  request.authorizationMode === "TrustedServing" && queryProgramSourcesCacheSafe(request);

function queryProgramSourcesCacheSafe(request: QueryProgramRequest): boolean {
  for (const [, s] of request.reads.primary.sources)
    if (!queryProgramSourceCacheSafe(s)) return false;
  for (const [, read] of request.reads.factReads)
    for (const [, s] of read.sources) if (!queryProgramSourceCacheSafe(s)) return false;
  return true;
}

const queryProgramCacheKey = (
  request: QueryProgramRequest,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
) => `request=${debugFmt(request)};access_paths=${debugFmt(accessPaths)}`;

const emptyPaths = () => new SortedMap<SourceId, any>(cmpSourceId);

async function ensureQueryProgramRequestSupported(
  this: NodeState,
  request: QueryProgramRequest,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
): Promise<void> {
  const key = admissionProgramKey(request, accessPaths);
  if (
    key &&
    this.query.supportedQueryProgramRequests.some((e: SupportedQueryProgram) =>
      equalBytes(e.fingerprint, key),
    )
  )
    return;
  const program = await this.compileQueryProgramRequestWithAccessPaths(request, accessPaths);
  if (key) this.rememberSupportedQueryProgram(key, program);
}

function rememberSupportedQueryProgram(
  this: NodeState,
  key: Uint8Array,
  program: QueryProgram | undefined,
): void {
  const entries: SupportedQueryProgram[] = this.query.supportedQueryProgramRequests;
  let entry = entries.find((e) => equalBytes(e.fingerprint, key));
  if (entry) {
    if (program === undefined) return;
    entry.program = undefined;
  } else {
    if (entries.length === MAX_ADMISSIONS) entries.shift();
    entries.push((entry = { fingerprint: key, program: undefined }));
  }
  if (program !== undefined) {
    if (entries.filter((e) => e.program !== undefined).length >= ADMISSION_HANDOFF_MAX_PROGRAMS) {
      const oldest = entries.find((e) => e.program !== undefined);
      if (oldest) oldest.program = undefined;
    }
    entry.program = program;
  }
}

async function compileQueryProgramRequest(
  this: NodeState,
  request: QueryProgramRequest,
): Promise<QueryProgram> {
  return this.compileQueryProgramRequestWithAccessPaths(request, emptyPaths());
}

async function compileQueryProgramRequestWithAccessPaths(
  this: NodeState,
  request: QueryProgramRequest,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
): Promise<QueryProgram> {
  const key = admissionProgramKey(request, accessPaths);
  if (key) {
    const entry = this.query.supportedQueryProgramRequests.find((e: SupportedQueryProgram) =>
      equalBytes(e.fingerprint, key),
    );
    if (entry?.program !== undefined) {
      const program = entry.program;
      entry.program = undefined;
      return program;
    }
  }
  const cacheKey = queryProgramCacheSafe(request)
    ? queryProgramCacheKey(request, accessPaths)
    : undefined;
  const cache: Map<string, QueryProgram> = this.query.compiledQueryProgramCache;
  const cached = cacheKey !== undefined ? cache.get(cacheKey) : undefined;
  if (cached) {
    if (key) this.rememberSupportedQueryProgram(key, undefined);
    return structuredClone(cached);
  }
  const program = await this.compileQueryProgramRequestWithInlineSourcesAndAccessPaths(
    request,
    emptyPaths(),
    accessPaths,
  );
  if (cacheKey !== undefined) {
    if (cache.size >= COMPILED_QUERY_PROGRAM_CACHE_MAX_ENTRIES) {
      const evict = [...cache.keys()].sort()[0];
      if (evict !== undefined) cache.delete(evict);
    }
    cache.set(cacheKey, structuredClone(program));
  }
  if (key) this.rememberSupportedQueryProgram(key, undefined);
  return program;
}

function compileQueryProgramRequestWithInlineSourcesAndAccessPaths(
  this: NodeState,
  request: QueryProgramRequest,
  inlineSources: SortedMap<SourceId, CurrentRow[]>,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
): Promise<QueryProgram> {
  return this.compileQueryProgramRequestWithInlineSourcesAccessPathsAndCoveredInputs(
    request,
    inlineSources,
    accessPaths,
    emptyPaths(),
    emptyPaths(),
  );
}

function compileQueryProgramRequestWithInlineSourcesAccessPathsAndCoveredInputs(
  this: NodeState,
  request: QueryProgramRequest,
  inlineSources: SortedMap<SourceId, CurrentRow[]>,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
  coveredInputSources: SortedMap<SourceId, GraphBuilder>,
  coveredInputDescriptors: SortedMap<SourceId, RecordDescriptor>,
): Promise<QueryProgram> {
  return this.compileQueryProgramRequestWithInlineSourcesAndAccessPathsInner(
    request,
    inlineSources,
    accessPaths,
    coveredInputSources,
    coveredInputDescriptors,
    true,
    undefined,
    TransactionWriteOverlay.default(),
  );
}

function compileQueryProgramRequestWithInlineSourcesAndTransactionOverlay(
  this: NodeState,
  request: QueryProgramRequest,
  inlineSources: SortedMap<SourceId, CurrentRow[]>,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
  transactionOverlay: any,
): Promise<QueryProgram> {
  return this.compileQueryProgramRequestWithInlineSourcesAndAccessPathsInner(
    request,
    inlineSources,
    accessPaths,
    emptyPaths(),
    emptyPaths(),
    true,
    undefined,
    transactionOverlay,
  );
}

function compileQueryProgramRequestWithSharedAccessPaths(
  this: NodeState,
  request: QueryProgramRequest,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
  boundedDeletionRegister: [SourceId, GraphBuilder] | undefined,
): Promise<QueryProgram> {
  return this.compileQueryProgramRequestWithInlineSourcesAndAccessPathsInner(
    request,
    emptyPaths(),
    accessPaths,
    emptyPaths(),
    emptyPaths(),
    false,
    boundedDeletionRegister,
    TransactionWriteOverlay.default(),
  );
}

function compileQueryProgramRequestWithBoundedDeletionRegister(
  this: NodeState,
  request: QueryProgramRequest,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
  boundedDeletionRegister: [SourceId, GraphBuilder],
): Promise<QueryProgram> {
  return this.compileQueryProgramRequestWithInlineSourcesAndAccessPathsInner(
    request,
    emptyPaths(),
    accessPaths,
    emptyPaths(),
    emptyPaths(),
    true,
    boundedDeletionRegister,
    TransactionWriteOverlay.default(),
  );
}

type Lease = { alive: boolean };

async function compileQueryProgramRequestWithInlineSourcesAndAccessPathsInner(
  this: NodeState,
  request: QueryProgramRequest,
  inlineSources: SortedMap<SourceId, CurrentRow[]>,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
  coveredInputSources: SortedMap<SourceId, GraphBuilder>,
  coveredInputDescriptors: SortedMap<SourceId, RecordDescriptor>,
  countAccessPathMetrics: boolean,
  boundedDeletionRegister: [SourceId, GraphBuilder] | undefined,
  transactionOverlay: any,
): Promise<QueryProgram> {
  this.restoreExpiredPolicyCompilationState();
  if (coveredInputTrace() && coveredInputSources.size)
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=compile_receiver_program requested_sources=${debugFmt([...request.reads.primary.sources.keysArray()])} runtime_sources=${debugFmt(coveredInputSources.keysArray())}`,
    );
  const lease: Lease = { alive: true };
  try {
    let compilation;
    try {
      compilation = QueryProgramCompilation.analyze(request);
    } catch (report) {
      throw Err.QueryCapability(debugFmt(report));
    }
    const req = compilation.request();
    const footprint: PolicyDependencyFootprint = await this.prepareQueryProgramPolicyDependencies(
      req,
      compilation.sources(),
      accessPaths,
      boundedDeletionRegister,
      lease,
    );
    const traceRequest = capabilityTraceEnabled() ? req : undefined;
    const readView = req.reads.primary;
    const resolver = new JazzSourceGraphPreparer({
      localUnavailableScope: localUnavailablePolicyBinding(req),
      node: this,
      readView,
      inlineSources,
      coveredInputSources,
      coveredInputDescriptors,
      accessPaths,
      boundedDeletionRegister,
      countAccessPathMetrics,
      currentProjectionTargets: new Map(),
      policySubplan: req.policy.kind === "AuthorizationSubplan",
      transactionOverlay,
    });
    let program: QueryProgram | undefined;
    let error: unknown;
    try {
      const [sources, explain] = await prepareQueryProgramSources(compilation, resolver);
      program = this.query.queryProgramTemplates.lower(
        compilation,
        sources,
        explain,
        (graph: GraphBuilder) => this.database.describeTemplateInput(graph),
      );
      this.scopeProgramBindingSource(program);
      for (const t of footprint.tables) program!.lowered.targetedRefreshTables.add(t);
      program!.lowered.targetedRefreshUncertain ||= footprint.uncertain;
    } catch (e) {
      error = e;
    }
    this.restoreScopedPolicyAuthorizationGraphs(lease);
    if (traceRequest)
      traceCapabilityCompile(this.nodeUuid, this.selfNodeAlias, traceRequest, program ?? error);
    if (error !== undefined) throw Err.QueryCapability(debugFmt(error));
    return program!;
  } finally {
    lease.alive = false;
  }
}

async function prepareQueryProgramPolicyDependencies(
  this: NodeState,
  request: QueryProgramRequest,
  sourceRequests: SourceRequest[],
  outerAccessPaths: SortedMap<SourceId, CurrentAccessPath>,
  boundedDeletionRegister: [SourceId, GraphBuilder] | undefined,
  lease: Lease,
): Promise<PolicyDependencyFootprint> {
  const policySourceRequests = [
    ...sourceRequests,
    ...sourceRequests
      .map(authorizedDeletionPreimageSourceRequest)
      .filter((s: any) => s !== undefined),
  ];
  const preparer = new JazzSourceGraphPreparer({
    localUnavailableScope: undefined,
    node: this,
    readView: request.reads.primary,
    inlineSources: emptyPaths(),
    coveredInputSources: emptyPaths(),
    coveredInputDescriptors: emptyPaths(),
    accessPaths: emptyPaths(),
    boundedDeletionRegister: undefined,
    countAccessPathMetrics: true,
    currentProjectionTargets: new Map(),
    policySubplan: false,
    transactionOverlay: TransactionWriteOverlay.default(),
  });
  const footprint: PolicyDependencyFootprint = { tables: new Set(), uncertain: false };
  const grouped = new SortedMap<string, [QueryProgramRequest, any, any]>(cmpStr);
  for (const source of policySourceRequests) {
    if (source.authorization.kind !== "PolicyFiltered") continue;
    const dependency = preparer.policyDependencyRequest(source);
    if (dependency === undefined) {
      footprint.uncertain = true;
      footprint.tables.add(source.source.table);
      continue;
    }
    includeFootprint(footprint, dependency);
    const path = outerAccessPaths.get(source.source);
    const register =
      boundedDeletionRegister && cmpSourceId(boundedDeletionRegister[0], source.source) === 0
        ? boundedDeletionRegister[1]
        : undefined;
    const key = policyAuthorizationGraphCacheKey(dependency);
    const entry = grouped.getOrInsert(key, () => [dependency, path, register]);
    if (debugFmt(entry[1]) !== debugFmt(path) || debugFmt(entry[2]) !== debugFmt(register)) {
      entry[1] = undefined;
      entry[2] = undefined;
    }
  }
  const emptyGraph = (): PolicyAuthorizationGraph => ({
    graph: emptyAuthorizedRowIdGraph(),
    routeFields: new Set(),
    accessPaths: emptyPaths(),
  });
  for (const [cacheKey, [dependency, path, register]] of grouped) {
    const subplan = dependency.policy.kind === "AuthorizationSubplan";
    if (subplan && path !== undefined) {
      const accessPaths = emptyPaths();
      accessPaths.set(dependency.policy.protectedSource, path);
      this.beginScopedPolicyAuthorizationGraphReplacement(cacheKey, lease);
      const boundedRegister =
        register !== undefined ? [dependency.policy.protectedSource, register] : undefined;
      try {
        const graph = await this.pointPolicyAuthorizationRowIdGraph(
          dependency,
          accessPaths,
          boundedRegister,
        );
        this.query.policyAuthorizationGraphCache.set(cacheKey, graph);
      } catch (error: any) {
        if (
          error?.kind === "QueryCapability" &&
          !String(error.message).includes("PolicyProofCycle")
        )
          this.query.policyAuthorizationGraphCache.set(cacheKey, emptyGraph());
        else {
          this.restoreScopedPolicyAuthorizationGraphs(lease);
          throw error;
        }
      }
    } else {
      try {
        await this.policyAuthorizationRowIdGraph(dependency);
      } catch (error: any) {
        if (
          error?.kind === "QueryCapability" &&
          !String(error.message).includes("PolicyProofCycle")
        ) {
          this.beginScopedPolicyAuthorizationGraphReplacement(cacheKey, lease);
          this.query.policyAuthorizationGraphCache.set(cacheKey, emptyGraph());
        } else {
          this.restoreScopedPolicyAuthorizationGraphs(lease);
          throw error;
        }
      }
    }
  }
  return footprint;
}

type Replacement = { previous: PolicyAuthorizationGraph | undefined; lease: Lease };

function restoreExpiredPolicyCompilationState(this: NodeState): void {
  this.query.policyProofStack = this.query.policyProofStack.filter((e: any) => e.lease.alive);
  const replacements: Map<string, Replacement[]> = this.query.policyAuthorizationGraphReplacements;
  const expired = [...replacements]
    .filter(([, r]) => r.length && !r[r.length - 1].lease.alive)
    .map(([k]) => k);
  for (const key of expired)
    for (;;) {
      const stack = replacements.get(key);
      if (!stack?.length || stack[stack.length - 1].lease.alive) break;
      this.restoreScopedPolicyAuthorizationGraph(key);
    }
}

function policyAuthorizationGraphCacheGet(
  this: NodeState,
  cacheKey: string,
): PolicyAuthorizationGraph | undefined {
  this.restoreExpiredPolicyCompilationState();
  return this.query.policyAuthorizationGraphCache.get(cacheKey);
}

function beginScopedPolicyAuthorizationGraphReplacement(
  this: NodeState,
  cacheKey: string,
  lease: Lease,
): void {
  this.restoreExpiredPolicyCompilationState();
  const cache: Map<string, PolicyAuthorizationGraph> = this.query.policyAuthorizationGraphCache;
  const previous = cache.get(cacheKey);
  cache.delete(cacheKey);
  const replacements: Map<string, Replacement[]> = this.query.policyAuthorizationGraphReplacements;
  let stack = replacements.get(cacheKey);
  if (!stack) replacements.set(cacheKey, (stack = []));
  stack.push({ previous, lease });
}

function restoreScopedPolicyAuthorizationGraphs(this: NodeState, lease: Lease): void {
  const replacements: Map<string, Replacement[]> = this.query.policyAuthorizationGraphReplacements;
  const keys = [...replacements]
    .filter(([, r]) => r.length && r[r.length - 1].lease === lease)
    .map(([k]) => k);
  for (const key of keys) this.restoreScopedPolicyAuthorizationGraph(key);
}

function restoreScopedPolicyAuthorizationGraph(this: NodeState, cacheKey: string): void {
  const replacements: Map<string, Replacement[]> = this.query.policyAuthorizationGraphReplacements;
  const stack = replacements.get(cacheKey);
  if (!stack) return;
  const replacement = stack.pop();
  if (!replacement) throw Err.InvalidStoredValue("policy graph replacement stack is non-empty");
  if (!stack.length) replacements.delete(cacheKey);
  const cache: Map<string, PolicyAuthorizationGraph> = this.query.policyAuthorizationGraphCache;
  cache.delete(cacheKey);
  if (replacement.previous) cache.set(cacheKey, replacement.previous);
}

async function preparedQueryPlanFromProgram(
  this: NodeState,
  program: QueryProgram,
  _shape: ValidatedQuery,
  _binding: Binding,
): Promise<PreparedQueryPlan> {
  const output = appRowTerminalSchema(program.lowered.output);
  const graph = loweredMaterializationAppRowsGraph(program);
  const appRowFields = this.materializationAppRowFields(program, graph);
  const params = preparedParamsFromDomain(program.lowered.parameters);
  const eligible = new Set(appRowTerminalRouteEligibleFields(program.lowered.output));
  const routeFields = params.map(preparedParamRouteField).filter((f) => eligible.has(f));
  const bindingDescriptor = RecordDescriptorNew(params.map((p) => [p.name, p.ty]));
  if (!params.length) return { kind: "Graph", graph, output };
  const bindingSourceShape =
    program.request.input.binding.sourceShape ?? queryBindingSourceShapeForPreparedParams(params);
  const prepared = await this.database.prepare(
    [
      new RoutedMultisinkTerminal(
        JAZZ_APP_ROWS_SINK,
        graph,
        routeFields,
        appRowFields,
      ).withRouteValueIndices(preparedRouteValueIndices(params, routeFields)),
    ],
    bindingSourceShape,
    bindingDescriptor,
  );
  return { kind: "Prepared", shape: prepared.id(), params, output };
}

function materializationAppRowFields(
  this: NodeState,
  program: QueryProgram,
  graph: GraphBuilder,
): string[] {
  return materializesInternalAppRowsGraph(program)
    ? descriptorFieldNames(this.database.graphOutputDescriptor(graph))
    : appRowTerminalFields(program.lowered.output);
}

async function subscribeLoweredProgramWithRootValues(
  this: NodeState,
  program: QueryProgram,
  binding: Binding,
  bindingSourceShape: string,
  mode: PreparedClaimBindingMode,
  progressWaker: (() => void) | undefined,
  rootIndirectValues: RootIndirectValues,
): Promise<MultisinkSubscription> {
  const [subscription] = await this.installLoweredProgramSubscription(
    program,
    binding,
    bindingSourceShape,
    mode,
    progressWaker,
    SubscriptionLifetime.Retained,
    rootIndirectValues,
    undefined,
  );
  return subscription;
}

async function installLoweredProgramSubscription(
  this: NodeState,
  program: QueryProgram,
  binding: Binding,
  bindingSourceShape: string,
  mode: PreparedClaimBindingMode,
  progressWaker: (() => void) | undefined,
  lifetime: any,
  rootIndirectValues: RootIndirectValues,
  appRowPublicFields: string[] | undefined,
): Promise<[MultisinkSubscription, PreparedShapeId | undefined]> {
  const params = preparedParamsFromDomain(program.lowered.parameters);
  const routeParams = preparedRouteParamNames(program.lowered.parameters);
  if (!params.length) {
    try {
      return [
        this.database.subscribeWithLifetimeAndRootValues(
          loweredProgramSinks(program),
          lifetime,
          rootIndirectValues,
          progressWaker,
        ),
        undefined,
      ];
    } catch (error) {
      if (coveredInputTrace())
        eprintln(
          `JAZZ_COVERED_INPUT_TRACE stage=subscribe_receiver_error error=${debugFmt(error)}`,
        );
      throw Err.Groove(error);
    }
  }
  const bindingDescriptor = RecordDescriptorNew(params.map((p) => [p.name, p.ty]));
  const values = bindingValuesForPlan(binding, params, program.request.policy, mode);
  const terminals = [...program.lowered.executionTerminals()].map((terminal: any) => {
    const publicFields =
      appRowPublicFields && terminal.sink === JAZZ_APP_ROWS_SINK
        ? appRowPublicFields
        : terminalPublicFields(terminal.output);
    const routeFields = terminalRouteFields(
      routeParams,
      terminalRouteEligibleFields(terminal.output),
    );
    return new RoutedMultisinkTerminal(
      terminal.sink,
      terminal.graph,
      routeFields,
      publicFields,
    ).withRouteValueIndices(preparedRouteValueIndices(params, routeFields));
  });
  const sharedShape = lifetime === SubscriptionLifetime.Retained;
  let prepared;
  try {
    prepared = sharedShape
      ? await this.database.prepareShared(terminals, bindingSourceShape, bindingDescriptor)
      : await this.database.prepare(terminals, bindingSourceShape, bindingDescriptor);
  } catch (error) {
    if (coveredInputTrace())
      eprintln(`JAZZ_COVERED_INPUT_TRACE stage=prepare_receiver_error error=${debugFmt(error)}`);
    throw Err.Groove(error);
  }
  const owner = new HydrationSubscription(this.database, undefined, prepared.id(), sharedShape);
  let subscription;
  try {
    subscription = await owner.database.bindShapeWithLifetimeAndRootValues(
      prepared.id(),
      values,
      lifetime,
      rootIndirectValues,
      progressWaker,
    );
  } catch (error) {
    if (coveredInputTrace())
      eprintln(`JAZZ_COVERED_INPUT_TRACE stage=bind_receiver_error error=${debugFmt(error)}`);
    try {
      owner.release();
    } catch {}
    throw Err.Groove(error);
  }
  owner.preparedShape = undefined;
  return [subscription, lifetime === SubscriptionLifetime.FirstResult ? prepared.id() : undefined];
}

async function hydrateLoweredProgramOnce(
  this: NodeState,
  program: QueryProgram,
  binding: Binding,
  rootIndirectValues: RootIndirectValues,
): Promise<RecordDeltas> {
  const graph = loweredMaterializationAppRowsGraph(program);
  const appRowPublicFields = materializesInternalAppRowsGraph(program)
    ? this.materializationAppRowFields(program, graph)
    : undefined;
  program.lowered.terminals = program.lowered.terminals.filter(
    (t: any) => t.sink === JAZZ_APP_ROWS_SINK,
  );
  program.lowered.terminals[0].graph = graph;
  const bindingSourceShape =
    program.request.input.binding.sourceShape ??
    queryBindingSourceShapeForPreparedParams(preparedParamsFromDomain(program.lowered.parameters));
  const [subscription, preparedShape] = await this.installLoweredProgramSubscription(
    program,
    binding,
    bindingSourceShape,
    PreparedClaimBindingMode.Strict,
    undefined,
    SubscriptionLifetime.FirstResult,
    rootIndirectValues,
    appRowPublicFields,
  );
  const owner = new HydrationSubscription(this.database, subscription, preparedShape, false);
  let event;
  try {
    for (;;) {
      event = subscription.pollNextEvent();
      if (event) break;
      await owner.database.pollProgress();
      event = subscription.pollNextEvent();
      if (event) break;
      await subscription.ready();
    }
  } catch (error) {
    try {
      owner.release();
    } catch {}
    throw Err.Groove(error);
  }
  owner.release();
  if (event.kind === "Error") throw Err.Groove(event.error);
  return takeRequiredSinkDeltas(event.update.deltas, JAZZ_APP_ROWS_SINK);
}

installNodeMethods({
  ensureQueryProgramRequestSupported,
  rememberSupportedQueryProgram,
  compileQueryProgramRequest,
  compileQueryProgramRequestWithAccessPaths,
  compileQueryProgramRequestWithInlineSourcesAndAccessPaths,
  compileQueryProgramRequestWithInlineSourcesAccessPathsAndCoveredInputs,
  compileQueryProgramRequestWithInlineSourcesAndTransactionOverlay,
  compileQueryProgramRequestWithSharedAccessPaths,
  compileQueryProgramRequestWithBoundedDeletionRegister,
  compileQueryProgramRequestWithInlineSourcesAndAccessPathsInner,
  prepareQueryProgramPolicyDependencies,
  restoreExpiredPolicyCompilationState,
  policyAuthorizationGraphCacheGet,
  beginScopedPolicyAuthorizationGraphReplacement,
  restoreScopedPolicyAuthorizationGraphs,
  restoreScopedPolicyAuthorizationGraph,
  preparedQueryPlanFromProgram,
  materializationAppRowFields,
  subscribeLoweredProgramWithRootValues,
  installLoweredProgramSubscription,
  hydrateLoweredProgramOnce,
});
