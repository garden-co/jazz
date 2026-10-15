import {
  SortedMap,
  SortedSet,
  cmpStr,
  concatBytes,
  utf8,
  compareBytes,
  uuidV5,
  uuidFromString,
  nowMs,
} from "../../support/index.js";
import type {
  NodeState,
  ResultMemberEntry,
  ResultMemberPayloadEntry,
  RowUuid,
  OutputOccurrenceId,
  MultisinkDeltas,
  RecordDeltas,
  BindingViewKey,
  ValidatedQuery,
  Binding,
  DurabilityTier as Tier,
  AuthorSubject,
  QueryProgram,
  QueryProgramRequest,
  ReadViewSpec,
  SourceId,
  CurrentAccessPath,
  GraphBuilder,
  CurrentRow,
  TableSchema,
  AppRowSchema,
  Query,
  SchemaVersionId,
  GlobalTime,
  Snapshot,
  OpenTransactionId,
  RelationSnapshot,
  VersionRow,
  PreparedQueryPlanHandle,
  AuthorityResultKey,
  RecordDescriptor,
  QueryReadProfile,
  RootIndirectValues as RootValues,
  BorrowedRecord,
  Value,
  Predicate,
  Operand,
  ReadViewKey,
  PreparedShapeId,
} from "../../_extern/node-qe-b.js";
import {
  Err,
  installNodeMethods,
  debugFmt,
  DurabilityTier,
  QueryAuthorizationMode,
  ReadViewSpecDefault,
  ObjectId,
  OutputOccurrenceIdSingleSource,
  rootSourceId,
  cmpSourceId,
  querySourceRequests,
  physicalGlobalCurrentTableName,
  physicalCurrentCompositeIndexName,
  BranchKey,
  StaticScanSpec,
  GraphBuilderStatic,
  PredicateExpr,
  registerStorageFieldsForQueryEngine,
  currentQueryOutputRequest,
  storageBackedMaintainedViewEligible,
  decodeCurrentRow,
  sortCurrentRows,
  RootIndirectValues,
  userColumnField,
  tableUserColumnField,
  aggregateOutputField,
  aggregateOutputColumn,
  RetainedRootWindowSource,
  RegisterShapeOptions,
  TransactionRowRecord,
  GlobalCurrentRowRecord,
  coveredInputSourceMetadata,
  currentRowDescriptorWithHiddenSourceFieldsForCurrentStorage,
  HydrationLifetime,
  MaintainedSubscriptionView,
  ResultTransitionsDefault,
  coveredInputTrace,
  eprintln,
  CurrentRowNew,
  CurrentRowNewWithPublicationFields,
  OwnedRecord,
  DescriptorField,
  RecordDescriptorNewWithFields,
  FieldIdentityName,
  AuthorSubjectStatic,
  relationUnionPresentationOrder,
  relationOutputProjectionIfPresent,
  relationUnionParts,
  relationUnionLeafProjections,
  ExclusiveSourceReads,
  cmpGlobalPhysicalTableId,
} from "../../_extern/node-qe-b.js";
import {
  PreparedClaimBindingMode,
  ParamBindingMode,
  bindingClaimParamsForShape,
  inlineSnapshotBindFilterLiterals,
  bindQueryParamsWithMode,
} from "./query_eval/bindings.js";
import {
  queryBindingSourceShapeForPartsIfNeeded,
  policyPlanCacheSignature,
  bindingValuesForPlan,
  queryBindingValueSignature,
  queryBindingSourceShapeForPreparedParams,
} from "./query_eval/prepared_bindings.js";
import {
  historicalQueryReadSet,
  snapshotQueryReadSet,
  queryReadSetForReadView,
  txQueryReadSet,
} from "./query_eval/query_read_sets.js";
import {
  aggregateOutputSchema,
  aggregateRecordFieldIndex,
} from "./query_eval/query_result_rows.js";
import {
  materializationAppRowSchema,
  loweredAppRowsGraph,
  loweredMaterializationAppRowsGraph,
  loweredProgramSinks,
  preparedParamsFromDomain,
} from "./query_eval/lowering.js";
import "./query_eval/authorization.js";
import {
  CoveredInputReceiver,
  LocalMaintainedViewSubscription,
} from "./query_eval/maintained_views.js";
import "./query_eval/materialization.js";
import "./query_eval/subscriptions.js";
import "./query_eval/subscription_watermarks.js";

export { coercePreparedBindingValue } from "./query_eval/prepared_bindings.js";
export { authorizationQueryFromReadPolicy } from "./query_eval/bindings.js";
export type { PolicyAuthorizationGraph, SupportedQueryProgram } from "./query_eval/lowering.js";

export const JAZZ_APP_ROWS_SINK = "app_rows";
const ORDERED_PAGE_PROBE_ATTEMPTS = 3;
const ORDERED_PAGE_PROBE_MAX_CAP = 4096;
const UUID_PAGE_PROBE_ATTEMPTS = 3;
const UUID_PAGE_PROBE_MAX_CAP = 4096;

type Paths = SortedMap<SourceId, CurrentAccessPath>;
const newPaths = (): Paths => new SortedMap(cmpSourceId);

const isPublicAggregateResultMember = (
  member: ResultMemberEntry,
  _resultTable: string,
  aggregateQuery: boolean,
) => aggregateQuery && member.kind === "Synthetic";

type ClientSettledBindingView = { key: BindingViewKey; retainedWindow: any };

export function replaceStaleAuthoritativeOccurrenceMember(
  resultSet: SortedSet<ResultMemberEntry>,
  resultPayloads: SortedMap<ResultMemberEntry, ResultMemberPayloadEntry>,
  authoritativeMemberAdds: SortedSet<ResultMemberEntry>,
  member: ResultMemberEntry,
  resultTable: string,
  aggregateQuery: boolean,
): void {
  if (!authoritativeMemberAdds.has(member)) return;
  const occurrenceId = publicResultMemberOccurrenceId(member, resultTable, aggregateQuery);
  if (occurrenceId === undefined) return;
  const key = debugFmt(occurrenceId);
  const replaced = resultSet.toArray().filter((candidate) => {
    if (candidate === member || debugFmt(candidate) === debugFmt(member)) return false;
    const id = publicResultMemberOccurrenceId(candidate, resultTable, aggregateQuery);
    return id !== undefined && debugFmt(id) === key;
  });
  for (const r of replaced) {
    resultSet.delete(r);
    resultPayloads.delete(r);
  }
}

export const isPublicResultMember = (
  member: ResultMemberEntry,
  resultTable: string,
  aggregateQuery: boolean,
) =>
  member.tableName() === resultTable ||
  isPublicAggregateResultMember(member, resultTable, aggregateQuery);

const OID_NAMESPACE = uuidFromString("6ba7b812-9dad-11d1-80b4-00c04fd430c8");

function u64be(n: number): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt(n));
  return b;
}

export function aggregateResultMemberRowUuid(member: ResultMemberEntry): RowUuid {
  if (member.kind !== "Synthetic")
    throw Err.InvalidStoredValue("aggregate result member must be synthetic");
  const table = utf8(member.table);
  const identity = concatBytes(
    utf8("jazz:aggregate-result:v1"),
    u64be(table.length),
    table,
    u64be(member.row.length),
    member.row,
  );
  return uuidV5(OID_NAMESPACE, identity);
}

export function publicResultMemberOccurrenceId(
  member: ResultMemberEntry,
  resultTable: string,
  aggregateQuery: boolean,
): OutputOccurrenceId | undefined {
  const occurrence = member.outputOccurrenceId();
  if (occurrence !== undefined) return occurrence;
  return isPublicAggregateResultMember(member, resultTable, aggregateQuery)
    ? OutputOccurrenceIdSingleSource(ObjectId.fromUuid(aggregateResultMemberRowUuid(member)))
    : undefined;
}

export function takeRequiredSinkDeltas(deltas: MultisinkDeltas, sink: string): RecordDeltas {
  const out = deltas.sinks.get(sink);
  if (out === undefined)
    throw Err.InvalidStoredValue("multisink subscription did not deliver required sink");
  deltas.sinks.delete(sink);
  return out;
}

export enum CurrentQueryProgramOutput {
  AppRows,
  PolicyPredicate,
  AuthorizedRows,
  RelationSnapshot,
  MaintainedView,
}

const unknownSchema = () => Err.InvalidStoredValue("query schema version is unknown");

function catalogueSchema(node: NodeState, version: SchemaVersionId): any {
  const schema = node.catalogue.catalogueSchemas.get(version);
  if (!schema) throw unknownSchema();
  return schema;
}

async function resolveTimeTravelPosition(this: NodeState, time: number): Promise<GlobalTime> {
  const raws =
    time === Number.MAX_SAFE_INTEGER
      ? await this.database.primaryKeyScanRaw("jazz_transactions", [])
      : await this.database.primaryKeyScanRangeRaw(
          "jazz_transactions",
          [
            { kind: "U64", value: 0 },
            { kind: "U64", value: 0 },
          ],
          [
            { kind: "U64", value: time + 1 },
            { kind: "U64", value: 0 },
          ],
        );
  let position = 0;
  for (const raw of raws) {
    const globalTime = raw.record().getNullableU64(TransactionRowRecord.FIELD_GLOBAL_TIME_IDX);
    if (globalTime !== undefined) position = Math.max(position, globalTime);
  }
  return position;
}

function registeredShape(this: NodeState, shapeId: any): ValidatedQuery | undefined {
  return this.query.registeredShapes.get(shapeId);
}

function compiledCoveredInputSourcesForSubscription(this: NodeState, subscription: any): any {
  const registered = this.uniqueRegisteredBindingForSubscription(subscription);
  if (!registered) throw Err.InvalidStoredValue("subscription referenced unregistered binding");
  const shape = this.query.registeredShapes.get(subscription.shapeId);
  if (!shape) throw Err.InvalidStoredValue("subscription referenced unregistered shape");
  const keys = [...shape.params().keys()];
  const binding = shape.bind(new Map(keys.map((k, i) => [k, registered.values[i]])));
  const request: QueryProgramRequest = this.currentQueryProgramRequest(
    shape,
    binding,
    registered.options.tier,
    registered.compilerIdentity,
    CurrentQueryProgramOutput.MaintainedView,
    registered.options.readView,
    registered.bindingViewKey,
    QueryAuthorizationMode.ClientLocal,
  );
  let sourceRequests;
  try {
    sourceRequests = querySourceRequests(request);
  } catch (report) {
    throw Err.QueryCapability(debugFmt(report));
  }
  const tables = new SortedSet<string>(cmpStr);
  for (const sr of sourceRequests)
    if (
      sr.visibility === "Visible" &&
      request.reads.primary.sources.get(sr.source)?.kind === "SettledBindingView"
    )
      tables.add(sr.source.table);
  return new Map(
    tables
      .toArray()
      .map((table) => [this.scopePhysicalTable(request.reads.primary.readSchema, table), table]),
  );
}

function compileCurrentQueryProgram(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
): Promise<QueryProgram> {
  return this.compileCurrentQueryProgramInAuthorizationMode(
    shape,
    binding,
    tier,
    identity,
    output,
    QueryAuthorizationMode.TrustedServing,
  );
}

function compileCurrentQueryProgramInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  mode: any,
): Promise<QueryProgram> {
  return this.compileCurrentQueryProgramWithSettledView(
    shape,
    binding,
    tier,
    identity,
    output,
    ReadViewSpecDefault(),
    undefined,
    mode,
  );
}

function compileCurrentQueryProgramForReadViewInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  readView: ReadViewSpec,
  mode: any,
): Promise<QueryProgram> {
  return this.compileCurrentQueryProgramWithSettledView(
    shape,
    binding,
    tier,
    identity,
    output,
    readView,
    undefined,
    mode,
  );
}

function compileCurrentQueryProgramWithSettledView(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  readView: ReadViewSpec,
  settledBindingView: BindingViewKey | undefined,
  mode: any,
): Promise<QueryProgram> {
  return this.compileCurrentQueryProgramWithSettledViewAndPreparedClaimMode(
    shape,
    binding,
    tier,
    identity,
    output,
    readView,
    settledBindingView,
    mode,
    PreparedClaimBindingMode.Strict,
  );
}

async function compileCurrentQueryProgramWithSettledViewAndPreparedClaimMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  readView: ReadViewSpec,
  settledBindingView: BindingViewKey | undefined,
  mode: any,
  claimMode: PreparedClaimBindingMode,
): Promise<QueryProgram> {
  const [request, accessPaths] = this.currentQueryProgramRequestAndAccessPaths(
    shape,
    binding,
    tier,
    identity,
    output,
    readView,
    settledBindingView,
    mode,
    claimMode,
  );
  return this.compileQueryProgramRequestWithAccessPaths(request, accessPaths);
}

function currentQueryProgramRequestAndAccessPaths(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  readView: ReadViewSpec,
  settledBindingView: BindingViewKey | undefined,
  mode: any,
  claimMode: PreparedClaimBindingMode,
): [QueryProgramRequest, Paths] {
  const allowSecondaryIndexes =
    output === CurrentQueryProgramOutput.MaintainedView || readView.source.kind === "BranchView";
  const request = this.currentQueryProgramRequestWithPreparedClaimMode(
    shape,
    binding,
    tier,
    identity,
    output,
    readView,
    settledBindingView,
    mode,
    claimMode,
    false,
  );
  const accessPaths: Paths = this.currentQueryPrimaryKeyAccessPaths(shape, binding);
  if (allowSecondaryIndexes)
    for (const [source, path] of this.queryProgramAccessPaths(request, true) as Paths)
      if (path.kind === "Index") accessPaths.set(source, path);
  return [request, accessPaths];
}

async function compileCurrentQueryProgramForOneShotRead(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  settledBindingView: BindingViewKey | undefined,
  mode: any,
): Promise<QueryProgram> {
  const request = this.currentQueryProgramRequest(
    shape,
    binding,
    tier,
    identity,
    CurrentQueryProgramOutput.AppRows,
    ReadViewSpecDefault(),
    settledBindingView,
    mode,
  );
  const accessPaths = this.currentQueryHydrationAccessPaths(
    request,
    shape,
    binding,
    HydrationLifetime.FirstResult,
  );
  return this.compileQueryProgramRequestWithAccessPaths(request, accessPaths);
}

async function compileUuidPageProbeProgram(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  cap: number,
): Promise<[QueryProgram, boolean] | undefined> {
  const request = this.currentQueryProgramRequest(
    shape,
    binding,
    DurabilityTier.Global,
    identity,
    CurrentQueryProgramOutput.AppRows,
    ReadViewSpecDefault(),
    undefined,
    QueryAuthorizationMode.TrustedServing,
  );
  const root = rootSourceId(shape.query().table);
  if (request.reads.primary.sourceCurrentTier(root) !== DurabilityTier.Global) return undefined;
  const accessPaths: Paths = this.currentQueryHydrationAccessPaths(
    request,
    shape,
    binding,
    HydrationLifetime.FirstResult,
  );
  if (accessPaths.has(root)) return undefined;
  accessPaths.set(root, { kind: "PrimaryKeyPage", cap });
  const [register, exhausted] = await this.boundedDeletionRegisterForUuidPage(shape, cap);
  const program = await this.compileQueryProgramRequestWithBoundedDeletionRegister(
    request,
    accessPaths,
    [root, register],
  );
  return [program, exhausted];
}

function physicalTableMapping(node: NodeState, shape: ValidatedQuery, message: string): any {
  const mapping = node.catalogue.physicalMappings
    .get(shape.schemaVersion())
    ?.tables.get(shape.query().table);
  if (!mapping) throw Err.InvalidStoredValue(message);
  return mapping;
}

async function boundedDeletionRegisterForUuidPage(
  this: NodeState,
  shape: ValidatedQuery,
  cap: number,
): Promise<[GraphBuilder, boolean]> {
  const mapping = physicalTableMapping(
    this,
    shape,
    "UUID page probe has no physical table mapping",
  );
  const contentTable = physicalGlobalCurrentTableName(mapping.tableId);
  const branch: Value = { kind: "Bytes", value: BranchKey.default().canonicalBytes() };
  const scan = StaticScanSpec.PrefixLimit([branch], cap);
  const projection = this.ensurePhysicalCurrentProjectionForEnumColumns(
    shape.schemaVersion(),
    shape.query().table,
    new Set(),
  );
  const candidates = await this.database.queryGraph(
    GraphBuilderStatic.variantSourceScan(contentTable, projection, scan).project(["row_uuid"]),
  );
  const rowUuids: Uint8Array[] = [...candidates].map(([row]: any) => row.getUuid(0));
  let exhausted = false;
  if (rowUuids.length < cap) {
    const lastRaw = await this.database.primaryKeyLastRaw(contentTable, [branch]);
    const rowUuidIndex = this.database
      .tableSchema(contentTable)
      .recordSchema()
      .fieldIndex("row_uuid");
    if (rowUuidIndex === undefined)
      throw Err.InvalidStoredValue("UUID page probe physical row ID is missing");
    const lastRawUuid = lastRaw?.record().getUuid(rowUuidIndex);
    const max = rowUuids.reduce<Uint8Array | undefined>(
      (m, u) => (m === undefined || compareBytes(u, m) > 0 ? u : m),
      undefined,
    );
    exhausted =
      lastRawUuid === undefined
        ? max === undefined
        : max !== undefined && compareBytes(lastRawUuid, max) === 0;
  }
  const register = this.physicalGlobalMarkerPageGraph(
    shape.schemaVersion(),
    shape.query().table,
    scan,
  )
    .projectFields(registerStorageFieldsForQueryEngine(""))
    .filter(PredicateExpr.isNotNull("_deletion"));
  return [register, exhausted];
}

function plainPageQuery(query: Query, orderBy: boolean): boolean {
  return (
    query.offset === 0 &&
    (orderBy ? query.orderBy.length === 1 : query.orderBy.length === 0 && !query.filters.length) &&
    query.select === undefined &&
    !query.joins.length &&
    query.flatJoin === undefined &&
    !query.policyBranches.length &&
    !query.reachable.length &&
    !query.inherits.length &&
    !query.includes.length &&
    !query.arraySubqueries.length &&
    query.aggregate === undefined &&
    query.relation === undefined
  );
}

async function tryUuidPageProbe(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
): Promise<CurrentRow[] | undefined> {
  const query = shape.query();
  const limit = query.limit;
  if (limit === undefined || limit <= 0 || limit > 1000 || !plainPageQuery(query, false))
    return undefined;
  const schema = catalogueSchema(this, shape.schemaVersion()).schema;
  const table = this.tableInSchema(query.table, shape.schemaVersion());
  let cap = limit + 1;
  const maxCap = Math.max(cap, UUID_PAGE_PROBE_MAX_CAP);
  for (let attempt = 0; attempt < UUID_PAGE_PROBE_ATTEMPTS; attempt++) {
    const probeQuery = query.clone();
    probeQuery.limit = cap;
    const probeShape = probeQuery.validateWithSchemaVersion(schema, shape.schemaVersion());
    const probeBinding = probeShape.bind(new Map(binding.values()));
    const probe = await this.compileUuidPageProbeProgram(probeShape, probeBinding, identity, cap);
    if (!probe) return undefined;
    const [program, exhausted] = probe;
    const appOutput = materializationAppRowSchema(undefined, program);
    const rootValues = this.projectionDroppedRootValues(probeQuery, shape.schemaVersion());
    const deltas = await this.hydrateLoweredProgramOnce(program, probeBinding, rootValues);
    const rows: CurrentRow[] = this.materializeAndFinalizeQueryRows(
      probeQuery,
      shape.schemaVersion(),
      table,
      appOutput,
      deltas,
      undefined,
    );
    if (rows.length > limit || exhausted) return rows.slice(0, limit);
    if (attempt + 1 === UUID_PAGE_PROBE_ATTEMPTS || cap >= maxCap) break;
    cap = Math.min(cap * 4, maxCap);
  }
  return undefined;
}

async function compileOrderedPageProbeProgram(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  equalityColumn: string,
  cap: number,
): Promise<[QueryProgram, boolean] | undefined> {
  const request = this.currentQueryProgramRequest(
    shape,
    binding,
    DurabilityTier.Global,
    identity,
    CurrentQueryProgramOutput.AppRows,
    ReadViewSpecDefault(),
    undefined,
    QueryAuthorizationMode.TrustedServing,
  );
  const root = rootSourceId(shape.query().table);
  if (request.reads.primary.sourceCurrentTier(root) !== DurabilityTier.Global) return undefined;
  const accessPaths: Paths = this.currentQueryHydrationAccessPaths(
    request,
    shape,
    binding,
    HydrationLifetime.FirstResult,
  );
  const path = accessPaths.get(root);
  if (path?.kind !== "Index" || path.candidateFilter !== undefined) return undefined;
  if (
    path.column !== equalityColumn ||
    path.orderColumn !== undefined ||
    path.prefix.length !== 1 ||
    path.intersections.length ||
    path.sourceLimit !== undefined
  )
    return undefined;
  const order = shape.query().orderBy[0];
  path.orderColumn = order.column;
  path.reverse = order.direction === "Desc";
  path.sourceLimit = cap;
  path.maintained = false;
  const [register, exhausted] = await this.boundedDeletionRegisterForOrderedPage(shape, path, cap);
  const program = await this.compileQueryProgramRequestWithBoundedDeletionRegister(
    request,
    accessPaths,
    [root, register],
  );
  return [program, exhausted];
}

async function boundedDeletionRegisterForOrderedPage(
  this: NodeState,
  shape: ValidatedQuery,
  path: CurrentAccessPath,
  cap: number,
): Promise<[GraphBuilder, boolean]> {
  if (path.kind !== "Index" || path.orderColumn === undefined)
    throw Err.InvalidStoredValue("ordered page probe requires a composite index");
  if (path.intersections.length)
    throw Err.InvalidStoredValue("ordered page probe cannot intersect indexes");
  const mapping = physicalTableMapping(
    this,
    shape,
    "ordered page probe has no physical table mapping",
  );
  const columnId = mapping.columns.get(path.column);
  if (columnId === undefined)
    throw Err.InvalidStoredValue("ordered page probe has no equality column mapping");
  const orderColumnId = mapping.columns.get(path.orderColumn);
  if (orderColumnId === undefined)
    throw Err.InvalidStoredValue("ordered page probe has no order column mapping");
  const contentTable = physicalGlobalCurrentTableName(mapping.tableId);
  const index = physicalCurrentCompositeIndexName([columnId, orderColumnId]);
  const branch: Value = { kind: "Bytes", value: BranchKey.default().canonicalBytes() };
  const scanPrefix = [branch, ...path.prefix];
  const scan = path.reverse
    ? StaticScanSpec.ReversePrefixLimit(scanPrefix, cap)
    : StaticScanSpec.PrefixLimit(scanPrefix, cap);
  const projection = this.ensurePhysicalCurrentProjectionForEnumColumns(
    shape.schemaVersion(),
    shape.query().table,
    new Set(),
  );
  const candidates = await this.database.queryGraph(
    GraphBuilderStatic.variantIndexScan(contentTable, index, projection, scan).project([
      "row_uuid",
    ]),
  );
  const rowUuids = [...candidates].map(([row]: any) => row.getUuid(0));
  const exhausted =
    rowUuids.length < cap &&
    (await this.database.queryGraph(GraphBuilderStatic.indexScan(contentTable, index, scan))).deltas
      .length < cap;
  const register = this.physicalGlobalMarkerIndexGraph(
    shape.schemaVersion(),
    shape.query().table,
    index,
    scan,
  )
    .projectFields(registerStorageFieldsForQueryEngine(""))
    .filter(PredicateExpr.isNotNull("_deletion"));
  return [register, exhausted];
}

const ORDERED_PROBE_TYPES = new Set([
  "U8",
  "U16",
  "U32",
  "U64",
  "I32",
  "I64",
  "Bool",
  "String",
  "Bytes",
  "Uuid",
]);

async function tryOrderedPageProbe(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
): Promise<CurrentRow[] | undefined> {
  const query = shape.query();
  const limit = query.limit;
  if (limit === undefined || limit <= 0 || !plainPageQuery(query, true)) return undefined;
  const orderColumn = query.orderBy[0].column;
  const table: TableSchema = this.tableInSchema(query.table, shape.schemaVersion());
  if (
    !table.columns.some(
      (c: any) => c.name === orderColumn && ORDERED_PROBE_TYPES.has(c.columnType.kind),
    )
  )
    return undefined;
  const paths: Paths = this.oneShotAccessPaths(shape, binding, DurabilityTier.Global);
  const rootPath = paths.get(rootSourceId(query.table));
  if (rootPath?.kind !== "Index") return undefined;
  const column = rootPath.column;
  if (
    rootPath.intersections.length ||
    !table.compositeIndexes.some(
      (ix: string[]) => ix.length === 2 && ix[0] === column && ix[1] === orderColumn,
    )
  )
    return undefined;
  const policy = table.readPolicy;
  if (
    policy &&
    !policy.policyBranches.length &&
    policy.filters.some((filter: Predicate) => {
      if (filter.kind !== "Eq") return false;
      const claimColumn =
        filter.left.kind === "Column" && filter.right.kind === "Claim"
          ? filter.left.name
          : filter.left.kind === "Claim" && filter.right.kind === "Column"
            ? filter.right.name
            : undefined;
      return claimColumn !== undefined && claimColumn !== column;
    })
  )
    return undefined;
  const schema = catalogueSchema(this, shape.schemaVersion()).schema;
  let cap = limit + 1;
  const maxCap = Math.max(cap, ORDERED_PAGE_PROBE_MAX_CAP);
  for (let attempt = 0; attempt < ORDERED_PAGE_PROBE_ATTEMPTS; attempt++) {
    const probeQuery = query.clone();
    probeQuery.limit = cap;
    const probeShape = probeQuery.validateWithSchemaVersion(schema, shape.schemaVersion());
    const probeBinding = probeShape.bind(new Map(binding.values()));
    const probe = await this.compileOrderedPageProbeProgram(
      probeShape,
      probeBinding,
      identity,
      column,
      cap,
    );
    if (!probe) return undefined;
    const [program, exhausted] = probe;
    const appOutput = materializationAppRowSchema(undefined, program);
    const rootValues = this.projectionDroppedRootValues(probeQuery, shape.schemaVersion());
    const deltas = await this.hydrateLoweredProgramOnce(program, probeBinding, rootValues);
    const rows: CurrentRow[] = this.materializeAndFinalizeQueryRows(
      probeQuery,
      shape.schemaVersion(),
      table,
      appOutput,
      deltas,
      undefined,
    );
    const strictlyWorseRow =
      rows.length > limit &&
      debugFmt(queryOrderValue(rows[limit - 1], table, orderColumn)) !==
        debugFmt(queryOrderValue(rows[rows.length - 1], table, orderColumn));
    if (strictlyWorseRow || exhausted) return rows.slice(0, limit);
    if (attempt + 1 === ORDERED_PAGE_PROBE_ATTEMPTS || cap >= maxCap) break;
    cap = Math.min(cap * 4, maxCap);
  }
  return undefined;
}

function compileCurrentQueryProgramWithAccessPaths(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  accessPaths: Paths,
  mode: any,
): Promise<QueryProgram> {
  const request = this.currentQueryProgramRequestWithInlineBindingSource(
    shape,
    binding,
    tier,
    identity,
    output,
    ReadViewSpecDefault(),
    mode,
  );
  return this.compileQueryProgramRequestWithAccessPaths(request, accessPaths);
}

function rowSetProgramInput(
  node: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  inputShape: any,
) {
  const claims = bindingClaimParamsForShape(inputShape, shape.params());
  return {
    binding: node.programBindingForShape(
      shape,
      binding,
      queryBindingSourceShapeForPartsIfNeeded(shape.params(), claims),
      new Map(),
      claims,
    ),
    shape: inputShape,
  };
}

function compileHistoricalQueryProgram(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  position: GlobalTime,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
): Promise<QueryProgram> {
  catalogueSchema(this, shape.schemaVersion());
  const input = rowSetProgramInput(
    this,
    shape,
    binding,
    this.normalizedRowSetShape(shape, binding),
  );
  return this.compileQueryProgramRequest({
    authorizationMode: QueryAuthorizationMode.TrustedServing,
    reads: historicalQueryReadSet(input.shape, shape.schemaVersion(), position),
    policy: this.queryProgramPolicyContext(identity),
    input,
    output: currentQueryOutputRequest(output, shape.query()),
  });
}

function compileSnapshotQueryProgram(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  snapshot: Snapshot,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
): Promise<QueryProgram> {
  catalogueSchema(this, shape.schemaVersion());
  const input = rowSetProgramInput(
    this,
    shape,
    binding,
    this.normalizedRowSetShape(shape, binding),
  );
  return this.compileQueryProgramRequest({
    authorizationMode: QueryAuthorizationMode.TrustedServing,
    reads: snapshotQueryReadSet(input.shape, shape.schemaVersion(), snapshot),
    policy: this.queryProgramPolicyContext(identity),
    input,
    output: currentQueryOutputRequest(output, shape.query()),
  });
}

function compileIncludeDeletedQueryProgramInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  mode: any,
  readView: ReadViewSpec,
  physicalRow: RowUuid | undefined,
): Promise<QueryProgram> {
  catalogueSchema(this, shape.schemaVersion());
  const input = rowSetProgramInput(
    this,
    shape,
    binding,
    this.normalizedIncludeDeletedRowSetShape(shape, binding),
  );
  const request = {
    authorizationMode: mode,
    reads: queryReadSetForReadView(
      input.shape,
      shape.schemaVersion(),
      this.readPolicySchemaForTableName(shape.query().table, shape.schemaVersion(), input.shape),
      tier,
      readView,
      undefined,
      undefined,
      this.catalogue.schema,
    ),
    policy: this.queryProgramPolicyContext(identity),
    input,
    output: currentQueryOutputRequest(CurrentQueryProgramOutput.AppRows, shape.query()),
  };
  let accessPaths: Paths;
  if (physicalRow !== undefined) {
    accessPaths = newPaths();
    accessPaths.set(rootSourceId(shape.query().table), {
      kind: "PrimaryKey",
      values: [{ kind: "Uuid", value: physicalRow }],
    });
  } else accessPaths = this.oneShotAccessPaths(shape, binding, tier);
  return this.compileQueryProgramRequestWithAccessPaths(request, accessPaths);
}

function compileOpenTxQueryProgram(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  includeDeleted: boolean,
  mode: any,
): Promise<QueryProgram> {
  const snapshot = this.openTx(txId).baseSnapshot;
  const readSchema = catalogueSchema(this, shape.schemaVersion());
  const lowered = inlineSnapshotBindFilterLiterals(shape, binding, readSchema.schema);
  const loweredBinding = lowered.bind(new Map());
  catalogueSchema(this, lowered.schemaVersion());
  const inputShape = includeDeleted
    ? this.normalizedIncludeDeletedRowSetShape(lowered, loweredBinding)
    : this.normalizedRowSetShape(lowered, loweredBinding);
  const input = rowSetProgramInput(this, lowered, loweredBinding, inputShape);
  return this.compileQueryProgramRequest({
    authorizationMode: mode,
    reads: txQueryReadSet(input.shape, lowered.schemaVersion(), txId, snapshot),
    policy: this.queryProgramPolicyContext(identity),
    input,
    output: currentQueryOutputRequest(output, lowered.query()),
  });
}

function currentQueryProgramRequest(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  readView: ReadViewSpec,
  settledBindingView: BindingViewKey | undefined,
  mode: any,
): QueryProgramRequest {
  return this.currentQueryProgramRequestWithPreparedClaimMode(
    shape,
    binding,
    tier,
    identity,
    output,
    readView,
    settledBindingView,
    mode,
    PreparedClaimBindingMode.Strict,
    false,
  );
}

function currentQueryProgramRequestWithInlineBindingSource(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  readView: ReadViewSpec,
  mode: any,
): QueryProgramRequest {
  return this.currentQueryProgramRequestWithPreparedClaimMode(
    shape,
    binding,
    tier,
    identity,
    output,
    readView,
    undefined,
    mode,
    PreparedClaimBindingMode.Strict,
    true,
  );
}

function storageBackedMaintainedRootHasIdentityTableMapping(
  this: NodeState,
  schemaVersion: SchemaVersionId,
  table: string,
): boolean {
  if (schemaVersion !== this.catalogue.localSchemaVersionId) return false;
  const tableId = this.catalogue.physicalMappings.get(schemaVersion)?.tables.get(table)?.tableId;
  if (tableId === undefined) return false;
  for (const mapping of this.catalogue.physicalMappings.values())
    for (const [name, candidate] of mapping.tables)
      if (candidate.tableId === tableId && name !== table) return false;
  return true;
}

function currentQueryProgramRequestWithPreparedClaimMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  readView: ReadViewSpec,
  settledBindingView: BindingViewKey | undefined,
  mode: any,
  claimMode: PreparedClaimBindingMode,
  forceInlineBindingSource: boolean,
): QueryProgramRequest {
  const policy = this.queryProgramPolicyContext(identity);
  const requestedShapeId = shape.shapeId();
  const stripsPolicyBranches =
    policy.kind === "System" || mode === QueryAuthorizationMode.ClientLocal;
  if (stripsPolicyBranches) {
    const stripped = this.policyStrippedShape(shape, binding);
    if (stripped) [shape, binding] = stripped;
  }
  const clientLocal = mode === QueryAuthorizationMode.ClientLocal;
  const clientLocalPrepared =
    clientLocal &&
    tier === DurabilityTier.Local &&
    readView.isDefault() &&
    output === CurrentQueryProgramOutput.MaintainedView;
  const usePrepared =
    (!clientLocal || clientLocalPrepared) &&
    !forceInlineBindingSource &&
    this.canUsePreparedCurrentQueryPlan(shape) &&
    settledBindingView === undefined &&
    output !== CurrentQueryProgramOutput.RelationSnapshot;
  if (!usePrepared) {
    const readSchema = catalogueSchema(this, shape.schemaVersion());
    shape = inlineSnapshotBindFilterLiterals(shape, binding, readSchema.schema);
    binding = shape.bind(new Map());
  }
  const inputShape = this.normalizedRowSetShape(shape, binding);
  if (
    settledBindingView !== undefined &&
    debugFmt(settledBindingView.shapeId) === debugFmt(requestedShapeId) &&
    shape.query().aggregate === undefined
  ) {
    const root = inputShape.nodes.get(inputShape.root);
    if (root?.kind === "Slice") root.offset = 0;
  }
  const policySchemaVersion = this.readPolicySchemaForTableName(
    shape.query().table,
    shape.schemaVersion(),
    inputShape,
  );
  let claimParams = bindingClaimParamsForShape(inputShape, shape.params());
  if (usePrepared && !clientLocal) {
    let policySchema;
    if (policySchemaVersion === this.catalogue.activeSchema.schema)
      policySchema = this.catalogue.activeSchema.compiled;
    else if (policySchemaVersion === this.catalogue.localSchemaVersionId)
      policySchema = this.catalogue.schema;
    else {
      const entry = this.catalogue.catalogueSchemas.get(policySchemaVersion);
      if (!entry) throw Err.InvalidStoredValue("policy schema version is unknown");
      policySchema = entry.schema;
    }
    this.collectPolicyDependencyClaimParams(policySchema, policy, inputShape, claimParams);
  }
  if (policy.kind === "System") claimParams = new SortedMap(cmpStr);
  let sourceShape = usePrepared
    ? queryBindingSourceShapeForPartsIfNeeded(shape.params(), claimParams)
    : undefined;
  if (sourceShape !== undefined && clientLocal) {
    const scope = this.activeSessionClaimScopeKey(identity);
    if (scope !== undefined) sourceShape = `${sourceShape}:session:${scope}`;
  }
  if (coveredInputTrace())
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=program_scope identity=${debugFmt(identity)} mode=${debugFmt(mode)} prepared=${usePrepared} source_shape=${debugFmt(sourceShape)} strips_policy_branches=${stripsPolicyBranches} query_policy_branches=${shape.query().policyBranches.length} query_includes=${shape.query().includes.length} policy=${debugFmt(policy)}`,
    );
  const querySchema =
    shape.schemaVersion() === this.catalogue.activeSchema.schema
      ? this.catalogue.activeSchema.compiled
      : catalogueSchema(this, shape.schemaVersion()).schema;
  const rootHasReadPolicy =
    this.tableInSchemaRef(shape.query().table, shape.schemaVersion()).readPolicy !== undefined;
  const storageBacked =
    output === CurrentQueryProgramOutput.MaintainedView &&
    (mode !== QueryAuthorizationMode.ClientLocal || tier === DurabilityTier.Local) &&
    settledBindingView === undefined &&
    !rootHasReadPolicy &&
    this.storageBackedMaintainedRootHasIdentityTableMapping(
      shape.schemaVersion(),
      shape.query().table,
    ) &&
    storageBackedMaintainedViewEligible(shape.query(), tier, readView, inputShape);
  const programBinding = this.programBindingForShapeAndPolicyWithPreparedClaimMode(
    shape,
    binding,
    sourceShape,
    new Map(),
    claimParams,
    policy,
    claimMode,
  );
  if (clientLocal && programBinding.sourceShape !== undefined)
    programBinding.sourceShape += ":client-local";
  const input = { binding: programBinding, shape: inputShape };
  const outputRequest = currentQueryOutputRequest(output, shape.query());
  if (storageBacked) outputRequest.facts.delete("VersionWitnesses");
  return {
    authorizationMode: mode,
    reads: queryReadSetForReadView(
      inputShape,
      shape.schemaVersion(),
      policySchemaVersion,
      tier,
      readView,
      settledBindingView,
      undefined,
      querySchema,
    ),
    policy,
    input,
    output: outputRequest,
  };
}

function queryRowsWithPreparedPlan(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  plan: PreparedQueryPlanHandle | undefined,
): Promise<CurrentRow[]> {
  return this.queryRowsWithPreparedPlanForIdentity(
    shape,
    binding,
    tier,
    plan,
    AuthorSubjectStatic.SYSTEM,
  );
}

function queryRowsWithPreparedPlanForIdentity(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  plan: PreparedQueryPlanHandle | undefined,
  identity: AuthorSubject,
): Promise<CurrentRow[]> {
  return this.queryRowsWithOptionsForIdentity(
    shape,
    binding,
    tier,
    plan,
    identity,
    false,
    QueryAuthorizationMode.TrustedServing,
    undefined,
  );
}

function queryRowsForClient(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
): Promise<CurrentRow[]> {
  return this.queryRowsWithOptionsForIdentity(
    shape,
    binding,
    tier,
    undefined,
    identity,
    false,
    QueryAuthorizationMode.ClientLocal,
    undefined,
  );
}

function allocateClientReceiverInputSources(
  this: NodeState,
  request: QueryProgramRequest,
): [Paths, SortedMap<SourceId, RecordDescriptor>, SortedMap<any, any>] {
  const runtimeSources = newPaths() as SortedMap<SourceId, any>;
  const runtimeDescriptors = new SortedMap<SourceId, RecordDescriptor>(cmpSourceId);
  const sources = new SortedMap<any, any>(cmpGlobalPhysicalTableId);
  const tableMetadata = new SortedMap<string, Map<string, any>>(cmpStr);
  const occurrences: [SourceId, RecordDescriptor][] = [];
  const readSchema = request.reads.primary.readSchema;
  let sourceRequests;
  try {
    sourceRequests = querySourceRequests(request);
  } catch (report) {
    throw Err.QueryCapability(debugFmt(report));
  }
  for (const sr of sourceRequests) {
    if (sr.visibility !== "Visible") continue;
    let expression = request.reads.primary.sources.get(sr.source);
    if (!expression) continue;
    if (
      expression.kind === "WithOverlays" &&
      expression.overlays.entries.length === 1 &&
      expression.overlays.entries[0].kind === "PendingLocal"
    )
      expression = expression.input;
    if (expression.kind !== "SettledBindingView") continue;
    const table = this.tableInSchemaRef(sr.source.table, readSchema);
    const metadata = coveredInputSourceMetadata(sr.requirements, table);
    const descriptor = currentRowDescriptorWithHiddenSourceFieldsForCurrentStorage(table, metadata);
    const merged = tableMetadata.getOrInsert(sr.source.table, () => new Map());
    for (const [k, v] of metadata) merged.set(k, v);
    occurrences.push([sr.source, descriptor]);
  }
  for (const [tableName, metadata] of tableMetadata) {
    const table = this.tableInSchemaRef(tableName, readSchema);
    const descriptor = currentRowDescriptorWithHiddenSourceFieldsForCurrentStorage(table, metadata);
    const id = this.database.allocateInputSource(descriptor);
    sources.set(this.scopePhysicalTable(readSchema, tableName), {
      id,
      descriptor,
      table: tableName,
    });
  }
  for (const [occurrence, descriptor] of occurrences) {
    const shared = sources.get(this.scopePhysicalTable(readSchema, occurrence.table));
    let graph = GraphBuilderStatic.inputSource(shared.id, shared.descriptor);
    if (!shared.descriptor.equals(descriptor))
      graph = graph.project(
        descriptor.fields().map((f: any) => {
          if (f.name === undefined) throw Err.InvalidStoredValue("covered input fields are named");
          return f.name;
        }),
      );
    runtimeSources.set(occurrence, graph);
    runtimeDescriptors.set(occurrence, descriptor);
  }
  return [runtimeSources, runtimeDescriptors, sources];
}

async function compileClientOneShotWithCoveredInputs(
  this: NodeState,
  request: QueryProgramRequest,
  accessPaths: Paths,
  resultSchemaVersion: SchemaVersionId,
  readView: ReadViewSpec,
  authorityResultKey: AuthorityResultKey,
): Promise<[QueryProgram, any] | undefined> {
  const [runtimeSources, runtimeDescriptors, sources] =
    this.allocateClientReceiverInputSources(request);
  if (!sources.size)
    throw Err.InvalidStoredValue("client settled read has no covered-input source occurrences");
  let program;
  try {
    program = await this.compileQueryProgramRequestWithInlineSourcesAccessPathsAndCoveredInputs(
      request,
      newPaths(),
      accessPaths,
      runtimeSources,
      runtimeDescriptors,
    );
  } catch (error) {
    await this.retireCoveredInputSources(sources);
    throw error;
  }
  const receiver = new CoveredInputReceiver(sources, readView);
  let installed;
  try {
    installed = await this.replaceCoveredInputReceiver(
      receiver,
      resultSchemaVersion,
      authorityResultKey,
    );
  } catch (error) {
    await this.retireCoveredInputSources(receiver.sources);
    throw error;
  }
  if (!installed) {
    await this.retireCoveredInputSources(receiver.sources);
    return undefined;
  }
  return [program, receiver];
}

async function retireCoveredInputSources(
  this: NodeState,
  sources: SortedMap<any, any>,
): Promise<void> {
  try {
    await this.database.retireInputSources([...sources.valuesArray()].map((s) => s.id));
  } catch (error) {
    throw Err.Groove(error);
  }
}

function queryRowsLocalPreview(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  plan: PreparedQueryPlanHandle | undefined,
): Promise<CurrentRow[]> {
  return this.queryRowsWithPreparedPlan(shape, binding, DurabilityTier.Local, plan);
}

function queryRowsLocalPreviewProfiled(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  plan: PreparedQueryPlanHandle | undefined,
) {
  return this.queryRowsWithOptionsForIdentityProfiled(
    shape,
    binding,
    DurabilityTier.Local,
    plan,
    AuthorSubjectStatic.SYSTEM,
  );
}

function queryRowsIncludingDeletedInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  plan: PreparedQueryPlanHandle | undefined,
  identity: AuthorSubject,
  mode: any,
): Promise<CurrentRow[]> {
  return this.queryRowsWithOptionsForIdentity(
    shape,
    binding,
    tier,
    plan,
    identity,
    true,
    mode,
    undefined,
  );
}

async function queryRowsWithOptionsForIdentity(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  _plan: PreparedQueryPlanHandle | undefined,
  identity: AuthorSubject,
  includeDeleted: boolean,
  mode: any,
  profile: QueryReadProfile | undefined,
): Promise<CurrentRow[]> {
  let started = profile ? nowMs() : 0;
  if (includeDeleted) {
    const rows = await this.queryRowsIncludingDeletedWithQueryEngine(
      shape,
      binding,
      tier,
      identity,
      mode,
      ReadViewSpecDefault(),
    );
    this.finishEngineQueryRowsInSchema(shape.query(), shape.schemaVersion(), rows);
    this.applyProjectionInSchema(shape.query(), shape.schemaVersion(), rows);
    return rows;
  }
  if (mode === QueryAuthorizationMode.TrustedServing && tier === DurabilityTier.Global) {
    const rows =
      (await this.tryUuidPageProbe(shape, binding, identity)) ??
      (await this.tryOrderedPageProbe(shape, binding, identity));
    if (rows) return rows;
  }
  const clientView: ClientSettledBindingView | undefined =
    mode === QueryAuthorizationMode.ClientLocal
      ? this.clientSettledBindingViewForQuery(shape, binding, tier, ReadViewSpecDefault())
      : undefined;
  const settledBindingView =
    mode === QueryAuthorizationMode.ClientLocal ? clientView?.key : undefined;
  if (
    mode === QueryAuthorizationMode.ClientLocal &&
    tier >= DurabilityTier.Global &&
    settledBindingView === undefined
  )
    return [];
  const settledAuthorityResultKey =
    mode === QueryAuthorizationMode.ClientLocal && settledBindingView !== undefined
      ? this.uniqueAuthorityResultKeyForBindingView(settledBindingView)
      : undefined;
  if (
    mode === QueryAuthorizationMode.ClientLocal &&
    settledBindingView !== undefined &&
    settledAuthorityResultKey === undefined
  )
    return [];
  if (clientView?.retainedWindow) {
    const schema = catalogueSchema(this, shape.schemaVersion());
    const rebased = shape.query().clone();
    [rebased.offset, rebased.limit] = clientView.retainedWindow.relativeWindowFor(shape);
    const rebasedShape = rebased.validateWithSchemaVersion(schema.schema, shape.schemaVersion());
    binding = rebasedShape.bind(new Map(binding.values()));
    shape = rebasedShape;
  }
  let ephemeral: any;
  if (profile) {
    profile.resolveView = nowMs() - started;
    started = nowMs();
  }
  let program: QueryProgram;
  if (settledAuthorityResultKey !== undefined) {
    const request = this.currentQueryProgramRequest(
      shape,
      binding,
      tier,
      identity,
      CurrentQueryProgramOutput.AppRows,
      ReadViewSpecDefault(),
      settledBindingView,
      mode,
    );
    const accessPaths = this.currentQueryHydrationAccessPaths(
      request,
      shape,
      binding,
      HydrationLifetime.FirstResult,
    );
    const compiled = await this.compileClientOneShotWithCoveredInputs(
      request,
      accessPaths,
      shape.schemaVersion(),
      ReadViewSpecDefault(),
      settledAuthorityResultKey,
    );
    if (!compiled) return [];
    [program, ephemeral] = compiled;
  } else
    program = await this.compileCurrentQueryProgramForOneShotRead(
      shape,
      binding,
      tier,
      identity,
      settledBindingView,
      mode,
    );
  const appOutput = materializationAppRowSchema(undefined, program);
  if (profile) {
    profile.compileProgram = nowMs() - started;
    started = nowMs();
  }
  const rootValues = this.projectionDroppedRootValues(shape.query(), shape.schemaVersion());
  let deltas, failure: unknown;
  try {
    deltas = await this.hydrateLoweredProgramOnce(program, binding, rootValues);
  } catch (e) {
    failure = e;
  }
  if (ephemeral) await this.retireCoveredInputSources(ephemeral.sources);
  if (failure !== undefined) throw failure;
  if (profile) profile.executePlan = nowMs() - started;
  const tableSchema = this.queryOutputTable(shape.query(), shape.schemaVersion());
  return this.materializeAndFinalizeQueryRows(
    shape.query(),
    shape.schemaVersion(),
    tableSchema,
    appOutput,
    deltas,
    profile,
  );
}

function projectionDroppedRootValues(
  this: NodeState,
  query: Query,
  schemaVersion: SchemaVersionId,
): RootValues {
  const selected = query.select;
  if (
    selected === undefined ||
    query.aggregate !== undefined ||
    query.relation !== undefined ||
    query.flatJoin !== undefined ||
    query.arraySubqueries.length
  )
    return RootIndirectValues.Materialize;
  const table = this.tableInSchema(query.table, schemaVersion);
  const dropped = new Set<string>(
    table.columns
      .filter(
        (c: any) =>
          !selected.includes(c.name) && !query.orderBy.some((o: any) => o.column === c.name),
      )
      .map((c: any) => userColumnField(c.name)),
  );
  return dropped.size ? RootIndirectValues.PhysicalFields(dropped) : RootIndirectValues.Materialize;
}

function maintainedProjectionDroppedRootValues(
  this: NodeState,
  query: Query,
  schemaVersion: SchemaVersionId,
): RootValues {
  const values = this.projectionDroppedRootValues(query, schemaVersion);
  if (values.kind !== "PhysicalFields") return RootIndirectValues.Materialize;
  const table = this.tableInSchema(query.table, schemaVersion);
  const dropped = new Set<string>(values.fields);
  for (const c of table.columns)
    if (values.fields.has(userColumnField(c.name)))
      dropped.add(tableUserColumnField(table.name, c.name));
  return RootIndirectValues.PhysicalFields(dropped);
}

function materializeAndFinalizeQueryRows(
  this: NodeState,
  query: Query,
  schemaVersion: SchemaVersionId,
  tableSchema: TableSchema,
  appOutput: AppRowSchema,
  deltas: RecordDeltas,
  profile: QueryReadProfile | undefined,
): CurrentRow[] {
  let started = profile ? nowMs() : 0;
  let rows: CurrentRow[];
  if (query.aggregate !== undefined)
    rows = this.materializeAggregateQueryRows(query, appOutput, deltas);
  else if (query.flatJoin !== undefined)
    rows = [...deltas]
      .filter(([, w]: any) => w > 0)
      .map(([record]: any) =>
        CurrentRowNew(query.table, new OwnedRecord(record.raw().slice(), record.descriptor())),
      );
  else {
    rows = [];
    for (const [record, weight] of deltas)
      if (weight > 0)
        rows.push(this.materializeCurrentRow(tableSchema, decodeCurrentRow(tableSchema, record)));
  }
  if (query.aggregate === undefined)
    for (const row of rows) this.bindAppRowSchemaFields(row, appOutput);
  if (query.relation === undefined && query.flatJoin === undefined && !query.arraySubqueries.length)
    normalizePublicCurrentRows(query, tableSchema, rows);
  if (profile) {
    profile.decodeMaterialize = nowMs() - started;
    started = nowMs();
  }
  this.finishEngineQueryRowsInSchema(query, schemaVersion, rows);
  if (profile) {
    profile.finishRows = nowMs() - started;
    started = nowMs();
  }
  if (query.flatJoin === undefined && !query.arraySubqueries.length)
    this.applyProjectionInSchema(query, schemaVersion, rows);
  if (profile) profile.applyProjection = nowMs() - started;
  return rows;
}

async function queryRowsWithOptionsForIdentityProfiled(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  plan: PreparedQueryPlanHandle | undefined,
  identity: AuthorSubject,
): Promise<[CurrentRow[], QueryReadProfile]> {
  const started = nowMs();
  const profile: QueryReadProfile = {
    resolveView: 0,
    compileProgram: 0,
    executePlan: 0,
    decodeMaterialize: 0,
    finishRows: 0,
    applyProjection: 0,
    total: 0,
  };
  const rows = await this.queryRowsWithOptionsForIdentity(
    shape,
    binding,
    tier,
    plan,
    identity,
    false,
    QueryAuthorizationMode.TrustedServing,
    profile,
  );
  profile.total = nowMs() - started;
  return [rows, profile];
}

function clientSettledBindingViewKeyForQuery(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  readView: ReadViewSpec,
) {
  return this.clientSettledBindingViewForQuery(shape, binding, tier, readView)?.key;
}

export function readViewKeyFor(tier: Tier, readView: ReadViewSpec): ReadViewKey {
  return RegisterShapeOptions.withDefaults({ tier, readView }).readViewKey();
}

function clientSettledBindingViewForQuery(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  readView: ReadViewSpec,
): ClientSettledBindingView | undefined {
  if (tier === DurabilityTier.None || tier === DurabilityTier.Local) return undefined;
  const key = {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: readViewKeyFor(tier, readView),
  };
  const window = RetainedRootWindowSource.forShape(shape);
  return { key, retainedWindow: window.isBounded() ? window : undefined };
}

function policyStrippedShape(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
): [ValidatedQuery, Binding] | undefined {
  if (!shape.query().policyBranches.length) return undefined;
  const schema =
    shape.schemaVersion() === this.catalogue.localSchemaVersionId
      ? this.catalogue.schema
      : catalogueSchema(this, shape.schemaVersion()).schema;
  const query = shape.query().clone();
  query.policyBranches = [];
  const residual = query.validateWithSchemaVersion(schema, shape.schemaVersion());
  const residualBinding = residual.bind(
    new Map([...binding.values()].filter(([name]) => residual.params().has(name))),
  );
  return [residual, residualBinding];
}

function clientLocalPreparedSourceName(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
): string | undefined {
  const residual = this.policyStrippedShape(shape, binding);
  if (residual) [shape, binding] = residual;
  let sourceShape: string | undefined;
  if (this.queryProgramPolicyContext(identity).kind === "System")
    sourceShape = queryBindingSourceShapeForPartsIfNeeded(shape.params(), new Map());
  else {
    const inputShape = this.normalizedRowSetShape(shape, binding);
    sourceShape = queryBindingSourceShapeForPartsIfNeeded(
      shape.params(),
      bindingClaimParamsForShape(inputShape, shape.params()),
    );
    const scope = sourceShape !== undefined ? this.activeSessionClaimScopeKey(identity) : undefined;
    if (scope !== undefined) sourceShape = `${sourceShape}:session:${scope}`;
  }
  return sourceShape === undefined ? undefined : `${sourceShape}:client-local`;
}

function canUsePreparedCurrentQueryPlan(this: NodeState, shape: ValidatedQuery): boolean {
  return (
    shape.schemaVersion() === this.catalogue.localSchemaVersionId &&
    !this.requiredIncludeMembershipIsIdentitySensitive(shape)
  );
}

function requiredIncludeMembershipIsIdentitySensitive(
  this: NodeState,
  shape: ValidatedQuery,
): boolean {
  return shape
    .query()
    .includes.some((include: any) => include.require || include.joinMode === "Inner");
}

async function queryRowsAt(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  position: GlobalTime,
): Promise<CurrentRow[]> {
  this.requireCatalogueReady();
  return this.queryRowsAtForIdentity(shape, binding, position, AuthorSubjectStatic.SYSTEM);
}

async function queryRowsAtForIdentity(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  position: GlobalTime,
  identity: AuthorSubject,
): Promise<CurrentRow[]> {
  const rows = await this.queryRowsAtWithQueryEngine(shape, binding, position, identity);
  const query = shape.query();
  this.finishEngineQueryRowsInSchema(query, shape.schemaVersion(), rows);
  if (query.flatJoin === undefined && !query.arraySubqueries.length)
    this.applyProjectionInSchema(query, shape.schemaVersion(), rows);
  return rows;
}

async function queryGraphOrGroove(node: NodeState, graph: GraphBuilder): Promise<RecordDeltas> {
  try {
    return await node.database.queryGraph(graph);
  } catch (e) {
    throw Err.Groove(e);
  }
}

async function queryRowsAtWithQueryEngine(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  position: GlobalTime,
  identity: AuthorSubject,
): Promise<CurrentRow[]> {
  const readSchema = catalogueSchema(this, shape.schemaVersion());
  const lowered = inlineSnapshotBindFilterLiterals(shape, binding, readSchema.schema);
  const program = await this.compileHistoricalQueryProgram(
    lowered,
    lowered.bind(new Map()),
    position,
    identity,
    CurrentQueryProgramOutput.AppRows,
  );
  const deltas = await queryGraphOrGroove(this, loweredAppRowsGraph(program));
  if (lowered.query().aggregate !== undefined)
    return this.materializeAggregateQueryRows(
      lowered.query(),
      materializationAppRowSchema(undefined, program),
      deltas,
    );
  return this.materializeHistoricalQueryRows(
    this.tableInSchemaRef(lowered.query().table, lowered.schemaVersion()),
    deltas,
  );
}

async function queryRowsAtSnapshot(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  snapshot: Snapshot,
): Promise<CurrentRow[]> {
  const readSchema = catalogueSchema(this, shape.schemaVersion());
  const lowered = inlineSnapshotBindFilterLiterals(shape, binding, readSchema.schema);
  const program = await this.compileSnapshotQueryProgram(
    lowered,
    lowered.bind(new Map()),
    snapshot,
    AuthorSubjectStatic.SYSTEM,
    CurrentQueryProgramOutput.AppRows,
  );
  const deltas = await queryGraphOrGroove(this, loweredAppRowsGraph(program));
  const rows =
    lowered.query().aggregate !== undefined
      ? this.materializeAggregateQueryRows(
          lowered.query(),
          materializationAppRowSchema(undefined, program),
          deltas,
        )
      : this.materializeHistoricalQueryRows(
          this.tableInSchemaRef(lowered.query().table, lowered.schemaVersion()),
          deltas,
        );
  this.finishEngineQueryRowsInSchema(lowered.query(), lowered.schemaVersion(), rows);
  return rows;
}

async function queryRowsIncludingDeletedWithQueryEngine(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  mode: any,
  readView: ReadViewSpec,
): Promise<CurrentRow[]> {
  const readSchema = catalogueSchema(this, shape.schemaVersion());
  const lowered = inlineSnapshotBindFilterLiterals(shape, binding, readSchema.schema);
  const query = lowered.query();
  const table =
    query.aggregate !== undefined
      ? this.queryOutputTable(query, lowered.schemaVersion())
      : this.tableInSchemaRef(query.table, lowered.schemaVersion());
  const program = await this.compileIncludeDeletedQueryProgramInAuthorizationMode(
    lowered,
    lowered.bind(new Map()),
    tier,
    identity,
    mode,
    readView,
    undefined,
  );
  const deltas = await queryGraphOrGroove(this, loweredMaterializationAppRowsGraph(program));
  if (query.aggregate !== undefined)
    return this.materializeAggregateQueryRows(
      query,
      materializationAppRowSchema(undefined, program),
      deltas,
    );
  return this.materializeIncludeDeletedQueryRows(table, deltas);
}

async function currentRowsAt(
  this: NodeState,
  table: string,
  position: GlobalTime,
): Promise<CurrentRow[]> {
  this.queryEngineReadMetrics.sourceGlobalTimeRangeScans += 1;
  return this.boundedHistoricalCurrentRows(table, position);
}

async function boundedHistoricalCurrentRows(
  this: NodeState,
  table: string,
  position: GlobalTime,
): Promise<CurrentRow[]> {
  const tableSchema = this.table(table);
  const tableId = this.physicalTableIdForSchema(this.catalogue.localSchemaVersionId, table);
  const currentTable = physicalGlobalCurrentTableName(tableId);
  const branch = BranchKey.default();
  const records = (
    await this.database.primaryKeyScanRaw(currentTable, [
      { kind: "Bytes", value: branch.canonicalBytes() },
    ])
  ).map((r: any) => r.ownedRecord());
  const rows: CurrentRow[] = [];
  for (const owned of records) {
    const record = owned.borrowed();
    const rowUuid = record.getUuid(GlobalCurrentRowRecord.FIELD_ROW_UUID_IDX);
    const seq = record.getNullableU64(GlobalCurrentRowRecord.FIELD_GLOBAL_TIME_IDX) ?? 0;
    let version: VersionRow;
    if (seq <= position) {
      const txTime = record.getU64(GlobalCurrentRowRecord.FIELD_TX_TIME_IDX);
      const txNode = record.getU64(GlobalCurrentRowRecord.FIELD_TX_NODE_ID_IDX);
      version = await this.queryVersionByAlias(table, rowUuid, txTime, txNode);
      if (version === undefined) throw Err.InvalidStoredValue("historical row winner is missing");
    } else {
      let best: [GlobalTime, VersionRow] | undefined;
      for (const candidate of await this.queryRowVersionsInBranch(table, branch, rowUuid)) {
        const state = await this.queryTransactionState(this.versionTxId(candidate));
        const candidateSeq = state?.[1];
        if (candidateSeq === undefined) continue;
        if (candidateSeq <= position && (!best || best[0] < candidateSeq))
          best = [candidateSeq, candidate];
      }
      if (!best) continue;
      version = best[1];
    }
    if (version.isDeleted()) continue;
    rows.push(this.currentRowFromMaterializedVersion(tableSchema, version));
  }
  sortCurrentRows(rows);
  return rows;
}

const emptyRelationSnapshot = (): RelationSnapshot => ({ rootCount: 0, rows: [], edges: [] });

async function queryRelationSnapshotInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  readView: ReadViewSpec,
  mode: any,
): Promise<RelationSnapshot> {
  const clientView =
    mode === QueryAuthorizationMode.ClientLocal
      ? this.clientSettledBindingViewForQuery(shape, binding, tier, readView)
      : undefined;
  const settledBindingView = clientView?.key;
  if (
    mode === QueryAuthorizationMode.ClientLocal &&
    tier >= DurabilityTier.Global &&
    settledBindingView === undefined
  )
    return emptyRelationSnapshot();
  const authorityResultKey =
    settledBindingView !== undefined
      ? this.uniqueAuthorityResultKeyForBindingView(settledBindingView)
      : undefined;
  if (
    mode === QueryAuthorizationMode.ClientLocal &&
    settledBindingView !== undefined &&
    authorityResultKey === undefined
  )
    return emptyRelationSnapshot();
  let program: QueryProgram, receiver: any;
  if (authorityResultKey !== undefined) {
    const request = this.currentQueryProgramRequest(
      shape,
      binding,
      tier,
      identity,
      CurrentQueryProgramOutput.RelationSnapshot,
      readView,
      settledBindingView,
      mode,
    );
    const accessPaths = this.oneShotAccessPaths(shape, binding, tier);
    const compiled = await this.compileClientOneShotWithCoveredInputs(
      request,
      accessPaths,
      shape.schemaVersion(),
      readView,
      authorityResultKey,
    );
    if (!compiled) return emptyRelationSnapshot();
    [program, receiver] = compiled;
  } else
    program = await this.compileCurrentQueryProgramForReadViewInAuthorizationMode(
      shape,
      binding,
      tier,
      identity,
      CurrentQueryProgramOutput.RelationSnapshot,
      readView,
      mode,
    );
  let snapshots, failure: unknown;
  try {
    snapshots = await this.database.queryGraphs(loweredProgramSinks(program));
  } catch (e) {
    failure = Err.Groove(e);
  }
  if (receiver) await this.retireCoveredInputSources(receiver.sources);
  if (failure !== undefined) throw failure;
  return this.materializeRelationSnapshotFromQueryEngine(shape, readView, snapshots, program);
}

function maintainedWitnessForResultMember(
  this: NodeState,
  versions: VersionRow[],
  resultSchema: SchemaVersionId,
  resultTable: string,
  rowUuid: RowUuid,
): VersionRow | undefined {
  const tableId = this.physicalTableIdForSchema(resultSchema, resultTable);
  for (let i = versions.length - 1; i >= 0; i--) {
    const v = versions[i];
    if (
      compareBytes(v.rowUuid(), rowUuid) === 0 &&
      !v.isDeleted() &&
      this.physicalTableIdForVersion(v) === tableId
    )
      return v;
  }
  return undefined;
}

async function prepareQueryBindingForLink(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
) {
  const [s, b] = this.queryBindingForLink(shape, binding);
  return [s, b, await this.preparedQueryPlan(s, b, tier, identity)];
}

function queryBindingForLink(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
): [ValidatedQuery, Binding] {
  const schema = catalogueSchema(this, shape.schemaVersion());
  const bound = bindQueryParamsWithMode(
    shape,
    binding,
    schema.schema,
    ParamBindingMode.RetainAllParams,
  );
  return [bound, bound.bind(new Map(binding.values()))];
}

function queryRowsForLink(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
): Promise<CurrentRow[]> {
  return this.queryRowsWithPreparedPlanForIdentity(shape, binding, tier, undefined, identity);
}

function queryRowsForLinkPhysicalRow(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  rowUuid: RowUuid,
) {
  return this.queryRowsForPhysicalRowInAuthorizationMode(
    shape,
    binding,
    tier,
    identity,
    rowUuid,
    QueryAuthorizationMode.TrustedServing,
    RootIndirectValues.Materialize,
  );
}

function queryRowVisibilityForLinkPhysicalRow(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  rowUuid: RowUuid,
) {
  return this.queryRowsForPhysicalRowInAuthorizationMode(
    shape,
    binding,
    tier,
    identity,
    rowUuid,
    QueryAuthorizationMode.TrustedServing,
    RootIndirectValues.Physical,
  );
}

function queryRowsForClientPhysicalRow(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  rowUuid: RowUuid,
) {
  return this.queryRowsForPhysicalRowInAuthorizationMode(
    shape,
    binding,
    tier,
    identity,
    rowUuid,
    QueryAuthorizationMode.ClientLocal,
    RootIndirectValues.Materialize,
  );
}

async function deltasForPreparedPlan(
  node: NodeState,
  plan: any,
  binding: Binding,
  identity: AuthorSubject,
  rootValues: RootValues,
): Promise<RecordDeltas> {
  const policy = node.queryProgramPolicyContext(identity);
  switch (plan.kind) {
    case "Prepared":
      return node.bindDisposableShapeSnapshot(
        plan.shape,
        bindingValuesForPlan(binding, plan.params, policy, PreparedClaimBindingMode.Strict),
        rootValues,
      );
    case "Graph":
      try {
        return await node.database.queryGraphWithRootValues(plan.graph, rootValues);
      } catch (e) {
        throw Err.Groove(e);
      }
  }
  throw Err.InvalidStoredValue("point reads never use peer-maintained plans");
}

async function queryRowsForPhysicalRowInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  rowUuid: RowUuid,
  mode: any,
  rootValues: RootValues,
): Promise<CurrentRow[]> {
  const table = this.tableInSchemaRef(shape.query().table, shape.schemaVersion());
  const accessPaths = newPaths();
  accessPaths.set(rootSourceId(shape.query().table), {
    kind: "PrimaryKey",
    values: [{ kind: "Uuid", value: rowUuid }],
  });
  const program = await this.compileCurrentQueryProgramWithAccessPaths(
    shape,
    binding,
    tier,
    identity,
    CurrentQueryProgramOutput.AppRows,
    accessPaths,
    mode,
  );
  const plan = await this.preparedQueryPlanFromProgram(program, shape, binding);
  const deltas = await deltasForPreparedPlan(this, plan, binding, identity, rootValues);
  const rows = this.materializeInlineCurrentQueryRows(table, deltas);
  this.finishEngineQueryRowsInSchema(shape.query(), shape.schemaVersion(), rows);
  return rows;
}

async function queryReadableCurrentRowIncludingDeleted(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  rowUuid: RowUuid,
): Promise<CurrentRow[]> {
  const table = this.tableInSchemaRef(shape.query().table, shape.schemaVersion());
  const program = await this.compileIncludeDeletedQueryProgramInAuthorizationMode(
    shape,
    binding,
    tier,
    identity,
    QueryAuthorizationMode.TrustedServing,
    ReadViewSpecDefault(),
    rowUuid,
  );
  const plan = await this.preparedQueryPlanFromProgram(program, shape, binding);
  const deltas = await deltasForPreparedPlan(
    this,
    plan,
    binding,
    identity,
    RootIndirectValues.Materialize,
  );
  return this.materializeIncludeDeletedQueryRows(table, deltas);
}

function queryRelationSnapshotForServingInReadView(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  readView: ReadViewSpec,
) {
  return this.queryRelationSnapshotInAuthorizationMode(
    shape,
    binding,
    tier,
    identity,
    readView,
    QueryAuthorizationMode.TrustedServing,
  );
}

function queryRelationSnapshotForClient(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  readView: ReadViewSpec,
) {
  return this.queryRelationSnapshotInAuthorizationMode(
    shape,
    binding,
    tier,
    identity,
    readView,
    QueryAuthorizationMode.ClientLocal,
  );
}

async function subscriptionSnapshotInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  readView: ReadViewSpec,
  mode: any,
): Promise<RelationSnapshot> {
  const client = mode === QueryAuthorizationMode.ClientLocal;
  if (!shape.query().arraySubqueries.length && readView.isDefault()) {
    const rows = client
      ? await this.queryRowsForClient(shape, binding, tier, identity)
      : await this.queryRowsWithPreparedPlanForIdentity(shape, binding, tier, undefined, identity);
    return { rootCount: rows.length, rows, edges: [] };
  }
  return client
    ? this.queryRelationSnapshotForClient(shape, binding, tier, identity, readView)
    : this.queryRelationSnapshotForServingInReadView(shape, binding, tier, identity, readView);
}

function queryRowsAtForLink(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  position: GlobalTime,
  identity: AuthorSubject,
) {
  return this.queryRowsAtForIdentity(shape, binding, position, identity);
}

function usesSchemaProjectedRead(this: NodeState, shape: ValidatedQuery): boolean {
  return shape.schemaVersion() !== this.catalogue.localSchemaVersionId;
}

function applyQueryOrderWithOccurrences(
  this: NodeState,
  query: Query,
  rows: CurrentRow[],
  occurrenceIds: OutputOccurrenceId[],
): void {
  const presentation = query.clone();
  if (!presentation.orderBy.length && presentation.relation) {
    const orderBy = relationUnionPresentationOrder(presentation.relation);
    if (orderBy) presentation.orderBy = orderBy;
  }
  const table =
    !presentation.orderBy.length || presentation.aggregate !== undefined
      ? undefined
      : this.tableInSchemaRef(presentation.table, this.catalogue.activeSchema.schema);
  this.sortQueryRowsWithOccurrences(presentation, table, rows, occurrenceIds);
}

function applyProjection(this: NodeState, query: Query, rows: CurrentRow[]): void {
  this.applyProjectionInSchema(query, this.catalogue.activeSchema.schema, rows);
}

function txQuery(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
) {
  return this.txQueryWithOptions(txId, shape, binding, false);
}

function txQueryWithOptions(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  includeDeleted: boolean,
) {
  return this.txQueryInAuthorizationMode(
    txId,
    shape,
    binding,
    AuthorSubjectStatic.SYSTEM,
    includeDeleted,
    QueryAuthorizationMode.ClientLocal,
  );
}

function txQueryForIdentity(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
) {
  return this.txQueryForIdentityWithOptions(txId, shape, binding, identity, false);
}

function txQueryForIdentityWithOptions(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  includeDeleted: boolean,
) {
  return this.txQueryInAuthorizationMode(
    txId,
    shape,
    binding,
    identity,
    includeDeleted,
    QueryAuthorizationMode.TrustedServing,
  );
}

function txRelationSnapshotWithOptions(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  includeDeleted: boolean,
) {
  return this.txRelationSnapshotInAuthorizationMode(
    txId,
    shape,
    binding,
    AuthorSubjectStatic.SYSTEM,
    includeDeleted,
    QueryAuthorizationMode.ClientLocal,
  );
}

function txRelationSnapshotForIdentityWithOptions(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  includeDeleted: boolean,
) {
  return this.txRelationSnapshotInAuthorizationMode(
    txId,
    shape,
    binding,
    identity,
    includeDeleted,
    QueryAuthorizationMode.TrustedServing,
  );
}

async function compileTxProgramWithNarrowing(
  node: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  output: CurrentQueryProgramOutput,
  includeDeleted: boolean,
  mode: any,
): Promise<[QueryProgram, any]> {
  const narrowing = node.offerTxQueryNarrowedReads(txId, shape, binding, includeDeleted);
  let program, failure: unknown;
  try {
    program = await node.compileOpenTxQueryProgram(
      txId,
      shape,
      binding,
      identity,
      output,
      includeDeleted,
      mode,
    );
  } catch (e) {
    failure = e;
  }
  const narrowedReads = node.withdrawTxNarrowedSourceReads(txId, narrowing);
  if (failure !== undefined) throw failure;
  return [program, narrowedReads];
}

function predicateReadFor(shape: ValidatedQuery, binding: Binding) {
  return {
    table: shape.query().table,
    shapeId: shape.shapeId(),
    shape: shape.query().clone(),
    bindingId: binding.bindingId(),
    bindingValues: new Map(binding.values()),
  };
}

async function txRelationSnapshotInAuthorizationMode(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  includeDeleted: boolean,
  mode: any,
): Promise<RelationSnapshot> {
  identity = this.transactionQueryIdentity(txId, identity, mode);
  const [program, narrowedReads] = await compileTxProgramWithNarrowing(
    this,
    txId,
    shape,
    binding,
    identity,
    CurrentQueryProgramOutput.RelationSnapshot,
    includeDeleted,
    mode,
  );
  let snapshots;
  try {
    snapshots = await this.database.queryGraphs(loweredProgramSinks(program));
  } catch (e) {
    throw Err.Groove(e);
  }
  const snapshot: RelationSnapshot = await this.materializeRelationSnapshotFromQueryEngine(
    shape,
    ReadViewSpecDefault(),
    snapshots,
    program,
  );
  this.openTxMut(txId).predicateReads.push(predicateReadFor(shape, binding));
  if (shape.query().aggregate === undefined) {
    const returned = new SortedMap<string, RowUuid[]>(cmpStr);
    for (const row of snapshot.rows)
      returned.getOrInsert(row.table(), () => []).push(row.rowUuid());
    for (const [table, rows] of returned)
      await this.recordTxQueryRowReads(txId, shape.schemaVersion(), table, rows);
  } else await this.recordTxAggregateInputReads(txId, shape, binding, identity, mode);
  await this.recordTxNarrowedSourceReads(txId, narrowedReads, identity, mode);
  return snapshot;
}

async function txQueryInAuthorizationMode(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  includeDeleted: boolean,
  mode: any,
): Promise<CurrentRow[]> {
  identity = this.transactionQueryIdentity(txId, identity, mode);
  const query = shape.query();
  const table = this.tableInSchema(query.table, shape.schemaVersion());
  const [program, narrowedReads] = await compileTxProgramWithNarrowing(
    this,
    txId,
    shape,
    binding,
    identity,
    CurrentQueryProgramOutput.AppRows,
    includeDeleted,
    mode,
  );
  const deltas = await queryGraphOrGroove(this, loweredMaterializationAppRowsGraph(program));
  const rows: CurrentRow[] =
    query.aggregate !== undefined
      ? this.materializeAggregateQueryRows(
          query,
          materializationAppRowSchema(undefined, program),
          deltas,
        )
      : this.materializeInlineCurrentQueryRows(table, deltas);
  this.openTxMut(txId).predicateReads.push(predicateReadFor(shape, binding));
  if (query.aggregate === undefined) {
    const rootRows = rows.filter((r) => r.table() === query.table).map((r) => r.rowUuid());
    await this.recordTxQueryRowReads(txId, shape.schemaVersion(), query.table, rootRows);
  } else await this.recordTxAggregateInputReads(txId, shape, binding, identity, mode);
  await this.recordTxNarrowedSourceReads(txId, narrowedReads, identity, mode);
  this.finishEngineQueryRowsInSchema(query, shape.schemaVersion(), rows);
  if (!query.arraySubqueries.length)
    this.applyProjectionInSchema(query, shape.schemaVersion(), rows);
  return rows;
}

async function recordTxAggregateInputReads(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  mode: any,
): Promise<void> {
  const inputQuery = shape.query().aggregateInput();
  if (inputQuery === undefined || !this.transactionIsExclusive(txId)) return;
  const entry = this.catalogue.catalogueSchemas.get(shape.schemaVersion());
  if (!entry) throw Err.InvalidStoredValue("transaction schema is unknown");
  const input = inputQuery.validate(entry.schema);
  await this.txQueryInAuthorizationMode(
    txId,
    input,
    input.bind(new Map(binding.values())),
    identity,
    false,
    mode,
  );
}

function exclusiveSourceHydrationQueries(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  includeDeleted: boolean,
): Query[] {
  const queries: Query[] = [];
  const seen = new Set<string>();
  for (const read of this.exclusiveSourceReads(shape, binding, includeDeleted).reads.values()) {
    const query = read.shape.query().clone();
    const key = debugFmt(query);
    if (!seen.has(key)) {
      seen.add(key);
      queries.push(query);
    }
  }
  return queries;
}

function offerTxQueryNarrowedReads(
  this: NodeState,
  txId: OpenTransactionId,
  shape: ValidatedQuery,
  binding: Binding,
  includeDeleted: boolean,
): any {
  const reads =
    this.transactionIsExclusive(txId) && !this.openTx(txId).sourceNarrowing.recording
      ? this.exclusiveSourceReads(shape, binding, includeDeleted)
      : ExclusiveSourceReads.default();
  return this.offerTxNarrowedSourceReads(txId, reads);
}

async function recordTxNarrowedSourceReads(
  this: NodeState,
  txId: OpenTransactionId,
  reads: any[],
  identity: AuthorSubject,
  mode: any,
): Promise<void> {
  if (!reads.length || !this.transactionIsExclusive(txId)) return;
  for (const read of reads) {
    const key = `${debugFmt(read.shape.shapeId())}|${debugFmt(read.binding.bindingId())}`;
    if (this.openTx(txId).narrowedPredicateReads.has(key)) continue;
    this.openTxMut(txId).sourceNarrowing.recording = true;
    try {
      await this.txQueryInAuthorizationMode(txId, read.shape, read.binding, identity, false, mode);
    } finally {
      this.openTxMut(txId).sourceNarrowing.recording = false;
    }
    this.openTxMut(txId).narrowedPredicateReads.add(key);
  }
}

function transactionQueryIdentity(
  this: NodeState,
  txId: OpenTransactionId,
  identity: AuthorSubject,
  mode: any,
): AuthorSubject {
  const kind = this.openTx(txId).kind;
  const bound = kind.permissionSubject;
  if (kind.kind === "Mergeable" && bound === undefined) return identity;
  if (mode !== QueryAuthorizationMode.ClientLocal && debugFmt(identity) !== debugFmt(bound))
    throw Err.OpenTransactionIdentityMismatch();
  return bound;
}

function preparedPlanClaimScope(this: NodeState, identity: AuthorSubject): string {
  const scope = this.activeSessionClaimScopeKey(identity);
  return scope !== undefined
    ? `request:${scope}`
    : `session:${this.sessionClaimRevision(identity)}`;
}

async function preparedQueryPlan(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
): Promise<PreparedQueryPlanHandle> {
  const key = `${debugFmt(shape.shapeId())}|${debugFmt(tier)}|${policyPlanCacheSignature(binding, identity, this.preparedPlanClaimScope(identity))}`;
  const cached = this.query.queryShapeCache.get(key);
  if (cached && cached.kind !== "PeerMaintainedMarker") return cached;
  const program = await this.compileCurrentQueryProgram(
    shape,
    binding,
    tier,
    identity,
    CurrentQueryProgramOutput.AppRows,
  );
  const plan = await this.preparedQueryPlanFromProgram(program, shape, binding);
  this.query.queryShapeCache.set(key, plan);
  return plan;
}

async function ensurePeerMaintainedSubscriptionViewSupported(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
  identity: AuthorSubject,
  readView: ReadViewSpec,
  mode: any,
): Promise<void> {
  const [request, accessPaths] = this.currentQueryProgramRequestAndAccessPaths(
    shape,
    binding,
    tier,
    identity,
    CurrentQueryProgramOutput.MaintainedView,
    readView,
    undefined,
    mode,
    PreparedClaimBindingMode.Strict,
  );
  await this.ensureQueryProgramRequestSupported(request, accessPaths);
}

function markPeerMaintainedQueryShapeCache(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  tier: Tier,
): PreparedQueryPlanHandle {
  const key = `${debugFmt(shape.shapeId())}|${debugFmt(tier)}|${queryBindingValueSignature(binding)}`;
  let plan = this.query.queryShapeCache.get(key);
  if (!plan) this.query.queryShapeCache.set(key, (plan = { kind: "PeerMaintainedMarker" }));
  return plan;
}

function openSeededMaintainedSubscriptionView(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  tier: Tier,
  readView: ReadViewSpec,
) {
  return this.openSeededMaintainedSubscriptionViewWithWaker(
    shape,
    binding,
    identity,
    tier,
    readView,
    readViewKeyFor(tier, readView),
    undefined,
  );
}

async function openSeededMaintainedSubscriptionViewWithWaker(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  tier: Tier,
  readView: ReadViewSpec,
  readViewKey: ReadViewKey,
  waker: (() => void) | undefined,
) {
  const opened = await this.openSeededMaintainedSubscriptionViewInAuthorizationMode(
    shape,
    binding,
    identity,
    tier,
    readView,
    readViewKey,
    this.peerQueryAuthorizationMode(),
    undefined,
    undefined,
    PreparedClaimBindingMode.Strict,
    false,
    waker,
  );
  return opened.slice(0, 6);
}

function peerQueryAuthorizationMode(this: NodeState) {
  return this.clientRelayScope() !== undefined
    ? QueryAuthorizationMode.ClientLocal
    : QueryAuthorizationMode.TrustedServing;
}

function openSeededRelaySubscriptionView(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  readView: ReadViewSpec,
  authorityResultKey: AuthorityResultKey,
) {
  return this.openSeededRelaySubscriptionViewWithWaker(
    shape,
    binding,
    identity,
    readView,
    readViewKeyFor(DurabilityTier.Global, readView),
    authorityResultKey,
    undefined,
  );
}

export function mergeTransitions(into: any, extra: any): void {
  into.adds.push(...extra.adds);
  into.removes.push(...extra.removes);
  for (const [k, v] of extra.resultPayloadAdds) into.resultPayloadAdds.set(k, v);
  for (const k of extra.resultPayloadRemoves) into.resultPayloadRemoves.add(k);
  for (const k of extra.programFactAdds) into.programFactAdds.add(k);
  for (const k of extra.programFactRemoves) into.programFactRemoves.add(k);
  into.terminalOperations.push(...extra.terminalOperations);
}

async function openSeededRelaySubscriptionViewWithWaker(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  readView: ReadViewSpec,
  readViewKey: ReadViewKey,
  authorityResultKey: AuthorityResultKey,
  waker: (() => void) | undefined,
) {
  const [
    subscription,
    maintained,
    terminalSchemas,
    transitions,
    tables,
    initialReceived,
    coveredInputSources,
  ] = await this.openSeededMaintainedSubscriptionViewInAuthorizationMode(
    shape,
    binding,
    identity,
    DurabilityTier.Global,
    readView,
    readViewKey,
    QueryAuthorizationMode.ClientLocal,
    authorityResultKey.bindingView,
    authorityResultKey,
    PreparedClaimBindingMode.Strict,
    false,
    waker,
  );
  const query = shape.query();
  const relation = query.relation;
  const local = new LocalMaintainedViewSubscription({
    subscription,
    retainedPreparedPlan: undefined,
    maintained,
    terminalSchemas,
    tables,
    resultQuery: query.clone(),
    resultTable: query.table,
    resultSchemaVersion: shape.schemaVersion(),
    resultSelect: query.select,
    resultRelationProjection: relation ? relationOutputProjectionIfPresent(relation) : undefined,
    resultRelationProjections:
      relation && relationUnionParts(relation.rel) !== undefined
        ? relationUnionLeafProjections(relation)
        : undefined,
    rootOccurrenceIds: [],
    initialReceived,
    coveredInputReceiver: new CoveredInputReceiver(coveredInputSources, readView),
  });
  await this.applyLocalMaintainedViewTransitionsInner(local, structuredClone(transitions), false);
  if (!local.coveredInputReceiver.isEmpty()) {
    const extra = await this.installOpenedLocalCoveredReceiver(local, authorityResultKey, waker);
    if (extra) mergeTransitions(transitions, extra);
    else local.initialReceived = false;
  }
  if (local.initialReceived) {
    const witnesses = await this.selectedDeletionWitnesses(
      authorityResultKey,
      shape.schemaVersion(),
    );
    transitions.supportingChanged ||= local.maintained.replaceSelectedDeletionWitnesses(witnesses);
  }
  return [
    local.subscription,
    local.maintained,
    local.terminalSchemas,
    transitions,
    local.tables,
    local.initialReceived,
    local.coveredInputReceiver,
  ];
}

function openSeededAuthorizationSupportSubscriptionView(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  tier: Tier,
  readView: ReadViewSpec,
) {
  return this.openSeededAuthorizationSupportSubscriptionViewWithWaker(
    shape,
    binding,
    identity,
    tier,
    readView,
    readViewKeyFor(tier, readView),
    undefined,
  );
}

async function openSeededAuthorizationSupportSubscriptionViewWithWaker(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  tier: Tier,
  readView: ReadViewSpec,
  readViewKey: ReadViewKey,
  waker: (() => void) | undefined,
) {
  const opened = await this.openSeededMaintainedSubscriptionViewInAuthorizationMode(
    shape,
    binding,
    identity,
    tier,
    readView,
    readViewKey,
    QueryAuthorizationMode.TrustedServing,
    undefined,
    undefined,
    PreparedClaimBindingMode.FailClosedAuthorizationSupport,
    false,
    waker,
  );
  return opened.slice(0, 6);
}

async function openSeededMaintainedSubscriptionViewInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  tier: Tier,
  readView: ReadViewSpec,
  readViewKey: ReadViewKey,
  mode: any,
  settledBindingView: BindingViewKey | undefined,
  settledAuthorityResultKey: AuthorityResultKey | undefined,
  claimMode: PreparedClaimBindingMode,
  pendingOverlay: boolean,
  waker: (() => void) | undefined,
): Promise<any[]> {
  const schema = catalogueSchema(this, shape.schemaVersion());
  shape = bindQueryParamsWithMode(shape, binding, schema.schema, ParamBindingMode.RetainAllParams);
  binding = shape.bind(new Map(binding.values()));
  let loneClientLocalSource: string | undefined;
  if (
    mode === QueryAuthorizationMode.ClientLocal &&
    tier === DurabilityTier.Local &&
    readView.isDefault() &&
    settledBindingView === undefined
  ) {
    const source = this.clientLocalPreparedSourceName(shape, binding, identity);
    if (source !== undefined) {
      const literal = this.clientLocalLiteralShapes.get(source);
      const siblingOpen = literal !== undefined && literal[0].alive && !literal[1].equals(binding);
      if (!siblingOpen && !this.database.preparedBindingSourceIsBound(source))
        loneClientLocalSource = source;
    }
  }
  const request = this.currentQueryProgramRequestWithPreparedClaimMode(
    shape,
    binding,
    tier,
    identity,
    CurrentQueryProgramOutput.MaintainedView,
    readView,
    settledBindingView,
    mode,
    claimMode,
    loneClientLocalSource !== undefined,
  );
  const primarySources = request.reads.primary.sources;
  if (settledAuthorityResultKey !== undefined)
    for (const [, source] of primarySources)
      if (source.kind === "SettledBindingView")
        source.authorityResultKey = settledAuthorityResultKey;
  if (pendingOverlay) {
    if (mode !== QueryAuthorizationMode.ClientLocal)
      throw Err.InvalidStoredValue("pending receiver overlay requires client-local execution");
    for (const [id, source] of primarySources)
      if (source.kind === "SettledBindingView")
        primarySources.set(id, {
          kind: "WithOverlays",
          input: source,
          overlays: { entries: [{ kind: "PendingLocal" }] },
        });
  }
  const accessPaths = this.currentQueryHydrationAccessPaths(
    request,
    shape,
    binding,
    HydrationLifetime.Retained,
  );
  const [runtimeSources, runtimeDescriptors, coveredInputSources] =
    mode === QueryAuthorizationMode.ClientLocal
      ? this.allocateClientReceiverInputSources(request)
      : [newPaths(), new SortedMap(cmpSourceId), new SortedMap(cmpGlobalPhysicalTableId)];
  const retiring = async <T>(f: () => T | Promise<T>): Promise<T> => {
    try {
      return await f();
    } catch (error) {
      await this.retireCoveredInputSources(coveredInputSources);
      throw error;
    }
  };
  const program: QueryProgram = await retiring(() =>
    runtimeSources.size
      ? this.compileQueryProgramRequestWithInlineSourcesAccessPathsAndCoveredInputs(
          request,
          newPaths(),
          accessPaths,
          runtimeSources,
          runtimeDescriptors,
        )
      : this.compileQueryProgramRequestWithAccessPaths(request, accessPaths),
  );
  if (coveredInputTrace())
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=opened_program table=${shape.query().table} node=${debugFmt(this.nodeUuid)} mode=${debugFmt(mode)} identity=${debugFmt(identity)} tier=${debugFmt(tier)} settled_view=${debugFmt(settledBindingView)} authority_key=${debugFmt(settledAuthorityResultKey)} sources=${debugFmt(program.request.reads.primary.sources.keysArray())} descriptors=${debugFmt(program.sourceDescriptors.keysArray())}`,
    );
  const tables = program.lowered.maintainedTerminalTables;
  const targetedRefreshTables = program.lowered.targetedRefreshTables;
  const targetedRefreshUncertain = program.lowered.targetedRefreshUncertain;
  const terminalSchemas = MaintainedSubscriptionView.terminalSchemasForProgram(program);
  const bindingSourceShape =
    program.request.input.binding.sourceShape ??
    queryBindingSourceShapeForPreparedParams(preparedParamsFromDomain(program.lowered.parameters));
  const storageBacked = !program.request.output.facts.has("VersionWitnesses");
  const inlineContentBranchKeys = new Map<string, any>();
  for (const [, source] of program.request.reads.primary.sources)
    if (source.kind === "BranchView" && source.base?.kind === "Snapshot")
      inlineContentBranchKeys.set(debugFmt(source.base.branchKey), source.base.branchKey);
  const rootValues = await retiring(() =>
    this.maintainedProjectionDroppedRootValues(shape.query(), shape.schemaVersion()),
  );
  const subscription = await retiring(() =>
    this.subscribeLoweredProgramWithRootValues(
      program,
      binding,
      bindingSourceShape,
      claimMode,
      waker,
      rootValues,
    ),
  );
  if (coveredInputTrace()) eprintln("JAZZ_COVERED_INPUT_TRACE stage=receiver_subscription_opened");
  const maintained = MaintainedSubscriptionView.default();
  const physicalMapping = this.catalogue.physicalMappings.get(shape.schemaVersion());
  if (!physicalMapping)
    throw Err.InvalidStoredValue("maintained scope schema has no physical mapping");
  maintained.physicalTables = new Map(
    [...physicalMapping.identities.tables].map(([name, table]: any) => [name, table.id]),
  );
  maintained.targetedRefreshTables = targetedRefreshTables;
  maintained.targetedRefreshUncertain = targetedRefreshUncertain;
  maintained.setReadView(readViewKey);
  const witnessTableNames = new Map<string, string>();
  for (const logicalName of tables.keys()) {
    const tableId = this.physicalTableIdForSchema(shape.schemaVersion(), logicalName);
    for (const [schemaId, mapping] of this.catalogue.physicalMappings) {
      const alias = this.catalogue.schemaVersionAliases.get(schemaId);
      if (alias === undefined) continue;
      for (const [authoredName, tableMapping] of mapping.tables)
        if (tableMapping.tableId === tableId && authoredName !== logicalName)
          witnessTableNames.set(`${logicalName}|${alias}`, authoredName);
    }
  }
  maintained.setWitnessTableNames(witnessTableNames);
  if (storageBacked) maintained.enableStorageBackedResultMaterialization();
  for (const branchKey of inlineContentBranchKeys.values())
    maintained.enableInlineContentBranchKey(branchKey);
  const transitions = ResultTransitionsDefault();
  const fail = async (error: unknown): Promise<never> => {
    this.database.unsubscribe(subscription.id());
    await this.retireCoveredInputSources(coveredInputSources);
    throw error;
  };
  const apply = async (deltas: any) => {
    let next;
    try {
      next = maintained.applyMultisinkDeltas(deltas, terminalSchemas, tables, this.nodeAliases);
    } catch (error) {
      return fail(error);
    }
    mergeTransitions(transitions, next);
    transitions.supportingChanged ||= next.supportingChanged;
  };
  let initialReceived = false;
  let event = subscription.pollNextEvent(waker);
  if (event?.kind === "Update") {
    if (coveredInputTrace()) eprintln("JAZZ_COVERED_INPUT_TRACE stage=receiver_initial_snapshot");
    await apply(event.update.deltas);
    initialReceived = true;
  } else if (event?.kind === "Error") await fail(Err.Groove(event.error));
  if (initialReceived)
    while ((event = subscription.pollNextEvent(waker))) {
      if (event.kind === "Error") await fail(Err.Groove(event.error));
      await apply(event.update.deltas);
    }
  if (coveredInputTrace()) eprintln("JAZZ_COVERED_INPUT_TRACE stage=receiver_initial_applied");
  if (loneClientLocalSource !== undefined) {
    const token = { alive: true };
    for (const [k, [t]] of [...this.clientLocalLiteralShapes])
      if (!t.alive) this.clientLocalLiteralShapes.delete(k);
    this.clientLocalLiteralShapes.set(loneClientLocalSource, [token, binding]);
    maintained.holdClientLocalLiteralToken(token);
  }
  return [
    subscription,
    maintained,
    terminalSchemas,
    transitions,
    tables,
    initialReceived,
    coveredInputSources,
  ];
}

async function awaitFirstEvent(subscription: any): Promise<any> {
  for (;;) {
    const event = subscription.pollNextEvent();
    if (event) return event;
    await subscription.ready();
  }
}

async function bindShapeSnapshot(
  this: NodeState,
  shape: PreparedShapeId,
  values: Value[],
): Promise<MultisinkDeltas> {
  let subscription;
  try {
    subscription = await this.database.bindShape(shape, values);
  } catch (e) {
    throw Err.Groove(e);
  }
  const event = await awaitFirstEvent(subscription);
  this.database.unsubscribe(subscription.id());
  if (event.kind === "Error") throw Err.Groove(event.error);
  return event.update.deltas;
}

async function bindDisposableShapeSnapshot(
  this: NodeState,
  shape: PreparedShapeId,
  values: Value[],
  rootValues: RootValues,
): Promise<RecordDeltas> {
  let subscription;
  try {
    subscription = await this.database.bindShapeWithRootValues(shape, values, rootValues);
  } catch (e) {
    this.database.retirePreparedShape(shape);
    throw Err.Groove(e);
  }
  const event = await awaitFirstEvent(subscription);
  this.database.unsubscribe(subscription.id());
  this.database.retirePreparedShape(shape);
  if (event.kind === "Error") throw Err.Groove(event.error);
  return takeRequiredSinkDeltas(event.update.deltas, JAZZ_APP_ROWS_SINK);
}

export function aggregateCurrentRowFromRecord(
  query: Query,
  output: AppRowSchema,
  rowUuid: RowUuid,
  record: BorrowedRecord,
): CurrentRow {
  const fields = [new DescriptorField("row_uuid", { kind: "Uuid" })];
  const values: Value[] = [{ kind: "Uuid", value: rowUuid }];
  const publicationFields: any[] = [
    { kind: "ResultField", name: "row_uuid", visibility: "HiddenMetadata" },
  ];
  const aggregate = query.aggregate;
  if (!aggregate) throw Err.InvalidStoredValue("aggregate record has no aggregate query shape");
  const schema = aggregateOutputSchema(output);
  if (
    schema.groupKeyFields.length !== (aggregate.groupBy !== undefined ? 1 : 0) ||
    schema.valueFields.length !== aggregate.aggregates.length
  )
    throw Err.InvalidStoredValue("aggregate query differs from its lowered output shape");
  const append = (field: any, publicName: string, carrier: string) => {
    const index = aggregateRecordFieldIndex(record, field);
    const binding = field.name !== undefined ? output.publicationFields.get(field.name) : undefined;
    if (!binding)
      throw Err.InvalidStoredValue("lowered aggregate field has no publication binding");
    if (binding.applicationName() !== publicName)
      throw Err.InvalidStoredValue("aggregate publication name differs from query output");
    fields.push(
      new DescriptorField(carrier, record.descriptor().fields()[index].valueType).withIdentity(
        FieldIdentityName(publicName),
      ),
    );
    values.push(record.getIdx(index));
    publicationFields.push(binding);
  };
  if (aggregate.groupBy !== undefined)
    append(schema.groupKeyFields[0], aggregate.groupBy, userColumnField(aggregate.groupBy));
  aggregate.aggregates.forEach((expression: any, i: number) =>
    append(
      schema.valueFields[i],
      expression.alias,
      userColumnField(aggregateOutputField(expression.alias)),
    ),
  );
  const descriptor = RecordDescriptorNewWithFields(fields);
  return CurrentRowNewWithPublicationFields(
    query.table,
    new OwnedRecord(descriptor.create(values), descriptor),
    publicationFields,
  );
}

/** HistoricalRead::read */
export async function historicalRead(
  handle: { node: NodeState; position: GlobalTime },
  shape: ValidatedQuery,
  binding: Binding,
): Promise<CurrentRow[]> {
  if (!handle.node.isHistoryCompleteFor(shape, handle.position))
    throw Err.HistoricalReadRequiresServer();
  return handle.node.queryRowsAt(shape, binding, handle.position);
}

export function localMaintainedViewContentWitness(
  versions: VersionRow[],
  table: string,
  rowUuid: RowUuid,
): VersionRow | undefined {
  for (let i = versions.length - 1; i >= 0; i--) {
    const v = versions[i];
    if (v.table() === table && compareBytes(v.rowUuid(), rowUuid) === 0 && !v.isDeleted()) return v;
  }
  return undefined;
}

export function currentRowHasRequiredSubscriptionCells(
  row: CurrentRow,
  table: TableSchema,
  projection: string[] | undefined,
): boolean {
  return table.columns.every(
    (c: any) =>
      (projection !== undefined && !projection.includes(c.name)) ||
      c.columnType.kind === "Nullable" ||
      row.cell(table, c.name) !== undefined,
  );
}

export function sortQueryDefaultRows(rows: CurrentRow[]): void {
  rows.sort(defaultQueryRowOrder);
}

function normalizePublicCurrentRows(query: Query, table: TableSchema, rows: CurrentRow[]): void {
  const columns: string[] = table.columns.map((c: any) => c.name);
  rows.forEach((row, i) => {
    const aggregate = query.aggregate;
    if (!aggregate) {
      rows[i] = row.project(table, columns);
      return;
    }
    const bindings = new Map<string, number>();
    if (aggregate.groupBy !== undefined) {
      const index = row.publicationFields.findIndex(
        (f: any) => f.kind === "StoredColumn" && f.applicationName() === aggregate.groupBy,
      );
      if (index < 0)
        throw Err.InvalidStoredValue("aggregate group has no source publication binding");
      bindings.set(aggregate.groupBy, index);
    }
    for (const out of aggregate.aggregates) {
      const index = row.publicationFields.findIndex(
        (f: any) =>
          f.kind === "ResultField" &&
          f.visibility === "ApplicationCell" &&
          f.applicationName() === out.alias,
      );
      if (index < 0)
        throw Err.InvalidStoredValue("aggregate output has no result publication binding");
      bindings.set(aggregateOutputColumn(out.alias), index);
    }
    rows[i] = row.projectWithColumnBinding(table, columns, (name: string) => bindings.get(name));
  });
}

function defaultQueryRowOrder(left: CurrentRow, right: CurrentRow): number {
  return (
    compareBytes(left.rowUuid(), right.rowUuid()) ||
    cmpStr(debugFmt(left.projectedTxAlias()), debugFmt(right.projectedTxAlias())) ||
    compareBytes(left.record.raw(), right.record.raw())
  );
}

export function applyQueryWindow(query: Query, rows: CurrentRow[]): CurrentRow[] {
  const offset = Math.min(query.offset, rows.length);
  const limit = query.limit ?? Math.max(rows.length - offset, 0);
  const end = Math.min(offset + limit, rows.length);
  return offset > 0 || end < rows.length ? rows.slice(offset, end) : rows;
}

export function magicCurrentColumnType(column: string): any {
  const ty = AuthorSubjectStatic.metadataType(column);
  if (ty !== undefined) return ty;
  return column === "$createdAt" || column === "$updatedAt" ? { kind: "U64" } : undefined;
}

export const isMagicCurrentColumn = (column: string) =>
  magicCurrentColumnType(column) !== undefined;

export function predicateParams(predicates: Predicate[]): Set<string> {
  const params = new Set<string>();
  const operand = (o: Operand) => o.kind === "Param" && params.add(o.name);
  const walk = (p: Predicate): void => {
    switch (p.kind) {
      case "All":
      case "Any":
        return p.items.forEach(walk);
      case "Not":
        return walk(p.inner);
      case "In":
        operand(p.operand);
        return p.values.forEach(operand);
      case "IsNull":
        operand(p.operand);
        return;
      case "EnumMatch":
        return walk(p.payload);
      default:
        operand(p.left);
        operand(p.right);
    }
  };
  predicates.forEach(walk);
  return params;
}

export function queryOrderValue(
  row: CurrentRow,
  table: TableSchema,
  column: string,
): Value | undefined {
  if (column === "id") return { kind: "Uuid", value: row.rowUuid() };
  if (isMagicCurrentColumn(column)) return row.rawField(column);
  return row.cell(table, column);
}

installNodeMethods({
  resolveTimeTravelPosition,
  registeredShape,
  compiledCoveredInputSourcesForSubscription,
  compileCurrentQueryProgram,
  compileCurrentQueryProgramInAuthorizationMode,
  compileCurrentQueryProgramForReadViewInAuthorizationMode,
  compileCurrentQueryProgramWithSettledView,
  compileCurrentQueryProgramWithSettledViewAndPreparedClaimMode,
  currentQueryProgramRequestAndAccessPaths,
  compileCurrentQueryProgramForOneShotRead,
  compileUuidPageProbeProgram,
  boundedDeletionRegisterForUuidPage,
  tryUuidPageProbe,
  compileOrderedPageProbeProgram,
  boundedDeletionRegisterForOrderedPage,
  tryOrderedPageProbe,
  compileCurrentQueryProgramWithAccessPaths,
  compileHistoricalQueryProgram,
  compileSnapshotQueryProgram,
  compileIncludeDeletedQueryProgramInAuthorizationMode,
  compileOpenTxQueryProgram,
  currentQueryProgramRequest,
  currentQueryProgramRequestWithInlineBindingSource,
  storageBackedMaintainedRootHasIdentityTableMapping,
  currentQueryProgramRequestWithPreparedClaimMode,
  queryRowsWithPreparedPlan,
  queryRowsWithPreparedPlanForIdentity,
  queryRowsForClient,
  allocateClientReceiverInputSources,
  compileClientOneShotWithCoveredInputs,
  retireCoveredInputSources,
  queryRowsLocalPreview,
  queryRowsLocalPreviewProfiled,
  queryRowsIncludingDeletedInAuthorizationMode,
  queryRowsWithOptionsForIdentity,
  projectionDroppedRootValues,
  maintainedProjectionDroppedRootValues,
  materializeAndFinalizeQueryRows,
  queryRowsWithOptionsForIdentityProfiled,
  clientSettledBindingViewKeyForQuery,
  clientSettledBindingViewForQuery,
  policyStrippedShape,
  clientLocalPreparedSourceName,
  canUsePreparedCurrentQueryPlan,
  requiredIncludeMembershipIsIdentitySensitive,
  queryRowsAt,
  queryRowsAtForIdentity,
  queryRowsAtWithQueryEngine,
  queryRowsAtSnapshot,
  queryRowsIncludingDeletedWithQueryEngine,
  currentRowsAt,
  boundedHistoricalCurrentRows,
  queryRelationSnapshotInAuthorizationMode,
  maintainedWitnessForResultMember,
  prepareQueryBindingForLink,
  queryBindingForLink,
  queryRowsForLink,
  queryRowsForLinkPhysicalRow,
  queryRowVisibilityForLinkPhysicalRow,
  queryRowsForClientPhysicalRow,
  queryRowsForPhysicalRowInAuthorizationMode,
  queryReadableCurrentRowIncludingDeleted,
  queryRelationSnapshotForServingInReadView,
  queryRelationSnapshotForClient,
  subscriptionSnapshotInAuthorizationMode,
  queryRowsAtForLink,
  usesSchemaProjectedRead,
  applyQueryOrderWithOccurrences,
  applyProjection,
  txQuery,
  txQueryWithOptions,
  txQueryForIdentity,
  txQueryForIdentityWithOptions,
  txRelationSnapshotWithOptions,
  txRelationSnapshotForIdentityWithOptions,
  txRelationSnapshotInAuthorizationMode,
  txQueryInAuthorizationMode,
  recordTxAggregateInputReads,
  exclusiveSourceHydrationQueries,
  offerTxQueryNarrowedReads,
  recordTxNarrowedSourceReads,
  transactionQueryIdentity,
  preparedPlanClaimScope,
  preparedQueryPlan,
  ensurePeerMaintainedSubscriptionViewSupported,
  markPeerMaintainedQueryShapeCache,
  openSeededMaintainedSubscriptionView,
  openSeededMaintainedSubscriptionViewWithWaker,
  peerQueryAuthorizationMode,
  openSeededRelaySubscriptionView,
  openSeededRelaySubscriptionViewWithWaker,
  openSeededAuthorizationSupportSubscriptionView,
  openSeededAuthorizationSupportSubscriptionViewWithWaker,
  openSeededMaintainedSubscriptionViewInAuthorizationMode,
  bindShapeSnapshot,
  bindDisposableShapeSnapshot,
});
