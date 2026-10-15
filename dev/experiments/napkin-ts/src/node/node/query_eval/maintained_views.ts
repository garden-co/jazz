import { SortedMap, SortedSet, cmpStr, equalBytes } from "../../../support/index.js";
import type {
  NodeState,
  MultisinkSubscription,
  TableSchema,
  Query,
  SchemaVersionId,
  ResultMemberEntry,
  ResultMemberPayloadEntry,
  OutputOccurrenceId,
  RecordDescriptor,
  ReadViewSpec,
  AuthorityResultKey,
  ValidatedQuery,
  Binding,
  AuthorSubject,
  DurabilityTier as Tier,
  RelationSnapshot,
  SupportingRow,
  CurrentRow,
  VersionRow,
  RowUuid,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  installNodeMethods,
  debugFmt,
  LocalAuthorityReconciliation,
  cmpGlobalPhysicalTableId,
  cmpSupportingRow,
  cmpResultMemberEntry,
  cmpProgramFactEntry,
  coveredInputTrace,
  eprintln,
  QueryAuthorizationMode,
  terminalRootOccurrenceIdWithRootUnion,
  relationUnionPresentationOrder,
  relationUnionParts,
  relationOutputProjectionIfPresent,
  relationUnionLeafProjections,
  coalesceAuthoritySourceIncrementals,
  coveredInputRecord,
  ResultTransitionsDefault,
  OutputOccurrenceIdSingleSource,
  ObjectId,
} from "../../../_extern/node-qe-b.js";
import {
  readViewKeyFor,
  isPublicResultMember,
  publicResultMemberOccurrenceId,
  aggregateResultMemberRowUuid,
  replaceStaleAuthoritativeOccurrenceMember,
} from "../query_eval.js";
import { PreparedClaimBindingMode } from "./bindings.js";

export type CoveredInputSource = { id: number; table: string; descriptor: RecordDescriptor };

export class CoveredInputReceiver {
  localAuthority = new LocalAuthorityReconciliation();
  installedClosure: [AuthorityResultKey, number] | undefined;
  installedGeneration: number | undefined;
  installedRecords = new SortedMap<any, SortedMap<SupportingRow, Uint8Array>>(
    cmpGlobalPhysicalTableId,
  );
  constructor(
    public sources: SortedMap<any, CoveredInputSource>,
    public readView: ReadViewSpec,
  ) {}
  isEmpty(): boolean {
    return this.sources.size === 0;
  }
}

export type SubscriptionPreparedPlan = { plan: any; authorizationMode: any };

export class LocalMaintainedViewSubscription {
  subscription!: MultisinkSubscription;
  retainedPreparedPlan: SubscriptionPreparedPlan | undefined;
  maintained: any;
  terminalSchemas: any;
  tables!: Map<string, TableSchema>;
  resultQuery!: Query;
  resultTable!: string;
  resultSchemaVersion!: SchemaVersionId;
  resultSelect: string[] | undefined;
  resultRelationProjection: any[] | undefined;
  resultRelationProjections: Map<string, any[]> | undefined;
  resultSet = new SortedSet<ResultMemberEntry>(cmpResultMemberEntry);
  resultPayloads = new SortedMap<ResultMemberEntry, ResultMemberPayloadEntry>(cmpResultMemberEntry);
  programFacts = new SortedSet<any>(cmpProgramFactEntry);
  rootOccurrenceIds: OutputOccurrenceId[] = [];
  initialReceived = false;
  coveredInputReceiver!: CoveredInputReceiver;

  constructor(init: Partial<LocalMaintainedViewSubscription>) {
    Object.assign(this, init);
  }

  decodedTerminalRecords(): Map<OutputOccurrenceId, any> {
    const out = new Map();
    const rootUnion = this.terminalRootLayout()?.rootUnionArm ?? false;
    for (const [key, record] of this.maintained.decodedTerminalRecords())
      out.set(terminalRootOccurrenceIdWithRootUnion(key, rootUnion), record);
    return out;
  }
  terminalRootLayout(): any {
    return this.terminalSchemas.terminalRootLayout();
  }
  hasRootCollector(): boolean {
    return this.terminalSchemas.hasRootCollector();
  }
  initialSnapshotReceived(): boolean {
    return this.initialReceived;
  }
  relationProjectionForMember(member: ResultMemberEntry): any[] | undefined {
    const occurrence = publicResultMemberOccurrenceId(
      member,
      this.resultTable,
      this.resultQuery.aggregate !== undefined,
    );
    if (occurrence === undefined)
      throw Err.InvalidStoredValue("maintained union member has no occurrence identity");
    return this.relationProjectionForOccurrence(occurrence);
  }
  relationProjectionForOccurrence(occurrence: OutputOccurrenceId): any[] | undefined {
    const projections = this.resultRelationProjections;
    if (!projections) return this.resultRelationProjection;
    const arm = occurrence.unionArms().find(([position]: [number, string]) => position === 0);
    if (!arm) throw Err.InvalidStoredValue("maintained union member has no position-0 arm label");
    const projection = projections.get(arm[1]);
    if (!projection)
      throw Err.InvalidStoredValue("maintained union member has an unknown arm label");
    return projection;
  }
  subscriptionId(): number {
    return this.subscription.id();
  }
  usesLogicalTables(changed: Set<string>): boolean {
    return (
      this.maintained.targetedRefreshUncertain ||
      [...this.maintained.targetedRefreshTables].some((t: string) => changed.has(t))
    );
  }
  hasCoveredInputSources(): boolean {
    return !this.coveredInputReceiver.isEmpty();
  }
  hasInstalledCoveredClosure(key: AuthorityResultKey, generation: number): boolean {
    const installed = this.coveredInputReceiver.installedClosure;
    return (
      installed !== undefined &&
      debugFmt(installed[0]) === debugFmt(key) &&
      installed[1] === generation
    );
  }
  needsOrderedRelationSnapshot(): boolean {
    const q = this.resultQuery;
    const queryHasWindow = q.orderBy.length > 0 || q.limit !== undefined || q.offset !== 0;
    const relation = q.relation;
    const parts = relation ? relationUnionParts(relation.rel) : undefined;
    const unionWindow =
      relation !== undefined &&
      (relationUnionPresentationOrder(relation) !== undefined ||
        (parts !== undefined && (parts.limit !== undefined || parts.offset !== undefined)));
    return (
      (this.resultRelationProjection !== undefined ||
        this.resultRelationProjections !== undefined) &&
      (queryHasWindow || unionWindow)
    );
  }
}

export type LocalMaintainedViewSubscriptionUpdate =
  | { kind: "Flat"; added: [OutputOccurrenceId, CurrentRow][]; removed: OutputOccurrenceId[] }
  | { kind: "OrderedWindow"; snapshot: RelationSnapshot; occurrenceIds: OutputOccurrenceId[] }
  | { kind: "Structured"; terminalOperations: any[] };

enum CoveredInstall {
  DetachCold,
  Complete,
}

function openMaintainedViewSubscriptionInAuthorizationMode(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  tier: Tier,
  readView: ReadViewSpec,
  retained: SubscriptionPreparedPlan | undefined,
  mode: any,
) {
  return this.openMaintainedViewSubscriptionInAuthorizationModeWithWaker(
    shape,
    binding,
    identity,
    tier,
    readView,
    retained,
    mode,
    false,
    undefined,
  );
}

async function openMaintainedViewSubscriptionInAuthorizationModeWithWaker(
  this: NodeState,
  shape: ValidatedQuery,
  binding: Binding,
  identity: AuthorSubject,
  tier: Tier,
  readView: ReadViewSpec,
  retained: SubscriptionPreparedPlan | undefined,
  mode: any,
  pendingOverlay: boolean,
  waker: (() => void) | undefined,
): Promise<[LocalMaintainedViewSubscription, RelationSnapshot]> {
  if (retained && retained.authorizationMode !== mode)
    throw Err.InvalidStoredValue(
      "maintained subscription retained a plan from another authorization mode",
    );
  const settledBindingView =
    mode === QueryAuthorizationMode.ClientLocal
      ? this.clientSettledBindingViewKeyForQuery(shape, binding, tier, readView)
      : undefined;
  const settledKey =
    settledBindingView !== undefined
      ? this.uniqueAuthorityResultKeyForBindingView(settledBindingView)
      : undefined;
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
    tier,
    readView,
    readViewKeyFor(tier, readView),
    mode,
    settledBindingView,
    settledKey,
    PreparedClaimBindingMode.Strict,
    pendingOverlay,
    waker,
  );
  const query = shape.query();
  const relation = query.relation;
  const local = new LocalMaintainedViewSubscription({
    subscription,
    retainedPreparedPlan: retained,
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
    initialReceived,
    coveredInputReceiver: new CoveredInputReceiver(coveredInputSources, readView),
  });
  if (coveredInputTrace()) {
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=open root_terminal=${local.hasRootCollector()} sources=${local.coveredInputReceiver.sources.size} initial_received=${local.initialReceived}`,
    );
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=initial_transitions adds=${transitions.adds.length} removes=${transitions.removes.length} facts_adds=${transitions.programFactAdds.length} facts_removes=${transitions.programFactRemoves.length} terminal_ops=${transitions.terminalOperations.length}`,
    );
  }
  await this.applyLocalMaintainedViewTransitions(local, transitions);
  if (local.hasCoveredInputSources() && settledKey !== undefined)
    await this.installOpenedLocalCoveredReceiver(local, settledKey, waker);
  const initial = await this.materializeLocalMaintainedRelationSnapshotWithOccurrences(local);
  local.rootOccurrenceIds = initial.rootOccurrenceIds;
  return [local, initial.snapshot];
}

function drainLocalMaintainedViewSubscription(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey | undefined,
) {
  return this.drainLocalMaintainedViewSubscriptionWithWaker(local, key, undefined);
}

async function drainLocalMaintainedViewSubscriptionWithWaker(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey | undefined,
  waker: (() => void) | undefined,
): Promise<LocalMaintainedViewSubscriptionUpdate | undefined> {
  const [update] = await this.drainLocalMaintainedViewSubscriptionPreservingRowsWithWaker(
    local,
    key,
    new Set(),
    waker,
  );
  return update;
}

async function drainLocalMaintainedViewSubscriptionPreservingRowsWithWaker(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey | undefined,
  _preserved: Set<string>,
  waker: (() => void) | undefined,
): Promise<[LocalMaintainedViewSubscriptionUpdate | undefined, boolean]> {
  const transitions = await this.drainLocalMaintainedViewSubscriptionTransitions(local, key, waker);
  if (!transitions) return [undefined, false];
  return [await this.applyLocalMaintainedViewTransitions(local, transitions), false];
}

async function drainLocalMaintainedViewSubscriptionStateWithWaker(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey | undefined,
  waker: (() => void) | undefined,
): Promise<boolean> {
  const transitions = await this.drainLocalMaintainedViewSubscriptionTransitions(local, key, waker);
  if (!transitions) return false;
  await this.applyLocalMaintainedViewTransitionsInner(local, transitions, false);
  return true;
}

async function installOpenedLocalCoveredReceiver(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey,
  waker: (() => void) | undefined,
): Promise<any> {
  if (!(await this.replaceLocalMaintainedCoveredInputs(local, key))) return undefined;
  await this.driveReadyQueryRuntimeWithWaker(waker);
  const transitions =
    (await this.drainLocalMaintainedViewSubscriptionTransitions(local, key, waker)) ??
    ResultTransitionsDefault();
  await this.applyLocalMaintainedViewTransitionsInner(local, structuredClone(transitions), false);
  return transitions;
}

function localMaintainedAuthorityReconciliationDue(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey,
): boolean {
  return this.coveredInputReceiverReconciliationDue(local.coveredInputReceiver, key);
}

function coveredInputReceiverReconciliationDue(
  this: NodeState,
  receiver: CoveredInputReceiver,
  key: AuthorityResultKey,
): boolean {
  if (receiver.isEmpty()) return false;
  const result = this.query.authorityResults.get(key);
  if (!result || result.sourceClosure.kind !== "Claimed") return false;
  return receiver.localAuthority.isDue(key, result.sourceClosure.generation);
}

function coveredReceiverEvaluationPending(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
): boolean {
  return (
    local.hasCoveredInputSources() &&
    this.subscriptionHasPendingQueryEvaluation(local.subscriptionId())
  );
}

async function replaceLocalMaintainedCoveredInputs(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey,
): Promise<boolean> {
  const result = this.query.authorityResults.get(key);
  if (!result)
    throw Err.InvalidStoredValue("covered input reconciliation has no exact authority receipt");
  if (result.sourceClosure.kind === "Pending") return false;
  const generation = result.sourceClosure.generation;
  const receiver = local.coveredInputReceiver;
  const installedForAuthority =
    receiver.installedClosure !== undefined &&
    debugFmt(receiver.installedClosure[0]) === debugFmt(key);
  if (!installedForAuthority || !result.sourceIncrementals.length) {
    if (installedForAuthority && receiver.installedGeneration === generation) return false;
    return this.replaceCoveredInputReceiverIn(
      receiver,
      local.resultSchemaVersion,
      key,
      this.localCoveredInstall(),
    );
  }
  if (receiver.installedGeneration === generation) return false;
  if (receiver.installedGeneration === undefined)
    throw Err.InvalidStoredValue("installed receiver has a source receipt");
  const incremental = coalesceAuthoritySourceIncrementals(
    result.sourceIncrementals,
    receiver.installedGeneration,
    generation,
  );
  return this.applyCoveredInputReceiverIncremental(
    receiver,
    local.resultSchemaVersion,
    key,
    incremental,
  );
}

async function applyCoveredInputReceiverIncremental(
  this: NodeState,
  receiver: CoveredInputReceiver,
  resultSchemaVersion: SchemaVersionId,
  key: AuthorityResultKey,
  incremental: any,
): Promise<boolean> {
  if (receiver.installedGeneration !== incremental.predecessorGeneration)
    throw Err.InvalidStoredValue(
      "covered input incremental delta does not name receiver predecessor",
    );
  const staged = new SortedMap<SupportingRow, Uint8Array | undefined>(cmpSupportingRow);
  const changes = new SortedMap<any, [Uint8Array[], Uint8Array[]]>(cmpGlobalPhysicalTableId);
  for (const input of incremental.removes) {
    const records = receiver.installedRecords.get(input.physicalTable);
    if (!records)
      throw Err.InvalidStoredValue(
        "incremental covered input removes an unknown source occurrence",
      );
    const record = records.get(input);
    if (record) {
      staged.set(input, undefined);
      changes.getOrInsert(input.physicalTable, () => [[], []])[1].push(record);
    } else if (input.version.layer === "Content")
      throw Err.InvalidStoredValue(
        "incremental covered input removes a content witness absent from receiver predecessor",
      );
  }
  for (const input of incremental.adds) {
    const runtimeSource = receiver.sources.get(input.physicalTable);
    if (!runtimeSource)
      throw Err.InvalidStoredValue("incremental covered input names no compiled source occurrence");
    const record = await this.coveredInputRuntimeRecord(
      input,
      runtimeSource,
      resultSchemaVersion,
      receiver.readView,
    );
    if (!record) continue;
    const present = staged.has(input)
      ? staged.get(input) !== undefined
      : receiver.installedRecords.get(input.physicalTable)!.has(input);
    if (present)
      throw Err.InvalidStoredValue(
        "incremental covered input duplicates a receiver content witness",
      );
    staged.set(input, record);
    changes.getOrInsert(input.physicalTable, () => [[], []])[0].push(record);
  }
  const deltas = [...changes].map(([source, [adds, removes]]) => {
    const runtimeSource = receiver.sources.get(source)!;
    return { id: runtimeSource.id, descriptor: runtimeSource.descriptor, adds, removes };
  });
  let metrics;
  try {
    metrics =
      this.localCoveredInstall() === CoveredInstall.DetachCold
        ? await this.database.applyInputSourceDeltasDetachingCold(deltas)
        : await this.database.applyInputSourceDeltas(deltas);
  } catch (e) {
    throw Err.Groove(e);
  }
  if (coveredInputTrace())
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=incremental_receiver_delta predecessor=${incremental.predecessorGeneration} generation=${incremental.generation} tick=${metrics.tick} processed=${metrics.recordsProcessed}`,
    );
  receiver.localAuthority.replaceSource(key, incremental.generation);
  receiver.installedClosure = [key, incremental.generation];
  receiver.installedGeneration = incremental.generation;
  for (const [input, record] of staged) {
    const records = receiver.installedRecords.get(input.physicalTable)!;
    if (record) records.set(input, record);
    else records.delete(input);
  }
  return true;
}

function replaceCoveredInputReceiver(
  this: NodeState,
  receiver: CoveredInputReceiver,
  resultSchemaVersion: SchemaVersionId,
  key: AuthorityResultKey,
) {
  return this.replaceCoveredInputReceiverIn(
    receiver,
    resultSchemaVersion,
    key,
    CoveredInstall.Complete,
  );
}

function localCoveredInstall(this: NodeState): CoveredInstall {
  return this.detachesCoveredChunkWaits() ? CoveredInstall.DetachCold : CoveredInstall.Complete;
}

async function replaceCoveredInputReceiverIn(
  this: NodeState,
  receiver: CoveredInputReceiver,
  resultSchemaVersion: SchemaVersionId,
  key: AuthorityResultKey,
  install: CoveredInstall,
): Promise<boolean> {
  if (receiver.isEmpty()) return false;
  const remapped = new SortedMap<any, CoveredInputSource>(cmpGlobalPhysicalTableId);
  for (const [, source] of receiver.sources)
    remapped.set(this.scopePhysicalTable(resultSchemaVersion, source.table), source);
  receiver.sources = remapped;
  receiver.installedClosure = undefined;
  const result = this.query.authorityResults.get(key);
  if (!result)
    throw Err.InvalidStoredValue("covered input replacement has no exact authority receipt");
  if (result.sourceClosure.kind === "Pending") {
    if (coveredInputTrace())
      eprintln(
        `JAZZ_COVERED_INPUT_TRACE stage=covered_closure_pending sources=${receiver.sources.size}`,
      );
    return false;
  }
  const generation = result.sourceClosure.generation;
  const records = new SortedMap<any, SortedMap<SupportingRow, Uint8Array>>(
    cmpGlobalPhysicalTableId,
  );
  for (const [source] of receiver.sources) records.set(source, new SortedMap(cmpSupportingRow));
  for (const input of [...result.coveredInputVersions.valuesArray()] as SupportingRow[]) {
    const runtimeSource = receiver.sources.get(input.physicalTable);
    if (!runtimeSource)
      throw Err.InvalidStoredValue("authority covered input names no compiled source occurrence");
    const record = await this.coveredInputRuntimeRecord(
      input,
      runtimeSource,
      resultSchemaVersion,
      receiver.readView,
    );
    if (record) records.get(input.physicalTable)!.set(input, record);
  }
  const counts = [...records].map(([source, rows]) => [source, rows.size]);
  const replacements = [...receiver.sources].map(([source, runtime]) => ({
    id: runtime.id,
    descriptor: runtime.descriptor,
    records: [...(records.get(source)?.valuesArray() ?? [])],
  }));
  let metrics;
  try {
    metrics =
      install === CoveredInstall.DetachCold
        ? await this.database.replaceInputSourcesDetachingCold(replacements)
        : await this.database.replaceInputSources(replacements);
  } catch (e) {
    throw Err.Groove(e);
  }
  if (coveredInputTrace())
    eprintln(
      `JAZZ_COVERED_INPUT_TRACE stage=replaced sources=${debugFmt(counts)} tick=${metrics.tick} processed=${metrics.recordsProcessed} notifications=${metrics.notificationsSent} notification_records=${metrics.notificationRecords}`,
    );
  receiver.localAuthority.replaceSource(key, generation);
  receiver.installedClosure = [key, generation];
  receiver.installedGeneration = generation;
  receiver.installedRecords = records;
  return true;
}

async function coveredInputRuntimeRecord(
  this: NodeState,
  input: SupportingRow,
  runtimeSource: CoveredInputSource,
  resultSchemaVersion: SchemaVersionId,
  readView: ReadViewSpec,
): Promise<Uint8Array | undefined> {
  const sourceTable = this.tableInSchema(runtimeSource.table, resultSchemaVersion);
  const version = await this.coveredInputVersion(input, resultSchemaVersion);
  if (!version) throw Err.MissingTransaction(input.version.tx);
  if (version.isDeleted()) return undefined;
  let row = this.projectedCurrentRowFromMaterializedVersionInReadSchema(
    resultSchemaVersion,
    version,
  );
  if (!row)
    throw Err.InvalidStoredValue(
      "authority covered content version cannot materialize a current row",
    );
  row = this.projectCoveredInputRowInReadView(
    sourceTable,
    resultSchemaVersion,
    readView,
    version,
    row,
  );
  if (row.table() !== sourceTable.name)
    throw Err.InvalidStoredValue(
      "authority covered input does not project into its compiled source schema",
    );
  return coveredInputRecord(
    sourceTable,
    runtimeSource.descriptor,
    row,
    version.schemaVersionAlias(),
    version.branchKey(),
  );
}

function projectCoveredInputRowInReadView(
  this: NodeState,
  table: TableSchema,
  schema: SchemaVersionId,
  readView: ReadViewSpec,
  version: VersionRow,
  row: CurrentRow,
): CurrentRow {
  if (readView.source.kind !== "BranchView") return row;
  const entry = this.catalogue.catalogueSchemas.get(schema);
  if (!entry) throw Err.InvalidStoredValue("covered input read schema is unknown");
  let head;
  try {
    [head] = entry.schema.projectBranchViewSelector(table, readView.source.head);
  } catch (e) {
    throw Err.InvalidBranchKey(e);
  }
  const winners = this.materializeBranchViewWinners(
    table.name,
    schema,
    head,
    new Map(),
    new Map(),
    new Map([[version.rowUuid(), version]]),
    new Map(),
  );
  const first = winners[0];
  if (!first) throw Err.InvalidStoredValue("covered branch input did not materialize");
  return first;
}

async function drainLocalMaintainedViewSubscriptionTransitions(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  key: AuthorityResultKey | undefined,
  waker: (() => void) | undefined,
): Promise<any> {
  if (key !== undefined && this.localMaintainedAuthorityReconciliationDue(local, key)) {
    if (!(await this.replaceLocalMaintainedCoveredInputs(local, key))) return undefined;
  }
  await this.driveReadyQueryRuntimeWithWaker(waker);
  if (key !== undefined && this.coveredReceiverEvaluationPending(local)) return undefined;
  const states = new SortedMap<ResultMemberEntry, [boolean, boolean]>(cmpResultMemberEntry);
  const payloadStates = new SortedMap<ResultMemberEntry, [any, any]>(cmpResultMemberEntry);
  const factStates = new SortedMap<any, [boolean, boolean]>(cmpProgramFactEntry);
  const terminalOperations: any[] = [];
  const track = <K, V>(map: SortedMap<K, [V, V]>, k: K, before: () => V, after: V) => {
    const existing = map.get(k);
    if (existing) existing[1] = after;
    else map.set(k, [before(), after]);
  };
  for (;;) {
    const received = local.subscription.tryRecv();
    if (received.kind === "Empty") break;
    if (received.kind === "Disconnected") throw Err.SubscriptionClosed();
    const deltas = received.deltas;
    local.initialReceived = true;
    if (coveredInputTrace())
      eprintln(
        `JAZZ_COVERED_INPUT_TRACE stage=drain sinks=${deltas.sinks.size} terminals=${deltas.terminalSinks.size}`,
      );
    const t = local.maintained.applyMultisinkDeltas(
      deltas,
      local.terminalSchemas,
      local.tables,
      this.nodeAliases,
    );
    terminalOperations.push(...t.terminalOperations);
    if (coveredInputTrace())
      eprintln(
        `JAZZ_COVERED_INPUT_TRACE stage=transitions terminal_ops=${terminalOperations.length} adds=${t.adds.length} removes=${t.removes.length}`,
      );
    for (const e of t.adds) track(states, e, () => local.resultSet.has(e), true);
    for (const e of t.removes) track(states, e, () => local.resultSet.has(e), false);
    for (const m of t.resultPayloadRemoves)
      track(payloadStates, m, () => local.resultPayloads.get(m), undefined);
    for (const [m, p] of t.resultPayloadAdds)
      track(payloadStates, m, () => local.resultPayloads.get(m), p);
    for (const f of t.programFactAdds) track(factStates, f, () => local.programFacts.has(f), true);
    for (const f of t.programFactRemoves)
      track(factStates, f, () => local.programFacts.has(f), false);
  }
  if (
    drainedTransitionIsEmpty(
      !states.size,
      !payloadStates.size,
      !factStates.size,
      terminalOperations,
    )
  )
    return undefined;
  const transitions = {
    ...ResultTransitionsDefault(),
    authoritativeMembershipChanged: false,
    terminalOperations,
  };
  for (const [entry, [before, after]] of states) {
    if (!before && after) transitions.adds.push(entry);
    else if (before && !after) transitions.removes.push(entry);
  }
  for (const [member, [before, after]] of payloadStates) {
    if (before === undefined && after !== undefined)
      transitions.resultPayloadAdds.push([member, after]);
    else if (before !== undefined && after === undefined)
      transitions.resultPayloadRemoves.push(member);
    else if (before !== undefined && after !== undefined && debugFmt(before) !== debugFmt(after)) {
      transitions.resultPayloadRemoves.push(member);
      transitions.resultPayloadAdds.push([member, after]);
    }
  }
  for (const [fact, [before, after]] of factStates) {
    if (!before && after) transitions.programFactAdds.push(fact);
    else if (before && !after) transitions.programFactRemoves.push(fact);
  }
  if (local.resultQuery.aggregate !== undefined) {
    transitions.resultPayloadAdds = transitions.programFactAdds
      .filter((f: any) => f.kind === "ResultPayload")
      .map((f: any) => [f.payload.member, f.payload]);
    transitions.resultPayloadRemoves = transitions.programFactRemoves
      .filter((f: any) => f.kind === "ResultPayload")
      .map((f: any) => f.payload.member);
  }
  return transitions;
}

function applyLocalMaintainedViewTransitions(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  transitions: any,
) {
  return this.applyLocalMaintainedViewTransitionsInner(local, transitions, true);
}

async function applyLocalMaintainedViewTransitionsInner(
  this: NodeState,
  local: LocalMaintainedViewSubscription,
  transitions: any,
  materializeUpdate: boolean,
): Promise<LocalMaintainedViewSubscriptionUpdate> {
  const structuredOutput = local.hasRootCollector();
  const aggregateQuery = local.resultQuery.aggregate !== undefined;
  const isAggregateMember = (m: ResultMemberEntry) => aggregateQuery && m.kind === "Synthetic";
  const terminalOperations = transitions.terminalOperations;
  const aggregateReplacements = new Set<string>(
    transitions.adds
      .filter(isAggregateMember)
      .map((m: ResultMemberEntry) => debugFmt(aggregateResultMemberRowUuid(m))),
  );
  const added: [OutputOccurrenceId, CurrentRow][] = [];
  const removed: OutputOccurrenceId[] = [];
  for (const member of transitions.resultPayloadRemoves) local.resultPayloads.delete(member);
  for (const [member, payload] of transitions.resultPayloadAdds)
    if (isPublicResultMember(member, local.resultTable, aggregateQuery))
      local.resultPayloads.set(member, payload);
  for (const member of transitions.adds) {
    if (!isPublicResultMember(member, local.resultTable, aggregateQuery)) continue;
    replaceStaleAuthoritativeOccurrenceMember(
      local.resultSet,
      local.resultPayloads,
      transitions.authoritativeMemberAdds,
      member,
      local.resultTable,
      aggregateQuery,
    );
    if (local.resultSet.add(member) && materializeUpdate && !structuredOutput) {
      const row = await this.materializeLocalMaintainedViewResultMember(local, member);
      const occurrence = row
        ? publicResultMemberOccurrenceId(member, local.resultTable, aggregateQuery)
        : undefined;
      if (row && occurrence !== undefined) {
        if (coveredInputTrace())
          eprintln(
            `JAZZ_COVERED_INPUT_TRACE stage=local_maintained_added table=${row.table()} occurrence=${debugFmt(occurrence)}`,
          );
        added.push([occurrence, row]);
      }
    }
  }
  for (const member of transitions.removes) {
    if (!isPublicResultMember(member, local.resultTable, aggregateQuery)) continue;
    if (!local.resultSet.delete(member) || !materializeUpdate || structuredOutput) continue;
    const occurrence = member.outputOccurrenceId();
    if (occurrence !== undefined) removed.push(occurrence);
    else if (isAggregateMember(member)) {
      const rowUuid = aggregateResultMemberRowUuid(member);
      const replacementIsCurrent = local.resultSet
        .toArray()
        .some((c) => isAggregateMember(c) && equalBytes(aggregateResultMemberRowUuid(c), rowUuid));
      if (!aggregateReplacements.has(debugFmt(rowUuid)) && !replacementIsCurrent)
        removed.push(OutputOccurrenceIdSingleSource(ObjectId.fromUuid(rowUuid)));
    }
  }
  for (const fact of transitions.programFactRemoves) local.programFacts.delete(fact);
  for (const fact of transitions.programFactAdds) local.programFacts.add(fact);
  if (structuredOutput) return { kind: "Structured", terminalOperations };
  if (materializeUpdate && (aggregateQuery || local.needsOrderedRelationSnapshot())) {
    const materialized =
      await this.materializeLocalMaintainedRelationSnapshotWithOccurrences(local);
    local.rootOccurrenceIds = [...materialized.rootOccurrenceIds];
    return {
      kind: "OrderedWindow",
      snapshot: materialized.snapshot,
      occurrenceIds: materialized.rootOccurrenceIds,
    };
  }
  if (terminalOperations.length)
    throw Err.InvalidStoredValue("non-terminal app-row path emitted collector terminal operations");
  return { kind: "Flat", added, removed };
}

function drainedTransitionIsEmpty(
  statesEmpty: boolean,
  payloadEmpty: boolean,
  factEmpty: boolean,
  terminalOperations: any[],
): boolean {
  return statesEmpty && payloadEmpty && factEmpty && !terminalOperations.length;
}

installNodeMethods({
  openMaintainedViewSubscriptionInAuthorizationMode,
  openMaintainedViewSubscriptionInAuthorizationModeWithWaker,
  drainLocalMaintainedViewSubscription,
  drainLocalMaintainedViewSubscriptionWithWaker,
  drainLocalMaintainedViewSubscriptionPreservingRowsWithWaker,
  drainLocalMaintainedViewSubscriptionStateWithWaker,
  installOpenedLocalCoveredReceiver,
  localMaintainedAuthorityReconciliationDue,
  coveredInputReceiverReconciliationDue,
  coveredReceiverEvaluationPending,
  replaceLocalMaintainedCoveredInputs,
  applyCoveredInputReceiverIncremental,
  replaceCoveredInputReceiver,
  localCoveredInstall,
  replaceCoveredInputReceiverIn,
  coveredInputRuntimeRecord,
  projectCoveredInputRowInReadView,
  drainLocalMaintainedViewSubscriptionTransitions,
  applyLocalMaintainedViewTransitions,
  applyLocalMaintainedViewTransitionsInner,
});
