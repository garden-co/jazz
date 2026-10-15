// Durable read surfaces: catalogue accessors, transaction audit, history, rejections and runtime pass-throughs.
import {
  type AuthorSubject,
  type GlobalTime,
  type NodeUuid,
  type RowUuid,
  type SchemaVersionId,
  type TxId,
  type TxTime,
  AUTHORITY_POLICY_BINDINGS_STORE,
  AuthorSubjectOps,
  AuthorSubjectSystem,
  PolicyBindingKey,
  QueryFrom,
  RejectedVersionRowRecord,
  RowAuthor,
  TransactionRowRecord,
  TxIdNew,
  TxTimeOps,
  authorSubjectEquals,
  cmpTxId,
  durabilityFromDiscriminant,
  durabilityRank,
  physicalRejectedVersionsTableName,
  rejectedTransactionPrimaryKey,
  rejectedVersionPrimaryKeyFromRecord,
  schemaIdKey,
  txIdKey,
  uuidEquals,
} from "../../../_extern/node-state.js";
import {
  type CurrentRow,
  type NodeState,
  AuthorityResultState,
  CatalogueBootstrapState,
  HistoricalRead,
  V,
  invalidStored,
  newQueryEngineReadMetrics,
  nodeErr,
} from "../index.js";

const GLOBAL_RANK = () => durabilityRank("Global");

/** Compare a request with durable provenance without turning node attribution back into the system capability. */
const durableAuthorMatches = (requested: AuthorSubject, stored: AuthorSubject) =>
  authorSubjectEquals(requested, AuthorSubjectSystem)
    ? AuthorSubjectOps.isSystemAt(stored)
    : authorSubjectEquals(stored, requested);

const madeByOf = (record: any): AuthorSubject => {
  try {
    return RowAuthor.fromRecord(
      record.getRecord(TransactionRowRecord.FIELD_MADE_BY_IDX),
    ).asAuthorSubject();
  } catch {
    throw nodeErr("Groove", { kind: "NonCanonicalRecord" });
  }
};

const dedupSorted = (ids: TxId[]) => {
  ids.sort(cmpTxId);
  return ids.filter((id, i) => i === 0 || cmpTxId(ids[i - 1], id) !== 0);
};

export const durableMethods = {
  syncMetricsRef(this: NodeState) {
    return this.syncMetrics;
  },
  recordDroppedPeerRequest(this: NodeState): void {
    this.syncMetrics.droppedPeerRequestMessages++;
  },
  recordTransportBackpressureRetry(this: NodeState): void {
    this.syncMetrics.transportBackpressureRetries++;
  },
  recordAuthoritativeResetMissingPayloadFallback(this: NodeState): void {
    this.syncMetrics.authoritativeResetMissingPayloadFallbacks++;
  },
  recordPeerPayloadInventoryMissingFallback(this: NodeState): void {
    this.syncMetrics.peerPayloadInventoryMissingFallbacks++;
  },
  queryEngineReadMetricsRef(this: NodeState) {
    return this.queryEngineReadMetrics;
  },
  resetQueryEngineReadMetrics(this: NodeState): void {
    this.queryEngineReadMetrics = newQueryEngineReadMetrics();
  },
  catalogueSchemasRef(this: NodeState) {
    return this.catalogue.catalogueSchemas;
  },
  schemaWithActivePermissions(this: NodeState, id: SchemaVersionId) {
    if (uuidEquals(id, this.catalogue.activeSchema.schema))
      return this.catalogue.activeSchema.compiled;
    return this.catalogue.catalogueSchemas.get(schemaIdKey(id))?.schema;
  },
  /** Highest contiguously activated authoritative catalogue position. */
  activeCatalogueSeqValue(this: NodeState): number {
    return this.catalogue.activeCatalogueSeq;
  },
  catalogueLensesRef(this: NodeState) {
    return this.catalogue.catalogueLenses;
  },
  catalogueBootstrapStateValue(this: NodeState): CatalogueBootstrapState {
    return this.catalogueBootstrapState;
  },
  /** The authoritative current-write pointer, failing closed before the first trusted catalogue. */
  tryCurrentWriteSchema(this: NodeState) {
    this.requireCatalogueReady();
    return this.catalogue.activeSchema.wirePointer();
  },
  tryCurrentSchema(this: NodeState) {
    this.requireCatalogueReady();
    return this.catalogue.schema;
  },
  requireCatalogueReady(this: NodeState): void {
    this.database.ensureUsable();
    if (this.catalogueBootstrapState === CatalogueBootstrapState.Uninitialized)
      throw nodeErr("CatalogueUninitialized");
  },
  currentWriteSchema(this: NodeState) {
    return this.tryCurrentWriteSchema();
  },
  catalogueSnapshot(this: NodeState) {
    this.requireCatalogueReady();
    const c = this.catalogue;
    const activeKey = schemaIdKey(c.activeSchema.schema);
    const schemas = [...c.catalogueSchemas.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, schema]) =>
        key === activeKey ? { ...schema, schema: c.activeSchema.compiled } : schema,
      );
    const lineages = [...c.activeLineagesByTarget.values()]
      .map((l) => [l.catalogueSeq, l.publication] as const)
      .sort((a, b) => a[0] - b[0]);
    // The write pointer is independent of the authority's unique genesis.
    const roots = [...c.catalogueSchemas.keys()].filter((k) => !c.activeLineagesByTarget.has(k));
    if (!roots.length) throw invalidStored("catalogue genesis schema is missing");
    if (roots.length > 1) throw invalidStored("catalogue has multiple genesis schemas");
    const genesisMapping = c.physicalMappings.get(roots[0]);
    if (!genesisMapping) throw invalidStored("genesis physical mapping missing");
    return {
      genesisPhysicalIdentities: genesisMapping.identities,
      schemas,
      lineages,
      currentWriteSchema: c.activeSchema.wirePointer(),
    };
  },
  /** A historical read handle at an exact global settle position. */
  at(this: NodeState, position: GlobalTime): HistoricalRead {
    return new HistoricalRead(this, position);
  },
  /** A historical read handle for the latest settle position at or before `time` (best-effort under skew). */
  async atTime(this: NodeState, time: TxTime): Promise<HistoricalRead> {
    return this.at(await this.resolveTimeTravelPosition(time));
  },
  isHistoryCompleteFor(this: NodeState, _shape: any, position: GlobalTime): boolean {
    return this.historyComplete && position <= this.clock.committedGlobalTime;
  },
  isHistoryComplete(this: NodeState): boolean {
    return this.historyComplete;
  },
  /** Current rows for a subscription at the requested tier, through the same local Groove terminal as serving. */
  async subscriptionCurrentRows(
    this: NodeState,
    table: string,
    settled: "None" | "Local" | "Global",
  ): Promise<CurrentRow[]> {
    if (settled !== "Global") return this.currentRows(table, settled);
    let shape: any;
    let binding: any;
    try {
      shape = QueryFrom(table).validate(this.catalogue.schema);
      binding = shape.bind(new Map());
    } catch (e) {
      throw nodeErr("Query", e);
    }
    if (this.isHistoryComplete())
      return this.queryRowsWithPreparedPlanForIdentity(
        shape,
        binding,
        "Global",
        undefined,
        AuthorSubjectSystem,
      );
    // A partial client consumes the scoped CoveredInput closure installed by the received subscription.
    return this.queryRowsForClient(shape, binding, "Global", AuthorSubjectSystem);
  },
  /** The legacy transaction fate tuple projected from stored status. */
  async transactionState(
    this: NodeState,
    txId: TxId,
  ): Promise<[any, GlobalTime | undefined, any] | undefined> {
    let state: any;
    try {
      state = await this.queryTransactionState(txId);
    } catch {
      return undefined;
    }
    if (!state) return undefined;
    const [fate, globalTime, stored] = state;
    return [fate, globalTime, this.pendingPersistence.has(txId) ? "None" : stored];
  },
  /** The durable audit record for a transaction, including rejected transactions. */
  async transactionRecord(this: NodeState, txId: TxId): Promise<any> {
    try {
      return (await this.queryTransaction(txId))?.toRecord();
    } catch {
      return undefined;
    }
  },
  /** Locally originated transactions (same node and author) that still need upstream settlement. */
  async pendingTransactionIdsFor(
    this: NodeState,
    node: NodeUuid,
    author: AuthorSubject,
  ): Promise<TxId[]> {
    return (await this.pendingTransactionScanFor(node, author)).txIds;
  },
  /** Unsettled transactions by `author`, irrespective of their originating node. */
  pendingTransactionIdsForAuthor(this: NodeState, author: AuthorSubject) {
    return this.belowGlobalTransactionIds(author, false);
  },
  /** A Global synchronization barrier also needs the authority timestamp. */
  synchronizingTransactionIdsForAuthor(this: NodeState, author: AuthorSubject) {
    return this.belowGlobalTransactionIds(author, true);
  },
  /** Restrict by transaction origin, never by its SYSTEM display identity. */
  async synchronizingTransactionIdsForNode(this: NodeState, node: NodeUuid): Promise<TxId[]> {
    return (await this.belowGlobalTransactionIds(undefined, true)).filter((tx: TxId) =>
      uuidEquals(tx.node, node),
    );
  },
  async belowGlobalTransactionIds(
    this: NodeState,
    author: AuthorSubject | undefined,
    includeMissingTimestamp: boolean,
  ) {
    const candidates: [number, TxTime][] = [];
    for (const raw of await this.database.indexScanRaw("jazz_transactions", "by_global_time", [
      V("Nullable"),
    ])) {
      const record = raw.record();
      const fate = record.getEnum(TransactionRowRecord.FIELD_FATE_IDX);
      const madeBy = madeByOf(record);
      const durability = durabilityFromDiscriminant(
        record.getEnum(TransactionRowRecord.FIELD_DURABILITY_IDX),
      );
      if (
        (author !== undefined && !durableAuthorMatches(author, madeBy)) ||
        !(fate === 0 || fate === 1) ||
        (!includeMissingTimestamp && durabilityRank(durability) >= GLOBAL_RANK())
      )
        continue;
      candidates.push([
        Number(record.getU64(TransactionRowRecord.FIELD_NODE_ID_IDX)),
        record.getU64(TransactionRowRecord.FIELD_TIME_IDX),
      ]);
    }
    const txIds: TxId[] = [];
    for (const [alias, time] of candidates) {
      const node = await this.resolveNodeAlias(alias);
      if (node !== undefined) txIds.push(TxIdNew(time, node));
    }
    return dedupSorted(txIds);
  },
  /** Replayable local transactions in the null slice of `by_global_time`. */
  async pendingTransactionScanFor(this: NodeState, node: NodeUuid, author: AuthorSubject) {
    const scan = { txIds: [] as TxId[], recordsVisited: 0 };
    const nodeAlias = this.nodeAliases.get(node);
    if (nodeAlias === undefined) return scan;
    // `$madeBy` is durable provenance: a local system capability records its specific node origin.
    const durableAuthor = authorSubjectEquals(author, AuthorSubjectSystem)
      ? AuthorSubjectOps.systemAt(node)
      : author;
    for (const raw of await this.database.indexScanRaw("jazz_transactions", "by_global_time", [
      V("Nullable"),
    ])) {
      scan.recordsVisited++;
      const record = raw.record();
      if (
        Number(record.getU64(TransactionRowRecord.FIELD_NODE_ID_IDX)) !== nodeAlias ||
        !authorSubjectEquals(madeByOf(record), durableAuthor)
      )
        continue;
      const fate = record.getEnum(TransactionRowRecord.FIELD_FATE_IDX);
      if (
        !(fate === 0 || fate === 1) ||
        durabilityRank(
          durabilityFromDiscriminant(record.getEnum(TransactionRowRecord.FIELD_DURABILITY_IDX)),
        ) >= GLOBAL_RANK()
      )
        continue;
      scan.txIds.push(TxIdNew(record.getU64(TransactionRowRecord.FIELD_TIME_IDX), node));
    }
    scan.txIds = dedupSorted(scan.txIds);
    return scan;
  },
  /** Creator/updater provenance for a projected current row. */
  rowProvenance(this: NodeState, row: CurrentRow) {
    return row.provenance();
  },
  async currentRowTxId(this: NodeState, row: CurrentRow): Promise<TxId | undefined> {
    const projected = row.projectedTxAlias();
    if (!projected) return undefined;
    let node: NodeUuid | undefined;
    try {
      node = await this.resolveNodeAlias(projected[1]);
    } catch {
      return undefined;
    }
    return node === undefined ? undefined : TxIdNew(projected[0], node);
  },
  /** Discard authority proof during rebuild or eviction, preserving live receipt ordering. */
  invalidateSubscriptionScopes(this: NodeState): void {
    for (const [key, state] of this.query.authorityResults) {
      const fresh = new AuthorityResultState();
      fresh.appliedViewUpdateGeneration = state.appliedViewUpdateGeneration;
      this.query.authorityResults.set(key, fresh);
    }
    this.query.retainedRootWindowSources.clear();
    this.query.watermarksInvalidated = true;
  },
  async persistPolicyBindingDirectory(this: NodeState, policy: any): Promise<void> {
    const digest = policy.directoryDigest();
    let claims: any;
    try {
      claims = policy.directoryValue();
    } catch {
      throw invalidStored("policy binding claims must encode");
    }
    const store = this.database.directRecordStore(AUTHORITY_POLICY_BINDINGS_STORE);
    const key = [V("Bytes", digest)];
    const existing = await store.get(key);
    if (existing) {
      const subject = existing.getIdx(0);
      if (subject.kind !== "String")
        throw invalidStored("policy binding directory subject must be string");
      let identity: AuthorSubject;
      try {
        identity = AuthorSubjectOps.fromCanonical(subject.value);
      } catch {
        throw invalidStored("policy binding directory subject is invalid");
      }
      let stored: any;
      try {
        stored = PolicyBindingKey.fromDirectoryValue(identity, existing.getIdx(1));
      } catch {
        throw invalidStored("policy binding directory claims are invalid");
      }
      if (!stored.equals(policy))
        throw invalidStored("policy binding digest aliases a distinct exact policy identity");
      return;
    }
    await store.set(key, [V("String", AuthorSubjectOps.canonical(policy.identity)), claims]);
  },
  async close(this: NodeState): Promise<void> {
    await this.database.flush();
    await this.database.close();
  },
  /** Locally-originated rejected transactions retained for retry. */
  rejectedTransactions(this: NodeState): TxId[] {
    return [...this.rejections.rejectedTransactions.keysArray()];
  },
  rejectedTransaction(this: NodeState, txId: TxId) {
    return this.rejections.rejectedTransactions.get(txId);
  },
  /** Discard a locally-retained rejected transaction after the app acknowledges it. */
  async discardRejection(this: NodeState, txId: TxId): Promise<void> {
    if (!uuidEquals(txId.node, this.nodeUuidValue)) return;
    const alias = this.nodeAliases.get(this.nodeUuidValue);
    if (alias === undefined) return;
    const batch = this.database.openBatch();
    batch.delete("jazz_rejected_transactions", rejectedTransactionPrimaryKey(alias, txId));
    for (const tableId of this.physicalTableIds()) {
      const storageTable = physicalRejectedVersionsTableName(tableId);
      for (const raw of await this.database.primaryKeyScanRaw(storageTable, [
        V("U64", txId.time),
        V("U64", alias),
      ])) {
        const record = raw.record();
        if (
          Number(record.getU64(RejectedVersionRowRecord.FIELD_TX_NODE_ID_IDX)) !== alias ||
          record.getU64(RejectedVersionRowRecord.FIELD_TX_TIME_IDX) !== txId.time
        )
          continue;
        batch.delete(storageTable, rejectedVersionPrimaryKeyFromRecord(record));
      }
    }
    await this.applyAndPersistNodeBatch(batch);
    this.rejections.rejectedTransactions.delete(txId);
  },
  /** Stored edit-history entries for one row ordered by HLC observation order (no policy filtering). */
  async rowHistory(this: NodeState, table: string, rowUuid: RowUuid): Promise<any[]> {
    const entries: any[] = [];
    const isWinner = (winner: any, txId: TxId) => {
      if (!winner) return false;
      try {
        return txIdKey(this.versionTxId(winner)) === txIdKey(txId);
      } catch {
        return false;
      }
    };
    for (const version of await this.queryRowVersions(table, rowUuid)) {
      const txId = this.versionTxId(version);
      const tx = await this.queryTransaction(txId);
      if (!tx) throw nodeErr("MissingTransaction", txId);
      const localCurrent = isWinner(
        await this.queryLocalWinnerInBranch(table, version.branchKey(), rowUuid),
        txId,
      );
      const globalCurrent = isWinner(
        await this.queryGlobalWinnerInBranch(table, version.branchKey(), rowUuid),
        txId,
      );
      entries.push(version.toHistoryEntry(tx, localCurrent, globalCurrent));
    }
    entries.sort((a, b) => TxTimeOps.sortKeyCmp(a.txId(), b.txId()));
    return entries;
  },
  /** Consume the node and return the underlying groove database. */
  intoDatabase(this: NodeState) {
    return this.databaseSlot.intoInner();
  },
  unsubscribeGrooveSubscription(this: NodeState, id: any): boolean {
    return this.database.unsubscribe(id);
  },
  /** Resume suspended Groove evaluation until every active session completes. */
  async driveQueryRuntime(this: NodeState): Promise<void> {
    try {
      await this.database.driveProgress();
    } catch (e) {
      throw nodeErr("Groove", e);
    }
  },
  driveReadyQueryRuntime(this: NodeState) {
    return this.driveReadyQueryRuntimeWithWaker(undefined);
  },
  async driveReadyQueryRuntimeWithWaker(this: NodeState, waker?: () => void): Promise<void> {
    try {
      await this.database.driveReadyProgressWithWaker(waker);
    } catch (e) {
      throw nodeErr("Groove", e);
    }
  },
  hasPendingQueryRuntime(this: NodeState): boolean {
    return this.database.hasPendingProgress();
  },
  subscriptionHasPendingQueryRuntime(this: NodeState, subscription: any): boolean {
    return this.database.subscriptionHasPendingProgress(subscription);
  },
  subscriptionHasPendingQueryEvaluation(this: NodeState, subscription: any): boolean {
    return this.database.subscriptionHasPendingEvaluation(subscription);
  },
  async setInitialSyncFlushCadence(this: NodeState, every: number): Promise<void> {
    this.initialSyncFlushCadence = every;
    // The cadence only relaxes durability while the first snapshot is active.
    await this.database.setWriteFlushCadence(1);
  },
  async beginInitialSyncFlushCadence(this: NodeState): Promise<void> {
    const every = this.initialSyncFlushCadence;
    if (every === undefined || this.initialSyncFlushActive || this.initialSyncFlushCompleted)
      return;
    await this.database.setWriteFlushCadence(every);
    this.initialSyncFlushActive = true;
  },
  async finishInitialSyncFlushCadence(this: NodeState): Promise<void> {
    if (!this.initialSyncFlushActive) return;
    await this.database.flushWriteBoundary();
    await this.database.setWriteFlushCadence(1);
    this.initialSyncFlushActive = false;
    this.initialSyncFlushCompleted = true;
  },
  /** Check and register under the owner lock so settlement cannot race registration. */
  deferCatalogueForPersistence(this: NodeState, waker?: () => void): boolean {
    return this.database.waitForPublicationSettlement(waker);
  },
  physicalIdentityGenerationValue(this: NodeState): number {
    return this.physicalIdentityGeneration;
  },
  grooveRuntimeTokenValue(this: NodeState): number {
    return this.grooveRuntimeToken;
  },
  lastCommitMetrics(this: NodeState) {
    return this.database.lastCommitMetrics();
  },
  lastTickMetrics(this: NodeState) {
    return this.database.lastTickMetrics();
  },
  storageReadMetrics(this: NodeState) {
    return this.database.storageReadMetrics();
  },
  resetStorageReadMetrics(this: NodeState): void {
    this.database.resetStorageReadMetrics();
  },
  takeStorageReadMetrics(this: NodeState) {
    return this.database.takeStorageReadMetrics();
  },
};
