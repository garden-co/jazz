// Db open/close, schema views, write waits, connection admission and ticks.
import {
  AUTHOR_SYSTEM,
  Db,
  JazzDbError,
  JazzSchema,
  MergeableCommit,
  NodeRuntime,
  NodeState,
  PendingOpenSchema,
  PublicationOutcome,
  SchemaVersion,
  SchemaViewId,
  StackSafeFuture,
  WriteClock,
  defaultCellForColumnType,
  directSchemaViewLens,
  productionRowIdSource,
} from "../../_extern/db-rest-wasm.js";

type DbT = any;

export const identity = (db: DbT) => db.identity;
export const setLargeValueStagingPolicy = (db: DbT, policy: any) =>
  db.node.setLargeValueStagingPolicy(policy);
export const evictExpiredStagedLargeValues = (db: DbT) => db.node.evictExpiredStagedLargeValues();

/** Open a database over the supplied storage and recover local state. */
export async function open(config: any): Promise<DbT> {
  const node = await NodeState.newClient(
    config.identity.node,
    config.schema,
    config.storage,
    false,
  );
  const db = fromOpenedNode(node, config.schema, config.identity, config.idSource);
  await db.node.restorePendingUploads(db.identity);
  return db;
}

/** Wrap an opened node, leaving an unknown schema pending open-time admission. */
function fromOpenedNode(node: any, schema: any, identity: any, idSource: any): DbT {
  const schemaVersionId = schema.versionId();
  const schemaViews = new Map([[SchemaViewId.forSchema(schema).key(), schema]]);
  const requiresOpenSchemaAdmission = !node.catalogueSchemas().has(schemaVersionId);
  const runtime = new NodeRuntime(node);
  if (requiresOpenSchemaAdmission)
    runtime.openSchemaAdmission = new PendingOpenSchema(schemaVersionId);
  return new Db({
    schema,
    schemaVersionId,
    schemaViewIsFixed: false,
    requiresOpenSchemaAdmission,
    schemaViews,
    identity,
    node: runtime,
    rowIdSource: { current: idSource ?? productionRowIdSource },
    rowIdSourceGuaranteesFresh: idSource === undefined,
    nextNowMs: new WriteClock(),
    reservedTxId: undefined,
    ownerOperationAdmitted: false,
    backendAttribution: false,
  });
}

/** Open the durable, scope-admitted half of a client relay. */
export async function openScopeIsolatedClientRelay(config: any, scope: any): Promise<DbT> {
  const db = await open(config);
  db.node.configureScopeIsolatedClientRelay(scope);
  return db;
}

/** Open a Db allowed to record external provenance. */
export async function openWithBackendAttribution(config: any): Promise<DbT> {
  const db = await open(config);
  db.backendAttribution = true;
  await db.node.restoreBackendPendingUploads(db.identity.node);
  return db;
}

/** Open a database as a history-complete serving core. */
export function openHistoryComplete(config: any): Promise<DbT> {
  return openHistoryCompleteInner(config, false);
}

async function openHistoryCompleteInner(config: any, recoverClient: boolean): Promise<DbT> {
  const node = recoverClient
    ? await NodeState.newClient(config.identity.node, config.schema, config.storage, true)
    : await NodeState.newHistoryComplete(config.identity.node, config.schema, config.storage);
  return fromOpenedNode(node, config.schema, config.identity, config.idSource);
}

export async function openHistoryCompleteWithBackendAttribution(config: any): Promise<DbT> {
  const db = await openHistoryCompleteInner(config, true);
  db.backendAttribution = true;
  await db.node.restoreBackendPendingUploads(db.identity.node);
  return db;
}

/** Register a typed schema view on this database owner. */
export async function registerSchemaView(db: DbT, schema: any): Promise<DbT> {
  const schemaVersionId = schema.versionId();
  const schemaViewId = SchemaViewId.forSchema(schema);
  // A replica opened ahead of its durable catalogue must first receive the lineage.
  db.ensureOpenSchemaAdmitted();
  await admitLocalSchemaViewIfNeeded(db, schema);
  const admitted = db.node.node.borrow().schemaWithActivePermissions(schemaVersionId);
  if (!admitted) throw new JazzDbError("Schema", "registered schema is missing");
  if (!schemaPolicyMetadataMatches(admitted, schema))
    throw new JazzDbError(
      "Schema",
      "schema view policy metadata conflicts with its admitted structural schema",
    );
  if (!schemaIndexMetadataMatches(admitted, schema))
    throw new JazzDbError(
      "Schema",
      "schema view index metadata conflicts with its admitted structural schema",
    );
  const existing = db.schemaViews.get(schemaViewId.key());
  if (existing) {
    if (!existing.equals(schema))
      throw new JazzDbError("Schema", `schema view id collision for ${schemaViewId}`);
  } else db.schemaViews.set(schemaViewId.key(), schema);
  return db.cloneWith({
    schema,
    schemaVersionId,
    schemaViewIsFixed: true,
    requiresOpenSchemaAdmission: false,
    reservedTxId: undefined,
    ownerOperationAdmitted: false,
  });
}

/** Attach an already-registered typed schema view to this owner. */
export function schemaView(db: DbT, schemaViewId: any): Promise<DbT> {
  const schema = db.schemaViews.get(schemaViewId.key());
  if (!schema) throw new JazzDbError("Schema", `schema view ${schemaViewId} is not registered`);
  return registerSchemaView(db, schema);
}

export const schemaViewIdOf = (db: DbT) => SchemaViewId.forSchema(db.schema);

/** Admit the first application schema into an owner opened with the empty schema. */
async function admitLocalSchemaViewIfNeeded(db: DbT, schema: any): Promise<void> {
  const emptyId = JazzSchema.empty().versionId();
  const targetId = schema.versionId();
  const node = db.node.node.borrow();
  if (node.catalogueSchemas().has(targetId)) return;
  const current = node.currentWriteSchema();
  const source = node.catalogueSchemas().get(current.schema)?.schema;
  if (!source) throw new JazzDbError("Schema", "current schema view is missing");
  const catalogueSeq = node.activeCatalogueSeq() + 1;
  const bootstrapCurrent = current.schema.equals(emptyId) && node.catalogueSchemas().size === 1;
  const [lens, newTables, droppedTables] = directSchemaViewLens(source, schema);
  const owner = await db.node.node.lock();
  const publication = owner.authorSchemaLineagePublication(
    new SchemaVersion(schema),
    lens,
    newTables,
    droppedTables,
  );
  const outcome = await owner.applyTrustedCatalogueMessage({
    kind: "PublishSchemaWithLens",
    author: AUTHOR_SYSTEM,
    catalogueSeq,
    publication,
  });
  await db.finishPublicationOutcome(outcome);
  if (bootstrapCurrent) await (await db.node.node.lock()).activateSchema(1, schema);
}

/** Close maintenance admission, terminalize transactions and streams, close storage. */
export async function close(db: DbT): Promise<void> {
  if (db.schemaViewIsFixed) return;
  const n = db.node;
  n.beginMutationShutdown();
  // Admission and Drop maintenance cross their shutdown boundary before suspending.
  n.beginTransactionAbandonmentShutdown();
  n.beginSubscriptionFinalizationShutdown();
  await n.lockCloseOwner();
  await n.drainQueuedMutations();
  await n.drainTransactionWaitObservers();
  await n.flushDeferredRejectionDiscards();
  await n.finishTransactionAbandonmentShutdown();
  await n.drainSubscriptionFinalizations();
  // Query evaluation detached on a remote chunk (#3349) cannot finish now.
  n.chunkResolver.failLocalDemandForClose();
  await (await n.node.lock()).close();
  n.retireSubscriptionRuntimeAfterClose();
}

export const setNonDurableClient = (db: DbT) => db.node.setNonDurableClient();
export const useWallClockForWrites = (db: DbT) => (db.nextNowMs.wall = true);
export const foregroundTxTimeHighWater = async (db: DbT) =>
  (await db.node.node.lock()).txTimeHighWater();
export const seedForegroundTxTimeHighWater = async (db: DbT, highWater: any) =>
  (await db.node.node.lock()).seedTxTimeHighWater(highWater);
export const reserveTransactionIdAtMs = (db: DbT, nowMs: number) =>
  db.node.reserveTransactionId(nowMs);
export const driveQueuedMutationOnce = (db: DbT) => db.node.pollQueuedMutationOnce();
export const queuedMutationCount = (db: DbT): number => db.node.queuedMutationCount();
export const queuedMutationBarrier = (db: DbT) => db.node.queuedMutationBarrier();
export const queuedTransactionError = (db: DbT, id: any) => db.node.queuedTransactionError(id);

/** Enqueue exact cleanup for an opaque binding-owned streaming upload. */
export function enqueueStreamingValueUploadCleanup(db: DbT, upload: any) {
  const ticket = { result: undefined as any };
  const id = upload.cleanupId();
  const node = db.node.node;
  db.node.enqueueTransactionCleanupWithCompletion(
    async () => (await node.lock()).evictPendingLargeValueUpload(id),
    (outcome: any) => (ticket.result = outcome),
  );
  return ticket;
}

export const drainQueuedMutationsForBinding = (db: DbT) => db.node.drainQueuedMutations();
export const enqueueTransactionRead = (db: DbT, txId: any, read: () => Promise<any>) =>
  db.node.enqueueTransactionRead(txId, read);

export const cloneForReservedTransaction = (db: DbT, txId: any) =>
  db.cloneWith({ reservedTxId: txId, ownerOperationAdmitted: true });
export const cloneForOwnerOperation = (db: DbT) =>
  db.cloneWith({ reservedTxId: undefined, ownerOperationAdmitted: true });

export function ensureMutationOperationAdmitted(db: DbT): void {
  db.ensureOpenSchemaAdmitted();
  if (!db.ownerOperationAdmitted) db.node.ensureMutationAdmissionOpen();
}

export const restoreBrowserRelayPendingUploads = (db: DbT) =>
  db.node.restoreBrowserRelayPendingUploads(db.identity.author);
export const setDeferredLocalPersistence = (db: DbT, deferred: boolean) =>
  db.node.setDeferredLocalPersistence(deferred);
export const setInitialSyncFlushCadence = (db: DbT, cadence: any) =>
  db.node.node.borrow().setInitialSyncFlushCadence(cadence.writes());

/** Seed a settled mergeable row for server bootstrap/import flows. */
export async function seedSettledMergeableForBootstrap(
  db: DbT,
  table: string,
  row: any,
  madeBy: any,
  cells: Map<string, any>,
): Promise<any> {
  const [writeSchema, writeSchemaVersion] = db.currentWriteSchemaForQuery();
  const tableSchema = writeSchema.tables.find((t: any) => t.name === table);
  if (!tableSchema) throw new JazzDbError("Schema", `unknown table ${table}`);
  for (const column of tableSchema.columns)
    if (!cells.has(column.name) && column.default !== undefined)
      cells.set(column.name, defaultCellForColumnType(column.columnType, column.default));
  const node = db.node.node.borrow();
  const published = await node.commitMergeableInSchema(
    writeSchemaVersion,
    new MergeableCommit(table, row, db.nextNowMs.next()).madeBy(madeBy).cells(cells),
  );
  const txId = published.txId;
  await db.finishPublicationOutcome(PublicationOutcome.published(undefined, published));
  await db.finishPublicationOutcome(await node.finalizeLocalMergeableCommit(txId));
  db.node.markSubscriberConnectionsDirty();
  return txId;
}

/** Return the locally observed fate and durability for a write transaction. */
export async function writeState(db: DbT, txId: any): Promise<any> {
  const queued = db.node.queuedMutationWriteState(txId);
  if (queued) {
    if (queued.error) throw queued.error;
    return queued.value;
  }
  const state = await (await db.node.node.lock()).transactionState(txId);
  if (!state) throw new JazzDbError("NotObserved", `transaction ${txId} is not known locally`);
  const [fate, globalTime, durability] = state;
  return { fate, globalTime, durability };
}
export const writeStateAsync = writeState;

export const takeQueuedMutationFailure = (db: DbT, txId: any) =>
  db.node.takeQueuedMutationFailure(txId);

/** Wait for this author's pending writes, including recovered durable writes. */
export async function waitForPendingWrites(db: DbT, tier: any): Promise<void> {
  await db.node.settleLocalPublications();
  const node = await db.node.node.lock();
  const pending = db.backendAttribution
    ? await node.synchronizingTransactionIdsForNode(db.identity.node)
    : await node.synchronizingTransactionIdsForAuthor(db.identity.author);
  for (const txId of pending) await waitForTransaction(db, txId, tier);
}

const waitOptions = (o: any) => (typeof o === "object" && o && "tier" in o ? o : { tier: o });
export const waitForTransaction = (db: DbT, txId: any, options: any) =>
  db.node.waitForTransaction(txId, waitOptions(options));
export const waitForTransactionWith = (db: DbT, txId: any, options: any, cb: (r: any) => void) =>
  db.node.waitForTransactionWith(txId, waitOptions(options), cb);
export const waitForWriteWith = (db: DbT, write: any, options: any, cb: (r: any) => void) =>
  db.node.waitForWriteWith(write.txId, write.queuedAlias, waitOptions(options), cb);
export const nextWriteStateChange = (db: DbT, txId: any) => db.node.registerWriteStateWaiter(txId);
export const onMutationError = (db: DbT, cb: any) => db.node.setMutationErrorCallback(cb);
export const clearMutationErrorCallback = (db: DbT) => db.node.setMutationErrorCallback(undefined);
export const connectUpstream = (db: DbT, transport: any) => db.node.connectUpstream(transport);
export const tryConnectUpstream = (db: DbT, transport: any) =>
  db.node.tryConnectUpstream(transport);
export const setTickScheduler = (db: DbT, scheduler: any) => db.node.setScheduler(scheduler);
export const markSubscriberConnectionsDirtyAfterQueryRuntimeWake = (db: DbT) =>
  db.node.markSubscriberConnectionsDirtyAfterQueryRuntimeWake();
export const scheduleTick = (db: DbT, urgency: any) => db.node.scheduleTick(urgency);
export const requestPermissionAdvice = (db: DbT, action: any) =>
  db.node.requestPermissionAdvice(action);
export const requestPermissionAdviceWithDelegatedSession = (db: DbT, action: any, session: any) =>
  db.node.requestPermissionAdviceWithDelegatedSession(action, session);
export const cancelPermissionAdviceRequest = (db: DbT, requestId: any) =>
  db.node.cancelPermissionAdviceRequest(requestId);
export const acceptSubscriber = (db: DbT, transport: any, identity: any) =>
  db.node.acceptSubscriber(transport, identity);
export const acceptScopeIsolatedRelaySubscriber = (
  db: DbT,
  transport: any,
  identity: any,
  claims: any,
  admissionEpoch: number,
) => db.node.acceptScopeIsolatedRelaySubscriber(transport, identity, claims, admissionEpoch);
export const acceptSubscriberWithClaims = (db: DbT, transport: any, identity: any, claims: any) =>
  db.node.acceptSubscriberWithClaims(transport, identity, claims);
export const acceptSubscriberWithClaimsAsync = (
  db: DbT,
  transport: any,
  identity: any,
  claims: any,
) => db.node.acceptSubscriberWithClaimsAsync(transport, identity, claims);
export const acceptSubscriberWithClaimsAndTrust = (
  db: DbT,
  transport: any,
  identity: any,
  claims: any,
  trust: any,
) => db.node.acceptSubscriberWithClaimsAndTrust(transport, identity, claims, trust);
export const acceptSubscriberWithResume = (db: DbT, transport: any, identity: any, cursor: any) =>
  db.node.acceptSubscriberWithResume(transport, identity, cursor);
export const detachConnection = (db: DbT, connection: any): boolean =>
  db.node.detachConnection(connection);
export const detachConnectionAsync = (db: DbT, connection: any): Promise<boolean> =>
  db.node.detachConnectionAsync(connection);

/** Service every connection once. */
export function tick(db: DbT): Promise<void> {
  return StackSafeFuture(() => tickStats(db).then(() => undefined));
}

/** Service every connection once and return binding-observable wake counts. */
export async function tickStats(db: DbT): Promise<any> {
  const n = db.node;
  if (n.ownerIsAvailable()) n.deliverPendingMutationErrors();
  // Previously admitted resident publications keep progressing even when the
  // next FIFO preparation is cold.
  if (n.hasPendingLocalPublications()) n.pollLocalPublicationSettlementOnce();
  n.pollQueuedMutationOnce();
  n.pollTransactionWaitObservers();
  // Do not retain the host turn behind another lock owner.
  if (!n.ownerIsAvailableOrWakeWhenReleased()) return {};
  await n.flushDeferredRejectionDiscards();
  await n.drainSubscriptionFinalizations();
  if (n.hasPendingLocalPublications()) await n.settleLocalPublications();
  const stats = await n.tick();
  n.pollTransactionWaitObservers();
  if (n.ownerIsAvailableOrWakeWhenReleased()) await n.flushDeferredRejectionDiscards();
  return stats;
}

export const refreshSubscriptions = (db: DbT) => refreshSubscriptionsForTables(db, undefined);

export async function refreshSubscriptionsForTables(
  db: DbT,
  changedTables: Set<string> | undefined,
): Promise<number> {
  const refreshed = await db.node.refreshSubscriptionsWithTables(changedTables);
  if (refreshed > 0) db.node.markSubscriberConnectionsDirty();
  return refreshed;
}

const sameJson = (a: any, b: any) => JSON.stringify(a) === JSON.stringify(b);

function schemaPolicyMetadataMatches(left: any, right: any): boolean {
  return (
    left.tables.length === right.tables.length &&
    left.tables.every((l: any) =>
      right.tables.some(
        (r: any) =>
          l.name === r.name &&
          sameJson(l.readPolicy, r.readPolicy) &&
          sameJson(l.writePolicies, r.writePolicies),
      ),
    )
  );
}

function schemaIndexMetadataMatches(left: any, right: any): boolean {
  return (
    left.tables.length === right.tables.length &&
    left.tables.every((l: any) =>
      right.tables.some(
        (r: any) =>
          l.name === r.name &&
          sameJson(l.indexedColumns, r.indexedColumns) &&
          sameJson(l.compositeIndexes, r.compositeIndexes),
      ),
    )
  );
}
