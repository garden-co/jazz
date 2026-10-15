// Staging of transactions and their versions into history/current storage, plus authored-version validation.
import { SortedSet, cmpBytes } from "../../../support/collections.js";
import { equalBytes } from "../../../support/bytes.js";
import {
  nodeError,
  isNodeError,
  BranchKey,
  cmpTxId,
  txIdKey,
  txTimeFromPhysicalMs,
  descriptorEquals,
  valueEquals,
  versionRecordEquals,
  versionIndirectDescriptors,
  type NodeState,
  type SyncMessage,
  type Transaction,
  type VersionRecord,
  type Fate,
  type TxId,
  type DurabilityTier,
  type DatabaseBatch,
  type Uuid,
} from "../../../_extern/node-physical-ingest.js";
import {
  type StoredTransaction,
  VersionRow,
  reconcileFate,
  transactionValues,
  transactionValuesWithPartialFlag,
  preparedWireRecordDescriptor,
  validateCanonicalVersionParts,
} from "../codec.js";
import {
  versionStorageWriteBinding,
  versionStoragePrimaryKey,
  authoredColumnIdsForNames,
  synchronizePhysicalVersionTables,
} from "../physical.js";
import { viewVersionKeyForIngest } from "../ingest.js";
import { applyFateUpdate, removeRejectedLocalVersions } from "./fates.js";
import {
  mergedGlobalPostImage,
  globalCurrentSeqInBatch,
  writeAheadCurrentInsert,
  writeGlobalCurrentUpdate,
  rebaseAheadOverlays,
  cleanupFatedAheadCurrentForVersions,
  pruneAheadCurrentForGlobalTime,
  aheadOverlayKey,
} from "./view_updates.js";

type Staged = [VersionRow[], Fate, bigint | undefined, any | undefined];

export const ingestTransactionAndVersions = (
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
) =>
  ingestTransactionAndVersionsWithCurrentIndexes(
    node,
    tx,
    versions,
    fate,
    globalTime,
    durability,
    true,
    false,
  );

export const ingestTransactionFragmentWithoutCurrentIndexes = (
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
) =>
  ingestTransactionAndVersionsWithCurrentIndexes(
    node,
    tx,
    versions,
    fate,
    globalTime,
    durability,
    false,
    true,
  );

export const ingestViewScopedTransactionWithCurrentIndexes = (
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
) =>
  ingestTransactionAndVersionsWithCurrentIndexes(
    node,
    tx,
    versions,
    fate,
    globalTime,
    durability,
    true,
    true,
  );

export async function stageViewScopedTransactionWithCurrentIndexes(
  node: NodeState,
  batch: DatabaseBatch,
  tx: Transaction,
  versions: VersionRecord[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
  stagedGlobalTimes: [bigint, TxId][],
  stagedContentVersions: VersionRow[],
  stagedRejections: any[],
): Promise<void> {
  const txId = tx.txId;
  const [stagedVersions, stagedFate, stagedGlobalTime, rejected] =
    await stageTransactionAndVersionsWithCurrentIndexes(
      node,
      batch,
      tx,
      versions,
      fate,
      globalTime,
      durability,
      true,
      true,
      stagedContentVersions,
    );
  if (rejected) stagedRejections.push(rejected);
  await finalizeStagedTransactionIngest(
    node,
    batch,
    txId,
    stagedFate,
    stagedGlobalTime,
    stagedGlobalTimes,
    stagedVersions,
  );
}

export async function publishPendingTransactionAndVersions(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  durability: DurabilityTier,
): Promise<{ txId: TxId; persistence: any }> {
  const txId = tx.txId;
  const batch = node.database.openBatch();
  await stageTransactionAndVersionsWithCurrentIndexes(
    node,
    batch,
    tx,
    versions,
    { kind: "Pending" },
    undefined,
    durability,
    true,
    false,
    undefined,
  );
  const persistence = await node.applyNodeBatch(batch);
  node.pendingPersistence.add(txId);
  return { txId, persistence };
}

async function storedFromWire(
  node: NodeState,
  version: VersionRecord,
  txNodeAlias: number,
  txTime: bigint,
  seq: bigint,
): Promise<VersionRow> {
  const authorSchema = version.schemaVersion();
  // Fail with TableNotFound before allocating aliases or resolving authored columns.
  node.tableInSchemaRef(version.table(), authorSchema);
  const schemaAlias: number = await node.ensureSchemaVersionAlias(authorSchema);
  const authored = authoredColumnIdsForNames(
    node,
    authorSchema,
    version.table(),
    version.authoredColumns(),
  );
  const table = node.tableInSchemaRef(version.table(), authorSchema);
  return VersionRow.fromWire(table, version, authored, txNodeAlias, schemaAlias, txTime, seq);
}

export const prepareExactHistoryVersion = (
  node: NodeState,
  txNodeAlias: number,
  txTime: bigint,
  seq: bigint,
  version: VersionRecord,
) => storedFromWire(node, version, txNodeAlias, txTime, seq);

async function ingestTransactionAndVersionsWithCurrentIndexes(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
  updateCurrentIndexes: boolean,
  isPartial: boolean,
): Promise<void> {
  return ingestTransactionAndVersionsWithCurrentIndexesInBatch(
    node,
    node.database.openBatch(),
    tx,
    versions,
    [],
    fate,
    globalTime,
    durability,
    updateCurrentIndexes,
    isPartial,
  );
}

export async function ingestTransactionAndVersionsWithCurrentIndexesInBatch(
  node: NodeState,
  batch: DatabaseBatch,
  tx: Transaction,
  versions: VersionRecord[],
  _previouslyStored: VersionRow[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
  updateCurrentIndexes: boolean,
  isPartial: boolean,
): Promise<void> {
  const txId = tx.txId;
  // `previouslyStored` versions are re-staged by the `settleExisting` path: merging them again would apply a
  // merge column's operation twice.
  const [stagedVersions, stagedFate, stagedGlobalTime, rejected] =
    await stageTransactionAndVersionsWithCurrentIndexes(
      node,
      batch,
      tx,
      versions,
      fate,
      globalTime,
      durability,
      updateCurrentIndexes,
      isPartial,
      undefined,
    );
  await finalizeStagedTransactionIngest(
    node,
    batch,
    txId,
    stagedFate,
    stagedGlobalTime,
    [],
    stagedVersions,
  );
  batch.deliverNotifications("AfterPersistence");
  const applied = await node.applyNodeBatch(batch);
  const persisted = await node.database.persistWithProgress(applied);
  node.database.finishPersistence(persisted);
  if (rejected) node.rejections.rejectedTransactions.set(txIdKey(txId), rejected);
  // Pending fragments may cache only this call's versions: never mistake that subset for the whole transaction.
  node.invalidateTxVersionTablesCache(txId);
}

export async function stageTransactionAndVersionsWithCurrentIndexes(
  node: NodeState,
  batch: DatabaseBatch,
  tx: Transaction,
  versions: VersionRecord[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
  updateCurrentIndexes: boolean,
  isPartial: boolean,
  stagedContentVersions: VersionRow[] | undefined,
): Promise<Staged> {
  // Provenance operation identities participate in merge deduplication: admit them on every ingress.
  let contributionMerge = node.admitContributionMergeForStorage(tx);
  for (const stagedId of await node.currentStagedIdsForDescriptors(
    versionIndirectDescriptors(versions),
    false,
  ))
    batch.acceptLargeValue(stagedId);
  node.mergeTxTime(tx.txId.time);
  const txNodeAlias: number = await node.ensureNodeAlias(tx.txId.node);
  for (const schema of new SortedSet<Uuid>(
    cmpBytes,
    versions.map((v) => v.schemaVersion()),
  ).toArray())
    await node.ensureSchemaVersionAlias(schema);
  const storedTx: StoredTransaction | undefined = await node.queryTransaction(tx.txId);
  if (storedTx) {
    const reconciled = { ...storedTx };
    reconcileFate(reconciled, fate, globalTime, durability);
    ({ fate, globalTime, durability } = reconciled);
  }
  // A rejected transaction has no live history left to extend.
  if (storedTx?.fate.kind === "Rejected") versions = [];
  else versions = versions.slice();
  // Fate belongs to the whole transaction: a terminal transition or complete payload covers earlier fragments.
  const settleExisting =
    !!storedTx &&
    (fate.kind === "Rejected" ||
      (globalTime !== undefined && (storedTx.globalTime !== globalTime || !isPartial)));
  if (settleExisting) {
    const incoming = new Map(versions.map((v, i) => [viewVersionKeyForIngest(v), i]));
    for (const stored of (await node.queryVersionsForTx(tx.txId)) as VersionRow[]) {
      if (node.aheadCurrentKeys.has(aheadOverlayKey(node, stored))) updateCurrentIndexes = true;
      const version = node.versionRecordFromRow(stored);
      const index = incoming.get(viewVersionKeyForIngest(version));
      if (index === undefined) versions.push(version);
      else if (fate.kind === "Rejected" && !versionRecordEquals(versions[index], version))
        throw nodeError("ConflictingCommitUnit", tx.txId);
    }
  }
  const txAlreadyKnown = !!storedTx;
  const preserveAuthoritativeCardinality = isPartial && !!storedTx && !storedTx.isPartial;
  const storageTx = preserveAuthoritativeCardinality ? storedTx!.tx : tx;
  if (storageTx !== tx) contributionMerge = node.admitContributionMergeForStorage(storageTx);
  const txValues = transactionValuesWithPartialFlag(
    txNodeAlias,
    storageTx,
    fate,
    globalTime,
    durability,
    isPartial && !preserveAuthoritativeCardinality,
    contributionMerge,
  );
  if (txAlreadyKnown) batch.update("jazz_transactions", txValues);
  else batch.insert("jazz_transactions", txValues);
  if (!equalBytes(tx.txId.node, node.nodeUuid)) {
    if (fate.kind === "Pending" && globalTime === undefined)
      node.openTx.pendingForeignTransactions.add(tx.txId);
    else node.openTx.pendingForeignTransactions.delete(txIdKey(tx.txId));
  }

  const pendingGlobalUpdates = new Map<string, VersionRow>();
  const contentVersions: VersionRow[] = [];
  const storedVersions: VersionRow[] = [];
  const rejectedFate = fate.kind === "Rejected";
  for (const version of versions) {
    const authorSchema = version.schemaVersion();
    // An accepted write is stored at its seq, a pending one at 0 (SPEC 2 §2.7.1).
    const seq = fate.kind === "Accepted" && globalTime !== undefined ? globalTime : 0n;
    let stored = await storedFromWire(node, version, txNodeAlias, tx.txId.time, seq);
    const tableSchema = node.tableInSchema(version.table(), authorSchema);
    if (updateCurrentIndexes && fate.kind === "Accepted" && globalTime !== undefined) {
      const key = `${stored.table}\0${BranchKey.key(stored.branchKey)}\0${Array.from(stored.rowUuid()).join(",")}`;
      if (node.mintingGlobalTime) {
        const merged = await mergedGlobalPostImage(
          node,
          batch,
          authorSchema,
          tableSchema,
          stored,
          tx.txId,
          globalTime,
          true,
        );
        if (merged) {
          // History holds the post-image at this seq; peers receive it for this transaction.
          stored = merged;
          pendingGlobalUpdates.set(key, merged);
        }
      } else {
        // Accepted upstream rows are post-images at their seq: a newer (or equal, authority) seq replaces the row.
        const currentSeq = await globalCurrentSeqInBatch(
          node,
          batch,
          authorSchema,
          stored.table,
          stored.branchKey,
          stored.rowUuid(),
        );
        if (currentSeq === undefined || currentSeq <= globalTime)
          pendingGlobalUpdates.set(key, stored);
      }
    }
    if (!rejectedFate) contentVersions.push(stored);
    storedVersions.push(stored);
    // Rejected payloads go directly to rejection cleanup: never insert and delete immutable history in one batch.
    if (!rejectedFate) {
      const [historyTable, record] = versionStorageWriteBinding(node, stored, tx.madeBy);
      const storageKey = versionStoragePrimaryKey(node, stored);
      // The write is accepted now: its pending record moves to its seq.
      if (txAlreadyKnown && stored.seq() !== 0n)
        batch.delete(historyTable, versionStoragePrimaryKey(node, stored.withSeq(0n)));
      if (globalTime !== undefined && fate.kind === "Accepted" && !node.mintingGlobalTime)
        batch.updateRaw(historyTable, storageKey, record);
      else if (
        (await batch.ensureExact(node.database, historyTable, storageKey, record)) === "Conflict"
      )
        throw nodeError("ConflictingCommitUnit", tx.txId);
    }
    if (updateCurrentIndexes && !rejectedFate && globalTime === undefined)
      writeAheadCurrentInsert(node, batch, stored);
  }
  if (updateCurrentIndexes && !rejectedFate) stagedContentVersions?.push(...contentVersions);
  if (updateCurrentIndexes && fate.kind === "Accepted" && globalTime !== undefined) {
    for (const stored of pendingGlobalUpdates.values())
      writeGlobalCurrentUpdate(node, batch, stored, globalTime);
    await rebaseAheadOverlays(
      node,
      batch,
      [...pendingGlobalUpdates.values()],
      new SortedSet<TxId>(cmpTxId, [tx.txId]),
    );
  }
  node.cacheTxVersions(tx.txId, storedVersions.slice());
  const rejectedPayload = rejectedFate
    ? await removeRejectedLocalVersions(
        node,
        tx.txId,
        {
          tx: storageTx,
          nodeAlias: txNodeAlias,
          fate,
          globalTime,
          durability,
          isPartial: isPartial && !preserveAuthoritativeCardinality,
        },
        batch,
      )
    : undefined;
  return [storedVersions, fate, globalTime, rejectedPayload];
}

export async function finalizeStagedTransactionIngest(
  node: NodeState,
  batch: DatabaseBatch,
  txId: TxId,
  fate: Fate,
  globalTime: bigint | undefined,
  stagedGlobalTimes: [bigint, TxId][],
  stagedVersions: VersionRow[],
): Promise<void> {
  if (fate.kind !== "Accepted" || globalTime === undefined) return;
  stagedGlobalTimes.push([globalTime, txId]);
  const advanced: bigint[] = node.recordAppliedGlobalTime(globalTime, txId);
  await cleanupFatedAheadCurrentForVersions(node, batch, stagedVersions);
  for (const gt of advanced)
    if (gt !== globalTime) await pruneAheadCurrentForGlobalTime(node, batch, gt);
}

const physicalMsInRange = (ms: number): boolean => {
  try {
    txTimeFromPhysicalMs(ms);
    return true;
  } catch {
    return false;
  }
};

/**
 * A wire row version is a complete row under the schema id it declares; a known schema must never accept a
 * descriptor borrowed from another version.
 */
export function malformedAuthoredVersionReason(
  node: NodeState,
  versions: VersionRecord[],
): string | undefined {
  for (const version of versions) {
    for (const [field, physicalMs] of [
      ["created_at_ms", version.createdAtMs()],
      ["updated_at_ms", version.updatedAtMs()],
    ] as const)
      if (!physicalMsInRange(physicalMs))
        return `row version for table '${version.table()}' has ${field} outside the packed HLC physical-millisecond range`;
    try {
      validateCanonicalVersionParts(version.branchKey());
    } catch (error) {
      if (isNodeError(error, "InvalidMergeableCommit")) return (error as any).detail;
      throw error;
    }
    const schema = node.catalogue.catalogueSchemas.get(version.schemaVersion());
    if (!schema) continue;
    const table = schema.schema.tables.find((t: any) => t.name === version.table());
    if (!table) return `row version table '${version.table()}' is absent from its authored schema`;
    if (!descriptorEquals(version.record().descriptor, preparedWireRecordDescriptor(table)))
      return `row version for table '${version.table()}' does not carry the complete descriptor of its authored schema`;
    const reason = malformedAuthoredBranchKeyReason(schema.schema, table, version);
    if (reason !== undefined) return reason;
  }
  return undefined;
}

function malformedAuthoredBranchKeyReason(
  schema: any,
  table: any,
  version: VersionRecord,
): string | undefined {
  let branchCells: [string, any][];
  try {
    branchCells = schema.validateAuthoredBranchKey(table, version.branchKey());
  } catch (reason) {
    return `row version for table '${version.table()}' has an invalid branch key: ${reason}`;
  }
  for (const [column, branchValue] of branchCells) {
    const position = table.columns.findIndex((c: any) => c.name === column);
    if (position < 0)
      return `row version for table '${version.table()}' binds a missing branch column '${column}'`;
    const cell = version.cellAt(position);
    if (cell === undefined || !valueEquals(cell, branchValue))
      return `row version for table '${version.table()}' has branch key content that disagrees with column '${column}'`;
  }
  return undefined;
}

/**
 * Validate row versions carried by a view or repair payload before it may advance receiver state. View payloads
 * cannot park for a missing catalogue entry.
 */
export const validateViewPayloadVersions = (node: NodeState, versions: VersionRecord[]): void =>
  validateViewPayloadVersionsPrepared(node, versions, new Map());

export function validateViewPayloadVersionsPrepared(
  node: NodeState,
  versions: VersionRecord[],
  descriptors: Map<string, any>,
): void {
  for (const version of versions) {
    const schema = node.catalogue.catalogueSchemas.get(version.schemaVersion());
    if (!schema)
      throw nodeError("MalformedViewUpdate", "row version names an unknown authored schema");
    const table = schema.schema.tables.find((t: any) => t.name === version.table());
    if (!table)
      throw nodeError(
        "MalformedViewUpdate",
        "row version table is absent from its authored schema",
      );
    const key = `${Array.from(version.schemaVersion() as Uint8Array).join(",")}\0${version.table()}`;
    let descriptor = descriptors.get(key);
    if (!descriptor) descriptors.set(key, (descriptor = table.wireRecordDescriptor()));
    if (!descriptorEquals(version.record().descriptor, descriptor))
      throw nodeError(
        "MalformedViewUpdate",
        "row version does not carry the complete descriptor of its authored schema",
      );
    if (!physicalMsInRange(version.createdAtMs()) || !physicalMsInRange(version.updatedAtMs()))
      throw nodeError(
        "MalformedViewUpdate",
        "row version provenance exceeds packed HLC physical-millisecond range",
      );
    if (malformedAuthoredBranchKeyReason(schema.schema, table, version) !== undefined)
      throw nodeError(
        "MalformedViewUpdate",
        "row version does not carry a valid authored branch key",
      );
  }
}

export async function rejectMalformedCommit(
  node: NodeState,
  tx: Transaction,
  reason: string,
): Promise<SyncMessage[]> {
  const fate: Fate = { kind: "Rejected", reason: { kind: "MalformedCommit", detail: reason } };
  // Malformed provenance cannot be encoded safely: retain the terminal outcome without the invalid payload.
  await ingestRejectedTransaction(node, { ...tx, contributionMerge: undefined }, fate);
  return [
    { kind: "FateUpdate", txId: tx.txId, fate, globalTime: undefined, durability: undefined },
  ];
}

/**
 * Ensure every known authored schema named by an arriving commit has a local alias and registered shared-storage
 * variant. Unknown schemas stay parked until their catalogue lineage arrives.
 */
export async function prepareAuthoredSchemaVariantsForCommit(
  node: NodeState,
  versions: VersionRecord[],
): Promise<void> {
  if (malformedAuthoredVersionReason(node, versions) !== undefined)
    throw nodeError("InvalidStoredValue", "wire version record does not match authored schema");
  if (versions.some((v) => !node.catalogue.catalogueSchemas.has(v.schemaVersion()))) return;
  const variants = new Map<string, [string, Uuid]>();
  for (const v of versions)
    variants.set(`${v.table()}\0${Array.from(v.schemaVersion() as Uint8Array)}`, [
      v.table(),
      v.schemaVersion(),
    ]);
  let registeredMapping = false;
  for (const [table, schemaVersion] of variants.values()) {
    node.tableInSchemaRef(table, schemaVersion);
    registeredMapping ||=
      !node.catalogue.schemaVersionAliases.has(schemaVersion) ||
      !node.catalogue.physicalMappings.has(schemaVersion);
    await node.ensureSchemaVersionAlias(schemaVersion);
  }
  if (registeredMapping) await synchronizePhysicalVersionTables(node);
}

export async function ingestRejectedTransaction(
  node: NodeState,
  tx: Transaction,
  fate: Fate,
): Promise<void> {
  const contributionMerge = node.admitContributionMergeForStorage(tx);
  if (await node.queryTransaction(tx.txId))
    return applyFateUpdate(node, tx.txId, fate, undefined, undefined);
  const txNodeAlias: number = await node.ensureNodeAlias(tx.txId.node);
  const batch = node.database.openBatch();
  batch.insert(
    "jazz_transactions",
    transactionValues(txNodeAlias, tx, fate, undefined, "Local", contributionMerge),
  );
  const applied = await node.applyNodeBatch(batch);
  const persisted = await node.database.persistWithProgress(applied);
  node.database.finishPersistence(persisted);
}
