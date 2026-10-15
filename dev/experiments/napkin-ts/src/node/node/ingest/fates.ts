// Fate application, exclusive-commit validation, write-policy admission, and parked commit units.
import {
  SortedMap,
  SortedSet,
  cmpBytes,
  cmpNum,
  cmpStr,
  cmpTuple,
} from "../../../support/collections.js";
import { equalBytes } from "../../../support/bytes.js";
import {
  nodeError,
  PublicationOutcome,
  BranchKey,
  PK,
  AuthorSubject,
  cmpTxId,
  txIdEquals,
  txIdKey,
  transactionEquals,
  versionRecordEquals,
  type NodeState,
  type SyncMessage,
  type Transaction,
  type VersionRecord,
  type Fate,
  type TxId,
  type DurabilityTier,
  type CommitUnitIngestContext,
  type DatabaseBatch,
  type Uuid,
} from "../../../_extern/node-physical-ingest.js";
import {
  type StoredTransaction,
  type VersionRow,
  type ParkedCommitUnit,
  reconcileFate,
  transactionValuesWithPartialFlag,
  rejectedTransactionValues,
  rejectedVersionValues,
  ownedRecordFromStorageValues,
  strongestIngressRole,
  TX_TOUCHED_ROWS_TABLE,
} from "../codec.js";
import {
  contributionMergeStorageValue,
  physicalTableIdForSchema,
  rejectedVersionStorageWriteBinding,
  versionStorageTableForRow,
  versionStoragePrimaryKey,
} from "../physical.js";
import { PARKED_PREDECESSOR_WAIT_MS, type CommitUnitParkMode } from "../ingest.js";
import { ingestCommitUnitOnce, ingestRelayCommitUnitOnce } from "./commit_bundles.js";
import { malformedAuthoredVersionReason } from "./validation.js";
import {
  writeGlobalCurrentUpdate,
  writeHistoryPostImage,
  rebaseAheadOverlays,
  cleanupFatedAheadCurrentForVersions,
  pruneAheadCurrentForGlobalTime,
  writeAheadCurrentDelete,
  restoreAheadOverlayAfterReject,
} from "./view_updates.js";
import { pendingPredecessorRefusal } from "./write_base.js";

type Outcome = PublicationOutcome<SyncMessage[]>;
const rowKey = (table: string, row: Uuid) => `${table}\0${Array.from(row).join(",")}`;

/** Apply an upstream fate update. */
export async function applyFateUpdate(
  node: NodeState,
  txId: TxId,
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier | undefined,
): Promise<void> {
  node.requireCatalogueReady();
  const persisted = { terminal: false };
  try {
    await applyFateUpdateOnce(node, txId, fate, globalTime, durability, persisted);
  } finally {
    if (persisted.terminal) node.openTx.localPermissionSubjects.delete(txIdKey(txId));
  }
}

async function applyFateUpdateOnce(
  node: NodeState,
  txId: TxId,
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier | undefined,
  persisted: { terminal: boolean },
): Promise<void> {
  if (fate.kind !== "Pending" || globalTime !== undefined)
    node.openTx.pendingForeignTransactions.delete(txIdKey(txId));
  const stored: StoredTransaction | undefined = await node.queryTransaction(txId);
  if (!stored) throw nodeError("MissingTransaction", txId);
  const alreadyAccepted = stored.fate.kind === "Accepted";
  const previousGlobalTime = stored.globalTime;
  const previousDurability = stored.durability;
  reconcileFate(stored, fate, globalTime, durability);
  const advancedGlobalTimes: bigint[] =
    stored.fate.kind === "Accepted" && stored.globalTime !== undefined
      ? node.recordAppliedGlobalTime(stored.globalTime, txId)
      : [];

  if (
    alreadyAccepted &&
    stored.tx.kind === "Mergeable" &&
    !stored.isPartial &&
    stored.globalTime !== undefined &&
    stored.globalTime === previousGlobalTime &&
    stored.durability === previousDurability &&
    advancedGlobalTimes.length === 0
  ) {
    // A complete mergeable transaction installed global current atomically with its accepted metadata:
    // replaying the same receipt has no durable work. Exclusive fragments still take the repair path below.
    persisted.terminal = true;
    return;
  }

  const batch: DatabaseBatch = node.database.openBatch();
  let globalCurrentUpdates: VersionRow[] = [];
  const cleanupRejectedVersions = stored.fate.kind === "Rejected";
  const txVersions: VersionRow[] = await node.queryVersionsForTx(txId);
  if (stored.fate.kind === "Accepted" && stored.globalTime !== undefined)
    globalCurrentUpdates = await node.globalCurrentUpdatesForVersions(
      batch,
      txId,
      stored.globalTime,
      txVersions,
    );
  const contributionMerge = contributionMergeStorageValue(node, stored.tx.contributionMerge);
  batch.update(
    "jazz_transactions",
    transactionValuesWithPartialFlag(
      stored.nodeAlias,
      stored.tx,
      stored.fate,
      stored.globalTime,
      stored.durability,
      stored.isPartial,
      contributionMerge,
    ),
  );
  if (stored.globalTime !== undefined) {
    const gt = stored.globalTime;
    for (const version of globalCurrentUpdates) {
      writeGlobalCurrentUpdate(node, batch, version, gt);
      // History holds the row's post-image at this seq, which is what peers receive for this transaction.
      writeHistoryPostImage(node, batch, version, stored.tx.madeBy);
    }
    let moved = globalCurrentUpdates.length > 0;
    if (stored.fate.kind === "Accepted") {
      // Every pending record of an accepted write moves to its seq, also when the row's image did not change.
      for (const version of txVersions) {
        const updated = globalCurrentUpdates.some(
          (u) =>
            u.table === version.table &&
            BranchKey.equals(u.branchKey, version.branchKey) &&
            equalBytes(u.rowUuid(), version.rowUuid()),
        );
        if (!updated && version.seq() === 0n) {
          writeHistoryPostImage(node, batch, version.withSeq(gt), stored.tx.madeBy);
          moved = true;
        }
      }
    }
    if (moved) node.invalidateTxVersionTablesCache(txId);
    await rebaseAheadOverlays(
      node,
      batch,
      globalCurrentUpdates,
      new SortedSet<TxId>(cmpTxId, [txId]),
    );
  }
  if (stored.fate.kind === "Rejected" || stored.globalTime !== undefined)
    await cleanupFatedAheadCurrentForVersions(node, batch, txVersions);
  for (const gt of advancedGlobalTimes)
    if (gt !== stored.globalTime) await pruneAheadCurrentForGlobalTime(node, batch, gt);
  const rejectedPayload = cleanupRejectedVersions
    ? await removeRejectedLocalVersions(node, txId, stored, batch)
    : undefined;
  const applied = await node.applyNodeBatch(batch);
  const result = await node.database.persistWithProgress(applied);
  node.database.finishPersistence(result);
  persisted.terminal = stored.fate.kind !== "Pending";
  if (rejectedPayload)
    node.rejections.rejectedTransactions.set(txIdKey(rejectedPayload.txId()), rejectedPayload);
}

/** Validate an exclusive commit unit's row/absent/predicate reads and per-write first-committer-wins. */
export async function validateExclusiveCommitUnit(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
): Promise<boolean> {
  const baseSnapshot = tx.baseSnapshot;
  if (!baseSnapshot) return false;
  const rowReads: any[] = tx.rowReadSet ?? [];
  const absentReads: any[] = tx.absentReadSet ?? [];
  // Point-read records carry names, not schema IDs: a retained alias is usable only when every mapping agrees.
  const readSchemas = new Map<string, Uuid>();
  for (const table of [...rowReads, ...absentReads].map((r) => r.table as string)) {
    if (readSchemas.has(table)) continue;
    let found: [Uuid, number] | undefined;
    for (const [schema, mapping] of node.catalogue.physicalMappings) {
      const t = mapping.tables.get(table);
      if (!t) continue;
      if (!found) found = [schema, t.tableId];
      else if (found[1] !== t.tableId) return false;
    }
    if (!found) return false;
    readSchemas.set(table, found[0]);
  }
  // Read sets and write CAS both validate the current row image, whose deletion state is one of its cells.
  const visibleRowMemo = new Map<string, TxId | undefined>();
  const visibleWinnerMemo = new Map<string, TxId | undefined>();
  for (const read of rowReads) {
    const current = await visibleGlobalRowTxIdNowMemoized(
      node,
      readSchemas.get(read.table)!,
      read.table,
      read.rowUuid,
      visibleRowMemo,
    );
    if (!current || !txIdEquals(current, read.version)) return false;
  }
  for (const absent of absentReads)
    if (
      await visibleGlobalRowTxIdNowMemoized(
        node,
        readSchemas.get(absent.table)!,
        absent.table,
        absent.rowUuid,
        visibleRowMemo,
      )
    )
      return false;
  // Predicate reads are validated against the rows the transaction proved it read (garden-co/jazz#3694).
  const identity = tx.permissionSubject ?? tx.madeBy;
  const proven = new Map<string, SortedSet<Uuid>>();
  for (const read of rowReads) {
    let set = proven.get(read.table);
    if (!set) proven.set(read.table, (set = new SortedSet<Uuid>(cmpBytes)));
    set.add(read.rowUuid);
  }
  const noRows = new SortedSet<Uuid>(cmpBytes);
  for (const predicate of tx.predicateReadSet ?? [])
    if (
      await predicateReadDiffersFromProven(
        node,
        predicate,
        baseSnapshot,
        identity,
        proven.get(predicate.table) ?? noRows,
      )
    )
      return false;
  for (const version of versions) {
    node.tableInSchemaRef(version.table(), version.schemaVersion());
    const tableId = physicalTableIdForSchema(node, version.schemaVersion(), version.table());
    const current = await visibleGlobalTxIdNowMemoized(
      node,
      tableId,
      version.rowUuid(),
      visibleWinnerMemo,
    );
    // First committer wins: the written register must be unchanged since the transaction's base snapshot.
    if (current && !(await node.snapshotCovers(current, baseSnapshot))) return false;
  }
  return true;
}

async function visibleGlobalRowTxIdNowMemoized(
  node: NodeState,
  schemaVersion: Uuid,
  table: string,
  rowUuid: Uuid,
  memo: Map<string, TxId | undefined>,
): Promise<TxId | undefined> {
  const key = rowKey(table, rowUuid);
  if (memo.has(key)) return memo.get(key);
  const current: TxId | undefined = await node.visibleGlobalRowTxIdNow(
    schemaVersion,
    table,
    rowUuid,
  );
  memo.set(key, current);
  return current;
}

async function visibleGlobalTxIdNowMemoized(
  node: NodeState,
  tableId: number,
  rowUuid: Uuid,
  memo: Map<string, TxId | undefined>,
): Promise<TxId | undefined> {
  const key = rowKey(String(tableId), rowUuid);
  if (memo.has(key)) return memo.get(key);
  const current: TxId | undefined = await node.visibleGlobalTxIdForPhysicalTableNow(
    tableId,
    rowUuid,
  );
  memo.set(key, current);
  return current;
}

export function predicateReadIsDegenerateWholeTable(node: NodeState, predicate: any): boolean {
  let shape;
  try {
    shape = node.queryFromTable(predicate.table).validate(node.catalogue.schema);
  } catch {
    return false;
  }
  const binding = shape.bind(new Map());
  return (
    equalBytes(predicate.shapeId, shape.shapeId()) &&
    equalBytes(predicate.bindingId, binding.bindingId())
  );
}

/**
 * Whether an exclusive predicate read no longer returns what the transaction read: every row the predicate returns
 * now must be one the transaction proved it read, and no proved row may have left the result.
 */
async function predicateReadDiffersFromProven(
  node: NodeState,
  predicate: any,
  snapshot: any,
  identity: any,
  proven: SortedSet<Uuid>,
): Promise<boolean> {
  const validation = predicateReadValidationShape(node, predicate);
  if (!validation) return true;
  const [shape, binding] = validation;
  const table = shape.query().table;
  const rows: any[] = await node.queryRowsWithPreparedPlanForIdentity(
    shape,
    binding,
    "Global",
    undefined,
    identity,
  );
  const now = new SortedSet<Uuid>(
    cmpBytes,
    rows.filter((row) => row.table() === table).map((row) => row.rowUuid()),
  );
  if (now.toArray().some((row) => !proven.has(row))) return true;
  const atSnapshot = await shapeOutputTxSetAtSnapshot(node, shape, binding, snapshot);
  return atSnapshot.toArray().some(([row]) => proven.has(row) && !now.has(row));
}

/** The shape and binding validating a predicate read: its own shape, or for an aggregate the rows it consumed. */
function predicateReadValidationShape(node: NodeState, predicate: any): [any, any] | undefined {
  // Shape IDs include the authoring schema: a migration must not make an unchanged read conflict.
  let validated: [any, any] | undefined;
  for (const schema of [
    node.catalogue.schema,
    ...node.catalogue.catalogueSchemas.valuesArray().map((s: any) => s.schema),
  ]) {
    try {
      const shape = predicate.shape.validate(schema);
      if (equalBytes(shape.shapeId(), predicate.shapeId)) {
        validated = [shape, schema];
        break;
      }
    } catch {
      // not this schema
    }
  }
  if (!validated) return undefined;
  const [shape, schema] = validated;
  const binding = shape.bind(predicate.bindingValues);
  if (!equalBytes(binding.bindingId(), predicate.bindingId)) return undefined;
  const input = shape.query().aggregateInput();
  if (!input) return [shape, binding];
  const inputShape = input.validate(schema);
  return [inputShape, inputShape.bind(predicate.bindingValues)];
}

/** Authority checks use current global output; local checks compare against an explicit snapshot. */
export async function shapePredicateOutputsDiffer(
  node: NodeState,
  predicate: any,
  snapshot: any,
  comparisonSnapshot: any | undefined,
): Promise<boolean> {
  const tryValidate = (schema: any) => {
    try {
      const shape = predicate.shape.validate(schema);
      return equalBytes(shape.shapeId(), predicate.shapeId) ? shape : undefined;
    } catch {
      return undefined;
    }
  };
  // Only mismatched views scan schema history.
  let shape = tryValidate(node.catalogue.schema);
  if (!shape)
    for (const s of node.catalogue.catalogueSchemas.valuesArray())
      if ((shape = tryValidate(s.schema))) break;
  if (!shape) return true;
  const binding = shape.bind(predicate.bindingValues);
  if (!equalBytes(binding.bindingId(), predicate.bindingId)) return true;
  if (shape.query().aggregate !== undefined) {
    const atBase = await node.queryRowsAtSnapshot(shape, binding, snapshot);
    const atNow = comparisonSnapshot
      ? await node.queryRowsAtSnapshot(shape, binding, comparisonSnapshot)
      : await node.queryRows(shape, binding, "Global");
    return !aggregateQueryOutputsEquivalent(atBase, atNow);
  }
  const atBase = await shapeOutputTxSetAtSnapshot(node, shape, binding, snapshot);
  const atNow = comparisonSnapshot
    ? await shapeOutputTxSetAtSnapshot(node, shape, binding, comparisonSnapshot)
    : await shapeOutputTxSetNow(node, shape, binding);
  const a = atBase.toArray();
  const b = atNow.toArray();
  return (
    a.length !== b.length ||
    a.some((x, i) => !equalBytes(x[0], b[i][0]) || !txIdEquals(x[1], b[i][1]))
  );
}

function aggregateQueryOutputsEquivalent(left: any[], right: any[]): boolean {
  if (left.length !== right.length) return false;
  const rightById = new Map<string, any>();
  for (const row of right) {
    const key = Array.from(row.rowUuid() as Uint8Array).join(",");
    if (rightById.has(key)) return false;
    rightById.set(key, row);
  }
  for (const row of left) {
    const key = Array.from(row.rowUuid() as Uint8Array).join(",");
    const other = rightById.get(key);
    if (!other) return false;
    rightById.delete(key);
    if (!row.aggregatePayloadEquivalent(other)) return false;
  }
  return true;
}

const newOutputSet = () => new SortedSet<[Uuid, TxId]>(cmpTuple(cmpBytes, cmpTxId));

async function shapeOutputTxSetNow(
  node: NodeState,
  shape: any,
  binding: any,
): Promise<SortedSet<[Uuid, TxId]>> {
  const table = shape.query().table;
  const set = newOutputSet();
  for (const row of await node.queryRows(shape, binding, "Global")) {
    const txId = await node.visibleGlobalContentTxIdInSchemaNow(
      shape.schemaVersion(),
      table,
      row.rowUuid(),
    );
    if (txId) set.add([row.rowUuid(), txId]);
  }
  return set;
}

async function shapeOutputTxSetAtSnapshot(
  node: NodeState,
  shape: any,
  binding: any,
  snapshot: any,
): Promise<SortedSet<[Uuid, TxId]>> {
  // An origin snapshot has no visible application rows: do not ask the historical compiler for provenance.
  if (snapshot.globalBase === 0n && snapshot.localBase === 0n && snapshot.dots.length === 0)
    return newOutputSet();
  const table = shape.query().table;
  const set = newOutputSet();
  for (const row of await node.queryRowsAtSnapshot(shape, binding, snapshot)) {
    const rowUuid = row.rowUuid();
    const txId = await node.snapshotContentWitness(shape.schemaVersion(), table, rowUuid, snapshot);
    if (!txId)
      throw nodeError(
        "InvalidStoredValue",
        "historical query output row must have visible content",
      );
    set.add([rowUuid, txId]);
  }
  return set;
}

/**
 * The rejection a commit unit's write policies call for, if any: `AuthorizationDenied` when a check fails, or
 * `MalformedCommit` naming the unsupported pattern when evidence exceeds the budget.
 */
export async function commitUnitWritePolicyRejection(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  ingestContext: CommitUnitIngestContext | undefined,
): Promise<any | undefined> {
  const admission = commitUnitAdmissionSubject(tx, ingestContext);
  if (admission.kind === "Err") return admission.reason;
  if (admission.subject === undefined) return undefined;
  const permissionSubject = admission.subject;
  const denied = { kind: "AuthorizationDenied" };
  // Non-root branch writes have mandatory canonical operation intent bound to the exact authored version.
  const branchVersions = versions.filter((v) => v.branchKey().values.length > 0);
  if (branchVersions.length > 0) {
    if (tx.kind !== "Mergeable") return denied;
    const provenance = tx.contributionMerge;
    if (!provenance) return denied;
    const tableIdOf = (v: VersionRecord): number | undefined => {
      try {
        return physicalTableIdForSchema(node, v.schemaVersion(), v.table());
      } catch {
        return undefined;
      }
    };
    // Content and deletion registers may emit separate versions for one branch row: one intent per row/head.
    const coordinates = new Set<string>();
    for (const version of branchVersions) {
      const tableId = tableIdOf(version);
      if (tableId === undefined) return denied;
      coordinates.add(
        `${tableId}\0${Array.from(version.schemaVersion() as Uint8Array)}\0${Array.from(version.rowUuid() as Uint8Array)}\0${BranchKey.key(version.branchKey())}`,
      );
    }
    if (provenance.branchWriteIntents.length !== coordinates.size) return denied;
    for (const intent of provenance.branchWriteIntents) {
      const matching = branchVersions.filter(
        (v) =>
          equalBytes(v.schemaVersion(), intent.authoredSchema) &&
          equalBytes(v.rowUuid(), intent.rowUuid) &&
          BranchKey.equals(v.branchKey(), intent.head) &&
          tableIdOf(v) === intent.physicalTableId,
      );
      if (matching.length === 0) return denied;
      if (intent.operation.kind === "ViewUpdateCopy") {
        if (
          matching.some((v) => v.deletion() === "Deleted") ||
          !(await node.branchViewCopySatisfiesReadForWriteVisibility(
            intent.operation.evidence,
            intent.authoredSchema,
            permissionSubject,
            tx.txId,
          ))
        )
          return denied;
      }
      // ExactHeadInsert/ExactHeadUpdate: linear history, checked by the ordinary policies below.
    }
  }
  if (tx.kind === "Mergeable")
    for (const version of versions)
      if (!(await node.versionSatisfiesReadForWriteVisibility(version, permissionSubject, tx.txId)))
        return denied;
  const decision = await node.commitUnitWritePoliciesAllow(versions, permissionSubject, tx.txId);
  switch (decision.kind) {
    case "Allowed":
      return undefined;
    case "Denied":
      return denied;
    default:
      return { kind: "MalformedCommit", detail: decision.reason };
  }
}

/** Evaluate every write policy of one candidate commit unit under the active exact session scope. */
export async function commitUnitSatisfiesWritePolicy(
  node: NodeState,
  versions: VersionRecord[],
  author: any,
  candidateTxId: TxId,
): Promise<boolean> {
  return (
    (await node.commitUnitWritePoliciesAllow(versions, author, candidateTxId)).kind === "Allowed"
  );
}

const sameParkedAuthority = (
  a: CommitUnitIngestContext | undefined,
  b: CommitUnitIngestContext | undefined,
): boolean =>
  a === undefined || b === undefined
    ? a === b
    : a.trust === b.trust &&
      a.admittedWriteAuthorization === b.admittedWriteAuthorization &&
      AuthorSubject.equals(a.identity, b.identity);

export function parkCommitUnitIfMissingSchemaVersionsWithMode(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  nowMs: number,
  mode: CommitUnitParkMode,
): boolean {
  const schemasKnown = versions.every((v) =>
    node.catalogue.catalogueSchemas.has(v.schemaVersion()),
  );
  // A write chained on a predecessor parked here waits behind it (SPEC 4 §4.6).
  const behindParkedPredecessor =
    mode.ingressRole === "Authority" && hasParkedSameNodePredecessor(node, tx, versions);
  if (schemasKnown && !behindParkedPredecessor) return false;
  const parked: SortedMap<TxId, ParkedCommitUnit> = node.parking.parkedCommitUnits;
  const existing = parked.get(tx.txId);
  if (existing) {
    if (
      !transactionEquals(existing.tx, tx) ||
      existing.versions.length !== versions.length ||
      existing.versions.some((v: VersionRecord, i: number) => !versionRecordEquals(v, versions[i]))
    )
      throw nodeError("ConflictingCommitUnit", tx.txId);
    if (!sameParkedAuthority(existing.ingestContext, mode.ingestContext))
      throw nodeError("ConflictingCommitUnit", tx.txId);
    if (existing.ingestContext && mode.ingestContext)
      existing.ingestContext.versionReceiptsValidated &&=
        mode.ingestContext.versionReceiptsValidated;
    existing.ingressRole = strongestIngressRole(existing.ingressRole, mode.ingressRole);
    return true;
  }
  node.syncMetrics.parkedOrphans += 1;
  if (!schemasKnown) {
    node.syncMetrics.parkedCatalogueOrphans += 1;
    node.parking.parkedCatalogueCommitUnits.add(tx.txId);
  }
  parked.set(tx.txId, {
    tx,
    versions: versions.slice(),
    nowMs,
    ingestContext: mode.ingestContext,
    ingressRole: mode.ingressRole,
  });
  return true;
}

/**
 * Whether a version names, as its pending predecessor, an older write of the unit's own node parked here for the
 * authority for less than `PARKED_PREDECESSOR_WAIT_MS` on the authority's ingest clock.
 */
function hasParkedSameNodePredecessor(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
): boolean {
  return versions.some((version) => {
    const predecessor = version.base().pending;
    if (!predecessor || pendingPredecessorRefusal(predecessor, tx.txId) !== undefined) return false;
    const unit = node.parking.parkedCommitUnits.get(predecessor);
    return (
      !!unit &&
      unit.ingressRole === "Authority" &&
      unit.nowMs + PARKED_PREDECESSOR_WAIT_MS > node.parking.newestIngestMs
    );
  });
}

export async function drainParkedCommitUnits(node: NodeState): Promise<Outcome> {
  const updates = PublicationOutcome.settled<SyncMessage[]>([]);
  for (;;) {
    const parked: SortedMap<TxId, ParkedCommitUnit> = node.parking.parkedCommitUnits;
    const ready = [...parked]
      .filter(
        ([, unit]) =>
          unit.ingressRole !== "Relay" &&
          unit.versions.every((v: VersionRecord) =>
            node.catalogue.catalogueSchemas.has(v.schemaVersion()),
          ) &&
          // Its parked predecessor goes first, on a later pass.
          !hasParkedSameNodePredecessor(node, unit.tx, unit.versions),
      )
      .map(([txId]) => txId);
    if (ready.length === 0) break;
    for (const txId of ready) {
      const unit = parked.delete(txId);
      if (!unit) continue;
      node.syncMetrics.parkedOrphansResolved += 1;
      if (node.parking.parkedCatalogueCommitUnits.delete(txId))
        node.syncMetrics.parkedCatalogueOrphansResolved += 1;
      updates.extend(
        await ingestCommitUnitOnce(node, unit.tx, unit.versions, unit.nowMs, unit.ingestContext),
      );
    }
  }
  return updates;
}

export async function drainParkedRelayCommitUnits(node: NodeState): Promise<void> {
  for (;;) {
    const parked: SortedMap<TxId, ParkedCommitUnit> = node.parking.parkedCommitUnits;
    const ready = [...parked]
      .filter(
        ([, unit]) =>
          unit.ingressRole === "Relay" &&
          unit.versions.every((v: VersionRecord) =>
            node.catalogue.catalogueSchemas.has(v.schemaVersion()),
          ),
      )
      .map(([txId]) => txId);
    if (ready.length === 0) break;
    for (const txId of ready) {
      const unit = parked.delete(txId);
      if (!unit) continue;
      // A relay has no fate authority: an incomplete row record is discarded once its schema is known.
      if (malformedAuthoredVersionReason(node, unit.versions) !== undefined) {
        node.parking.parkedCatalogueCommitUnits.delete(txId);
        node.syncMetrics.droppedMalformedRelayCommitUnits += 1;
        continue;
      }
      node.syncMetrics.parkedOrphansResolved += 1;
      if (node.parking.parkedCatalogueCommitUnits.delete(txId))
        node.syncMetrics.parkedCatalogueOrphansResolved += 1;
      await ingestRelayCommitUnitOnce(node, unit.tx, unit.versions);
    }
  }
}

export async function removeRejectedLocalVersions(
  node: NodeState,
  txId: TxId,
  tx: StoredTransaction,
  batch: DatabaseBatch,
): Promise<any | undefined> {
  const rejected: VersionRow[] = await node.queryVersionsForTx(txId);
  if (rejected.length === 0) return undefined;
  let rejectedPayload: any;
  if (equalBytes(txId.node, node.nodeUuid) && tx.fate.kind === "Rejected") {
    const rejectedTxValues = rejectedTransactionValues(tx.nodeAlias, tx.tx, tx.fate.reason);
    batch.insert("jazz_rejected_transactions", rejectedTxValues.slice());
    const rejectedTxTable = node.catalogue.schema
      .storageTables()
      .find((t: any) => t.name === "jazz_rejected_transactions");
    if (!rejectedTxTable)
      throw nodeError("InvalidStoredValue", "missing rejected transaction table");
    const rejectedTxRecord = ownedRecordFromStorageValues(rejectedTxTable, rejectedTxValues);
    const rejectedVersions: any[] = [];
    for (const version of rejected) {
      const schemaVersion = node.schemaVersionForAlias(version.schemaVersionAlias());
      if (!schemaVersion) throw nodeError("InvalidStoredValue", "unknown schema version alias");
      const tableSchema = node.tableInSchemaRef(version.table, schemaVersion);
      const record = ownedRecordFromStorageValues(
        tableSchema.rejectedVersionsStorageTable(),
        rejectedVersionValues(tableSchema, version),
      );
      const [storageTable, storageRecord] = rejectedVersionStorageWriteBinding(
        node,
        version,
        record,
      );
      batch.insert(storageTable, storageRecord);
      rejectedVersions.push(node.newRejectedVersion(version.table, record));
    }
    rejectedVersions.sort(
      (a, b) =>
        cmpStr(a.table(), b.table()) ||
        cmpBytes(a.rowUuid(), b.rowUuid()) ||
        cmpNum(+(a.deletion() !== undefined), +(b.deletion() !== undefined)),
    );
    rejectedPayload = node.newRejectedTransaction(txId, rejectedTxRecord, rejectedVersions);
  }
  for (const version of rejected) {
    if (writeAheadCurrentDelete(node, batch, version))
      await restoreAheadOverlayAfterReject(node, batch, version);
    batch.delete(versionStorageTableForRow(node, version), versionStoragePrimaryKey(node, version));
  }
  // The rejected images are gone, so is the list naming them.
  batch.delete(TX_TOUCHED_ROWS_TABLE, PK.composite([PK.u64(txId.time), PK.u64(tx.nodeAlias)]));
  node.invalidateTxVersionTablesCache(txId);
  return rejectedPayload;
}

export type AdmissionSubject =
  | { kind: "Ok"; subject: any | undefined }
  | { kind: "Err"; reason: any };

/**
 * The cheap identity part of commit-unit admission: the permission subject write policies evaluate, or the refusal
 * when the connection may not make the unit at all. `subject: undefined` means no policy evaluation is owed.
 */
export function commitUnitAdmissionSubject(
  tx: Transaction,
  ingestContext: CommitUnitIngestContext | undefined,
): AdmissionSubject {
  const denied: AdmissionSubject = { kind: "Err", reason: { kind: "AuthorizationDenied" } };
  let permissionSubject;
  if (ingestContext) {
    switch (ingestContext.trust) {
      case "TrustedAdmin":
        return { kind: "Ok", subject: undefined };
      case "Session":
        if (!AuthorSubject.equals(tx.madeBy, ingestContext.identity)) return denied;
        permissionSubject = ingestContext.identity;
        break;
      // Relay transport has no permission subject: it must reach an authority through topology-owned admission.
      case "Relay":
        return ingestContext.admittedWriteAuthorization
          ? { kind: "Ok", subject: undefined }
          : denied;
      default:
        permissionSubject = tx.permissionSubject ?? tx.madeBy;
    }
  } else permissionSubject = tx.permissionSubject ?? tx.madeBy;
  // Relayed anonymous sessions stay read-only without changing trusted-backend attribution.
  if (permissionSubject.isAnonymous()) return denied;
  return { kind: "Ok", subject: permissionSubject };
}
