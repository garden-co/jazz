// Authority and relay commit-unit admission, known-transaction reconciliation, and reset-bundle bulk ingest.
import { SortedMap, SortedSet, cmpStr, cmpTuple, cmpBytes } from "../../../support/collections.js";
import {
  nodeError,
  PublicationOutcome,
  CommitUnitTrust,
  SKEW_TOLERANCE_MS,
  BranchKey,
  cmpTxId,
  txIdEquals,
  txIdKey,
  txTimePhysicalMs,
  authorityNowMs,
  validateVersionRecords,
  commitUnitLimitViolation,
  versionRecordEquals,
  cmpVersionRecord,
  transactionEquals,
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
  VersionRow,
  canonicalVersions,
  reconcileFate,
  knownTransactionPayloadMatches,
  knownTransactionPayloadMatchesRedactedPermissionSubject,
  knownTransactionPayloadMatchesRedactedCardinality,
  transactionWithoutPermissionSubject,
  transactionValuesWithPartialFlag,
} from "../codec.js";
import {
  versionStorageWriteBinding,
  versionStoragePrimaryKey,
  contributionMergeStorageValue,
  authoredColumnIdsForNames,
  physicalCurrentTableForSchema,
} from "../physical.js";
import {
  commitUnitWriteCountMatches,
  fateUpdateDurabilityClaim,
  defaultCommitUnitParkMode,
  validateReceivedViewBundleGlobalTimeDurability,
} from "../ingest.js";
import {
  applyFateUpdate,
  drainParkedCommitUnits,
  drainParkedRelayCommitUnits,
  validateExclusiveCommitUnit,
  commitUnitWritePolicyRejection,
  parkCommitUnitIfMissingSchemaVersionsWithMode,
  commitUnitAdmissionSubject,
} from "./fates.js";
import {
  rejectMalformedCommit,
  ingestRejectedTransaction,
  malformedAuthoredVersionReason,
  prepareAuthoredSchemaVariantsForCommit,
  ingestTransactionAndVersions,
  prepareExactHistoryVersion,
  ingestTransactionAndVersionsWithCurrentIndexesInBatch,
  validateViewPayloadVersions,
  stageTransactionAndVersionsWithCurrentIndexes,
  finalizeStagedTransactionIngest,
} from "./validation.js";
import {
  mergeOpRejection,
  globalCurrentSeqInBatch,
  writeGlobalCurrentUpdate,
  rebaseAheadOverlays,
} from "./view_updates.js";
import { recordSupersededByRefusal } from "./write_base.js";

type Outcome = PublicationOutcome<SyncMessage[]>;

const fateUpdate = (
  txId: TxId,
  fate: Fate,
  globalTime?: bigint,
  durability?: DurabilityTier,
): SyncMessage => ({
  kind: "FateUpdate",
  txId,
  fate,
  globalTime,
  durability,
});

/** Store a terminal rejection and answer with its fate update. */
async function rejectWith(node: NodeState, tx: Transaction, reason: any): Promise<Outcome> {
  const fate: Fate = { kind: "Rejected", reason };
  await ingestRejectedTransaction(node, tx, fate);
  return PublicationOutcome.settled([fateUpdate(tx.txId, fate)]);
}

async function storedVersionRecords(node: NodeState, txId: TxId): Promise<VersionRecord[]> {
  const rows: any[] = await node.queryVersionsForTx(txId);
  return rows.map((stored) => node.versionRecordFromRow(stored)).sort(cmpVersionRecord);
}

const sameVersions = (a: VersionRecord[], b: VersionRecord[]) =>
  a.length === b.length && a.every((v, i) => versionRecordEquals(v, b[i]));
const isSessionOrRelay = (ctx: CommitUnitIngestContext | undefined) =>
  ctx?.trust === "Session" || ctx?.trust === "Relay";

/** Ingest a commit unit as fate authority. */
export async function ingestCommitUnit(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  nowMs: number,
): Promise<Outcome> {
  node.requireCatalogueReady();
  return ingestCommitUnitWithContext(node, tx, versions, nowMs, undefined);
}

/**
 * Ingest a commit unit as fate authority with an optional authenticated connection identity. Policy is evaluated
 * against the connection subject; `made_by` is provenance unless the link is an untrusted session.
 */
export async function ingestCommitUnitWithContext(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  nowMs: number,
  ingestContext: CommitUnitIngestContext | undefined,
): Promise<Outcome> {
  node.requireCatalogueReady();
  node.parking.newestIngestMs = Math.max(node.parking.newestIngestMs, nowMs);
  // A session or relay transport may not select a durable policy capability.
  if (isSessionOrRelay(ingestContext)) tx = { ...tx, permissionSubject: undefined };
  const updates = await ingestCommitUnitChecked(node, tx, versions, nowMs, ingestContext);
  updates.extend(await drainParkedCommitUnits(node));
  return updates;
}

/** Authority ingest of one received unit: the receipt and size checks, then one admission attempt. */
async function ingestCommitUnitChecked(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  nowMs: number,
  ingestContext: CommitUnitIngestContext | undefined,
): Promise<Outcome> {
  // A checked wire decoder already validated these exact receipts.
  const needsReceiptCheck =
    !ingestContext ||
    (!CommitUnitTrust.isTrusted(ingestContext.trust) && !ingestContext.versionReceiptsValidated);
  if (needsReceiptCheck && !validateVersionRecords(versions))
    return PublicationOutcome.settled(
      await rejectMalformedCommit(node, tx, "malformed version receipt"),
    );
  const violation = commitUnitLimitViolation(versions);
  if (violation !== undefined)
    return rejectWith(node, tx, { kind: "MalformedCommit", detail: violation });
  if (commitUnitWriteCountMatches(tx, versions.length)) {
    const reason = malformedAuthoredVersionReason(node, versions);
    if (reason !== undefined)
      return PublicationOutcome.settled(await rejectMalformedCommit(node, tx, reason));
  }
  const clockBeforeIngest = node.clock.clone();
  try {
    return await ingestCommitUnitOnce(node, tx, versions, nowMs, ingestContext);
  } catch (error) {
    restoreClockAfterFailedAuthorityIngest(node, clockBeforeIngest);
    throw error;
  }
}

/**
 * Undo speculative authority clock work after a rejected ingest without reusing a sequence that became durable
 * before a later derived write failed.
 */
function restoreClockAfterFailedAuthorityIngest(node: NodeState, before: any): void {
  const clock = node.clock;
  if (
    clock.committedGlobalTime === before.committedGlobalTime &&
    clock.appliedGlobalTimesAfterFrontier.size === before.appliedGlobalTimesAfterFrontier.size &&
    clock.appliedGlobalTimesAfterFrontier
      .toArray()
      .every((t: bigint, i: number) => t === before.appliedGlobalTimesAfterFrontier.toArray()[i])
  ) {
    node.clock = before;
    return;
  }
  const highestRecovered: bigint =
    clock.appliedGlobalTimesAfterFrontier.last() ?? clock.committedGlobalTime;
  let register: bigint = clock.globalTimeRegister;
  if (before.globalTimeRegister > register) register = before.globalTimeRegister;
  if (highestRecovered > register) register = highestRecovered;
  clock.globalTimeRegister = register;
}

/** Mint the next global timestamp for a self-finalized pending commit and accept it at Global durability. */
async function selfAccept(node: NodeState, txId: TxId): Promise<void> {
  const globalTime = node.clock.allocateGlobalTime(txTimePhysicalMs(txId.time));
  // This node mints the seq, so it derives the row's post-image.
  node.mintingGlobalTime = true;
  try {
    await applyFateUpdate(node, txId, { kind: "Accepted" }, globalTime, "Global");
  } finally {
    node.mintingGlobalTime = false;
  }
}

/**
 * Finalize a locally-authored pending mergeable commit as the global authority. Idempotent: a non-pending
 * transaction is left untouched.
 */
export async function finalizeLocalMergeableCommit(
  node: NodeState,
  txId: TxId,
): Promise<PublicationOutcome<void>> {
  node.requireCatalogueReady();
  const stored: StoredTransaction | undefined = await node.queryTransaction(txId);
  if (!stored) throw nodeError("MissingTransaction", txId);
  if (stored.tx.kind !== "Mergeable")
    throw nodeError("UnsupportedCommitUnit", "self-finalize is mergeable-only");
  if (stored.fate.kind !== "Pending") return PublicationOutcome.settled(undefined);
  const rows: any[] = await node.queryVersionsForTx(txId);
  const records = rows.map((row) => node.versionRecordFromRow(row));
  const permissionSubject =
    node.openTx.localPermissionSubjects.get(txIdKey(txId)) ??
    stored.tx.permissionSubject ??
    stored.tx.madeBy;
  const policyReason = await commitUnitWritePolicyRejection(
    node,
    { ...stored.tx, permissionSubject },
    records,
    undefined,
  );
  const reason = policyReason ?? (await mergeOpRejection(node, txId, records));
  if (reason !== undefined) {
    await ingestRejectedTransaction(node, stored.tx, { kind: "Rejected", reason });
    return PublicationOutcome.settled(undefined);
  }
  await selfAccept(node, txId);
  // Core merged the writes into each row's post-image per column; nothing further is minted.
  return PublicationOutcome.settled(undefined);
}

/**
 * Finalize a locally-authored pending exclusive commit as the global authority, validating the in-memory commit
 * unit through the same path as a foreign one.
 */
export async function finalizeLocalExclusiveCommit(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
): Promise<PublicationOutcome<Fate>> {
  node.requireCatalogueReady();
  const txId = tx.txId;
  if (tx.kind !== "Exclusive")
    throw nodeError(
      "UnsupportedCommitUnit",
      "exclusive self-finalize requires an exclusive transaction",
    );
  const stored: StoredTransaction | undefined = await node.queryTransaction(txId);
  if (!stored) throw nodeError("MissingTransaction", txId);
  if (stored.fate.kind !== "Pending") return PublicationOutcome.settled(stored.fate);
  const reject = async (reason: any): Promise<PublicationOutcome<Fate>> => {
    const fate: Fate = { kind: "Rejected", reason };
    await ingestRejectedTransaction(node, tx, fate);
    return PublicationOutcome.settled(fate);
  };
  const policyReason = await commitUnitWritePolicyRejection(node, tx, versions, undefined);
  if (policyReason !== undefined) return reject(policyReason);
  // Row/absent/predicate reads and per-write first-committer-wins, through the shared authority path.
  if (!(await validateExclusiveCommitUnit(node, tx, versions)))
    return reject({ kind: "ExclusiveConflict" });
  const mergeReason = await mergeOpRejection(node, txId, versions);
  if (mergeReason !== undefined) return reject(mergeReason);
  await selfAccept(node, txId);
  return PublicationOutcome.settled({ kind: "Accepted" });
}

/** Ingest an unfated commit unit at a Local relay without assigning fate. */
export const ingestRelayCommitUnit = (
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
): Promise<void> => ingestRelayCommitUnitWithEncoderTrust(node, tx, versions, false);

export async function ingestRelayCommitUnitWithEncoderTrust(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  trustedEncoder: boolean,
): Promise<void> {
  node.requireCatalogueReady();
  tx = { ...tx, permissionSubject: undefined };
  if (
    (!trustedEncoder && !validateVersionRecords(versions)) ||
    commitUnitLimitViolation(versions) !== undefined ||
    !commitUnitWriteCountMatches(tx, versions.length) ||
    malformedAuthoredVersionReason(node, versions) !== undefined
  )
    throw nodeError("UnsupportedCommitUnit", "malformed relay commit unit");
  await ingestRelayCommitUnitOnce(node, tx, versions);
  await drainParkedRelayCommitUnits(node);
}

/** Whether an untrusted retransmit is already stored with the same durable payload and version set. */
export async function knownUntrustedCommitUnitMatches(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
): Promise<boolean> {
  const existing: StoredTransaction | undefined = await node.queryTransaction(tx.txId);
  if (!existing) return false;
  const existingVersions = await storedVersionRecords(node, tx.txId);
  return (
    knownTransactionPayloadMatchesRedactedPermissionSubject(existing.tx, tx) &&
    sameVersions(existingVersions, canonicalVersions(versions.slice()))
  );
}

export async function ingestRelayCommitUnitOnce(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
): Promise<void> {
  if (tx.kind !== "Mergeable" && tx.kind !== "Exclusive")
    throw nodeError("UnsupportedCommitUnit", "unsupported commit unit kind");
  versions = canonicalVersions(versions);
  const existing: StoredTransaction | undefined = await node.queryTransaction(tx.txId);
  if (existing) {
    const existingVersions = await storedVersionRecords(node, tx.txId);
    // Once Core sequenced the unit this relay stores Core's post-images: match a resend by addressed rows.
    const samePayload =
      existing.fate.kind === "Accepted" && existing.globalTime !== undefined
        ? sameAddressedRows(existingVersions, versions)
        : sameVersions(existingVersions, versions);
    if (!knownTransactionPayloadMatchesRedactedPermissionSubject(existing.tx, tx) || !samePayload)
      throw nodeError("ConflictingCommitUnit", tx.txId);
    return;
  }
  if (!commitUnitWriteCountMatches(tx, versions.length))
    throw nodeError(
      "UnsupportedCommitUnit",
      "commit unit version count does not match transaction n_total_writes",
    );
  const relayMode = { ...defaultCommitUnitParkMode(), ingressRole: "Relay" as const };
  if (
    parkCommitUnitIfMissingSchemaVersionsWithMode(
      node,
      tx,
      versions,
      Number.MAX_SAFE_INTEGER - SKEW_TOLERANCE_MS,
      relayMode,
    )
  )
    return;
  await prepareAuthoredSchemaVariantsForCommit(node, versions);
  await ingestTransactionAndVersions(node, tx, versions, { kind: "Pending" }, undefined, "Local");
}

/**
 * One admission attempt of a unit at the authority: its decision and, when refused, the superseded-writes
 * markers the refusal leaves.
 */
export async function ingestCommitUnitOnce(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  nowMs: number,
  ingestContext: CommitUnitIngestContext | undefined,
): Promise<Outcome> {
  const txId = tx.txId;
  const predecessors = versions.flatMap((version) => {
    const pending = version.base().pending;
    return pending
      ? [
          [
            version.schemaVersion(),
            version.table(),
            version.branchKey(),
            version.rowUuid(),
            pending,
          ] as const,
        ]
      : [];
  });
  const outcome = await decideCommitUnit(node, tx, versions, nowMs, ingestContext);
  if (
    predecessors.length > 0 &&
    outcome.value.some(
      (m: SyncMessage) =>
        m.kind === "FateUpdate" && m.fate.kind === "Rejected" && txIdEquals(m.txId, txId),
    )
  )
    await recordSupersededByRefusal(node, txId, predecessors as any);
  return outcome;
}

async function decideCommitUnit(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  nowMs: number,
  ingestContext: CommitUnitIngestContext | undefined,
): Promise<Outcome> {
  const redactPermissionSubject = isSessionOrRelay(ingestContext);
  versions = canonicalVersions(versions);
  if (!commitUnitWriteCountMatches(tx, versions.length))
    return rejectWith(node, tx, {
      kind: "MalformedCommit",
      detail: "commit unit version count does not match transaction n_total_writes",
    });
  const malformed = malformedAuthoredVersionReason(node, versions);
  if (malformed !== undefined)
    return PublicationOutcome.settled(await rejectMalformedCommit(node, tx, malformed));
  const existing: StoredTransaction | undefined = await node.queryTransaction(tx.txId);
  if (existing) {
    const matches = redactPermissionSubject
      ? knownTransactionPayloadMatchesRedactedPermissionSubject(existing.tx, tx)
      : knownTransactionPayloadMatches(existing.tx, tx);
    const echo = () =>
      PublicationOutcome.settled([
        fateUpdate(
          tx.txId,
          existing.fate,
          existing.globalTime,
          fateUpdateDurabilityClaim(existing.fate, existing.durability),
        ),
      ]);
    if (tx.kind === "Exclusive" || existing.fate.kind === "Rejected") {
      if (!matches) throw nodeError("ConflictingCommitUnit", tx.txId);
      return echo();
    }
    const existingVersions = await storedVersionRecords(node, tx.txId);
    // Once Core accepted a unit with a seq its stored versions are post-images: tx identity decides redelivery,
    // which must still address the same rows with the same deletion intent.
    const storedPostImages = existing.fate.kind === "Accepted" && existing.globalTime !== undefined;
    const samePayload = storedPostImages
      ? sameAddressedRows(existingVersions, versions)
      : sameVersions(existingVersions, versions);
    if (!matches || !samePayload) throw nodeError("ConflictingCommitUnit", tx.txId);
    // A stored Pending mergeable unit has no authority fate yet: re-enter validation instead of echoing Pending.
    if (!(tx.kind === "Mergeable" && existing.fate.kind === "Pending")) return echo();
  }
  if (
    parkCommitUnitIfMissingSchemaVersionsWithMode(node, tx, versions, nowMs, {
      ...defaultCommitUnitParkMode(),
      ingestContext,
    })
  )
    return PublicationOutcome.settled([]);
  await prepareAuthoredSchemaVariantsForCommit(node, versions);
  // Validate untrusted metadata before a missing history parent can park the unit.
  try {
    node.validateContributionMergeOperationIdentities(tx);
  } catch {
    return PublicationOutcome.settled(
      await rejectMalformedCommit(node, tx, "invalid contribution provenance"),
    );
  }
  if (txTimePhysicalMs(tx.txId.time) > nowMs + SKEW_TOLERANCE_MS)
    return rejectWith(node, tx, { kind: "ClientClockTooFarAhead" });
  const admission = commitUnitAdmissionSubject(tx, ingestContext);
  if (admission.kind === "Err") return rejectWith(node, tx, admission.reason);
  // A unit of an unknown kind is refused before anything else is looked up for it.
  if (tx.kind !== "Mergeable" && tx.kind !== "Exclusive")
    throw nodeError("UnsupportedCommitUnit", "unsupported commit unit kind");
  const policyReason = await commitUnitWritePolicyRejection(node, tx, versions, ingestContext);
  if (policyReason !== undefined) return rejectWith(node, tx, policyReason);
  if (tx.kind === "Exclusive" && !(await validateExclusiveCommitUnit(node, tx, versions)))
    return rejectWith(node, tx, { kind: "ExclusiveConflict" });
  const mergeReason = await mergeOpRejection(node, tx.txId, versions);
  if (mergeReason !== undefined) return rejectWith(node, tx, mergeReason);
  const globalTime: bigint = node.clock.allocateGlobalTime(
    authorityNowMs(nowMs, txTimePhysicalMs(tx.txId.time)),
  );
  const fate: Fate = { kind: "Accepted" };
  // This node mints the seq, so it merges the patch into the row's post-image.
  node.mintingGlobalTime = true;
  try {
    await ingestKnownTransaction(node, tx, versions, fate, globalTime, "Global");
  } finally {
    node.mintingGlobalTime = false;
  }
  return PublicationOutcome.settled([fateUpdate(tx.txId, fate, globalTime, "Global")]);
}

export async function ingestKnownTransaction(
  node: NodeState,
  tx: Transaction,
  versions: VersionRecord[],
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
): Promise<void> {
  node.requireCatalogueReady();
  node.mergeTxTime(tx.txId.time);
  versions = canonicalVersions(versions);
  await prepareAuthoredSchemaVariantsForCommit(node, versions);
  const existing: StoredTransaction | undefined = await node.queryTransaction(tx.txId);
  if (!existing)
    return ingestTransactionAndVersions(node, tx, versions, fate, globalTime, durability);
  if (
    !(
      knownTransactionPayloadMatchesRedactedPermissionSubject(existing.tx, tx) ||
      (existing.isPartial && knownTransactionPayloadMatchesRedactedCardinality(existing.tx, tx))
    )
  )
    throw nodeError("ConflictingCommitUnit", tx.txId);
  const reconciled: StoredTransaction = { ...existing };
  reconcileFate(reconciled, fate, globalTime, durability);
  if (reconciled.fate.kind === "Rejected")
    return ingestTransactionAndVersions(node, tx, versions, fate, globalTime, durability);
  // Normalize aliases before establishing the batch's resident base.
  const schemas = new SortedSet<Uuid>(
    cmpBytes,
    versions.map((v) => v.schemaVersion()),
  );
  for (const schema of schemas.toArray()) await node.ensureSchemaVersionAlias(schema);
  const batch: DatabaseBatch = node.database.openBatch();
  const versionBundles: VersionRecord[] = [];
  const previouslyStored: VersionRow[] = [];
  // A write is stored at its seq once accepted, at 0 while pending.
  const seq =
    reconciled.fate.kind === "Accepted" &&
    reconciled.globalTime !== undefined &&
    !node.mintingGlobalTime
      ? reconciled.globalTime
      : 0n;
  for (const version of versions) {
    const stored = await prepareExactHistoryVersion(
      node,
      existing.nodeAlias,
      tx.txId.time,
      seq,
      version,
    );
    const [table, record] = versionStorageWriteBinding(node, stored, existing.tx.madeBy);
    const key = versionStoragePrimaryKey(node, stored);
    if (globalTime !== undefined && fate.kind === "Accepted" && !node.mintingGlobalTime) {
      // The authority's post-image replaces this node's copy, which moves from its pending key to its seq.
      if (seq !== 0n) batch.delete(table, versionStoragePrimaryKey(node, stored.withSeq(0n)));
      batch.updateRaw(table, key, record);
      versionBundles.push(version);
      continue;
    }
    switch (await batch.ensureExact(node.database, table, key, record)) {
      case "Inserted":
        versionBundles.push(version);
        break;
      case "AlreadyIdentical":
        previouslyStored.push(stored);
        break;
      case "Conflict":
        throw nodeError("ConflictingCommitUnit", tx.txId);
    }
  }
  if (versionBundles.length === 0 && !existing.isPartial) {
    await applyFateUpdate(node, tx.txId, fate, globalTime, durability);
    return;
  }
  return ingestTransactionAndVersionsWithCurrentIndexesInBatch(
    node,
    batch,
    tx,
    versionBundles,
    previouslyStored,
    fate,
    globalTime,
    durability,
    true,
    false,
  );
}

export async function stageKnownTransaction(
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
  versions = canonicalVersions(versions);
  // The batched ViewUpdate path has no authority role and cannot synthesize a fate: this is the backstop.
  validateViewPayloadVersions(node, versions);
  node.mergeTxTime(tx.txId.time);
  const txId = tx.txId;
  if (await node.queryTransaction(txId))
    return ingestKnownTransaction(node, tx, versions, fate, globalTime, durability);
  const [stagedVersions, stagedFate, stagedGlobalTime, rejectedPayload] =
    await stageTransactionAndVersionsWithCurrentIndexes(
      node,
      batch,
      tx,
      versions,
      fate,
      globalTime,
      durability,
      true,
      false,
      stagedContentVersions,
    );
  if (rejectedPayload) stagedRejections.push(rejectedPayload);
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

const rowKey = (table: string, branchKey: any, row: Uuid) =>
  `${table}\0${BranchKey.key(branchKey)}\0${Array.from(row).join(",")}`;
const versionKey = (v: VersionRecord) =>
  `${rowKey(v.table(), v.branchKey(), v.rowUuid())}\0${Array.from(v.schemaVersion() as Uint8Array).join(",")}`;

function txIdentity(tx: Transaction): Transaction {
  return { ...transactionWithoutPermissionSubject(tx), nTotalWrites: 0 };
}

export async function ingestResetViewBundleRefsInBulk(
  node: NodeState,
  bundles: any[],
  preflightPersistedTxIds: SortedSet<TxId> | undefined,
): Promise<SortedSet<TxId>> {
  const bundlesByTx = new SortedMap<TxId, any[]>(cmpTxId);
  for (const bundle of bundles) {
    // Reset fast paths call this directly: do not rely on an outer ViewUpdate preflight.
    node.admitContributionMergeForStorage(bundle.tx);
    validateReceivedViewBundleGlobalTimeDurability(bundle.globalTime, bundle.durability);
    bundlesByTx.getOrInsert(bundle.tx.txId, () => []).push(bundle);
  }
  const eligible: [any[], Transaction, boolean][] = [];
  const loadedTxIds = new SortedSet<TxId>(cmpTxId);
  for (const [txId, txBundles] of bundlesByTx) {
    const first = txBundles[0];
    const viewScoped = first.scope === "ViewScoped";
    // Each view-scoped bundle declares only its own redacted cardinality: compare immutable identity here.
    const firstIdentity = txIdentity(first.tx);
    if (
      txBundles.some(
        (b) =>
          !transactionEquals(txIdentity(b.tx), firstIdentity) ||
          (b.scope === "ViewScoped") !== viewScoped ||
          b.fate.kind !== first.fate.kind ||
          b.globalTime !== first.globalTime ||
          b.durability !== first.durability,
      )
    )
      continue;
    if (first.fate.kind !== "Accepted" || first.globalTime === undefined) continue;
    if (first.tx.kind !== "Mergeable" && first.tx.kind !== "Exclusive") continue;
    const uniqueVersions = new Map<string, VersionRecord>();
    for (const bundle of txBundles)
      for (const version of bundle.versions) {
        const key = versionKey(version);
        const existing = uniqueVersions.get(key);
        if (existing && !versionRecordEquals(existing, version))
          throw nodeError("ConflictingCommitUnit", txId);
        if (!existing) uniqueVersions.set(key, version);
      }
    const versionCount = uniqueVersions.size;
    if (
      first.tx.kind === "Exclusive" &&
      !viewScoped &&
      Number(first.tx.nTotalWrites) !== versionCount
    )
      continue;
    if (
      preflightPersistedTxIds
        ? preflightPersistedTxIds.has(txId)
        : await node.queryTransaction(txId)
    )
      continue;
    if (loadedTxIds.add(txId)) {
      const localTx = transactionWithoutPermissionSubject(first.tx);
      if (viewScoped) localTx.nTotalWrites = versionCount;
      eligible.push([txBundles, localTx, viewScoped]);
    }
  }
  if (eligible.length === 0) return loadedTxIds;
  await prepareAuthoredSchemaVariantsForCommit(
    node,
    eligible.flatMap(([bs]) => bs.flatMap((b) => b.versions)),
  );

  const batch: DatabaseBatch = node.database.openBatch();
  node.syncMetrics.receiverBulkIngestCommits += 1;
  node.syncMetrics.receiverBulkBundleIngests += eligible.length;
  const versionCount = eligible.reduce(
    (n, [bs]) => n + bs.reduce((m: number, b: any) => m + b.versions.length, 0),
    0,
  );
  batch.reserve(eligible.length + versionCount * 2);
  const currentUpdates = new SortedMap<[string, string, Uuid], [VersionRow, bigint]>(
    cmpTuple(cmpStr, cmpStr, cmpBytes),
  );
  const appliedGlobalTimes: [bigint, TxId][] = [];

  for (const [txBundles, tx, viewScoped] of eligible) {
    const first = txBundles[0];
    // A reset is a clock observation just like incremental ingestion.
    node.mergeTxTime(tx.txId.time);
    const txNodeAlias: number = await node.ensureNodeAlias(tx.txId.node);
    const globalTime: bigint = first.globalTime;
    appliedGlobalTimes.push([globalTime, tx.txId]);
    const contributionMerge = contributionMergeStorageValue(node, tx.contributionMerge);
    // Preserve the view scope marker even when the redacted write count equals this fragment's length.
    batch.insert(
      "jazz_transactions",
      transactionValuesWithPartialFlag(
        txNodeAlias,
        tx,
        first.fate,
        first.globalTime,
        first.durability,
        viewScoped,
        contributionMerge,
      ),
    );
    const uniqueVersions = new Map<string, VersionRecord>();
    for (const bundle of txBundles)
      for (const version of bundle.versions)
        if (!uniqueVersions.has(versionKey(version)))
          uniqueVersions.set(versionKey(version), version);
    for (const version of [...uniqueVersions.values()].sort(cmpVersionRecord)) {
      const authorSchema = version.schemaVersion();
      node.tableInSchemaRef(version.table(), authorSchema);
      const schemaVersionAlias: number = await node.ensureSchemaVersionAlias(authorSchema);
      const authoredColumnIds = authoredColumnIdsForNames(
        node,
        authorSchema,
        version.table(),
        version.authoredColumns(),
      );
      const sourceTableSchema = node.tableInSchemaRef(version.table(), authorSchema);
      const stored = VersionRow.fromWire(
        sourceTableSchema,
        version,
        authoredColumnIds,
        txNodeAlias,
        schemaVersionAlias,
        tx.txId.time,
        globalTime,
      );
      const [historyTable, record] = versionStorageWriteBinding(node, stored, tx.madeBy);
      batch.insertRaw(historyTable, versionStoragePrimaryKey(node, stored), record);
      const key: [string, string, Uuid] = [
        stored.table,
        BranchKey.key(stored.branchKey),
        stored.rowUuid(),
      ];
      // Accepted rows are post-images at their seq: the newest seq is the row.
      const prior = currentUpdates.get(key);
      if (!prior || prior[1] < globalTime) currentUpdates.set(key, [stored, globalTime]);
    }
  }

  // Another subscription may already have supplied a newer accepted version: never rewind the current winner.
  const residentTables = new Map<string, boolean>();
  const winningUpdates: [VersionRow, bigint][] = [];
  for (const [stored, globalTime] of currentUpdates.valuesArray()) {
    const schema = node.schemaVersionForAlias(stored.schemaVersionAlias());
    if (!schema) throw nodeError("InvalidStoredValue", "unknown schema version alias");
    const table = physicalCurrentTableForSchema(node, schema, stored.table, "Global");
    let hasResidentRows = residentTables.get(table);
    if (hasResidentRows === undefined) {
      hasResidentRows = (await node.database.tableHasStoredRows(table)) as boolean;
      residentTables.set(table, hasResidentRows);
    }
    if (hasResidentRows) {
      const currentSeq = await globalCurrentSeqInBatch(
        node,
        batch,
        schema,
        stored.table,
        stored.branchKey,
        stored.rowUuid(),
      );
      // The authority's image replaces a locally merged image at the same seq; only a newer seq keeps the row.
      if (currentSeq !== undefined && currentSeq > globalTime) continue;
    }
    winningUpdates.push([stored, globalTime]);
  }
  for (const [stored, globalTime] of winningUpdates)
    writeGlobalCurrentUpdate(node, batch, stored, globalTime);
  // A new synced image rebases any pending local patches on that row.
  const settling = new SortedSet<TxId>(
    cmpTxId,
    winningUpdates.map(([stored]) => node.versionTxId(stored)),
  );
  await rebaseAheadOverlays(
    node,
    batch,
    winningUpdates.map(([stored]) => stored),
    settling,
  );
  const applied = await node.applyNodeBatch(batch);
  const persisted = await node.database.persistWithProgress(applied);
  node.database.finishPersistence(persisted);
  for (const txId of loadedTxIds.toArray()) node.invalidateTxVersionTablesCache(txId);
  for (const [globalTime, txId] of appliedGlobalTimes)
    node.recordAppliedGlobalTime(globalTime, txId);
  return loadedTxIds;
}

/** Whether two unit payloads address the same rows with the same deletion intent. */
function sameAddressedRows(stored: VersionRecord[], resent: VersionRecord[]): boolean {
  const rows = (records: VersionRecord[]) =>
    new Set(
      records.map((r) => `${rowKey(r.table(), r.branchKey(), r.rowUuid())}\0${r.deletesRow()}`),
    );
  const a = rows(stored);
  const b = rows(resent);
  return a.size === b.size && [...a].every((k) => b.has(k));
}
