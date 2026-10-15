// View-update construction for subscribers and sync peers, and receiver-side
// application of view updates. NodeState methods are free functions.
import { SortedMap, cmpStr } from "../../support/collections.js";
import {
  type NodeErrorLike,
  type NodeState,
  type SupportingRow,
  type SyncMessage,
  type TxId,
  type VersionBundle,
  type VersionRecord,
  type VersionRow,
  CoveredInputCoordinate,
  NodeErrors,
  SupportingRowsUpdate,
  ViewEvaluationContext,
  buildVersionCarriersFromSingletons,
  cmpTxId,
  cmpVersionRecord,
  coveredKey,
  fateEquals,
  isWireValid,
  knownTransactionPayloadMatches,
  readPolicyAdmitsEveryRow,
  recordMaintainedViewRemovalStreamBundle,
  recordMaintainedViewStreamBAddBundle,
  rowUuidKey,
  schemaVersionKey,
  supportingRowEquals,
  transactionEquals,
  transactionWithoutPermissionSubject,
  txIdKey,
  versionRecordEquals,
  authorityResultKeyString,
  validateReceivedViewBundleGlobalTimeDurability,
} from "../../_extern/node-views.js";
import {
  preloadTransactionMemo,
  queryTransaction,
  queryVersionByAliasInBranch,
  queryVersionByAliasWithStorageInSchema,
  versionStorageSources,
  queryVersionsForTx,
  queryVersionsForTxRowsByAlias,
} from "./currency.js";
import type { MaintainedSubscriptionView } from "./maintained_subscription_view.js";

type RowUuid = any;
const rk = (table: string, row: RowUuid) => `${table}\u0000${rowUuidKey(row)}`;
const rtk = (table: string, row: RowUuid, tx: TxId) => `${rk(table, row)}\u0000${txIdKey(tx)}`;

/** Ordered TxId-keyed map. */
const txMap = <V>() => new SortedMap<TxId, V>(cmpTxId);

function applyCoveredInputClosureAdmissionDelta(
  state: any,
  cleared: boolean,
  adds: SupportingRow[],
  removes: SupportingRow[],
): void {
  if (cleared) state.coveredInputVersions.clear();
  for (const input of removes)
    state.coveredInputVersions.delete(CoveredInputCoordinate.from(input));
  for (const input of adds)
    state.coveredInputVersions.set(CoveredInputCoordinate.from(input), input);
}

function maintainedViewTxVersionsContainWinner(
  txVersions: VersionRow[],
  winner: VersionRow,
): boolean {
  return txVersions.some(
    (c) => c.table() === winner.table() && rowUuidKey(c.rowUuid()) === rowUuidKey(winner.rowUuid()),
  );
}

function versionBundleRecordKey(v: VersionRecord): string {
  return [
    v.table(),
    v.branchKey().key(),
    rowUuidKey(v.rowUuid()),
    schemaVersionKey(v.schemaVersion()),
    v.deletion() !== undefined ? "1" : "0",
  ].join("\u0000");
}

const tooLarge = (n: number) => {
  if (n > 0xffffffff) throw NodeErrors.invalidStoredValue("view payload is too large");
  return n;
};

function keyedVersions(versions: VersionRecord[]): SortedMap<string, VersionRecord> {
  return new SortedMap(
    cmpStr,
    versions.map((v) => [versionBundleRecordKey(v), v] as [string, VersionRecord]),
  );
}

function mergeReceiverVersionBundleRef(bundles: SortedMap<TxId, VersionBundle>, bundle: any): void {
  const txId = bundle.tx.txId;
  const incoming = canonicalReceiverBundle(bundle);
  const existing = bundles.get(txId);
  if (!existing) {
    bundles.set(txId, incoming);
    return;
  }
  const conflict = () => NodeErrors.conflictingCommitUnit(txId);
  if (
    !transactionEquals({ ...existing.tx, nTotalWrites: 0 }, { ...incoming.tx, nTotalWrites: 0 }) ||
    !fateEquals(existing.fate, incoming.fate) ||
    existing.globalTime !== incoming.globalTime ||
    existing.durability !== incoming.durability
  )
    throw conflict();
  const seen = keyedVersions(existing.versions);
  for (const version of incoming.versions) {
    const key = versionBundleRecordKey(version);
    const prior = seen.get(key);
    if (prior === undefined) seen.set(key, version);
    else if (!versionRecordEquals(prior, version)) throw conflict();
  }
  const sameVersions = (a: VersionRecord[], b: VersionRecord[]) =>
    a.length === b.length && a.every((v, i) => versionRecordEquals(v, b[i]));
  switch (`${existing.scope}/${incoming.scope}`) {
    case "CompleteTransaction/CompleteTransaction":
      if (
        existing.tx.nTotalWrites !== incoming.tx.nTotalWrites ||
        !sameVersions(existing.versions, incoming.versions)
      )
        throw conflict();
      break;
    case "CompleteTransaction/ViewScoped":
      validateVersionSubset(incoming.versions, existing.versions, txId);
      break;
    case "ViewScoped/CompleteTransaction":
      validateVersionSubset(existing.versions, incoming.versions, txId);
      bundles.set(txId, incoming);
      break;
    default:
      existing.versions = [...seen.valuesArray()];
      existing.tx.nTotalWrites = tooLarge(existing.versions.length);
  }
}

function canonicalReceiverBundle(bundle: any): VersionBundle {
  const versions = new SortedMap<string, VersionRecord>(cmpStr);
  for (const version of bundle.versions) {
    const key = versionBundleRecordKey(version);
    const existing = versions.get(key);
    if (existing === undefined) versions.set(key, version);
    else if (!versionRecordEquals(existing, version))
      throw NodeErrors.conflictingCommitUnit(bundle.tx.txId);
  }
  if (bundle.tx.nTotalWrites !== versions.size)
    throw NodeErrors.conflictingCommitUnit(bundle.tx.txId);
  const owned = bundle.toOwnedBundle();
  owned.versions = [...versions.valuesArray()];
  owned.tx = transactionWithoutPermissionSubject(owned.tx);
  return owned;
}

function validateVersionSubset(
  subset: VersionRecord[],
  complete: VersionRecord[],
  txId: TxId,
): void {
  const byKey = keyedVersions(complete);
  for (const version of subset) {
    const c = byKey.get(versionBundleRecordKey(version));
    if (c === undefined || !versionRecordEquals(c, version))
      throw NodeErrors.conflictingCommitUnit(txId);
  }
}

function versionBundleRefsForCarriers(carriers: any[]): any[] {
  const refs: any[] = [];
  for (const carrier of carriers) {
    try {
      refs.push(...carrier.bundleRefs());
    } catch {
      throw NodeErrors.malformedViewUpdate("malformed version-bundle run");
    }
  }
  return refs;
}

/** Convert a structurally malformed authority payload into the source-closure diagnostic. */
function invalidAuthoritySourceClosureError(subscription: any, error: unknown): unknown {
  const e = error as NodeErrorLike;
  if (e?.kind === "MalformedViewUpdate")
    return NodeErrors.invalidAuthoritySourceClosure(
      subscription,
      `authority source-closure payload failed validation: ${e.reason}`,
    );
  return error;
}
async function wrapClosure<T>(subscription: any, f: () => T | Promise<T>): Promise<T> {
  try {
    return await f();
  } catch (error) {
    throw invalidAuthoritySourceClosureError(subscription, error);
  }
}

export function supportingRowDiagnostic(row: SupportingRow): string {
  return `${row.versionTable} ${row.row} ${JSON.stringify(row.version.layer)} tx ${JSON.stringify(row.version.tx)}`;
}

function contentRowMembersForBundle(members: any[], _context: string): [string, RowUuid, TxId][] {
  const out: [string, RowUuid, TxId][] = [];
  for (const member of members) {
    const row = member.asRow();
    if (row) out.push(row);
  }
  return out;
}

export function simpleScalarExitQuery(query: any): boolean {
  return query.filters.length > 0 && singleTableScalarQuery(query);
}

/** One unprojected table with only scalar filters. */
export function singleTableScalarQuery(query: any): boolean {
  const scalar = (op: any) =>
    !(op.kind === "Column" && (op.name.includes(".") || op.name.startsWith("$")));
  const predicate = (f: any): boolean => {
    switch (f.kind) {
      case "All":
      case "Any":
        return f.parts.every(predicate);
      case "Not":
        return predicate(f.part);
      case "Eq":
      case "Ne":
      case "Gt":
      case "Gte":
      case "Lt":
      case "Lte":
      case "Contains":
        return scalar(f.a) && scalar(f.b);
      case "In":
        return scalar(f.a) && f.values.every(scalar);
      case "IsNull":
        return scalar(f.a);
      default:
        return false;
    }
  };
  return (
    query.filters.every(predicate) &&
    query.joins.length === 0 &&
    query.flatJoin === undefined &&
    query.policyBranches.length === 0 &&
    query.reachable.length === 0 &&
    query.inherits.length === 0 &&
    query.includes.length === 0 &&
    query.arraySubqueries.length === 0 &&
    query.select === undefined &&
    query.aggregate === undefined &&
    query.relation === undefined &&
    query.limit === undefined &&
    query.offset === 0
  );
}

export interface MaintainedViewBundleInputs {
  supportingUpdate: any | undefined;
  shape: any;
  hasDefaultReadView: boolean;
  allowAuthoritativeScalarExitRefresh: boolean;
  subscription: any;
  settledThrough: number;
  peerCompleteTxPayloads: SortedMap<TxId, true>;
  knownState: any | undefined;
  leaveScanAfter: number | undefined;
  completeExclusivePayloads: boolean;
  previousResultSet: SortedMap<TxId, true>;
  resultMemberAdds: any[];
  resultMemberRemoves: any[];
  identity: any;
  tier: "Global" | "Local" | "None";
  maintainedFacts: MaintainedSubscriptionView;
  allowStorageWitnessFallback: boolean;
}

interface ViewBundlePreflight {
  bundles: SortedMap<TxId, VersionBundle>;
  persistedTxIds: SortedMap<TxId, true>;
}

function storageBackedMaintainedDeletionWinner(
  self: NodeState,
  table: string,
  rowUuid: RowUuid,
  tier: string,
  context: any,
) {
  return storageBackedCurrentImage(self, table, rowUuid, tier, context, true);
}

async function storageBackedCurrentImage(
  self: NodeState,
  table: string,
  rowUuid: RowUuid,
  tier: string,
  context: any,
  deletionOnly: boolean,
): Promise<VersionRow | undefined> {
  const tableId = self.physicalTableIdForSchema(self.catalogue.localSchemaVersionId, table);
  let txId: TxId | undefined;
  if (tier === "Global") txId = await self.visibleGlobalTxIdForPhysicalTableNow(tableId, rowUuid);
  else if (tier === "Local") txId = await self.localDeletionWinnerTxId(table, rowUuid);
  if (txId === undefined) return undefined;
  const storedTx = await self.queryTransactionMemo(txId, context);
  if (!storedTx) throw NodeErrors.missingTransaction(txId);
  const versions = await queryVersionsForTxRowsByAlias(self, txId, storedTx.nodeAlias, [
    [table, rowUuid],
  ]);
  return versions.find((v) => !deletionOnly || v.deletion() !== undefined);
}

async function preflightViewBundleConflicts(
  self: NodeState,
  bundles: any[],
): Promise<ViewBundlePreflight> {
  const merged = txMap<VersionBundle>();
  for (const bundle of bundles) mergeReceiverVersionBundleRef(merged, bundle);
  const persistedTxIds = txMap<true>();
  for (const [txId, bundle] of merged) {
    const stored = await queryTransaction(self, txId);
    if (!stored) continue;
    persistedTxIds.set(txId, true);
    const storedIdentity = transactionWithoutPermissionSubject({ ...stored.tx, nTotalWrites: 0 });
    const incomingIdentity = { ...bundle.tx, nTotalWrites: 0 };
    if (!knownTransactionPayloadMatches(storedIdentity, incomingIdentity))
      throw NodeErrors.conflictingCommitUnit(txId);
    const storedByKey = new SortedMap<string, VersionRecord>(cmpStr);
    for (const sv of await queryVersionsForTx(self, txId)) {
      const version = self.versionRecordFromRow(sv);
      storedByKey.set(versionBundleRecordKey(version), version);
    }
    const incomingByKey = keyedVersions(bundle.versions);
    const carriesPostImages =
      bundle.fate.kind === "Accepted" &&
      bundle.globalTime !== undefined &&
      (txId.node.equals(self.nodeUuid) ||
        (self.clientRelayScope() !== undefined &&
          (stored.globalTime === undefined || stored.globalTime === bundle.globalTime)));
    for (const [key, incoming] of incomingByKey) {
      const existing = storedByKey.get(key);
      if (existing !== undefined && !versionRecordEquals(existing, incoming) && !carriesPostImages)
        throw NodeErrors.conflictingCommitUnit(txId);
    }
    const sk = storedByKey.keysArray();
    const ik = incomingByKey.keysArray();
    const conflict = () => NodeErrors.conflictingCommitUnit(txId);
    if (!stored.isPartial && bundle.scope === "CompleteTransaction") {
      if (
        stored.tx.nTotalWrites !== bundle.tx.nTotalWrites ||
        sk.length !== ik.length ||
        sk.some((k, i) => k !== ik[i])
      )
        throw conflict();
    } else if (!stored.isPartial) {
      if (ik.some((k) => !storedByKey.has(k))) throw conflict();
    } else if (bundle.scope === "CompleteTransaction") {
      if (sk.some((k) => !incomingByKey.has(k))) throw conflict();
    } else {
      for (const [key, version] of storedByKey)
        if (!incomingByKey.has(key)) bundle.versions.push(version);
      bundle.versions.sort(cmpVersionRecord);
      bundle.tx.nTotalWrites = tooLarge(bundle.versions.length);
    }
  }
  return { bundles: merged, persistedTxIds };
}

/** Subscribe to the raw history storage table. */
export async function subscribeHistory(self: NodeState, table: string) {
  self.table(table);
  const source = self.physicalHistorySourceGraph(self.catalogue.localSchemaVersionId, table);
  try {
    return await self.database.subscribeOneSink(source);
  } catch (e) {
    throw NodeErrors.groove(e);
  }
}

export function scopePhysicalTable(self: NodeState, schema: any, table: string) {
  const t = self.catalogue.physicalMappings.get(schema)?.identities.tables.get(table);
  if (!t) throw NodeErrors.invalidStoredValue("scope physical table mapping missing");
  return t.id;
}

export function scopeLogicalTable(self: NodeState, schema: any, physical: any): string {
  const mapping = self.catalogue.physicalMappings.get(schema);
  if (mapping)
    for (const [name, t] of mapping.identities.tables) if (t.id.equals(physical)) return name;
  throw NodeErrors.invalidStoredValue("scope physical table is absent from read schema");
}

/** Preserve the physical frontier from wire to receiver. */
function normalizeSupportingUpdate(
  self: NodeState,
  update: any,
  revisions: Map<string, Uint8Array>,
): Uint8Array | undefined {
  let wire = update.wireRows;
  if (!wire) return undefined;
  const invalid = () =>
    NodeErrors.invalidAuthoritySourceClosure(
      update.subscription,
      "invalid supporting-set revision or predecessor",
    );
  if (update.openingPending) {
    if (!wire.isSnapshot() || wire.addedRows().length !== 0) throw invalid();
    update.resetInputSet = false;
    return undefined;
  }
  let key: any;
  try {
    key = self.authorityResultKeyForSubscription(update.subscription);
  } catch (error) {
    const e = error as NodeErrorLike;
    if (
      e.kind === "InvalidStoredValue" &&
      (e.message === "subscription referenced unregistered shape" ||
        e.message === "subscription referenced unregistered binding")
    )
      return undefined;
    throw error;
  }
  const keyStr = authorityResultKeyString(key);
  const revision: Uint8Array = wire.revision();
  if (revision.every((b) => b === 0)) throw invalid();
  const sameRev = (a: Uint8Array | undefined, b: Uint8Array | undefined) =>
    a !== undefined && b !== undefined && a.every((x, i) => x === b[i]);
  if (wire.kind === "CatchUp") {
    const state = self.query.authorityResults.get(key);
    if (!state || revisions.has(keyStr) || !sameRev(state.supportingRevision, wire.predecessor))
      throw invalid();
    const moved = new Set([...wire.changed, ...wire.left].map(coveredKey));
    const rows: SupportingRow[] = [];
    for (const [coord, row] of state.coveredInputVersions)
      if (!moved.has(CoveredInputCoordinate.key(coord))) rows.push(row);
    rows.push(...wire.changed);
    update.wireRows = wire = { kind: "Snapshot", revision: wire.revision, rows };
  }
  if (wire.kind === "Delta") {
    const current =
      revisions.get(keyStr) ?? self.query.authorityResults.get(key)?.supportingRevision;
    if (
      !sameRev(current, wire.predecessor) ||
      (sameRev(wire.predecessor, revision) &&
        (wire.addedRows().length > 0 || wire.removedRows().length > 0))
    )
      throw invalid();
  }
  update.resetInputSet = wire.isSnapshot();
  const state = self.query.authorityResults.get(key);
  if (
    wire.isSnapshot() &&
    !revisions.has(keyStr) &&
    state &&
    state.liveSettled &&
    state.pendingAuthoritativeReset === undefined &&
    state.sourceClosure.kind === "Claimed" &&
    state.supportingRevision !== undefined
  ) {
    const incoming = new Map<string, SupportingRow>();
    for (const row of wire.addedRows()) {
      const k = coveredKey(row);
      if (!isWireValid(row) || incoming.has(k))
        throw NodeErrors.invalidAuthoritySourceClosure(
          update.subscription,
          "invalid or duplicate supporting physical row coordinate",
        );
      incoming.set(k, row);
    }
    const prior = new Map<string, SupportingRow>();
    for (const [coord, row] of state.coveredInputVersions)
      prior.set(CoveredInputCoordinate.key(coord), row);
    const adds = [...incoming]
      .filter(([k, r]) => !prior.has(k) || !supportingRowEquals(prior.get(k)!, r))
      .map((e) => e[1]);
    const removes = [...prior]
      .filter(([k, r]) => !incoming.has(k) || !supportingRowEquals(incoming.get(k)!, r))
      .map((e) => e[1]);
    update.wireRows = {
      kind: "Delta",
      predecessor: state.supportingRevision,
      revision,
      adds,
      removes,
    };
    update.resetInputSet = false;
  }
  revisions.set(keyStr, revision);
  return revision;
}

/** Assemble the wire view update for a maintained subscription's member transitions. */
export async function viewUpdateForMaintainedResultMembers(
  self: NodeState,
  inputs: MaintainedViewBundleInputs,
): Promise<SyncMessage> {
  const {
    shape,
    hasDefaultReadView,
    allowAuthoritativeScalarExitRefresh,
    subscription,
    settledThrough,
    peerCompleteTxPayloads,
    knownState,
    leaveScanAfter,
    completeExclusivePayloads,
    resultMemberAdds,
    resultMemberRemoves,
    identity,
    tier,
    maintainedFacts,
  } = inputs;
  const allowStorageWitnessFallback =
    inputs.allowStorageWitnessFallback || maintainedFacts.usesStorageBackedResultMaterialization();
  const supportingUpdate =
    inputs.supportingUpdate ?? SupportingRowsUpdate.snapshot([...maintainedFacts.supportingRows()]);
  const logicalTables = new Map<string, string>();
  for (const [name, id] of maintainedFacts.physicalTables) logicalTables.set(String(id), name);
  const tableOf = (row: SupportingRow) => logicalTables.get(String(row.physicalTable));
  const context = ViewEvaluationContext.default_();
  const rowResultAdds = contentRowMembersForBundle(
    resultMemberAdds,
    "real row result member is missing content transaction for bundle shipping",
  );
  const rowResultRemoves = contentRowMembersForBundle(
    resultMemberRemoves,
    "real row result member removal is missing content transaction for replacement shipping",
  );
  const txVersionsCache = txMap<VersionRow[]>();
  const cachedVersions = (txId: TxId) =>
    txVersionsCache.getOrInsert(txId, () => maintainedFacts.versionsByTx(txId));
  const knownStatePosition: number | undefined = knownState?.position;
  const skippedKnownStateRows = new Set<string>();
  for (const member of resultMemberAdds) {
    const row = member.asRealRow();
    if (
      row?.settlePosition !== undefined &&
      knownStatePosition !== undefined &&
      row.settlePosition <= knownStatePosition
    )
      skippedKnownStateRows.add(rk(String(row.table), row.rowUuid));
  }
  const wantedAddRowsByTx = txMap<Map<string, [string, RowUuid]>>();
  const want = (txId: TxId, table: string, row: RowUuid) =>
    wantedAddRowsByTx.getOrInsert(txId, () => new Map()).set(rk(table, row), [table, row]);
  for (const row of supportingUpdate.addedRows()) {
    const table = tableOf(row);
    if (table === undefined)
      throw NodeErrors.invalidStoredValue("support body has no compiled physical table");
    want(row.version.tx, table, row.row);
  }
  const resettledRows = new Set(rowResultRemoves.map(([t, r, tx]) => rtk(t, r, tx)));
  for (const [table, row, tx] of rowResultAdds)
    if (resettledRows.has(rtk(table, row, tx))) want(tx, table, row);

  const exitCandidates = new Map<string, [string, RowUuid, TxId]>();
  for (const [t, r, tx] of rowResultRemoves) exitCandidates.set(rtk(t, r, tx), [t, r, tx]);
  for (const row of supportingUpdate.removedRows()) {
    const table = tableOf(row);
    if (table !== undefined && table === shape.query().table)
      exitCandidates.set(rtk(table, row.row, row.version.tx), [table, row.row, row.version.tx]);
  }
  if (
    self.authoritativeScalarExitRefresh &&
    self.clientRelayScope() === undefined &&
    allowAuthoritativeScalarExitRefresh &&
    exitCandidates.size > 0 &&
    hasDefaultReadView &&
    shape.schemaVersion() === self.catalogue.localSchemaVersionId &&
    simpleScalarExitQuery(shape.query())
  ) {
    const [readShape, readBinding] = wholeTableShapeBinding(self, shape.query().table);
    const addedRows = new Set(rowResultAdds.map(([t, r]) => rk(t, r)));
    for (const [table, rowUuid, oldTx] of exitCandidates.values()) {
      if (
        table !== shape.query().table ||
        maintainedFacts.replacementFor(table, rowUuid)[0] !== undefined ||
        addedRows.has(rk(table, rowUuid))
      )
        continue;
      const rows = await self.queryRowsForLinkPhysicalRow(
        readShape,
        readBinding,
        tier,
        identity,
        rowUuid,
      );
      const row = rows.find((r: any) => rowUuidKey(r.rowUuid()) === rowUuidKey(rowUuid));
      if (!row) continue;
      const txId = await self.currentRowTxId(row);
      if (txId === undefined || cmpTxId(txId, oldTx) === 0) continue;
      const storedTx = await self.queryTransactionMemo(txId, context);
      if (!storedTx) throw NodeErrors.missingTransaction(txId);
      const versions = await queryVersionsForTxRowsByAlias(self, txId, storedTx.nodeAlias, [
        [table, rowUuid],
      ]);
      cachedVersions(txId).push(...versions.filter((v) => !v.isDeleted()));
      want(txId, table, rowUuid);
    }
  }
  await preloadTransactionMemo(self, wantedAddRowsByTx.keysArray(), context);

  const versionBundles: VersionBundle[] = [];
  const peerPayloadInventoryRefs: TxId[] = [];
  const emittedVersions = txMap<true>();
  const resultAddDeletionWinners = new Map<string, VersionRow | undefined>();
  const resultAddCoordinates = new Set(rowResultAdds.map(([t, r, tx]) => rtk(t, r, tx)));
  const memo = async (txId: TxId) => {
    const tx = await self.queryTransactionMemo(txId, context);
    if (!tx) throw NodeErrors.missingTransaction(txId);
    return tx;
  };
  const deletionWinnerFor = async (table: string, rowUuid: RowUuid) => {
    const retained = maintainedFacts.replacementFor(table, rowUuid)[1];
    if (retained) return retained;
    if (allowStorageWitnessFallback)
      return storageBackedMaintainedDeletionWinner(self, table, rowUuid, tier, context);
    return undefined;
  };
  const versionKey = (v: VersionRow) => rk(v.table(), v.rowUuid());

  for (const [txId, wantedMap] of wantedAddRowsByTx) {
    const wantedRows = [...wantedMap.values()];
    if (knownStatePosition !== undefined) {
      const gt = (await self.queryTransactionMemo(txId, context))?.globalTime;
      if (gt !== undefined && gt <= knownStatePosition) continue;
    }
    if (peerCompleteTxPayloads.has(txId)) {
      peerPayloadInventoryRefs.push(txId);
      continue;
    }
    if (emittedVersions.has(txId)) continue;
    emittedVersions.set(txId, true);
    let txVersions = cachedVersions(txId);
    const contentCoordinates = new Set(txVersions.filter((v) => !v.isDeleted()).map(versionKey));
    let needsStorageFallback = false;
    for (const [table, rowUuid] of wantedRows) {
      const coordinate = rk(table, rowUuid);
      if (contentCoordinates.has(coordinate)) continue;
      const contentWinner = maintainedFacts.replacementFor(table, rowUuid)[0];
      if (contentWinner && cmpTxId(self.versionTxId(contentWinner), txId) === 0) {
        if (!contentWinner.isDeleted()) contentCoordinates.add(versionKey(contentWinner));
        txVersions.push(contentWinner);
      }
      if (!contentCoordinates.has(coordinate)) needsStorageFallback = true;
    }
    if (needsStorageFallback && allowStorageWitnessFallback) {
      const storedTx = await memo(txId);
      txVersionsCache.set(
        txId,
        completeExclusivePayloads && storedTx.tx.kind === "Exclusive"
          ? await queryVersionsForTx(self, txId)
          : await queryVersionsForTxRowsByAlias(self, txId, storedTx.nodeAlias, wantedRows),
      );
    }
    const sameTransactionDeletionWinners: VersionRow[] = [];
    for (const [table, rowUuid] of wantedRows) {
      if (!resultAddCoordinates.has(rtk(table, rowUuid, txId))) continue;
      const key = rk(table, rowUuid);
      let winner: VersionRow | undefined;
      if (resultAddDeletionWinners.has(key)) winner = resultAddDeletionWinners.get(key);
      else {
        winner = await deletionWinnerFor(table, rowUuid);
        resultAddDeletionWinners.set(key, winner);
      }
      if (winner && cmpTxId(self.versionTxId(winner), txId) === 0)
        sameTransactionDeletionWinners.push(winner);
    }
    txVersions = txVersionsCache.get(txId)!;
    for (const winner of sameTransactionDeletionWinners)
      if (!maintainedViewTxVersionsContainWinner(txVersions, winner)) txVersions.push(winner);
    if (txVersions.some((v) => wantedMap.has(versionKey(v)))) {
      const storedTx = await memo(txId);
      const exclusiveAll = completeExclusivePayloads && storedTx.tx.kind === "Exclusive";
      if (exclusiveAll && storedTx.tx.nTotalWrites !== txVersions.length) {
        txVersions = await queryVersionsForTx(self, txId);
        txVersionsCache.set(txId, txVersions);
      }
      const filtered = txVersions
        .filter((v) => exclusiveAll || wantedMap.has(versionKey(v)))
        .filter((v) => !skippedKnownStateRows.has(versionKey(v)));
      if (filtered.length === 0) continue;
      const bundle =
        maintainedFacts.usesStorageBackedResultMaterialization() &&
        needsStorageFallback &&
        exactStorageMaintainedVersionsAreUnambiguous(self, filtered)
          ? await versionBundleForMaintainedViewVersionsWithTxAndSource(
              self,
              storedTx,
              filtered,
              "ExactStorage",
            )
          : await versionBundleForMaintainedViewVersionsWithTx(self, storedTx, filtered);
      versionBundles.push(bundle);
      recordMaintainedViewStreamBAddBundle();
      continue;
    }
    throw NodeErrors.maintainedViewMissingBundleWitness(
      "add result row missing Stream B content witness",
    );
  }

  type Winner = [string, RowUuid, VersionRow, string];
  const replacementWinnersByTx = txMap<Winner[]>();
  for (const [table, rowUuid, contentTxId] of rowResultAdds) {
    const key = rk(table, rowUuid);
    const version = resultAddDeletionWinners.has(key)
      ? resultAddDeletionWinners.get(key)
      : await deletionWinnerFor(table, rowUuid);
    if (!version) continue;
    const txId = self.versionTxId(version);
    if (cmpTxId(txId, contentTxId) === 0 || emittedVersions.has(txId)) continue;
    replacementWinnersByTx
      .getOrInsert(txId, () => [])
      .push([table, rowUuid, version, "add result row missing deletion replacement witness"]);
  }
  const storageDeletionTxs = txMap<true>();
  const removedRowCandidates = new Map<
    string,
    { table: string; row: RowUuid; tx?: TxId; isMember: boolean }
  >();
  const addCandidate = (table: string, row: RowUuid, tx: TxId | undefined, isMember: boolean) => {
    const k = rk(table, row);
    const e = removedRowCandidates.get(k);
    if (e) e.isMember ||= isMember;
    else removedRowCandidates.set(k, { table, row, tx, isMember });
  };
  for (const [t, r, tx] of rowResultRemoves) addCandidate(t, r, tx, true);
  if (supportingUpdate.kind !== "CatchUp")
    for (const row of supportingUpdate.removedRows()) {
      const table = tableOf(row);
      if (table !== undefined) addCandidate(table, row.row, row.version.tx, false);
    }
  const joinedTables = [...new Set(logicalTables.values())].filter(
    (t) => t !== shape.query().table,
  );
  if (joinedTables.length > 0)
    for (const member of resultMemberRemoves) {
      const occurrence = member.asRealRow()?.occurrenceId;
      if (!occurrence) continue;
      for (const joined of occurrence.joinedSources())
        for (const table of joinedTables) addCandidate(table, joined.uuid(), undefined, false);
    }
  const supportingNow = new Set<string>();
  for (const row of supportingUpdate.addedRows()) {
    const table = tableOf(row);
    if (table !== undefined) supportingNow.add(rk(table, row.row));
  }
  const catchUpLeft = new Map<string, [string, RowUuid]>();
  const position = knownStatePosition ?? leaveScanAfter;
  if (
    (supportingUpdate.kind === "Snapshot" || supportingUpdate.kind === "CatchUp") &&
    position !== undefined
  ) {
    for (const table of new Set(logicalTables.values()))
      for (const rowUuid of await self.globalRowsChangedAfter(table, position)) {
        const key = rk(table, rowUuid);
        if (!supportingNow.has(key)) catchUpLeft.set(key, [table, rowUuid]);
        addCandidate(table, rowUuid, undefined, false);
      }
  }
  if (supportingUpdate.kind === "CatchUp")
    for (const row of supportingUpdate.left) {
      const table = tableOf(row);
      if (table !== undefined) catchUpLeft.set(rk(table, row.row), [table, row.row]);
    }
  for (const [key, [table, rowUuid]] of [...catchUpLeft]) {
    const policy = self.table(table).readPolicy;
    if (policy === undefined || readPolicyAdmitsEveryRow(policy)) continue;
    const [readShape, readBinding] = wholeTableShapeBinding(self, table);
    const rows = await self.queryRowsForLinkPhysicalRow(
      readShape,
      readBinding,
      tier,
      identity,
      rowUuid,
    );
    if (!rows.some((r: any) => rowUuidKey(r.rowUuid()) === rowUuidKey(rowUuid)))
      catchUpLeft.delete(key);
  }
  for (const [key, { table, row: rowUuid, tx: oldTx, isMember }] of [...removedRowCandidates].sort(
    (a, b) => cmpStr(a[0], b[0]),
  )) {
    const [content, retainedDeletion] = maintainedFacts.replacementFor(table, rowUuid);
    const contentWinner = isMember ? content : undefined;
    let deletionWinner = retainedDeletion;
    if (!deletionWinner) {
      const leaves = catchUpLeft.has(key);
      const w = await storageBackedCurrentImage(self, table, rowUuid, tier, context, !leaves);
      deletionWinner = w && (leaves || w.deletion() === "Deleted") ? w : undefined;
      if (deletionWinner) storageDeletionTxs.set(self.versionTxId(deletionWinner), true);
    }
    for (const [version, missing] of [
      [contentWinner, "removed result row missing content replacement witness"],
      [deletionWinner, "removed result row missing deletion replacement witness"],
    ] as const) {
      if (!version) continue;
      const txId = self.versionTxId(version);
      if ((oldTx !== undefined && cmpTxId(txId, oldTx) === 0) || emittedVersions.has(txId))
        continue;
      replacementWinnersByTx.getOrInsert(txId, () => []).push([table, rowUuid, version, missing]);
    }
  }
  for (const [txId, winners] of replacementWinnersByTx) {
    if (emittedVersions.has(txId)) continue;
    if (peerCompleteTxPayloads.has(txId)) {
      emittedVersions.set(txId, true);
      peerPayloadInventoryRefs.push(txId);
      recordMaintainedViewRemovalStreamBundle();
      continue;
    }
    const wantedRows = new Map(winners.map(([t, r]) => [rk(t, r), [t, r] as [string, RowUuid]]));
    const missingIn = (versions: VersionRow[]) =>
      winners.find(([, , w]) => !maintainedViewTxVersionsContainWinner(versions, w));
    const firstMissing = missingIn(cachedVersions(txId));
    if (firstMissing) {
      if (!allowStorageWitnessFallback && !storageDeletionTxs.has(txId))
        throw NodeErrors.maintainedViewMissingBundleWitness(firstMissing[3]);
      const storedTx = await memo(txId);
      txVersionsCache.set(
        txId,
        completeExclusivePayloads && storedTx.tx.kind === "Exclusive"
          ? await queryVersionsForTx(self, txId)
          : await queryVersionsForTxRowsByAlias(self, txId, storedTx.nodeAlias, [
              ...wantedRows.values(),
            ]),
      );
    }
    const storedTx = await memo(txId);
    const exclusiveAll = completeExclusivePayloads && storedTx.tx.kind === "Exclusive";
    if (exclusiveAll && storedTx.tx.nTotalWrites !== txVersionsCache.get(txId)?.length)
      txVersionsCache.set(txId, await queryVersionsForTx(self, txId));
    const txVersions = txVersionsCache.get(txId)!;
    const stillMissing = missingIn(txVersions);
    if (stillMissing) throw NodeErrors.maintainedViewMissingBundleWitness(stillMissing[3]);
    emittedVersions.set(txId, true);
    const bundled = exclusiveAll
      ? txVersions.slice()
      : txVersions.filter((v) => wantedRows.has(versionKey(v)));
    versionBundles.push(
      await versionBundleForMaintainedViewVersionsWithTx(self, storedTx, bundled),
    );
    recordMaintainedViewRemovalStreamBundle();
  }
  for (const bundle of versionBundles) {
    if (bundle.tx.kind !== "Exclusive" || completeExclusivePayloads) continue;
    const wanted = wantedAddRowsByTx.get(bundle.tx.txId);
    if (!wanted) continue;
    const physKey = (tableId: any, row: RowUuid) => `${String(tableId)}\u0000${rowUuidKey(row)}`;
    const wantedPhysical = new Set(
      [...wanted.values()].map(([t, r]) =>
        physKey(self.physicalTableIdForSchema(shape.schemaVersion(), t), r),
      ),
    );
    bundle.versions = bundle.versions.filter(
      (v: VersionRecord) =>
        v.deletion() !== undefined ||
        wantedPhysical.has(
          physKey(self.physicalTableIdForSchema(v.schemaVersion(), v.table()), v.rowUuid()),
        ),
    );
    bundle.tx.nTotalWrites = tooLarge(bundle.versions.length);
    bundle.scope = "ViewScoped";
  }
  let versionCarriers: any[];
  try {
    versionCarriers = buildVersionCarriersFromSingletons(versionBundles);
  } catch {
    throw NodeErrors.invalidStoredValue("failed to build version-bundle run");
  }
  return {
    kind: "ViewUpdate",
    payload: {
      subscription,
      settledThrough,
      versionCarriers,
      peerPayloadInventory: {
        completeTxPayloads: peerPayloadInventoryRefs,
        authorizationProgress: undefined,
        openingPending: false,
      },
      supportingRows: supportingUpdate,
    },
  };
}

/** Apply a downstream current-row view update. */
export async function applyViewUpdate(self: NodeState, update: any): Promise<void> {
  const snapshot = normalizeSupportingUpdate(self, update, new Map());
  const sub = update.subscription;
  await wrapClosure(sub, () => validateReceivedViewUpdateGlobalTimeDurability(update));
  await wrapClosure(sub, () => validateViewUpdatePayloads(self, [update]));
  const caches = await wrapClosure(sub, () => validateCoveredInputClosureAdmission(self, [update]));
  const refs = await wrapClosure(sub, () => versionBundleRefsForCarriers(update.versionCarriers));
  const preflight = await wrapClosure(sub, () => preflightViewBundleConflicts(self, refs));
  await wrapClosure(sub, () =>
    validateCoveredInputBodyWitnesses(self, [update], preflight.bundles),
  );
  await applyViewUpdateInner(self, update, undefined, snapshot);
  installCompiledCoveredInputSourceCaches(self, caches);
}

export async function applyViewUpdatesInBatch(self: NodeState, updates: any[]): Promise<void> {
  const priorSnapshots = new Map<string, Uint8Array>();
  const snapshots = updates.map((u) => normalizeSupportingUpdate(self, u, priorSnapshots));
  if (updates.length === 0) return;
  for (const u of updates)
    await wrapClosure(u.subscription, () => validateReceivedViewUpdateGlobalTimeDurability(u));
  for (const u of updates)
    await wrapClosure(u.subscription, () => validateViewUpdatePayloads(self, [u]));
  const caches = validateCoveredInputClosureAdmission(self, updates);
  const allBundleRefs: any[] = [];
  const bulkCandidates: any[] = [];
  const initialHydration = new Set<string>();
  for (const [key, state] of self.query.authorityResults)
    if (state.initialHydration) initialHydration.add(authorityResultKeyString(key));
  for (const update of updates) {
    const refs = versionBundleRefsForCarriers(update.versionCarriers);
    allBundleRefs.push(...refs);
    let key: any;
    try {
      key = self.authorityResultKeyForSubscription(update.subscription);
    } catch {
      continue;
    }
    const ks = authorityResultKeyString(key);
    if (update.resetInputSet) initialHydration.add(ks);
    const inInitial = initialHydration.has(ks);
    if (
      update.resetInputSet &&
      update.peerCompleteTxPayloadRefs.length === 0 &&
      update.resultMemberRemoves.length === 0
    )
      bulkCandidates.push(...refs);
    if (
      inInitial &&
      refs.length === 0 &&
      (!update.resetInputSet || update.peerCompleteTxPayloadRefs.length === 0)
    )
      initialHydration.delete(ks);
  }
  const preflight = await preflightViewBundleConflicts(self, allBundleRefs);
  await validateCoveredInputBodyWitnesses(self, updates, preflight.bundles);
  const bulkTxIds = new Set(bulkCandidates.map((b) => txIdKey(b.tx.txId)));
  const bulkRefs = [...preflight.bundles]
    .filter(([txId, b]) => bulkTxIds.has(txIdKey(txId)) && b.scope === "CompleteTransaction")
    .map(([, b]) => b.asRef());
  const bulkLoaded: SortedMap<TxId, true> = await self.ingestResetViewBundleRefsInBulk(
    bulkRefs,
    preflight.persistedTxIds,
  );
  const receiverCandidates = preflight.bundles;
  if (updates.some((u) => u.resetInputSet)) await self.beginInitialSyncFlushCadence();
  for (const [txId] of bulkLoaded) receiverCandidates.delete(txId);
  for (const bundle of receiverCandidates.valuesArray()) {
    await self.ensureNodeAlias(bundle.tx.txId.node);
    for (const version of bundle.versions)
      await self.ensureSchemaVersionAlias(version.schemaVersion());
  }
  const batch = self.database.openBatch();
  const batchTxIds = txMap<true>();
  const batchGlobalTimes: [number, TxId][] = [];
  const batchContentVersions: VersionRow[] = [];
  const batchRejections: any[] = [];
  let batchBundleCount = 0;
  const deferred: VersionBundle[] = [];
  for (const bundle of receiverCandidates.valuesArray()) {
    if (
      await stageViewBundle(
        self,
        batch,
        bundle,
        batchTxIds,
        batchGlobalTimes,
        batchContentVersions,
        batchRejections,
      )
    )
      batchBundleCount++;
    else deferred.push(bundle);
  }
  if (!batch.isEmpty()) {
    self.syncMetrics.receiverBulkIngestCommits += 1;
    self.syncMetrics.receiverBulkBundleIngests += batchBundleCount;
    const applied = await self.applyNodeBatch(batch);
    const persisted = await self.database.persistWithProgress(applied);
    self.database.finishPersistence(persisted);
    for (const rejected of batchRejections)
      self.rejections.rejectedTransactions.set(rejected.txId(), rejected);
    for (const [txId] of batchTxIds) self.invalidateTxVersionTablesCache(txId);
    for (const [gt, txId] of batchGlobalTimes) self.recordAppliedGlobalTime(gt, txId);
  }
  const preloaded = bulkLoaded.clone();
  for (const [txId] of batchTxIds) preloaded.set(txId, true);
  for (const bundle of deferred) {
    self.syncMetrics.receiverPerBundleIngests += 1;
    await ingestViewBundle(self, bundle);
    preloaded.set(bundle.tx.txId, true);
  }
  for (let i = 0; i < updates.length; i++)
    await applyViewUpdateInner(self, updates[i], preloaded, snapshots[i]);
  installCompiledCoveredInputSourceCaches(self, caches);
  if (
    self.initialSyncFlushActive &&
    [...self.query.authorityResults.values()].every((s: any) => !s.initialHydration)
  )
    await self.finishInitialSyncFlushCadence();
}

/** Keep a discarded Pending carrier's transaction identity for its fate observer. */
export async function rememberDiscardedPendingViewTransactions(
  self: NodeState,
  carriers: any[],
): Promise<void> {
  const headers = txMap<any>();
  for (const carrier of carriers) {
    let refs: any[];
    try {
      refs = carrier.bundleRefs();
    } catch {
      throw NodeErrors.malformedViewUpdate("malformed version-bundle run");
    }
    for (const bundle of refs) {
      if (bundle.fate.kind !== "Pending") continue;
      const tx = { ...transactionWithoutPermissionSubject(bundle.tx), nTotalWrites: 0 };
      self.admitContributionMergeForStorage(tx);
      const previous = headers.get(tx.txId);
      if (previous && !knownTransactionPayloadMatches(previous, tx))
        throw NodeErrors.conflictingCommitUnit(tx.txId);
      headers.set(tx.txId, tx);
    }
  }
  const missing: any[] = [];
  for (const [txId, tx] of headers) {
    const stored = await queryTransaction(self, txId);
    if (stored) {
      const identity = { ...transactionWithoutPermissionSubject(stored.tx), nTotalWrites: 0 };
      if (!knownTransactionPayloadMatches(identity, tx))
        throw NodeErrors.conflictingCommitUnit(txId);
    } else missing.push(tx);
  }
  for (const tx of missing)
    await self.ingestTransactionFragmentWithoutCurrentIndexes(
      tx,
      [],
      { kind: "Pending" },
      undefined,
      "Local",
    );
}

/** Persist immutable bodies carried by a stale authority link. */
export async function ingestUnselectedAuthorityViewBundles(
  self: NodeState,
  carriers: any[],
): Promise<void> {
  const preflight = await preflightViewBundleConflicts(
    self,
    versionBundleRefsForCarriers(carriers),
  );
  for (const bundle of preflight.bundles.valuesArray()) await ingestViewBundle(self, bundle);
}

function validateViewUpdatePayloads(self: NodeState, updates: any[]): void {
  const descriptors = new Map();
  for (const update of updates) {
    const invalid = (t: string) => NodeErrors.invalidAuthoritySourceClosure(update.subscription, t);
    if (
      update.openingPending &&
      (update.supportingAdds().length > 0 ||
        update.supportingRemoves().length > 0 ||
        update.versionCarriers.length > 0 ||
        update.peerCompleteTxPayloadRefs.length > 0)
    )
      throw invalid("opening-pending marker carries source data");
    if (update.resultMemberAdds.length > 0 || update.resultMemberRemoves.length > 0)
      throw invalid("authority view update carries retired result members");
    const adds: SupportingRow[] = update.supportingAdds();
    const removes: SupportingRow[] = update.supportingRemoves();
    const key = (r: SupportingRow) => JSON.stringify(r);
    const added = new Set(adds.map(key));
    const removed = new Set(removes.map(key));
    if (
      added.size !== adds.length ||
      removed.size !== removes.length ||
      [...added].some((k) => removed.has(k)) ||
      [...adds, ...removes].some((r) => !isWireValid(r))
    )
      throw invalid("invalid or duplicate supporting physical row version");
    for (const bundle of versionBundleRefsForCarriers(update.versionCarriers)) {
      self.admitContributionMergeForStorage(bundle.tx);
      self.validateViewPayloadVersionsPrepared(bundle.versions, descriptors);
    }
  }
}

/** Only an explicit physical snapshot replaces a scope. */
const resetReplacesAuthoritySourceClosure = (
  reset: boolean,
  hasScope: boolean,
  openingPending: boolean,
) => reset && hasScope && !openingPending;

/** Stage only changed coordinates; no full predecessor copy for deltas. */
function validateCoveredInputClosureAdmission(
  self: NodeState,
  updates: any[],
): Map<string, [any, any]> {
  const receiverRole = self.clientRelayScope() !== undefined ? "client relay" : "client";
  const caches = new Map<string, [any, any]>();
  const overlays = new Map<
    string,
    { cleared: boolean; changes: Map<string, SupportingRow | undefined> }
  >();
  for (const update of updates) {
    let key: any;
    try {
      key = self.authorityResultKeyForSubscription(update.subscription);
    } catch {
      continue;
    }
    const ks = authorityResultKeyString(key);
    const state = self.query.authorityResults.get(key);
    let tables = state?.compiledCoveredInputSources;
    if (!tables) {
      if (!caches.has(ks))
        caches.set(ks, [key, self.compiledCoveredInputSourcesForSubscription(update.subscription)]);
      tables = caches.get(ks)![1];
    }
    let overlay = overlays.get(ks);
    if (!overlay) overlays.set(ks, (overlay = { cleared: false, changes: new Map() }));
    if (
      resetReplacesAuthoritySourceClosure(
        update.resetInputSet,
        update.wireRows !== undefined,
        update.openingPending,
      )
    ) {
      overlay.cleared = true;
      overlay.changes.clear();
    }
    const invalid = (m: string) => NodeErrors.invalidAuthoritySourceClosure(update.subscription, m);
    for (const [rows, adding] of [
      [update.supportingRemoves(), false],
      [update.supportingAdds(), true],
    ] as [SupportingRow[], boolean][]) {
      for (const row of rows) {
        if (!tables.has(row.physicalTable))
          throw invalid("supporting physical table is outside compiled scope");
        const coordinate = CoveredInputCoordinate.from(row);
        const ck = CoveredInputCoordinate.key(coordinate);
        let current: SupportingRow | undefined;
        if (overlay.changes.has(ck)) current = overlay.changes.get(ck);
        else if (!overlay.cleared) current = state?.coveredInputVersions.get(coordinate);
        if (adding) {
          if (current)
            throw invalid(
              `scope addition duplicates a retained physical coordinate (receiver ${receiverRole}: retained ${supportingRowDiagnostic(current)}, added ${supportingRowDiagnostic(row)})`,
            );
          overlay.changes.set(ck, row);
        } else {
          if (!current || !supportingRowEquals(current, row))
            throw invalid(
              `scope removal is absent from exact predecessor (receiver ${receiverRole}: retained ${current ? supportingRowDiagnostic(current) : "none"}, removed ${supportingRowDiagnostic(row)})`,
            );
          overlay.changes.set(ck, undefined);
        }
      }
    }
  }
  return caches;
}

function installCompiledCoveredInputSourceCaches(
  self: NodeState,
  caches: Map<string, [any, any]>,
): void {
  for (const [key, compiled] of caches.values())
    self.authorityResultEntry(key).compiledCoveredInputSources = compiled;
}

async function validateCoveredInputBodyWitnesses(
  self: NodeState,
  updates: any[],
  staged: SortedMap<TxId, VersionBundle>,
): Promise<void> {
  const coord = (tx: TxId, table: string, row: RowUuid, branch: Uint8Array) =>
    `${txIdKey(tx)}\u0000${rk(table, row)}\u0000${branch.join(",")}`;
  const referenced = txMap<true>();
  for (const u of updates) for (const tx of u.peerCompleteTxPayloadRefs) referenced.set(tx, true);
  const admitted = new Set<string>();
  for (const bundle of staged.valuesArray())
    for (const v of bundle.versions)
      admitted.add(coord(bundle.tx.txId, v.table(), v.rowUuid(), v.branchKey().canonicalBytes()));
  for (const [txId] of referenced)
    for (const v of await queryVersionsForTx(self, txId))
      admitted.add(coord(txId, v.table(), v.rowUuid(), v.branchKey().canonicalBytes()));
  for (const update of updates)
    for (const input of update.supportingAdds() as SupportingRow[]) {
      const staged = admitted.has(
        coord(
          input.version.tx,
          String(input.versionTable),
          input.row,
          input.version.branchOrPrefix ?? new Uint8Array(),
        ),
      );
      let resident = false;
      if (!staged) {
        const shape = self.query.registeredShapes.get(update.subscription.shapeId);
        if (!shape) throw NodeErrors.invalidStoredValue("covered input shape is not registered");
        resident = await coveredInputHasResidentBody(self, input, shape.schemaVersion());
      }
      if (
        input.version.batch === undefined ||
        cmpTxId(input.version.batch, input.version.tx) !== 0 ||
        (!staged && !resident)
      )
        throw NodeErrors.invalidAuthoritySourceClosure(
          update.subscription,
          "covered input is not witnessed by admitted payload",
        );
    }
}

async function coveredInputHasResidentBody(
  self: NodeState,
  input: SupportingRow,
  readSchema: any,
): Promise<boolean> {
  if (!(await queryTransaction(self, input.version.tx))) return false;
  return (await coveredInputVersion(self, input, readSchema)) !== undefined;
}

/** Resolve an exact immutable witness via the receiver source's physical table. */
export async function coveredInputVersion(
  self: NodeState,
  input: SupportingRow,
  readSchema: any,
): Promise<VersionRow | undefined> {
  const alias: number | undefined = self.nodeAliases.get(input.version.tx.node);
  if (alias === undefined) return undefined;
  if (input.version.layer === "ContentOrDeletion")
    throw NodeErrors.invalidStoredValue("covered input must name one concrete version layer");
  let branchKey: any;
  try {
    branchKey = input.version.branchOrPrefix
      ? self.branchKeyFromCanonicalBytes(input.version.branchOrPrefix)
      : self.defaultBranchKey();
  } catch {
    throw NodeErrors.invalidStoredValue("covered input branch witness is malformed");
  }
  const version = await queryVersionByAliasInBranch(
    self,
    readSchema,
    scopeLogicalTable(self, readSchema, input.physicalTable),
    branchKey,
    input.row,
    input.version.tx.time,
    alias,
  );
  if (!version) return undefined;
  if (version.table() !== String(input.versionTable))
    throw NodeErrors.invalidStoredValue(
      "authority covered input authored table disagrees with stored version",
    );
  const expected: Uint8Array = input.version.branchOrPrefix ?? new Uint8Array();
  const actual: Uint8Array = version.branchKey().canonicalBytes();
  if (actual.length !== expected.length || actual.some((b, i) => b !== expected[i]))
    throw NodeErrors.invalidStoredValue(
      "authority covered input branch witness disagrees with stored version",
    );
  return version;
}

async function applyViewUpdateInner(
  self: NodeState,
  update: any,
  preloadedTxIds: SortedMap<TxId, true> | undefined,
  normalizedSnapshot: Uint8Array | undefined,
): Promise<void> {
  const {
    wireRows,
    subscription,
    settledThrough,
    deferSettlement,
    resetInputSet,
    versionCarriers,
    peerCompleteTxPayloadRefs,
    authorizationProgress,
    openingPending,
  } = update;
  const hasScope = wireRows !== undefined;
  let scopeAdds: SupportingRow[] = [];
  let scopeRemoves: SupportingRow[] = [];
  if (wireRows?.kind === "Snapshot") scopeAdds = wireRows.rows;
  else if (wireRows?.kind === "Delta")
    [scopeAdds, scopeRemoves] = [wireRows.adds, wireRows.removes];
  else if (wireRows?.kind === "CatchUp")
    throw NodeErrors.invalidAuthoritySourceClosure(
      subscription,
      "catch-up without an installed predecessor",
    );
  const refs = versionBundleRefsForCarriers(versionCarriers);
  try {
    self.bindingViewKeyForSubscription(subscription);
  } catch (error) {
    const e = error as NodeErrorLike;
    if (
      e.kind === "InvalidStoredValue" &&
      (e.message === "subscription referenced unregistered shape" ||
        e.message === "subscription referenced unregistered binding")
    ) {
      self.syncMetrics.droppedDetachedSubscriptionMessages += 1;
      return;
    }
    throw error;
  }
  const key = self.authorityResultKeyForSubscription(subscription);
  const entry = () => self.authorityResultEntry(key);
  const existing = self.query.authorityResults.get(key);
  if (existing) existing.supportingRevision = undefined;
  const resetCleared = resetReplacesAuthoritySourceClosure(resetInputSet, hasScope, openingPending);
  const pendingResetGeneration = resetCleared
    ? self.allocateAuthoritativeResetGeneration()
    : undefined;
  const preflight = preloadedTxIds ? undefined : await preflightViewBundleConflicts(self, refs);
  let bulkLoaded: SortedMap<TxId, true>;
  if (preloadedTxIds) bulkLoaded = preloadedTxIds.clone();
  else if (resetInputSet && peerCompleteTxPayloadRefs.length === 0) {
    const complete = preflight!.bundles
      .valuesArray()
      .filter((b: any) => b.scope === "CompleteTransaction");
    bulkLoaded = await self.ingestResetViewBundleRefsInBulk(
      complete.map((b: any) => b.asRef()),
      preflight!.persistedTxIds,
    );
  } else bulkLoaded = txMap();
  if (resetInputSet) entry().initialHydration = true;
  entry().deferredPublication = deferSettlement;
  if (deferSettlement) entry().liveSettled = false;
  if (preflight)
    for (const bundle of preflight.bundles.valuesArray()) {
      if (bulkLoaded.has(bundle.tx.txId)) continue;
      self.syncMetrics.receiverPerBundleIngests += 1;
      await ingestViewBundle(self, bundle);
    }
  for (const txId of peerCompleteTxPayloadRefs)
    if (!bulkLoaded.has(txId) && !(await queryTransaction(self, txId)))
      self.recordPeerPayloadInventoryMissingFallback();
  if (resetCleared) self.clearSettledResultView(key);
  applyCoveredInputClosureAdmissionDelta(entry(), resetCleared, scopeAdds, scopeRemoves);
  if (!deferSettlement && !openingPending) {
    const state = entry();
    state.liveSettled = true;
    state.settledThrough = settledThrough !== 0 ? settledThrough : undefined;
    if (pendingResetGeneration !== undefined)
      state.pendingAuthoritativeReset = pendingResetGeneration;
  }
  if (entry().initialHydration && refs.length === 0 && !deferSettlement && !openingPending)
    entry().initialHydration = false;
  if (authorizationProgress !== undefined) entry().authorizationProgress = authorizationProgress;
  entry().pendingOpening = openingPending;
  const state = entry();
  state.appliedViewUpdateGeneration = (state.appliedViewUpdateGeneration + 1) >>> 0;
  if (resetInputSet && !deferSettlement && !openingPending) {
    state.sourceClosure = { kind: "Claimed", generation: state.appliedViewUpdateGeneration };
    state.sourceIncrementals = [];
  } else if (
    !deferSettlement &&
    !openingPending &&
    (scopeAdds.length > 0 || scopeRemoves.length > 0)
  ) {
    if (state.sourceClosure.kind !== "Claimed")
      throw NodeErrors.invalidStoredValue(
        "incremental source closure arrived before a reset closure",
      );
    const generation = state.appliedViewUpdateGeneration;
    state.sourceIncrementals.push({
      predecessorGeneration: state.sourceClosure.generation,
      generation,
      adds: scopeAdds,
      removes: scopeRemoves,
    });
    state.sourceClosure = { kind: "Claimed", generation };
  }
  if (self.debugEnv.coveredInputTrace())
    console.error(
      `JAZZ_COVERED_INPUT_TRACE stage=view_update_applied node=${self.nodeUuid} reset=${resetInputSet} deferred=${deferSettlement} generation=${state.appliedViewUpdateGeneration} facts=${state.coveredInputVersions.size} closure=${JSON.stringify(state.sourceClosure)}`,
    );
  if (!deferSettlement && !openingPending) {
    const claimed = self.query.authorityResults.get(key);
    if (claimed && claimed.sourceClosure.kind === "Claimed") {
      const settled = claimed.settledThrough;
      if (settled !== undefined && normalizedSnapshot !== undefined) {
        const shape = self.registeredShape(subscription.shapeId);
        if (shape && self.watermarkCatchUpView(shape))
          await self.persistSubscriptionWatermark(key, settled, normalizedSnapshot);
      }
      const s = self.query.authorityResults.get(key);
      if (s) s.supportingRevision = normalizedSnapshot;
    }
  }
}

const viewVersionKey = (v: VersionRecord) =>
  `${v.table()}\u0000${v.branchKey().key()}\u0000${rowUuidKey(v.rowUuid())}`;

async function ingestViewBundle(self: NodeState, bundle: VersionBundle): Promise<void> {
  validateReceivedViewBundleGlobalTimeDurability(bundle.globalTime, bundle.durability);
  bundle.tx = transactionWithoutPermissionSubject(bundle.tx);
  if (bundle.tx.nTotalWrites !== bundle.versions.length)
    throw NodeErrors.malformedViewUpdate(
      "version bundle count does not match its declared scope payload",
    );
  const txId = bundle.tx.txId;
  const stored = await queryTransaction(self, txId);
  if (stored) {
    const storedIdentity = transactionWithoutPermissionSubject({ ...stored.tx, nTotalWrites: 0 });
    if (!knownTransactionPayloadMatches(storedIdentity, { ...bundle.tx, nTotalWrites: 0 }))
      throw NodeErrors.conflictingCommitUnit(txId);
  }
  const { fate, globalTime, durability } = bundle;
  if (bundle.tx.kind !== "Exclusive") {
    if (bundle.scope === "ViewScoped")
      return self.ingestViewScopedTransactionWithCurrentIndexes(
        bundle.tx,
        bundle.versions,
        fate,
        globalTime,
        durability,
      );
    return self.ingestKnownTransaction(bundle.tx, bundle.versions, fate, globalTime, durability);
  }
  const storedRecords = async (): Promise<VersionRecord[]> =>
    stored ? (await queryVersionsForTx(self, txId)).map((v) => self.versionRecordFromRow(v)) : [];
  if (bundle.scope === "ViewScoped") {
    const knownKeys = new Set((await storedRecords()).map(viewVersionKey));
    for (const v of bundle.versions) knownKeys.add(viewVersionKey(v));
    const redacted = { ...bundle.tx, nTotalWrites: tooLarge(knownKeys.size) };
    return stored?.isPartial
      ? self.ingestViewScopedTransactionWithCurrentIndexes(
          redacted,
          bundle.versions,
          fate,
          globalTime,
          durability,
        )
      : self.ingestTransactionFragmentWithoutCurrentIndexes(
          redacted,
          bundle.versions,
          fate,
          globalTime,
          durability,
        );
  }
  const completeLen = bundle.tx.nTotalWrites;
  const storedVersions = await storedRecords();
  const knownKeys = new Set([...storedVersions, ...bundle.versions].map(viewVersionKey));
  if (knownKeys.size > completeLen) throw NodeErrors.conflictingCommitUnit(txId);
  if (knownKeys.size === completeLen) {
    const seen = new Set<string>();
    const complete: VersionRecord[] = [];
    for (const v of [...bundle.versions, ...storedVersions]) {
      const k = viewVersionKey(v);
      if (!seen.has(k)) {
        seen.add(k);
        complete.push(v);
      }
    }
    return self.ingestKnownTransaction(bundle.tx, complete, fate, globalTime, durability);
  }
  return self.ingestTransactionFragmentWithoutCurrentIndexes(
    bundle.tx,
    bundle.versions,
    fate,
    globalTime,
    durability,
  );
}

async function stageViewBundle(
  self: NodeState,
  batch: any,
  bundle: VersionBundle,
  stagedTxIds: SortedMap<TxId, true>,
  stagedGlobalTimes: [number, TxId][],
  stagedContentVersions: VersionRow[],
  stagedRejections: any[],
): Promise<boolean> {
  validateReceivedViewBundleGlobalTimeDurability(bundle.globalTime, bundle.durability);
  if (bundle.tx.nTotalWrites !== bundle.versions.length)
    throw NodeErrors.malformedViewUpdate(
      "version bundle count does not match its declared scope payload",
    );
  const txId = bundle.tx.txId;
  const args = [
    batch,
    bundle.tx,
    bundle.versions.slice(),
    bundle.fate,
    bundle.globalTime,
    bundle.durability,
    stagedGlobalTimes,
    stagedContentVersions,
    stagedRejections,
  ];
  if (bundle.scope === "ViewScoped") {
    if (stagedTxIds.has(txId)) return true;
    stagedTxIds.set(txId, true);
    await self.stageViewScopedTransactionWithCurrentIndexes(...args);
    return true;
  }
  if (await queryTransaction(self, txId)) return false;
  if (bundle.tx.kind === "Exclusive" && bundle.versions.length !== bundle.tx.nTotalWrites)
    return false;
  if (stagedTxIds.has(txId)) return true;
  stagedTxIds.set(txId, true);
  await self.stageKnownTransaction(...args);
  return true;
}

function validateReceivedViewUpdateGlobalTimeDurability(update: any): void {
  for (const bundle of versionBundleRefsForCarriers(update.versionCarriers))
    validateReceivedViewBundleGlobalTimeDurability(bundle.globalTime, bundle.durability);
}

export function wholeTableSubscriptionKey(self: NodeState, table: string) {
  const [shape, binding] = wholeTableShapeBinding(self, table);
  return { shapeId: shape.shapeId(), bindingId: binding.bindingId(), readView: undefined };
}

export function wholeTableShapeBinding(self: NodeState, table: string): [any, any] {
  let schema: any;
  let schemaVersion: any;
  try {
    self.table(table);
    schema = self.catalogue.schema;
    schemaVersion = self.catalogue.localSchemaVersionId;
  } catch {
    schemaVersion = self.catalogue.activeSchema.schema;
    const payload = self.catalogue.catalogueSchemas.get(schemaVersion);
    if (!payload) throw NodeErrors.invalidStoredValue("current write schema payload missing");
    schema = payload.schema;
  }
  const shape = self.queryFromTable(table).validateWithSchemaVersion(schema, schemaVersion);
  return [shape, shape.bind(new Map())];
}

export function versionBundleForMaintainedViewVersionsWithTx(
  self: NodeState,
  storedTx: any,
  txVersions: VersionRow[],
) {
  return versionBundleForMaintainedViewVersionsWithTxAndSource(
    self,
    storedTx,
    txVersions,
    "IvmWitness",
  );
}

/** Whether direct immutable-store reads can skip witness canonicalization. */
function exactStorageMaintainedVersionsAreUnambiguous(
  self: NodeState,
  versions: VersionRow[],
): boolean {
  let all = true;
  for (const version of versions) {
    const versionTableId = String(self.physicalTableIdForVersion(version));
    const ids = new Set<string>();
    for (const mapping of self.catalogue.physicalMappings.values()) {
      const m = mapping.tables.get(version.table());
      if (m) ids.add(String(m.tableId));
    }
    all = all && ids.size === 1 && ids.has(versionTableId);
  }
  return all;
}

async function versionBundleForMaintainedViewVersionsWithTxAndSource(
  self: NodeState,
  storedTx: any,
  txVersions: VersionRow[],
  source: "IvmWitness" | "ExactStorage",
): Promise<VersionBundle> {
  const { txId, kind, nTotalWrites, madeBy, userMetadataJson, contributionMerge } = storedTx.tx;
  const versions = new SortedMap<string, VersionRecord>(cmpStr);
  for (const version of txVersions) {
    const canonical =
      source === "ExactStorage"
        ? version
        : await canonicalHistoryVersionForMaintainedWitness(self, version);
    const record = self.versionRecordFromRow(canonical);
    const key = versionBundleRecordKey(record);
    const existing = versions.get(key);
    if (existing === undefined) versions.set(key, record);
    else if (!versionRecordEquals(existing, record)) throw NodeErrors.conflictingCommitUnit(txId);
  }
  const list = [...versions.valuesArray()];
  const scope =
    !storedTx.isPartial && nTotalWrites === list.length ? "CompleteTransaction" : "ViewScoped";
  return {
    tx: {
      txId,
      kind,
      nTotalWrites: scope === "CompleteTransaction" ? nTotalWrites : tooLarge(list.length),
      madeBy,
      permissionSubject: undefined,
      baseSnapshot: undefined,
      rowReadSet: undefined,
      absentReadSet: undefined,
      predicateReadSet: undefined,
      userMetadataJson,
      contributionMerge,
    },
    versions: list,
    scope,
    fate: storedTx.fate,
    globalTime: storedTx.globalTime,
    durability: storedTx.durability,
  };
}

/** Return the stored, authored history row for a maintained witness. */
export async function canonicalHistoryVersionForMaintainedWitness(
  self: NodeState,
  version: VersionRow,
): Promise<VersionRow> {
  const logical = new Set<string>();
  for (const mapping of self.catalogue.physicalMappings.values()) {
    const m = mapping.tables.get(version.table());
    if (m) logical.add(String(m.tableId));
  }
  const witnessTxId = self.versionTxId(version);
  const sameRowBranch = (c: VersionRow) =>
    rowUuidKey(c.rowUuid()) === rowUuidKey(version.rowUuid()) &&
    c.branchKey().equals(version.branchKey());
  const tableIdOf = (c: VersionRow): string | undefined => {
    try {
      return String(self.physicalTableIdForVersion(c));
    } catch {
      return undefined;
    }
  };
  let physical = logical;
  if (logical.size !== 1) {
    physical = new Set();
    for (const c of await queryVersionsForTx(self, witnessTxId)) {
      const id = sameRowBranch(c) ? tableIdOf(c) : undefined;
      if (id !== undefined && logical.has(id)) physical.add(id);
    }
  }
  if (physical.size !== 1)
    throw NodeErrors.invalidStoredValue(
      "maintained witness maps to zero or multiple physical tables",
    );
  const [projected] = physical;
  const authoredSchema = self.schemaVersionForAlias(version.schemaVersionAlias());
  if (authoredSchema === undefined)
    throw NodeErrors.invalidStoredValue("maintained witness schema version alias must exist");
  try {
    self.tableInSchemaRef(version.table(), authoredSchema);
  } catch (error) {
    if ((error as NodeErrorLike).kind !== "TableNotFound") throw error;
    const canonical = (await queryVersionsForTx(self, witnessTxId)).find(
      (c) => sameRowBranch(c) && tableIdOf(c) === projected,
    );
    if (canonical) return canonical;
    throw NodeErrors.maintainedViewMissingBundleWitness(
      "maintained witness projection is missing its canonical history row",
    );
  }
  for (const storageTable of versionStorageSources(self, version.table())) {
    const canonical = await queryVersionByAliasWithStorageInSchema(
      self,
      authoredSchema,
      version.table(),
      storageTable,
      version.branchKey(),
      version.rowUuid(),
      version.txTime(),
      version.txNodeAlias(),
    );
    if (canonical && String(self.physicalTableIdForVersion(canonical)) === projected)
      return canonical;
  }
  const authoredTable = self.tableInSchemaRef(version.table(), authoredSchema);
  const authoredDescriptor = authoredTable.historyStorageTable().recordSchema();
  const hasAuthoredLayout = version.record.descriptor().equals(authoredDescriptor);
  let complete = false;
  if (hasAuthoredLayout) {
    if (version.deletion() !== undefined) complete = true;
    else {
      const authored: string[] | undefined = self.authoredColumnsForVersion(version);
      complete =
        authored === undefined ||
        authored.every((column) => {
          if (column === self.DELETION_COLUMN_NAME) return true;
          try {
            return version.cell(authoredTable, column) !== undefined;
          } catch {
            return false;
          }
        });
    }
  }
  if (complete) return version;
  throw NodeErrors.maintainedViewMissingBundleWitness(
    "maintained witness is missing its canonical history row",
  );
}
