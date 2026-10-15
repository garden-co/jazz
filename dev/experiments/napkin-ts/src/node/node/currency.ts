// Currency and version-selection reads over local and global storage.
// NodeState methods are ported as free functions taking the state first.
import { SortedMap, SortedSet, cmpNum, cmpStr } from "../../support/collections.js";
import {
  type BranchKey,
  type NodeState,
  type OwnedRecord,
  type StoredTransaction,
  type TxId,
  type Value,
  type VersionRow,
  BranchKey as BranchKeyApi,
  GlobalCurrentRowRecord as GC,
  HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES,
  HistoryRowRecord as HR,
  NodeAliasRowRecord as NA,
  NodeErrors,
  OwnedRecord as OwnedRecordApi,
  RecordDescriptorApi,
  RowAuthor,
  SchemaFields,
  TransactionRowRecord as TR,
  TxId as TxIdApi,
  cmpRowUuid,
  cmpSortKey,
  cmpTxId,
  decodeEvidenceSlots,
  decodePersistedBranchKey,
  durabilityFromDiscriminant,
  fateFromEncodedFields,
  historyKeyValues,
  historySeqCandidates,
  internTable,
  nodeUuidKey,
  nullableType,
  physicalHistoryTableName,
  physicalVersionTableId,
  readOptionalAuthorSubject,
  readOptionalRecord,
  rowAuthorValue,
  txIdKey,
  txKindFromDiscriminant,
  txTimeFromPhysicalMs,
  txTimeSortKey,
  versionTxIdFromAliases,
  VersionRowApi,
} from "../../_extern/node-views.js";

const bytesV = (bytes: Uint8Array): Value => ({ kind: "Bytes", bytes });
const uuidV = (uuid: any): Value => ({ kind: "Uuid", uuid });
const u64V = (n: number): Value => ({ kind: "U64", value: n });

function sortByRowThenTx(self: NodeState, versions: VersionRow[]): VersionRow[] {
  const aliases = self.nodeAliases;
  return versions.sort(
    (a, b) =>
      cmpRowUuid(a.rowUuid(), b.rowUuid()) ||
      cmpTxId(versionTxIdFromAliases(a, aliases), versionTxIdFromAliases(b, aliases)),
  );
}
const sortByTableThenRow = (versions: VersionRow[]) =>
  versions.sort((a, b) => cmpStr(a.table(), b.table()) || cmpRowUuid(a.rowUuid(), b.rowUuid()));

async function scanHistory(self: NodeState, table: string, prefix: Value[]): Promise<VersionRow[]> {
  const versions: VersionRow[] = [];
  for (const storageTable of versionStorageSources(self, table)) {
    const records = (await self.database.primaryKeyScanRaw(storageTable, prefix)).map((r: any) =>
      r.ownedRecord(),
    );
    for (const record of records)
      versions.push(await decodeStoredHistoryRecord(self, undefined, table, storageTable, record));
  }
  return sortByRowThenTx(self, versions);
}

export function queryRowVersions(self: NodeState, table: string, rowUuid: any) {
  return queryRowVersionsInBranch(self, table, BranchKeyApi.default_(), rowUuid);
}

export function queryRowVersionsInBranch(
  self: NodeState,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
) {
  return scanHistory(self, table, [bytesV(branchKey.canonicalBytes()), uuidV(rowUuid)]);
}

export async function queryVersionsInSchema(
  self: NodeState,
  schemaVersion: any,
  table: string,
  rowUuid: any | undefined,
): Promise<VersionRow[]> {
  const tableId = self.physicalTableIdForSchema(schemaVersion, table);
  const prefix = [bytesV(BranchKeyApi.default_().canonicalBytes())];
  if (rowUuid !== undefined) prefix.push(uuidV(rowUuid));
  const storageTable = physicalHistoryTableName(tableId);
  const records = (await self.database.primaryKeyScanRaw(storageTable, prefix)).map((r: any) =>
    r.ownedRecord(),
  );
  const versions: VersionRow[] = [];
  for (const record of records)
    versions.push(await decodeStoredPhysicalHistoryRecord(self, storageTable, record));
  return sortByRowThenTx(self, versions);
}

export function queryTableVersionsInBranch(self: NodeState, table: string, branchKey: BranchKey) {
  return scanHistory(self, table, [bytesV(branchKey.canonicalBytes())]);
}

export function queryLocalWinner(self: NodeState, table: string, rowUuid: any) {
  return queryWinnerFromPk(self, table, rowUuid);
}

export function queryLocalWinnerInBranch(
  self: NodeState,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
) {
  return queryCurrentWinnerInBranch(self, table, branchKey, rowUuid, "Ahead");
}

export async function queryLocalViewWinnerInBranch(
  self: NodeState,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
): Promise<VersionRow | undefined> {
  const overlay = await queryLocalWinnerInBranch(self, table, branchKey, rowUuid);
  if (overlay) return overlay;
  return queryGlobalWinnerInBranch(self, table, branchKey, rowUuid);
}

export async function queryLocalWinnerInBranchExcludingTx(
  self: NodeState,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
  excludedTxId: TxId,
): Promise<VersionRow | undefined> {
  let winner: VersionRow | undefined;
  for (const candidate of await queryRowVersionsInBranch(self, table, branchKey, rowUuid)) {
    const candidateTx = self.versionTxId(candidate);
    if (cmpTxId(candidateTx, excludedTxId) === 0) continue;
    const replace =
      winner === undefined ||
      cmpSortKey(
        txTimeSortKey(candidate.txTime(), candidateTx.node),
        txTimeSortKey(winner.txTime(), self.versionTxId(winner).node),
      ) > 0;
    if (replace) winner = candidate;
  }
  return winner;
}

export function queryGlobalWinner(self: NodeState, table: string, rowUuid: any) {
  let schemaVersion = self.catalogue.activeSchema.schema;
  try {
    self.tableInSchemaRef(table, schemaVersion);
  } catch {
    self.tableInSchemaRef(table, self.catalogue.localSchemaVersionId);
    schemaVersion = self.catalogue.localSchemaVersionId;
  }
  return queryGlobalWinnerInSchema(self, schemaVersion, table, rowUuid);
}

export function queryGlobalWinnerInBranch(
  self: NodeState,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
) {
  return queryCurrentWinnerInBranch(self, table, branchKey, rowUuid, "Global");
}

async function winnerFromCurrentTable(
  self: NodeState,
  schemaVersion: any,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
  currentTable: string,
): Promise<VersionRow | undefined> {
  const raw = await self.database.primaryKeyGetRaw(currentTable, [
    bytesV(branchKey.canonicalBytes()),
    uuidV(rowUuid),
  ]);
  if (!raw) return undefined;
  const variantTag = raw.variantTag();
  const current = raw.ownedRecord().borrowed();
  const winner = historyImageFromCurrentRecord(self, schemaVersion, table, variantTag, current);
  if (winner) return winner;
  const txTime = current.getU64(GC.FIELD_TX_TIME_IDX);
  const txNodeAlias = current.getU64(GC.FIELD_TX_NODE_ID_IDX);
  return queryVersionByAliasInBranch(
    self,
    schemaVersion,
    table,
    branchKey,
    rowUuid,
    txTime,
    txNodeAlias,
  );
}

function queryCurrentWinnerInBranch(
  self: NodeState,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
  cls: "Ahead" | "Global",
) {
  const schemaVersion = self.catalogue.activeSchema.schema;
  const currentTable = self.physicalCurrentTableForSchema(schemaVersion, table, cls);
  return winnerFromCurrentTable(self, schemaVersion, table, branchKey, rowUuid, currentTable);
}

export function queryGlobalWinnerInSchema(
  self: NodeState,
  schemaVersion: any,
  table: string,
  rowUuid: any,
) {
  return queryGlobalWinnerInSchemaAndBranch(
    self,
    schemaVersion,
    table,
    BranchKeyApi.default_(),
    rowUuid,
  );
}

export function queryGlobalWinnerInSchemaAndBranch(
  self: NodeState,
  schemaVersion: any,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
) {
  const currentTable = self.physicalCurrentTableForSchema(schemaVersion, table, "Global");
  return winnerFromCurrentTable(self, schemaVersion, table, branchKey, rowUuid, currentTable);
}

/** Rebuild the winner's history image from its global current record. */
export function historyImageFromCurrentRecord(
  self: NodeState,
  schemaVersion: any,
  table: string,
  variantTag: number,
  current: any,
): VersionRow | undefined {
  const historyTable = physicalHistoryTableName(
    self.physicalTableIdForSchema(schemaVersion, table),
  );
  const historyDescriptor = self.database
    .tableSchema(historyTable)
    .recordSchemaForVariant(variantTag);
  if (!historyDescriptor) return undefined;
  const currentDescriptor = current.descriptor();
  const globalTimeIdx = GC.FIELD_GLOBAL_TIME_IDX;
  const updatedByIdx = HR.FIELD_UPDATED_BY_IDX;
  const signsIdx: number | undefined = historyDescriptor.fieldIndex(
    SchemaFields.COUNTER_SIGNS_FIELD,
  );
  if (signsIdx === undefined) return undefined;
  const hFields = historyDescriptor.fields();
  const cFields = currentDescriptor.fields();
  const synthesized = (i: number) => i >= signsIdx && i < hFields.length;
  const sourceOf = (i: number) => (i >= globalTimeIdx ? i + 1 : i);
  if (cFields.length + (hFields.length - signsIdx) !== hFields.length + 1) return undefined;
  for (let index = 0; index < hFields.length; index++) {
    if (synthesized(index)) continue;
    let historyType = hFields[index].valueType;
    if (historyType.kind === "Nullable" && index === updatedByIdx) historyType = historyType.inner;
    if (!cFields[sourceOf(index)].valueType.equals(historyType)) return undefined;
  }
  const raw = historyDescriptor.createWithEncodedFields(
    current.raw().length,
    (index: number, output: number[]) => {
      if (synthesized(index)) {
        const name = hFields[index].name;
        let value: Value;
        if (name === SchemaFields.SEQ_FIELD)
          value = u64V(current.getNullableU64(globalTimeIdx) ?? 0);
        else if (name === SchemaFields.BASE_SEQ_FIELD || name === SchemaFields.BASE_PENDING_FIELD)
          value = { kind: "Nullable", value: undefined };
        else value = bytesV(new Uint8Array());
        historyDescriptor.encodeFieldInto(index, value, output);
        return;
      }
      if (index === updatedByIdx) {
        const author = current.getIdx(GC.FIELD_UPDATED_BY_IDX);
        historyDescriptor.encodeFieldInto(index, { kind: "Nullable", value: author }, output);
        return;
      }
      let ms: number | undefined;
      if (index === HR.FIELD_CREATED_AT_IDX) ms = current.getU64(GC.FIELD_CREATED_AT_IDX);
      else if (index === HR.FIELD_UPDATED_AT_IDX) ms = current.getU64(GC.FIELD_UPDATED_AT_IDX);
      if (ms !== undefined) {
        let time: number;
        try {
          time = txTimeFromPhysicalMs(ms);
        } catch {
          throw NodeErrors.invalidStoredValue("current provenance ms exceeds HLC range");
        }
        historyDescriptor.encodeFieldInto(index, u64V(time), output);
        return;
      }
      const [start, end] = currentDescriptor.fieldSpan(current.raw(), sourceOf(index));
      output.push(...current.raw().subarray(start, end));
    },
  );
  return decodeHistoryOwnedRecord(
    self,
    table,
    historyTable,
    OwnedRecordApi.new(raw, historyDescriptor),
  );
}

export function queryWinnerFromPk(self: NodeState, table: string, rowUuid: any) {
  return queryWinnerFromPkInBranch(self, table, BranchKeyApi.default_(), rowUuid);
}

/** Newest pending write if any, else newest accepted write. */
export async function queryWinnerFromPkInBranch(
  self: NodeState,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
): Promise<VersionRow | undefined> {
  let winner: [boolean, number, VersionRow] | undefined;
  for (const storageTable of versionStorageSources(self, table)) {
    const rowPrefix = [bytesV(branchKey.canonicalBytes()), uuidV(rowUuid)];
    const pendingPrefix = [...rowPrefix, u64V(0)];
    let found: [boolean, OwnedRecord] | undefined;
    for (const [pending, prefix] of [
      [true, pendingPrefix],
      [false, rowPrefix],
    ] as const) {
      const raw = await self.database.primaryKeyLastRaw(storageTable, prefix);
      if (raw) {
        found = [pending, raw.ownedRecord()];
        break;
      }
    }
    if (!found) continue;
    const [pending, raw] = found;
    const candidate = await decodeStoredHistoryRecord(self, undefined, table, storageTable, raw);
    const seq: number = candidate.seq();
    let replace = true;
    if (winner) {
      const [ep, es, existing] = winner;
      const ck = txTimeSortKey(candidate.txTime(), self.versionTxId(candidate).node);
      const ek = txTimeSortKey(existing.txTime(), self.versionTxId(existing).node);
      replace = pending !== ep ? pending : seq !== es ? seq > es : cmpSortKey(ck, ek) > 0;
    }
    if (replace) winner = [pending, seq, candidate];
  }
  return winner?.[2];
}

export async function queryTableVersions(self: NodeState, table: string): Promise<VersionRow[]> {
  const byKey = new Map<string, VersionRow>();
  for (const storageTable of versionStorageSources(self, table)) {
    const raws = (await self.database.primaryKeyScanRaw(storageTable, [])).map((r: any) =>
      r.ownedRecord(),
    );
    for (const record of raws) {
      const version = await decodeStoredHistoryRecord(self, undefined, table, storageTable, record);
      const txId = self.versionTxId(version);
      byKey.set(`${version.branchKey().key()}|${version.rowUuid()}|${txIdKey(txId)}`, version);
    }
  }
  return sortByRowThenTx(self, [...byKey.values()]);
}

export async function queryVersionsForTx(self: NodeState, txId: TxId): Promise<VersionRow[]> {
  const tx = await queryTransaction(self, txId);
  if (!tx) return [];
  rememberHistoryTxAuthor(self, tx.tx.txId.time, tx.nodeAlias, tx.tx.madeBy);
  const cached: VersionRow[] | undefined = self.cachedTxVersions(txId);
  if (cached) return sortByTableThenRow(cached);
  const versions: VersionRow[] = [];
  const touchedRows = await self.loadTxTouchedRows(undefined, txId.time, tx.nodeAlias);
  const seqs = historySeqCandidates(tx.globalTime);
  for (const [tableId, branchKey, rowUuid] of touchedRows) {
    const storageTable = physicalHistoryTableName(tableId);
    let found: OwnedRecord | undefined;
    for (const seq of seqs) {
      found = (
        await self.database.primaryKeyGetRaw(
          storageTable,
          historyKeyValues(branchKey, rowUuid, seq, txId.time, tx.nodeAlias),
        )
      )?.ownedRecord();
      if (found) break;
    }
    if (!found) continue;
    versions.push(await decodeStoredPhysicalHistoryRecord(self, storageTable, found));
  }
  return sortByTableThenRow(versions);
}

export async function queryVersionsForTxRowsByAlias(
  self: NodeState,
  txId: TxId,
  txNodeAlias: number,
  rows: Iterable<[string, any]>,
): Promise<VersionRow[]> {
  const versions: VersionRow[] = [];
  for (const [table, rowUuid] of rows) {
    const version = await queryVersionByAlias(self, table, rowUuid, txId.time, txNodeAlias);
    if (version) versions.push(version);
  }
  return sortByTableThenRow(versions);
}

export function versionStorageSources(self: NodeState, table: string): string[] {
  const cache: Map<string, string[]> = self.query.versionStorageSourcesCache;
  const hit = cache.get(table);
  if (hit) return hit.slice();
  const sources = [...new Set(physicalVersionStorageSources(self, table))].sort();
  if (sources.length === 0) throw NodeErrors.tableNotFound(table);
  cache.set(table, sources.slice());
  return sources;
}

function physicalVersionStorageSources(self: NodeState, table: string): string[] {
  const out: string[] = [];
  for (const mapping of self.catalogue.physicalMappings.values()) {
    const m = mapping.tables.get(table);
    if (m) out.push(physicalHistoryTableName(m.tableId));
  }
  return out;
}

export function decodeHistoryRecord(self: NodeState, table: string, record: any): VersionRow {
  return decodeHistoryOwnedRecord(
    self,
    table,
    "",
    OwnedRecordApi.new(record.raw().slice(), record.descriptor()),
  );
}

export async function decodeStoredHistoryRecord(
  self: NodeState,
  batch: any,
  requestedTable: string,
  storageTable: string,
  record: OwnedRecord,
): Promise<VersionRow> {
  const version = decodeHistoryOwnedRecord(self, requestedTable, storageTable, record);
  return resolveHistoryUpdatedBy(self, batch, version);
}

const authorKey = (txTime: number, alias: number) => `${txTime}:${alias}`;

export function rememberHistoryTxAuthor(
  self: NodeState,
  txTime: number,
  txNodeAlias: number,
  madeBy: any,
): void {
  const authors: Map<string, Uint8Array> = self.historyTxAuthors;
  const key = authorKey(txTime, txNodeAlias);
  if (authors.size >= HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES && !authors.has(key)) authors.clear();
  authors.set(key, encodeHistoryUpdatedBy(rowAuthorValue(madeBy)));
}

export function decodeStoredPhysicalHistoryRecord(
  self: NodeState,
  storageTable: string,
  record: OwnedRecord,
) {
  if (physicalVersionTableId(storageTable) === undefined)
    throw NodeErrors.invalidStoredValue("history record is not from a physical history table");
  return decodeStoredHistoryRecord(self, undefined, "", storageTable, record);
}

export async function resolveHistoryUpdatedBy(
  self: NodeState,
  batch: any,
  version: VersionRow,
): Promise<VersionRow> {
  if (!version.updatedByIsImplicit()) return version;
  const txTime = version.txTime();
  const alias = version.txNodeAlias();
  const author =
    (self.historyTxAuthors as Map<string, Uint8Array>).get(authorKey(txTime, alias)) ??
    (await loadHistoryTxAuthor(self, batch, txTime, alias));
  const input = version.record.borrowed();
  const descriptor = input.descriptor();
  const raw = descriptor.createWithEncodedFields(
    input.raw().length + author.length,
    (index: number, output: number[]) => {
      if (index === HR.FIELD_UPDATED_BY_IDX) output.push(...author);
      else {
        const [s, e] = descriptor.fieldSpan(input.raw(), index);
        output.push(...input.raw().subarray(s, e));
      }
    },
  );
  return VersionRowApi.new_(version.table, version.branchKey, OwnedRecordApi.new(raw, descriptor));
}

function rememberHistoryTxSeq(
  self: NodeState,
  txTime: number,
  alias: number,
  record: any,
): number | undefined {
  const globalTime: number | undefined = record.getNullableU64(TR.FIELD_GLOBAL_TIME_IDX);
  if (globalTime !== undefined) {
    const seqs: Map<string, number> = self.historyTxSeqs;
    if (seqs.size >= HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES) seqs.clear();
    seqs.set(authorKey(txTime, alias), globalTime);
  }
  return globalTime;
}

async function getTransactionRaw(self: NodeState, batch: any, pk: Value[]) {
  return batch
    ? self.database.primaryKeyGetRawInBatch(batch, "jazz_transactions", pk)
    : self.database.primaryKeyGetRaw("jazz_transactions", pk);
}

export async function historyTxSeq(
  self: NodeState,
  batch: any,
  txTime: number,
  txNodeAlias: number,
): Promise<number | undefined> {
  const seq = (self.historyTxSeqs as Map<string, number>).get(authorKey(txTime, txNodeAlias));
  if (seq !== undefined) return seq;
  const raw = await getTransactionRaw(self, batch, [u64V(txTime), u64V(txNodeAlias)]);
  if (!raw) return undefined;
  return rememberHistoryTxSeq(self, txTime, txNodeAlias, raw.record());
}

async function loadHistoryTxAuthor(self: NodeState, batch: any, txTime: number, alias: number) {
  const raw = await getTransactionRaw(self, batch, [u64V(txTime), u64V(alias)]);
  if (!raw)
    throw NodeErrors.invalidStoredValue(
      "a history image without updated_by needs its transaction record",
    );
  rememberHistoryTxSeq(self, txTime, alias, raw.record());
  const author = encodeHistoryUpdatedBy(raw.record().getIdx(TR.FIELD_MADE_BY_IDX));
  const authors: Map<string, Uint8Array> = self.historyTxAuthors;
  if (authors.size >= HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES) authors.clear();
  authors.set(authorKey(txTime, alias), author);
  return author;
}

export function decodeHistoryOwnedRecord(
  self: NodeState,
  requestedTable: string,
  storageTable: string,
  record: OwnedRecord,
): VersionRow {
  const view = record.borrowed();
  const schemaAlias = view.getU64(HR.FIELD_SCHEMA_VERSION_IDX);
  const schemaVersion = self.schemaVersionForAlias(schemaAlias);
  if (schemaVersion === undefined)
    throw NodeErrors.invalidStoredValue("version storage schema version alias must exist");
  let table: string | undefined;
  if (!storageTable.startsWith("jazz_physical_")) table = requestedTable;
  else {
    const tableId = physicalVersionTableId(storageTable);
    if (tableId === undefined)
      throw NodeErrors.invalidStoredValue("physical version storage logical table mapping missing");
    const mapping = self.catalogue.physicalMappings.get(schemaVersion);
    if (mapping)
      for (const [logical, m] of mapping.tables)
        if (m.tableId === tableId) {
          table = logical;
          break;
        }
    if (table === undefined)
      throw NodeErrors.invalidStoredValue("physical version storage logical table mapping missing");
  }
  const tableSchema = self.tableInSchemaRef(table, schemaVersion);
  const txNodeAlias = view.getU64(HR.FIELD_TX_NODE_ID_IDX);
  if (self.nodeAliases.nodeForAlias(txNodeAlias) === undefined)
    throw NodeErrors.invalidStoredValue("history tx node alias must exist");
  let branchKey: BranchKey;
  try {
    branchKey = decodePersistedBranchKey(tableSchema, view.getBytes(HR.FIELD_BRANCH_KEY_IDX));
  } catch {
    throw NodeErrors.invalidStoredValue("invalid stored branch key");
  }
  const version = VersionRowApi.new_(internTable(table), branchKey, record);
  version.validateCanonical();
  return version;
}

export function queryTransaction(
  self: NodeState,
  txId: TxId,
): Promise<StoredTransaction | undefined> {
  return queryTransactionFields(self, txId, (state, alias, record) =>
    storedTransactionFromRecord(state, txId, alias, record),
  );
}

export function queryTransactionState(self: NodeState, txId: TxId) {
  return queryTransactionFields(self, txId, (_s, _a, record) => [
    fateFromEncodedFields(record),
    record.getNullableU64(TR.FIELD_GLOBAL_TIME_IDX) as number | undefined,
    durabilityFromDiscriminant(record.getEnum(TR.FIELD_DURABILITY_IDX)),
  ]);
}

export function queryTransactionGlobalTime(self: NodeState, txId: TxId) {
  return queryTransactionFields(
    self,
    txId,
    (_s, _a, record) => [record.getNullableU64(TR.FIELD_GLOBAL_TIME_IDX)] as [number | undefined],
  );
}

type Decode<T> = (state: NodeState, alias: number, record: any) => T;

async function queryTransactionFields<T>(
  self: NodeState,
  txId: TxId,
  decode: Decode<T>,
): Promise<T | undefined> {
  const known: number | undefined = self.nodeAliases.get(txId.node);
  if (known !== undefined) return queryTransactionFieldsByAlias(self, txId, known, decode);
  if (
    self.absentNodeAlias !== undefined &&
    nodeUuidKey(self.absentNodeAlias) === nodeUuidKey(txId.node)
  ) {
    self.database.tableSchema("jazz_nodes");
    return undefined;
  }
  const aliases: number[] = [];
  for (const raw of await self.database.primaryKeyScanRaw("jazz_nodes", [])) {
    const record = raw.record();
    if (nodeUuidKey(record.getUuid(NA.FIELD_UUID_IDX)) === nodeUuidKey(txId.node))
      aliases.push(record.getU64(NA.FIELD_ID_IDX));
  }
  if (aliases.length === 0) {
    self.absentNodeAlias = txId.node;
    return undefined;
  }
  if (aliases.length === 1) self.nodeAliases.insert(txId.node, aliases[0]);
  for (const expected of aliases) {
    const tx = await queryTransactionFieldsByAlias(self, txId, expected, decode);
    if (tx !== undefined) {
      self.nodeAliases.insert(txId.node, expected);
      return tx;
    }
  }
  return undefined;
}

export async function preloadTransactionMemo(
  self: NodeState,
  txIds: Iterable<TxId>,
  context: any,
): Promise<void> {
  const txRows: Map<string, [TxId, StoredTransaction | undefined]> = context.txRows;
  const byAlias = new Map<string, { node: any; alias: number; times: SortedSet<number> }>();
  for (const txId of txIds) {
    if (txRows.has(txIdKey(txId))) continue;
    const alias: number | undefined = self.nodeAliases.get(txId.node);
    if (alias !== undefined) {
      const k = `${nodeUuidKey(txId.node)}:${alias}`;
      let e = byAlias.get(k);
      if (!e) byAlias.set(k, (e = { node: txId.node, alias, times: new SortedSet(cmpNum) }));
      e.times.add(txId.time);
    } else txRows.set(txIdKey(txId), [txId, await queryTransaction(self, txId)]);
  }
  for (const { node, alias, times } of byAlias.values()) {
    if (times.size === 1) {
      const txId = TxIdApi.new_(times.first()!, node);
      txRows.set(txIdKey(txId), [txId, await queryTransactionByAlias(self, txId, alias)]);
      continue;
    }
    const minTime = times.first()!;
    const maxTime = times.last()!;
    if (maxTime >= Number.MAX_SAFE_INTEGER) {
      for (const time of times) {
        const txId = TxIdApi.new_(time, node);
        txRows.set(txIdKey(txId), [txId, await queryTransactionByAlias(self, txId, alias)]);
      }
      continue;
    }
    for (const time of times) {
      const txId = TxIdApi.new_(time, node);
      txRows.set(txIdKey(txId), [txId, undefined]);
    }
    for (const raw of await self.database.primaryKeyScanRangeRaw(
      "jazz_transactions",
      [u64V(minTime), u64V(0)],
      [u64V(maxTime + 1), u64V(0)],
    )) {
      const record = raw.record();
      const rowAlias = record.getU64(TR.FIELD_NODE_ID_IDX);
      const time = record.getU64(TR.FIELD_TIME_IDX);
      if (rowAlias !== alias || !times.has(time)) continue;
      const txId = TxIdApi.new_(time, node);
      txRows.set(txIdKey(txId), [txId, storedTransactionFromRecord(self, txId, alias, record)]);
    }
  }
}

function queryTransactionByAlias(self: NodeState, txId: TxId, alias: number) {
  return queryTransactionFieldsByAlias(self, txId, alias, (s, a, r) =>
    storedTransactionFromRecord(s, txId, a, r),
  );
}

async function queryTransactionFieldsByAlias<T>(
  self: NodeState,
  txId: TxId,
  expectedAlias: number,
  decode: Decode<T>,
): Promise<T | undefined> {
  const raw = await self.database.primaryKeyGetRaw("jazz_transactions", [
    u64V(txId.time),
    u64V(expectedAlias),
  ]);
  if (!raw) return undefined;
  const record = raw.record();
  const nodeAlias = record.getU64(TR.FIELD_NODE_ID_IDX);
  const time = record.getU64(TR.FIELD_TIME_IDX);
  if (nodeAlias !== expectedAlias || time !== txId.time) return undefined;
  rememberHistoryTxSeq(self, time, nodeAlias, record);
  return decode(self, expectedAlias, record);
}

function storedTransactionFromRecord(
  self: NodeState,
  txId: TxId,
  expectedAlias: number,
  record: any,
): StoredTransaction {
  const kind = txKindFromDiscriminant(record.getEnum(TR.FIELD_KIND_IDX));
  const evidenceSlot = (idx: number): Uint8Array | undefined => {
    const v = record.getIdx(idx);
    if (v.kind !== "Nullable")
      throw NodeErrors.invalidStoredValue("exclusive read evidence slot is not nullable");
    if (v.value === undefined) return undefined;
    if (v.value.kind !== "Bytes")
      throw NodeErrors.invalidStoredValue("exclusive read evidence slot is not bytes");
    return v.value.bytes;
  };
  const slots = [
    evidenceSlot(TR.FIELD_BASE_SNAPSHOT_IDX),
    evidenceSlot(TR.FIELD_ROW_READ_SET_IDX),
    evidenceSlot(TR.FIELD_ABSENT_READ_SET_IDX),
    evidenceSlot(TR.FIELD_PREDICATE_READ_SET_IDX),
  ];
  if (kind !== "Exclusive" && slots.some((s) => s !== undefined))
    throw NodeErrors.invalidStoredValue("mergeable transaction carries exclusive read evidence");
  const evidence = decodeEvidenceSlots(...slots);
  let madeBy: any;
  try {
    madeBy = RowAuthor.fromRecord(record.getRecord(TR.FIELD_MADE_BY_IDX)).asAuthorSubject();
  } catch {
    throw NodeErrors.groove({ kind: "NonCanonicalRecord" });
  }
  const merge = readOptionalRecord(record, TR.FIELD_CONTRIBUTION_MERGE_IDX);
  const tx = {
    txId,
    kind,
    nTotalWrites: record.getU32(TR.FIELD_N_TOTAL_WRITES_IDX),
    madeBy,
    permissionSubject: readOptionalAuthorSubject(record, TR.FIELD_PERMISSION_SUBJECT_IDX),
    baseSnapshot: evidence.baseSnapshot,
    rowReadSet: evidence.rowReadSet,
    absentReadSet: evidence.absentReadSet,
    predicateReadSet: evidence.predicateReadSet,
    userMetadataJson: record.getNullableString(TR.FIELD_USER_METADATA_IDX),
    contributionMerge: merge && self.contributionMergeFromStorageRecord(merge),
  };
  self.validateContributionMergeOperationIdentities(tx);
  return {
    tx,
    nodeAlias: expectedAlias,
    fate: fateFromEncodedFields(record),
    globalTime: record.getNullableU64(TR.FIELD_GLOBAL_TIME_IDX),
    durability: durabilityFromDiscriminant(record.getEnum(TR.FIELD_DURABILITY_IDX)),
    isPartial: record.getBool(TR.FIELD_IS_PARTIAL_IDX),
  };
}

export async function transactionExists(self: NodeState, txId: TxId): Promise<boolean> {
  const alias: number | undefined = self.nodeAliases.get(txId.node);
  if (alias === undefined) return false;
  return (
    (await self.database.primaryKeyGetRaw("jazz_transactions", [u64V(txId.time), u64V(alias)])) !==
    undefined
  );
}

export async function queryVersionByAlias(
  self: NodeState,
  table: string,
  rowUuid: any,
  txTime: number,
  txNodeAlias: number,
): Promise<VersionRow | undefined> {
  for (const storageTable of versionStorageSources(self, table)) {
    const v = await queryVersionByAliasWithStorage(
      self,
      table,
      storageTable,
      rowUuid,
      txTime,
      txNodeAlias,
    );
    if (v) return v;
  }
  return undefined;
}

export function queryVersionByAliasInBranch(
  self: NodeState,
  schemaVersion: any,
  table: string,
  branchKey: BranchKey,
  rowUuid: any,
  txTime: number,
  txNodeAlias: number,
) {
  const storageTable = physicalHistoryTableName(
    self.physicalTableIdForSchema(schemaVersion, table),
  );
  return queryVersionByAliasWithStorageInSchema(
    self,
    schemaVersion,
    table,
    storageTable,
    branchKey,
    rowUuid,
    txTime,
    txNodeAlias,
  );
}

export function queryVersionByAliasWithStorage(
  self: NodeState,
  table: string,
  storageTable: string,
  rowUuid: any,
  txTime: number,
  txNodeAlias: number,
) {
  let schemaVersion = self.catalogue.activeSchema.schema;
  try {
    self.tableInSchemaRef(table, schemaVersion);
  } catch {
    schemaVersion = self.catalogue.localSchemaVersionId;
  }
  return queryVersionByAliasWithStorageInSchema(
    self,
    schemaVersion,
    table,
    storageTable,
    BranchKeyApi.default_(),
    rowUuid,
    txTime,
    txNodeAlias,
  );
}

export async function queryVersionByAliasWithStorageInSchema(
  self: NodeState,
  _schemaVersion: any,
  table: string,
  storageTable: string,
  branchKey: BranchKey,
  rowUuid: any,
  txTime: number,
  txNodeAlias: number,
): Promise<VersionRow | undefined> {
  const seqs = historySeqCandidates(await historyTxSeq(self, undefined, txTime, txNodeAlias));
  let raw: OwnedRecord | undefined;
  for (const seq of seqs) {
    const key = historyKeyValues(branchKey.canonicalBytes(), rowUuid, seq, txTime, txNodeAlias);
    raw = (await self.database.primaryKeyGetRaw(storageTable, key))?.ownedRecord();
    if (raw) break;
  }
  if (!raw) return undefined;
  return decodeStoredHistoryRecord(self, undefined, table, storageTable, raw);
}

/** Encoded history `updated_by` field holding `author`. */
function encodeHistoryUpdatedBy(author: Value): Uint8Array {
  const descriptor = RecordDescriptorApi.new_([
    ["updated_by", nullableType(RowAuthor.valueType())],
  ]);
  const encoded: number[] = [];
  descriptor.encodeFieldInto(0, { kind: "Nullable", value: author }, encoded);
  return new Uint8Array(encoded);
}
