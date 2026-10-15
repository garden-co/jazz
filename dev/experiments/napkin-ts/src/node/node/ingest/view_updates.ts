// Post-image merging at the authority, merge-op admission, and global/ahead current-row maintenance.
import { SortedSet } from "../../../support/collections.js";
import { equalBytes } from "../../../support/bytes.js";
import {
  nodeError,
  V,
  valueTypeEquals,
  descriptorEquals,
  mergeOps,
  applyCompiledLensPath,
  PK,
  cmpTxId,
  txIdEquals,
  type NodeState,
  type VersionRecord,
  type TxId,
  type DatabaseBatch,
  type Uuid,
  type Value,
  type ValueType,
} from "../../../_extern/node-physical-ingest.js";
import {
  type VersionRow,
  HistoryRowRecord,
  GlobalCurrentRowRecord,
  TransactionRowRecord,
  DELETION_COLUMN_ID,
  DELETION_COLUMN_NAME,
  historyRecordDescriptor,
  historySeqCandidates,
  historyKeyValues,
  globalCurrentPrimaryKey,
} from "../codec.js";
import {
  physicalCurrentTableForSchema,
  physicalTableIdForSchema,
  preparedPhysicalWritePlan,
  encodePhysicalVersionRecord,
  versionStorageWriteBinding,
  versionStoragePrimaryKey,
  authoredColumnsForVersion,
  physicalCellToAuthored,
} from "../physical.js";
import {
  validateWriteBase,
  lateWriteRefusal,
  passedOverPredecessor,
  recordSupersededWrites,
  imagePhysicalCells,
  deletionCellType,
} from "./write_base.js";

const invalid = (msg: string) => nodeError("InvalidStoredValue", msg);
const authorsIn = (authored: SortedSet<string> | undefined) => (name: string) =>
  !authored || authored.has(name);
const rowPk = (branchKey: any, rowUuid: Uuid) => [
  V.bytes(branchKey.canonicalBytes()),
  V.uuid(rowUuid),
];

/**
 * Apply an accepted write to the row's post-image as a patch, one cell at a time (SPEC 4 §4.6, "Patch rule").
 * Only the node that minted `globalTime` derives the post-image; elsewhere the merge is a prediction that yields
 * to Core's image (returns `undefined` where it cannot be made over the image held). Returns `undefined` when the
 * post-image does not change.
 */
export async function mergedGlobalPostImage(
  node: NodeState,
  batch: DatabaseBatch,
  schemaVersion: Uuid,
  tableSchema: any,
  incoming: VersionRow,
  incomingTx: TxId,
  globalTime: bigint,
  authority: boolean,
): Promise<VersionRow | undefined> {
  const authored = authoredColumnsForVersion(node, incoming);
  const authors = authorsIn(authored);
  const incomingDescriptor = incoming.record.descriptor;
  const base = incoming.base();
  const atSeq = (values: Value[]) =>
    incoming.withRecordValues(values).withMergeFields(globalTime, base);
  const winner = await queryGlobalWinnerWithSeqInBatch(
    node,
    batch,
    schemaVersion,
    tableSchema.name,
    incoming.branchKey,
    incoming.rowUuid(),
  );
  if (!winner) {
    // The first image of a row: each counter cell's low bits are the written value itself.
    const values: Value[] = incoming.record.toValues();
    mergeOps.clearCounterSigns(values, incomingDescriptor);
    return atSeq(values);
  }
  const [previous, previousSeq] = winner;
  if (txIdEquals(node.versionTxId(previous), incomingTx)) return undefined;
  // Core's image at a later seq already counts this write; the authority mints in order and never gets here.
  if (previousSeq !== undefined && previousSeq > globalTime) return undefined;
  const sameLayout = previous.schemaVersionAlias() === incoming.schemaVersionAlias();
  if (!authority && !sameLayout) {
    // A prediction across layouts: the write replaces the row whole, keeping merge columns' settled values.
    const values: Value[] = incoming.record.toValues();
    const carried = crossSchemaSettledMergeCells(
      node,
      schemaVersion,
      tableSchema,
      authored,
      previous,
    );
    if (carried.kind === "Err") return undefined;
    for (const [field, value] of carried.cells) values[field] = value;
    mergeOps.clearCounterSigns(values, incomingDescriptor);
    return atSeq(values);
  }

  const current = imagePhysicalCells(node, previous);
  const mapping = node.catalogue.physicalMappings
    .get(schemaVersion)
    ?.tables.get(tableSchema.name)?.columns;
  if (!mapping) throw invalid("incoming physical table mapping missing");
  // The current value of a cell when the current image carries its physical column with this layout's type.
  const carried = (id: number, columnType: ValueType): [Value | undefined] | undefined => {
    const cell = current.get(id);
    return cell && valueTypeEquals(cell[0], columnType) ? [cell[1]] : undefined;
  };
  // The post-image keeps the incoming write's identity and layout: it is the row as of this write's seq.
  const merged: Value[] = incoming.record.toValues();
  const previousValues: Value[] | undefined = sameLayout ? previous.record.toValues() : undefined;
  // Stored images spell enum cells with physical tags, uploaded writes with authored ones: re-tag carried cells.
  const incomingAuthored = descriptorEquals(
    incomingDescriptor,
    historyRecordDescriptor(tableSchema),
  );
  const counterSigns = incoming.counterSigns();
  // An authored `_deletion` applies; otherwise the row keeps its deletion state (SPEC 4 §4.6, "Edit after delete").
  if (!authors(DELETION_COLUMN_NAME)) {
    const kept = carried(DELETION_COLUMN_ID, deletionCellType());
    if (kept) merged[HistoryRowRecord.FIELD__DELETION_IDX] = V.nullable(kept[0]);
  }
  let crossLayoutMergeCells: [number, Value][] = [];
  if (!sameLayout) {
    const result = crossSchemaSettledMergeCells(
      node,
      schemaVersion,
      tableSchema,
      authored,
      previous,
    );
    if (result.kind === "Err")
      throw invalid("accepted cross-schema write cannot carry a merge column's settled value");
    crossLayoutMergeCells = result.cells;
  }
  for (let index = 0; index < tableSchema.columns.length; index++) {
    const column = tableSchema.columns[index];
    const field = HistoryRowRecord.USER_CELLS + index;
    const strategy = tableSchema.mergeStrategy(column.name);
    if (strategy === "Lww") {
      const id = mapping.get(column.name);
      if (id === undefined) throw invalid("incoming column physical mapping missing");
      // Authored cells take the write's value; unauthored ones keep the row's where its image carries the column.
      const keep = authors(column.name) ? undefined : carried(id, column.columnType);
      if (keep) {
        const value = V.nullable(keep[0]);
        merged[field] = incomingAuthored
          ? physicalCellToAuthored(node, schemaVersion, tableSchema.name, index, value)
          : value;
      }
      continue;
    }
    if (!previousValues) {
      // Across layouts the authority refused merge-column writes before minting the seq (#3899).
      if (authors(column.name)) throw invalid("accepted merge-column write across schema versions");
      continue;
    }
    if (!authors(column.name)) {
      merged[field] = previousValues[field];
      continue;
    }
    // Merge columns are ops: they apply in seq order.
    const negative = mergeOps.counterSign(tableSchema, counterSigns, index);
    let applied: Value;
    if (strategy === "Counter") {
      const result = mergeOps.counterAfterOp(
        column.columnType,
        previousValues[field],
        merged[field],
        negative,
      );
      if (result === undefined) {
        // Out of range over a stale local image.
        if (!authority) return undefined;
        throw invalid("accepted counter op leaves the column's range");
      }
      applied = result;
    } else
      applied = mergeOps.applyMergeOp(
        strategy,
        column.columnType,
        previousValues[field],
        merged[field],
        negative,
      );
    // History cells are stored nullable.
    merged[field] = V.nullable(applied);
  }
  for (const [field, value] of crossLayoutMergeCells) merged[field] = value;
  for (const index of [
    HistoryRowRecord.FIELD_CREATED_BY_IDX,
    HistoryRowRecord.FIELD_CREATED_AT_IDX,
  ])
    merged[index] = previous.record.getIdx(index);
  mergeOps.clearCounterSigns(merged, incomingDescriptor);
  return atSeq(merged);
}

/**
 * The authority refuses a write it cannot merge into the row without losing or inventing merge-column state, a
 * counter op leaving its type's range, or a base it cannot name exactly (`INV-HIST-20`).
 */
export async function mergeOpRejection(
  node: NodeState,
  txId: TxId,
  versions: VersionRecord[],
): Promise<any | undefined> {
  const batch = node.database.openBatch();
  const malformed = (detail: string) => ({ kind: "MalformedCommit", detail });
  // Writes of the unit's node that applying it passes over, recorded once the unit passes every check here.
  const passedOver: [VersionRecord, bigint][] = [];
  for (const version of versions) {
    const schemaVersion = version.schemaVersion();
    const [currentSeq, currentWriter] =
      (await globalCurrentSeqAndWriterInBatch(
        node,
        batch,
        schemaVersion,
        version.table(),
        version.branchKey(),
        version.rowUuid(),
      )) ?? [];
    const late = await lateWriteRefusal(
      node,
      batch,
      schemaVersion,
      version.table(),
      version.branchKey(),
      version.rowUuid(),
      version.base(),
      txId,
      currentSeq,
      currentWriter,
    );
    if (late !== undefined) return malformed(late);
    if (!version.base().isEmpty()) {
      const refusal = await validateWriteBase(
        node,
        batch,
        schemaVersion,
        version.table(),
        version.branchKey(),
        version.rowUuid(),
        version.base(),
        txId,
        currentSeq,
      );
      if (refusal !== undefined) return malformed(refusal);
    }
    const through = await passedOverPredecessor(node, version.base(), txId, currentWriter);
    if (through !== undefined) passedOver.push([version, through]);
    const tableSchema = node.tableInSchema(version.table(), schemaVersion);
    const authored: SortedSet<string> | undefined = version.authoredColumns();
    const authors = authorsIn(authored);
    const isMerge = (column: any) => tableSchema.mergeStrategy(column.name) !== "Lww";
    const tableHasMerge = tableSchema.columns.some(isMerge);
    // The row's image may hold merge state from another schema version even when this table has none.
    if (!tableHasMerge && !anyKnownMergeColumn(node, version.table())) continue;
    const winner = await queryGlobalWinnerWithSeqInBatch(
      node,
      batch,
      schemaVersion,
      version.table(),
      version.branchKey(),
      version.rowUuid(),
    );
    if (!winner) continue;
    const [current] = winner;
    const notSupported = (column: string) =>
      malformed(
        `merge column '${version.table()}.${column}' cannot be merged with the row's current image, which is stored under a different schema version; merge-column writes across schema versions are not supported yet (#3899)`,
      );
    const currentSchema = node.schemaVersionForAlias(current.schemaVersionAlias());
    if (!currentSchema || !equalBytes(currentSchema, schemaVersion)) {
      const authoredMerge = tableSchema.columns.find((c: any) => isMerge(c) && authors(c.name));
      if (authoredMerge) return notSupported(authoredMerge.name);
      const notCarried = imageMergeColumnNotCarried(node, schemaVersion, tableSchema, current);
      if (notCarried !== undefined) return notSupported(notCarried);
      const settled = crossSchemaSettledMergeCells(
        node,
        schemaVersion,
        tableSchema,
        authored,
        current,
      );
      if (settled.kind === "Err") return notSupported(settled.column);
      continue;
    }
    if (!tableHasMerge) continue;
    for (let index = 0; index < tableSchema.columns.length; index++) {
      const column = tableSchema.columns[index];
      if (tableSchema.mergeStrategy(column.name) !== "Counter" || !authors(column.name)) continue;
      const op = version.optionalCellAt(index);
      if (op === undefined) continue;
      const negative = mergeOps.counterSign(tableSchema, version.counterSigns(), index);
      const previous = current.cell(tableSchema, column.name) ?? V.nullable();
      if (mergeOps.counterAfterOp(column.columnType, previous, op, negative) === undefined) {
        const delta: bigint = mergeOps.counterOpDelta(column.columnType, op, negative);
        return malformed(
          `counter '${version.table()}.${column.name}' would be out of range for its ${JSON.stringify(column.columnType)} column: ${mergeOps.counterToI128(previous)} ${delta >= 0n ? "+" : ""}${delta} leaves the type's range, so the write is rejected rather than wrapped`,
        );
      }
    }
  }
  for (const [version, through] of passedOver)
    await recordSupersededWrites(
      node,
      physicalTableIdForSchema(node, version.schemaVersion(), version.table()),
      version.branchKey(),
      version.rowUuid(),
      txId.node,
      through,
    );
  return undefined;
}

/** Whether a row of `table` can have an image holding merge state under some schema version this node knows. */
function anyKnownMergeColumn(node: NodeState, table: string): boolean {
  const hasMerge = (schema: any, name: string | undefined) =>
    schema.tables.some(
      (t: any) =>
        (name === undefined || t.name === name) &&
        t.columns.some((c: any) => t.mergeStrategy(c.name) !== "Lww"),
    );
  const anySchema = (name: string | undefined) =>
    hasMerge(node.catalogue.schema, name) ||
    hasMerge(node.catalogue.activeSchema.compiled, name) ||
    node.catalogue.catalogueSchemas.valuesArray().some((s: any) => hasMerge(s.schema, name));
  const renamesATable = () =>
    node.catalogue.catalogueLenses
      .valuesArray()
      .some((lens: any) => lens.tableLenses.some((tl: any) => tl.sourceTable !== tl.targetTable));
  return anySchema(table) || (renamesATable() && anySchema(undefined));
}

const lensTouches = (op: any, name: string) =>
  op.kind === "Rename" || op.kind === "Copy"
    ? op.from === name || op.to === name
    : op.column === name;

/** A merge column of the row's current image (another schema version) that a write under `schemaVersion` would not carry. */
function imageMergeColumnNotCarried(
  node: NodeState,
  schemaVersion: Uuid,
  tableSchema: any,
  current: VersionRow,
): string | undefined {
  const imageSchema = node.schemaVersionForAlias(current.schemaVersionAlias());
  if (!imageSchema) throw invalid("global version schema alias must exist");
  const imageTable = node.tableInSchema(current.table, imageSchema);
  const imageMergeColumns = imageTable.columns.filter(
    (c: any) => imageTable.mergeStrategy(c.name) !== "Lww",
  );
  if (imageMergeColumns.length === 0) return undefined;
  const path = node.compiledLensPath(imageSchema, schemaVersion, current.table);
  if (!path || path.targetTable !== tableSchema.name) return imageMergeColumns[0].name;
  for (const column of imageMergeColumns) {
    const name: string = column.name;
    const touched = path.ops.some((op: any) => lensTouches(op, name));
    const carried =
      !touched &&
      tableSchema.columns.some(
        (c: any) =>
          c.name === name &&
          valueTypeEquals(c.columnType, column.columnType) &&
          tableSchema.mergeStrategy(name) === imageTable.mergeStrategy(name),
      );
    if (!carried) return name;
  }
  return undefined;
}

type SettledCells = { kind: "Ok"; cells: [number, Value][] } | { kind: "Err"; column: string };

/**
 * The row's settled values of the merge columns a write across schema versions did not author, as
 * `[history field, value]` in the write's layout; `Err(column)` when a value cannot be carried without guessing.
 */
function crossSchemaSettledMergeCells(
  node: NodeState,
  schemaVersion: Uuid,
  tableSchema: any,
  authored: SortedSet<string> | undefined,
  previous: VersionRow,
): SettledCells {
  const unauthored: [number, any][] = [];
  tableSchema.columns.forEach((column: any, index: number) => {
    if (tableSchema.mergeStrategy(column.name) !== "Lww" && authored && !authored.has(column.name))
      unauthored.push([index, column]);
  });
  if (unauthored.length === 0) return { kind: "Ok", cells: [] };
  const first = unauthored[0][1].name;
  const previousSchema = node.schemaVersionForAlias(previous.schemaVersionAlias());
  if (!previousSchema) return { kind: "Err", column: first };
  const previousTable = node.tableInSchema(previous.table, previousSchema);
  const path = node.compiledLensPath(previousSchema, schemaVersion, previous.table);
  if (!path || path.targetTable !== tableSchema.name) return { kind: "Err", column: first };
  const previousCells: Map<string, Value> = previous.cells(previousTable);
  const translated = new Map(previousCells);
  applyCompiledLensPath(path, translated);
  const cells: [number, Value][] = [];
  for (const [index, column] of unauthored) {
    const name: string = column.name;
    let touched = false;
    let mappedFromAnotherColumn = false;
    for (const op of path.ops) {
      if (!lensTouches(op, name)) continue;
      touched = true;
      if (op.kind === "Rename" || op.kind === "Copy") mappedFromAnotherColumn = true;
    }
    const sameColumn = previousTable.columns.find((c: any) => c.name === name);
    let value: Value | undefined;
    if (
      sameColumn &&
      !touched &&
      valueTypeEquals(sameColumn.columnType, column.columnType) &&
      previousTable.mergeStrategy(name) === tableSchema.mergeStrategy(name)
    )
      value = previousCells.get(name);
    else if (!sameColumn && !mappedFromAnotherColumn) value = translated.get(name);
    else return { kind: "Err", column: name };
    // History cells are stored nullable.
    cells.push([HistoryRowRecord.USER_CELLS + index, V.nullable(value)]);
  }
  return { kind: "Ok", cells };
}

export async function globalCurrentSeqInBatch(
  node: NodeState,
  batch: DatabaseBatch,
  schemaVersion: Uuid,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
): Promise<bigint | undefined> {
  return (
    await globalCurrentSeqAndWriterInBatch(node, batch, schemaVersion, table, branchKey, rowUuid)
  )?.[0];
}

/** The row's current accepted seq and the transaction that wrote its current image, from its global-current record. */
export async function globalCurrentSeqAndWriterInBatch(
  node: NodeState,
  batch: DatabaseBatch,
  schemaVersion: Uuid,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
): Promise<[bigint | undefined, TxId | undefined] | undefined> {
  const currentTable = physicalCurrentTableForSchema(node, schemaVersion, table, "Global");
  const raw = await node.database.primaryKeyGetRawInBatch(
    batch,
    currentTable,
    rowPk(branchKey, rowUuid),
  );
  if (!raw) return undefined;
  const record = raw.record();
  // `global_time` is a nullable field: read it as one.
  const seq: bigint | undefined = record.getNullableU64(
    GlobalCurrentRowRecord.FIELD_GLOBAL_TIME_IDX,
  );
  const txTime: bigint = record.getU64(GlobalCurrentRowRecord.FIELD_TX_TIME_IDX);
  const nodeUuid = node.nodeForAlias(
    Number(record.getU64(GlobalCurrentRowRecord.FIELD_TX_NODE_ID_IDX)),
  );
  return [seq, nodeUuid ? { time: txTime, node: nodeUuid } : undefined];
}

async function queryGlobalWinnerInBatch(
  node: NodeState,
  batch: DatabaseBatch,
  schemaVersion: Uuid,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
): Promise<VersionRow | undefined> {
  return (
    await queryGlobalWinnerWithSeqInBatch(node, batch, schemaVersion, table, branchKey, rowUuid)
  )?.[0];
}

/** The row's global post-image together with the seq it is the row at. */
async function queryGlobalWinnerWithSeqInBatch(
  node: NodeState,
  batch: DatabaseBatch,
  schemaVersion: Uuid,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
): Promise<[VersionRow, bigint | undefined] | undefined> {
  const currentTable = physicalCurrentTableForSchema(node, schemaVersion, table, "Global");
  const raw = await node.database.primaryKeyGetRawInBatch(
    batch,
    currentTable,
    rowPk(branchKey, rowUuid),
  );
  if (!raw) return undefined;
  const current = raw.ownedRecord();
  const seq: bigint | undefined = current.getNullableU64(
    GlobalCurrentRowRecord.FIELD_GLOBAL_TIME_IDX,
  );
  const winner = node.historyImageFromCurrentRecord(
    schemaVersion,
    table,
    raw.variantTag(),
    current,
  );
  if (winner) return [winner, seq];
  const txTime: bigint = current.getU64(GlobalCurrentRowRecord.FIELD_TX_TIME_IDX);
  const txNodeAlias = Number(current.getU64(GlobalCurrentRowRecord.FIELD_TX_NODE_ID_IDX));
  const version = await queryVersionByAliasInBatch(
    node,
    batch,
    table,
    branchKey,
    rowUuid,
    txTime,
    txNodeAlias,
  );
  return version ? [version, seq] : undefined;
}

async function queryVersionByAliasInBatch(
  node: NodeState,
  batch: DatabaseBatch,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
  txTime: bigint,
  txNodeAlias: number,
): Promise<VersionRow | undefined> {
  const seqs = historySeqCandidates(await node.historyTxSeq(batch, txTime, txNodeAlias));
  for (const storageTable of node.versionStorageSources(table) as string[]) {
    let record;
    for (const seq of seqs) {
      const raw = await node.database.primaryKeyGetRawInBatch(
        batch,
        storageTable,
        historyKeyValues(branchKey.canonicalBytes(), rowUuid, seq, txTime, txNodeAlias),
      );
      if ((record = raw?.ownedRecord())) break;
    }
    if (!record) continue;
    return node.decodeStoredHistoryRecord(batch, table, storageTable, record);
  }
  return undefined;
}

export function writeHistoryPostImage(
  node: NodeState,
  batch: DatabaseBatch,
  version: VersionRow,
  txAuthor: any,
): void {
  const [historyTable, record] = versionStorageWriteBinding(node, version, txAuthor);
  // An accepted write's pending record, if any, moves to its seq.
  if (version.seq() !== 0n)
    batch.delete(historyTable, versionStoragePrimaryKey(node, version.withSeq(0n)));
  batch.updateRaw(historyTable, versionStoragePrimaryKey(node, version), record);
}

function versionSchema(node: NodeState, version: VersionRow): Uuid {
  const schemaVersion = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (!schemaVersion) throw invalid("unknown schema version alias");
  return schemaVersion;
}

export function writeGlobalCurrentUpdate(
  node: NodeState,
  batch: DatabaseBatch,
  version: VersionRow,
  globalTime: bigint,
): void {
  const plan = preparedPhysicalWritePlan(
    node,
    versionSchema(node, version),
    version.table,
    "GlobalCurrent",
  );
  // Validate node-local authored column aliases before deriving the current carrier.
  authoredColumnsForVersion(node, version);
  const physical = encodePhysicalVersionRecord(plan, version, globalTime, undefined);
  batch.updateRaw(
    plan.storageTable,
    globalCurrentPrimaryKey(version.branchKey, version.rowUuid()),
    physical,
  );
}

/**
 * The client overlay holds one row per row: the newest pending local image, already the fold of every pending
 * patch for that row. Replays and older images are ignored.
 */
export function writeAheadCurrentInsert(
  node: NodeState,
  batch: DatabaseBatch,
  version: VersionRow,
): void {
  const overlayKey = aheadOverlayKey(node, version);
  const txId = node.versionTxId(version);
  const existing: TxId | undefined = node.aheadCurrentKeys.get(overlayKey);
  if (existing && cmpTxId(existing, txId) >= 0) return;
  const plan = preparedPhysicalWritePlan(
    node,
    versionSchema(node, version),
    version.table,
    "AheadCurrent",
  );
  authoredColumnsForVersion(node, version);
  const physical = encodePhysicalVersionRecord(plan, version, undefined, undefined);
  batch.updateRaw(
    plan.storageTable,
    globalCurrentPrimaryKey(version.branchKey, version.rowUuid()),
    physical,
  );
  node.aheadCurrentKeys.set(overlayKey, txId);
}

/** Drop the overlay row when it holds this version's image; returns whether it was dropped. */
export function writeAheadCurrentDelete(
  node: NodeState,
  batch: DatabaseBatch,
  version: VersionRow,
): boolean {
  const schemaVersion = versionSchema(node, version);
  const overlayKey = aheadOverlayKey(node, version);
  const existing: TxId | undefined = node.aheadCurrentKeys.get(overlayKey);
  if (!existing || !txIdEquals(existing, node.versionTxId(version))) return false;
  batch.delete(
    physicalCurrentTableForSchema(node, schemaVersion, version.table, "Ahead"),
    globalCurrentPrimaryKey(version.branchKey, version.rowUuid()),
  );
  node.aheadCurrentKeys.delete(overlayKey);
  return true;
}

/** After a rejected image leaves the overlay, the newest remaining pending image for the row takes its place. */
export async function restoreAheadOverlayAfterReject(
  node: NodeState,
  batch: DatabaseBatch,
  rejected: VersionRow,
): Promise<void> {
  await recomputeAheadOverlay(
    node,
    batch,
    rejected,
    new SortedSet<TxId>(cmpTxId, [node.versionTxId(rejected)]),
  );
}

/** Rebuild one row's ahead overlay: its synced image with the still-pending local patches folded on in tx order. */
export async function recomputeAheadOverlay(
  node: NodeState,
  batch: DatabaseBatch,
  row: VersionRow,
  settling: SortedSet<TxId>,
): Promise<void> {
  const pending: [TxId, VersionRow][] = [];
  for (const version of (await node.queryRowVersionsInBranch(
    row.table,
    row.branchKey,
    row.rowUuid(),
  )) as VersionRow[]) {
    const txId = node.versionTxId(version);
    if (settling.has(txId)) continue;
    const state = await node.queryTransactionState(txId);
    if (state && state[0].kind === "Pending" && state[1] === undefined)
      pending.push([txId, version]);
  }
  pending.sort((a, b) => cmpTxId(a[0], b[0]));
  if (pending.length === 0) return;
  const first = pending[0][1];
  const schemaVersion = node.schemaVersionForAlias(first.schemaVersionAlias());
  if (!schemaVersion) throw invalid("pending version schema alias must exist");
  // A renamed table names the row differently in the pending patch's schema than in the settling version's.
  const tableSchema = node.tableInSchema(first.table, schemaVersion);
  let image = await queryGlobalWinnerInBatch(
    node,
    batch,
    schemaVersion,
    tableSchema.name,
    row.branchKey,
    row.rowUuid(),
  );
  for (const [, patch] of pending)
    image =
      image && image.schemaVersionAlias() === patch.schemaVersionAlias()
        ? foldPendingPatch(node, tableSchema, image, patch)
        : patch;
  if (image) {
    node.aheadCurrentKeys.delete(aheadOverlayKey(node, image));
    writeAheadCurrentInsert(node, batch, image);
  }
}

/** A new synced image rebases any other pending local patches on its row. */
export async function rebaseAheadOverlays(
  node: NodeState,
  batch: DatabaseBatch,
  synced: VersionRow[],
  settling: SortedSet<TxId>,
): Promise<void> {
  for (const version of synced)
    if (hasForeignAheadOverlay(node, version, node.versionTxId(version)))
      await recomputeAheadOverlay(node, batch, version, settling);
}

export function aheadOverlayKey(node: NodeState, version: VersionRow): string {
  const tableId = physicalTableIdForSchema(node, versionSchema(node, version), version.table);
  return `${tableId}\0${Array.from(PK.intoBytes(globalCurrentPrimaryKey(version.branchKey, version.rowUuid()))).join(",")}`;
}

/** Whether a pending overlay row other than `txId`'s own image covers this row. */
export function hasForeignAheadOverlay(node: NodeState, version: VersionRow, txId: TxId): boolean {
  const overlay: TxId | undefined = node.aheadCurrentKeys.get(aheadOverlayKey(node, version));
  return !!overlay && !txIdEquals(overlay, txId);
}

/** Apply one pending patch over an image of the same layout: authored plain columns replace, merge columns apply. */
function foldPendingPatch(
  node: NodeState,
  tableSchema: any,
  base: VersionRow,
  patch: VersionRow,
): VersionRow {
  const authors = authorsIn(authoredColumnsForVersion(node, patch));
  const folded: Value[] = patch.record.toValues();
  const baseValues: Value[] = base.record.toValues();
  const counterSigns = patch.counterSigns();
  if (!authors(DELETION_COLUMN_NAME))
    folded[HistoryRowRecord.FIELD__DELETION_IDX] = baseValues[HistoryRowRecord.FIELD__DELETION_IDX];
  tableSchema.columns.forEach((column: any, position: number) => {
    const index = HistoryRowRecord.USER_CELLS + position;
    const strategy = tableSchema.mergeStrategy(column.name);
    if (!authors(column.name)) folded[index] = baseValues[index];
    else if (strategy !== "Lww")
      folded[index] = V.nullable(
        mergeOps.applyPendingMergeOp(
          strategy,
          column.columnType,
          baseValues[index],
          folded[index],
          mergeOps.counterSign(tableSchema, counterSigns, position),
        ),
      );
  });
  for (const index of [
    HistoryRowRecord.FIELD_CREATED_BY_IDX,
    HistoryRowRecord.FIELD_CREATED_AT_IDX,
  ])
    folded[index] = baseValues[index];
  mergeOps.clearCounterSigns(folded, patch.record.descriptor);
  return patch.withRecordValues(folded);
}

/** Drop the overlays a fated transaction owns, refolding other still-pending patches over the synced image. */
export async function cleanupFatedAheadCurrentForVersions(
  node: NodeState,
  batch: DatabaseBatch,
  versions: VersionRow[],
): Promise<void> {
  const settling = new SortedSet<TxId>(
    cmpTxId,
    versions.map((v) => node.versionTxId(v)),
  );
  for (const version of versions)
    if (writeAheadCurrentDelete(node, batch, version))
      await recomputeAheadOverlay(node, batch, version, settling);
}

export async function pruneAheadCurrentForGlobalTime(
  node: NodeState,
  batch: DatabaseBatch,
  globalTime: bigint,
): Promise<void> {
  const txIds: TxId[] = [];
  for (const raw of await node.database.indexScanRaw("jazz_transactions", "by_global_time", [
    V.u64(globalTime),
  ])) {
    const record = raw.record();
    const nodeUuid = node.nodeForAlias(
      Number(record.getU64(TransactionRowRecord.FIELD_NODE_ID_IDX)),
    );
    if (!nodeUuid) throw invalid("transaction node alias must exist");
    txIds.push({ time: record.getU64(TransactionRowRecord.FIELD_TIME_IDX), node: nodeUuid });
  }
  for (const txId of txIds)
    for (const version of await node.queryVersionsForTx(txId))
      writeAheadCurrentDelete(node, batch, version);
}
