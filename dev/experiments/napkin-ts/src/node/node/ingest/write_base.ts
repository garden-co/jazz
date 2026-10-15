// Write-base validation at the authority, late-write refusal, and superseded-writes markers.
import { SortedMap, cmpNum } from "../../../support/collections.js";
import { equalBytes } from "../../../support/bytes.js";
import {
  nodeError,
  V,
  VT,
  SUPERSEDED_WRITES_STORE,
  type NodeState,
  type TxId,
  type DatabaseBatch,
  type Uuid,
  type Value,
  type ValueType,
} from "../../../_extern/node-physical-ingest.js";
import {
  type VersionRow,
  type RowBase,
  HistoryRowRecord,
  DELETION_COLUMN_ID,
  nullableValue,
} from "../codec.js";
import { physicalHistoryTableName, physicalTableIdForSchema } from "../physical.js";

/** Plain cells of an image keyed by physical column id: each cell's type and value. */
export type PhysicalCells = SortedMap<number, [ValueType, Value | undefined]>;

/** The `_deletion` cell's type: the same in every layout. */
export const deletionCellType = (): ValueType => VT.nullable({ kind: "Bool" });

const baseIsEmpty = (base: RowBase) => base.seq === undefined && base.pending === undefined;
const uuidText = (u: Uuid) => Array.from(u, (b) => b.toString(16).padStart(2, "0")).join("");

/**
 * Why Core cannot chain a write over the pending predecessor its base names, or `undefined` when it can: the
 * predecessor is an older transaction of the writer's own node (`INV-HIST-20`).
 */
export function pendingPredecessorRefusal(predecessor: TxId, writer: TxId): string | undefined {
  if (!equalBytes(predecessor.node, writer.node))
    return "its pending predecessor was written by another node";
  if (predecessor.time >= writer.time) return "its pending predecessor is not older than the write";
  return undefined;
}

/**
 * Check the base of a write by `writer` to a row whose current image is at `currentSeq` (SPEC 4 §4.6). Returns the
 * refusal reason: the authority accepts a base it can name exactly or not at all (`INV-HIST-20`).
 */
export async function validateWriteBase(
  node: NodeState,
  batch: DatabaseBatch,
  schemaVersion: Uuid,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
  base: RowBase,
  writer: TxId,
  currentSeq: bigint | undefined,
): Promise<string | undefined> {
  if (baseIsEmpty(base)) return undefined;
  const notSupported = (detail: string) =>
    `the base of the write to '${table}' row ${uuidText(rowUuid)} cannot be resolved: ${detail}; writes over such a base are not supported yet`;
  // Seq 0 is where history keeps writes without an accepted fate.
  if (base.seq === 0n) return notSupported("base seq 0 names no accepted write");
  if (base.seq !== undefined && (currentSeq === undefined || base.seq > currentSeq))
    return notSupported(`base seq ${base.seq} is above the row's current seq`);
  if (base.pending) {
    const refusal = pendingPredecessorRefusal(base.pending, writer);
    if (refusal !== undefined) return notSupported(refusal);
    // An undecided predecessor never arrives: the write applies over the row's current image (`INV-HIST-20`).
  }
  const seq = base.seq;
  if (seq === undefined || seq === currentSeq) return undefined;
  // The base seq must name an accepted write of this row.
  const historyTable = physicalHistoryTableName(
    physicalTableIdForSchema(node, schemaVersion, table),
  );
  const found: any[] = await node.database.primaryKeyScanRawInBatch(batch, historyTable, [
    V.bytes(branchKey.canonicalBytes()),
    V.uuid(rowUuid),
    V.u64(seq),
  ]);
  if (found.length === 0) return notSupported(`no accepted write of the row is at base seq ${seq}`);
  return undefined;
}

/**
 * Why Core refuses `writer`'s write as arriving after a later write of its own node Core already accepted
 * (`INV-HIST-23`). Reads are bounded by the writes accepted since the writer's base.
 */
export async function lateWriteRefusal(
  node: NodeState,
  batch: DatabaseBatch,
  schemaVersion: Uuid,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
  base: RowBase,
  writer: TxId,
  currentSeq: bigint | undefined,
  currentWriter: TxId | undefined,
): Promise<string | undefined> {
  const late = `the write to '${table}' row ${uuidText(rowUuid)} arrived after a later write of its writer; a writer's writes must reach the authority in the order they were written`;
  // No accepted write of the row.
  if (currentSeq === undefined) return undefined;
  // The writer's newest accepted write of the row is the current image's.
  if (currentWriter && equalBytes(currentWriter.node, writer.node))
    return currentWriter.time > writer.time ? late : undefined;
  let isLate: boolean;
  if (base.seq !== undefined) {
    // Nothing accepted after the seen image, or a base above current seq (refused by `validateWriteBase`).
    isLate =
      base.seq >= currentSeq
        ? false
        : await ownWriteAcceptedBetween(
            node,
            batch,
            table,
            branchKey,
            rowUuid,
            writer,
            base.seq,
            currentSeq,
          );
  } else {
    const through = await supersededThrough(
      node,
      physicalTableIdForSchema(node, schemaVersion, table),
      branchKey,
      rowUuid,
      writer.node,
    );
    isLate = through !== undefined && writer.time <= through;
  }
  return isLate ? late : undefined;
}

/** Whether the row holds an accepted record of `writer`'s node newer than `writer` at a seq in `after+1..=through`. */
async function ownWriteAcceptedBetween(
  node: NodeState,
  batch: DatabaseBatch,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
  writer: TxId,
  after: bigint,
  through: bigint,
): Promise<boolean> {
  const bound = (seq: bigint) => [
    V.bytes(branchKey.canonicalBytes()),
    V.uuid(rowUuid),
    V.u64(seq),
    V.u64(0),
    V.u64(0),
  ];
  const [start, end] = [bound(after + 1n), bound(through + 1n)];
  for (const historyTable of node.versionStorageSources(table) as string[])
    for (const raw of await node.database.primaryKeyScanRangeRawInBatch(
      batch,
      historyTable,
      start,
      end,
    )) {
      const record = raw.record();
      if (record.getU64(HistoryRowRecord.FIELD_TX_TIME_IDX) <= writer.time) continue;
      const writerNode = node.nodeForAlias(
        Number(record.getU64(HistoryRowRecord.FIELD_TX_NODE_ID_IDX)),
      );
      if (writerNode && equalBytes(writerNode, writer.node)) return true;
    }
  return false;
}

/** The superseded-writes marker key of `node` for a row, naming the table by its rename-stable physical id. */
const supersededWritesKey = (
  table: number,
  branchKey: any,
  rowUuid: Uuid,
  writerNode: Uuid,
): Value[] => [
  V.u64(table),
  V.bytes(branchKey.canonicalBytes()),
  V.uuid(rowUuid),
  V.uuid(writerNode),
];

/** The newest transaction time of `node`'s writes to the row that Core passed over: one point read. */
async function supersededThrough(
  node: NodeState,
  table: number,
  branchKey: any,
  rowUuid: Uuid,
  writerNode: Uuid,
): Promise<bigint | undefined> {
  const store = node.database.directRecordStore(SUPERSEDED_WRITES_STORE);
  const record = await store.get(supersededWritesKey(table, branchKey, rowUuid, writerNode));
  if (!record) return undefined;
  const value = record.getIdx(0);
  if (value.kind !== "U64")
    throw nodeError("InvalidStoredValue", "superseded-writes marker is not a transaction time");
  return BigInt(value.value);
}

/**
 * The writes of `writer`'s node to a row that Core passes over by applying `writer`'s write: through its pending
 * predecessor's time, when Core holds no decision for that predecessor.
 */
export async function passedOverPredecessor(
  node: NodeState,
  base: RowBase,
  writer: TxId,
  currentWriter: TxId | undefined,
): Promise<bigint | undefined> {
  const pending = base.pending;
  if (!pending) return undefined;
  // The row's image is the predecessor's, or a newer write of its node accepted after it.
  if (
    currentWriter &&
    equalBytes(currentWriter.node, writer.node) &&
    currentWriter.time >= pending.time
  )
    return undefined;
  return undecidedPredecessor(node, pending, writer);
}

/** `pending`'s time when it is a well-formed predecessor of `writer` that Core holds no decision for. */
async function undecidedPredecessor(
  node: NodeState,
  pending: TxId,
  writer: TxId,
): Promise<bigint | undefined> {
  if (pendingPredecessorRefusal(pending, writer) !== undefined) return undefined;
  const stored = await node.queryTransaction(pending);
  return !stored || stored.fate.kind === "Pending" ? pending.time : undefined;
}

/** Record the marker a refused write leaves for each row whose pending predecessor Core holds no decision for. */
export async function recordSupersededByRefusal(
  node: NodeState,
  writer: TxId,
  predecessors: [Uuid, string, any, Uuid, TxId][],
): Promise<void> {
  for (const [schemaVersion, table, branchKey, rowUuid, pending] of predecessors) {
    let physicalTable: number;
    try {
      physicalTable = physicalTableIdForSchema(node, schemaVersion, table);
    } catch {
      continue;
    }
    const through = await undecidedPredecessor(node, pending, writer);
    if (through !== undefined)
      await recordSupersededWrites(node, physicalTable, branchKey, rowUuid, writer.node, through);
  }
}

/** Record that Core passed over `node`'s writes to a row through `through`. Markers only grow. */
export async function recordSupersededWrites(
  node: NodeState,
  table: number,
  branchKey: any,
  rowUuid: Uuid,
  writerNode: Uuid,
  through: bigint,
): Promise<void> {
  const recorded = await supersededThrough(node, table, branchKey, rowUuid, writerNode);
  if (recorded !== undefined && recorded >= through) return;
  await node.database
    .directRecordStore(SUPERSEDED_WRITES_STORE)
    .set(supersededWritesKey(table, branchKey, rowUuid, writerNode), [V.u64(through)]);
}

/** This node's newest write to the row that has no fate yet, as a writer's base names it (history keeps it at seq 0). */
export async function newestOwnPendingWriteInBatch(
  node: NodeState,
  batch: DatabaseBatch,
  table: string,
  branchKey: any,
  rowUuid: Uuid,
): Promise<TxId | undefined> {
  const ownAlias: number | undefined = node.selfNodeAlias;
  if (ownAlias === undefined) return undefined;
  const prefix = [V.bytes(branchKey.canonicalBytes()), V.uuid(rowUuid), V.u64(0)];
  let newest: bigint | undefined;
  for (const historyTable of node.versionStorageSources(table) as string[])
    for (const raw of await node.database.primaryKeyScanRawInBatch(batch, historyTable, prefix)) {
      const record = raw.record();
      if (Number(record.getU64(HistoryRowRecord.FIELD_TX_NODE_ID_IDX)) !== ownAlias) continue;
      const time: bigint = record.getU64(HistoryRowRecord.FIELD_TX_TIME_IDX);
      if (newest === undefined || time > newest) newest = time;
    }
  return newest === undefined ? undefined : { time: newest, node: node.nodeUuid };
}

/** Every plain cell and `_deletion` of a row image, keyed by physical column id. */
export function imagePhysicalCells(node: NodeState, image: VersionRow): PhysicalCells {
  const schemaVersion = node.schemaVersionForAlias(image.schemaVersionAlias());
  if (!schemaVersion)
    throw nodeError("InvalidStoredValue", "history schema version alias must exist");
  const table = node.tableInSchemaRef(image.table, schemaVersion);
  const mapping = node.catalogue.physicalMappings
    .get(schemaVersion)
    ?.tables.get(image.table)?.columns;
  if (!mapping) throw nodeError("InvalidStoredValue", "history physical table mapping missing");
  const record = image.record;
  const cells: PhysicalCells = new SortedMap(cmpNum);
  table.columns.forEach((column: any, index: number) => {
    if (table.mergeStrategy(column.name) !== "Lww") return;
    const id = mapping.get(column.name);
    if (id === undefined)
      throw nodeError("InvalidStoredValue", "history column physical mapping missing");
    cells.set(id, [
      column.columnType,
      nullableValue(record.getIdx(HistoryRowRecord.USER_CELLS + index)),
    ]);
  });
  cells.set(DELETION_COLUMN_ID, [
    deletionCellType(),
    nullableValue(record.getIdx(HistoryRowRecord.FIELD__DELETION_IDX)),
  ]);
  return cells;
}
