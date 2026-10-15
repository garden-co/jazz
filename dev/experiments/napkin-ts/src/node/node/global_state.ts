// Read-only derivations over the settled global layer.
import { SortedMap, uuidV4 } from "../../support/index.js";
import {
  type GlobalTime,
  type RowUuid,
  type SchemaVersionId,
  type TxId,
  BranchKeyDefault,
  GLOBAL_CURRENT_BY_SEQ_INDEX,
  GlobalCurrentRowRecord,
  PhysicalCurrentClass,
  TxIdNew,
  deletionEventFromValue,
  physicalGlobalCurrentTableName,
  cmpRowUuid,
} from "../../_extern/node-state.js";
import { type NodeState, invalidStored } from "./index.js";

const U64_MAX = 0xffffffffffffffffn;

function txOfCurrentRecord(node: NodeState, record: any): TxId | undefined {
  const time = record.getU64(GlobalCurrentRowRecord.FIELD_TX_TIME_IDX);
  const node_ = node.nodeForAlias(record.getU64(GlobalCurrentRowRecord.FIELD_TX_NODE_ID_IDX));
  return node_ === undefined ? undefined : TxIdNew(time, node_);
}

export const globalStateMethods = {
  /** The global winner for a physical row lineage. */
  async visibleGlobalTxIdForPhysicalTableNow(
    this: NodeState,
    tableId: any,
    rowUuid: RowUuid,
  ): Promise<TxId | undefined> {
    try {
      const raw = await this.database.primaryKeyGetRaw(physicalGlobalCurrentTableName(tableId), [
        { kind: "Bytes", value: BranchKeyDefault().canonicalBytes() },
        { kind: "Uuid", value: rowUuid },
      ]);
      return raw ? txOfCurrentRecord(this, raw.record()) : undefined;
    } catch {
      return undefined;
    }
  },

  /** Global-current records of `table` whose latest accepted seq is after `position`. */
  async globalCurrentRecordsAfter(
    this: NodeState,
    table: string,
    position: GlobalTime,
  ): Promise<any[]> {
    const tableId = this.physicalTableIdForSchema(this.catalogue.localSchemaVersionId, table);
    const branch = { kind: "Bytes", value: BranchKeyDefault().canonicalBytes() };
    const raws = await this.database.indexScanRangeRaw(
      physicalGlobalCurrentTableName(tableId),
      GLOBAL_CURRENT_BY_SEQ_INDEX,
      [branch, { kind: "Nullable", value: { kind: "U64", value: BigInt(position) + 1n } }],
      [branch, { kind: "Nullable", value: { kind: "U64", value: U64_MAX } }],
    );
    return raws.map((raw: any) => raw.ownedRecord());
  },

  async globalRowsChangedAfter(
    this: NodeState,
    table: string,
    position: GlobalTime,
  ): Promise<RowUuid[]> {
    const rows = new SortedMap<RowUuid, true>(cmpRowUuid);
    for (const record of await this.globalCurrentRecordsAfter(table, position))
      rows.set(record.borrowed().getUuid(GlobalCurrentRowRecord.FIELD_ROW_UUID_IDX), true);
    return [...rows.keysArray()];
  },

  /** Answer "Q at W" for a single-table supporting set from the `by_seq` index. */
  async supportingCatchUpAfter(
    this: NodeState,
    table: string,
    physicalTable: any,
    current: Iterable<any>,
    position: GlobalTime,
    predecessor: Uint8Array,
  ): Promise<any> {
    const moved = new SortedMap<RowUuid, TxId>(cmpRowUuid);
    for (const owned of await this.globalCurrentRecordsAfter(table, position)) {
      const record = owned.borrowed();
      const tx = txOfCurrentRecord(this, record);
      if (!tx) throw invalidStored("global current node alias must exist");
      moved.set(record.getUuid(GlobalCurrentRowRecord.FIELD_ROW_UUID_IDX), tx);
    }
    const changed: any[] = [];
    for (const row of current)
      if (
        String(row.physicalTable) === String(physicalTable) &&
        moved.delete(row.row) !== undefined
      )
        changed.push(row);
    const branch = BranchKeyDefault().canonicalBytes();
    const left = [...moved].map(([row, tx]) => ({
      physicalTable,
      versionTable: table,
      row,
      version: {
        tx,
        schemaVersion: undefined,
        layer: "Content",
        batch: tx,
        branchOrPrefix: branch.length ? branch : undefined,
        rowDigest: undefined,
      },
    }));
    return { kind: "CatchUp", predecessor, revision: uuidV4(), changed, left };
  },

  /** The transaction whose row image is globally visible, with its deletion state. */
  async visibleGlobalCurrentNow(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ): Promise<[TxId, boolean] | undefined> {
    try {
      const currentTable = this.physicalCurrentTableForSchema(
        schemaVersion,
        table,
        PhysicalCurrentClass.Global,
      );
      const raw = await this.database.primaryKeyGetRaw(currentTable, [
        { kind: "Bytes", value: BranchKeyDefault().canonicalBytes() },
        { kind: "Uuid", value: rowUuid },
      ]);
      if (!raw) return undefined;
      const record = raw.record();
      const tag = record.getNullableEnum(GlobalCurrentRowRecord.FIELD__DELETION_IDX);
      let deleted = false;
      if (tag !== undefined) {
        try {
          deleted = deletionEventFromValue({ kind: "EnumTag", value: tag }) === "Deleted";
        } catch {
          deleted = false;
        }
      }
      const tx = txOfCurrentRecord(this, record);
      return tx ? [tx, deleted] : undefined;
    } catch {
      return undefined;
    }
  },

  async visibleGlobalRowTxIdNow(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ) {
    return (await this.visibleGlobalCurrentNow(schemaVersion, table, rowUuid))?.[0];
  },

  async visibleGlobalContentTxIdInSchemaNow(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ): Promise<TxId | undefined> {
    const current = await this.visibleGlobalCurrentNow(schemaVersion, table, rowUuid);
    return current && !current[1] ? current[0] : undefined;
  },

  /** Apply an accepted transaction's writes to global current, merging per column. */
  async globalCurrentUpdatesForVersions(
    this: NodeState,
    batch: any,
    txId: TxId,
    globalTime: GlobalTime,
    versions: any[],
  ): Promise<any[]> {
    const updates = new Map<string, any>();
    for (const version of versions) {
      const authoredSchema = this.schemaVersionForAlias(version.schemaVersionAlias());
      if (authoredSchema === undefined)
        throw invalidStored("global version schema alias must exist");
      const tableSchema = this.tableInSchema(version.table(), authoredSchema);
      const merged = await this.mergedGlobalPostImage(
        batch,
        authoredSchema,
        tableSchema,
        version,
        txId,
        globalTime,
        this.mintingGlobalTime,
      );
      if (merged !== undefined)
        updates.set(
          `${version.table()}|${version.branchKey().toString()}|${String(version.rowUuid())}`,
          merged,
        );
    }
    return [...updates.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([, v]) => v);
  },
};
