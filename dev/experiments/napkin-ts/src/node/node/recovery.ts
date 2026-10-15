// Startup recovery and durable-state rehydration from groove storage.
import { SortedSet, cmpNum } from "../../support/index.js";
import {
  type NodeAlias,
  type TxId,
  NodeAliasRowRecord,
  RejectedTransactionNew,
  RejectedTransactionRowRecord,
  RejectedVersionNew,
  RejectedVersionRowRecord,
  TransactionRowRecord,
  TxIdNew,
  cmpTxId,
  cmpRowUuid,
  durabilityFromDiscriminant,
  fateFromEncodedFields,
  physicalRejectedVersionsTableName,
  OwnedRecord,
} from "../../_extern/node-state.js";
import { type NodeState, invalidStored } from "./index.js";

const U64_MAX = 0xffffffffffffffffn;

export const recoveryMethods = {
  async rejectedVersionsFor(this: NodeState, alias: NodeAlias, txId: TxId): Promise<any[]> {
    const versions: any[] = [];
    for (const tableId of this.physicalTableIds()) {
      const storageTable = physicalRejectedVersionsTableName(tableId);
      for (const raw of await this.database.primaryKeyScanRaw(storageTable, [
        { kind: "U64", value: txId.time },
        { kind: "U64", value: alias },
      ])) {
        const record = raw.record();
        if (
          record.getU64(RejectedVersionRowRecord.FIELD_TX_NODE_ID_IDX) !== alias ||
          record.getU64(RejectedVersionRowRecord.FIELD_TX_TIME_IDX) !== txId.time
        )
          continue;
        const schemaAlias = raw.variantTag();
        const schemaVersion = this.schemaVersionForAlias(schemaAlias);
        if (schemaVersion === undefined)
          throw invalidStored("rejected row schema version alias missing");
        const logicalTable = this.logicalTableForPhysicalAlias(tableId, schemaAlias);
        const descriptor = this.tableInSchemaRef(logicalTable, schemaVersion)
          .rejectedVersionsStorageTable()
          .recordSchema();
        versions.push(
          RejectedVersionNew(logicalTable, OwnedRecord.new(raw.raw().slice(), descriptor)),
        );
      }
    }
    versions.sort(
      (a, b) =>
        (a.table() < b.table() ? -1 : a.table() > b.table() ? 1 : 0) ||
        cmpRowUuid(a.rowUuid(), b.rowUuid()) ||
        Number(a.deletion() !== undefined) - Number(b.deletion() !== undefined),
    );
    return versions;
  },

  async recoverFromStorage(this: NodeState): Promise<void> {
    for (const raw of await this.database.primaryKeyScanRaw("jazz_nodes", [])) {
      const record = raw.record();
      const alias = record.getU64(NodeAliasRowRecord.FIELD_ID_IDX);
      const uuid = record.getUuid(NodeAliasRowRecord.FIELD_UUID_IDX);
      const existing = this.nodeAliases.get(uuid);
      if (existing !== undefined && existing !== alias)
        throw invalidStored("node UUID has conflicting durable aliases");
      const existingUuid = this.nodeAliases.nodeForAlias(alias);
      if (existingUuid !== undefined && String(existingUuid) !== String(uuid))
        throw invalidStored("node alias maps to multiple durable UUIDs");
      this.nodeAliases.insert(uuid, alias);
    }
    const aliasToNode = new Map([...this.nodeAliases.iter()].map(([node, alias]) => [alias, node]));

    // The newest stored transaction is the transaction-clock high-water mark.
    const last = await this.database.primaryKeyLastRaw("jazz_transactions", []);
    if (last) this.mergeTxTime(last.record().getU64(TransactionRowRecord.FIELD_TIME_IDX));

    // Range over only the `Some` bucket of the nullable global-time index.
    const first = { kind: "Nullable", value: { kind: "U64", value: 0 } };
    const lastGlobal = { kind: "Nullable", value: { kind: "U64", value: U64_MAX } };
    const sequenced = [
      ...(await this.database.indexScanRangeRaw(
        "jazz_transactions",
        "by_global_time",
        [first],
        [lastGlobal],
      )),
      ...(await this.database.indexScanRaw("jazz_transactions", "by_global_time", [lastGlobal])),
    ];
    const accepted: [number, TxId][] = [];
    for (const raw of sequenced) {
      const record = raw.record();
      const globalTime = record.getNullableU64(TransactionRowRecord.FIELD_GLOBAL_TIME_IDX);
      if (
        globalTime !== undefined &&
        durabilityFromDiscriminant(record.getEnum(TransactionRowRecord.FIELD_DURABILITY_IDX)) !==
          "Global"
      )
        throw invalidStored("global timestamp requires Global durability");
      if (fateFromEncodedFields(record).kind !== "Accepted" || globalTime === undefined) continue;
      const node = this.nodeForAlias(record.getU64(TransactionRowRecord.FIELD_NODE_ID_IDX));
      if (node === undefined) throw invalidStored("transaction node alias must exist");
      accepted.push([
        globalTime,
        TxIdNew(record.getU64(TransactionRowRecord.FIELD_TIME_IDX), node),
      ]);
    }
    accepted.sort((a, b) => a[0] - b[0] || cmpTxId(a[1], b[1]));
    let previous: [number, TxId] | undefined;
    for (const entry of accepted) {
      if (previous && previous[0] === entry[0] && cmpTxId(previous[1], entry[1]) === 0) continue;
      previous = entry;
      this.recordAppliedGlobalTime(entry[0], entry[1]);
    }
    if (this.historyComplete) {
      this.clock.committedGlobalTime = this.clock.globalTimeRegister;
      this.clock.appliedGlobalTimesAfterFrontier = new SortedSet(cmpNum);
      this.clock.frontierDots.clear();
    }

    const rejectedHeaders: [NodeAlias, TxId, any][] = [];
    for (const raw of await this.database.primaryKeyScanRaw("jazz_rejected_transactions", [])) {
      const record = raw.record();
      const nodeAlias = record.getU64(RejectedTransactionRowRecord.FIELD_NODE_ID_IDX);
      const node = aliasToNode.get(nodeAlias);
      if (node === undefined) throw invalidStored("rejected transaction node alias must exist");
      if (String(node) !== String(this.nodeUuidValue)) continue;
      const txId = TxIdNew(record.getU64(RejectedTransactionRowRecord.FIELD_TIME_IDX), node);
      rejectedHeaders.push([
        nodeAlias,
        txId,
        OwnedRecord.new(raw.raw().slice(), record.descriptor()),
      ]);
    }
    for (const [nodeAlias, txId, record] of rejectedHeaders) {
      const versions = await this.rejectedVersionsFor(nodeAlias, txId);
      this.rejections.rejectedTransactions.set(
        txId,
        RejectedTransactionNew(txId, record, versions),
      );
    }
  },
};
