import { type Bytes, toHex } from "../../support/bytes.js";
import type { Value, VariantRecord } from "../../_extern/groove-db-storage.js";
import { StagedWriteOverlay, StagedWriteState } from "../storage/index.js";
import { type Database, DbError } from "./index.js";
import { encodePrimaryKeyPart } from "./encoding.js";
import { MeteredStorage, type PendingTableWrite, storedRecord } from "./storage_helpers.js";

/** When subscription notifications produced by a batch become observable. */
export type NotificationTiming = "Immediate" | "AfterPersistence";

/** Outcome of a batch-scoped immutable-record insertion. */
export type EnsureExactOutcome = "Inserted" | "AlreadyIdentical" | "Conflict";

/** Logical or already-encoded row supplied to an ordinary table write. */
export type RecordInput =
  | { kind: "Values"; values: Value[] }
  | { kind: "Record"; record: VariantRecord };

/** Encoded row supplied with an explicit primary key. */
export type RawRecordInput =
  | { kind: "Payload"; payload: Bytes }
  | { kind: "Record"; record: VariantRecord }
  | { kind: "ValidatedRecord"; record: any };

export const recordInput = (r: Value[] | VariantRecord): RecordInput =>
  Array.isArray(r) ? { kind: "Values", values: r } : { kind: "Record", record: r };
export const rawRecordInput = (r: Bytes | VariantRecord | RawRecordInput): RawRecordInput =>
  r instanceof Uint8Array
    ? { kind: "Payload", payload: r }
    : "kind" in r
      ? r
      : { kind: "Record", record: r };

export type BatchOperation =
  | { kind: "Insert" | "Update"; table: string; record: RecordInput }
  | {
      kind: "InsertRaw" | "InsertRawFresh" | "UpdateRaw";
      table: string;
      key: PrimaryKeyValue;
      record: RawRecordInput;
    }
  | { kind: "Delete"; table: string; key: PrimaryKeyValue };

export type PrimaryKeyValue =
  | { kind: "U8" | "U16" | "U32"; v: number }
  | { kind: "U64"; v: bigint }
  | { kind: "Bool"; v: boolean }
  | { kind: "String"; v: string }
  | { kind: "Bytes" | "Uuid"; v: Bytes }
  | { kind: "Composite"; v: PrimaryKeyValue[] };

/** Encode this logical primary key exactly as Groove stores it. */
export function primaryKeyValueBytes(key: PrimaryKeyValue): Bytes {
  const out: number[] = [];
  const go = (k: PrimaryKeyValue) => {
    if (k.kind === "Composite") k.v.forEach(go);
    else encodePrimaryKeyPart(out, k as Value);
  };
  go(key);
  return Uint8Array.from(out);
}

export interface PreparedBatchWrites {
  owner?: object;
  writes: PendingTableWrite[];
}

/** Mutable collection of table writes committed atomically at storage level. */
export class DatabaseBatch {
  operations: BatchOperation[] = [];
  exactBase?: [object, number];
  exactConflict = false;
  exactKeys = new Map<string, [string, Bytes, Bytes]>();
  txnOperations = new StagedWriteState();
  txnIndexedOperations = 0;
  // A prepared prefix belongs to one database/schema generation.
  prepared: PreparedBatchWrites = { writes: [] };
  notificationTiming: NotificationTiming = "Immediate";
  acceptedLargeValues: Bytes[] = [];
  /** Every table an operation of this batch writes. */
  writtenTables = new Set<string>();

  /** Stage an immutable record, or accept its identical existing bytes. */
  async ensureExact(
    database: Database,
    table: string,
    key: PrimaryKeyValue,
    record: Bytes | VariantRecord | RawRecordInput,
  ): Promise<EnsureExactOutcome> {
    database.ensureNotPoisoned();
    this.checkExactBase(database);
    this.exactBase = [database.immutableBatchOwner, database.nextPublicationId];
    const operation: BatchOperation = {
      kind: "InsertRawFresh",
      table,
      key,
      record: rawRecordInput(record),
    };
    const pending = database.pendingWriteFromOperation(operation);
    const expected = storedRecord(pending)!;
    database.ensureBatchStorageTxn(this);
    const overlay = new StagedWriteOverlay(database.residentStorage(), this.txnOperations);
    const storage = new MeteredStorage(overlay, database.storageReadMetrics);
    let comparison;
    try {
      comparison = await storage.compareValue(table, pending.key, expected);
    } catch (error) {
      throw DbError.from(error);
    }
    if (comparison !== "Different")
      this.exactKeys.set(`${table}\0${toHex(pending.key)}`, [table, pending.key, expected]);
    switch (comparison) {
      case "Absent":
        this.pushOperation(operation);
        // Keep the conflict check's preparation for overlay reads and commit.
        this.txnOperations.stage(database.ownedStorageOperationForPending(pending));
        this.prepared.writes.push(pending);
        this.txnIndexedOperations = this.operations.length;
        return "Inserted";
      case "Identical":
        return "AlreadyIdentical";
      case "Different":
        this.exactConflict = true;
        return "Conflict";
    }
    return "Conflict";
  }

  checkExactBase(database: Database) {
    if (this.exactConflict) throw new DbError("ImmutableBatchConflict");
    if (
      this.exactBase &&
      (this.exactBase[0] !== database.immutableBatchOwner ||
        this.exactBase[1] !== database.nextPublicationId)
    )
      throw new DbError("StaleImmutableBatch");
  }

  deliverNotifications(timing: NotificationTiming) {
    this.notificationTiming = timing;
  }

  /** Atomically consume a Groove staging root with this physical-record batch. */
  acceptLargeValue(id: Bytes) {
    const hex = toHex(id);
    if (!this.acceptedLargeValues.some((a) => toHex(a) === hex)) this.acceptedLargeValues.push(id);
  }

  insert(table: string, record: Value[] | VariantRecord) {
    this.pushOperation({ kind: "Insert", table, record: recordInput(record) });
  }
  insertRaw(table: string, key: PrimaryKeyValue, record: Bytes | VariantRecord | RawRecordInput) {
    this.pushOperation({ kind: "InsertRaw", table, key, record: rawRecordInput(record) });
  }
  /** Caller guarantees `key` is absent from storage and earlier batch operations. */
  insertRawFresh(
    table: string,
    key: PrimaryKeyValue,
    record: Bytes | VariantRecord | RawRecordInput,
  ) {
    this.pushOperation({ kind: "InsertRawFresh", table, key, record: rawRecordInput(record) });
  }
  update(table: string, record: Value[] | VariantRecord) {
    this.pushOperation({ kind: "Update", table, record: recordInput(record) });
  }
  updateRaw(table: string, key: PrimaryKeyValue, record: Bytes | VariantRecord | RawRecordInput) {
    this.pushOperation({ kind: "UpdateRaw", table, key, record: rawRecordInput(record) });
  }
  delete(table: string, key: PrimaryKeyValue) {
    this.pushOperation({ kind: "Delete", table, key });
  }
  isEmpty() {
    return this.operations.length === 0;
  }
  pushOperation(operation: BatchOperation) {
    this.writtenTables.add(operation.table);
    this.operations.push(operation);
  }
  writesTable(table: string) {
    return this.writtenTables.has(table);
  }
}
