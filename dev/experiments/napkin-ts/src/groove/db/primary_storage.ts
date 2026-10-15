import { type Bytes, equalBytes } from "../../support/bytes.js";
import {
  type Value,
  type TableSchema,
  type IndexSchema,
  type VariantRecord,
  durableIndexKeyPrefix,
  encodeKeyPart,
} from "../../_extern/groove-db-storage.js";
import {
  type KeyValue,
  type OrderedKvStorage,
  RecordStore,
  StagedWriteOverlay,
  scanPrefix,
} from "../storage/index.js";
import { type Database, DbError } from "./index.js";
import type { DatabaseBatch } from "./batch.js";
import {
  encodeIndexPrefixPart,
  encodePrimaryKeyPart,
  ensurePrimaryKeyValueType,
  indexRecordDescriptor,
  persistedIndexPrimaryKey,
} from "./encoding.js";
import { type EncodedKeyValue, MeteredStorage } from "./storage_helpers.js";
import { dbTry } from "./commit.js";

/** How many key columns a probe must supply: all ("Exact") or a leading run ("Prefix"). */
type KeyArity = "Exact" | "Prefix";
const admits = (arity: KeyArity, expected: number, actual: number) =>
  arity === "Exact" ? actual === expected : actual <= expected;

function checkIndex(arity: KeyArity, index: string, expected: number, key: Value[]) {
  if (!admits(arity, expected, key.length))
    throw new DbError("IndexKeyArity", { index, expected, actual: key.length });
}

function encodePrimaryKey(
  arity: KeyArity,
  table: string,
  schema: TableSchema,
  values: Value[],
): Bytes {
  const columns = schema.primaryKey!.columns;
  if (!admits(arity, columns.length, values.length))
    throw new DbError("PrimaryKeyArity", {
      table,
      expected: columns.length,
      actual: values.length,
    });
  const out: number[] = [];
  values.forEach((value, i) => {
    ensurePrimaryKeyValueType(schema, columns[i], value);
    encodePrimaryKeyPart(out, value);
  });
  return Uint8Array.from(out);
}

const variantParts = (entries: EncodedKeyValue[]) => entries.map((e) => e.variantRecord);

export class PrimaryStorageMethods {
  primaryKeySchema(this: Database, table: string): TableSchema {
    const schema = this.table(table);
    if (!schema.primaryKey) throw new DbError("MissingPrimaryKey", table);
    return schema;
  }

  meteredResident(this: Database, batch?: DatabaseBatch): MeteredStorage {
    const resident = this.residentStorage();
    const base = batch ? new StagedWriteOverlay(resident, batch.txnOperations) : resident;
    return new MeteredStorage(base, this.storageReadMetrics);
  }

  /** Whether this physical table contains any stored record in the applied resident state. */
  async tableHasStoredRows(this: Database, table: string): Promise<boolean> {
    this.table(table);
    const req = scanPrefix(table, new Uint8Array());
    req.maxItems = 1;
    const cursor = await dbTry(this.meteredResident().scan(req));
    // Backends may return an empty batch without having reached EOF.
    for (let rows; (rows = await dbTry(cursor.nextBatch())); ) if (rows.length) return true;
    return false;
  }

  /** How many entries of an index start with `prefix`, counting no further than `maxItems`. */
  async indexPrefixEntriesUpTo(
    this: Database,
    table: string,
    indexName: string,
    prefix: Value[],
    maxItems: number,
  ): Promise<number> {
    if (maxItems === 0) return 0;
    const index = this.index(table, indexName);
    if (prefix.length > index.columns.length)
      throw new DbError("IndexKeyArity", {
        index: indexName,
        expected: index.columns.length,
        actual: prefix.length,
      });
    const storagePrefix: number[] = [
      ...durableIndexKeyPrefix(this.ivmRuntime.indexStorageId(table, index)),
    ];
    for (const value of prefix) {
      try {
        encodeKeyPart(storagePrefix, value);
      } catch (error) {
        throw new DbError("IvmRuntime", error);
      }
    }
    const req = scanPrefix("indices", Uint8Array.from(storagePrefix));
    req.maxItems = maxItems;
    const cursor = await dbTry(this.meteredResident().scan(req));
    let entries = 0;
    for (let rows; entries < maxItems && (rows = await dbTry(cursor.nextBatch())); )
      entries += rows.length;
    return Math.min(entries, maxItems);
  }

  async indexGet(
    this: Database,
    table: string,
    indexName: string,
    key: Value[],
  ): Promise<VariantRecord[]> {
    checkIndex("Exact", indexName, this.index(table, indexName).columns.length, key);
    return this.indexScan(table, indexName, key);
  }

  async indexScan(
    this: Database,
    table: string,
    indexName: string,
    prefix: Value[],
  ): Promise<VariantRecord[]> {
    checkIndex("Prefix", indexName, this.index(table, indexName).columns.length, prefix);
    return this.decodeIndexRecords(
      table,
      indexName,
      await this.indexScanRaw(table, indexName, prefix),
    );
  }

  async indexScanRange(
    this: Database,
    table: string,
    indexName: string,
    start: Value[],
    end: Value[],
  ) {
    const n = this.index(table, indexName).columns.length;
    checkIndex("Prefix", indexName, n, start);
    checkIndex("Prefix", indexName, n, end);
    return this.decodeIndexRecords(
      table,
      indexName,
      await this.indexScanRangeRaw(table, indexName, start, end),
    );
  }

  decodeIndexRecords(
    this: Database,
    table: string,
    _indexName: string,
    raw: EncodedKeyValue[],
  ): VariantRecord[] {
    this.table(table);
    return variantParts(raw);
  }

  async primaryKeyScan(this: Database, table: string, prefix: Value[]): Promise<VariantRecord[]> {
    return this.primaryKeyScanWithStorage(this.meteredResident(), table, prefix);
  }

  async primaryKeyScanWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    table: string,
    prefix: Value[],
  ) {
    return variantParts(await this.primaryKeyScanRawWithStorage(storage, table, prefix));
  }

  primaryKeyScanRaw(this: Database, table: string, prefix: Value[]) {
    return this.primaryKeyScanRawWithStorage(this.meteredResident(), table, prefix);
  }

  primaryKeyScanRawInBatch(this: Database, batch: DatabaseBatch, table: string, prefix: Value[]) {
    this.ensureBatchStorageTxn(batch);
    return this.primaryKeyScanRawWithStorage(this.meteredResident(batch), table, prefix);
  }

  primaryKeyGetRaw(this: Database, table: string, key: Value[]) {
    return this.primaryKeyGetRawWithStorage(this.meteredResident(), table, key);
  }

  async primaryKeyGet(
    this: Database,
    table: string,
    key: Value[],
  ): Promise<VariantRecord | undefined> {
    return (await this.primaryKeyGetRaw(table, key))?.variantRecord;
  }

  /** Point read that also observes writes already staged in `batch`. */
  async primaryKeyGetRawInBatch(this: Database, batch: DatabaseBatch, table: string, key: Value[]) {
    // A table this batch never writes reads exactly as resident storage.
    const batchWritesTable = batch.writesTable(table);
    if (batchWritesTable) this.ensureBatchStorageTxn(batch);
    const schema = this.primaryKeySchema(table);
    const descriptor = this.tableStorageDescriptor(table);
    const encodedKey = encodePrimaryKey("Exact", table, schema, key);
    const stagedContainsKey =
      batchWritesTable && batch.txnOperations.containsKey(table, encodedKey);
    const storage = this.meteredResident(stagedContainsKey ? batch : undefined);
    const value = await dbTry(new RecordStore(storage, table, descriptor).getRaw(encodedKey));
    return value && this.decodeStoredKeyValue(schema, encodedKey, value);
  }

  async primaryKeyGetRawWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    table: string,
    key: Value[],
  ) {
    const schema = this.primaryKeySchema(table);
    const descriptor = this.tableStorageDescriptor(table);
    const encodedKey = encodePrimaryKey("Exact", table, schema, key);
    const value = await dbTry(new RecordStore(storage, table, descriptor).getRaw(encodedKey));
    return value && this.decodeStoredKeyValue(schema, encodedKey, value);
  }

  async primaryKeyScanRawWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    table: string,
    prefix: Value[],
  ) {
    const schema = this.primaryKeySchema(table);
    const descriptor = this.tableStorageDescriptor(table);
    const keyPrefix = encodePrimaryKey("Prefix", table, schema, prefix);
    const rows = await dbTry(new RecordStore(storage, table, descriptor).prefix(keyPrefix));
    return rows.map(([k, v]) => this.decodeStoredKeyValue(schema, k, v));
  }

  async primaryKeyLastRawWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    table: string,
    prefix: Value[],
  ) {
    const schema = this.primaryKeySchema(table);
    const descriptor = this.tableStorageDescriptor(table);
    const keyPrefix = encodePrimaryKey("Prefix", table, schema, prefix);
    const row = await dbTry(new RecordStore(storage, table, descriptor).lastWithPrefix(keyPrefix));
    return row && this.decodeStoredKeyValue(schema, row[0], row[1]);
  }

  /** Explicit primary-key logical range: inclusive start, exclusive end, full keys. */
  primaryKeyScanRangeRaw(this: Database, table: string, start: Value[], end: Value[]) {
    return this.primaryKeyScanRangeRawWithStorage(this.meteredResident(), table, start, end);
  }

  primaryKeyScanRangeRawInBatch(
    this: Database,
    batch: DatabaseBatch,
    table: string,
    start: Value[],
    end: Value[],
  ) {
    this.ensureBatchStorageTxn(batch);
    return this.primaryKeyScanRangeRawWithStorage(this.meteredResident(batch), table, start, end);
  }

  async primaryKeyScanRangeRawWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    table: string,
    start: Value[],
    end: Value[],
  ) {
    const schema = this.primaryKeySchema(table);
    const descriptor = this.tableStorageDescriptor(table);
    const startKey = encodePrimaryKey("Exact", table, schema, start);
    const endKey = encodePrimaryKey("Exact", table, schema, end);
    const rows = await dbTry(new RecordStore(storage, table, descriptor).range(startKey, endKey));
    return rows.map(([k, v]) => this.decodeStoredKeyValue(schema, k, v));
  }

  primaryKeyLastRaw(this: Database, table: string, prefix: Value[]) {
    return this.primaryKeyLastRawWithStorage(this.meteredResident(), table, prefix);
  }

  /** Last record starting with `prefix` whose full key is <= `upper`. */
  async primaryKeyLastBeforeOrAtRaw(
    this: Database,
    table: string,
    prefix: Value[],
    upper: Value[],
  ) {
    const schema = this.primaryKeySchema(table);
    const descriptor = this.tableStorageDescriptor(table);
    const keyPrefix = encodePrimaryKey("Prefix", table, schema, prefix);
    const upperKey = encodePrimaryKey("Exact", table, schema, upper);
    if (
      upperKey.length < keyPrefix.length ||
      !equalBytes(upperKey.subarray(0, keyPrefix.length), keyPrefix)
    )
      return undefined;
    const store = new RecordStore(this.meteredResident(), table, descriptor);
    const row = await dbTry(store.lastWithPrefixBeforeOrAt(keyPrefix, upperKey));
    return row && this.decodeStoredKeyValue(schema, row[0], row[1]);
  }

  async indexGetRaw(this: Database, table: string, indexName: string, key: Value[]) {
    checkIndex("Exact", indexName, this.index(table, indexName).columns.length, key);
    return this.indexScanRaw(table, indexName, key);
  }

  indexScanRaw(this: Database, table: string, indexName: string, prefix: Value[]) {
    return this.indexScanRawWithStorage(this.meteredResident(), table, indexName, prefix);
  }

  async indexScanRawWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    table: string,
    indexName: string,
    prefix: Value[],
  ) {
    checkIndex("Prefix", indexName, this.index(table, indexName).columns.length, prefix);
    const storagePrefix = this.persistedIndexScanPrefix(table, indexName, prefix);
    const raw = await dbTry(
      this.durableIndicesStoreWithStorage(storage, indexRecordDescriptor()).prefix(storagePrefix),
    );
    return this.decodeRawIndexEntriesWithStorage(storage, table, indexName, raw);
  }

  async indexLastRaw(this: Database, table: string, indexName: string, prefix: Value[]) {
    checkIndex("Prefix", indexName, this.index(table, indexName).columns.length, prefix);
    const storagePrefix = this.persistedIndexScanPrefix(table, indexName, prefix);
    const storage = this.meteredResident();
    const raw = await dbTry(
      this.durableIndicesStoreWithStorage(storage, indexRecordDescriptor()).lastWithPrefix(
        storagePrefix,
      ),
    );
    if (!raw) return undefined;
    return (await this.decodeRawIndexEntriesWithStorage(storage, table, indexName, [raw]))[0];
  }

  async indexScanRangeRaw(
    this: Database,
    table: string,
    indexName: string,
    start: Value[],
    end: Value[],
  ) {
    const n = this.index(table, indexName).columns.length;
    checkIndex("Prefix", indexName, n, start);
    checkIndex("Prefix", indexName, n, end);
    const s = this.persistedIndexScanPrefix(table, indexName, start);
    const e = this.persistedIndexScanPrefix(table, indexName, end);
    const storage = this.meteredResident();
    const raw = await dbTry(
      this.durableIndicesStoreWithStorage(storage, indexRecordDescriptor()).range(s, e),
    );
    return this.decodeRawIndexEntriesWithStorage(storage, table, indexName, raw);
  }

  async decodeRawIndexEntriesWithStorage(
    this: Database,
    storage: OrderedKvStorage,
    table: string,
    indexName: string,
    raw: KeyValue[],
  ): Promise<EncodedKeyValue[]> {
    const schema = this.table(table);
    const store = new RecordStore(storage, table, this.tableStorageDescriptor(table));
    const index = this.index(table, indexName);
    const records: EncodedKeyValue[] = [];
    for (const [storageKey, storedValue] of raw) {
      const primaryKey = persistedIndexPrimaryKey(
        schema,
        indexName,
        index,
        storageKey,
        storedValue,
      );
      const record = await dbTry(store.getRaw(primaryKey));
      if (record) records.push(this.decodeStoredKeyValue(schema, primaryKey, record));
      else if (schema.primaryKey) throw new DbError("InvalidPersistedIndex", indexName);
    }
    return records;
  }

  index(this: Database, table: string, indexName: string): IndexSchema {
    this.ensureNotPoisoned();
    const index = this.ivmRuntime.index(table, indexName);
    if (!index) throw new DbError("IndexNotFound", { table, index: indexName });
    return index;
  }

  directRecordStoreSchema(this: Database, store: string) {
    this.ensureNotPoisoned();
    const schema = this.ivmRuntime.directRecordStore(store);
    if (!schema) throw new DbError("DirectRecordStoreNotFound", store);
    return schema;
  }

  persistedIndexScanPrefix(
    this: Database,
    table: string,
    indexName: string,
    prefix: Value[],
  ): Bytes {
    const schema = this.table(table);
    const index = this.index(table, indexName);
    // Index id followed directly by ordered, prefix-free part encodings.
    const out: number[] = [...durableIndexKeyPrefix(this.ivmRuntime.indexStorageId(table, index))];
    prefix.forEach((value, i) => {
      const columnName = index.columns[i];
      if (columnName === undefined) return;
      const column = schema.columns.find((c: any) => c.name === columnName);
      if (!column)
        throw new DbError(
          "InvalidPersistedIndex",
          `index ${indexName} references unknown column ${columnName}`,
        );
      encodeIndexPrefixPart(out, value, column.columnType);
    });
    return Uint8Array.from(out);
  }
}
