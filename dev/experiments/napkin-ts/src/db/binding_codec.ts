import { Reader, Writer, encode, PostcardError } from "../support/postcard.js";
import { equalBytes } from "../support/bytes.js";
import type {
  CurrentRow,
  OwnedRecord,
  RecordDescriptor,
  RelationSnapshot,
  ValueType,
} from "../_extern/db-core.js";
import {
  OwnedRecordT,
  ResultKey,
  encodePhysicalColumnId,
  encodeResultKey,
  encodeRowUuid,
  valueTypeEq,
} from "../_extern/db-core.js";
import type { RemovedRow, RowCells, SubscriptionOutputRow } from "./db.js";
import {
  readDescriptor,
  writeNativeDescriptor,
  writeNativeValueType,
} from "./binding_codec/publication_type.js";

export function encodeNamedCellDescriptor(descriptor: RecordDescriptor): Uint8Array {
  return encode(descriptor, writeNativeDescriptor);
}

export function encodeNamedCells(record: OwnedRecord): Uint8Array {
  const w = new Writer();
  writeNativeDescriptor(w, record.descriptor());
  w.bytes(record.raw());
  return w.finish();
}

export function decodeNamedCells(bytes: Uint8Array): RowCells {
  const r = new Reader(bytes);
  const descriptor = readDescriptor(r);
  const raw = r.bytes().slice();
  if (r.remaining !== 0) throw new Error("trailing bytes in named-cell envelope");
  const record = OwnedRecordT.new(raw, descriptor);
  if (!equalBytes(encodeNamedCells(record), bytes))
    throw new Error("noncanonical named-cell envelope");
  const values = record.toValues();
  if (!equalBytes(descriptor.create(values), record.raw()))
    throw new Error("noncanonical named-cell record");
  const cells: RowCells = new Map();
  const fields = descriptor.fields();
  for (let i = 0; i < fields.length; i++) {
    const name = fields[i].name;
    if (name === undefined) throw new Error("encoded cells must use named fields");
    if (cells.has(name)) throw new Error("encoded cells contain duplicate names");
    cells.set(name, values[i]);
  }
  return cells;
}

export type RowDescriptorFieldName =
  | { kind: "StoredColumn"; id: any; outputName: string }
  | { kind: "ResultField"; name: string }
  | { kind: "HiddenMetadata"; name: string };

export interface RowDescriptorField {
  name: RowDescriptorFieldName;
  valueType: ValueType;
}

export interface Row {
  rowId: any;
  deleted: boolean;
  raw: Uint8Array;
}

export interface RowBatch {
  table: string;
  descriptor: RowDescriptorField[];
  rows: Row[];
}

function writeRowBatch(w: Writer, b: RowBatch): void {
  w.str(b.table);
  w.seq(b.descriptor, (w, f) => {
    switch (f.name.kind) {
      case "StoredColumn":
        w.variant(0);
        encodePhysicalColumnId(w, f.name.id);
        w.str(f.name.outputName);
        break;
      case "ResultField":
        w.variant(1).str(f.name.name);
        break;
      case "HiddenMetadata":
        w.variant(2).str(f.name.name);
        break;
    }
    writeNativeValueType(w, f.valueType);
  });
  w.seq(b.rows, (w, row) => {
    encodeRowUuid(w, row.rowId);
    w.bool(row.deleted).bytes(row.raw);
  });
}

export function encodeRows(rows: CurrentRow[]): Uint8Array {
  const w = new Writer();
  w.seq(rowBatches(rows), writeRowBatch);
  return w.finish();
}

export function encodeRelationSnapshot(snapshot: RelationSnapshot): Uint8Array {
  const w = new Writer();
  w.varint(snapshot.rootCount);
  w.seq(rowBatches(snapshot.rows), writeRowBatch);
  return w.finish();
}

export function encodeSubscriptionDelta(
  added: SubscriptionOutputRow[],
  updated: SubscriptionOutputRow[],
  removed: RemovedRow[],
): Uint8Array {
  const w = new Writer();
  w.seq(rowBatches(added.map((r) => r.row)), writeRowBatch);
  w.seq(rowBatches(updated.map((r) => r.row)), writeRowBatch);
  w.seq(removed, (w, r) => {
    w.str(r.table);
    encodeRowUuid(w, r.rowUuid);
  });
  const keys = (rows: { occurrenceId: any }[]) =>
    w.seq(rows, (w, r) => encodeResultKey(w, ResultKey.fromOccurrence(r.occurrenceId)));
  keys(added);
  keys(updated);
  keys(removed);
  const nums = (xs: number[]) => w.seq(xs, (w, n) => w.varint(n));
  nums(added.map((r) => r.index));
  nums(updated.map((r) => r.previousIndex ?? r.index));
  nums(updated.map((r) => r.index));
  nums(removed.map((r) => r.index));
  return w.finish();
}

export interface PublishedRecord {
  descriptor: RecordDescriptor;
  raw: Uint8Array;
  fields: any[];
}

export function publishedRecord(row: CurrentRow): PublishedRecord {
  const [descriptor, raw] = row.encodedRecord();
  return { descriptor, raw, fields: row.publicationFields() };
}

function descriptorEq(a: RowDescriptorField[], b: RowDescriptorField[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i].name as any;
    const y = b[i].name as any;
    if (x.kind !== y.kind || x.name !== y.name || x.outputName !== y.outputName || x.id !== y.id)
      return false;
    if (!valueTypeEq(a[i].valueType, b[i].valueType)) return false;
  }
  return true;
}

export function rowBatches(rows: CurrentRow[]): RowBatch[] {
  const batches: RowBatch[] = [];
  for (const row of rows) {
    const published = publishedRecord(row);
    const fields = published.descriptor.fields();
    const descriptor: RowDescriptorField[] = fields.map((field: any, i: number) => {
      const binding = published.fields[i];
      let name: RowDescriptorFieldName;
      switch (binding.kind) {
        case "StoredColumn":
          name = { kind: "StoredColumn", id: binding.id, outputName: binding.outputName };
          break;
        case "ResultField":
          name =
            binding.visibility === "HiddenMetadata"
              ? { kind: "HiddenMetadata", name: binding.name }
              : { kind: "ResultField", name: binding.name };
          break;
        default:
          throw new PostcardError("Serde Serialization Error");
      }
      return { name, valueType: field.valueType };
    });
    const entry: Row = { rowId: row.rowUuid(), deleted: row.isDeleted(), raw: published.raw };
    const last = batches[batches.length - 1];
    if (last && last.table === row.table() && descriptorEq(last.descriptor, descriptor))
      last.rows.push(entry);
    else batches.push({ table: row.table(), descriptor, rows: [entry] });
  }
  return batches;
}

export function terminalOperationsToJson(operations: any[]): any[] {
  return operations.map((op) => {
    const { root_descriptor: _, ...rest } = op;
    return rest;
  });
}
