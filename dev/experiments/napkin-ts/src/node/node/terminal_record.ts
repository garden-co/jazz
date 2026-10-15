// Receiver-local terminal records. Child edits mutate decoded state.
import { SortedMap, cmpBytes, cmpNum } from "../../support/collections.js";
import { equalBytes } from "../../support/bytes.js";
import {
  type OwnedRecord,
  type RecordDescriptor,
  type TerminalEdit,
  type TerminalOperation,
  type TerminalPathSegment,
  type Value,
  ErrorCode,
  OwnedRecord as OwnedRecordApi,
  descriptorEquals,
  nodeError,
  postcardValuesSize,
  terminalOccurrenceKey,
} from "../../_extern/node-views.js";

const invalid = (message: string) => nodeError(ErrorCode.Protocol, message);

const isRecordArray = (vt: any) => vt.kind === "Array" && vt.element.kind === "Record";

interface TerminalCollectionState {
  descriptor: RecordDescriptor;
  order: Uint8Array[];
  rows: SortedMap<Uint8Array, TerminalRecordState>;
}

export class TerminalRecordState {
  private collections = new SortedMap<number, TerminalCollectionState>(cmpNum);
  private constructor(
    private readonly descriptor: RecordDescriptor,
    private values: Value[],
  ) {}

  static new(record: OwnedRecord): TerminalRecordState {
    let values: Value[];
    try {
      values = record.borrowed().toValues();
    } catch (error) {
      throw invalid(`invalid retained terminal record: ${error}`);
    }
    return new TerminalRecordState(record.descriptor(), values);
  }

  apply(operation: TerminalOperation): void {
    if (!descriptorEquals(this.descriptor, operation.rootDescriptor))
      throw invalid("terminal descendant descriptor disagrees with its retained root");
    this.applyPath(operation.path, operation.edit);
  }

  updateRecord(record: OwnedRecord): void {
    if (!descriptorEquals(record.descriptor(), this.descriptor))
      throw invalid("terminal update changed its retained descriptor");
    const replacement = TerminalRecordState.new(record);
    this.descriptor.fields().forEach((field: any, index: number) => {
      if (isRecordArray(field.valueType)) replacement.values[index] = this.values[index];
    });
    this.values = replacement.values;
  }

  private applyPath(path: TerminalPathSegment[], edit: TerminalEdit): void {
    const [head, ...rest] = path;
    if (!head || head.kind !== "Collection")
      throw invalid("terminal descendant path must begin with a collection");
    const index: number | undefined = this.descriptor.fieldIndex(head.name);
    if (index === undefined) throw invalid("terminal descendant path names an unknown collection");
    if (!this.collections.has(index)) {
      const vt = this.descriptor.fields()[index].valueType;
      if (vt.kind !== "Array") throw invalid("terminal descendant path names a non-array field");
      if (vt.element.kind !== "Record")
        throw invalid("terminal descendant collection does not contain records");
      const children = this.values[index];
      if (!children || children.kind !== "Array")
        throw invalid("terminal descendant collection payload is not an array");
      const collection: TerminalCollectionState = {
        descriptor: vt.element.descriptor,
        order: [],
        rows: new SortedMap(cmpBytes),
      };
      const occurrences = new SortedMap<Uint8Array, number>(cmpBytes);
      for (const child of children.items as Value[]) {
        if (child.kind !== "Record")
          throw invalid("terminal descendant collection contains a non-record child");
        const rowKey = terminalChildKey(child);
        const occurrence = occurrences.get(rowKey) ?? 0;
        const key = terminalOccurrenceKey(rowKey, occurrence);
        occurrences.set(rowKey, occurrence + 1);
        const state = TerminalRecordState.new(child.record);
        if (collection.rows.set(key, state) !== undefined)
          throw invalid("terminal descendant collection has duplicate child keys");
        collection.order.push(key);
      }
      children.items = [];
      this.collections.set(index, collection);
    }
    const collection = this.collections.get(index)!;
    if (rest.length === 0) return applyCollection(collection, edit);
    if (rest[0].kind === "Key" && rest.length === 1) {
      if (edit.kind === "Update" && equalBytes(edit.key, rest[0].key))
        return applyCollection(collection, edit);
      throw invalid("terminal keyed path must match its scalar update key");
    }
    if (rest[0].kind === "Key") {
      const child = collection.rows.get(rest[0].key);
      if (!child) throw invalid("terminal child path addressed a missing key");
      return child.applyPath(rest.slice(1), edit);
    }
    throw invalid("terminal descendant path does not alternate collection and key");
  }

  record(): OwnedRecord {
    const values = this.values.slice();
    for (const [index, collection] of this.collections)
      values[index] = {
        kind: "Array",
        items: collection.order.map((key) => ({
          kind: "Record",
          record: collection.rows.get(key)!.record(),
        })),
      };
    let raw: Uint8Array;
    try {
      raw = this.descriptor.create(values);
    } catch (error) {
      throw invalid(`cannot encode retained terminal record: ${error}`);
    }
    return OwnedRecordApi.new(raw, this.descriptor);
  }

  retainedBytes(): number {
    let total = postcardValuesSize(this.values) + this.values.length * 32;
    for (const [, c] of this.collections) {
      total += c.order.length * 24;
      for (const k of c.order) total += k.length;
      for (const [key, row] of c.rows) total += key.length + row.retainedBytes();
    }
    return total;
  }
}

function applyCollection(c: TerminalCollectionState, edit: TerminalEdit): void {
  const key: Uint8Array = edit.key;
  switch (edit.kind) {
    case "Insert":
    case "Update": {
      const record = OwnedRecordApi.new(edit.value, c.descriptor);
      const rowKey = terminalChildKey({ kind: "Record", record });
      const n = rowKey.length;
      const validKey =
        equalBytes(key, rowKey) ||
        (key.length === n + 9 &&
          equalBytes(key.subarray(0, n), rowKey) &&
          key[n] === 0xff &&
          key.subarray(n + 1).some((b) => b !== 0));
      if (!validKey) throw invalid("terminal child payload key disagrees with edit key");
      if (edit.kind === "Insert") {
        const state = TerminalRecordState.new(record);
        if (c.rows.has(key)) c.order = c.order.filter((k) => !equalBytes(k, key));
        c.order.splice(Math.min(edit.index, c.order.length), 0, key);
        c.rows.set(key, state);
      } else {
        const row = c.rows.get(key);
        if (!row) throw invalid("terminal child update addressed a missing key");
        row.updateRecord(record);
      }
      return;
    }
    case "Remove":
      if (c.rows.delete(key) === undefined)
        throw invalid("terminal child removal addressed a missing key");
      c.order = c.order.filter((k) => !equalBytes(k, key));
      return;
    case "Move": {
      const previous = c.order.findIndex((k) => equalBytes(k, key));
      if (previous < 0) throw invalid("terminal child move addressed a missing key");
      const [moved] = c.order.splice(previous, 1);
      c.order.splice(Math.min(edit.index, c.order.length), 0, moved);
    }
  }
}

export function terminalChildKey(value: Value): Uint8Array {
  if (value.kind !== "Record")
    throw nodeError(
      ErrorCode.Protocol,
      "terminal descendant collection contains a non-record child",
    );
  let first: Value;
  try {
    first = value.record.getIdx(0);
  } catch (error) {
    throw nodeError(ErrorCode.Protocol, `cannot decode terminal child key: ${error}`);
  }
  if (first.kind !== "Uuid")
    throw nodeError(
      ErrorCode.Protocol,
      "terminal descendant child key must be its physical row UUID",
    );
  const key = new Uint8Array(17);
  key[0] = 10;
  key.set(first.bytes, 1);
  return key;
}
