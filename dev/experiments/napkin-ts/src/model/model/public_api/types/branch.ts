import { Blake3Hasher, uuidV5, uuidFromString } from "../../../../support/hash.js";
import { toHex, fromHex, utf8, equalBytes, type Bytes } from "../../../../support/bytes.js";
import { cmpStr } from "../../../../support/collections.js";
import {
  objectIdFromUuid,
  objectIdUuidBytes,
  type ObjectId,
} from "../../../../_extern/model-core.js";
import type { Value } from "./value.js";
import {
  type Schema,
  type RowDescriptor,
  type ColumnDescriptor,
  type ColumnType,
  type EnumCaseDescriptor,
  canonicalCompositeIndexOrder,
} from "./schema.js";

export const NAMESPACE_DNS = uuidFromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

/** Content-addressed BLAKE3 hash of a schema's structural elements. */
export class SchemaHash {
  constructor(readonly bytes: Bytes) {}
  static fromHex(hex: string): SchemaHash | undefined {
    try {
      const b = fromHex(hex);
      return b.length === 32 ? new SchemaHash(b) : undefined;
    } catch {
      return undefined;
    }
  }
  static fromJson(s: string): SchemaHash {
    const b = fromHex(s);
    if (b.length !== 32) throw new Error("SchemaHash must be 32 bytes");
    return new SchemaHash(b);
  }
  short(): string {
    return toHex(this.bytes.subarray(0, 6));
  }
  toHex(): string {
    return toHex(this.bytes);
  }
  toString(): string {
    return this.toHex();
  }
  toJSON(): string {
    return this.toHex();
  }
  equals(o: SchemaHash): boolean {
    return equalBytes(this.bytes, o.bytes);
  }
  toObjectId(): ObjectId {
    return objectIdFromUuid(uuidV5(NAMESPACE_DNS, this.bytes));
  }

  static compute(schema: Schema): SchemaHash {
    const h = new Blake3Hasher();
    const names = [...schema.keysArray()].sort(cmpStr);
    for (const name of names) {
      const t = schema.get(name)!;
      hashCstr(h, name);
      hashRowDescriptor(h, t.columns);
      if (t.indexed_columns) {
        h.update(Uint8Array.of(1));
        for (const c of [...t.indexed_columns].sort(cmpStr)) hashCstr(h, c);
      }
      if (t.composite_indexes.length) {
        h.update(utf8("composite_indexes\0"));
        h.update(utf8(JSON.stringify(canonicalCompositeIndexOrder(t.composite_indexes))));
      }
      if (t.branch_by.length) {
        h.update(utf8("branch_by\0"));
        h.update(utf8(JSON.stringify(t.branch_by)));
        h.update(Uint8Array.of(0));
      }
    }
    return new SchemaHash(h.finalize());
  }
}

const u64le = (n: number | bigint) => {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt.asUintN(64, BigInt(n)), true);
  return b;
};

export function hashRowDescriptor(h: Blake3Hasher, d: RowDescriptor): void {
  for (const c of d.columns) hashColumnDescriptor(h, c);
}

function hashColumnDescriptor(h: Blake3Hasher, c: ColumnDescriptor): void {
  hashCstr(h, c.name);
  hashColumnType(h, c.column_type);
  h.update(Uint8Array.of(c.nullable ? 1 : 0));
  if (c.references !== undefined) {
    h.update(Uint8Array.of(1));
    h.update(utf8(c.references));
  } else h.update(Uint8Array.of(0));
  if (c.default !== undefined) {
    h.update(Uint8Array.of(1));
    hashValue(h, c.default);
  }
  if (c.merge_strategy) h.update(Uint8Array.of(1, c.merge_strategy === "Counter" ? 1 : 2));
  else h.update(Uint8Array.of(0));
  h.update(Uint8Array.of(0));
}

function hashValues(h: Blake3Hasher, tag: number, values: Value[]) {
  h.update(Uint8Array.of(tag));
  h.update(u64le(values.length));
  for (const v of values) hashValue(h, v);
}

function hashValue(h: Blake3Hasher, v: Value): void {
  const dv = new DataView(new ArrayBuffer(8));
  switch (v.type) {
    case "Integer":
      dv.setInt32(0, v.value, true);
      h.update(Uint8Array.of(1)).update(new Uint8Array(dv.buffer, 0, 4));
      return;
    case "BigInt":
      dv.setBigInt64(0, v.value, true);
      h.update(Uint8Array.of(2)).update(new Uint8Array(dv.buffer));
      return;
    case "Double":
      dv.setFloat64(0, v.value, true);
      h.update(Uint8Array.of(10)).update(new Uint8Array(dv.buffer));
      return;
    case "Boolean":
      h.update(Uint8Array.of(3, v.value ? 1 : 0));
      return;
    case "Text":
      h.update(Uint8Array.of(4));
      hashCstr(h, v.value);
      return;
    case "Timestamp":
      h.update(Uint8Array.of(5)).update(u64le(v.value));
      return;
    case "Uuid":
      h.update(Uint8Array.of(6)).update(objectIdUuidBytes(v.value));
      return;
    case "TransactionId":
      h.update(Uint8Array.of(12)).update(v.value);
      return;
    case "Bytea":
      h.update(Uint8Array.of(11)).update(u64le(v.value.length)).update(v.value);
      return;
    case "Array":
      return hashValues(h, 7, v.value);
    case "Row":
      return hashValues(h, 8, v.values);
    case "Enum":
      h.update(Uint8Array.of(14));
      hashCstr(h, v.case);
      h.update(u64le(v.values.length));
      for (const inner of v.values) hashValue(h, inner);
      return;
    case "Null":
      h.update(Uint8Array.of(9));
  }
}

function hashCstr(h: Blake3Hasher, s: string): void {
  h.update(utf8(s)).update(Uint8Array.of(0));
}

function hashEnumPayloadCases(h: Blake3Hasher, cases: EnumCaseDescriptor[]) {
  h.update(u64le(cases.length));
  for (const c of cases) {
    hashCstr(h, c.name);
    h.update(u64le(c.fields.length));
    for (const f of c.fields) {
      hashCstr(h, f.name);
      hashColumnType(h, f.column_type);
      h.update(Uint8Array.of(f.nullable ? 1 : 0));
    }
  }
}

function hashVariants(h: Blake3Hasher, variants: string[]) {
  h.update(u64le(variants.length));
  for (const v of variants) hashCstr(h, v);
}

const SIMPLE_TYPE_TAGS: Record<string, number> = {
  Integer: 1,
  BigInt: 2,
  Double: 10,
  Boolean: 3,
  Text: 4,
  Timestamp: 5,
  Uuid: 6,
  TransactionId: 12,
  Bytea: 16,
};

function hashColumnType(h: Blake3Hasher, t: ColumnType): void {
  switch (t.type) {
    case "Enum":
      h.update(Uint8Array.of(9));
      return hashVariants(h, t.variants);
    case "ScalarEnum":
      h.update(Uint8Array.of(14));
      hashCstr(h, t.name);
      return hashVariants(h, t.variants);
    case "CatalogueEnumPayload":
      h.update(Uint8Array.of(15));
      hashCstr(h, t.name);
      return hashEnumPayloadCases(h, t.cases);
    case "EnumPayload":
      h.update(Uint8Array.of(13));
      return hashEnumPayloadCases(h, t.cases);
    case "Json":
      h.update(Uint8Array.of(11));
      if (t.schema !== undefined) {
        const encoded = utf8(JSON.stringify(t.schema));
        h.update(Uint8Array.of(1)).update(u64le(encoded.length)).update(encoded);
      } else h.update(Uint8Array.of(0));
      return;
    case "Array":
      h.update(Uint8Array.of(7));
      return hashColumnType(h, t.element);
    case "Row":
      h.update(Uint8Array.of(8));
      return hashRowDescriptor(h, t.columns);
    default:
      h.update(Uint8Array.of(SIMPLE_TYPE_TAGS[t.type]));
  }
}
