// Schema hashes as Jazz computed them before alpha.60: BLAKE3 over the
// structural bytes Rust's schema hash still takes SHA-256 over. Only
// `validate` uses this, to recognise migrations and snapshots named by those
// hashes; nothing at runtime does.
import { blake3 } from "@noble/hashes/blake3.js";
import { bytesToHex } from "@noble/hashes/utils.js";
import type {
  ColumnDescriptor,
  ColumnMergeStrategy,
  ColumnType as WasmColumnType,
  Value,
  WasmSchema,
} from "../drivers/types.js";
import { canonicalJsonSchema } from "../runtime/schema-metadata.js";
import { parseUuidBytes } from "../runtime/hex.js";
import { canonicalCompositeIndexOrder } from "./schema-utils.js";

/** The schema hash an alpha.59 or older tool gave `schema`. */
export function legacySchemaHash(schema: WasmSchema): string {
  return bytesToHex(blake3(legacyStructuralBytes(schema)));
}

// The merge-strategy bytes of Rust's schema hash (branch.rs).
const MERGE_STRATEGY_HASH_BYTES: Record<ColumnMergeStrategy, number> = {
  Counter: 1,
  GSet: 2,
  Text: 3,
  Markdown: 4,
  Html: 5,
};

/**
 * The bytes an alpha.59 or older tool hashed: frozen, as the names of the
 * files they wrote are. (Today's schema hash is Rust's, through jazz-napi.)
 */
function legacyStructuralBytes(schema: WasmSchema): Uint8Array {
  const writer = new StructuralHashWriter();

  for (const tableName of Object.keys(schema).sort()) {
    const table = schema[tableName]!;

    writer.stringBytes(tableName);
    writer.byte(0);
    hashColumns(writer, table.columns);

    if (table.indexed_columns) {
      writer.byte(1);
      for (const column of [...table.indexed_columns].sort()) {
        writer.stringBytes(column);
        writer.byte(0);
      }
    }
    if (table.composite_indexes?.length) {
      writer.stringBytes("composite_indexes\0");
      writer.stringBytes(JSON.stringify(canonicalCompositeIndexOrder(table.composite_indexes)));
    }
    if (table.branchBy?.length) {
      writer.stringBytes("branch_by\0");
      writer.stringBytes(JSON.stringify(table.branchBy));
      writer.byte(0);
    }
    // Full history is not hashed, as in Rust's `SchemaHash::compute`.
    if (table.history === "latest") {
      writer.stringBytes("history\0latest\0");
    }
  }

  return writer.bytes();
}

class StructuralHashWriter {
  private chunks: number[] = [];
  private textEncoder = new TextEncoder();

  byte(value: number): void {
    this.chunks.push(value & 0xff);
  }

  bytes(): Uint8Array;
  bytes(value: Uint8Array): void;
  bytes(value: ArrayBufferLike): void;
  bytes(value?: Uint8Array | ArrayBufferLike): Uint8Array | void {
    if (value === undefined) {
      return Uint8Array.from(this.chunks);
    }
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
    for (const byte of bytes) {
      this.byte(byte);
    }
  }

  stringBytes(value: string): void {
    this.bytes(this.textEncoder.encode(value));
  }

  u64(value: number): void {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
    this.bytes(bytes);
  }

  i32(value: number): void {
    const bytes = new Uint8Array(4);
    new DataView(bytes.buffer).setInt32(0, value, true);
    this.bytes(bytes);
  }

  i64(value: number | bigint): void {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setBigInt64(0, BigInt(value), true);
    this.bytes(bytes);
  }

  f64(value: number): void {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, value, true);
    this.bytes(bytes);
  }
}

function hashColumns(writer: StructuralHashWriter, columns: ColumnDescriptor[]): void {
  for (const column of columns) {
    writer.stringBytes(column.name);
    writer.byte(0);
    hashColumnType(writer, column.column_type);
    writer.byte(column.nullable ? 1 : 0);

    if (column.references) {
      writer.byte(1);
      writer.stringBytes(column.references);
    } else {
      writer.byte(0);
    }

    if (column.default) {
      writer.byte(1);
      hashValue(writer, column.default);
    }

    if (column.merge_strategy) {
      writer.byte(1);
      writer.byte(MERGE_STRATEGY_HASH_BYTES[column.merge_strategy]);
    } else {
      writer.byte(0);
    }
    writer.byte(0); // Column delimiter, including when a default is present.
  }
}

function hashValue(writer: StructuralHashWriter, value: Value): void {
  switch (value.type) {
    case "Integer":
      writer.byte(1);
      writer.i32(value.value);
      return;
    case "BigInt":
      writer.byte(2);
      writer.i64(value.value);
      return;
    case "Double":
      writer.byte(10);
      writer.f64(value.value);
      return;
    case "Boolean":
      writer.byte(3);
      writer.byte(value.value ? 1 : 0);
      return;
    case "Text":
      writer.byte(4);
      writer.stringBytes(value.value);
      writer.byte(0);
      return;
    case "Timestamp":
      writer.byte(5);
      writer.i64(value.value);
      return;
    case "Uuid":
      writer.byte(6);
      writer.bytes(uuidBytes(value.value));
      return;
    case "Bytea":
      writer.byte(11);
      writer.u64(value.value.length);
      writer.bytes(value.value);
      return;
    case "Array":
      writer.byte(7);
      writer.u64(value.value.length);
      for (const inner of value.value) {
        hashValue(writer, inner);
      }
      return;
    case "Row":
      writer.byte(8);
      writer.u64(value.value.values.length);
      for (const inner of value.value.values) {
        hashValue(writer, inner);
      }
      return;
    case "Enum":
      writer.byte(14);
      writer.stringBytes(value.value.case);
      writer.byte(0);
      writer.u64(value.value.values.length);
      for (const inner of value.value.values) hashValue(writer, inner);
      return;
    case "Null":
      writer.byte(9);
      return;
  }
  const exhaustive: never = value;
  throw new Error(`Unhandled schema default: ${String(exhaustive)}`);
}

function hashColumnType(writer: StructuralHashWriter, columnType: WasmColumnType): void {
  switch (columnType.type) {
    case "Integer":
      writer.byte(1);
      return;
    case "BigInt":
      writer.byte(2);
      return;
    case "Double":
      writer.byte(10);
      return;
    case "Boolean":
      writer.byte(3);
      return;
    case "Text":
      writer.byte(4);
      return;
    case "Enum": {
      writer.byte(9);
      writer.u64(columnType.variants.length);
      for (const variant of columnType.variants) {
        writer.stringBytes(variant);
        writer.byte(0);
      }
      return;
    }
    case "EnumPayload": {
      // Keep this byte layout in lockstep with Rust's
      // `public_api::types::branch::hash_column_type`: payload-case order and
      // field order are semantic, while a payload field contributes only its
      // name, type, and nullability.
      writer.byte(13);
      writer.u64(columnType.cases.length);
      for (const entry of columnType.cases) {
        writer.stringBytes(entry.name);
        writer.byte(0);
        writer.u64(entry.fields.length);
        for (const field of entry.fields) {
          writer.stringBytes(field.name);
          writer.byte(0);
          hashColumnType(writer, field.column_type);
          writer.byte(field.nullable ? 1 : 0);
        }
      }
      return;
    }
    case "Timestamp":
      writer.byte(5);
      return;
    case "Uuid":
      writer.byte(6);
      return;
    case "Bytea":
      writer.byte(16);
      return;
    case "Json":
      writer.byte(11);
      if (columnType.schema) {
        writer.byte(1);
        const encoded = new TextEncoder().encode(canonicalJsonSchema(columnType.schema));
        writer.u64(encoded.length);
        writer.bytes(encoded);
      } else {
        writer.byte(0);
      }
      return;
    case "Array":
      writer.byte(7);
      hashColumnType(writer, columnType.element);
      return;
    case "Row":
      writer.byte(8);
      hashColumns(writer, columnType.columns);
      return;
  }

  const exhaustive: never = columnType;
  throw new Error(`Unhandled column type: ${JSON.stringify(exhaustive)}`);
}

function uuidBytes(value: string): Uint8Array {
  const bytes = parseUuidBytes(value);
  if (!bytes) throw new Error(`Invalid UUID default value: ${value}`);
  return bytes;
}
