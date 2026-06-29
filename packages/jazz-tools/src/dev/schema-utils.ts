import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  Value,
  WasmSchema,
} from "../drivers/types.js";
import { blake3 } from "@noble/hashes/blake3.js";
import { bytesToHex } from "@noble/hashes/utils.js";

const SHORT_SCHEMA_HASH_LENGTH = 12;

export function normalizeSchemaHashInput(hash: string, label: string): string {
  const normalized = hash.trim().toLowerCase();
  if (!/^[0-9a-f]{12,64}$/.test(normalized)) {
    throw new Error(`${label} must be a 12-64 character lowercase hex schema hash.`);
  }
  return normalized;
}

export function shortSchemaHash(hash: string): string {
  return normalizeSchemaHashInput(hash, "schema hash").slice(0, SHORT_SCHEMA_HASH_LENGTH);
}

export function structuralSchemaHash(schema: WasmSchema): string {
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
  }

  return bytesToHex(blake3(writer.bytes()));
}

export function columnTypeSignature(columnType: WasmColumnType): string {
  return JSON.stringify(columnType);
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
    } else {
      writer.byte(0);
    }

    if (column.merge_strategy) {
      writer.byte(1);
      writer.byte(column.merge_strategy === "Counter" ? 1 : 2);
    } else {
      writer.byte(0);
    }

    if (column.large_value) {
      writer.byte(1);
      writer.byte(column.large_value === "Text" ? 1 : 2);
    } else {
      writer.byte(0);
    }
  }
}

function hashValue(writer: StructuralHashWriter, value: Value): void {
  switch (value.type) {
    case "Integer":
      writer.byte(1);
      writer.i64(value.value);
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
    case "Null":
      writer.byte(9);
      return;
  }
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
      const variants = [...new Set(columnType.variants)].sort();
      writer.u64(variants.length);
      for (const variant of variants) {
        writer.stringBytes(variant);
        writer.byte(0);
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
      writer.byte(10);
      return;
    case "Json":
      writer.byte(11);
      if (columnType.schema) {
        writer.byte(1);
        const encoded = new TextEncoder().encode(JSON.stringify(columnType.schema));
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
}

function uuidBytes(value: string): Uint8Array {
  const hex = value.replace(/-/g, "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) {
    throw new Error(`Invalid UUID default value: ${value}`);
  }
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function columnsEqual(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return (
    left.name === right.name &&
    left.nullable === right.nullable &&
    left.references === right.references &&
    left.merge_strategy === right.merge_strategy &&
    left.large_value === right.large_value &&
    columnTypeSignature(left.column_type) === columnTypeSignature(right.column_type)
  );
}

function indexedColumnsEqual(
  left: readonly string[] | undefined,
  right: readonly string[] | undefined,
): boolean {
  if (!left && !right) {
    return true;
  }
  if (!left || !right || left.length !== right.length) {
    return false;
  }

  const leftColumns = [...left].sort();
  const rightColumns = [...right].sort();
  return leftColumns.every((column, index) => column === rightColumns[index]);
}

export function tableSchemasEqual(
  left: WasmSchema[string] | undefined,
  right: WasmSchema[string] | undefined,
): boolean {
  if (!left || !right) {
    return false;
  }

  if (left.columns.length !== right.columns.length) {
    return false;
  }

  if (!indexedColumnsEqual(left.indexed_columns, right.indexed_columns)) {
    return false;
  }

  const leftColumns = [...left.columns].sort((a, b) => a.name.localeCompare(b.name));
  const rightColumns = [...right.columns].sort((a, b) => a.name.localeCompare(b.name));

  return leftColumns.every((column, index) => columnsEqual(column, rightColumns[index]!));
}

export function wasmSchemasEqual(left: WasmSchema, right: WasmSchema): boolean {
  const leftTableNames = Object.keys(left).sort();
  const rightTableNames = Object.keys(right).sort();

  if (leftTableNames.length !== rightTableNames.length) {
    return false;
  }

  return leftTableNames.every((tableName, index) => {
    if (tableName !== rightTableNames[index]) {
      return false;
    }
    return tableSchemasEqual(left[tableName], right[tableName]);
  });
}
