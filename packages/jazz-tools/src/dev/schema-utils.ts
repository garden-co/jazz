import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  Value,
  WasmSchema,
} from "../drivers/types.js";
import { blake3 } from "@noble/hashes/blake3.js";
import { bytesToHex } from "@noble/hashes/utils.js";

const SHORT_SCHEMA_HASH_LENGTH = 12;

const COLUMN_TYPE_HASH_TAG = {
  Integer: 1,
  BigInt: 2,
  Boolean: 3,
  Text: 4,
  Timestamp: 5,
  Uuid: 6,
  Array: 7,
  Row: 8,
  Enum: 9,
  Double: 10,
  Json: 11,
  TransactionId: 12,
  EnumPayload: 13,
  ScalarEnum: 14,
  CatalogueEnumPayload: 15,
  Bytea: 16,
} as const satisfies Record<
  WasmColumnType["type"] | "TransactionId" | "ScalarEnum" | "CatalogueEnumPayload",
  number
>;

const VALUE_HASH_TAG = {
  Integer: 1,
  BigInt: 2,
  Boolean: 3,
  Text: 4,
  Timestamp: 5,
  Uuid: 6,
  Array: 7,
  Row: 8,
  Null: 9,
  Double: 10,
  Bytea: 11,
  Enum: 14,
} as const satisfies Record<Value["type"], number>;

const CURRENT_STRUCTURAL_HASH_FORMAT = {
  byteaTag: COLUMN_TYPE_HASH_TAG.Bytea,
} as const;

const LEGACY_BYTEA_STRUCTURAL_HASH_FORMAT = {
  byteaTag: COLUMN_TYPE_HASH_TAG.Double,
} as const;

type StructuralHashFormat = {
  byteaTag: number;
};

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
  return structuralSchemaHashWithFormat(schema, CURRENT_STRUCTURAL_HASH_FORMAT);
}

/**
 * Computes the historical identity where Bytea shared Double's column-type tag.
 *
 * Use this only to resolve an existing catalogue identity and connect it to the
 * current identity with a durable migration edge. New catalogue identities
 * always use {@link structuralSchemaHash}.
 */
export function legacyByteaStructuralSchemaHash(schema: WasmSchema): string {
  return structuralSchemaHashWithFormat(schema, LEGACY_BYTEA_STRUCTURAL_HASH_FORMAT);
}

function structuralSchemaHashWithFormat(schema: WasmSchema, format: StructuralHashFormat): string {
  const writer = new StructuralHashWriter();

  for (const tableName of Object.keys(schema).sort()) {
    const table = schema[tableName]!;

    writer.stringBytes(tableName);
    writer.byte(0);
    hashColumns(writer, table.columns, format);

    if (table.indexed_columns) {
      writer.byte(1);
      for (const column of [...table.indexed_columns].sort()) {
        writer.stringBytes(column);
        writer.byte(0);
      }
    }

    if (table.branchBy?.length) {
      writer.stringBytes("branch_by");
      writer.byte(0);
      writer.stringBytes(JSON.stringify(table.branchBy));
      writer.byte(0);
    }
  }

  return bytesToHex(blake3(writer.bytes()));
}

export function columnTypeSignature(columnType: WasmColumnType): string {
  return JSON.stringify(canonicalizeJsonObject(columnType));
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

function hashColumns(
  writer: StructuralHashWriter,
  columns: ColumnDescriptor[],
  format: StructuralHashFormat,
): void {
  for (const column of columns) {
    writer.stringBytes(column.name);
    writer.byte(0);
    hashColumnType(writer, column.column_type, format);
    writer.byte(column.nullable ? 1 : 0);

    if (column.references) {
      writer.byte(1);
      writer.stringBytes(column.references);
    } else {
      writer.byte(0);
    }

    // Absence remains untagged for compatibility with schemas authored before
    // column defaults were included in structural identity.
    if (column.default) {
      writer.byte(1);
      hashValue(writer, column.default);
    }

    if (column.merge_strategy) {
      writer.byte(1);
      writer.byte(column.merge_strategy === "Counter" ? 1 : 2);
    } else {
      writer.byte(0);
    }

    writer.byte(0);
  }
}

function hashValue(writer: StructuralHashWriter, value: Value): void {
  switch (value.type) {
    case "Integer":
      writer.byte(VALUE_HASH_TAG.Integer);
      writer.i32(value.value);
      return;
    case "BigInt":
      writer.byte(VALUE_HASH_TAG.BigInt);
      writer.i64(value.value);
      return;
    case "Double":
      writer.byte(VALUE_HASH_TAG.Double);
      writer.f64(value.value);
      return;
    case "Boolean":
      writer.byte(VALUE_HASH_TAG.Boolean);
      writer.byte(value.value ? 1 : 0);
      return;
    case "Text":
      writer.byte(VALUE_HASH_TAG.Text);
      writer.stringBytes(value.value);
      writer.byte(0);
      return;
    case "Timestamp":
      writer.byte(VALUE_HASH_TAG.Timestamp);
      writer.i64(value.value);
      return;
    case "Uuid":
      writer.byte(VALUE_HASH_TAG.Uuid);
      writer.bytes(uuidBytes(value.value));
      return;
    case "Bytea":
      writer.byte(VALUE_HASH_TAG.Bytea);
      writer.u64(value.value.length);
      writer.bytes(value.value);
      return;
    case "Array":
      writer.byte(VALUE_HASH_TAG.Array);
      writer.u64(value.value.length);
      for (const inner of value.value) {
        hashValue(writer, inner);
      }
      return;
    case "Row":
      writer.byte(VALUE_HASH_TAG.Row);
      writer.u64(value.value.values.length);
      for (const inner of value.value.values) {
        hashValue(writer, inner);
      }
      return;
    case "Enum":
      writer.byte(VALUE_HASH_TAG.Enum);
      writer.stringBytes(value.value.case);
      writer.byte(0);
      writer.u64(value.value.values.length);
      for (const inner of value.value.values) {
        hashValue(writer, inner);
      }
      return;
    case "Null":
      writer.byte(VALUE_HASH_TAG.Null);
      return;
  }
}

function hashColumnType(
  writer: StructuralHashWriter,
  columnType: WasmColumnType,
  format: StructuralHashFormat,
): void {
  switch (columnType.type) {
    case "Integer":
      writer.byte(COLUMN_TYPE_HASH_TAG.Integer);
      return;
    case "BigInt":
      writer.byte(COLUMN_TYPE_HASH_TAG.BigInt);
      return;
    case "Double":
      writer.byte(COLUMN_TYPE_HASH_TAG.Double);
      return;
    case "Boolean":
      writer.byte(COLUMN_TYPE_HASH_TAG.Boolean);
      return;
    case "Text":
      writer.byte(COLUMN_TYPE_HASH_TAG.Text);
      return;
    case "Enum": {
      writer.byte(COLUMN_TYPE_HASH_TAG.Enum);
      writer.u64(columnType.variants.length);
      for (const variant of columnType.variants) {
        writer.stringBytes(variant);
        writer.byte(0);
      }
      return;
    }
    case "EnumPayload":
      writer.byte(COLUMN_TYPE_HASH_TAG.EnumPayload);
      writer.u64(columnType.cases.length);
      for (const enumCase of columnType.cases) {
        writer.stringBytes(enumCase.name);
        writer.byte(0);
        writer.u64(enumCase.fields.length);
        for (const field of enumCase.fields) {
          writer.stringBytes(field.name);
          writer.byte(0);
          hashColumnType(writer, field.column_type, format);
          writer.byte(field.nullable ? 1 : 0);
        }
      }
      return;
    case "Timestamp":
      writer.byte(COLUMN_TYPE_HASH_TAG.Timestamp);
      return;
    case "Uuid":
      writer.byte(COLUMN_TYPE_HASH_TAG.Uuid);
      return;
    case "Bytea":
      writer.byte(format.byteaTag);
      return;
    case "Json":
      writer.byte(COLUMN_TYPE_HASH_TAG.Json);
      if (columnType.schema) {
        writer.byte(1);
        const encoded = new TextEncoder().encode(
          JSON.stringify(canonicalizeJsonObject(columnType.schema)),
        );
        writer.u64(encoded.length);
        writer.bytes(encoded);
      } else {
        writer.byte(0);
      }
      return;
    case "Array":
      writer.byte(COLUMN_TYPE_HASH_TAG.Array);
      hashColumnType(writer, columnType.element, format);
      return;
    case "Row":
      writer.byte(COLUMN_TYPE_HASH_TAG.Row);
      hashColumns(writer, columnType.columns, format);
      return;
  }
}

function canonicalizeJsonObject(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeJsonObject);
  }
  if (value === null || typeof value !== "object") {
    return value;
  }

  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => compareUnicodeScalarOrder(left, right))
      .map(([key, entry]) => [key, canonicalizeJsonObject(entry)]),
  );
}

function compareUnicodeScalarOrder(left: string, right: string): number {
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.length && rightIndex < right.length) {
    const leftCodePoint = left.codePointAt(leftIndex)!;
    const rightCodePoint = right.codePointAt(rightIndex)!;
    if (leftCodePoint !== rightCodePoint) {
      return leftCodePoint - rightCodePoint;
    }
    leftIndex += leftCodePoint > 0xffff ? 2 : 1;
    rightIndex += rightCodePoint > 0xffff ? 2 : 1;
  }
  return left.length - right.length;
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
  if (!indexedColumnsEqual(left.branchBy, right.branchBy)) {
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
