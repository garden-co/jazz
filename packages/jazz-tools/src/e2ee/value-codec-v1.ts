/**
 * jazz.e2ee.logical-value.v1 keeps the original E2EE typed-record framing.
 * Its inline text/JSON/enum/bytes discriminator is 2, independently of the
 * current native binding transport's discriminator 0. No indirect value is
 * admitted here. The hand-authored equality corpus pins the complete grammar.
 */
import { Utf8Decoder } from "../runtime/utf8.js";
import type { ColumnDescriptor, ColumnType, Value } from "../drivers/types.js";
import {
  decodeNativeRowValues,
  encodeNativeRowValues,
  decodeRecordValue,
  nativeFixedValueSize,
  storageColumnTypeToValueType,
  storageColumnValueType,
} from "../runtime/native-runtime/native-row-codec.js";

function u32(bytes: Uint8Array, offset: number): number {
  if (offset < 0 || offset + 4 > bytes.length) throw new Error("Invalid E2EE value v1 length");
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(offset, true);
}

function record(
  columns: readonly ColumnDescriptor[],
  bytes: Uint8Array,
  from: number,
  to: number,
): void {
  const descriptor = columns.map((column) => ({
    name: column.name,
    valueType: storageColumnValueType(column),
  }));
  for (let index = 0; index < columns.length; index += 1) {
    const field = decodeRecordValue(descriptor, bytes, index);
    if (field !== null) value(columns[index]!.column_type, field, from, to);
  }
}

function nested(
  columns: readonly ColumnDescriptor[],
  bytes: Uint8Array,
  from: number,
  to: number,
): void {
  if (bytes[0] !== 0 && bytes[0] !== 1) throw new Error("Invalid E2EE value v1 row identity");
  const offset = 1 + (bytes[0] === 1 ? 16 : 0);
  const length = u32(bytes, offset);
  if (offset + 4 + length !== bytes.length) throw new Error("Invalid E2EE value v1 row length");
  record(columns, bytes.subarray(offset + 4), from, to);
}

function value(type: ColumnType, bytes: Uint8Array, from: number, to: number): void {
  switch (type.type) {
    case "Text":
    case "Json":
    case "Enum":
    case "Bytea":
      if (bytes[0] !== from) throw new Error("Unsupported E2EE value v1 scalar discriminator");
      bytes[0] = to;
      return;
    case "Row":
      nested(type.columns, bytes, from, to);
      return;
    case "EnumPayload": {
      const length = u32(bytes, 0);
      if (length > bytes.length - 4) throw new Error("Invalid E2EE value v1 enum name");
      const name = new Utf8Decoder({ fatal: true }).decode(bytes.subarray(4, 4 + length));
      const variant = type.cases.find((entry) => entry.name === name);
      if (!variant) throw new Error("Unknown E2EE value v1 enum case");
      nested(variant.fields, bytes.subarray(4 + length), from, to);
      return;
    }
    case "Array": {
      // Fixed-width scalar elements contain no inline discriminator.
      if (nativeFixedValueSize(storageColumnTypeToValueType(type.element)) != null) return;
      const count = u32(bytes, 0);
      const header = 4 + Math.max(0, count - 1) * 4;
      if (header > bytes.length || (count === 0 && bytes.length !== 4))
        throw new Error("Invalid E2EE value v1 array offsets");
      let start = header;
      for (let index = 0; index < count; index += 1) {
        const end = index + 1 === count ? bytes.length : u32(bytes, 4 + index * 4);
        if (end < start || end > bytes.length)
          throw new Error("Invalid E2EE value v1 array offset");
        value(type.element, bytes.subarray(start, end), from, to);
        start = end;
      }
      return;
    }
  }
}

export function encodeE2eeValueV1(
  columns: readonly ColumnDescriptor[],
  values: readonly Value[],
): Uint8Array {
  const bytes = encodeNativeRowValues(columns, values);
  record(columns, bytes, 0, 2);
  return bytes;
}

export function decodeE2eeValueV1(
  columns: readonly ColumnDescriptor[],
  encoded: Uint8Array,
): Value[] {
  const bytes = encoded.slice();
  record(columns, bytes, 2, 0);
  return decodeNativeRowValues(columns, bytes);
}
