import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  createRecord,
  decodeNativeRowValues,
  decodeRecordValue,
  encodeNativeRowValues,
  readDescriptor,
  writeDescriptor,
} from "./native-row-codec.js";

type NativeRowCodecFixture = {
  cases: NativeRowCodecCase[];
};
type NativeRowCodecCase = {
  name: string;
  descriptor_hex: string[];
  record_hex: string[];
  fields: { name: string; encoded_hex: string; decoded_hex: string | null }[];
};

describe("native row codec", () => {
  it("round-trips the Record descriptor payload before reading the next field", () => {
    const writer = new PostcardWriter();
    writeDescriptor(writer, [
      {
        name: "nested",
        valueType: { tag: 15, record: [{ name: "label", valueType: { tag: 8 } }] },
      },
      { name: "count", valueType: { tag: 4 } },
    ]);
    writer.u64(42);

    const reader = new PostcardReader(writer.finish());
    expect(readDescriptor(reader)).toEqual([
      {
        name: "nested",
        valueType: { tag: 15, record: [{ name: "label", valueType: { tag: 8 } }] },
      },
      { name: "count", valueType: { tag: 4 } },
    ]);
    expect(reader.u64()).toBe(42);
  });

  it("round-trips every Groove ValueType fixture, including depth-three nesting", () => {
    const fixture = nativeRowCodecFixture();
    const testCase = fixture.cases.find(
      (candidate) => candidate.name === "all_value_types_depth_three",
    );
    expect(testCase).toBeDefined();
    const descriptorBytes = hexToBytes(testCase!.descriptor_hex.join(""));
    const raw = hexToBytes(testCase!.record_hex.join(""));
    const descriptor = readDescriptor(new PostcardReader(descriptorBytes));

    expect(new Set(descriptor.map((field) => field.valueType.tag))).toEqual(
      new Set(Array.from({ length: 16 }, (_, tag) => tag)),
    );
    expect(descriptor[9]?.valueType).toMatchObject({
      tag: 11,
      enumSchema: { name: "mode", variants: ["low", "high"] },
    });
    expect(descriptor[10]?.valueType.members?.map((member) => member.tag)).toEqual([0, 5, 14, 4]);
    expect(descriptor[13]?.valueType).toMatchObject({
      tag: 14,
      inner: { tag: 13, inner: { tag: 14 } },
    });
    expect(descriptor[15]?.valueType).toMatchObject({ tag: 13, inner: { tag: 15 } });

    const descriptorWriter = new PostcardWriter();
    writeDescriptor(descriptorWriter, descriptor);
    expect(descriptorWriter.finish()).toEqual(descriptorBytes);
    expect(
      createRecord(
        descriptor,
        testCase!.fields.map((field) => hexToBytes(field.encoded_hex)),
      ),
    ).toEqual(raw);

    for (const [index, field] of testCase!.fields.entries()) {
      expect(descriptor[index]?.name).toBe(field.name);
      const decoded = decodeRecordValue(descriptor, raw, index);
      expect(decoded == null ? null : bytesToHex(decoded)).toBe(field.decoded_hex);
    }
  });

  it("round-trips signed storage values, including nested nullable and array values", () => {
    const columns = [
      { name: "i32_min", column_type: { type: "Integer" as const }, nullable: false },
      { name: "i32_negative_one", column_type: { type: "Integer" as const }, nullable: false },
      { name: "i32_zero", column_type: { type: "Integer" as const }, nullable: false },
      { name: "i32_max", column_type: { type: "Integer" as const }, nullable: false },
      { name: "i64_min", column_type: { type: "BigInt" as const }, nullable: false },
      { name: "i64_negative_one", column_type: { type: "BigInt" as const }, nullable: false },
      { name: "i64_zero", column_type: { type: "BigInt" as const }, nullable: false },
      { name: "i64_max", column_type: { type: "BigInt" as const }, nullable: false },
      { name: "nullable_i32", column_type: { type: "Integer" as const }, nullable: true },
      {
        name: "i64_array",
        column_type: { type: "Array" as const, element: { type: "BigInt" as const } },
        nullable: false,
      },
    ];
    const values = [
      { type: "Integer" as const, value: -2_147_483_648 },
      { type: "Integer" as const, value: -1 },
      { type: "Integer" as const, value: 0 },
      { type: "Integer" as const, value: 2_147_483_647 },
      { type: "BigInt" as const, value: -(1n << 63n) },
      { type: "BigInt" as const, value: -1n },
      { type: "BigInt" as const, value: 0n },
      { type: "BigInt" as const, value: (1n << 63n) - 1n },
      { type: "Integer" as const, value: -42 },
      {
        type: "Array" as const,
        value: [
          { type: "BigInt" as const, value: -(1n << 63n) },
          { type: "BigInt" as const, value: -1n },
          { type: "BigInt" as const, value: 0n },
          { type: "BigInt" as const, value: (1n << 63n) - 1n },
        ],
      },
    ];

    const encoded = encodeNativeRowValues(columns, values);
    expect(bytesToHex(encoded)).toBe(
      "00000080ffffffff00000000ffffff7f0000000000000080ffffffffffffffff0000000000000000ffffffffffffff7f01d6ffffff0000000000000080ffffffffffffffff0000000000000000ffffffffffffff7f",
    );
    expect(decodeNativeRowValues(columns, encoded)).toEqual(values);
  });

  it("rejects signed integer values outside their declared widths", () => {
    const integerColumn = [
      { name: "value", column_type: { type: "Integer" as const }, nullable: false },
    ];
    const bigintColumn = [
      { name: "value", column_type: { type: "BigInt" as const }, nullable: false },
    ];

    for (const value of [-2_147_483_649, 2_147_483_648]) {
      expect(() => encodeNativeRowValues(integerColumn, [{ type: "Integer", value }])).toThrow(
        "Integer value must be a signed 32-bit integer",
      );
    }
    for (const value of [-(1n << 63n) - 1n, 1n << 63n]) {
      expect(() => encodeNativeRowValues(bigintColumn, [{ type: "BigInt", value }])).toThrow(
        "BigInt value must be a signed 64-bit integer",
      );
    }
  });

  it("decodes sparse terminal carriers without leaking their presence tag", () => {
    const columns = [
      {
        name: "title",
        column_type: { type: "Text" as const },
        nullable: false,
        sparse: true,
      },
    ];

    // Outer nullable 1 means the wildcard current-row carrier contains the
    // field. The public value is still the unwrapped text.
    const encoded = encodeNativeRowValues(columns, [{ type: "Text", value: "hello" }]);
    expect(decodeNativeRowValues(columns, encoded)).toEqual([{ type: "Text", value: "hello" }]);
  });

  it("keeps sparse absence distinct from an explicit application null", () => {
    const columns = [
      {
        name: "ownerId",
        column_type: { type: "Uuid" as const },
        nullable: true,
        sparse: true,
      },
    ];

    // The first tag is sparse presence; the second is the application's
    // nullable value. Explicit null must consume both layers.
    const explicitNull = encodeNativeRowValues(columns, [{ type: "Null" }]);
    const sparseAbsence = encodeNativeRowValues(columns, []);
    expect(explicitNull).not.toEqual(sparseAbsence);
    expect(explicitNull[0]).toBe(1);
    expect(sparseAbsence[0]).toBe(0);
    expect(decodeNativeRowValues(columns, explicitNull)).toEqual([{ type: "Null" }]);
    expect(decodeNativeRowValues(columns, sparseAbsence)).toEqual([{ type: "Null" }]);
  });
});

function nativeRowCodecFixture(): NativeRowCodecFixture {
  return JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/native_row_codec.json", import.meta.url),
      "utf8",
    ),
  ) as NativeRowCodecFixture;
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
