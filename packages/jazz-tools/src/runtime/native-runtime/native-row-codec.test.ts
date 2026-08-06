import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  createRecord,
  decodeNativeRowValues,
  decodeRecordValue,
  encodeNativeRowValues,
  readDescriptor,
  storageColumnTypeToValueType,
  writeDescriptor,
} from "./native-row-codec.js";
import { encodeSchema } from "./schema-codec.js";

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
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
      { name: "count", valueType: { tag: 15 } },
    ]);
    writer.u64(42);

    const reader = new PostcardReader(writer.finish());
    expect(readDescriptor(reader)).toEqual([
      {
        name: "nested",
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
      { name: "count", valueType: { tag: 15 } },
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
      tag: 9,
      enumSchema: { name: "mode", variants: ["low", "high"] },
    });
    expect(descriptor[10]?.valueType.members?.map((member) => member.tag)).toEqual([0, 14, 12, 15]);
    expect(descriptor[13]?.valueType).toMatchObject({
      tag: 12,
      inner: { tag: 11, inner: { tag: 12 } },
    });
    expect(descriptor[15]?.valueType).toMatchObject({ tag: 11, inner: { tag: 13 } });

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
      "00000000ffffff7f00000080ffffffff0000000000000000ffffffffffffff7f0000000000000080ffffffffffffffff01d6ffff7f0000000000000000ffffffffffffff7f0000000000000080ffffffffffffffff",
    );
    expect(decodeNativeRowValues(columns, encoded)).toEqual(values);
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

describe("native row and schema value-type codecs", () => {
  it("keeps Jazz JSON schema tag 15 separate from Groove I32 descriptor tag 15", () => {
    const schema = encodeSchema({
      documents: {
        columns: [
          {
            name: "payload",
            column_type: { type: "Json", schema: { type: "object" } },
            nullable: false,
          },
        ],
      },
    });
    const schemaReader = new PostcardReader(schema);
    expect(schemaReader.u64()).toBe(1);
    expect(schemaReader.string()).toBe("documents");
    expect(schemaReader.u64()).toBe(1);
    expect(schemaReader.string()).toBe("payload");
    expect(schemaReader.u64()).toBe(15);
    expect(schemaReader.option((reader) => reader.string())).toBe('{"type":"object"}');

    expect(storageColumnTypeToValueType({ type: "Integer" })).toEqual({ tag: 15 });
    expect(storageColumnTypeToValueType({ type: "BigInt" })).toEqual({ tag: 14 });

    const descriptorWriter = new PostcardWriter();
    writeDescriptor(descriptorWriter, [
      { name: "count", valueType: storageColumnTypeToValueType({ type: "Integer" }) },
      {
        name: "nested",
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
    ]);
    descriptorWriter.u64(42);
    const descriptorReader = new PostcardReader(descriptorWriter.finish());
    expect(readDescriptor(descriptorReader)).toEqual([
      { name: "count", valueType: { tag: 15 } },
      {
        name: "nested",
        valueType: { tag: 13, record: [{ name: "label", valueType: { tag: 6 } }] },
      },
    ]);
    expect(descriptorReader.u64()).toBe(42);
  });
});
