import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { PostcardReader, PostcardWriter } from "./native-codec.js";
import { encodeCellsForPatch, encodeCellsForRow } from "./native-runtime-adapter.js";
import {
  createRecord,
  decodeNativeTerminalRow,
  decodeNativeRowValues,
  decodeRecordValue,
  encodeNativeRowValues,
  readDescriptor,
  storageColumnValueType,
  writeDescriptor,
} from "./native-row-codec.js";
import type { ColumnDescriptor, Value } from "../../drivers/types.js";

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
  it("keeps mutation cells byte-for-byte aligned with packed row values", () => {
    const nestedColumns: ColumnDescriptor[] = [
      { name: "label", column_type: { type: "Text" }, nullable: false },
    ];
    const columns: ColumnDescriptor[] = [
      { name: "active", column_type: { type: "Boolean" }, nullable: false },
      { name: "choice", column_type: { type: "Enum" }, nullable: false },
      {
        name: "labels",
        column_type: { type: "Array", element: { type: "Text" } },
        nullable: false,
      },
      { name: "note", column_type: { type: "Text" }, nullable: true },
      {
        name: "nested",
        column_type: { type: "Row", columns: nestedColumns },
        nullable: false,
      },
      { name: "sparse", column_type: { type: "Integer" }, nullable: false, sparse: true },
    ];
    const values: Record<string, Value> = {
      active: { type: "Boolean", value: false },
      choice: { type: "Text", value: "published" },
      labels: {
        type: "Array",
        value: [
          { type: "Text", value: "one" },
          { type: "Text", value: "two" },
        ],
      },
      note: { type: "Null" },
      nested: {
        type: "Row",
        value: {
          id: "00000000-0000-4000-8000-000000000001",
          values: [{ type: "Text", value: "child" }],
        },
      },
      sparse: { type: "Integer", value: 7 },
    };
    const sortedColumns = [...columns].sort((left, right) => left.name.localeCompare(right.name));
    const writer = new PostcardWriter();
    writeDescriptor(
      writer,
      sortedColumns.map((column) => ({
        name: column.name,
        valueType: storageColumnValueType(column),
      })),
    );
    writer.bytes(
      encodeNativeRowValues(
        sortedColumns,
        sortedColumns.map((column) => values[column.name]!),
      ),
    );

    const cells = encodeCellsForRow({ columns }, values);
    expect(cells).toEqual(writer.finish());
    expect(bytesToHex(cells)).toMatchInlineSnapshot(
      `"06010661637469766507010663686f6963650801066c6162656c730d0801066e65737465640901046e6f74650e0801067370617273650e04440001070000001b00000029000000430000007075626c6973686564020000000b0000006f6e6574776f0100000000000040008000000000000001050000006368696c6400"`,
    );
  });

  it.each([
    ["scalar", { type: "Text", value: "wrong" }],
    ["nullable", { type: "Text", value: "wrong" }],
    ["array", { type: "Text", value: "wrong" }],
    ["row", { type: "Text", value: "wrong" }],
    ["enum", { type: "Boolean", value: true }],
  ] as const)("rejects an invalid %s value tag rather than encoding a fallback", (_kind, value) => {
    const column: ColumnDescriptor =
      _kind === "nullable"
        ? { name: "value", column_type: { type: "Boolean" }, nullable: true }
        : _kind === "array"
          ? {
              name: "value",
              column_type: { type: "Array", element: { type: "Text" } },
              nullable: false,
            }
          : _kind === "row"
            ? {
                name: "value",
                column_type: {
                  type: "Row",
                  columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
                },
                nullable: false,
              }
            : _kind === "enum"
              ? { name: "value", column_type: { type: "Enum" }, nullable: false }
              : { name: "value", column_type: { type: "Boolean" }, nullable: false };

    expect(() =>
      encodeCellsForRow({ columns: [column] }, { value } as Record<string, Value>),
    ).toThrow();
  });

  it("keeps an explicit sparse nullable null present for both rows and patches", () => {
    const column: ColumnDescriptor = {
      name: "value",
      column_type: { type: "Text" },
      nullable: true,
      sparse: true,
    };
    const writer = new PostcardWriter();
    writeDescriptor(writer, [{ name: column.name, valueType: storageColumnValueType(column) }]);
    writer.bytes(encodeNativeRowValues([column], [{ type: "Null" }]));
    const expected = writer.finish();

    const row = encodeCellsForRow({ columns: [column] }, { value: { type: "Null" } });
    const patch = encodeCellsForPatch({ columns: [column] }, { value: { type: "Null" } });
    expect(row).toEqual(expected);
    expect(patch).toEqual(expected);
    expect(bytesToHex(row)).toMatchInlineSnapshot(`"01010576616c75650e0e08020100"`);
  });

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

  it("round-trips a payload enum descriptor at ValueType tag 16", () => {
    // Keep this fixture explicit: a tag-16 decoder which merely consumes the
    // enum header, or skips a case payload descriptor, leaves the trailing
    // value unread and is rejected below.
    const descriptor: Parameters<typeof writeDescriptor>[1] = [
      {
        name: "event",
        valueType: {
          tag: 16,
          enumSchema: {
            registryId: 41,
            name: "event",
            cases: [
              { name: "connected", payload: [] },
              {
                name: "message",
                payload: [
                  { name: "body", valueType: { tag: 8 } },
                  { name: "priority", valueType: { tag: 14, inner: { tag: 4 } } },
                ],
              },
            ],
          },
        },
      },
      { name: "following", valueType: { tag: 4 } },
    ];
    const writer = new PostcardWriter();
    writeDescriptor(writer, descriptor);
    writer.u64(42);

    const reader = new PostcardReader(writer.finish());
    expect(readDescriptor(reader)).toEqual(descriptor);
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

  it("decodes terminal arrays as physical nested records rather than packed row envelopes", () => {
    const rootId = "00000000-0000-4000-8000-000000000001";
    const firstChildId = "00000000-0000-4000-8000-000000000002";
    const secondChildId = "00000000-0000-4000-8000-000000000003";
    const children = [
      { name: "children", column_type: { type: "Text" as const }, nullable: false },
    ];
    const columns = [
      {
        name: "children",
        column_type: {
          type: "Array" as const,
          element: { type: "Row" as const, columns: children },
        },
        nullable: false,
      },
    ];
    const childRecord = (id: string, name: string) =>
      Uint8Array.from([...uuidBytes(id), ...new TextEncoder().encode(name)]);
    const first = childRecord(firstChildId, "first");
    const second = childRecord(secondChildId, "second");
    const childArray = Uint8Array.from([
      2,
      0,
      0,
      0,
      4 + 4 + first.length,
      0,
      0,
      0,
      ...first,
      ...second,
    ]);
    const descriptor = [
      { name: "__jazz_terminal_row_key", valueType: { tag: 10 } },
      { name: "children", valueType: { tag: 13, inner: { tag: 15, record: [] } } },
    ];
    const raw = createRecord(descriptor, [uuidBytes(rootId), childArray]);

    expect(decodeNativeTerminalRow(rootId, columns, raw)).toMatchObject({
      id: rootId,
      values: [
        {
          type: "Array",
          value: [
            {
              type: "Row",
              value: { id: firstChildId, values: [{ type: "Text", value: "first" }] },
            },
            {
              type: "Row",
              value: { id: secondChildId, values: [{ type: "Text", value: "second" }] },
            },
          ],
        },
      ],
    });
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

function uuidBytes(id: string): Uint8Array {
  return Uint8Array.from(
    id
      .replaceAll("-", "")
      .match(/../g)!
      .map((hex) => Number.parseInt(hex, 16)),
  );
}
