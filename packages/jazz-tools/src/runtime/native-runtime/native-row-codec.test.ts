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
  assertTerminalRootDescriptorCompatible,
  fieldIndex,
  readDescriptor,
  readNativeRowDescriptor,
  writeNativeRowDescriptor,
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
  it("pins explicit hidden metadata tag 2 and rejects unknown publication tags", () => {
    const descriptor = [
      { kind: "hidden-metadata" as const, name: "schema_version", valueType: { tag: 3 } },
    ];
    const writer = new PostcardWriter();
    writeNativeRowDescriptor(writer, descriptor);
    expect(Buffer.from(writer.finish()).toString("hex")).toBe(
      "01020e736368656d615f76657273696f6e03",
    );
    expect(readNativeRowDescriptor(new PostcardReader(writer.finish()))).toEqual(descriptor);
    expect(() => readNativeRowDescriptor(new PostcardReader(Uint8Array.from([1, 3])))).toThrow();
  });

  it("round-trips sealed internal physical value types without exposing them as columns", () => {
    const descriptor = [
      { name: "raw_text", valueType: { tag: 10, internal: { tag: 0 } } },
      { name: "raw_bytes", valueType: { tag: 10, internal: { tag: 1 } } },
      { name: "stored_bytes", valueType: { tag: 10, internal: { tag: 2, kind: 0 } } },
      { name: "stored_text", valueType: { tag: 10, internal: { tag: 2, kind: 1 } } },
      { name: "stored_json", valueType: { tag: 10, internal: { tag: 2, kind: 2 } } },
    ];
    const writer = new PostcardWriter();
    writeDescriptor(writer, descriptor);

    expect(readDescriptor(new PostcardReader(writer.finish()))).toEqual(descriptor);
  });

  it("rejects the old UUID-at-tag-10 descriptor instead of treating it compatibly", () => {
    const writer = new PostcardWriter();
    expect(() => writeDescriptor(writer, [{ name: "old_uuid", valueType: { tag: 10 } }])).toThrow(
      "missing physical type for ValueType::Internal",
    );
  });

  it("accepts nested terminal descriptors with producer fields beyond the root output", () => {
    const columns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ];
    const descriptor = [
      { name: "__jazz_terminal_row_key", valueType: { tag: 11 } },
      { name: "title", valueType: { tag: 8 } },
      {
        name: "members",
        valueType: { tag: 14, inner: { tag: 16, record: [] } },
      },
    ];

    expect(() => assertTerminalRootDescriptorCompatible(descriptor, columns)).not.toThrow();
  });

  it("rejects terminal descriptors that omit, reorder, rename, or change required root fields", () => {
    const columns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "body", column_type: { type: "Text" }, nullable: false },
    ];
    const missingRootField = [{ name: "__jazz_terminal_row_key", valueType: { tag: 11 } }];
    const wrongRootType = [
      { name: "__jazz_terminal_row_key", valueType: { tag: 11 } },
      { name: "title", valueType: { tag: 4 } },
      { name: "members", valueType: { tag: 14, inner: { tag: 16, record: [] } } },
    ];
    const reorderedSameTypeFields = [
      { name: "__jazz_terminal_row_key", valueType: { tag: 11 } },
      { name: "body", valueType: { tag: 8 } },
      { name: "title", valueType: { tag: 8 } },
    ];
    const wrongKeyName = [
      { name: "not_the_terminal_key", valueType: { tag: 11 } },
      { name: "title", valueType: { tag: 8 } },
      { name: "body", valueType: { tag: 8 } },
    ];

    expect(() => assertTerminalRootDescriptorCompatible(missingRootField, columns)).toThrow(
      "terminal root descriptor does not match the public projection",
    );
    expect(() => assertTerminalRootDescriptorCompatible(wrongRootType, columns)).toThrow(
      "terminal root descriptor does not match the public projection",
    );
    expect(() => assertTerminalRootDescriptorCompatible(reorderedSameTypeFields, columns)).toThrow(
      "terminal root descriptor does not match the public projection",
    );
    expect(() => assertTerminalRootDescriptorCompatible(wrongKeyName, columns)).toThrow(
      "terminal root descriptor does not match the public projection",
    );
  });

  it("accepts only the canonical CurrentRow carrier layout", () => {
    const columns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ];
    const currentRow = [
      { name: "row_uuid", valueType: { tag: 11 } },
      { name: "_app_title", valueType: { tag: 15, inner: { tag: 8 } } },
      { name: "_app_done", valueType: { tag: 15, inner: { tag: 7 } } },
      { name: "$createdBy", valueType: { tag: 8 } },
    ];
    const nullableLogicalNames = [
      { name: "__jazz_terminal_row_key", valueType: { tag: 11 } },
      { name: "title", valueType: { tag: 15, inner: { tag: 8 } } },
      { name: "done", valueType: { tag: 15, inner: { tag: 7 } } },
    ];
    const reorderedCarriers = [currentRow[0]!, currentRow[2]!, currentRow[1]!];

    expect(() => assertTerminalRootDescriptorCompatible(currentRow, columns)).not.toThrow();
    expect(() => assertTerminalRootDescriptorCompatible(nullableLogicalNames, columns)).toThrow(
      "terminal root descriptor does not match the public projection",
    );
    expect(() => assertTerminalRootDescriptorCompatible(reorderedCarriers, columns)).toThrow(
      "terminal root descriptor does not match the public projection",
    );
  });

  it("does not infer _app_ aliases when resolving raw record field positions", () => {
    const descriptor = [
      { name: "_app_title", valueType: { tag: 8 } },
      { name: "title", valueType: { tag: 8 } },
    ];

    expect(fieldIndex(descriptor, "_app_title")).toBe(0);
    expect(fieldIndex(descriptor, "title")).toBe(1);
    expect(() => fieldIndex([{ name: "_app_title", valueType: { tag: 8 } }], "title")).toThrow(
      "missing title field",
    );
  });

  it("requires payload enum terminal descriptors to preserve their declared case layouts", () => {
    const columns: ColumnDescriptor[] = [
      {
        name: "event",
        column_type: {
          type: "EnumPayload",
          cases: [
            {
              name: "message",
              fields: [
                { name: "text", column_type: { type: "Text" }, nullable: false },
                { name: "level", column_type: { type: "Integer" }, nullable: true },
              ],
            },
          ],
        },
        nullable: false,
      },
    ];
    const descriptor = [
      { name: "__jazz_terminal_row_key", valueType: { tag: 11 } },
      {
        name: "event",
        valueType: {
          tag: 17,
          enumSchema: {
            registryId: 37,
            name: "physical_event",
            cases: [
              {
                name: "message",
                payload: [
                  { name: "text", valueType: { tag: 8 } },
                  { name: "level", valueType: { tag: 15, inner: { tag: 4 } } },
                ],
              },
            ],
          },
        },
      },
    ];
    const wrongFieldType = structuredClone(descriptor);
    wrongFieldType[1]!.valueType.enumSchema!.cases![0]!.payload[1]!.valueType = { tag: 8 };

    expect(() => assertTerminalRootDescriptorCompatible(descriptor, columns)).not.toThrow();
    expect(() => assertTerminalRootDescriptorCompatible(wrongFieldType, columns)).toThrow(
      "terminal root descriptor does not match the public projection",
    );
  });

  it("keeps mutation cells byte-for-byte aligned with packed row values", () => {
    const nestedColumns: ColumnDescriptor[] = [
      { name: "label", column_type: { type: "Text" }, nullable: false },
    ];
    const columns: ColumnDescriptor[] = [
      { name: "active", column_type: { type: "Boolean" }, nullable: false },
      {
        name: "choice",
        column_type: { type: "Enum", variants: ["draft", "published"] },
        nullable: false,
      },
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
      `"06010661637469766507010663686f6963650801066c6162656c730e0801066e65737465640901046e6f74650f0801067370617273650f04480001070000001c0000002c00000047000000027075626c6973686564020000000c000000026f6e650274776f010000000000004000800000000000000106000000026368696c6400"`,
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
              ? {
                  name: "value",
                  column_type: { type: "Enum", variants: ["draft", "published"] },
                  nullable: false,
                }
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
    expect(bytesToHex(row)).toMatchInlineSnapshot(`"01010576616c75650f0f08020100"`);
  });

  it("round-trips scalar payload enum cases, nullable fields, and descriptor metadata", () => {
    const columns = [
      {
        name: "event",
        column_type: {
          type: "EnumPayload" as const,
          cases: [
            {
              name: "message",
              fields: [
                { name: "text", column_type: { type: "Text" as const }, nullable: false },
                { name: "level", column_type: { type: "Integer" as const }, nullable: true },
              ],
            },
            {
              name: "closed",
              fields: [
                { name: "code", column_type: { type: "Integer" as const }, nullable: false },
              ],
            },
          ],
        },
        nullable: true,
      },
    ];
    const payload = {
      type: "Enum" as const,
      value: {
        case: "message",
        values: [{ type: "Text" as const, value: "hello" }, { type: "Null" as const }],
      },
    };
    const encoded = encodeNativeRowValues(columns, [payload]);
    expect(decodeNativeRowValues(columns, encoded)).toEqual([payload]);
    const storage = storageColumnValueType(columns[0]!);
    expect(storage).toMatchObject({ tag: 15, inner: { tag: 17, enumSchema: { name: "event" } } });
    expect(storage.inner?.enumSchema?.cases?.[0]?.payload[0]).toMatchObject({
      name: "text",
      valueType: { tag: 8 },
    });
    expect(() =>
      encodeNativeRowValues(columns, [{ type: "Enum", value: { case: "missing", values: [] } }]),
    ).toThrow("invalid Enum payload case or width");
  });

  it("round-trips the Record descriptor payload before reading the next field", () => {
    const writer = new PostcardWriter();
    writeDescriptor(writer, [
      {
        name: "nested",
        valueType: { tag: 16, record: [{ name: "label", valueType: { tag: 8 } }] },
      },
      { name: "count", valueType: { tag: 4 } },
    ]);
    writer.u64(42);

    const reader = new PostcardReader(writer.finish());
    expect(readDescriptor(reader)).toEqual([
      {
        name: "nested",
        valueType: { tag: 16, record: [{ name: "label", valueType: { tag: 8 } }] },
      },
      { name: "count", valueType: { tag: 4 } },
    ]);
    expect(reader.u64()).toBe(42);
  });

  it("round-trips a payload enum descriptor at ValueType tag 17", () => {
    // Keep this fixture explicit: a tag-16 decoder which merely consumes the
    // enum header, or skips a case payload descriptor, leaves the trailing
    // value unread and is rejected below.
    const descriptor: Parameters<typeof writeDescriptor>[1] = [
      {
        name: "event",
        valueType: {
          tag: 17,
          enumSchema: {
            registryId: 41,
            name: "event",
            cases: [
              { name: "connected", payload: [] },
              {
                name: "message",
                payload: [
                  { name: "body", valueType: { tag: 8 } },
                  { name: "priority", valueType: { tag: 15, inner: { tag: 4 } } },
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
      new Set([
        ...Array.from({ length: 10 }, (_, tag) => tag),
        ...Array.from({ length: 6 }, (_, index) => index + 11),
      ]),
    );
    expect(descriptor[9]?.valueType).toMatchObject({
      tag: 12,
      enumSchema: { name: "mode", variants: ["low", "high"] },
    });
    expect(descriptor[10]?.valueType.members?.map((member) => member.tag)).toEqual([0, 5, 15, 4]);
    expect(descriptor[13]?.valueType).toMatchObject({
      tag: 15,
      inner: { tag: 14, inner: { tag: 15 } },
    });
    expect(descriptor[15]?.valueType).toMatchObject({ tag: 14, inner: { tag: 16 } });

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

  it("requires exact numeric BigInt values while preserving signed i64 boundaries", () => {
    const bigintColumn = [
      { name: "value", column_type: { type: "BigInt" as const }, nullable: false },
    ];
    const minI64 = -(1n << 63n);
    const maxI64 = (1n << 63n) - 1n;

    for (const value of [
      -Number.MAX_SAFE_INTEGER,
      -(Number.MAX_SAFE_INTEGER - 1),
      Number.MAX_SAFE_INTEGER - 1,
      Number.MAX_SAFE_INTEGER,
      minI64,
      maxI64,
    ]) {
      const encoded = encodeNativeRowValues(bigintColumn, [{ type: "BigInt", value }]);
      expect(decodeNativeRowValues(bigintColumn, encoded)).toEqual([
        { type: "BigInt", value: BigInt(value) },
      ]);
    }

    const maxSafePlusOne = Number.MAX_SAFE_INTEGER + 1;
    // This source literal is rounded by JavaScript before it reaches the codec.
    const roundedUnsafeNegative = -9_007_199_254_740_993;
    expect(roundedUnsafeNegative).toBe(-9_007_199_254_740_992);
    for (const value of [maxSafePlusOne, roundedUnsafeNegative]) {
      expect(() => encodeNativeRowValues(bigintColumn, [{ type: "BigInt", value }])).toThrow(
        "BigInt value must be a safe integer when passed as a number",
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
      Uint8Array.from([...uuidBytes(id), 2, ...new TextEncoder().encode(name)]);
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
      { name: "__jazz_terminal_row_key", valueType: { tag: 11 } },
      { name: "children", valueType: { tag: 14, inner: { tag: 16, record: [] } } },
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
