import { describe, expect, it } from "vitest";
import type { ColumnDescriptor, WasmSchema } from "../../drivers/types.js";
import { createRecord } from "./native-codec.js";
import { decodeNestedRowBytes, formatUuid, rowsFromBatches } from "./native-runtime-adapter.js";
import { nativeRowFieldPlanCacheKey, valueTypeCacheKey } from "./native-row-descriptor-key.js";

describe("formatUuid", () => {
  it("formats the first 16 bytes without depending on the view offset", () => {
    const bytes = Uint8Array.from([
      255, 255, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
      0x0e, 0x0f, 255,
    ]);

    expect(formatUuid(bytes.subarray(2, 18))).toBe("00010203-0405-0607-0809-0a0b0c0d0e0f");
  });
});

describe("native row descriptor cache keys", () => {
  it("uses explicit field provenance for hybrid physical and logical user_ fields", () => {
    const schema = {
      notes: {
        columns: [{ name: "check", column_type: { type: "Text" }, nullable: false }],
      },
    } satisfies WasmSchema;
    const descriptor = [
      { kind: "physical-column" as const, name: "user_check", valueType: { tag: 8 } as const },
      { kind: "logical-field" as const, name: "user_check", valueType: { tag: 8 } as const },
    ];
    const hybridBatch = {
      table: "notes",
      descriptor,
      rows: [
        {
          rowId: uuidBytes("00000000-0000-0000-0000-0000000000a0"),
          deleted: false,
          raw: createRecord(descriptor, [
            Uint8Array.from([2, ...new TextEncoder().encode("included")]),
            Uint8Array.from([2, ...new TextEncoder().encode("collector")]),
          ]),
        },
      ],
    };
    const hybrid = rowsFromBatches([hybridBatch], schema)[0] as {
      valuesByColumn?: Map<string, unknown>;
    };
    expect(hybrid?.valuesByColumn?.get("check")).toMatchObject({ type: "Text" });
    expect(hybrid?.valuesByColumn?.get("user_check")).toMatchObject({ type: "Bytea" });
    expect(nativeRowFieldPlanCacheKey(hybridBatch)).not.toBe(
      nativeRowFieldPlanCacheKey({
        ...hybridBatch,
        descriptor: [{ ...descriptor[0]!, kind: "logical-field" as const }, descriptor[1]!],
      }),
    );
  });

  it("normalizes physical grouped and aggregate fields without rewriting a logical aggregate collision", () => {
    const schema = {
      totals: {
        columns: [
          { name: "category", column_type: { type: "Text" }, nullable: false },
          { name: "total", column_type: { type: "Integer" }, nullable: false },
        ],
      },
    } satisfies WasmSchema;
    const descriptor = [
      { kind: "physical-column" as const, name: "user_category", valueType: { tag: 8 } as const },
      {
        kind: "physical-column" as const,
        name: "user___jazz_aggregate_total",
        valueType: { tag: 4 } as const,
      },
      {
        kind: "logical-field" as const,
        name: "user___jazz_aggregate_total",
        valueType: { tag: 8 } as const,
      },
    ];
    const batch = {
      table: "totals",
      descriptor,
      rows: [
        {
          rowId: uuidBytes("00000000-0000-0000-0000-0000000000a1"),
          deleted: false,
          raw: createRecord(descriptor, [
            Uint8Array.from([2, ...new TextEncoder().encode("books")]),
            i32Bytes(42),
            Uint8Array.from([2, ...new TextEncoder().encode("logical collision")]),
          ]),
        },
      ],
    };

    const decoded = rowsFromBatches([batch], schema)[0] as {
      valuesByColumn?: Map<string, unknown>;
    };
    expect(decoded?.valuesByColumn?.get("category")).toEqual({ type: "Text", value: "books" });
    expect(decoded?.valuesByColumn?.get("total")).toEqual({ type: "Integer", value: 42 });
    expect(decoded?.valuesByColumn?.get("user___jazz_aggregate_total")).toMatchObject({
      type: "Bytea",
    });
  });

  it("normalizes physical user_total while preserving a logical user_total field", () => {
    const schema = {
      totals: {
        columns: [{ name: "total", column_type: { type: "Integer" }, nullable: false }],
      },
    } satisfies WasmSchema;
    const descriptor = [
      { kind: "physical-column" as const, name: "user_total", valueType: { tag: 4 } as const },
      { kind: "logical-field" as const, name: "user_total", valueType: { tag: 8 } as const },
    ];
    const batch = {
      table: "totals",
      descriptor,
      rows: [
        {
          rowId: uuidBytes("00000000-0000-0000-0000-0000000000a2"),
          deleted: false,
          raw: createRecord(descriptor, [
            i32Bytes(7),
            Uint8Array.from([2, ...new TextEncoder().encode("collector total")]),
          ]),
        },
      ],
    };

    const decoded = rowsFromBatches([batch], schema)[0] as {
      valuesByColumn?: Map<string, unknown>;
    };
    expect(decoded?.valuesByColumn?.get("total")).toEqual({ type: "Integer", value: 7 });
    expect(decoded?.valuesByColumn?.get("user_total")).toMatchObject({ type: "Bytea" });
  });

  it("includes the table identity", () => {
    const descriptor = [{ kind: "physical-column" as const, name: "value", valueType: { tag: 8 } }];

    expect(nativeRowFieldPlanCacheKey({ table: "first", descriptor })).not.toBe(
      nativeRowFieldPlanCacheKey({ table: "second", descriptor }),
    );
  });

  it("includes payload enum registry identity when cases match", () => {
    const firstRegistry = {
      tag: 17,
      enumSchema: {
        registryId: 3,
        name: "event",
        cases: [{ name: "message", payload: [{ name: "body", valueType: { tag: 8 } }] }],
      },
    };
    const secondRegistry = {
      tag: 17,
      enumSchema: {
        registryId: 4,
        name: "event",
        cases: [{ name: "message", payload: [{ name: "body", valueType: { tag: 8 } }] }],
      },
    };

    expect(valueTypeCacheKey(firstRegistry)).not.toBe(valueTypeCacheKey(secondRegistry));
  });

  it("includes recursive descriptor and enum schema data", () => {
    const nestedText = {
      tag: 16,
      record: [{ name: "body", valueType: { tag: 8 } }],
    };
    const nestedInteger = {
      tag: 16,
      record: [{ name: "body", valueType: { tag: 4 } }],
    };
    const tupleWithText = { tag: 13, members: [{ tag: 7 }, { tag: 8 }] };
    const tupleWithInteger = { tag: 13, members: [{ tag: 7 }, { tag: 4 }] };
    const textArray = { tag: 14, inner: { tag: 8 } };
    const integerArray = { tag: 14, inner: { tag: 4 } };
    const nullableText = { tag: 15, inner: { tag: 8 } };
    const nullableInteger = { tag: 15, inner: { tag: 4 } };
    const scalarEnum = {
      tag: 12,
      enumSchema: { registryId: 7, name: "status", variants: ["draft", "published"] },
    };
    const otherScalarEnumRegistry = {
      tag: 12,
      enumSchema: { registryId: 8, name: "status", variants: ["draft", "published"] },
    };
    const otherScalarEnumVariants = {
      tag: 12,
      enumSchema: { registryId: 7, name: "status", variants: ["draft", "archived"] },
    };
    const payloadEnum = {
      tag: 17,
      enumSchema: {
        registryId: 3,
        name: "event",
        cases: [{ name: "message", payload: [{ name: "body", valueType: { tag: 8 } }] }],
      },
    };
    const changedPayloadEnum = {
      tag: 17,
      enumSchema: {
        registryId: 3,
        name: "event",
        cases: [{ name: "message", payload: [{ name: "body", valueType: { tag: 4 } }] }],
      },
    };
    const changedPayloadEnumCase = {
      tag: 17,
      enumSchema: {
        registryId: 3,
        name: "event",
        cases: [{ name: "reaction", payload: [{ name: "body", valueType: { tag: 8 } }] }],
      },
    };

    expect(valueTypeCacheKey(nestedText)).not.toBe(valueTypeCacheKey(nestedInteger));
    expect(valueTypeCacheKey(tupleWithText)).not.toBe(valueTypeCacheKey(tupleWithInteger));
    expect(valueTypeCacheKey(textArray)).not.toBe(valueTypeCacheKey(integerArray));
    expect(valueTypeCacheKey(nullableText)).not.toBe(valueTypeCacheKey(nullableInteger));
    expect(valueTypeCacheKey(scalarEnum)).not.toBe(valueTypeCacheKey(otherScalarEnumRegistry));
    expect(valueTypeCacheKey(scalarEnum)).not.toBe(valueTypeCacheKey(otherScalarEnumVariants));
    expect(valueTypeCacheKey(payloadEnum)).not.toBe(valueTypeCacheKey(changedPayloadEnum));
    expect(valueTypeCacheKey(payloadEnum)).not.toBe(valueTypeCacheKey(changedPayloadEnumCase));
    expect(
      nativeRowFieldPlanCacheKey({
        table: "events",
        descriptor: [{ kind: "physical-column", name: "event", valueType: payloadEnum }],
      }),
    ).not.toBe(
      nativeRowFieldPlanCacheKey({
        table: "events",
        descriptor: [{ kind: "physical-column", name: "event", valueType: changedPayloadEnum }],
      }),
    );
  });

  it("rebuilds a row decoder plan when a nested record descriptor changes", () => {
    const childColumns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ];
    const schema = {
      parents: {
        columns: [
          {
            name: "child",
            column_type: { type: "Row", columns: childColumns },
            nullable: false,
          },
        ],
      },
    } satisfies WasmSchema;
    const firstChildDescriptor = [
      { name: "row_uuid", valueType: { tag: 11 } },
      { name: "title", valueType: { tag: 8 } },
    ];
    const secondChildDescriptor = [
      { name: "row_uuid", valueType: { tag: 11 } },
      { name: "title", valueType: { tag: 8 } },
      { name: "ignored_fixed_field", valueType: { tag: 4 } },
    ];
    const firstDescriptor = [
      {
        kind: "physical-column" as const,
        name: "child",
        valueType: { tag: 16, record: firstChildDescriptor },
      },
    ];
    const secondDescriptor = [
      {
        kind: "physical-column" as const,
        name: "child",
        valueType: { tag: 16, record: secondChildDescriptor },
      },
    ];
    const childId = "00000000-0000-0000-0000-0000000000c1";
    const firstBatch = {
      table: "parents",
      descriptor: firstDescriptor,
      rows: [
        {
          rowId: uuidBytes("00000000-0000-0000-0000-0000000000a1"),
          deleted: false,
          raw: createRecord(firstDescriptor, [
            createRecord(firstChildDescriptor, [
              uuidBytes(childId),
              Uint8Array.from([2, ...new TextEncoder().encode("first")]),
            ]),
          ]),
        },
      ],
    };
    const secondBatch = {
      table: "parents",
      descriptor: secondDescriptor,
      rows: [
        {
          rowId: uuidBytes("00000000-0000-0000-0000-0000000000a2"),
          deleted: false,
          raw: createRecord(secondDescriptor, [
            createRecord(secondChildDescriptor, [
              uuidBytes(childId),
              Uint8Array.from([2, ...new TextEncoder().encode("second")]),
              i32Bytes(42),
            ]),
          ]),
        },
      ],
    };

    expect(rowsFromBatches([firstBatch], schema)).toEqual([
      {
        table: "parents",
        id: "00000000-0000-0000-0000-0000000000a1",
        values: [
          {
            type: "Row",
            value: {
              id: childId,
              values: [{ type: "Text", value: "first" }],
            },
          },
        ],
      },
    ]);
    expect(rowsFromBatches([secondBatch], schema)).toEqual([
      {
        table: "parents",
        id: "00000000-0000-0000-0000-0000000000a2",
        values: [
          {
            type: "Row",
            value: {
              id: childId,
              values: [{ type: "Text", value: "second" }],
            },
          },
        ],
      },
    ]);
  });
});

describe("nested row physical carriers", () => {
  const id = "00000000-0000-0000-0000-000000000002";
  const columns: ColumnDescriptor[] = [
    { name: "title", column_type: { type: "Text" }, nullable: false },
  ];
  const descriptor = [
    { name: "row_uuid", valueType: { tag: 11 } as const },
    { name: "title", valueType: { tag: 8 } as const },
  ];

  it("decodes a full snapshot record without stripping its row_uuid field", () => {
    const bytes = createRecord(descriptor, [
      uuidBytes(id),
      Uint8Array.from([2, ...new TextEncoder().encode("snapshot")]),
    ]);

    const row = decodeNestedRowBytes(columns, bytes, descriptor, "full-record");

    expect(row.id).toBe(id);
    expect(row.values).toEqual([{ type: "Text", value: "snapshot" }]);
  });

  it("decodes an explicitly keyed terminal payload with the same descriptor", () => {
    const bytes = concatBytes([
      uuidBytes(id),
      createRecord(descriptor.slice(1), [
        Uint8Array.from([2, ...new TextEncoder().encode("terminal")]),
      ]),
    ]);

    const row = decodeNestedRowBytes(columns, bytes, descriptor, "keyed-terminal");

    expect(row.id).toBe(id);
    expect(row.values).toEqual([{ type: "Text", value: "terminal" }]);
  });

  it("decodes the complete raw Record element carried by a terminal array", () => {
    const todoColumns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "priority", column_type: { type: "Integer" }, nullable: true },
      { name: "owner_id", column_type: { type: "Uuid" }, nullable: true },
      { name: "tags", column_type: { type: "Array", element: { type: "Text" } }, nullable: false },
    ];
    const todoDescriptor = [
      { name: "row_uuid", valueType: { tag: 11 } as const },
      { name: "title", valueType: { tag: 8 } as const },
      { name: "done", valueType: { tag: 7 } as const },
      { name: "priority", valueType: { tag: 15, inner: { tag: 4 } } as const },
      { name: "owner_id", valueType: { tag: 15, inner: { tag: 11 } } as const },
      { name: "tags", valueType: { tag: 14, inner: { tag: 8 } } as const },
    ];
    const bytes = createRecord(todoDescriptor, [
      uuidBytes("e20942bf-8789-e652-23fd-c86c3a105743"),
      Uint8Array.from([2, ...new TextEncoder().encode("owned-todo")]),
      Uint8Array.of(0),
      presentBytes(Uint8Array.of(1, 0, 0, 0)),
      presentBytes(uuidBytes("06839e1b-9b29-732c-1b39-8ee592bd2a68")),
      concatBytes([Uint8Array.of(1, 0, 0, 0, 2), new TextEncoder().encode("x")]),
    ]);

    expect(decodeNestedRowBytes(todoColumns, bytes, todoDescriptor, "full-record")).toMatchObject({
      id: "e20942bf-8789-e652-23fd-c86c3a105743",
      values: [
        { type: "Text", value: "owned-todo" },
        { type: "Boolean", value: false },
        { type: "Integer", value: 1 },
        { type: "Uuid", value: "06839e1b-9b29-732c-1b39-8ee592bd2a68" },
        { type: "Array", value: [{ type: "Text", value: "x" }] },
      ],
    });
  });
});
function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function i32Bytes(value: number): Uint8Array {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setInt32(0, value, true);
  return bytes;
}

function presentBytes(bytes: Uint8Array): Uint8Array {
  const out = new Uint8Array(bytes.length + 1);
  out[0] = 1;
  out.set(bytes, 1);
  return out;
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
