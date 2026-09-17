import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import type { ColumnType, WasmSchema } from "../drivers/types.js";

import { schema as s } from "../index.js";
import { structuralSchemaHash, wasmSchemasEqual } from "./schema-utils.js";

function schemaWithColumnType(columnType: ColumnType, nullable = false): WasmSchema {
  return {
    values: {
      columns: [{ name: "value", column_type: columnType, nullable }],
    },
  };
}

type StructuralHashFixture = {
  schemaLayoutVersion: number;
  columnTypeCases: Array<{
    name: string;
    columnType: ColumnType;
    nullable: boolean;
    hash: string;
  }>;
};

const portableColumnTypeTags = [
  "Integer",
  "BigInt",
  "Double",
  "Boolean",
  "Text",
  "Json",
  "Enum",
  "EnumPayload",
  "Timestamp",
  "Uuid",
  "Bytea",
  "Array",
  "Row",
] as const;

type Assert<T extends true> = T;
type IsEqual<Left, Right> =
  (<T>() => T extends Left ? 1 : 2) extends <T>() => T extends Right ? 1 : 2 ? true : false;
type PortableColumnTypeTagsAreExhaustive = Assert<
  IsEqual<(typeof portableColumnTypeTags)[number], ColumnType["type"]>
>;

describe("structuralSchemaHash", () => {
  const structuralHashFixture = JSON.parse(
    readFileSync(
      new URL("../testing/fixtures/structural-schema-hashes.json", import.meta.url),
      "utf8",
    ),
  ) as StructuralHashFixture;

  it("matches Rust for every portable column type and representative nested shape", () => {
    expect(structuralHashFixture.schemaLayoutVersion).toBe(11);
    expect(
      new Set(structuralHashFixture.columnTypeCases.map((entry) => entry.columnType.type)),
    ).toEqual(new Set(portableColumnTypeTags));

    const hashes = structuralHashFixture.columnTypeCases.map(
      ({ name, columnType, nullable, hash }) => {
        const actual = structuralSchemaHash(schemaWithColumnType(columnType, nullable));
        expect(actual, name).toBe(hash);
        return actual;
      },
    );

    expect(new Set(hashes).size).toBe(hashes.length);
  });

  it("keeps the portable type-tag list exhaustive at compile time", () => {
    const _exhaustive: PortableColumnTypeTagsAreExhaustive = true;
    expect(_exhaustive).toBe(true);
  });
});

describe("canonical witness default equality", () => {
  it("distinguishes missing and altered defaults for every supported column value", () => {
    const pairs = [
      [s.string().default("before"), s.string().default("after")],
      [s.boolean().default(false), s.boolean().default(true)],
      [s.int().default(0), s.int().default(1)],
      [s.bigint().default(9007199254740992n), s.bigint().default(9007199254740993n)],
      [s.float().default(-0), s.float().default(0)],
      [s.timestamp().default(1), s.timestamp().default(2)],
      [
        s.uuid().default("11111111-1111-4111-8111-111111111111"),
        s.uuid().default("22222222-2222-4222-8222-222222222222"),
      ],
      [s.bytes().default(new Uint8Array([0, 255])), s.bytes().default(new Uint8Array([0, 254]))],
      [s.json().default({ a: 1 }), s.json().default({ a: 2 })],
      [s.enum("a", "b").default("a"), s.enum("a", "b").default("b")],
      [s.array(s.array(s.bigint())).default([[1n]]), s.array(s.array(s.bigint())).default([[2n]])],
      [s.string().optional().default(null), s.string().optional().default("value")],
      [
        s.enum({ active: { count: s.int() } }).default({ type: "active", count: 1 }),
        s.enum({ active: { count: s.int() } }).default({ type: "active", count: 2 }),
      ],
    ] as const;
    for (const [before, after] of pairs) {
      const left = s.defineApp({ records: s.table({ value: before }, {}) }).wasmSchema;
      const right = s.defineApp({ records: s.table({ value: after }, {}) }).wasmSchema;
      expect(wasmSchemasEqual(left, right)).toBe(false);
      expect(wasmSchemasEqual(left, structuredClone(left))).toBe(true);
      const missing = structuredClone(left);
      delete missing.records.columns[0]!.default;
      expect(wasmSchemasEqual(left, missing)).toBe(false);
    }
  });
});
