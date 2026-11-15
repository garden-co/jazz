import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import type { ColumnDescriptor, ColumnType, WasmSchema } from "../drivers/types.js";

import { compileTestApp, testApp } from "../testing/index.js";
import { wasmSchemasEqual } from "./schema-utils.js";

function schemaWithColumnType(columnType: ColumnType, nullable = false): WasmSchema {
  return {
    values: {
      columns: [{ name: "value", column_type: columnType, nullable }],
    },
  };
}

type StructuralHashFixture = {
  schemaLayoutVersion: number;
  defaultCases: Array<{
    name: string;
    columnType: ColumnType;
    nullable: boolean;
    default?: ColumnDescriptor["default"];
    branchBy?: string[];
    mergeStrategy?: ColumnDescriptor["merge_strategy"];
    hash: string;
  }>;
  columnTypeCases: Array<{
    name: string;
    columnType: ColumnType;
    nullable: boolean;
    hash: string;
  }>;
  compositeIndexCases: Array<{
    name: string;
    columns: string[];
    compositeIndexes: string[][];
    hash: string;
  }>;
  historyCases: Array<{
    name: string;
    history: "latest" | null;
    hash: string;
  }>;
};

describe("wasmSchemasEqual", () => {
  const structuralHashFixture = JSON.parse(
    readFileSync(
      new URL("../testing/fixtures/structural-schema-hashes.json", import.meta.url),
      "utf8",
    ),
  ) as StructuralHashFixture;

  it("tells schemas apart by their specialised indexes, in any order", () => {
    const withIndexes = (...names: string[]): WasmSchema => {
      const schema = schemaWithColumnType({ type: "Text" });
      schema.values!.specialised_indexes = names.map((name) => ({
        name,
        kind: { text: { fields: { value: 1 }, analyzer: "jazz-text/1:en" } },
      }));
      return schema;
    };
    expect(wasmSchemasEqual(withIndexes("a", "b"), withIndexes("b", "a"))).toBe(true);
    expect(wasmSchemasEqual(withIndexes("a"), withIndexes("a", "b"))).toBe(false);
    expect(wasmSchemasEqual(withIndexes("a"), schemaWithColumnType({ type: "Text" }))).toBe(false);
  });

  it("tells latest-only history apart from full history", () => {
    const latest = schemaWithColumnType({ type: "Text" });
    latest.values!.history = "latest";
    expect(wasmSchemasEqual(latest, schemaWithColumnType({ type: "Text" }))).toBe(false);
  });

  it("carries a table's latest-only retention into the wasm schema through other modifiers", () => {
    const app = testApp({
      tables: {
        presence: {
          columns: { session: "string", cursor: { json: true } },
          indexes: ["session"],
          history: "latest",
        },
        notes: { columns: { body: "string" } },
      },
    });
    expect(app.wasmSchema.presence!.history).toBe("latest");
    expect(app.wasmSchema.notes!.history).toBeUndefined();
    expect(() =>
      compileTestApp({ tables: { notes: { columns: { body: "string" }, history: "30d" } } }),
    ).toThrow(/the only history option is "latest"/);
  });

  it("compares Rust-produced defaults, merge strategies, JSON metadata, and branch bindings", () => {
    expect(structuralHashFixture.defaultCases.length).toBeGreaterThanOrEqual(22);
    for (const entry of structuralHashFixture.defaultCases) {
      // This corpus intentionally consumes Rust's human-JSON carriers, including
      // decimal-string bigint values and byte arrays, without changing their wire format.
      const schema: WasmSchema = {
        values: {
          columns: [
            {
              name: "value",
              column_type: entry.columnType,
              nullable: entry.nullable,
              ...(entry.default === undefined ? {} : { default: entry.default }),
              ...(entry.mergeStrategy === undefined ? {} : { merge_strategy: entry.mergeStrategy }),
            },
          ],
          ...(entry.branchBy === undefined ? {} : { branchBy: entry.branchBy }),
        },
      };
      expect(wasmSchemasEqual(schema, structuredClone(schema)), entry.name).toBe(true);
      if (entry.default !== undefined) {
        const missing = structuredClone(schema);
        delete missing.values.columns[0]!.default;
        expect(wasmSchemasEqual(schema, missing), entry.name).toBe(false);
      }
      if (entry.default?.type === "Row") {
        const altered = structuredClone(schema);
        const value = altered.values.columns[0]!.default!;
        if (value.type !== "Row") throw new Error("Expected row corpus default");
        value.value.values.reverse();
        expect(wasmSchemasEqual(schema, altered), "row default value order").toBe(false);
      }
    }
  });
});

/** The wasm schema of an app with one table `records` holding one column `value` (JSON text). */
function recordsSchema(column: string): WasmSchema {
  return testApp(`{ "format": 1, "tables": { "records": { "columns": { "value": ${column} } } } }`)
    .wasmSchema;
}

describe("canonical witness default equality", () => {
  it("normalizes exact human-JSON bigint defaults and rejects invalid carriers", () => {
    const schema = recordsSchema('{ "type": "bigint", "default": "9223372036854775807" }');
    const serialized = JSON.parse(
      JSON.stringify(schema, (_, value) => (typeof value === "bigint" ? value.toString() : value)),
    );
    expect(wasmSchemasEqual(schema, serialized)).toBe(true);
    for (const value of [
      9007199254740992,
      "9223372036854775808",
      "-9223372036854775809",
      "42n",
      "1; throw new Error()",
      null,
    ]) {
      serialized.records.columns[0].default.value = value;
      expect(() => wasmSchemasEqual(schema, serialized)).toThrow(
        "Invalid structural BigInt default",
      );
    }
  });

  it("distinguishes missing and altered defaults for every supported column value", () => {
    // Columns as jazz.app.json text: `-0` must reach the loader as written.
    const pairs = [
      ['{ "type": "string", "default": "before" }', '{ "type": "string", "default": "after" }'],
      ['{ "type": "boolean", "default": false }', '{ "type": "boolean", "default": true }'],
      ['{ "type": "int", "default": 0 }', '{ "type": "int", "default": 1 }'],
      [
        '{ "type": "bigint", "default": "9007199254740992" }',
        '{ "type": "bigint", "default": "9007199254740993" }',
      ],
      ['{ "type": "float", "default": -0 }', '{ "type": "float", "default": 0 }'],
      ['{ "type": "timestamp", "default": 1 }', '{ "type": "timestamp", "default": 2 }'],
      [
        '{ "type": "uuid", "default": "11111111-1111-4111-8111-111111111111" }',
        '{ "type": "uuid", "default": "22222222-2222-4222-8222-222222222222" }',
      ],
      // base64 of [0, 255] and [0, 254]
      ['{ "type": "bytes", "default": "AP8=" }', '{ "type": "bytes", "default": "AP4=" }'],
      ['{ "json": true, "default": { "a": 1 } }', '{ "json": true, "default": { "a": 2 } }'],
      ['{ "enum": ["a", "b"], "default": "a" }', '{ "enum": ["a", "b"], "default": "b" }'],
      [
        '{ "array": { "array": "bigint" }, "default": [["1"]] }',
        '{ "array": { "array": "bigint" }, "default": [["2"]] }',
      ],
      [
        '{ "type": "string", "optional": true, "default": null }',
        '{ "type": "string", "optional": true, "default": "value" }',
      ],
      [
        '{ "enum": { "active": { "count": "int" } }, "default": { "active": { "count": 1 } } }',
        '{ "enum": { "active": { "count": "int" } }, "default": { "active": { "count": 2 } } }',
      ],
    ] as const;
    for (const [before, after] of pairs) {
      const left = recordsSchema(before);
      const right = recordsSchema(after);
      expect(wasmSchemasEqual(left, right), before).toBe(false);
      expect(wasmSchemasEqual(left, structuredClone(left)), before).toBe(true);
      const missing = structuredClone(left);
      delete missing.records!.columns[0]!.default;
      expect(wasmSchemasEqual(left, missing), before).toBe(false);
    }
  });
});
