import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import type { ColumnType, WasmSchema } from "../drivers/types.js";

import { structuralSchemaHash } from "./schema-utils.js";

function enumSchema(variants: string[]) {
  return {
    todos: {
      columns: [
        {
          name: "status",
          column_type: { type: "Enum" as const, variants },
          nullable: false,
        },
      ],
    },
  };
}

function schemaWithColumnType(columnType: ColumnType): WasmSchema {
  return {
    values: {
      columns: [{ name: "value", column_type: columnType, nullable: false }],
    },
  };
}

type StructuralHashFixture = {
  schemaLayoutVersion: number;
  cases: Array<{ variants: string[]; hash: string }>;
  payloadCases: Array<{
    cases: Extract<ColumnType, { type: "EnumPayload" }>["cases"];
    hash: string;
  }>;
};

describe("structuralSchemaHash", () => {
  const orderedEnumFixture = JSON.parse(
    readFileSync(
      new URL("../testing/fixtures/ordered-enum-schema-hashes.json", import.meta.url),
      "utf8",
    ),
  ) as StructuralHashFixture;

  it("treats enum declaration order as durable tag meaning", () => {
    expect(structuralSchemaHash(enumSchema(["draft", "active"]))).not.toBe(
      structuralSchemaHash(enumSchema(["active", "draft"])),
    );
    expect(structuralSchemaHash(enumSchema(["draft", "active"]))).not.toBe(
      structuralSchemaHash(enumSchema(["draft"])),
    );
  });

  it("matches the Rust ordered-enum structural hash fixture", () => {
    expect(orderedEnumFixture.schemaLayoutVersion).toBe(10);
    const hashes = orderedEnumFixture.cases.map(({ variants, hash }) => {
      const actual = structuralSchemaHash(enumSchema(variants));
      expect(actual).toBe(hash);
      return actual;
    });

    expect(hashes[0]).not.toBe(hashes[1]);
  });

  it("matches Rust for payload enum case and field structure", () => {
    const hashes = orderedEnumFixture.payloadCases.map(({ cases, hash }) => {
      const actual = structuralSchemaHash(schemaWithColumnType({ type: "EnumPayload", cases }));
      expect(actual).toBe(hash);
      return actual;
    });

    expect(hashes[0]).not.toBe(hashes[1]);
    expect(hashes[0]).not.toBe(hashes[2]);
    expect(hashes[2]).not.toBe(hashes[3]);
  });

  it("distinguishes payload schemas through nested array and row boundaries", () => {
    const base = {
      type: "EnumPayload" as const,
      cases: [
        {
          name: "updated",
          fields: [
            {
              name: "changes",
              column_type: {
                type: "Array" as const,
                element: {
                  type: "Row" as const,
                  columns: [
                    { name: "title", column_type: { type: "Text" as const }, nullable: false },
                  ],
                },
              },
              nullable: false,
            },
          ],
        },
      ],
    };
    const changed = {
      ...base,
      cases: [
        {
          ...base.cases[0]!,
          fields: [
            {
              ...base.cases[0]!.fields[0]!,
              column_type: {
                type: "Array" as const,
                element: {
                  type: "Row" as const,
                  columns: [
                    { name: "title", column_type: { type: "Bytea" as const }, nullable: false },
                  ],
                },
              },
            },
          ],
        },
      ],
    };

    expect(structuralSchemaHash(schemaWithColumnType(base))).not.toBe(
      structuralSchemaHash(schemaWithColumnType(changed)),
    );
  });

  it("distinguishes Double and Bytea at recursive column-type boundaries", () => {
    expect(structuralSchemaHash(schemaWithColumnType({ type: "Double" }))).not.toBe(
      structuralSchemaHash(schemaWithColumnType({ type: "Bytea" })),
    );
    expect(
      structuralSchemaHash(schemaWithColumnType({ type: "Array", element: { type: "Double" } })),
    ).not.toBe(
      structuralSchemaHash(schemaWithColumnType({ type: "Array", element: { type: "Bytea" } })),
    );
  });
});
