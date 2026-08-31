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

describe("structuralSchemaHash", () => {
  const orderedEnumFixture = JSON.parse(
    readFileSync(
      new URL("../testing/fixtures/ordered-enum-schema-hashes.json", import.meta.url),
      "utf8",
    ),
  ) as { schemaLayoutVersion: number; cases: Array<{ variants: string[]; hash: string }> };

  it("treats enum declaration order as durable tag meaning", () => {
    expect(structuralSchemaHash(enumSchema(["draft", "active"]))).not.toBe(
      structuralSchemaHash(enumSchema(["active", "draft"])),
    );
    expect(structuralSchemaHash(enumSchema(["draft", "active"]))).not.toBe(
      structuralSchemaHash(enumSchema(["draft"])),
    );
  });

  it("matches the Rust ordered-enum structural hash fixture", () => {
    expect(orderedEnumFixture.schemaLayoutVersion).toBe(9);
    const hashes = orderedEnumFixture.cases.map(({ variants, hash }) => {
      const actual = structuralSchemaHash(enumSchema(variants));
      expect(actual).toBe(hash);
      return actual;
    });

    expect(hashes[0]).not.toBe(hashes[1]);
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
