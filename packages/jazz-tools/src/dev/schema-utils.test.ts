import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import type { ColumnType, WasmSchema } from "../drivers/types.js";

import { structuralSchemaHash } from "./schema-utils.js";

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
