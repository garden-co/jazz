import { describe, expect, it } from "vitest";
import type { StoredMigrationEdge, WasmSchema } from "jazz-tools";
import { compareSchemas, findShortestGhostEdges } from "./schema-analysis";

function schema(tables: WasmSchema): WasmSchema {
  return tables;
}

describe("schema-analysis", () => {
  it("prefers ghost edges between the most structurally similar disconnected schemas", () => {
    const schemas = {
      hashA: schema({
        users: {
          columns: [
            { name: "id", column_type: { type: "Uuid" }, nullable: false },
            { name: "name", column_type: { type: "Text" }, nullable: false },
          ],
        },
      }),
      hashB: schema({
        users: {
          columns: [
            { name: "id", column_type: { type: "Uuid" }, nullable: false },
            { name: "name", column_type: { type: "Text" }, nullable: false },
            { name: "email", column_type: { type: "Text" }, nullable: true },
          ],
        },
      }),
      hashC: schema({
        accounts: {
          columns: [
            { name: "id", column_type: { type: "Uuid" }, nullable: false },
            { name: "display_name", column_type: { type: "Text" }, nullable: false },
            { name: "email", column_type: { type: "Text" }, nullable: true },
          ],
        },
      }),
    } satisfies Record<string, WasmSchema>;

    const migrations: StoredMigrationEdge[] = [
      {
        fromHash: "hashA",
        toHash: "hashB",
        forward: [],
      },
    ];

    const ghostEdges = findShortestGhostEdges({ schemas, migrations });

    expect(ghostEdges).toEqual([
      expect.objectContaining({
        fromHash: "hashB",
        toHash: "hashC",
      }),
    ]);
  });

  it("marks same-type unmatched columns as unknown mappings when no compatibility path exists", () => {
    const left = schema({
      users: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          { name: "email", column_type: { type: "Text" }, nullable: false },
        ],
      },
    });
    const right = schema({
      users: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          { name: "email_address", column_type: { type: "Text" }, nullable: false },
        ],
      },
    });

    const comparison = compareSchemas(left, right, { hasCompatibilityPath: false });

    expect(comparison.tables).toEqual([
      expect.objectContaining({
        tableName: "users",
        unknownColumnMappings: [
          {
            fromColumn: "email",
            toColumn: "email_address",
          },
        ],
      }),
    ]);
  });
});
