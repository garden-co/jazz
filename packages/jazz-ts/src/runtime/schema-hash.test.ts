import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { computeSchemaHash } from "./schema-hash.js";

describe("computeSchemaHash", () => {
  it("is deterministic", () => {
    const schema: WasmSchema = {
      tables: {
        users: {
          columns: [
            { name: "id", column_type: { type: "Uuid" }, nullable: false },
            { name: "name", column_type: { type: "Text" }, nullable: false },
          ],
        },
      },
    };

    expect(computeSchemaHash(schema)).toBe(computeSchemaHash(schema));
  });

  it("is column-order independent", () => {
    const schemaA: WasmSchema = {
      tables: {
        users: {
          columns: [
            { name: "id", column_type: { type: "Uuid" }, nullable: false },
            { name: "name", column_type: { type: "Text" }, nullable: false },
            { name: "age", column_type: { type: "Integer" }, nullable: true },
          ],
        },
      },
    };

    const schemaB: WasmSchema = {
      tables: {
        users: {
          columns: [
            { name: "age", column_type: { type: "Integer" }, nullable: true },
            { name: "name", column_type: { type: "Text" }, nullable: false },
            { name: "id", column_type: { type: "Uuid" }, nullable: false },
          ],
        },
      },
    };

    expect(computeSchemaHash(schemaA)).toBe(computeSchemaHash(schemaB));
  });

  it("is table-order independent", () => {
    const schemaA: WasmSchema = {
      tables: {
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
        posts: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
    };

    const schemaB: WasmSchema = {
      tables: {
        posts: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
    };

    expect(computeSchemaHash(schemaA)).toBe(computeSchemaHash(schemaB));
  });

  it("changes for different schemas", () => {
    const schemaA: WasmSchema = {
      tables: {
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
    };

    const schemaB: WasmSchema = {
      tables: {
        users: {
          columns: [
            { name: "id", column_type: { type: "Uuid" }, nullable: false },
            { name: "email", column_type: { type: "Text" }, nullable: false },
          ],
        },
      },
    };

    expect(computeSchemaHash(schemaA)).not.toBe(computeSchemaHash(schemaB));
  });

  it("returns a 64-char lowercase hex hash", () => {
    const schema: WasmSchema = {
      tables: {
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
    };

    const hash = computeSchemaHash(schema);
    expect(hash).toMatch(/^[0-9a-f]{64}$/);
  });

  it("includes nested row and array structure", () => {
    const schemaA: WasmSchema = {
      tables: {
        shapes: {
          columns: [
            {
              name: "points",
              column_type: {
                type: "Array",
                element: {
                  type: "Row",
                  columns: [
                    { name: "x", column_type: { type: "Integer" }, nullable: false },
                    { name: "y", column_type: { type: "Integer" }, nullable: false },
                  ],
                },
              },
              nullable: false,
            },
          ],
        },
      },
    };

    const schemaB: WasmSchema = {
      tables: {
        shapes: {
          columns: [
            {
              name: "points",
              column_type: {
                type: "Array",
                element: {
                  type: "Row",
                  columns: [
                    { name: "y", column_type: { type: "Integer" }, nullable: false },
                    { name: "x", column_type: { type: "Integer" }, nullable: false },
                  ],
                },
              },
              nullable: false,
            },
          ],
        },
      },
    };

    expect(computeSchemaHash(schemaA)).toBe(computeSchemaHash(schemaB));
  });
});
