import { describe, expect, it } from "vitest";
import type { PolicyExpr, WasmSchema } from "../../drivers/types.js";
import { definePermissions } from "../../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { encodeSchema } from "./schema-codec.js";

type EncodedSchemaSource = {
  format_version: number;
  tables: Array<{ name: string; schema: WasmSchema[string] }>;
};

function decode(bytes: Uint8Array): EncodedSchemaSource {
  return JSON.parse(new TextDecoder().decode(bytes)) as EncodedSchemaSource;
}

describe("NativeRuntimeAdapter policy source encoding", () => {
  it("preserves PolicyExpr instead of lowering it to Query", () => {
    const policy: PolicyExpr = {
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "visibility",
          op: "Eq",
          value: { type: "Literal", value: { type: "Text", value: "public" } },
        },
        {
          type: "Exists",
          table: "document_members",
          condition: {
            type: "Cmp",
            column: "document_id",
            op: "Eq",
            value: { type: "SessionRef", path: ["__jazz_outer_row", "id"] },
          },
        },
      ],
    };
    const encoded = encodeSchema({
      documents: {
        columns: [{ name: "visibility", column_type: { type: "Text" }, nullable: false }],
        policies: { select: { using: policy } },
      },
      document_members: {
        columns: [{ name: "document_id", column_type: { type: "Uuid" }, nullable: false }],
      },
    });

    const source = decode(encoded);
    expect(source.format_version).toBe(1);
    expect(
      source.tables.find((table) => table.name === "documents")?.schema.policies?.select?.using,
    ).toEqual(policy);
    expect(new TextDecoder().decode(encoded)).not.toContain('"policy_branches"');
  });

  it("orders tables canonically", () => {
    const source = decode(
      encodeSchema({
        zebra: { columns: [] },
        alpha: { columns: [] },
        middle: { columns: [] },
      }),
    );

    expect(source.tables.map((table) => table.name)).toEqual(["alpha", "middle", "zebra"]);
  });

  it("normalizes host-only default values without changing their AST tags", () => {
    const source = decode(
      encodeSchema({
        values: {
          columns: [
            {
              name: "large",
              column_type: { type: "BigInt" },
              nullable: false,
              default: { type: "BigInt", value: 9_007_199_254_740_993n },
            },
            {
              name: "bytes",
              column_type: { type: "Bytea" },
              nullable: false,
              default: { type: "Bytea", value: Uint8Array.from([1, 2, 255]) },
            },
          ],
        },
      }),
    );

    expect(source.tables[0]?.schema.columns.map((column) => column.default)).toEqual([
      { type: "BigInt", value: "9007199254740993" },
      { type: "Bytea", value: [1, 2, 255] },
    ]);
  });

  it("encodes authored inherited permissions identically to their source form", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
        policies: { select: { using: { type: "True" } } },
      },
      entries: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      entries: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, allowedTo }) => {
      policy.entries.allowRead.where(allowedTo.read("resource"));
    });
    const authored: WasmSchema = {
      ...baseSchema,
      entries: {
        ...baseSchema.entries,
        policies: {
          select: {
            using: { type: "Inherits", operation: "Select", via_column: "resource" },
          },
          insert: {},
          update: {},
          delete: {},
        },
      },
    };

    expect(encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions))).toEqual(
      encodeSchema(authored),
    );
  });
});
