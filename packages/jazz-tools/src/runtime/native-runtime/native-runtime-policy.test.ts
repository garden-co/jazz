import { describe, expect, it } from "vitest";
import type { PolicyExpr, WasmSchema } from "../../drivers/types.js";
import { writeAppArtifact } from "jazz-napi";
import { runtimeSchemaJsonReplacer } from "../../drivers/schema-wire.js";
import { boundApp } from "../../typed-app.js";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "../../schema-permissions.js";
import { encodeSchema } from "./schema-codec.js";
import { testApp, testPermissions } from "../../testing/test-app.js";

type EncodedSchemaSource = {
  tables: WasmSchema;
};

function decode(bytes: Uint8Array): EncodedSchemaSource {
  return JSON.parse(new TextDecoder().decode(bytes)) as EncodedSchemaSource;
}

/**
 * `permissions` as Jazz's Rust writer renders them in jazz.permissions.json.
 * `schema` is a bound app's (testApp's), which carries its hash.
 */
async function permissionsFileOf(
  schema: WasmSchema,
  permissions: CompiledPermissionsMap,
): Promise<string> {
  const tables = mergePermissionsIntoWasmSchema(schema, {});
  const input = {
    current: boundApp(schema)!.schema.current,
    schemas: [{ tables }],
    permissions,
  };
  return writeAppArtifact(JSON.stringify(input, runtimeSchemaJsonReplacer)).permissions;
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
    expect(source.tables.documents?.policies?.select?.using).toEqual(policy);
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

    expect(Object.keys(source.tables)).toEqual(["alpha", "middle", "zebra"]);
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

    expect(source.tables.values?.columns.map((column) => column.default)).toEqual([
      { type: "BigInt", value: "9007199254740993" },
      { type: "Bytea", value: [1, 2, 255] },
    ]);
  });

  it("encodes authored inherited permissions identically to their source form", async () => {
    const app = testApp({
      tables: {
        resources: {
          columns: { label: "string" },
          relations: {
            entriesViaResource: { reverse: "entries", relation: "resourceRelation" },
          },
        },
        entries: {
          columns: { resource: "uuid" },
          relations: { resourceRelation: { rel: "resources", column: "resource" } },
        },
      },
    });
    const baseSchema: WasmSchema = app.wasmSchema;
    const permissions = testPermissions(app, {
      tables: {
        resources: { read: "true" },
        entries: { read: "allowedTo.read(resourceRelation)" },
      },
    });
    // The engine's form, as Jazz's Rust loader writes it: an absent side is
    // `null`, and keys are in serde (alphabetical) order.
    const absent = { using: null, with_check: null };
    const authored: WasmSchema = {
      ...baseSchema,
      resources: {
        ...baseSchema.resources,
        policies: {
          delete: absent,
          insert: absent,
          select: { using: { type: "True" }, with_check: null },
          update: absent,
        } as never,
      },
      entries: {
        ...baseSchema.entries,
        policies: {
          delete: absent,
          insert: absent,
          select: {
            using: {
              max_depth: null,
              operation: "Select",
              type: "Inherits",
              via_column: "resource",
            },
            with_check: null,
          },
          update: absent,
        } as never,
      },
    };

    expect(encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions))).toEqual(
      encodeSchema(authored),
    );

    // `null` and an absent field are the same policy to the engine: the
    // Rust writer renders both as the same jazz.permissions.json.
    const omitted: CompiledPermissionsMap = {
      resources: { select: { using: { type: "True" } }, insert: {}, update: {}, delete: {} },
      entries: {
        select: { using: { type: "Inherits", operation: "Select", via_column: "resource" } },
        insert: {},
        update: {},
        delete: {},
      },
    };
    expect(await permissionsFileOf(baseSchema, omitted)).toBe(
      await permissionsFileOf(baseSchema, permissions),
    );
  });
});
