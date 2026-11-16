import { readFileSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { admin, jwt } from "better-auth/plugins";
import { getAuthTables, type BetterAuthDBSchema } from "better-auth/db";
import {
  buildBetterAuthAppTables,
  buildBetterAuthSchemaSource,
  buildJazzSchema,
  buildJazzSchemaFromTables,
  createJazzSchemaSourceFile,
  createSchemaNameResolvers,
} from "./schema.js";
import { loadModule } from "../schema-loader.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import { testApp, testPermissions } from "../testing/test-app.js";

/** The Better Auth tables in jazz.app.json's readable form. */
function generatedTables(tables: BetterAuthDBSchema, usePlural?: boolean) {
  const resolvers = createSchemaNameResolvers({ tables, usePlural });
  return JSON.parse(JSON.stringify(buildBetterAuthAppTables({ tables, ...resolvers })));
}

describe("better-auth schema helpers", () => {
  it("keeps Better Auth's own timestamps as plain columns while preserving required and renamed fields", () => {
    const tables = getAuthTables({});
    // Better Auth owns these fields, including its required timestamps: they
    // keep their domain meaning instead of becoming Jazz provenance columns.
    expect(generatedTables(tables).user.columns).toMatchObject({
      email: "string",
      createdAt: "timestamp",
      updatedAt: "timestamp",
    });
    const renamed = buildBetterAuthAppTables({
      tables,
      getModelName: (model) => model,
      getFieldName: ({ field }) => (field === "createdAt" ? "registeredAt" : field),
    });
    expect(renamed.user!.columns.registeredAt).toBe("timestamp");
    expect(renamed.user!.columns).not.toHaveProperty("createdAt");
  });
  it.each(["__proto__", "union", "exists", "_schema", "wasmSchema"])(
    "rejects reserved table name %s in direct Better Auth schema generation",
    (tableName) => {
      const tables = {
        user: {
          modelName: "user",
          fields: {
            value: {
              type: "string",
              required: true,
            },
          },
        },
      } as BetterAuthDBSchema;
      const getModelName = () => tableName;
      const getFieldName = ({ field }: { model: string; field: string }) => field;

      expect(() =>
        buildJazzSchema({
          tables,
          getModelName,
          getFieldName,
        }),
      ).toThrow(/reserved/i);
      expect(() =>
        buildBetterAuthAppTables({
          tables,
          getModelName,
          getFieldName,
        }),
      ).toThrow(/reserved/i);
    },
  );

  it("keeps ordinary, prototype, and hyphenated table names usable in direct generation, and refuses names the app file can't hold", () => {
    const tables = {
      normal: {
        modelName: "normal",
        fields: { value: { type: "string", required: true } },
      },
      prototype: {
        modelName: "prototype",
        fields: { value: { type: "string", required: true } },
      },
      hyphenated: {
        modelName: "hyphenated",
        fields: { value: { type: "string", required: true } },
      },
    } as BetterAuthDBSchema;
    const getModelName = (model: string) => (model === "hyphenated" ? "hyphenated-name" : model);
    const getFieldName = ({ field }: { model: string; field: string }) => field;

    const schema = buildJazzSchema({ tables, getModelName, getFieldName });

    expect(Object.keys(schema).sort()).toEqual(["hyphenated-name", "normal", "prototype"]);
    // jazz.app.json only allows identifier names (SPEC §10.10).
    const { hyphenated: _, ...identifiers } = tables;
    expect(
      Object.keys(buildBetterAuthAppTables({ tables: identifiers, getModelName, getFieldName })),
    ).toEqual(["normal", "prototype"]);
    expect(() => buildBetterAuthAppTables({ tables, getModelName, getFieldName })).toThrow(
      /Model "hyphenated": the table name "hyphenated-name" can't be written to jazz\.app\.json.*modelName/,
    );
    expect(() =>
      buildBetterAuthAppTables({
        tables: identifiers,
        getModelName,
        getFieldName: () => "value-name",
      }),
    ).toThrow(/Field "normal\.value": the column name "value-name".*fieldName/);
  });

  it("retains the installed JWT plugin signing-key metadata", () => {
    const tables = getAuthTables({ plugins: [jwt()] });
    const schema = buildJazzSchemaFromTables({ tables });
    const generated = generatedTables(tables);
    for (const name of ["alg", "crv"]) {
      expect(schema.jwks?.columns).toContainEqual({
        name,
        column_type: { type: "Text" },
        nullable: true,
      });
      expect(generated.jwks.columns[name]).toBe("string?");
    }
  });

  it("builds a Jazz schema from Better Auth tables using transformed names", () => {
    const tables = {
      user: {
        modelName: "accountHolder",
        fields: {
          email: {
            type: "string",
            required: true,
            fieldName: "email_address",
          },
          role: {
            type: ["user", "admin"],
            required: true,
          },
          metadata: {
            type: "json",
            required: false,
          },
        },
      },
      account: {
        modelName: "account",
        fields: {
          userId: {
            type: "string",
            required: true,
            references: {
              model: "user",
              field: "id",
            },
          },
        },
      },
    } as BetterAuthDBSchema;

    const wasmSchema = buildJazzSchemaFromTables({
      tables,
      usePlural: true,
    });

    expect(wasmSchema.accountHolders?.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "email_address", column_type: { type: "Text" } }),
        expect.objectContaining({
          name: "role",
          column_type: { type: "Enum", variants: ["user", "admin"] },
        }),
        expect.objectContaining({
          name: "metadata",
          column_type: { type: "Json" },
          nullable: true,
        }),
      ]),
    );
    expect(wasmSchema.accounts?.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "userId",
          column_type: { type: "Uuid" },
          references: "accountHolders",
        }),
      ]),
    );
  });

  it("throws when a Better Auth field collides with the Jazz row id", () => {
    const tables = {
      user: {
        modelName: "user",
        fields: {
          email: {
            type: "string",
            required: true,
            fieldName: "id",
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(() => buildJazzSchemaFromTables({ tables })).toThrow(
      "conflicts with reserved Jazz row id",
    );
  });

  it("builds jazz.app.json tables from Better Auth tables using transformed names", () => {
    const tables = {
      user: {
        modelName: "accountHolder",
        fields: {
          id: {
            type: "string",
            required: true,
          },
          email: {
            type: "string",
            required: true,
            fieldName: "email_address",
          },
          role: {
            type: ["user", "admin"],
            required: true,
          },
          metadata: {
            type: "json",
            required: false,
          },
          deviceIds: {
            type: "string[]",
            required: false,
            references: {
              model: "device",
              field: "id",
            },
          },
        },
      },
      device: {
        modelName: "device",
        fields: {
          name: {
            type: "string",
            required: true,
          },
          tags: {
            type: "string[]",
            required: true,
          },
          loginCount: {
            type: "number",
            required: true,
          },
        },
      },
      session: {
        modelName: "session",
        fields: {
          createdAt: {
            type: "date",
            required: true,
          },
          retryCounts: {
            type: "number[]",
            required: false,
          },
          userId: {
            type: "string",
            required: false,
            references: {
              model: "user",
              field: "id",
            },
          },
        },
      },
    } as BetterAuthDBSchema;

    const file = JSON.stringify({ format: 1, tables: generatedTables(tables, true) });
    expect({
      $schema: "https://jazz.tools/schemas/jazz.app.schema.json",
      ...JSON.parse(file),
    }).toEqual(
      JSON.parse(
        [
          "{",
          '  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",',
          '  "format": 1,',
          '  "tables": {',
          '    "accountHolders": {',
          '      "columns": {',
          '        "email_address": "string",',
          '        "role": { "enum": ["user", "admin"] },',
          '        "metadata": { "json": true, "optional": true },',
          '        "deviceIds": { "array": "uuid", "optional": true }',
          "      },",
          '      "relations": { "deviceIdsRelation": { "rel": "devices", "column": "deviceIds" } }',
          "    },",
          '    "devices": {',
          '      "columns": { "name": "string", "tags": { "array": "string" }, "loginCount": "int" }',
          "    },",
          '    "sessions": {',
          '      "columns": {',
          '        "createdAt": "timestamp",',
          '        "retryCounts": { "array": "int", "optional": true },',
          '        "userId": "uuid?"',
          "      },",
          '      "relations": { "userIdRelation": { "rel": "accountHolders", "column": "userId" } }',
          "    }",
          "  }",
          "}",
          "",
        ].join("\n"),
      ),
    );
    // What it writes is a jazz.app.json the Rust loader takes.
    const app = testApp(file);
    expect(app.wasmSchema.sessions!.columns).toContainEqual({
      name: "userId",
      column_type: { type: "Uuid" },
      nullable: true,
      references: "accountHolders",
    });
    expect(app.wasmSchema.accountHolders!.relations?.deviceIdsRelation).toEqual({
      kind: "forward",
      table: "devices",
      column: "deviceIds",
    });
  });

  it("denies every client operation on the Better Auth tables, each as `false`", () => {
    const tables = {
      user: { modelName: "user", fields: { id: { type: "string", required: true } } },
      session: { modelName: "session", fields: { id: { type: "string", required: true } } },
    } as BetterAuthDBSchema;
    const tableNamesByModel = { user: "better_auth_user", session: "better_auth_session" } as const;
    const source = buildBetterAuthSchemaSource({
      tables,
      getModelName: (model) => tableNamesByModel[model as keyof typeof tableNamesByModel],
      getFieldName: ({ field }) => field,
    });
    for (const tableName of Object.values(tableNamesByModel)) {
      expect(source).toContain(
        `  ${tableName}: { read: false, insert: false, update: false, delete: false },`,
      );
    }
  });

  it("throws when app-file generation encounters magic column names", () => {
    const tables = {
      user: {
        modelName: "user",
        fields: {
          email: {
            type: "string",
            required: true,
            fieldName: "$canRead",
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(() => generatedTables(tables)).toThrow(/reserved for magic columns/i);
  });

  it("throws when app-file generation encounters fields renamed to id", () => {
    const tables = {
      user: {
        modelName: "user",
        fields: {
          email: {
            type: "string",
            required: true,
            fieldName: "id",
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(() => generatedTables(tables)).toThrow(/conflicts with reserved Jazz row id/i);
  });

  it("throws when app-file generation encounters bigint numbers", () => {
    const tables = {
      user: {
        modelName: "user",
        fields: {
          loginCount: {
            type: "number",
            bigint: true,
            required: true,
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(() => generatedTables(tables)).toThrow(/cannot represent/i);
  });

  it("throws when app-file generation encounters bigint number arrays", () => {
    const tables = {
      user: {
        modelName: "user",
        fields: {
          loginCounts: {
            type: "number[]",
            bigint: true,
            required: true,
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(() => generatedTables(tables)).toThrow(/cannot represent/i);
  });

  it("throws when app-file generation encounters non-id references", () => {
    const tables = {
      user: { modelName: "user", fields: { email: { type: "string", required: true } } },
      session: {
        modelName: "session",
        fields: {
          userId: {
            type: "string",
            required: true,
            references: {
              model: "user",
              field: "email",
            },
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(() => generatedTables(tables)).toThrow(/only support references to "id"/i);
  });

  it("declares a distinct relationship for an unsuffixed scalar UUID", () => {
    const tables = {
      user: { modelName: "user", fields: { email: { type: "string", required: true } } },
      session: {
        modelName: "session",
        fields: {
          owner: {
            type: "string",
            required: true,
            references: {
              model: "user",
              field: "id",
            },
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(generatedTables(tables).session.relations).toEqual({
      ownerRelation: { rel: "user", column: "owner" },
    });
  });

  it("declares a distinct relationship for an unsuffixed UUID array", () => {
    const tables = {
      user: { modelName: "user", fields: { email: { type: "string", required: true } } },
      session: {
        modelName: "session",
        fields: {
          owners: {
            type: "string[]",
            required: true,
            references: {
              model: "user",
              field: "id",
            },
          },
        },
      },
    } as BetterAuthDBSchema;

    expect(generatedTables(tables).session.relations).toEqual({
      ownersRelation: { rel: "user", column: "owners" },
    });
  });
});

describe("writing schema-better-auth/schema.ts", () => {
  const authTables = getAuthTables({ plugins: [jwt(), admin()] });
  const resolvers = {
    getModelName: (model: string) => `better_auth_${authTables[model]?.modelName ?? model}`,
    getFieldName: ({ model, field }: { model: string; field: string }) =>
      authTables[model]?.fields[field]?.fieldName ?? field,
  };

  it("exports tables and permissions an app spreads into its schema.ts, giving Better Auth's tables", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-better-auth-ts-"));
    try {
      await mkdir(join(dir, "schema-better-auth"));
      const created = createJazzSchemaSourceFile({ tables: authTables, ...resolvers });
      expect(created.path).toBe("./schema-better-auth/schema.ts");
      expect(created.overwrite).toBe(true);
      await writeFile(join(dir, "schema-better-auth", "schema.ts"), created.code);
      await writeFile(
        join(dir, "schema.ts"),
        [
          'import { schema as s } from "jazz-tools";',
          'import * as betterAuth from "./schema-better-auth/schema";',
          "export const schema = s.defineSchema({",
          "  ...betterAuth.tables,",
          "  notes: s.table({ body: s.string() }, {}),",
          "});",
          "export const permissions = schema.definePermissions({",
          "  ...betterAuth.permissions,",
          "  notes: { read: true },",
          "});",
          "",
        ].join("\n"),
      );
      const module = await loadModule(join(dir, "schema.ts"));
      const app = readableSchema(module.schema)!;
      const expected = buildBetterAuthAppTables({ tables: authTables, ...resolvers });
      for (const [name, table] of Object.entries(expected)) {
        expect(app.tables[name]).toEqual(JSON.parse(JSON.stringify(table)));
      }
      const permissions = (
        module.permissions as Record<symbol, { tables: Record<string, unknown> }>
      )[Symbol.for("jazz.permissions.readable")]!;
      for (const name of Object.keys(expected)) {
        expect(permissions.tables[name]).toEqual({
          read: "false",
          insert: "false",
          update: "false",
          delete: "false",
        });
      }
      expect(permissions.tables.notes).toEqual({ read: "true" });
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("matches the shipped example's Better Auth tables", () => {
    const exampleFile = readFileSync(
      new URL("../../../../examples/auth-betterauth-chat/jazz.app.json", import.meta.url),
      "utf8",
    );
    const example = JSON.parse(exampleFile).tables;
    const generated = buildBetterAuthAppTables({ tables: authTables, ...resolvers });
    for (const [name, table] of Object.entries(generated)) {
      expect(example[name].columns).toEqual(table.columns);
    }
  });
});
