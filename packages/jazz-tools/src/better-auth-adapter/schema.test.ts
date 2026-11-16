import { readFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { admin, jwt } from "better-auth/plugins";
import { getAuthTables, type BetterAuthDBSchema } from "better-auth/db";
import {
  betterAuthAppFileNotes,
  buildBetterAuthAppTables,
  buildJazzSchema,
  buildJazzSchemaFromTables,
  createJazzAppFile,
  mergeBetterAuthTables,
  mergeBetterAuthTablesFromTables,
} from "./schema.js";
import { testApp, testPermissions } from "../testing/test-app.js";

/** The Better Auth tables as `generate` writes them into a new jazz.app.json. */
function generatedAppFile(tables: BetterAuthDBSchema, usePlural?: boolean): string {
  return mergeBetterAuthTablesFromTables({ appFile: null, tables, usePlural });
}

function generatedTables(tables: BetterAuthDBSchema, usePlural?: boolean) {
  return JSON.parse(generatedAppFile(tables, usePlural)).tables;
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

    const file = generatedAppFile(tables, true);
    expect(file).toBe(
      [
        "{",
        '  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",',
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

  it("leaves every Better Auth table without policies, and prints the deny-all entries to paste", () => {
    const tables = {
      user: {
        modelName: "user",
        fields: {
          id: {
            type: "string",
            required: true,
          },
        },
      },
      session: {
        modelName: "session",
        fields: {
          id: {
            type: "string",
            required: true,
          },
        },
      },
    } as BetterAuthDBSchema;
    const tableNamesByModel = {
      user: "better_auth_user",
      session: "better_auth_session",
    } as const;
    const generated = buildBetterAuthAppTables({
      tables,
      getModelName: (model) => tableNamesByModel[model as keyof typeof tableNamesByModel],
      getFieldName: ({ field }) => field,
    });
    // A table without policies denies every client read and write.
    const app = testApp({ tables: generated });
    expect(testPermissions(app, { tables: {} })).toEqual({});

    const notes = betterAuthAppFileNotes(Object.keys(generated));
    for (const tableName of Object.values(tableNamesByModel)) {
      expect(notes).toContain(
        `"${tableName}": { "read": "false", "insert": "false", "update": "false", "delete": "false" }`,
      );
    }
    // The pasted entries are a valid `tables` object that denies every operation.
    const pasted = JSON.parse(`{${notes.split("jazz.permissions.json:\n")[1]}}`);
    const permissions = testPermissions(app, { tables: pasted });
    for (const tableName of Object.values(tableNamesByModel)) {
      expect(permissions[tableName]?.select?.using).toEqual({ type: "False" });
      expect(permissions[tableName]?.insert?.with_check).toEqual({ type: "False" });
      expect(permissions[tableName]?.update?.using).toEqual({ type: "False" });
      expect(permissions[tableName]?.update?.with_check).toEqual({ type: "False" });
      expect(permissions[tableName]?.delete?.using).toEqual({ type: "False" });
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

    expect(() => generatedAppFile(tables)).toThrow(/reserved for magic columns/i);
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

    expect(() => generatedAppFile(tables)).toThrow(/conflicts with reserved Jazz row id/i);
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

    expect(() => generatedAppFile(tables)).toThrow(/cannot represent/i);
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

    expect(() => generatedAppFile(tables)).toThrow(/cannot represent/i);
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

    expect(() => generatedAppFile(tables)).toThrow(/only support references to "id"/i);
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

describe("writing the Better Auth tables into jazz.app.json", () => {
  const authTables = getAuthTables({ plugins: [jwt(), admin()] });
  const resolvers = {
    getModelName: (model: string) => `better_auth_${authTables[model]?.modelName ?? model}`,
    getFieldName: ({ model, field }: { model: string; field: string }) =>
      authTables[model]?.fields[field]?.fieldName ?? field,
  };
  const exampleFile = readFileSync(
    new URL("../../../../examples/auth-betterauth-chat/jazz.app.json", import.meta.url),
    "utf8",
  );

  it("gives back the shipped example's file unchanged when Better Auth's tables match it", () => {
    expect(mergeBetterAuthTables({ appFile: exampleFile, tables: authTables, ...resolvers })).toBe(
      exampleFile,
    );
  });

  it("replaces the auth tables' columns and keeps every other table, history and migration", () => {
    const before = JSON.parse(exampleFile);
    // An app on Better Auth 1.7.1, whose account still carries `issuer`, with a
    // hand-written relation name and an index on the dropped column.
    before.tables.better_auth_account.columns = {
      issuer: "string",
      ...before.tables.better_auth_account.columns,
    };
    before.tables.better_auth_account.indexes = ["issuer", "providerId"];
    before.tables.messages = { columns: { text: "string" } };
    const merged = JSON.parse(
      mergeBetterAuthTables({
        appFile: JSON.stringify(before, null, 2),
        tables: authTables,
        ...resolvers,
      }),
    );
    expect(merged.tables.better_auth_account.columns).not.toHaveProperty("issuer");
    expect(merged.tables.better_auth_account.indexes).toEqual(["providerId"]);
    // The app's own relation names stay: no `userIdRelation` beside `user`.
    expect(merged.tables.better_auth_account.relations).toEqual({
      user: { rel: "better_auth_user", column: "userId" },
    });
    expect(merged.tables.better_auth_user.relations).toEqual(
      before.tables.better_auth_user.relations,
    );
    expect(merged.tables.messages).toEqual({ columns: { text: "string" } });
    expect(merged.history).toEqual(before.history);
    expect(merged.migrations).toEqual(before.migrations);
    expect(Object.keys(merged.tables)).toEqual(Object.keys(merged.tables).sort());
  });

  it("adds the auth tables to an app that has none, sorted among its tables, and keeps a table Better Auth no longer generates", () => {
    const appFile = JSON.stringify(
      {
        format: 1,
        tables: {
          zebras: { columns: { name: "string" } },
          apples: { columns: { name: "string" } },
          better_auth_passkey: { columns: { name: "string" } },
        },
      },
      null,
      2,
    );
    const merged = mergeBetterAuthTables({ appFile, tables: authTables, ...resolvers });
    expect(Object.keys(JSON.parse(merged).tables)).toEqual([
      "apples",
      "better_auth_account",
      "better_auth_jwks",
      "better_auth_passkey",
      "better_auth_session",
      "better_auth_user",
      "better_auth_verification",
      "zebras",
    ]);
    expect(merged.endsWith("}\n")).toBe(true);
    expect(Object.keys(testApp(merged).wasmSchema)).toHaveLength(8);
  });

  it("keeps the rest of the file as written, in its own layout", () => {
    const appFile = [
      "{",
      '  "format": 1,',
      '  "tables": { "notes": { "columns": { "body": "string" } } },',
      '  "history": {',
      '    "a00000000000": {"tables": {"notes": {"columns": {"body": "string?"}}}},',
      '    "b00000000000": {',
      '        "tables": { "notes": { "columns": { "body": "string" } } } }',
      "  }",
      "}",
      "",
    ].join("\n");
    const merged = mergeBetterAuthTables({ appFile, tables: authTables, ...resolvers });
    expect(merged.slice(merged.indexOf('  "history"'))).toBe(
      appFile.slice(appFile.indexOf('  "history"')),
    );
  });

  it("refuses a file that isn't an app file", () => {
    expect(() =>
      mergeBetterAuthTables({ appFile: "{ nope", tables: authTables, ...resolvers }),
    ).toThrow(/jazz\.app\.json is not valid JSON/);
    expect(() =>
      mergeBetterAuthTables({ appFile: '{ "format": 1 }', tables: authTables, ...resolvers }),
    ).toThrow(/no "tables" object/);
  });

  it("reads and returns the app file through createSchema", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-better-auth-generate-"));
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    try {
      const path = join(dir, "jazz.app.json");
      await writeFile(path, exampleFile);
      const created = await createJazzAppFile({ file: path, tables: authTables, ...resolvers });
      expect(created).toEqual({ path, overwrite: true, code: exampleFile });
      const fresh = await createJazzAppFile({
        file: join(dir, "missing.json"),
        tables: authTables,
        ...resolvers,
      });
      const freshFile = JSON.parse(fresh.code);
      expect(freshFile.$schema).toBe("https://jazz.tools/schemas/jazz.app.v1.json");
      expect(freshFile).not.toHaveProperty("format");
      expect(log.mock.calls.at(-1)?.[0]).toContain("npx jazz-tools generate");
    } finally {
      log.mockRestore();
      await rm(dir, { recursive: true, force: true });
    }
  });
});
