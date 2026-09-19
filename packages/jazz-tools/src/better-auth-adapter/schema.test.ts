import { describe, expect, it } from "vitest";
import { jwt } from "better-auth/plugins";
import { getAuthTables, type BetterAuthDBSchema } from "better-auth/db";
import {
  buildJazzSchema,
  buildJazzSchemaFromTables,
  buildJazzSchemaSourceText,
  buildJazzSchemaSourceTextFromTables,
} from "./schema.js";

describe("better-auth schema helpers", () => {
  it("acknowledges adapter timestamps while preserving required and renamed fields", () => {
    const tables = getAuthTables({});
    const source = buildJazzSchemaSourceTextFromTables({ tables });
    expect(source).toContain("createdAt: s.allowExternalProvenanceName(s.timestamp()),");
    expect(source).toContain("updatedAt: s.allowExternalProvenanceName(s.timestamp()),");
    expect(source).not.toContain("email: s.allowExternalProvenanceName");
    const renamed = buildJazzSchemaSourceText({
      tables,
      getModelName: (model) => model,
      getFieldName: ({ field }) => (field === "createdAt" ? "registeredAt" : field),
    });
    expect(renamed).toContain("registeredAt: s.timestamp(),");
    expect(renamed).not.toContain("registeredAt: s.allowExternalProvenanceName");
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
        buildJazzSchemaSourceText({
          tables,
          getModelName,
          getFieldName,
        }),
      ).toThrow(/reserved/i);
    },
  );

  it("keeps ordinary, prototype, and hyphenated table names usable in direct generation", () => {
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
    const source = buildJazzSchemaSourceText({ tables, getModelName, getFieldName });

    expect(Object.keys(schema).sort()).toEqual(["hyphenated-name", "normal", "prototype"]);
    expect(source).toContain("normal: s.table({");
    expect(source).toContain("prototype: s.table({");
    expect(source).toContain('"hyphenated-name": s.table({');
  });

  it("retains the installed JWT plugin signing-key metadata", () => {
    const tables = getAuthTables({ plugins: [jwt()] });
    const schema = buildJazzSchemaFromTables({ tables });
    const source = buildJazzSchemaSourceTextFromTables({ tables });
    for (const name of ["alg", "crv"]) {
      expect(schema.jwks?.columns).toContainEqual({
        name,
        column_type: { type: "Text" },
        nullable: true,
      });
      expect(source).toContain(`${name}: s.string().optional()`);
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

  it("builds schema.ts text from Better Auth tables using transformed names", () => {
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
            fieldName: "email-address",
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

    expect(buildJazzSchemaSourceTextFromTables({ tables, usePlural: true })).toBe(
      [
        'import { schema as s } from "jazz-tools";',
        "",
        "export const schema = {",
        "  accountHolders: s.table({",
        '    "email-address": s.string(),',
        '    role: s.enum("user", "admin"),',
        "    metadata: s.json().optional(),",
        "    deviceIds: s.array(s.uuid()).optional(),",
        "  }, {",
        '    deviceIdsRelation: s.rel("devices", "deviceIds"),',
        "  }),",
        "",
        "  devices: s.table({",
        "    name: s.string(),",
        "    tags: s.array(s.string()),",
        "    loginCount: s.int(),",
        "  }, {",
        "  }),",
        "",
        "  sessions: s.table({",
        "    createdAt: s.allowExternalProvenanceName(s.timestamp()),",
        "    retryCounts: s.array(s.int()).optional(),",
        "    userId: s.uuid().optional(),",
        "  }, {",
        '    userIdRelation: s.rel("accountHolders", "userId"),',
        "  }),",
        "};",
        "",
        "type AppSchema = s.Schema<typeof schema>;",
        "export const app: s.App<AppSchema> = s.defineApp(schema);",
        "export const wasmSchema = app.wasmSchema;",
        "",
        "export const permissions = s.definePermissions(app, ({ policy }) => {",
        "  policy.accountHolders.allowRead.never();",
        "  policy.accountHolders.allowInsert.never();",
        "  policy.accountHolders.allowUpdate.never();",
        "  policy.accountHolders.allowDelete.never();",
        "",
        "  policy.devices.allowRead.never();",
        "  policy.devices.allowInsert.never();",
        "  policy.devices.allowUpdate.never();",
        "  policy.devices.allowDelete.never();",
        "",
        "  policy.sessions.allowRead.never();",
        "  policy.sessions.allowInsert.never();",
        "  policy.sessions.allowUpdate.never();",
        "  policy.sessions.allowDelete.never();",
        "});",
        "",
      ].join("\n"),
    );
  });

  it("generates deny-all policies for every Better Auth table", () => {
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
      session: "better-auth-session",
    } as const;
    const source = buildJazzSchemaSourceText({
      tables,
      getModelName: (model) => tableNamesByModel[model as keyof typeof tableNamesByModel],
      getFieldName: ({ field }) => field,
    });

    for (const tableName of Object.values(tableNamesByModel)) {
      const policyTarget =
        tableName === "better_auth_user"
          ? "policy.better_auth_user"
          : 'policy["better-auth-session"]';

      expect(source).toContain(`${policyTarget}.allowRead.never();`);
      expect(source).toContain(`${policyTarget}.allowInsert.never();`);
      expect(source).toContain(`${policyTarget}.allowUpdate.never();`);
      expect(source).toContain(`${policyTarget}.allowDelete.never();`);
    }

    expect(source.match(/\.allow(Read|Insert|Update|Delete)\.never\(\);/g)).toHaveLength(
      Object.keys(tables).length * 4,
    );
  });

  it("throws when schema.ts generation encounters magic column names", () => {
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

    expect(() => buildJazzSchemaSourceTextFromTables({ tables })).toThrow(
      /reserved for magic columns/i,
    );
  });

  it("throws when schema.ts generation encounters fields renamed to id", () => {
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

    expect(() => buildJazzSchemaSourceTextFromTables({ tables })).toThrow(
      /conflicts with reserved Jazz row id/i,
    );
  });

  it("throws when schema.ts generation encounters bigint numbers", () => {
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

    expect(() => buildJazzSchemaSourceTextFromTables({ tables })).toThrow(/cannot represent/i);
  });

  it("throws when schema.ts generation encounters bigint number arrays", () => {
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

    expect(() => buildJazzSchemaSourceTextFromTables({ tables })).toThrow(/cannot represent/i);
  });

  it("throws when schema.ts generation encounters non-id references", () => {
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

    expect(() => buildJazzSchemaSourceTextFromTables({ tables })).toThrow(
      /only supports references to "id"/i,
    );
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

    expect(buildJazzSchemaSourceTextFromTables({ tables })).toContain(
      'ownerRelation: s.rel("user", "owner")',
    );
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

    expect(buildJazzSchemaSourceTextFromTables({ tables })).toContain(
      'ownersRelation: s.rel("user", "owners")',
    );
  });
});
