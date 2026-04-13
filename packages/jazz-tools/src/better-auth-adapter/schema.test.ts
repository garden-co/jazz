import { describe, expect, it } from "vitest";
import type { BetterAuthDBSchema } from "better-auth/db";
import { buildJazzSchemaSourceTextFromTables, buildJazzSchemaFromTables } from "./schema.js";

describe("better-auth schema helpers", () => {
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
        "const schema = {",
        "  accountHolders: s.table({",
        '    "email-address": s.string(),',
        '    role: s.enum("user", "admin"),',
        "    metadata: s.json().optional(),",
        '    deviceIds: s.array(s.ref("devices")).optional(),',
        "  }),",
        "",
        "  devices: s.table({",
        "    name: s.string(),",
        "    tags: s.array(s.string()),",
        "    loginCount: s.int(),",
        "  }),",
        "",
        "  sessions: s.table({",
        "    createdAt: s.timestamp(),",
        "    retryCounts: s.array(s.int()).optional(),",
        '    userId: s.ref("accountHolders").optional(),',
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

  it("throws when schema.ts generation encounters invalid scalar ref keys", () => {
    const tables = {
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

    expect(() => buildJazzSchemaSourceTextFromTables({ tables })).toThrow(
      /reference keys must end with "Id" or "_id"/i,
    );
  });

  it("throws when schema.ts generation encounters invalid array ref keys", () => {
    const tables = {
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

    expect(() => buildJazzSchemaSourceTextFromTables({ tables })).toThrow(
      /array reference keys must end with "Ids" or "_ids"/i,
    );
  });
});
