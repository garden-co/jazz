import { describe, expect, it } from "vitest";
import type { BetterAuthDBSchema } from "better-auth/db";
import type { WasmSchema } from "../drivers/types.js";
import {
  buildJazzCurrentSchemaTextFromTables,
  buildJazzSchemaFromTables,
  createJazzSchemaModule,
} from "./schema.js";

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

  it("builds current.ts text from Better Auth tables using transformed names", () => {
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

    expect(buildJazzCurrentSchemaTextFromTables({ tables, usePlural: true })).toBe(
      [
        'import { table, col } from "jazz-tools";',
        "",
        'table("accountHolders", {',
        '  "email-address": col.string(),',
        '  role: col.enum("user", "admin"),',
        "  metadata: col.json().optional(),",
        '  deviceIds: col.array(col.ref("devices")).optional(),',
        "});",
        "",
        'table("devices", {',
        "  name: col.string(),",
        "  tags: col.array(col.string()),",
        "  loginCount: col.int(),",
        "});",
        "",
        'table("sessions", {',
        "  createdAt: col.timestamp(),",
        "  retryCounts: col.array(col.int()).optional(),",
        '  userId: col.ref("accountHolders").optional(),',
        "});",
        "",
      ].join("\n"),
    );
  });

  it("throws when current.ts generation encounters magic column names", () => {
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

    expect(() => buildJazzCurrentSchemaTextFromTables({ tables })).toThrow(
      /reserved for magic columns/i,
    );
  });

  it("throws when current.ts generation encounters fields renamed to id", () => {
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

    expect(() => buildJazzCurrentSchemaTextFromTables({ tables })).toThrow(
      /conflicts with reserved Jazz row id/i,
    );
  });

  it("throws when current.ts generation encounters bigint numbers", () => {
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

    expect(() => buildJazzCurrentSchemaTextFromTables({ tables })).toThrow(/cannot represent/i);
  });

  it("throws when current.ts generation encounters bigint number arrays", () => {
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

    expect(() => buildJazzCurrentSchemaTextFromTables({ tables })).toThrow(/cannot represent/i);
  });

  it("throws when current.ts generation encounters non-id references", () => {
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

    expect(() => buildJazzCurrentSchemaTextFromTables({ tables })).toThrow(
      /only supports references to "id"/i,
    );
  });

  it("throws when current.ts generation encounters invalid scalar ref keys", () => {
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

    expect(() => buildJazzCurrentSchemaTextFromTables({ tables })).toThrow(
      /reference keys must end with "Id" or "_id"/i,
    );
  });

  it("throws when current.ts generation encounters invalid array ref keys", () => {
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

    expect(() => buildJazzCurrentSchemaTextFromTables({ tables })).toThrow(
      /array reference keys must end with "Ids" or "_ids"/i,
    );
  });

  it("creates schema modules with default and custom file paths", () => {
    const wasmSchema: WasmSchema = {
      user: {
        columns: [],
      },
    };

    expect(createJazzSchemaModule({ wasmSchema })).toMatchObject({
      path: "./better-auth-jazz-schema.ts",
      overwrite: true,
    });
    expect(
      createJazzSchemaModule({
        file: "./custom-better-auth-schema.ts",
        wasmSchema,
      }),
    ).toMatchObject({
      path: "./custom-better-auth-schema.ts",
      overwrite: true,
    });
  });
});
