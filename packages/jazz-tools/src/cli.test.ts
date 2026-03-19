import { spawnSync, type SpawnSyncReturns } from "node:child_process";
import { access, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { build, createMigration, exportSchema, pushMigration } from "./cli.js";

const dslPath = fileURLToPath(new URL("./dsl.ts", import.meta.url));
const typedAppPath = fileURLToPath(new URL("./typed-app.ts", import.meta.url));
const migrationsPath = fileURLToPath(new URL("./migrations.ts", import.meta.url));
const permissionsDslPath = fileURLToPath(new URL("./permissions/index.ts", import.meta.url));
const distDslPath = fileURLToPath(new URL("../dist/dsl.js", import.meta.url));
const distTypedAppPath = fileURLToPath(new URL("../dist/typed-app.js", import.meta.url));
const distPermissionsDslPath = fileURLToPath(
  new URL("../dist/permissions/index.js", import.meta.url),
);
const binPath = fileURLToPath(new URL("../bin/jazz-tools.js", import.meta.url));

const tempRoots: string[] = [];

afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function createWorkspace(): Promise<{ root: string; schemaDir: string }> {
  const root = await mkdtemp(join(tmpdir(), "jazz-tools-cli-test-"));
  tempRoots.push(root);
  const schemaDir = join(root, "schema");
  await mkdir(schemaDir, { recursive: true });
  return { root, schemaDir };
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function rootSchemaWithoutInlinePermissions(
  dslImportPath: string = dslPath,
  typedAppImportPath: string = typedAppPath,
): string {
  return `
import { col } from ${JSON.stringify(dslImportPath)};
import { defineApp, type DefinedSchema, type TypedApp } from ${JSON.stringify(typedAppImportPath)};

const schemaDef = {
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    ownerId: col.string(),
  },
};

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);
`;
}

function rootSchemaWithInlinePermissions(dslImportPath: string = dslPath): string {
  return `
import { table, col } from ${JSON.stringify(dslImportPath)};

table("todos", {
  title: col.string(),
}, {
  permissions: {
    select: { type: "True" },
  },
});
`;
}

function rootPermissionsSchema(
  appImportPath: string = "./schema.ts",
  importPath: string = permissionsDslPath,
): string {
  return `
import { definePermissions } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ ownerId: session.user_id }),
]);
`;
}

function permissionsSchemaMissingExport(): string {
  return `
export const nope = 42;
`;
}

function permissionsSchemaUnknownTable(): string {
  return `
export default {
  ghosts: {
    select: {
      using: { type: "True" },
    },
  },
};
`;
}

function permissionsSchemaNamedExport(
  appImportPath: string = "./schema.ts",
  importPath: string = permissionsDslPath,
): string {
  return `
import { definePermissions } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export const permissions = definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ ownerId: session.user_id }),
]);
`;
}

function permissionsSchemaInvalidShape(): string {
  return `
export default {
  todos: 123,
};
`;
}

describe("cli build", () => {
  it("validates root schema.ts without generating SQL or app artifacts", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await build({ schemaDir: root });

    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "schema", "app.ts"))).toBe(false);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
  });

  it("finds root schema.ts when pointed at the default ./schema shim directory", async () => {
    const { root, schemaDir } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await build({ schemaDir });

    expect(await fileExists(join(schemaDir, "current.sql"))).toBe(false);
    expect(await fileExists(join(schemaDir, "app.ts"))).toBe(false);
  });

  it("loads root permissions.ts that imports ./schema.ts", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());

    await build({ schemaDir: root });

    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
  });

  it("accepts named permissions exports for transitional ergonomics", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaNamedExport());

    await build({ schemaDir: root });
  });

  it("fails when schema.ts uses inline table permissions", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithInlinePermissions());

    await expect(build({ schemaDir: root })).rejects.toThrow(
      /inline table permissions are no longer supported/i,
    );
  });

  it("fails when permissions.ts has no default or named permissions export", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaMissingExport());

    await expect(build({ schemaDir: root })).rejects.toThrow(/missing permissions export/i);
  });

  it("fails when permissions.ts references unknown tables", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaUnknownTable());

    await expect(build({ schemaDir: root })).rejects.toThrow(
      /permissions\.ts defines permissions for unknown table\(s\): ghosts/i,
    );
  });

  it("fails when permissions.ts export shape is invalid", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaInvalidShape());

    await expect(build({ schemaDir: root })).rejects.toThrow(/invalid permissions export/i);
  });
});

describe("cli schema export", () => {
  it("prints the compiled schema representation as JSON", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());

    const writes: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    const writeSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      writes.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
      return true;
    }) as typeof process.stdout.write);

    try {
      await exportSchema({ schemaDir: root, format: "json" });
    } finally {
      writeSpy.mockRestore();
      process.stdout.write = originalWrite;
    }

    const exported = JSON.parse(writes.join(""));
    expect(exported.projects.columns[0].name).toBe("name");
    expect(exported.todos.columns.map((column: { name: string }) => column.name)).toEqual([
      "title",
      "ownerId",
    ]);
    expect(exported.todos.policies?.select?.using?.type).toBe("Cmp");
  });
});

describe("cli migrations", () => {
  it("generates a typed migration stub from stored schema hashes", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const toHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith(`/schema/${fromHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
            },
          }),
          { status: 200 },
        );
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [
                { name: "title", column_type: { type: "Text" }, nullable: false },
                { name: "notes", column_type: { type: "Text" }, nullable: true },
              ],
            },
          }),
          { status: 200 },
        );
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash,
      toHash,
    });

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain("defineMigration");
    expect(generated).toContain(`fromHash: "${fromHash}"`);
    expect(generated).toContain(`toHash: "${toHash}"`);
    expect(generated).toContain('t.add("notes", { default: null });');
  });

  it("skips table add/drop steps when inferring a migration stub", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "abababababababababababababababababababababababababababababababab";
    const toHash = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith(`/schema/${fromHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
            },
            legacy_users: {
              columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
            },
          }),
          { status: 200 },
        );
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [
                { name: "title", column_type: { type: "Text" }, nullable: false },
                { name: "notes", column_type: { type: "Text" }, nullable: true },
              ],
            },
            users: {
              columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
            },
          }),
          { status: 200 },
        );
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash,
      toHash,
    });

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain('m.table("todos", (t) => {');
    expect(generated).toContain('t.add("notes", { default: null });');
    expect(generated).not.toContain("createTable");
    expect(generated).not.toContain("dropTable");
    expect(generated).not.toContain('"legacy_users"');
    expect(generated).not.toContain('"users"');
  });

  it("pushes a reviewed migration via the admin migrations endpoint", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });

    const fromHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const toHash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const migrationPath = join(migrationsDir, `20260318-rename-${fromHash}-${toHash}.ts`);

    await writeFile(
      migrationPath,
      `
import { col } from ${JSON.stringify(dslPath)};
import { defineMigration } from ${JSON.stringify(migrationsPath)};

export default defineMigration({
  fromHash: ${JSON.stringify(fromHash)},
  toHash: ${JSON.stringify(toHash)},
  from: {
    users: {
      email: col.string(),
    },
  },
  to: {
    users: {
      email_address: col.string(),
    },
  },
  migrate: (m) => {
    m.table("users", (t) => {
      t.rename("email", "email_address");
    });
  },
});
`,
    );

    const fetchMock = vi.fn(async (_input: string, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      expect(body.fromHash).toBe(fromHash);
      expect(body.toHash).toBe(toHash);
      expect(body.forward).toEqual([
        {
          table: "users",
          operations: [
            {
              type: "rename",
              column: "email",
              value: "email_address",
            },
          ],
        },
      ]);
      return new Response(JSON.stringify({ ok: true }), { status: 201 });
    });
    vi.stubGlobal("fetch", fetchMock);

    await pushMigration({
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash,
      toHash,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

function runBin(args: string[]): SpawnSyncReturns<string> {
  return spawnSync(process.execPath, [binPath, ...args], {
    encoding: "utf8",
    env: process.env,
  });
}

describe("bin integration", () => {
  it("routes build through the TypeScript CLI for a root schema.ts project", async () => {
    const { root } = await createWorkspace();
    await writeFile(
      join(root, "schema.ts"),
      rootSchemaWithoutInlinePermissions(distDslPath, distTypedAppPath),
    );

    const result = runBin(["build", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "schema", "app.ts"))).toBe(false);
  });

  it("loads root permissions.ts through the bin entry point", async () => {
    const { root } = await createWorkspace();
    await writeFile(
      join(root, "schema.ts"),
      rootSchemaWithoutInlinePermissions(distDslPath, distTypedAppPath),
    );
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distPermissionsDslPath),
    );

    const result = runBin(["build", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
  });

  it("fails when no root schema.ts can be found", async () => {
    const { root } = await createWorkspace();

    const result = runBin(["build", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Schema file not found");
  });

  it("routes schema export through the TypeScript CLI", async () => {
    const { root } = await createWorkspace();
    await writeFile(
      join(root, "schema.ts"),
      rootSchemaWithoutInlinePermissions(distDslPath, distTypedAppPath),
    );
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distPermissionsDslPath),
    );

    const result = runBin(["schema", "export", "--schema-dir", root, "--format", "json"]);

    expect(result.status).toBe(0);
    const exported = JSON.parse(String(result.stdout));
    expect(
      exported.todos.columns.some((column: { name: string }) => column.name === "ownerId"),
    ).toBe(true);
  });
});
