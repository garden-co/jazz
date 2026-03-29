import { spawnSync, type SpawnSyncReturns } from "node:child_process";
import { access, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createMigration,
  exportSchema,
  permissionsStatus,
  pushMigration,
  pushPermissions,
  validate,
} from "./cli.js";

const dslPath = fileURLToPath(new URL("./dsl.ts", import.meta.url));
const indexPath = fileURLToPath(new URL("./index.ts", import.meta.url));
const distIndexPath = fileURLToPath(new URL("../dist/index.js", import.meta.url));
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
  await writeFile(join(root, "package.json"), '{ "type": "module" }\n');
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

async function captureConsoleLogs<T>(
  run: () => Promise<T>,
): Promise<{ result: T; logs: string[] }> {
  const logs: string[] = [];
  const spy = vi
    .spyOn(console, "log")
    .mockImplementation((message?: unknown, ...rest: unknown[]) => {
      logs.push([message, ...rest].map((value) => String(value ?? "")).join(" "));
    });

  try {
    const result = await run();
    return { result, logs };
  } finally {
    spy.mockRestore();
  }
}

function rootSchemaWithoutInlinePermissions(indexImportPath: string = indexPath): string {
  return `
import { schema as s } from ${JSON.stringify(indexImportPath)};

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    ownerId: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
}

function rootSchemaWithBooleanTodo(indexImportPath: string = indexPath): string {
  return `
import { schema as s } from ${JSON.stringify(indexImportPath)};

const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
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
  importPath: string = indexPath,
): string {
  return `
import { schema as s } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default s.definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ ownerId: session.user_id }),
]);
`;
}

function rootBooleanLiteralPermissionsSchema(
  appImportPath: string = "./schema.ts",
  importPath: string = indexPath,
): string {
  return `
import { schema as s } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default s.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.where({ done: true }),
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
  importPath: string = indexPath,
): string {
  return `
import { schema as s } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export const permissions = s.definePermissions(app, ({ policy, session }) => [
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

function storedRootSchema() {
  return {
    projects: {
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "ownerId", column_type: { type: "Text" }, nullable: false },
      ],
    },
  };
}

describe("cli validate", () => {
  it("validates root schema.ts without generating SQL or app artifacts", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await validate({ schemaDir: root });

    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "schema", "app.ts"))).toBe(false);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
  });

  it("finds root schema.ts when pointed at the default ./schema shim directory", async () => {
    const { root, schemaDir } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await validate({ schemaDir });

    expect(await fileExists(join(schemaDir, "current.sql"))).toBe(false);
    expect(await fileExists(join(schemaDir, "app.ts"))).toBe(false);
  });

  it("loads root permissions.ts that imports ./schema.ts", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());

    const { logs } = await captureConsoleLogs(() => validate({ schemaDir: root }));

    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
    expect(logs).toContain(`Loaded structural schema from ${join(root, "schema.ts")}.`);
    expect(logs).toContain(`Loaded current permissions from ${join(root, "permissions.ts")}.`);
    expect(logs).toContain(
      "Permission-only changes do not create schema hashes or require migrations.",
    );
  });

  it("accepts named permissions exports for transitional ergonomics", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaNamedExport());

    await validate({ schemaDir: root });
  });

  it("fails when schema.ts uses inline table permissions", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithInlinePermissions());

    await expect(validate({ schemaDir: root })).rejects.toThrow(
      /inline table permissions are no longer supported/i,
    );
  });

  it("fails when permissions.ts has no default or named permissions export", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaMissingExport());

    await expect(validate({ schemaDir: root })).rejects.toThrow(/missing permissions export/i);
  });

  it("fails when permissions.ts references unknown tables", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaUnknownTable());

    await expect(validate({ schemaDir: root })).rejects.toThrow(
      /permissions\.ts defines permissions for unknown table\(s\): ghosts/i,
    );
  });

  it("fails when permissions.ts export shape is invalid", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaInvalidShape());

    await expect(validate({ schemaDir: root })).rejects.toThrow(/invalid permissions export/i);
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
    expect(exported.todos.policies).toBeUndefined();
  });
});

describe("cli migrations", () => {
  it("generates a typed migration stub from stored schema hashes", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const toHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

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

    const { result: filePath, logs } = await captureConsoleLogs(() =>
      createMigration({
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        fromHash: fromShortHash,
        toHash: toShortHash,
      }),
    );

    const generated = await readFile(filePath, "utf8");
    expect(filePath).toContain(`-unnamed-${fromShortHash}-${toShortHash}.ts`);
    expect(generated).toContain("s.defineMigration");
    expect(generated).toContain(`fromHash: "${fromShortHash}"`);
    expect(generated).toContain(`toHash: "${toShortHash}"`);
    expect(generated).toContain("migrate: {");
    expect(generated).toContain('"notes": s.add.string({ default: null }),');
    expect(logs).toContain("Migration stubs are only for structural schema changes.");
    expect(logs).toContain(
      "Permission-only changes do not create schema hashes or require migrations.",
    );
  });

  it("skips table add/drop steps when inferring a migration stub", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "abababababababababababababababababababababababababababababababab";
    const toHash = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

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
      fromHash: fromShortHash,
      toHash: toShortHash,
    });

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain('"todos": {');
    expect(generated).toContain('"notes": s.add.string({ default: null }),');
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
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);
    const migrationPath = join(migrationsDir, `20260318-rename-${fromShortHash}-${toShortHash}.ts`);

    await writeFile(
      migrationPath,
      `
import { schema as s } from ${JSON.stringify(indexPath)};

export default s.defineMigration({
  migrate: {
    users: {
      email_address: s.renameFrom("email"),
    },
  },
  fromHash: ${JSON.stringify(fromShortHash)},
  toHash: ${JSON.stringify(toShortHash)},
  from: {
    users: s.table({
      email: s.string(),
    }),
  },
  to: {
    users: s.table({
      email_address: s.string(),
    }),
  },
});
`,
    );

    const fetchMock = vi.fn(async (_input: string, init?: RequestInit) => {
      if (_input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

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
      fromHash: fromShortHash,
      toHash: toShortHash,
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});

describe("cli permissions", () => {
  it("reports the current permissions head against the matching stored structural schema", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());

    const schemaHash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [schemaHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${schemaHash}`)) {
        return new Response(JSON.stringify(storedRootSchema()), { status: 200 });
      }

      if (input.endsWith("/admin/permissions/head")) {
        return new Response(
          JSON.stringify({
            head: {
              schemaHash,
              version: 3,
              parentBundleObjectId: "11111111-1111-1111-1111-111111111111",
              bundleObjectId: "22222222-2222-2222-2222-222222222222",
            },
          }),
          { status: 200 },
        );
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const { logs } = await captureConsoleLogs(() =>
      permissionsStatus({
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        schemaDir: root,
      }),
    );

    expect(logs).toContain(`Loaded structural schema from ${join(root, "schema.ts")}.`);
    expect(logs).toContain(`Loaded current permissions from ${join(root, "permissions.ts")}.`);
    expect(logs).toContain(
      `Local structural schema matches stored hash ${schemaHash.slice(0, 12)}.`,
    );
    expect(logs).toContain(`Server permissions head is v3 on ${schemaHash.slice(0, 12)}.`);
    expect(logs).toContain(
      "Next push will require parent bundle 22222222-2222-2222-2222-222222222222.",
    );
  });

  it("publishes permissions with the current head bundle as the expected parent", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());

    const schemaHash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    const currentHead = {
      schemaHash,
      version: 2,
      parentBundleObjectId: "11111111-1111-1111-1111-111111111111",
      bundleObjectId: "22222222-2222-2222-2222-222222222222",
    };

    const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [schemaHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${schemaHash}`)) {
        return new Response(JSON.stringify(storedRootSchema()), { status: 200 });
      }

      if (input.endsWith("/admin/permissions/head")) {
        return new Response(JSON.stringify({ head: currentHead }), { status: 200 });
      }

      if (input.endsWith("/admin/permissions")) {
        const body = JSON.parse(String(init?.body));
        expect(body.schemaHash).toBe(schemaHash);
        expect(body.expectedParentBundleObjectId).toBe(currentHead.bundleObjectId);
        expect(Object.keys(body.permissions)).toContain("todos");
        return new Response(
          JSON.stringify({
            head: {
              schemaHash,
              version: 3,
              parentBundleObjectId: currentHead.bundleObjectId,
              bundleObjectId: "33333333-3333-3333-3333-333333333333",
            },
          }),
          { status: 201 },
        );
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const { logs } = await captureConsoleLogs(() =>
      pushPermissions({
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        schemaDir: root,
      }),
    );

    expect(logs).toContain(`Resolved structural schema hash ${schemaHash.slice(0, 12)}.`);
    expect(logs).toContain(`Publishing from parent v2 on ${schemaHash.slice(0, 12)}.`);
    expect(logs).toContain(`Published permissions head v3 on ${schemaHash.slice(0, 12)}.`);
    expect(logs).toContain(
      "Permission-only changes do not create schema hashes or require migrations.",
    );
  });

  it("publishes permission literals using tagged wire values", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithBooleanTodo());
    await writeFile(join(root, "permissions.ts"), rootBooleanLiteralPermissionsSchema());

    const schemaHash = "abababababababababababababababababababababababababababababababab";
    const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [schemaHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${schemaHash}`)) {
        return new Response(
          JSON.stringify({
            todos: {
              columns: [
                { name: "title", column_type: { type: "Text" }, nullable: false },
                { name: "done", column_type: { type: "Boolean" }, nullable: false },
              ],
            },
          }),
          { status: 200 },
        );
      }

      if (input.endsWith("/admin/permissions/head")) {
        return new Response(JSON.stringify({ head: null }), { status: 200 });
      }

      if (input.endsWith("/admin/permissions")) {
        const body = JSON.parse(String(init?.body));
        expect(body.permissions.todos.select.using).toEqual({
          type: "Cmp",
          column: "done",
          op: "Eq",
          value: {
            type: "Literal",
            value: {
              type: "Boolean",
              value: true,
            },
          },
        });
        return new Response(
          JSON.stringify({
            head: {
              schemaHash,
              version: 1,
              parentBundleObjectId: null,
              bundleObjectId: "99999999-9999-9999-9999-999999999999",
            },
          }),
          { status: 201 },
        );
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    await pushPermissions({
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      schemaDir: root,
    });

    expect(fetchMock).toHaveBeenCalled();
  });
});

function runBin(args: string[]): SpawnSyncReturns<string> {
  return spawnSync(process.execPath, [binPath, ...args], {
    encoding: "utf8",
    env: process.env,
  });
}

describe("bin integration", () => {
  it("routes validate through the TypeScript CLI for a root schema.ts project", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "schema", "app.ts"))).toBe(false);
  });

  it("loads root permissions.ts through the validate command", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
  });

  it("fails when no root schema.ts can be found", async () => {
    const { root } = await createWorkspace();

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Schema file not found");
  });

  it("keeps build as a compatibility alias", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(["build", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "schema", "app.ts"))).toBe(false);
  });

  it("routes schema export through the TypeScript CLI", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );

    const result = runBin(["schema", "export", "--schema-dir", root, "--format", "json"]);

    expect(result.status).toBe(0);
    const exported = JSON.parse(String(result.stdout));
    expect(
      exported.todos.columns.some((column: { name: string }) => column.name === "ownerId"),
    ).toBe(true);
  });
});
