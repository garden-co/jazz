import { spawnSync, type SpawnSyncReturns } from "node:child_process";
import { constants } from "node:fs";
import {
  access,
  chmod,
  copyFile,
  mkdtemp,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, assert, describe, expect, it, vi } from "vitest";
import { loadWasmModule } from "./runtime/client.js";
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
const bootstrapVerifierPath = fileURLToPath(
  new URL("../scripts/verify-packed-runtime-bootstrap.mjs", import.meta.url),
);

const packageRoot = dirname(fileURLToPath(import.meta.url));
const tmpBase = join(packageRoot, ".test-tmp");
const tempRoots: string[] = [];

afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function createWorkspace(): Promise<{ root: string; schemaDir: string }> {
  await mkdir(tmpBase, { recursive: true });
  const root = await mkdtemp(join(tmpBase, "jazz-tools-cli-test-"));
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

async function computeTestSchemaHash(schema: object): Promise<string> {
  const wasmModule = await loadWasmModule();
  const runtime = new wasmModule.WasmRuntime(
    JSON.stringify(schema),
    "jazz-tools-cli-test",
    "dev",
    "main",
    undefined,
    undefined,
  );

  try {
    return runtime.getSchemaHash();
  } finally {
    (runtime as { free?: () => void }).free?.();
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

function rootSchemaWithTodoNotes(indexImportPath: string = indexPath): string {
  return `
import { schema as s } from ${JSON.stringify(indexImportPath)};

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    ownerId: s.string(),
    notes: s.string().optional(),
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

function storedRootSchemaWithReorderedColumns() {
  return {
    projects: {
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
    todos: {
      columns: [
        { name: "ownerId", column_type: { type: "Text" }, nullable: false },
        { name: "title", column_type: { type: "Text" }, nullable: false },
      ],
    },
  };
}

function storedSchemaResponse(
  schema: object,
  publishedAt: number | null = null,
  status: number = 200,
) {
  return new Response(
    JSON.stringify({
      schema,
      publishedAt,
    }),
    { status },
  );
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

  it("fails when pointed at the legacy ./schema shim directory", async () => {
    const { root, schemaDir } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await expect(validate({ schemaDir })).rejects.toThrow(/schema file not found/i);
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

  it("loads src/schema.ts and src/permissions.ts when schemaDir points at the app root", async () => {
    const { root } = await createWorkspace();
    const srcDir = join(root, "src");
    await mkdir(srcDir, { recursive: true });
    await writeFile(join(srcDir, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(srcDir, "permissions.ts"), rootPermissionsSchema());

    const { logs } = await captureConsoleLogs(() => validate({ schemaDir: root }));

    expect(logs).toContain(`Loaded structural schema from ${join(srcDir, "schema.ts")}.`);
    expect(logs).toContain(`Loaded current permissions from ${join(srcDir, "permissions.ts")}.`);
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
  it("prints the compiled schema representation as JSON and writes a snapshot", async () => {
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
      await exportSchema({ schemaDir: root });
    } finally {
      writeSpy.mockRestore();
      process.stdout.write = originalWrite;
    }

    const exported = JSON.parse(writes.join(""));
    const snapshotFiles = (await readdir(join(root, "migrations", "snapshots"))).filter((name) =>
      name.endsWith(".json"),
    );
    expect(exported.projects.columns[0].name).toBe("name");
    expect(exported.todos.columns.map((column: { name: string }) => column.name)).toEqual([
      "title",
      "ownerId",
    ]);
    expect(exported.todos.policies).toBeUndefined();
    expect(snapshotFiles).toHaveLength(1);
    expect(snapshotFiles[0]).toMatch(/^\d{8}T\d{6}-[0-9a-f]{12}\.json$/i);
  });

  it("does not write a duplicate snapshot when exporting the current schema twice", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    const originalWrite = process.stdout.write.bind(process.stdout);
    const writeSpy = vi.spyOn(process.stdout, "write").mockImplementation((() => {
      return true;
    }) as typeof process.stdout.write);

    try {
      await exportSchema({ schemaDir: root });
      await exportSchema({ schemaDir: root });
    } finally {
      writeSpy.mockRestore();
      process.stdout.write = originalWrite;
    }

    const snapshotFiles = (await readdir(join(root, "migrations", "snapshots"))).filter((name) =>
      name.endsWith(".json"),
    );
    expect(snapshotFiles).toHaveLength(1);
  });

  it("prints the compiled schema representation from src/schema.ts", async () => {
    const { root } = await createWorkspace();
    const srcDir = join(root, "src");
    await mkdir(srcDir, { recursive: true });
    await writeFile(join(srcDir, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(srcDir, "permissions.ts"), rootPermissionsSchema());

    const writes: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    const writeSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      writes.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
      return true;
    }) as typeof process.stdout.write);

    try {
      await exportSchema({ schemaDir: root });
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
  });
});

describe("cli migrations", () => {
  it("writes an initial committed snapshot on first run", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const snapshotsDir = join(migrationsDir, "snapshots");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    const { result, logs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
      }),
    );

    expect(result).toBeNull();
    const snapshotFiles = (await readdir(snapshotsDir)).filter((name) => name.endsWith(".json"));
    expect(snapshotFiles).toHaveLength(1);
    expect(snapshotFiles[0]).toMatch(/^\d{8}T\d{6}-[0-9a-f]{12}\.json$/i);
    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toHaveLength(0);
    expect(logs.some((line) => line.startsWith("Wrote initial schema snapshot:"))).toBe(true);
    expect(logs).toContain(
      "No migration created because there was no previous local schema baseline.",
    );
  });

  it("creates a migration from the latest committed snapshot and then no-ops when rerun", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const snapshotsDir = join(migrationsDir, "snapshots");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await createMigration({
      schemaDir: root,
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
    });

    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());
    // Wait for 1s to avoid migration timestamp collisions
    await new Promise((resolve) => setTimeout(resolve, 1100));

    const { result: filePath, logs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
      }),
    );

    expect(filePath).not.toBeNull();
    if (!filePath) {
      throw new Error("Expected createMigration() to return a migration file path.");
    }
    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain('"notes": s.add.string({ default: null }),');
    expect((await readdir(snapshotsDir)).filter((name) => name.endsWith(".json"))).toHaveLength(2);
    expect(logs.some((line) => line.startsWith("Generated:"))).toBe(true);

    const filesBeforeNoop = await readdir(snapshotsDir);
    const { result: noopResult, logs: noopLogs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
      }),
    );

    expect(noopResult).toBeNull();
    expect(await readdir(snapshotsDir)).toEqual(filesBeforeNoop);
    expect(noopLogs).toContain("No structural schema changes detected.");
  });

  it("uses --name to generate a named migration file and skips the rename reminder", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await createMigration({
      schemaDir: root,
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
    });

    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

    const { result: filePath, logs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        name: "Add Todo Notes",
      }),
    );

    expect(filePath).not.toBeNull();
    assert(filePath, "Expected createMigration() to return a migration file path.");

    expect(filePath).toContain("-add-todo-notes-");
    expect(filePath).not.toContain("-unnamed-");
    expect(logs).not.toContain("2. Rename the file by replacing 'unnamed'.");
  });

  it("generates a typed migration stub from an explicit historical fromHash to the current schema", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const snapshotsDir = join(migrationsDir, "snapshots");
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());
    const fromHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const fromShortHash = fromHash.slice(0, 12);

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${fromHash}`)) {
        return storedSchemaResponse({
          todos: {
            columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const { result: filePath, logs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        fromHash: fromShortHash,
      }),
    );

    if (!filePath) {
      throw new Error("Expected createMigration() to return a migration file path.");
    }
    const generated = await readFile(filePath, "utf8");
    expect(filePath).toContain(`-unnamed-${fromShortHash}-`);
    expect(generated).toContain("s.defineMigration");
    expect(generated).toContain(`fromHash: "${fromShortHash}"`);
    expect(generated).toContain("migrate: {");
    expect(generated).toContain('"notes": s.add.string({ default: null }),');
    const snapshotFiles = (await readdir(snapshotsDir)).filter((name) => name.endsWith(".json"));
    expect(snapshotFiles).toHaveLength(2);
    expect(
      snapshotFiles.some(
        (name) => /^\d{8}T\d{6}-/.test(name) && name.endsWith(`-${fromShortHash}.json`),
      ),
    ).toBe(true);
    expect(logs).toContain("Migration stubs are only for structural schema changes.");
    expect(logs).toContain(
      "Permission-only changes do not create schema hashes or require migrations.",
    );
  });

  it("renders createTables and dropTables when inferring table add/drop steps", async () => {
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
        return storedSchemaResponse({
          todos: {
            columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
          },
          legacy_users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return storedSchemaResponse({
          todos: {
            columns: [
              { name: "title", column_type: { type: "Text" }, nullable: false },
              { name: "notes", column_type: { type: "Text" }, nullable: true },
            ],
          },
          users: {
            columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      schemaDir: root,
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash: fromShortHash,
      toHash: toShortHash,
    });

    if (!filePath) {
      throw new Error("Expected createMigration() to return a migration file path.");
    }
    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain('"todos": {');
    expect(generated).toContain('"notes": s.add.string({ default: null }),');
    expect(generated).toContain("createTables: {");
    expect(generated).toContain('"users": true,');
    expect(generated).toContain("dropTables: {");
    expect(generated).toContain('"legacy_users": true,');
  });

  it("suggests renameTables for a single exact table rename", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
    const toHash = "1212121212121212121212121212121212121212121212121212121212121212";
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${fromHash}`)) {
        return storedSchemaResponse({
          users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return storedSchemaResponse({
          people: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      schemaDir: root,
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash: fromShortHash,
      toHash: toShortHash,
    });
    assert(filePath);

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain("renameTables: {");
    expect(generated).toContain('people: s.renameTableFrom("users"),');
    expect(generated).toContain("from: {");
    expect(generated).toContain('"users": s.table({');
    expect(generated).toContain("to: {");
    expect(generated).toContain('"people": s.table({');
    expect(generated).not.toContain("migrate: {");
  });

  it("suggests renameTables for multiple exact unambiguous table renames", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "3434343434343434343434343434343434343434343434343434343434343434";
    const toHash = "5656565656565656565656565656565656565656565656565656565656565656";
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${fromHash}`)) {
        return storedSchemaResponse({
          users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
          orgs: {
            columns: [{ name: "slug", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return storedSchemaResponse({
          companies: {
            columns: [{ name: "slug", column_type: { type: "Text" }, nullable: false }],
          },
          people: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      schemaDir: root,
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash: fromShortHash,
      toHash: toShortHash,
    });
    assert(filePath);

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain("renameTables: {");
    expect(generated).toContain('companies: s.renameTableFrom("orgs"),');
    expect(generated).toContain('people: s.renameTableFrom("users"),');
    expect(generated).not.toContain("createTables: {");
    expect(generated).not.toContain("dropTables: {");
    expect(generated).toContain('"orgs": s.table({');
    expect(generated).toContain('"users": s.table({');
    expect(generated).toContain('"companies": s.table({');
    expect(generated).toContain('"people": s.table({');
    expect(generated).not.toContain("migrate: {");
  });

  it("keeps duplicate-shape table changes as add/drop instead of guessing a rename", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "7878787878787878787878787878787878787878787878787878787878787878";
    const toHash = "9090909090909090909090909090909090909090909090909090909090909090";
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${fromHash}`)) {
        return storedSchemaResponse({
          archived_users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
          users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return storedSchemaResponse({
          people: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const filePath = await createMigration({
      schemaDir: root,
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
      migrationsDir,
      fromHash: fromShortHash,
      toHash: toShortHash,
    });
    assert(filePath);

    const generated = await readFile(filePath, "utf8");
    expect(generated).not.toContain("renameTables: {");
    expect(generated).toContain("createTables: {");
    expect(generated).toContain('"people": true,');
    expect(generated).toContain("dropTables: {");
    expect(generated).toContain('"archived_users": true,');
    expect(generated).toContain('"users": true,');
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

  it("pushes explicit table renames via renamedFrom on the admin migrations payload", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });

    const fromHash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const toHash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);
    const migrationPath = join(
      migrationsDir,
      `20260318-table-rename-${fromShortHash}-${toShortHash}.ts`,
    );

    await writeFile(
      migrationPath,
      `
import { schema as s } from ${JSON.stringify(indexPath)};

export default s.defineMigration({
  renameTables: {
    people: s.renameTableFrom("users"),
  },
  migrate: {
    people: {
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
    people: s.table({
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
          table: "people",
          renamedFrom: "users",
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

  it("pushes explicit createTables and dropTables via the admin migrations payload", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await mkdir(migrationsDir, { recursive: true });

    const fromHash = "1111111111111111111111111111111111111111111111111111111111111111";
    const toHash = "2222222222222222222222222222222222222222222222222222222222222222";
    const fromShortHash = fromHash.slice(0, 12);
    const toShortHash = toHash.slice(0, 12);
    const migrationPath = join(
      migrationsDir,
      `20260318-table-add-drop-${fromShortHash}-${toShortHash}.ts`,
    );

    await writeFile(
      migrationPath,
      `
import { schema as s } from ${JSON.stringify(indexPath)};

export default s.defineMigration({
  createTables: {
    profiles: true,
  },
  dropTables: {
    legacy_profiles: true,
  },
  fromHash: ${JSON.stringify(fromShortHash)},
  toHash: ${JSON.stringify(toShortHash)},
  from: {
    users: s.table({
      email: s.string(),
    }),
    legacy_profiles: s.table({
      bio: s.string().optional(),
    }),
  },
  to: {
    users: s.table({
      email: s.string(),
    }),
    profiles: s.table({
      bio: s.string().optional(),
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
          table: "profiles",
          added: true,
          operations: [],
        },
        {
          table: "legacy_profiles",
          removed: true,
          operations: [],
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
        return storedSchemaResponse(storedRootSchema());
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

  it("loads src/permissions.ts when reporting the current permissions head", async () => {
    const { root } = await createWorkspace();
    const srcDir = join(root, "src");
    await mkdir(srcDir, { recursive: true });
    await writeFile(join(srcDir, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(srcDir, "permissions.ts"), rootPermissionsSchema());

    const schemaHash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [schemaHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${schemaHash}`)) {
        return storedSchemaResponse(storedRootSchema());
      }

      if (input.endsWith("/admin/permissions/head")) {
        return new Response(JSON.stringify({ head: null }), { status: 200 });
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

    expect(logs).toContain(`Loaded structural schema from ${join(srcDir, "schema.ts")}.`);
    expect(logs).toContain(`Loaded current permissions from ${join(srcDir, "permissions.ts")}.`);
  });

  it("matches stored schemas even when server column order differs", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());

    const schemaHash = "ababeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [schemaHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${schemaHash}`)) {
        return storedSchemaResponse(storedRootSchemaWithReorderedColumns());
      }

      if (input.endsWith("/admin/permissions/head")) {
        return new Response(JSON.stringify({ head: null }), { status: 200 });
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

    expect(logs).toContain(
      `Local structural schema matches stored hash ${schemaHash.slice(0, 12)}.`,
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
        return storedSchemaResponse(storedRootSchema());
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
        return storedSchemaResponse({
          todos: {
            columns: [
              { name: "title", column_type: { type: "Text" }, nullable: false },
              { name: "done", column_type: { type: "Boolean" }, nullable: false },
            ],
          },
        });
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

function runBin(
  args: string[],
  options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
): SpawnSyncReturns<string> {
  return spawnSync(process.execPath, [binPath, ...args], {
    encoding: "utf8",
    cwd: options.cwd,
    env: options.env ?? process.env,
  });
}

function hostNativeBinaryName(): string | null {
  switch (`${process.platform}-${process.arch}`) {
    case "darwin-arm64":
      return "jazz-tools-darwin-arm64";
    case "darwin-x64":
      return "jazz-tools-darwin-x64";
    case "linux-arm64":
      return "jazz-tools-linux-arm64";
    case "linux-x64":
      return "jazz-tools-linux-x64";
    default:
      return null;
  }
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

  it("loads src/schema.ts and src/permissions.ts through the validate command", async () => {
    const { root } = await createWorkspace();
    const srcDir = join(root, "src");
    await mkdir(srcDir, { recursive: true });
    await writeFile(join(srcDir, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(srcDir, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(result.stdout).toContain(`Loaded structural schema from ${join(srcDir, "schema.ts")}.`);
    expect(result.stdout).toContain(
      `Loaded current permissions from ${join(srcDir, "permissions.ts")}.`,
    );
  });

  it("fails when validate is pointed at the legacy ./schema shim directory", async () => {
    const { root, schemaDir } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(["validate", "--schema-dir", schemaDir]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Schema file not found");
  });

  it("fails when no root schema.ts can be found", async () => {
    const { root } = await createWorkspace();

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Schema file not found");
  });

  it("rejects the removed build alias with a validate hint", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(["build", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("renamed to `jazz-tools validate`");
  });

  it("routes schema export through the TypeScript CLI", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );

    const result = runBin(["schema", "export", "--schema-dir", root]);

    expect(result.status).toBe(0);
    const exported = JSON.parse(String(result.stdout));
    expect(
      exported.todos.columns.some((column: { name: string }) => column.name === "ownerId"),
    ).toBe(true);
  });

  it("rejects schema export when both --schema-dir and --schema-hash are provided", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(
      ["schema", "export", "--schema-dir", root, "--schema-hash", "a".repeat(64)],
      { cwd: root },
    );

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("mutually exclusive");
  });

  it("loads schema export --schema-hash from a local snapshot without hitting the server", async () => {
    const { root } = await createWorkspace();
    const schema = storedRootSchema();
    const schemaHash = await computeTestSchemaHash(schema);
    const snapshotsDir = join(root, "migrations", "snapshots");
    await mkdir(snapshotsDir, { recursive: true });
    await writeFile(
      join(snapshotsDir, `20260406T120000-${schemaHash.slice(0, 12)}.json`),
      JSON.stringify(schema, null, 2),
      "utf8",
    );

    const result = runBin(["schema", "export", "--schema-hash", schemaHash], {
      cwd: root,
    });

    expect(result.status).toBe(0);
    expect(JSON.parse(String(result.stdout))).toEqual(schema);
  });

  it("loads schema export --schema-hash from a custom migrations dir", async () => {
    const { root } = await createWorkspace();
    const schema = storedRootSchema();
    const schemaHash = await computeTestSchemaHash(schema);
    const migrationsDir = join(root, "db", "generated-migrations");
    const snapshotsDir = join(migrationsDir, "snapshots");
    await mkdir(snapshotsDir, { recursive: true });
    await writeFile(
      join(snapshotsDir, `20260406T120000-${schemaHash.slice(0, 12)}.json`),
      JSON.stringify(schema, null, 2),
      "utf8",
    );

    const result = runBin(
      ["schema", "export", "--schema-hash", schemaHash, "--migrations-dir", migrationsDir],
      { cwd: root },
    );

    expect(result.status).toBe(0);
    expect(JSON.parse(String(result.stdout))).toEqual(schema);
  });

  it("fetches schema export --schema-hash from the server and persists the snapshot on miss", async () => {
    const { root } = await createWorkspace();
    const schema = storedRootSchema();
    const schemaHash = await computeTestSchemaHash(schema);
    const publishedAt = Date.UTC(2026, 3, 6, 12, 0, 0);
    const writes: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    const writeSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      writes.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
      return true;
    }) as typeof process.stdout.write);

    const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
      expect(input).toContain(`/schema/${schemaHash}`);
      expect(init?.method).toBe("GET");
      expect(init?.headers).toMatchObject({ "X-Jazz-Admin-Secret": "admin-secret" });
      return storedSchemaResponse(schema, publishedAt);
    });
    vi.stubGlobal("fetch", fetchMock);

    try {
      await exportSchema({
        schemaHash,
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
      });
    } finally {
      writeSpy.mockRestore();
      process.stdout.write = originalWrite;
    }

    expect(JSON.parse(writes.join(""))).toEqual(schema);
    const shortSchemaHash = schemaHash.slice(0, 12);
    const snapshotFiles = (await readdir(join(root, "migrations", "snapshots"))).filter((name) =>
      name.endsWith(`-${shortSchemaHash}.json`),
    );
    expect(snapshotFiles).toHaveLength(1);
    expect(snapshotFiles[0]).toBe(`20260406T120000-${shortSchemaHash}.json`);
    expect(
      JSON.parse(await readFile(join(root, "migrations", "snapshots", snapshotFiles[0]!), "utf8")),
    ).toEqual(schema);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("persists schema export --schema-hash into a custom migrations dir", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "db", "generated-migrations");
    const schema = storedRootSchema();
    const schemaHash = await computeTestSchemaHash(schema);
    const publishedAt = Date.UTC(2026, 3, 6, 12, 0, 0);
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

    const writes: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    const writeSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      writes.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
      return true;
    }) as typeof process.stdout.write);

    const exportFetchMock = vi.fn(async (input: string, init?: RequestInit) => {
      expect(input).toContain(`/schema/${schemaHash}`);
      expect(init?.method).toBe("GET");
      expect(init?.headers).toMatchObject({ "X-Jazz-Admin-Secret": "admin-secret" });
      return storedSchemaResponse(schema, publishedAt);
    });
    vi.stubGlobal("fetch", exportFetchMock);

    try {
      await exportSchema({
        schemaHash,
        schemaDir: root,
        migrationsDir,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
      });
    } finally {
      writeSpy.mockRestore();
      process.stdout.write = originalWrite;
    }

    expect(JSON.parse(writes.join(""))).toEqual(schema);
    const snapshotFiles = (await readdir(join(migrationsDir, "snapshots"))).filter((name) =>
      name.endsWith(`-${schemaHash.slice(0, 12)}.json`),
    );
    expect(snapshotFiles).toHaveLength(1);
    expect(
      JSON.parse(await readFile(join(migrationsDir, "snapshots", snapshotFiles[0]!), "utf8")),
    ).toEqual(schema);
    expect(exportFetchMock).toHaveBeenCalledTimes(1);

    // Later migrations use the exported local snapshot
    const migrationFetchMock = vi.fn(async () => {
      throw new Error("Expected createMigration() to use the exported local snapshot.");
    });
    vi.stubGlobal("fetch", migrationFetchMock);

    const filePath = await createMigration({
      schemaDir: root,
      migrationsDir,
      fromHash: schemaHash.slice(0, 12),
      serverUrl: "http://localhost:1625",
      adminSecret: "admin-secret",
    });

    expect(filePath).not.toBeNull();
    expect(migrationFetchMock).not.toHaveBeenCalled();
  });

  it("verifies packed runtime bootstrap with a native-only help probe", async () => {
    const hostBinaryName = hostNativeBinaryName();

    if (!hostBinaryName) {
      return;
    }

    const { root } = await createWorkspace();
    const packageRoot = join(root, "package");
    const nativeDir = join(packageRoot, "bin", "native");
    const argsPath = join(root, "captured-args.txt");
    const binaryPath = join(nativeDir, hostBinaryName);

    await mkdir(nativeDir, { recursive: true });
    await copyFile(binPath, join(packageRoot, "bin", "jazz-tools.js"));
    await writeFile(
      binaryPath,
      `#!/bin/sh
printf '%s\n' "$@" > ${JSON.stringify(argsPath)}
exit 0
`,
      "utf8",
    );
    await chmod(binaryPath, 0o644);

    const result = spawnSync(process.execPath, [bootstrapVerifierPath, packageRoot], {
      encoding: "utf8",
      env: process.env,
    });

    expect(result.status).toBe(0);
    expect(await readFile(argsPath, "utf8")).toBe("create\n--help\n");
    await expect(access(binaryPath, constants.X_OK)).resolves.toBeUndefined();
  });

  it("shows the wrapper command surface in --help output", () => {
    const result = runBin(["--help"]);

    expect(result.status).toBe(0);
    expect(result.stdout).toContain("validate");
    expect(result.stdout).toContain("schema export");
    expect(result.stdout).toContain("permissions push");
    expect(result.stdout).toContain("migrations push");
    expect(result.stdout).toContain("server");
    expect(result.stdout).toContain("create");
  });
});
