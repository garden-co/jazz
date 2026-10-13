import { mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { vi } from "vitest";
import {
  APP_ID_ENV_VARS,
  SERVER_URL_ENV_VARS,
  compileSchema,
  loadDotEnv,
  loadEnvFile,
  readEnvFiles,
  resolveEnvVar,
  validate,
} from "./cli.js";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  tmpBase,
  createWorkspace,
  fileExists,
  captureConsoleLogs,
  rootSchemaWithoutInlinePermissions,
  rootSchemaWithBooleanTodo,
  rootSchemaWithConventionalProvenance,
  rootSchemaWithExternalProvenance,
  rawRootSchemaWithExternalProvenance,
  rootSchemaWithInlinePermissions,
  rootPermissionsSchema,
  rootAllExplicitPermissionsSchema,
  rootTodoOwnerSchema,
  rootReadOnlyPermissionsSchema,
  rootUpdateWithoutDeletePermissionsSchema,
  permissionsSchemaMissingExport,
  permissionsSchemaUnknownTable,
  permissionsSchemaNamedExport,
  permissionsSchemaInvalidShape,
} = createCliFixtures(import.meta.url);

describe("loadDotEnv", () => {
  it("merges values from .env into the supplied env object", async () => {
    await mkdir(tmpBase, { recursive: true });
    const dir = await mkdtemp(join(tmpBase, "jazz-tools-dotenv-"));
    await writeFile(
      join(dir, ".env"),
      ["JAZZ_SERVER_URL=http://from-dotenv", "JAZZ_ADMIN_SECRET=secret-123", ""].join("\n"),
    );
    const env: Record<string, string | undefined> = {};
    loadDotEnv(dir, env);
    expect(env.JAZZ_SERVER_URL).toBe("http://from-dotenv");
    expect(env.JAZZ_ADMIN_SECRET).toBe("secret-123");
    await rm(dir, { recursive: true, force: true });
  });

  it("does not override values already set in the environment", async () => {
    await mkdir(tmpBase, { recursive: true });
    const dir = await mkdtemp(join(tmpBase, "jazz-tools-dotenv-"));
    await writeFile(join(dir, ".env"), "JAZZ_SERVER_URL=http://from-dotenv\n");
    const env: Record<string, string | undefined> = {
      JAZZ_SERVER_URL: "http://from-real-env",
    };
    loadDotEnv(dir, env);
    expect(env.JAZZ_SERVER_URL).toBe("http://from-real-env");
    await rm(dir, { recursive: true, force: true });
  });

  it("is a no-op when no .env file exists", async () => {
    await mkdir(tmpBase, { recursive: true });
    const dir = await mkdtemp(join(tmpBase, "jazz-tools-dotenv-"));
    const env: Record<string, string | undefined> = { PRE_EXISTING: "kept" };
    loadDotEnv(dir, env);
    expect(env).toEqual({ PRE_EXISTING: "kept" });
    await rm(dir, { recursive: true, force: true });
  });

  it("ignores comments and blank lines and strips surrounding quotes", async () => {
    await mkdir(tmpBase, { recursive: true });
    const dir = await mkdtemp(join(tmpBase, "jazz-tools-dotenv-"));
    await writeFile(
      join(dir, ".env"),
      [
        "# leading comment",
        "",
        'JAZZ_SERVER_URL="http://quoted"',
        "JAZZ_ADMIN_SECRET='single-quoted'",
        "JAZZ_APP_ID=plain",
      ].join("\n"),
    );
    const env: Record<string, string | undefined> = {};
    loadDotEnv(dir, env);
    expect(env.JAZZ_SERVER_URL).toBe("http://quoted");
    expect(env.JAZZ_ADMIN_SECRET).toBe("single-quoted");
    expect(env.JAZZ_APP_ID).toBe("plain");
    await rm(dir, { recursive: true, force: true });
  });

  it("loadEnvFile loads from an explicit absolute path", async () => {
    await mkdir(tmpBase, { recursive: true });
    const dir = await mkdtemp(join(tmpBase, "jazz-tools-dotenv-"));
    const filePath = join(dir, ".env.staging");
    await writeFile(filePath, "JAZZ_SERVER_URL=http://staging\n");
    const env: Record<string, string | undefined> = {};
    loadEnvFile(filePath, env);
    expect(env.JAZZ_SERVER_URL).toBe("http://staging");
    await rm(dir, { recursive: true, force: true });
  });

  it("loadEnvFile keeps real env precedence when called repeatedly — first file wins per key", async () => {
    await mkdir(tmpBase, { recursive: true });
    const dir = await mkdtemp(join(tmpBase, "jazz-tools-dotenv-"));
    await writeFile(join(dir, "a.env"), "JAZZ_SERVER_URL=from-a\nA_ONLY=a\n");
    await writeFile(join(dir, "b.env"), "JAZZ_SERVER_URL=from-b\nB_ONLY=b\n");
    const env: Record<string, string | undefined> = {};
    loadEnvFile(join(dir, "a.env"), env);
    loadEnvFile(join(dir, "b.env"), env);
    expect(env.JAZZ_SERVER_URL).toBe("from-a");
    expect(env.A_ONLY).toBe("a");
    expect(env.B_ONLY).toBe("b");
    await rm(dir, { recursive: true, force: true });
  });

  it("readEnvFiles collects --env-file=PATH and --env-file PATH in order", () => {
    expect(
      readEnvFiles([
        "--env-file=.env.staging",
        "deploy",
        "myAppId",
        "--env-file",
        ".env",
        "--admin-secret=xyz",
      ]),
    ).toEqual([".env.staging", ".env"]);
  });

  it("readEnvFiles returns an empty array when no flag is present", () => {
    expect(readEnvFiles(["deploy", "myAppId", "--admin-secret=xyz"])).toEqual([]);
  });

  it("loadEnvFile is a no-op for a missing path", async () => {
    const env: Record<string, string | undefined> = { KEPT: "yes" };
    loadEnvFile(join(tmpBase, "no-such-file.env"), env);
    expect(env).toEqual({ KEPT: "yes" });
  });

  it("flows through resolveEnvVar so deploy-style lookups pick up the .env value", async () => {
    await mkdir(tmpBase, { recursive: true });
    const dir = await mkdtemp(join(tmpBase, "jazz-tools-dotenv-"));
    await writeFile(join(dir, ".env"), "VITE_JAZZ_SERVER_URL=http://vite-from-dotenv\n");
    const env: Record<string, string | undefined> = {};
    loadDotEnv(dir, env);
    expect(resolveEnvVar(SERVER_URL_ENV_VARS, env)).toBe("http://vite-from-dotenv");
    await rm(dir, { recursive: true, force: true });
  });
});

describe("resolveEnvVar", () => {
  it("returns the first name that has a defined value", () => {
    const env = { VITE_JAZZ_SERVER_URL: "http://from-vite" };
    expect(resolveEnvVar(SERVER_URL_ENV_VARS, env)).toBe("http://from-vite");
  });

  it("prefers the unprefixed JAZZ_ name over framework prefixes", () => {
    const env = {
      JAZZ_SERVER_URL: "http://canonical",
      VITE_JAZZ_SERVER_URL: "http://from-vite",
      NEXT_PUBLIC_JAZZ_SERVER_URL: "http://from-next",
      PUBLIC_JAZZ_SERVER_URL: "http://from-sveltekit",
      EXPO_PUBLIC_JAZZ_SERVER_URL: "http://from-expo",
    };
    expect(resolveEnvVar(SERVER_URL_ENV_VARS, env)).toBe("http://canonical");
  });

  it("falls through each known framework prefix", () => {
    for (const name of [
      "PUBLIC_JAZZ_APP_ID",
      "VITE_JAZZ_APP_ID",
      "NEXT_PUBLIC_JAZZ_APP_ID",
      "EXPO_PUBLIC_JAZZ_APP_ID",
    ]) {
      expect(resolveEnvVar(APP_ID_ENV_VARS, { [name]: "app-123" })).toBe("app-123");
    }
  });

  it("ignores empty strings", () => {
    const env = { JAZZ_SERVER_URL: "", VITE_JAZZ_SERVER_URL: "http://from-vite" };
    expect(resolveEnvVar(SERVER_URL_ENV_VARS, env)).toBe("http://from-vite");
  });

  it("returns undefined when nothing matches", () => {
    expect(resolveEnvVar(SERVER_URL_ENV_VARS, {})).toBeUndefined();
  });
});

describe("cli validate", () => {
  it("warns with an exact Jazz provenance replacement", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithConventionalProvenance());
    await writeFile(join(root, "permissions.ts"), "export default {};\n");
    const { logs } = await captureConsoleLogs(() => validate({ schemaDir: root }));
    expect(logs.filter((line) => line.includes("built-in $createdAt"))).toEqual([
      expect.stringContaining("s.allowExternalProvenanceName(...)"),
    ]);
  });

  it("promotes conventional provenance guidance to an error in strict mode", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithConventionalProvenance());
    await writeFile(join(root, "permissions.ts"), "export default {};\n");
    await expect(validate({ schemaDir: root, strictProvenance: true })).rejects.toThrow(
      /forbidden by --strict-provenance[\s\S]*\$createdAt/i,
    );
  });

  it("allows explicitly marked external provenance without false positives", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithExternalProvenance());
    await writeFile(join(root, "permissions.ts"), "export default {};\n");
    const { logs } = await captureConsoleLogs(() =>
      validate({ schemaDir: root, strictProvenance: true }),
    );
    expect(logs.filter((line) => line.includes("built-in $"))).toEqual([]);
  });

  it("preserves external provenance allowances from raw schema exports", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rawRootSchemaWithExternalProvenance());
    await writeFile(join(root, "permissions.ts"), "export default {};\n");
    const { logs } = await captureConsoleLogs(() =>
      validate({ schemaDir: root, strictProvenance: true }),
    );
    expect(logs.filter((line) => line.includes("built-in $"))).toEqual([]);
  });

  it("validates root schema.ts without generating SQL or app artifacts", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), "export default {};\n");

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
    expect(logs).toContain(`Loaded schema from ${join(root, "schema.ts")}.`);
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

    expect(logs).toContain(`Loaded schema from ${join(srcDir, "schema.ts")}.`);
    expect(logs).toContain(`Loaded current permissions from ${join(srcDir, "permissions.ts")}.`);
  });

  it("accepts named permissions exports for transitional ergonomics", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), permissionsSchemaNamedExport());

    await validate({ schemaDir: root });
  });

  it("reports each denied table when permissions.ts explicitly grants nothing", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await writeFile(join(root, "permissions.ts"), "export default {};\n");

    const { logs } = await captureConsoleLogs(() => validate({ schemaDir: root }));

    const warnings = logs.filter((line) => line.includes("no policy declarations"));
    expect(warnings).toHaveLength(2);
    expect(warnings).toContain(
      'Warning: table "projects" has no policy declarations in permissions.ts; the server denies reads, inserts, updates, and deletes without explicit grants.',
    );
    expect(warnings).toContain(
      'Warning: table "todos" has no policy declarations in permissions.ts; the server denies reads, inserts, updates, and deletes without explicit grants.',
    );
  });

  it("warns only for missing operations in partial permissions", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithBooleanTodo());
    await writeFile(join(root, "permissions.ts"), rootReadOnlyPermissionsSchema());

    const { logs } = await captureConsoleLogs(() => validate({ schemaDir: root }));

    const warnings = logs.filter((line) => line.includes('table "todos"'));
    expect(warnings).toEqual([
      'Warning: table "todos" has a policy set but no explicit insert policy in permissions.ts; inserts will be denied.',
      'Warning: table "todos" has a policy set but no explicit update policy in permissions.ts; updates will be denied.',
      'Warning: table "todos" has a policy set but no explicit delete policy in permissions.ts; deletes will be denied.',
    ]);
  });

  it("treats always and never as explicit policies", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithBooleanTodo());
    await writeFile(join(root, "permissions.ts"), rootAllExplicitPermissionsSchema());

    const { logs } = await captureConsoleLogs(() => validate({ schemaDir: root }));

    expect(logs.filter((line) => line.includes("has no explicit"))).toEqual([]);
  });

  it("still warns when delete is omitted but update is explicit", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootTodoOwnerSchema());
    await writeFile(join(root, "permissions.ts"), rootUpdateWithoutDeletePermissionsSchema());

    const { logs } = await captureConsoleLogs(() => validate({ schemaDir: root }));

    const warnings = logs.filter((line) => line.includes("policy set but no explicit"));
    expect(warnings).toEqual([
      'Warning: table "todos" has a policy set but no explicit delete policy in permissions.ts; deletes will be denied.',
    ]);
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

describe("cli schema compile", () => {
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
      await compileSchema({ schemaDir: root });
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
      await compileSchema({ schemaDir: root });
      await compileSchema({ schemaDir: root });
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
      await compileSchema({ schemaDir: root });
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
