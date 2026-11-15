import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
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
  generate,
} from "./cli.js";
import { createAppMigration } from "./dev/project.js";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  tmpBase,
  createWorkspace,
  fileExists,
  captureConsoleLogs,
  writeAppFiles,
  todosApp,
  emptyPermissions,
} = createCliFixtures(import.meta.url);

/** `todos` with a title and a done flag. */
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

describe("cli generate", () => {
  it("checks root jazz.app.json, writing only jazz.generated.ts", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, todosApp(), emptyPermissions());

    await generate({ schemaDir: root });

    expect(await fileExists(join(root, "jazz.generated.ts"))).toBe(true);
    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "schema", "app.ts"))).toBe(false);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
  });

  // A migration entry `migrations create` left a TODO in must not deploy.
  it("refuses a migration that still has a TODO to resolve", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, migratedTodos("TODO: points needs the value existing rows get."));

    await expect(generate({ schemaDir: root })).rejects.toThrow(
      "jazz.app.json: 1 unresolved TODO left by `jazz-tools migrations create` or `upgrade`; replace each with the value it asks for, then run `jazz-tools generate` again:\n" +
        "  migrations[0].migrate.todos.points.default: TODO: points needs the value existing rows get.",
    );
  });

  it("accepts a migration once its TODO is filled in", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, migratedTodos(1));

    await generate({ schemaDir: root });
  });

  it("refuses a generated migration's TODO and accepts it once filled in", async () => {
    const { root } = await createWorkspace();
    const app = { format: 1, tables: { todos: { columns: { title: "string" } } } };
    await writeAppFiles(root, app, emptyPermissions());
    await generate({ schemaDir: root });
    await writeAppFiles(
      root,
      { format: 1, tables: { todos: { columns: { title: "string", points: "int" } } } },
      emptyPermissions(),
    );
    const created = await createAppMigration({ cwd: root, name: "add-points" });
    expect(created.todos).toEqual(["migrations[0].migrate.todos.points.default"]);
    await expect(generate({ schemaDir: root })).rejects.toThrow(
      "migrations[0].migrate.todos.points.default: TODO: added required column `points` needs the value existing rows get",
    );

    const file = join(root, "jazz.app.json");
    const written = JSON.parse(await readFile(file, "utf8"));
    written.migrations[0].migrate.todos.points.default = 1;
    await writeFile(file, JSON.stringify(written, null, 2));
    await generate({ schemaDir: root });
  });

  it("fails when pointed at a directory without jazz.app.json, such as the legacy ./schema shim", async () => {
    const { root, schemaDir } = await createWorkspace();
    await writeAppFiles(root);

    await expect(generate({ schemaDir })).rejects.toThrow(
      `No jazz.app.json in ${schemaDir} (looked in ., src, src/lib).`,
    );
  });

  it("checks root jazz.app.json and jazz.permissions.json", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);

    const { logs } = await captureConsoleLogs(() => generate({ schemaDir: root }));

    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
    expect(logs.join("\n")).toMatch(
      /Checked jazz\.app\.json and jazz\.permissions\.json; wrote .*jazz\.generated\.ts\. Commit it\./,
    );
  });

  it("finds src/jazz.app.json and src/jazz.permissions.json when schemaDir points at the app root", async () => {
    const { root } = await createWorkspace();
    const srcDir = join(root, "src");
    await writeAppFiles(srcDir);

    await generate({ schemaDir: root });

    expect(await fileExists(join(srcDir, "jazz.generated.ts"))).toBe(true);
    expect(await fileExists(join(root, "jazz.generated.ts"))).toBe(false);
  });

  it("warns for each table jazz.permissions.json gives no policy", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, todosApp(), { format: 1, tables: {} });

    const { logs } = await captureConsoleLogs(() => generate({ schemaDir: root }));

    const warning = (table: string) =>
      `table "${table}" has no policies in jazz.permissions.json, so users can't read, insert, update or delete its rows. ` +
      "Your backend isn't checked when it acts as the system, so if only it uses this table, that's fine: " +
      `write \`"${table}": { "read": "false" }\` to say so and silence this warning. ` +
      "Don't add allow rules just to silence it.";
    expect(logs.filter((line) => line.includes("has no policies"))).toEqual([
      warning("projects"),
      warning("todos"),
    ]);
  });

  it("treats true and false as explicit policies", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, todosApp(), {
      format: 1,
      tables: {
        projects: { read: "false" },
        todos: { read: "true", insert: "false", update: "false", delete: "false" },
      },
    });

    const { logs } = await captureConsoleLogs(() => generate({ schemaDir: root }));

    expect(logs.filter((line) => line.includes("has no policies"))).toEqual([]);
    expect(logs.join("\n")).toMatch(/Checked jazz\.app\.json and jazz\.permissions\.json/);
  });

  it("fails when jazz.permissions.json is missing", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, todosApp(), null);

    await expect(generate({ schemaDir: root })).rejects.toThrow(
      /jazz\.permissions\.json is missing/,
    );
  });

  it("fails when jazz.permissions.json references unknown tables", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, todosApp(), { format: 1, tables: { ghosts: { read: "true" } } });

    await expect(generate({ schemaDir: root })).rejects.toThrow(
      "jazz.permissions.json: tables.ghosts: jazz.app.json has no table `ghosts`",
    );
  });

  it("fails when a table's permissions have an invalid shape", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root, todosApp(), { format: 1, tables: { todos: 123 } });

    await expect(generate({ schemaDir: root })).rejects.toThrow(
      "jazz.permissions.json: tables.todos: must be an object with read, insert, update, delete",
    );
  });
});

/** todos gaining a required `points` column, through a migration whose default is `points`. */
function migratedTodos(points: unknown) {
  return {
    format: 1,
    tables: { todos: { columns: { title: "string", ownerId: "string", points: "int" } } },
    history: { v1: { tables: { todos: { columns: { title: "string", ownerId: "string" } } } } },
    migrations: [
      {
        name: "add-points",
        from: "v1",
        migrate: { todos: { points: { add: "int", default: points } } },
      },
    ],
  };
}

async function compileOutput(schemaDir: string): Promise<CompiledOutput> {
  const writes: string[] = [];
  const writeSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
    chunk: string | Uint8Array,
  ) => {
    writes.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
    return true;
  }) as typeof process.stdout.write);
  try {
    await compileSchema({ schemaDir });
  } finally {
    writeSpy.mockRestore();
  }
  return JSON.parse(writes.join("")) as CompiledOutput;
}

interface CompiledOutput {
  schema: {
    current: string;
    schemas: Array<{ hash: string; schema: { tables: Record<string, CompiledTable> } }>;
  };
}

interface CompiledTable {
  columns: Array<{ name: string }>;
  policies?: unknown;
}

function currentTables(compiled: CompiledOutput): Record<string, CompiledTable> {
  return compiled.schema.schemas.find(({ hash }) => hash === compiled.schema.current)!.schema
    .tables;
}

describe("cli schema compile", () => {
  it("prints the compiled schema representation as JSON, without permissions", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);

    const tables = currentTables(await compileOutput(root));

    expect(tables.projects!.columns[0]!.name).toBe("name");
    expect(tables.todos!.columns.map((column) => column.name)).toEqual(["title", "ownerId"]);
    expect(tables.todos!.policies).toBeUndefined();
    expect(await readdir(root)).not.toContain("migrations");
  });

  it("prints the compiled schema representation from src/jazz.app.json", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(join(root, "src"));

    const tables = currentTables(await compileOutput(root));

    expect(tables.projects!.columns[0]!.name).toBe("name");
    expect(tables.todos!.columns.map((column) => column.name)).toEqual(["title", "ownerId"]);
  });
});
