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
import { loadAppFiles } from "./dev/codegen-napi.js";
import * as napi from "jazz-napi";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const { tmpBase, createWorkspace, fileExists, captureConsoleLogs, writeAppFiles, writeTsApp } =
  createCliFixtures(import.meta.url);

/** Whether this jazz-napi checks `$generated` markers (the Rust writer of alpha.66). */
const napiVerifiesGenerated =
  typeof (napi as { verifyGenerated?: unknown }).verifyGenerated === "function";

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

const CHAT_FIXTURE = new URL("./permissions/__fixtures__/chat-react.ts", import.meta.url);

async function chatReactSchema(): Promise<string> {
  return (await readFile(CHAT_FIXTURE, "utf8")).replace('"../../index.js"', '"jazz-tools"');
}

describe("cli generate", () => {
  it("writes jazz.app.json, jazz.permissions.json and jazz.generated.ts from the chat-react schema.ts", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), await chatReactSchema());

    const { logs } = await captureConsoleLogs(() => generate({ schemaDir: root }));

    expect(logs.join("\n")).toMatch(
      /Wrote jazz\.app\.json, jazz\.permissions\.json, .*jazz\.generated\.ts/,
    );
    const example = (file: string) =>
      readFile(new URL(`../../../examples/chat-react/${file}`, import.meta.url), "utf8");
    const written = (file: string) => readFile(join(root, file), "utf8");
    const ours = (await loadAppFiles(
      await written("jazz.app.json"),
      await written("jazz.permissions.json"),
    )) as { current: string; permissions: Record<string, Record<string, unknown>> };
    const theirs = (await loadAppFiles(
      await example("jazz.app.json"),
      await example("jazz.permissions.json"),
    )) as typeof ours;
    // The same schema, and the same policies but the one the DSL rendering restructures.
    expect(ours.current).toBe(theirs.current);
    for (const [table, policies] of Object.entries(theirs.permissions)) {
      for (const [operation, policy] of Object.entries(policies)) {
        if (table === "chatMembers" && operation === "insert") continue;
        expect(ours.permissions[table]![operation], `${table}.${operation}`).toEqual(policy);
      }
    }
    expect(await fileExists(join(root, "jazz.generated.ts"))).toBe(true);
    await generate({ schemaDir: root, check: true });
  });

  it("finds src/schema.ts and writes the files beside it", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(join(root, "src"));

    await generate({ schemaDir: root });

    for (const file of ["jazz.app.json", "jazz.permissions.json", "jazz.generated.ts"]) {
      expect(await fileExists(join(root, "src", file))).toBe(true);
      expect(await fileExists(join(root, file))).toBe(false);
    }
  });

  it("refuses a project without schema.ts", async () => {
    const { root, schemaDir } = await createWorkspace();
    await expect(generate({ schemaDir })).rejects.toThrow(
      `No schema.ts in ${schemaDir} (looked in ., src, src/lib).`,
    );
    await writeAppFiles(root);
    await expect(generate({ schemaDir: root })).rejects.toThrow(/Run `npx jazz-tools upgrade`/);
  });

  it("refuses schema.ts in two roots", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root);
    await writeTsApp(join(root, "src"));
    await expect(generate({ schemaDir: root })).rejects.toThrow(
      /found schema\.ts in both schema\.ts and src\/schema\.ts/,
    );
  });

  it("refuses a schema.ts without the named export `schema`", async () => {
    const { root } = await createWorkspace();
    await writeFile(
      join(root, "schema.ts"),
      'import { schema as s } from "jazz-tools";\nexport default s.defineSchema({});\n',
    );
    await expect(generate({ schemaDir: root })).rejects.toThrow(/export the schema by name/);
  });

  it("reads the permissions from a sibling permissions.ts", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root, { permissionsFile: "permissions.ts" });

    await generate({ schemaDir: root });

    const permissions = JSON.parse(await readFile(join(root, "jazz.permissions.json"), "utf8"));
    expect(permissions.tables.todos).toEqual({ read: "ownerId = session.identity.subject" });
    expect(permissions.tables.projects).toEqual({ read: "false" });
  });

  it("refuses permissions defined twice", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root);
    await writeFile(
      join(root, "permissions.ts"),
      'import { schema } from "./schema";\nexport const permissions = schema.definePermissions({ projects: {}, todos: {} });\n',
    );
    await expect(generate({ schemaDir: root })).rejects.toThrow(/permissions are defined twice/);
  });

  it("denies everything, with a warning, when there are no permissions", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root, { permissions: null });

    const { logs } = await captureConsoleLogs(() => generate({ schemaDir: root }));

    expect(logs.join("\n")).toMatch(/No permissions: .*every table denies everything/);
    const permissions = JSON.parse(await readFile(join(root, "jazz.permissions.json"), "utf8"));
    expect(permissions.tables).toEqual({});
  });

  it("--check passes on fresh files, and fails without writing once the TS changed", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root);
    await generate({ schemaDir: root });
    await generate({ schemaDir: root, check: true });

    const before = await readFile(join(root, "jazz.permissions.json"), "utf8");
    await writeTsApp(root, {
      permissions: "  projects: { read: true },\n  todos: {},",
    });
    await expect(generate({ schemaDir: root, check: true })).rejects.toThrow(
      /jazz\.permissions\.json is out of date with the app's TS: run `jazz-tools generate`/,
    );
    expect(await readFile(join(root, "jazz.permissions.json"), "utf8")).toBe(before);
  });

  it("names the JSON path and the TS line of a rule the DSL refuses", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root, {
      permissions:
        "  projects: {},\n  todos: {\n    read: (todo) => ({ owner: session.user }),\n  },",
    });
    const error = await generate({ schemaDir: root }).catch((e: Error) => e);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(
      /tables\.todos\.read: unknown column "owner" in todos/,
    );
    expect((error as Error).message).toMatch(/schema\.ts:\d+/);
  });

  it("names the TS line of a rule the Rust loader refuses", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root, {
      permissions:
        "  projects: {},\n  todos: {\n    read: (todo) => schema.allowedTo.read(todo.title),\n  },",
    });
    const error = await generate({ schemaDir: root }).catch((e: Error) => e);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/\n  at \S*schema\.ts:13 \(tables\.todos\.read\)$/);
  });

  it.runIf(hasNapi("verifyGenerated"))(
    "refuses to overwrite a file edited by hand unless --force",
    async () => {
      const { root } = await createWorkspace();
      await writeTsApp(root);
      await generate({ schemaDir: root });
      const file = join(root, "jazz.permissions.json");
      const edited = JSON.parse(await readFile(file, "utf8"));
      edited.tables.projects = { read: "true" };
      await writeFile(file, `${JSON.stringify(edited, null, 2)}\n`);
      await writeTsApp(root, { permissions: "  projects: {},\n  todos: {}," });

      await expect(generate({ schemaDir: root })).rejects.toThrow(/edited by hand/);
      await generate({ schemaDir: root, force: true });
      // The hand edit is gone: a table that allows nothing has no entry.
      expect(JSON.parse(await readFile(file, "utf8")).tables.projects).toBeUndefined();
    },
  );

  it("ships the history migrations/ holds", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root);
    await createAppMigration({ cwd: root, name: "initial" });
    await generate({ schemaDir: root });
    await writeTsApp(root, {
      columns: "title: s.string(), ownerId: s.string(), notes: s.string().optional()",
    });
    const created = await createAppMigration({ cwd: root, name: "add-notes" });
    expect(created.changed).toBe(true);
    expect(created.file).toMatch(/add-notes/);

    await generate({ schemaDir: root });

    const app = JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
    expect(Object.keys(app.history ?? {})).toHaveLength(1);
    expect(app.migrations).toHaveLength(1);
    expect(app.tables.todos.columns.notes).toBe("string?");
  });
});

function hasNapi(name: string): boolean {
  return typeof (napi as unknown as Record<string, unknown>)[name] === "function";
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
