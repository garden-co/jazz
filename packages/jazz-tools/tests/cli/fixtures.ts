import { spawn, spawnSync, type SpawnSyncReturns } from "node:child_process";
import { createServer, type Server } from "node:http";
import { mkdtempSync } from "node:fs";
import { access, mkdtemp, mkdir, readdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, vi } from "vitest";
import { structuralSchemaHash } from "../../src/dev/schema-utils.js";
import { createMigration as rawCreateMigration, deploy as rawDeploy } from "../../src/cli.js";

export function createCliFixtures(moduleUrl: string) {
  const dslPath = fileURLToPath(new URL("./dsl.ts", moduleUrl));
  const indexPath = fileURLToPath(new URL("./index.ts", moduleUrl));
  const distIndexPath = fileURLToPath(new URL("../dist/index.js", moduleUrl));
  const distCliPath = fileURLToPath(new URL("../dist/cli.js", moduleUrl));
  const binPath = fileURLToPath(new URL("../bin/jazz-tools.js", moduleUrl));
  const bootstrapVerifierPath = fileURLToPath(
    new URL("../scripts/verify-packed-runtime-bootstrap.mjs", moduleUrl),
  );

  const packageRoot = dirname(fileURLToPath(new URL(moduleUrl)));
  const tmpBase = mkdtempSync(join(tmpdir(), "jazz-tools-cli-tests-"));
  afterAll(() => rm(tmpBase, { recursive: true, force: true }));
  const tempRoots: string[] = [];
  const APP_ID = "test-app";

  function withAppId<T extends { appId?: string }>(options: T): T & { appId: string } {
    return { appId: APP_ID, ...options };
  }

  const createMigration = (options: Parameters<typeof rawCreateMigration>[0]) =>
    rawCreateMigration(withAppId(options));
  const deploy = (options: Omit<Parameters<typeof rawDeploy>[0], "appId"> & { appId?: string }) =>
    rawDeploy(withAppId(options));

  afterEach(async () => {
    vi.unstubAllGlobals();
    await Promise.all(
      tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })),
    );
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

  function spawnMigrationCreate(
    root: string,
    migrationsDir: string,
    pauseAt:
      | "lock-held"
      | "lock-observed"
      | "lock-recovered"
      | "between-publications"
      | "journaled",
    marker: string,
    releaseMarker?: string,
  ) {
    return spawn(
      process.execPath,
      [
        distCliPath,
        "migrations",
        "create",
        "--schema-dir",
        root,
        "--migrations-dir",
        migrationsDir,
      ],
      {
        env: {
          ...process.env,
          NODE_ENV: "test",
          JAZZ_TEST_MIGRATION_PAUSE_AT: pauseAt,
          JAZZ_TEST_MIGRATION_PAUSE_MARKER: marker,
          ...(releaseMarker ? { JAZZ_TEST_MIGRATION_PAUSE_RELEASE_MARKER: releaseMarker } : {}),
        },
        stdio: "ignore",
      },
    );
  }

  async function migrationLockLeftovers(migrationsDir: string): Promise<string[]> {
    return (await readdir(migrationsDir)).filter((name) =>
      name.startsWith(".jazz-create-migration.lock"),
    );
  }

  async function typecheckGeneratedMigration(migrationPath: string): Promise<void> {
    const tsconfigPath = join(dirname(dirname(migrationPath)), "generated-migration.tsconfig.json");
    await writeFile(
      tsconfigPath,
      JSON.stringify({
        compilerOptions: {
          target: "ES2022",
          module: "NodeNext",
          moduleResolution: "NodeNext",
          skipLibCheck: true,
          ignoreDeprecations: "6.0",
          baseUrl: dirname(packageRoot),
          paths: { "jazz-tools": ["src/index.ts"] },
          // This config lives under /tmp, outside node_modules ancestry. Give
          // the real compiler the Node host types installed by this fixture;
          // changing cwd alone does not change TypeScript's type-root lookup.
          typeRoots: [join(dirname(packageRoot), "node_modules", "@types")],
        },
        files: [migrationPath],
      }),
    );
    const result = spawnSync(
      process.execPath,
      [
        join(dirname(packageRoot), "node_modules", "typescript", "bin", "tsc"),
        "--noEmit",
        "--project",
        tsconfigPath,
      ],
      // This intentionally invokes a real TypeScript compiler to validate the
      // generated public API.  Bound the child separately so a compiler hang is
      // reported as such instead of relying only on Vitest's worker timeout.
      { cwd: dirname(packageRoot), encoding: "utf8", timeout: 20_000 },
    );
    if (result.error) {
      throw new Error(
        `Generated migration typecheck could not run: ${result.error.message}\n${result.stdout}\n${result.stderr}`,
      );
    }
    if (result.status !== 0) {
      throw new Error(
        `Generated migration failed to typecheck:\n${result.stdout}\n${result.stderr}`,
      );
    }
  }

  async function waitForCrashMarker(
    marker: string,
    child: ReturnType<typeof spawn>,
  ): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (!(await fileExists(marker))) {
      if (child.exitCode !== null) {
        throw new Error(`migration child exited before reaching ${marker}`);
      }
      if (Date.now() >= deadline) throw new Error(`migration child did not reach ${marker}`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async function waitForMarkers(markers: readonly string[]): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (!(await Promise.all(markers.map(fileExists))).every(Boolean)) {
      if (Date.now() >= deadline) {
        throw new Error(
          `migration children did not all reach lock contention: ${markers.join(", ")}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  async function killChild(child: ReturnType<typeof spawn>): Promise<void> {
    if (child.exitCode !== null) return;
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
    child.kill("SIGKILL");
    await closed;
  }

  async function captureConsoleLogs<T>(
    run: () => Promise<T>,
  ): Promise<{ result: T; logs: string[] }> {
    const logs: string[] = [];
    const ansiEscape = String.fromCodePoint(27);
    const ansiSgrPattern = new RegExp(`${ansiEscape}\\[[0-9;]*m`, "g");
    const stripAnsi = (line: string): string => line.replace(ansiSgrPattern, "");
    const logSpy = vi
      .spyOn(console, "log")
      .mockImplementation((message?: unknown, ...rest: unknown[]) => {
        logs.push(stripAnsi([message, ...rest].map((value) => String(value ?? "")).join(" ")));
      });
    const warnSpy = vi
      .spyOn(console, "warn")
      .mockImplementation((message?: unknown, ...rest: unknown[]) => {
        logs.push(stripAnsi([message, ...rest].map((value) => String(value ?? "")).join(" ")));
      });

    try {
      const result = await run();
      return { result, logs };
    } finally {
      warnSpy.mockRestore();
      logSpy.mockRestore();
    }
  }

  async function computeTestSchemaHash(schema: object): Promise<string> {
    return structuralSchemaHash(schema as Parameters<typeof structuralSchemaHash>[0]);
  }

  function rootSchemaWithoutInlinePermissions(indexImportPath: string = indexPath): string {
    return `
import { schema as s } from ${JSON.stringify(indexImportPath)};

const schema = {
  projects: s.table({
    name: s.string(),
  }, {  }),
  todos: s.table({
    title: s.string(),
    ownerId: s.string(),
  }, {  }),
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
  }, {  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
  }

  function rootSchemaWithConventionalProvenance(indexImportPath: string = indexPath): string {
    return `
import { schema as s } from ${JSON.stringify(indexImportPath)};
const schema = { todos: s.table({ title: s.string(), createdAt: s.timestamp() }, {  }) };
type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
  }

  function rootSchemaWithExternalProvenance(indexImportPath: string = indexPath): string {
    return `
import { schema as s } from ${JSON.stringify(indexImportPath)};
const schema = { imports: s.table({
  sourceCreatedAt: s.timestamp(),
  createdAt: s.allowExternalProvenanceName(s.timestamp()),
  publishedAt: s.timestamp(),
  assignedBy: s.string(),
}, {  }) };
type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
  }

  function rawRootSchemaWithExternalProvenance(indexImportPath: string = indexPath): string {
    return `
import { schema as s } from ${JSON.stringify(indexImportPath)};
export const schema = { imports: s.table({
  createdAt: s.allowExternalProvenanceName(s.timestamp()),
}, {  }) };
`;
  }

  function rootSchemaWithIndexedTodo(indexImportPath: string = indexPath): string {
    return `
import { schema as s } from ${JSON.stringify(indexImportPath)};

const schema = {
  todos: s.table({
    title: s.string(),
    ownerId: s.string(),
  }, {  }).indexOnly(["ownerId"]),
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
  }, {  }),
  todos: s.table({
    title: s.string(),
    ownerId: s.string(),
    notes: s.string().optional(),
  }, {  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
  }

  function rootSchemaWithInlinePermissions(dslImportPath: string = dslPath): string {
    return `
import { table, schemaColumns as s } from ${JSON.stringify(dslImportPath)};

table("todos", {
  title: s.string(),
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
  policy.todos.allowRead.where({ ownerId: session.user.identity.subject }),
]);
`;
  }

  function rootAllExplicitPermissionsSchema(
    appImportPath: string = "./schema.ts",
    importPath: string = indexPath,
  ): string {
    return `
import { schema as s } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default s.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.never(),
  policy.todos.allowUpdate.never(),
  policy.todos.allowDelete.never(),
]);
`;
  }

  function rootTodoOwnerSchema(indexImportPath: string = indexPath): string {
    return `
import { schema as s } from ${JSON.stringify(indexImportPath)};

const schema = {
  todos: s.table({
    title: s.string(),
    ownerId: s.string(),
  }, {  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`;
  }

  function rootReadOnlyPermissionsSchema(
    appImportPath: string = "./schema.ts",
    importPath: string = indexPath,
  ): string {
    return `
import { schema as s } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default s.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.always(),
]);
`;
  }

  function rootUpdateWithoutDeletePermissionsSchema(
    appImportPath: string = "./schema.ts",
    importPath: string = indexPath,
  ): string {
    return `
import { schema as s } from ${JSON.stringify(importPath)};
import { app } from ${JSON.stringify(appImportPath)};

export default s.definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ ownerId: session.user.identity.subject }),
  policy.todos.allowInsert.where({ ownerId: session.user.identity.subject }),
  policy.todos.allowUpdate
    .whereOld({ ownerId: session.user.identity.subject })
    .whereNew({ ownerId: session.user.identity.subject }),
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
  policy.todos.allowRead.where({ ownerId: session.user.identity.subject }),
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

  function storedRootSchemaBeforeOwnerRename() {
    return {
      projects: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "owner_id", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
  }

  function storedUsersEmailSchema() {
    return {
      users: {
        columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
      },
    };
  }

  function storedUsersEmailAddressSchema(columnName: "email_address" | "emailAddress") {
    return {
      users: {
        columns: [{ name: columnName, column_type: { type: "Text" }, nullable: false }],
      },
    };
  }

  function storedPeopleEmailAddressSchema() {
    return {
      people: {
        columns: [{ name: "email_address", column_type: { type: "Text" }, nullable: false }],
      },
    };
  }

  function storedUsersWithLegacyProfilesSchema() {
    return {
      users: {
        columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
      },
      legacy_profiles: {
        columns: [{ name: "bio", column_type: { type: "Text" }, nullable: true }],
      },
    };
  }

  function storedUsersWithProfilesSchema() {
    return {
      users: {
        columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
      },
      profiles: {
        columns: [{ name: "bio", column_type: { type: "Text" }, nullable: true }],
      },
    };
  }

  function storedBooleanTodoSchema() {
    return {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
  }

  function storedBooleanTodoSchemaWithDefaultFalse() {
    return {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          {
            name: "done",
            column_type: { type: "Boolean" },
            nullable: false,
            default: { type: "Boolean", value: false },
          },
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

  function storedCounterSchema() {
    return {
      counters: {
        columns: [
          {
            name: "value",
            column_type: { type: "Integer" },
            nullable: false,
            merge_strategy: "Counter",
          },
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
        schema: { tables: schema },
        publishedAt,
      }),
      { status },
    );
  }

  function runBin(
    args: string[],
    options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
  ): SpawnSyncReturns<string> {
    return spawnSync(process.execPath, ["--no-warnings", binPath, ...args], {
      encoding: "utf8",
      cwd: options.cwd,
      env: options.env ?? process.env,
    });
  }

  async function runCli(
    args: readonly string[],
    options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
  ): Promise<{ status: number | null; stdout: string; stderr: string }> {
    let resolve!: (value: { status: number | null; stdout: string; stderr: string }) => void;
    const promise = new Promise<{ status: number | null; stdout: string; stderr: string }>(
      (resolvePromise) => {
        resolve = resolvePromise;
      },
    );
    const child = spawn(process.execPath, ["--no-warnings", distCliPath, ...args], {
      cwd: options.cwd,
      env: options.env ?? process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    child.on("close", (status) => resolve({ status, stdout, stderr }));
    return promise;
  }

  async function listenForDeployRequest(): Promise<{ server: Server; url: string }> {
    const server = createServer((request, response) => {
      response.statusCode = 400;
      response.end(
        `request=${request.url} secret=${request.headers["x-jazz-admin-secret"] ?? "<missing>"}`,
      );
    });
    let resolveListening!: () => void;
    let rejectListening!: (reason?: unknown) => void;
    const listening = new Promise<void>((resolvePromise, rejectPromise) => {
      resolveListening = resolvePromise;
      rejectListening = rejectPromise;
    });
    server.once("error", rejectListening);
    server.listen(0, "127.0.0.1", resolveListening);
    await listening;
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("Expected deploy test server to have a TCP address.");
    }
    return { server, url: `http://127.0.0.1:${address.port}` };
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

  return {
    indexPath,
    distIndexPath,
    distCliPath,
    binPath,
    bootstrapVerifierPath,
    tmpBase,
    tempRoots,
    APP_ID,
    createMigration,
    deploy,
    withAppId,
    createWorkspace,
    fileExists,
    spawnMigrationCreate,
    migrationLockLeftovers,
    typecheckGeneratedMigration,
    waitForCrashMarker,
    waitForMarkers,
    killChild,
    captureConsoleLogs,
    computeTestSchemaHash,
    rootSchemaWithoutInlinePermissions,
    rootSchemaWithBooleanTodo,
    rootSchemaWithConventionalProvenance,
    rootSchemaWithExternalProvenance,
    rawRootSchemaWithExternalProvenance,
    rootSchemaWithIndexedTodo,
    rootSchemaWithTodoNotes,
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
    storedRootSchema,
    storedRootSchemaBeforeOwnerRename,
    storedUsersEmailSchema,
    storedUsersEmailAddressSchema,
    storedPeopleEmailAddressSchema,
    storedUsersWithLegacyProfilesSchema,
    storedUsersWithProfilesSchema,
    storedBooleanTodoSchema,
    storedBooleanTodoSchemaWithDefaultFalse,
    storedRootSchemaWithReorderedColumns,
    storedCounterSchema,
    storedSchemaResponse,
    runBin,
    runCli,
    listenForDeployRequest,
    hostNativeBinaryName,
  };
}
