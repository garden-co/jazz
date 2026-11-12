import { mkdir, readFile, readdir, symlink, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { assert, vi } from "vitest";
import {
  createMigration as createCatalogueMigration,
  compileSchema as saveBaseline,
} from "./dev/catalogue-project.js";
import { describe, expect, it } from "vitest";
import { validate } from "./cli.js";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  APP_ID,
  createMigration,
  createWorkspace,
  fileExists,
  typecheckGeneratedMigration,
  captureConsoleLogs,
  rootPermissionsSchema,
  rootSchemaWithoutInlinePermissions,
  rootSchemaWithTodoNotes,
  storedRootSchema,
  storedBooleanTodoSchemaWithDefaultFalse,
  storedRootSchemaWithReorderedColumns,
  storedCounterSchema,
  storedSchemaResponse,
} = createCliFixtures(import.meta.url);

describe("cli migrations", () => {
  it.each(["empty-server", "failed-server"])(
    "does not save an initial snapshot without a usable baseline (%s)",
    async (kind) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      const fetchMock = vi.fn(async () =>
        kind === "failed-server"
          ? new Response("unavailable", { status: 503 })
          : Response.json({ latestBundleSchemaHash: null, schemas: [], migrations: [] }),
      );
      vi.stubGlobal("fetch", fetchMock);
      await expect(
        createCatalogueMigration({
          schemaDir: root,
          migrationsDir,
          appId: APP_ID,
          ...(kind === "offline"
            ? {}
            : { serverUrl: "http://localhost:1625", adminSecret: "admin-secret" }),
        }),
      ).rejects.toThrow(
        kind === "failed-server" ? "Migration graph fetch failed" : "No local schema snapshot",
      );
      expect(await fileExists(join(migrationsDir, "snapshots"))).toBe(false);
      expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toEqual([]);
    },
  );

  // An app on its own store (`sqlite()`) has no server to fetch a baseline
  // from: the docs' first step snapshots schema.ts, and the next schema
  // change migrates from it, with migrations/index.ts kept up to date
  // (alpha.60 adopter pass V4).
  it("snapshots schema.ts on the first offline run and migrates from it next", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const shipping = (source: string) =>
      `import { migrations } from "./migrations/index.ts";\n` +
      source.replace('{ migrations: "none" }', "{ migrations }");
    await writeFile(join(root, "schema.ts"), shipping(rootSchemaWithoutInlinePermissions()));
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());
    // The generated stub imports jazz-tools, as in an app.
    await mkdir(join(root, "node_modules"), { recursive: true });
    await symlink(
      fileURLToPath(new URL("..", import.meta.url)),
      join(root, "node_modules", "jazz-tools"),
      "dir",
    );
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    const { logs: firstValidate } = await captureConsoleLogs(() => validate({ schemaDir: root }));
    expect(firstValidate.join("\n")).toContain("no schema snapshot yet");

    const first = await createCatalogueMigration({ schemaDir: root, migrationsDir });
    assert(first.status === "initial-snapshot");
    expect(await fileExists(first.snapshotPath)).toBe(true);
    const index = () => readFile(join(migrationsDir, "index.ts"), "utf8");
    expect(await index()).toContain(first.hash);

    await writeFile(join(root, "schema.ts"), shipping(rootSchemaWithTodoNotes()));
    const { logs: changedValidate } = await captureConsoleLogs(() => validate({ schemaDir: root }));
    expect(changedValidate.join("\n")).toContain("changed since the latest snapshot");
    expect(changedValidate).not.toContain(
      "Permission-only changes do not create schema hashes or require migrations.",
    );

    const second = await createCatalogueMigration({
      schemaDir: root,
      migrationsDir,
      name: "add-notes",
    });
    assert(second.status === "generated");
    expect(second.fromHash).toBe(first.hash);
    // The stub is in migrations/index.ts without another `validate`.
    const bundled = await index();
    expect(bundled).toContain(`"fromHash": "${first.hash}"`);
    expect(bundled).toContain(`"toHash": "${second.toHash}"`);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  // An app still on `{ migrations: "none" }` got snapshots and a stub but no
  // migrations/index.ts, and nothing said why (alpha.61 adopter round).
  it("tells an app on migrations: none to import its migrations, then writes index.ts", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const index = join(root, "migrations", "index.ts");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());
    const hint = 'Add `import { migrations } from "./migrations";` to schema.ts';

    // Nothing to ship yet: no hint before the first snapshot.
    const { logs: fresh } = await captureConsoleLogs(() => validate({ schemaDir: root }));
    expect(fresh.join("\n")).not.toContain(hint);

    const { logs: created } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, migrationsDir }),
    );
    expect(created.join("\n")).toContain(hint);
    expect(await fileExists(index)).toBe(false);

    const { logs: validated } = await captureConsoleLogs(() => validate({ schemaDir: root }));
    expect(validated.join("\n")).toContain(hint);
    expect(await fileExists(index)).toBe(false);

    // Following the hint: the import, then validate, writes the module.
    await writeFile(
      join(root, "schema.ts"),
      `import { migrations } from "./migrations";\n` +
        rootSchemaWithoutInlinePermissions().replace('{ migrations: "none" }', "{ migrations }"),
    );
    const { logs: shipped } = await captureConsoleLogs(() => validate({ schemaDir: root }));
    expect(shipped.join("\n")).not.toContain(hint);
    const module = await readFile(index, "utf8");
    expect(module).toContain("`jazz-tools validate` and `jazz-tools migrations create` rewrite it");
    expect(module).not.toContain("`deploy`");
  });

  it("counts the steps a stub leaves to fill in", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());
    await createCatalogueMigration({ schemaDir: root, migrationsDir });
    await writeFile(
      join(root, "schema.ts"),
      rootSchemaWithTodoNotes().replace("notes: s.string().optional()", "points: s.int()"),
    );
    const { logs } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, migrationsDir, name: "add-points" }),
    );
    expect(logs).toContain("1. Fill in migrate: 1 step marked TODO.");
  });

  // A migration validate passed used to fail only at deploy (adopter b22):
  // renaming an optional column to a required one.
  it("refuses in validate a migration that a deploy would refuse, naming the column", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const shipping = (source: string) =>
      `import { migrations } from "./migrations/index.ts";\n` +
      source.replace('{ migrations: "none" }', "{ migrations }");
    await writeFile(join(root, "schema.ts"), shipping(rootSchemaWithTodoNotes()));
    await writeFile(join(root, "permissions.ts"), rootPermissionsSchema());
    await mkdir(join(root, "node_modules"), { recursive: true });
    await symlink(
      fileURLToPath(new URL("..", import.meta.url)),
      join(root, "node_modules", "jazz-tools"),
      "dir",
    );
    vi.stubGlobal("fetch", vi.fn());
    const first = await createCatalogueMigration({ schemaDir: root, migrationsDir });
    assert(first.status === "initial-snapshot");

    await writeFile(
      join(root, "schema.ts"),
      shipping(
        rootSchemaWithTodoNotes().replace(
          "notes: s.string().optional(),",
          'body: s.string().default(""),',
        ),
      ),
    );
    const second = await createCatalogueMigration({
      schemaDir: root,
      migrationsDir,
      name: "notes-to-body",
    });
    assert(second.status === "generated");
    await validate({ schemaDir: root });

    // The author keeps the values by renaming, but the column's optionality
    // changes with it, which no migration step does.
    const stub = await readFile(second.filePath, "utf8");
    await writeFile(
      second.filePath,
      stub
        .split("\n")
        .filter((line) => !line.includes('"notes": m.drop'))
        .map((line) =>
          line.includes('"body": m.add') ? '      "body": m.renameFrom("notes"),' : line,
        )
        .join("\n"),
    );
    await expect(validate({ schemaDir: root })).rejects.toThrow(
      /The migrations in migrations would not deploy: .*table "todos".*"body" comes out optional but is required/,
    );
  });

  it.each([false, true])(
    "uses the active server schema on first run (unchanged=%s)",
    async (unchanged) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      const baseline = await saveBaseline({
        schemaDir: root,
        migrationsDir: join(root, "server-fixture"),
      });
      if (!unchanged) await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());
      const fetchMock = vi.fn(async (url: string) => {
        if (url.endsWith("/migrations/graph"))
          return Response.json({
            latestBundleSchemaHash: baseline.hash,
            schemas: [baseline.hash],
            migrations: [],
          });
        expect(url).toBe(`http://localhost:1625/apps/${APP_ID}/schema/${baseline.hash}`);
        return Response.json({
          schema: { tables: baseline.schema },
          publishedAt: Date.UTC(2026, 0, 1),
        });
      });
      vi.stubGlobal("fetch", fetchMock);
      const result = await createCatalogueMigration({
        schemaDir: root,
        migrationsDir,
        appId: APP_ID,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
      });
      expect(result.status).toBe(unchanged ? "unchanged" : "generated");
      const snapshots = (await readdir(join(migrationsDir, "snapshots"))).sort();
      expect(snapshots).toHaveLength(unchanged ? 1 : 2);
      expect(
        JSON.parse(await readFile(join(migrationsDir, "snapshots", snapshots[0]!), "utf8")),
      ).toEqual(baseline.schema);
      if (result.status === "generated") {
        expect(result.fromHash).toBe(baseline.hash);
        expect(await readFile(result.filePath, "utf8")).toContain(
          '"notes": m.add.string({ default: null }),',
        );
      }
      fetchMock.mockClear();
      expect(await createCatalogueMigration({ schemaDir: root, migrationsDir })).toEqual({
        status: "unchanged",
      });
      expect(fetchMock).not.toHaveBeenCalled();
    },
  );

  it("creates a migration from the latest committed snapshot and then no-ops when rerun", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const snapshotsDir = join(migrationsDir, "snapshots");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-08-25T06:00:00.000Z"));
    try {
      await saveBaseline({ schemaDir: root, migrationsDir });

      await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

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
      expect(generated).toContain('"notes": m.add.string({ default: null }),');
      const snapshotFiles = (await readdir(snapshotsDir))
        .filter((name) => name.endsWith(".json"))
        .sort();
      expect(snapshotFiles).toEqual([
        expect.stringMatching(/^20260825T060000-[0-9a-f]{12}\.json$/i),
        expect.stringMatching(/^20260825T060001-[0-9a-f]{12}\.json$/i),
      ]);
      expect(logs.some((line) => line.startsWith("Generated:"))).toBe(true);
      // An optional column's stub is complete: nothing to fill in.
      expect(logs).toContain("1. Review migrate: it is complete as generated, nothing to fill in.");

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
      expect(noopLogs).toContain("No schema changes detected.");
    } finally {
      vi.useRealTimers();
    }
  });

  it("skips creating a migration file when hashes differ but no row transforms are required", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const snapshotsDir = join(migrationsDir, "snapshots");
    const fromHash = "7070707070707070707070707070707070707070707070707070707070707070";
    const toHash = "7171717171717171717171717171717171717171717171717171717171717171";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
        return storedSchemaResponse({
          todos: {
            columns: [
              { name: "title", column_type: { type: "Text" }, nullable: false },
              { name: "done", column_type: { type: "Boolean" }, nullable: false },
            ],
          },
        });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
        return storedSchemaResponse(storedBooleanTodoSchemaWithDefaultFalse());
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const { result, logs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        fromHash: fromHash.slice(0, 12),
        toHash: toHash.slice(0, 12),
      }),
    );

    expect(result).toBeNull();
    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toHaveLength(0);
    expect((await readdir(snapshotsDir)).filter((name) => name.endsWith(".json"))).toHaveLength(2);
    expect(logs).toContain(
      "No reviewed migration file needed because this schema change does not require row transformations.",
    );
    expect(logs.some((line) => line.includes("createJazz publishes"))).toBe(true);
  });

  it("skips creating a migration file for reordered columns across explicit schema hashes", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "1010101010101010101010101010101010101010101010101010101010101010";
    const toHash = "2020202020202020202020202020202020202020202020202020202020202020";
    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
        return storedSchemaResponse(storedRootSchema());
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
        return storedSchemaResponse(storedRootSchemaWithReorderedColumns());
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const { result, logs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        fromHash: fromHash.slice(0, 12),
        toHash: toHash.slice(0, 12),
      }),
    );

    expect(result).toBeNull();
    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toHaveLength(0);
    expect(logs).toContain(
      "No reviewed migration file needed because this schema change does not require row transformations.",
    );
  });

  it("skips creating a migration file for merge-strategy-only schema changes", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "2121212121212121212121212121212121212121212121212121212121212121";
    const toHash = "3131313131313131313131313131313131313131313131313131313131313131";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${fromHash}`)) {
        return storedSchemaResponse({
          counters: {
            columns: [{ name: "value", column_type: { type: "Integer" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return storedSchemaResponse(storedCounterSchema());
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const { result, logs } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        fromHash: fromHash.slice(0, 12),
        toHash: toHash.slice(0, 12),
      }),
    );

    expect(result).toBeNull();
    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toHaveLength(0);
    expect(logs).toContain(
      "No reviewed migration file needed because this schema change does not require row transformations.",
    );
  });

  // The assertion intentionally launches `tsc`. On a contended CI worker the
  // compiler startup can exceed Vitest's normal 5s unit-test budget, while the
  // child process itself remains bounded above.
  it("renders a type-valid BIGINT counter migration", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "4141414141414141414141414141414141414141414141414141414141414141";
    const toHash = "5151515151515151515151515151515151515151515151515151515151515151";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith("/schemas")) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/schema/${fromHash}`)) {
        return storedSchemaResponse({
          counters: {
            columns: [
              {
                name: "value",
                column_type: { type: "BigInt" },
                nullable: false,
                merge_strategy: "Counter",
              },
              { name: "removedValue", column_type: { type: "BigInt" }, nullable: true },
              { name: "label", column_type: { type: "Text" }, nullable: false },
            ],
          },
        });
      }

      if (input.endsWith(`/schema/${toHash}`)) {
        return storedSchemaResponse({
          counters: {
            columns: [
              {
                name: "value",
                column_type: { type: "BigInt" },
                nullable: false,
                merge_strategy: "Counter",
              },
              // Another type than removedValue, so it can't be its rename.
              { name: "addedValue", column_type: { type: "Text" }, nullable: true },
              { name: "label", column_type: { type: "Text" }, nullable: false },
            ],
          },
        });
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const { result: filePath } = await captureConsoleLogs(() =>
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        fromHash: fromHash.slice(0, 12),
        toHash: toHash.slice(0, 12),
      }),
    );

    expect(filePath).not.toBeNull();
    if (!filePath) {
      throw new Error("Expected createMigration() to return a migration file path.");
    }

    const generated = await readFile(filePath, "utf8");
    expect(generated).toContain('"addedValue": m.add.string({ default: null }),');
    expect(generated).toContain('"removedValue": m.drop.bigint({ backwardsDefault: null }),');
    expect(generated).toContain('"value": s.bigint().merge("counter"),');
    await typecheckGeneratedMigration(filePath);
  }, 30_000);

  it("refuses a same-column nullability change instead of emitting a stub that cannot typecheck", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const fromHash = "3030303030303030303030303030303030303030303030303030303030303030";
    const toHash = "4040404040404040404040404040404040404040404040404040404040404040";

    const fetchMock = vi.fn(async (input: string) => {
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
        return storedSchemaResponse({
          todos: {
            columns: [{ name: "done", column_type: { type: "Boolean" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
        return storedSchemaResponse({
          todos: {
            columns: [{ name: "done", column_type: { type: "Boolean" }, nullable: true }],
          },
        });
      }

      throw new Error(`Unexpected fetch: ${input}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    // #2163: no migration step changes a kept column's optionality.
    await expect(
      createMigration({
        schemaDir: root,
        serverUrl: "http://localhost:1625",
        adminSecret: "admin-secret",
        migrationsDir,
        fromHash: fromHash.slice(0, 12),
        toHash: toHash.slice(0, 12),
      }),
    ).rejects.toThrow(
      'Changing column "todos.done" from required to optional is not supported yet. A migration can add, drop or rename a column but not convert its values',
    );
    expect(
      (await readdir(migrationsDir).catch(() => [])).filter((name) => name.endsWith(".ts")),
    ).toHaveLength(0);
  });

  it.each([
    {
      change: "the JSON Schema of",
      from: { type: "Json", schema: { type: "object" } },
      to: { type: "Json", schema: { type: "object", required: ["state"] } },
    },
    {
      change: "the enum variants of",
      from: { type: "Enum", variants: ["done", "todo"] },
      to: { type: "Enum", variants: ["done", "doing", "todo"] },
    },
  ])(
    "refuses changing $change a kept column, which no migration step can do",
    async ({ change, from, to }) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      const fromHash = "5050505050505050505050505050505050505050505050505050505050505050";
      const toHash = "6060606060606060606060606060606060606060606060606060606060606060";
      const fetchMock = vi.fn(async (input: string) => {
        if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
          return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
        }
        for (const [hash, columnType] of [
          [fromHash, from],
          [toHash, to],
        ] as const) {
          if (input.endsWith(`/apps/${APP_ID}/schema/${hash}`)) {
            return storedSchemaResponse({
              todos: { columns: [{ name: "state", column_type: columnType, nullable: false }] },
            });
          }
        }
        throw new Error(`Unexpected fetch: ${input}`);
      });
      vi.stubGlobal("fetch", fetchMock);

      await expect(
        createMigration({
          schemaDir: root,
          serverUrl: "http://localhost:1625",
          adminSecret: "admin-secret",
          migrationsDir,
          fromHash: fromHash.slice(0, 12),
          toHash: toHash.slice(0, 12),
        }),
      ).rejects.toThrow(
        `Changing ${change} column "todos.state" is not supported yet. A migration can add, drop or rename a column but not convert its values`,
      );
      expect(
        (await readdir(migrationsDir).catch(() => [])).filter((name) => name.endsWith(".ts")),
      ).toHaveLength(0);
    },
  );

  it("uses --name to generate a named migration file and skips the rename reminder", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await saveBaseline({ schemaDir: root, migrationsDir });

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
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
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
    expect(generated).toContain("m.defineMigration");
    expect(generated).toContain(`fromHash: "${fromShortHash}"`);
    expect(generated).toContain("migrate: {");
    expect(generated).toContain('"notes": m.add.string({ default: null }),');
    const snapshotFiles = (await readdir(snapshotsDir)).filter((name) => name.endsWith(".json"));
    expect(snapshotFiles).toHaveLength(2);
    expect(
      snapshotFiles.some(
        (name) => /^\d{8}T\d{6}-/.test(name) && name.endsWith(`-${fromShortHash}.json`),
      ),
    ).toBe(true);
    expect(logs).toContain("Migration stubs are only for schema changes.");
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
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
        return storedSchemaResponse({
          todos: {
            columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
          },
          legacy_users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
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
    expect(generated).toContain('"notes": m.add.string({ default: null }),');
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
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
        return storedSchemaResponse({
          users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
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
    expect(generated).toContain('people: m.renameTableFrom("users"),');
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
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
        return storedSchemaResponse({
          users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
          orgs: {
            columns: [{ name: "slug", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
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
    expect(generated).toContain('companies: m.renameTableFrom("orgs"),');
    expect(generated).toContain('people: m.renameTableFrom("users"),');
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
      if (input.endsWith(`/apps/${APP_ID}/schemas`)) {
        return new Response(JSON.stringify({ hashes: [fromHash, toHash] }), { status: 200 });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${fromHash}`)) {
        return storedSchemaResponse({
          archived_users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
          users: {
            columns: [{ name: "email", column_type: { type: "Text" }, nullable: false }],
          },
        });
      }

      if (input.endsWith(`/apps/${APP_ID}/schema/${toHash}`)) {
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
});
