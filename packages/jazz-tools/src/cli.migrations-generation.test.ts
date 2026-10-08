import { readFile, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { assert, vi } from "vitest";
import {
  createMigration as createCatalogueMigration,
  compileSchema as saveBaseline,
} from "./dev/catalogue-project.js";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  APP_ID,
  createMigration,
  createWorkspace,
  fileExists,
  typecheckGeneratedMigration,
  captureConsoleLogs,
  rootSchemaWithoutInlinePermissions,
  rootSchemaWithTodoNotes,
  storedRootSchema,
  storedBooleanTodoSchemaWithDefaultFalse,
  storedRootSchemaWithReorderedColumns,
  storedCounterSchema,
  storedSchemaResponse,
} = createCliFixtures(import.meta.url);

describe("cli migrations", () => {
  it.each(["offline", "empty-server", "failed-server"])(
    "does not save an initial snapshot without a usable baseline (%s)",
    async (kind) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      const fetchMock = vi.fn(async () =>
        kind === "failed-server"
          ? new Response("unavailable", { status: 503 })
          : Response.json({ activeSchemaHash: null, schemas: [], migrations: [] }),
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
      if (kind === "offline") expect(fetchMock).not.toHaveBeenCalled();
    },
  );

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
            activeSchemaHash: baseline.hash,
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
    expect(logs.some((line) => line.includes("Run npx jazz-tools@"))).toBe(true);
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
              { name: "addedValue", column_type: { type: "BigInt" }, nullable: true },
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
    expect(generated).toContain('"addedValue": m.add.bigint({ default: null }),');
    expect(generated).toContain('"removedValue": m.drop.bigint({ backwardsDefault: null }),');
    expect(generated).toContain('"value": s.bigint().merge("counter"),');
    await typecheckGeneratedMigration(filePath);
  }, 30_000);

  it("still creates a migration file for nullability-only schema changes", async () => {
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

    expect(result).not.toBeNull();
    expect(logs.some((line) => line.startsWith("Generated:"))).toBe(true);
    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toHaveLength(1);
  });

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
