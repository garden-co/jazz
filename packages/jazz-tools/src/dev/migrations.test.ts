import { mkdtemp, readFile, readdir, rm, writeFile, mkdir, copyFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { migration as m } from "../migration-namespace.js";

// Migration files import "jazz-tools"; the DSL is all they need (the package
// root also loads the native runtime).
vi.mock("jazz-tools", async () => ({
  ...(await import("../schema-namespace.js")),
  ...(await import("../migration-namespace.js")),
}));
import {
  createMigration,
  loadMigrationHistory,
  snapshotFromVersion,
  snapshotToVersion,
  type ModuleLoader,
} from "./migrations.js";
import { loadAppFiles } from "./codegen-napi.js";
import type { ReadableTable } from "../schema-dsl/readable-app.js";
import { readableSchema } from "../schema-dsl/define-schema.js";

const FIXTURE = join(import.meta.dirname, "fixtures", "migrations-history");
const MIGRATION_FILE = "20261104T192647-remove-better-auth-issuer-8acd8a85eef5-c95f3dba1806.ts";

const loadModule: ModuleLoader = (file) => import(/* @vite-ignore */ file);

const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

async function tempDir(): Promise<string> {
  // Inside the package, so the migration files resolve "jazz-tools" as the app's would.
  const base = join(import.meta.dirname, "..", "..", "node_modules", ".cache");
  await mkdir(base, { recursive: true });
  const dir = await mkdtemp(join(base, "jazz-migrations-"));
  dirs.push(dir);
  return dir;
}

async function fixtureApp(): Promise<{ tables: Record<string, ReadableTable>; text: string }> {
  const text = await readFile(join(FIXTURE, "jazz.app.json"), "utf8");
  return { tables: JSON.parse(text).tables, text };
}

async function fixtureMigrations(withFromSnapshot: boolean): Promise<string> {
  const dir = join(await tempDir(), "migrations");
  await mkdir(join(dir, "snapshots"), { recursive: true });
  await copyFile(join(FIXTURE, `${MIGRATION_FILE}.fixture`), join(dir, MIGRATION_FILE));
  if (withFromSnapshot)
    await copyFile(
      join(FIXTURE, "snapshots", "8acd8a85eef5.json"),
      join(dir, "snapshots", "8acd8a85eef5.json"),
    );
  return dir;
}

describe("snapshots", () => {
  it("round-trips a .64 snapshot through the readable form with the same hash", async () => {
    const snapshot = JSON.parse(
      await readFile(join(FIXTURE, "snapshots", "8acd8a85eef5.json"), "utf8"),
    );
    const version = snapshotToVersion(snapshot);
    const back = await snapshotFromVersion(version.tables);
    expect(back.hash.slice(0, 12)).toBe("8acd8a85eef5");
  });

  it("hashes the current tables as main's jazz.app.json does", async () => {
    const { tables } = await fixtureApp();
    const current = await snapshotFromVersion(tables);
    expect(current.hash.slice(0, 12)).toBe("c95f3dba1806");
    // Relations are kept on the snapshot, as .64 wrote them.
    expect(current.schema.better_auth_account?.relations).toEqual({
      user: { kind: "forward", table: "better_auth_user", column: "userId" },
    });
  });
});

describe("defineMigration", () => {
  it("loads a real .64 stub unchanged", async () => {
    const dir = await fixtureMigrations(false);
    const loaded = (await loadModule(join(dir, MIGRATION_FILE))).default as {
      fromHash: string;
      toHash: string;
      steps: unknown;
    };
    expect(loaded.fromHash).toBe("8acd8a85eef5");
    expect(loaded.toHash).toBe("c95f3dba1806");
    expect(loaded.steps).toEqual({
      migrate: { better_auth_account: { issuer: { drop: "string", backwardsDefault: "" } } },
    });
  });

  it("writes add, drop, rename and table steps in readable form", () => {
    const migration = m.defineMigration({
      migrate: {
        todos: {
          due: m.add.timestamp({ default: new Date(5) }),
          title: m.renameFrom("name"),
          legacy: m.drop.int({ backwardsDefault: 0 }),
        },
      },
      renameTables: { lists: m.renameTableFrom("projects") },
      createTables: { tags: true },
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        todos: s.table({ name: s.string(), legacy: s.int() }, {}),
        projects: s.table({ name: s.string() }, {}),
      },
      to: {
        todos: s.table({ title: s.string(), due: s.timestamp() }, {}),
        lists: s.table({ name: s.string() }, {}),
        tags: s.table({ label: s.string() }, {}),
      },
    });
    expect(migration.steps).toEqual({
      createTables: ["tags"],
      renameTables: { lists: "projects" },
      migrate: {
        todos: {
          due: { add: "timestamp", default: 5 },
          title: { renameFrom: "name" },
          legacy: { drop: "int", backwardsDefault: 0 },
        },
      },
    });
  });
});

describe("loadMigrationHistory", () => {
  for (const withFromSnapshot of [true, false]) {
    it(`gives main's history and migrations (${withFromSnapshot ? "from a snapshot" : "derived from the witness"})`, async () => {
      const { tables, text } = await fixtureApp();
      const migrationsDir = await fixtureMigrations(withFromSnapshot);
      const loaded = await loadMigrationHistory({
        migrationsDir,
        currentTables: tables,
        loadModule,
      });
      const main = JSON.parse(text);
      expect(loaded.migrations).toEqual(main.migrations);
      expect(Object.keys(loaded.history)).toEqual(Object.keys(main.history));
      // The same version: Rust loads the app and finds the same hashes.
      const app = JSON.stringify({ format: 1, tables, ...loaded });
      const files = (await loadAppFiles(
        JSON.stringify({
          format: 1,
          tables,
          history: loaded.history,
          migrations: loaded.migrations,
        }),
        '{"format":1,"tables":{}}',
      )) as { current: string };
      expect(files.current).toBe(loaded.current);
      void app;
      const mainLoaded = (await loadAppFiles(text, '{"format":1,"tables":{}}')) as {
        bundle: string;
      };
      const ours = (await loadAppFiles(
        JSON.stringify({
          format: 1,
          tables,
          history: loaded.history,
          migrations: loaded.migrations,
        }),
        '{"format":1,"tables":{}}',
      )) as { bundle: string };
      expect(ours.bundle).toBe(mainLoaded.bundle);
    });
  }

  it("has no history without migrations", async () => {
    const { tables } = await fixtureApp();
    const loaded = await loadMigrationHistory({
      migrationsDir: join(await tempDir(), "missing"),
      currentTables: tables,
      loadModule,
    });
    expect(loaded).toMatchObject({ history: {}, migrations: [] });
  });
});

describe("createMigration", () => {
  const v1 = s.defineSchema({
    todos: s.table({ title: s.string(), done: s.boolean() }, {}),
  });
  const v2 = s.defineSchema({
    todos: s.table({ title: s.string(), done: s.boolean(), note: s.string() }, {}),
  });
  const tablesOf = (schema: unknown) => readableSchema(schema)!.tables;

  it("snapshots first, then writes a stub that loads back into the history", async () => {
    const migrationsDir = join(await tempDir(), "migrations");
    const now = new Date(Date.UTC(2026, 10, 16, 4, 0, 0));
    const first = await createMigration({
      migrationsDir,
      currentTables: tablesOf(v1),
      loadModule,
      now,
    });
    expect(first).toMatchObject({ changed: true, initial: true });
    expect(
      await createMigration({ migrationsDir, currentTables: tablesOf(v1), loadModule, now }),
    ).toEqual({ changed: false });

    const second = await createMigration({
      migrationsDir,
      currentTables: tablesOf(v2),
      name: "Add note",
      loadModule,
      now,
    });
    expect(second).toMatchObject({ changed: true, migrationRequired: true });
    expect(second.file).toMatch(/^20261116T040001-add-note-[0-9a-f]{12}-[0-9a-f]{12}\.ts$/);
    expect(second.todos?.length).toBe(1);
    const stubPath = join(migrationsDir, second.file!);
    const stub = await readFile(stubPath, "utf8");
    // Fill in the TODO as an author would.
    await writeFile(
      stubPath,
      stub.replace(
        '// TODO: Added required column "note" needs an explicit default.',
        'note: m.add.string({ default: "" }),',
      ),
    );
    expect((await readdir(join(migrationsDir, "snapshots"))).length).toBe(2);

    const history = await loadMigrationHistory({
      migrationsDir,
      currentTables: tablesOf(v2),
      loadModule,
    });
    expect(history.migrations).toEqual([
      {
        from: second.fromHash!.slice(0, 12),
        migrate: { todos: { note: { add: "string", default: "" } } },
      },
    ]);
    await loadAppFiles(
      JSON.stringify({
        format: 1,
        tables: tablesOf(v2),
        history: history.history,
        migrations: history.migrations,
      }),
      '{"format":1,"tables":{}}',
    );
  });
});
