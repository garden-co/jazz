import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { createMigration, generate } from "./cli.js";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const { createWorkspace, captureConsoleLogs } = createCliFixtures(import.meta.url);

type Tables = Record<string, { columns: Record<string, unknown> }>;

const ROOT_TABLES: Tables = {
  projects: { columns: { name: "string" } },
  todos: { columns: { title: "string", ownerId: "string" } },
};

const withTodoNotes = (notes: unknown = "string?"): Tables => ({
  ...ROOT_TABLES,
  todos: { columns: { ...ROOT_TABLES.todos!.columns, notes } },
});

/**
 * A project whose `jazz.generated.ts` was generated from `tables`: the
 * version `migrations create` migrates from (what a snapshot was before).
 */
async function project(tables: Tables, { withBindings = true } = {}): Promise<string> {
  const { root } = await createWorkspace();
  await writeTables(root, tables);
  await writeFile(
    join(root, "jazz.permissions.json"),
    JSON.stringify({ format: 1, tables: {} }, null, 2),
  );
  if (withBindings) await captureConsoleLogs(() => generate({ schemaDir: root }));
  return root;
}

/** Edit the current tables, as an author edits `jazz.app.json`. */
async function writeTables(root: string, tables: Tables): Promise<void> {
  const path = join(root, "jazz.app.json");
  const app = await readFile(path, "utf8")
    .then((text) => JSON.parse(text) as Record<string, unknown>)
    .catch(() => ({ format: 1 }));
  await writeFile(path, JSON.stringify({ ...app, tables }, null, 2));
}

async function readApp(root: string): Promise<{
  tables: Tables;
  history?: Record<string, { tables: Tables }>;
  migrations?: Array<Record<string, unknown>>;
}> {
  return JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
}

/** The migration `migrations create` wrote last. */
async function lastMigration(root: string): Promise<Record<string, unknown>> {
  const migrations = (await readApp(root)).migrations ?? [];
  expect(migrations.length).toBeGreaterThan(0);
  return migrations.at(-1)!;
}

/** `migrations create`, which must record a migration. */
async function create(root: string, name = "change"): Promise<Record<string, unknown>> {
  const { result } = await captureConsoleLogs(() => createMigration({ schemaDir: root, name }));
  expect(result).toBe(join(root, "jazz.app.json"));
  return lastMigration(root);
}

describe("cli migrations", () => {
  // Before an app's first `generate` there are no bindings, so no earlier
  // version: nothing to migrate from, and nothing is written.
  it("does not record a migration without a baseline (no jazz.generated.ts yet)", async () => {
    const root = await project(ROOT_TABLES, { withBindings: false });
    const before = await readFile(join(root, "jazz.app.json"), "utf8");
    const { result } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, name: "first" }),
    );
    expect(result).toBeNull();
    expect(await readFile(join(root, "jazz.app.json"), "utf8")).toBe(before);
  });

  // An app on its own store has no server to fetch a baseline from: the
  // committed bindings are it, and the next schema change migrates from them.
  it("migrates from the version jazz.generated.ts was generated from, without a server", async () => {
    const root = await project(ROOT_TABLES);
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const baseline = await readFile(join(root, "jazz.generated.ts"), "utf8");

    await writeTables(root, withTodoNotes());
    // generate refuses the edit until a migration records it.
    await expect(generate({ schemaDir: root })).rejects.toThrow(/migrations create/);
    expect(await readFile(join(root, "jazz.generated.ts"), "utf8")).toBe(baseline);

    const { logs } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, name: "add-notes" }),
    );
    // A migration is for a schema change: its output says nothing about
    // permissions (#4773).
    expect(logs.join("\n")).not.toMatch(/Permission-only|Changes to permissions/);
    const app = await readApp(root);
    const [version] = Object.keys(app.history ?? {});
    expect(app.history![version!]!.tables).toEqual(ROOT_TABLES);
    expect(app.migrations).toEqual([
      {
        name: "add-notes",
        from: version,
        migrate: { todos: { notes: { add: "string", default: null } } },
      },
    ]);
    // With nothing to resolve, the bindings are regenerated too.
    await expect(generate({ schemaDir: root, check: true })).resolves.toBeUndefined();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("counts the steps a stub leaves to fill in", async () => {
    const root = await project(ROOT_TABLES);
    await writeTables(root, {
      ...ROOT_TABLES,
      todos: { columns: { ...ROOT_TABLES.todos!.columns, points: "int" } },
    });
    const { logs } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, name: "add-points" }),
    );
    // The file is named relative to the working directory.
    expect(logs.join("\n")).toMatch(
      /Resolve 1 TODO in \S*jazz\.app\.json, then run `jazz-tools generate`:/,
    );
    expect(logs).toContain("  migrations[0].migrate.todos.points.default");
    // A stub with a TODO is refused until it is resolved.
    await expect(generate({ schemaDir: root })).rejects.toThrow(/TODO/);
  });

  // A migration generate passed used to fail only at deploy (adopter b22):
  // renaming an optional column to a required one.
  it("refuses in generate a migration that a deploy would refuse, naming the column", async () => {
    const root = await project(withTodoNotes());
    const { notes: _notes, ...kept } = withTodoNotes().todos!.columns;
    await writeTables(root, {
      ...ROOT_TABLES,
      todos: { columns: { ...kept, body: { type: "string", default: "" } } },
    });
    const migration = await create(root, "notes-to-body");
    expect(migration.migrate).toEqual({
      todos: {
        body: { add: "string", default: "" },
        notes: { drop: "string", backwardsDefault: null },
      },
    });
    await captureConsoleLogs(() => generate({ schemaDir: root }));

    // The author keeps the values by renaming, but the column's optionality
    // changes with it, which no migration step does.
    const app = await readApp(root);
    app.migrations![0]!.migrate = { todos: { body: { renameFrom: "notes" } } };
    await writeFile(join(root, "jazz.app.json"), JSON.stringify(app, null, 2));
    await expect(generate({ schemaDir: root })).rejects.toThrow(
      /^jazz\.app\.json: migrations: invalid migration from [0-9a-f]{64} to [0-9a-f]{64}: invalid catalogue update: the migration does not produce the columns of table "todos" \("body" comes out optional but is required\); a migration can add, drop or rename a column, but a column it keeps or renames must keep its type and optionality$/,
    );
  });

  // #5056: an enum column (or an array of enums) may gain variants at the
  // end of its list, with nothing to fill in: its rows pass unchanged and
  // sort as before. A variant missing from the new list (removed, or
  // renamed, which reads the same) still has no migration step, and moving
  // one (a reorder, or an addition before the end) would change how rows
  // sort and rewrite every stored value, so it is refused too.
  it("adds enum variants with a stub that needs nothing filled in, and refuses dropping one", async () => {
    const tasks = (status: string[], labels: string[]): Tables => ({
      tasks: {
        columns: {
          title: "string",
          status: { enum: status },
          labels: { array: { enum: labels }, optional: true },
        },
      },
    });
    const root = await project(tasks(["todo", "done"], ["bug"]));

    await writeTables(root, tasks(["todo", "done", "blocked"], ["bug", "ui"]));
    const { logs } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, name: "add-blocked" }),
    );
    expect(logs.join("\n")).not.toContain("TODO");
    const added = await lastMigration(root);
    expect(added.migrate).toEqual({
      tasks: {
        $comment:
          '`status` gains enum variant "blocked": existing rows are unchanged, so it needs no step. ' +
          '`labels` gains enum variant "ui": existing rows are unchanged, so it needs no step.',
      },
    });
    // generate runs the checks a deploy runs on the migrations.
    await captureConsoleLogs(() => generate({ schemaDir: root }));

    // Refusals come from the diff, so they start from a project whose
    // bindings are the three-variant version and has no migration yet: a
    // migration to the current tables (add-blocked above) would make the
    // loader refuse the edited file before any diff runs.
    const base = await project(tasks(["todo", "done", "blocked"], ["bug", "ui"]));
    for (const [status, labels, column, variant] of [
      [["todo", "blocked"], ["bug", "ui"], "status", "done"],
      [["open", "done", "blocked"], ["bug", "ui"], "status", "todo"],
      [["todo", "done", "blocked"], ["ui"], "labels", "bug"],
    ] as const) {
      const edited = tasks([...status], [...labels]);
      await writeTables(base, edited);
      await expect(createMigration({ schemaDir: base, name: "drop-variant" })).rejects.toThrow(
        `Changing the enum variants of column \`tasks.${column}\` is not supported yet: ` +
          `it no longer declares "${variant}". A migration can add enum variants, but renaming ` +
          "or removing one is planned for a later release.",
      );
      expect((await readApp(base)).tables).toEqual(edited);
      expect((await readApp(base)).migrations).toBeUndefined();
    }
    for (const [status, labels, column] of [
      [["blocked", "todo", "done"], ["bug", "ui"], "status"],
      [["todo", "archived", "done", "blocked"], ["bug", "ui"], "status"],
      [["todo", "done", "blocked"], ["ui", "bug"], "labels"],
    ] as const) {
      await writeTables(base, tasks([...status], [...labels]));
      await expect(createMigration({ schemaDir: base, name: "drop-variant" })).rejects.toThrow(
        `Changing the order of the enum variants of column \`tasks.${column}\` is not supported: ` +
          "add new variants at the end of the list.",
      );
      expect((await readApp(base)).migrations).toBeUndefined();
    }
  }, 60_000);

  it("creates a migration from the committed bindings and then no-ops when rerun", async () => {
    const root = await project(ROOT_TABLES);
    await writeTables(root, withTodoNotes());

    const { result: filePath, logs } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, name: "add-notes" }),
    );
    expect(filePath).toBe(join(root, "jazz.app.json"));
    expect((await lastMigration(root)).migrate).toEqual({
      todos: { notes: { add: "string", default: null } },
    });
    // An optional column's stub is complete: nothing to fill in.
    expect(logs).toContain(
      "Review the migration, then commit jazz.app.json and jazz.generated.ts.",
    );
    expect(logs.join("\n")).not.toContain("TODO");

    const before = await readFile(join(root, "jazz.app.json"), "utf8");
    const { result: noop, logs: noopLogs } = await captureConsoleLogs(() =>
      createMigration({ schemaDir: root, name: "add-notes-again" }),
    );
    expect(noop).toBeNull();
    expect(await readFile(join(root, "jazz.app.json"), "utf8")).toBe(before);
    expect(noopLogs).toContain(
      "No schema changes since jazz.generated.ts was generated: no migration needed.",
    );
  });

  // A change that needs no row transformation (a default added, columns
  // reordered, a merge strategy changed) is still a new schema version, so it
  // gets a migration: the table's step is only a comment saying rows pass
  // unchanged, with nothing left to resolve.
  it.each([
    {
      change: "a default added",
      table: "todos",
      from: { todos: { columns: { title: "string", done: "boolean" } } },
      to: { todos: { columns: { title: "string", done: { type: "boolean", default: false } } } },
    },
    {
      change: "reordered columns",
      table: "todos",
      from: ROOT_TABLES,
      to: { ...ROOT_TABLES, todos: { columns: { ownerId: "string", title: "string" } } },
    },
    {
      change: "a merge-strategy-only change",
      table: "counters",
      from: { counters: { columns: { value: "int" } } },
      to: { counters: { columns: { value: { type: "int", merge: "counter" } } } },
    },
  ] as Array<{ change: string; table: string; from: Tables; to: Tables }>)(
    "records $change as a migration whose rows pass unchanged, with no TODO",
    async ({ table, from, to }) => {
      const root = await project(from);
      await writeTables(root, to);
      const { logs } = await captureConsoleLogs(() =>
        createMigration({ schemaDir: root, name: "no-transform" }),
      );
      expect(logs.join("\n")).not.toMatch(/TODO/);
      const app = await readApp(root);
      const [version] = Object.keys(app.history ?? {});
      expect(app.migrations).toEqual([
        {
          name: "no-transform",
          from: version,
          migrate: {
            [table]: {
              $comment: expect.stringMatching(/^Rows pass unchanged: only .+ changed\.$/),
            },
          },
        },
      ]);
      await expect(generate({ schemaDir: root, check: true })).resolves.toBeUndefined();
    },
  );

  it("writes a complete BIGINT counter migration", async () => {
    const root = await project({
      counters: {
        columns: {
          value: { type: "bigint", merge: "counter" },
          removedValue: "bigint?",
          label: "string",
        },
      },
    });
    await writeTables(root, {
      counters: {
        columns: {
          value: { type: "bigint", merge: "counter" },
          // Another type than removedValue, so it can't be its rename.
          addedValue: "string?",
          label: "string",
        },
      },
    });
    const migration = await create(root);
    expect(migration.migrate).toEqual({
      counters: {
        addedValue: { add: "string", default: null },
        removedValue: { drop: "bigint", backwardsDefault: null },
      },
    });
    // The counter column is kept as declared, and the file loads.
    expect((await readApp(root)).tables.counters!.columns.value).toEqual({
      type: "bigint",
      merge: "counter",
    });
    await expect(generate({ schemaDir: root, check: true })).resolves.toBeUndefined();
  });

  it("refuses a same-column nullability change instead of emitting a stub", async () => {
    const root = await project({ todos: { columns: { done: "boolean" } } });
    await writeTables(root, { todos: { columns: { done: "boolean?" } } });
    // #2163: no migration step changes a kept column's optionality.
    await expect(createMigration({ schemaDir: root, name: "optional-done" })).rejects.toThrow(
      "Changing column `todos.done` from required to optional is not supported yet. A migration can add, drop or rename a column but not convert its values",
    );
    expect((await readApp(root)).migrations).toBeUndefined();
  });

  it.each([
    {
      change: "the JSON Schema of",
      from: { json: { type: "object" } },
      to: { json: { type: "object", required: ["state"] } },
    },
    { change: "the type of", from: "string", to: "int" },
  ])(
    "refuses changing $change a kept column, which no migration step can do",
    async ({ change, from, to }) => {
      const root = await project({ todos: { columns: { state: from } } });
      await writeTables(root, { todos: { columns: { state: to } } });
      await expect(createMigration({ schemaDir: root, name: "state" })).rejects.toThrow(
        `Changing ${change} column \`todos.state\` is not supported yet. A migration can add, drop or rename a column but not convert its values`,
      );
      expect((await readApp(root)).migrations).toBeUndefined();
    },
  );

  it("records --name as the migration's name", async () => {
    const root = await project(ROOT_TABLES);
    await writeTables(root, withTodoNotes());
    const migration = await create(root, "add-todo-notes");
    expect(migration.name).toBe("add-todo-notes");
  });

  it("writes createTables and dropTables when inferring table add/drop steps", async () => {
    const root = await project({
      todos: { columns: { title: "string" } },
      legacy_users: { columns: { email: "string" } },
    });
    await writeTables(root, {
      todos: { columns: { title: "string", notes: "string?" } },
      users: { columns: { name: "string" } },
    });
    const migration = await create(root);
    expect(migration).toMatchObject({
      createTables: ["users"],
      dropTables: ["legacy_users"],
      migrate: { todos: { notes: { add: "string", default: null } } },
    });
  });

  it("suggests renameTables for a single exact table rename", async () => {
    const root = await project({ users: { columns: { email: "string" } } });
    await writeTables(root, { people: { columns: { email: "string" } } });
    const migration = await create(root);
    expect(migration.renameTables).toEqual({ people: "users" });
    expect(migration).not.toHaveProperty("migrate");
    expect(migration).not.toHaveProperty("createTables");
    expect(migration).not.toHaveProperty("dropTables");
    const app = await readApp(root);
    expect(app.history![migration.from as string]!.tables).toEqual({
      users: { columns: { email: "string" } },
    });
    expect(app.tables).toEqual({ people: { columns: { email: "string" } } });
  });

  it("suggests renameTables for multiple exact unambiguous table renames", async () => {
    const root = await project({
      users: { columns: { email: "string" } },
      orgs: { columns: { slug: "string" } },
    });
    await writeTables(root, {
      companies: { columns: { slug: "string" } },
      people: { columns: { email: "string" } },
    });
    const migration = await create(root);
    expect(migration.renameTables).toEqual({ companies: "orgs", people: "users" });
    expect(migration).not.toHaveProperty("createTables");
    expect(migration).not.toHaveProperty("dropTables");
    expect(migration).not.toHaveProperty("migrate");
  });

  it("keeps duplicate-shape table changes as add/drop instead of guessing a rename", async () => {
    const root = await project({
      archived_users: { columns: { email: "string" } },
      users: { columns: { email: "string" } },
    });
    await writeTables(root, { people: { columns: { email: "string" } } });
    const migration = await create(root);
    expect(migration).not.toHaveProperty("renameTables");
    expect(migration.createTables).toEqual(["people"]);
    expect(migration.dropTables).toEqual(["archived_users", "users"]);
  });
});
