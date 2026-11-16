/**
 * `jazz-tools migrations create` and `generate` on a TS app's history:
 * snapshots in migrations/snapshots, `m.defineMigration` stubs beside them
 * (the .64 model, restored for alpha.66).
 */
import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";
import { UNFILLED_STUB_TODO } from "./dev/migrations.js";
import { createAppMigration, generateApp } from "./dev/project.js";

const { createWorkspace } = createCliFixtures(import.meta.url);

/** A schema.ts with these tables (each `name: s.table(…)`), every table denying everything. */
async function writeSchema(dir: string, tables: Record<string, string>): Promise<void> {
  const lines = [
    'import { schema as s } from "jazz-tools";',
    "",
    "export const schema = s.defineSchema({",
    ...Object.entries(tables).map(([name, table]) => `  ${name}: ${table},`),
    "});",
    "",
    "export const permissions = schema.definePermissions({",
    ...Object.keys(tables).map((name) => `  ${name}: {},`),
    "});",
    "",
  ];
  await writeFile(join(dir, "schema.ts"), lines.join("\n"));
}

const TODOS = "s.table({ title: s.string(), done: s.boolean() }, {})";

async function stubs(dir: string): Promise<string[]> {
  return (await readdir(join(dir, "migrations"))).filter((name) => name.endsWith(".ts")).sort();
}

async function stubText(dir: string, file: string | undefined): Promise<string> {
  if (!file) throw new Error("no migration stub was written");
  return readFile(file, "utf8");
}

/** A project whose current schema is snapshotted (as its first `generate` does). */
async function baseline(tables: Record<string, string>): Promise<string> {
  const { root } = await createWorkspace();
  await writeSchema(root, tables);
  await generateApp({ cwd: root });
  expect((await readdir(join(root, "migrations", "snapshots"))).length).toBe(1);
  return root;
}

describe("migrations create", () => {
  it("snapshots the schema first, migrates from it next, and no-ops when rerun", async () => {
    const { root } = await createWorkspace();
    await writeSchema(root, { todos: TODOS });
    const first = await createAppMigration({ cwd: root, name: "initial" });
    expect(first.file).toBeUndefined();
    expect(first.snapshot).toMatch(/snapshots[\\/]\d{8}T\d{6}-[0-9a-f]{12}\.json$/);
    expect(await stubs(root)).toEqual([]);

    await writeSchema(root, {
      todos: "s.table({ title: s.string(), done: s.boolean(), notes: s.string().optional() }, {})",
    });
    const second = await createAppMigration({ cwd: root, name: "add-notes" });
    expect(second.changed).toBe(true);
    expect(second.file).toMatch(/add-notes/);
    const stub = await stubText(root, second.file);
    expect(stub).toContain('import { schema as s, migration as m } from "jazz-tools";');
    expect(stub).toContain("m.defineMigration({");
    expect(stub).toMatch(/"?notes"?: m\.add\.string\(\{ default: null \}\)/);
    expect(second.todos).toEqual([]);

    const again = await createAppMigration({ cwd: root, name: "add-notes-again" });
    expect(again.changed).toBe(false);
    expect(await stubs(root)).toHaveLength(1);

    await generateApp({ cwd: root });
    const app = JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
    expect(Object.keys(app.history)).toHaveLength(1);
    expect(app.migrations).toHaveLength(1);
    expect(app.migrations[0].migrate.todos.notes).toEqual({ add: "string", default: null });
  });

  it("counts the steps a stub leaves to fill in, and generate refuses it until they are", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, {
      todos: "s.table({ title: s.string(), done: s.boolean(), priority: s.int() }, {})",
    });
    const created = await createAppMigration({ cwd: root, name: "add-priority" });
    expect(created.todos).toHaveLength(1);
    const stub = await stubText(root, created.file);
    const todo = stub.split("\n").find((line) => UNFILLED_STUB_TODO.test(line));
    expect(todo).toMatch(/Added required column "priority" needs an explicit default/);

    await expect(generateApp({ cwd: root })).rejects.toThrow(
      /has 1 unresolved TODO left by `jazz-tools migrations create`: line \d+: Added required column "priority" needs an explicit default/,
    );

    const filled = stub
      .split("\n")
      .map((line) =>
        UNFILLED_STUB_TODO.test(line) ? '      "priority": m.add.int({ default: 0 }),' : line,
      )
      .join("\n");
    await writeFile(created.file!, filled);
    await generateApp({ cwd: root });
    const app = JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
    expect(app.migrations[0].migrate.todos.priority).toEqual({ add: "int", default: 0 });
  });

  it("refuses a same-column nullability change instead of writing a stub", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, {
      todos: "s.table({ title: s.string().optional(), done: s.boolean() }, {})",
    });
    await expect(createAppMigration({ cwd: root, name: "optional-title" })).rejects.toThrow(
      /title/,
    );
    expect(await stubs(root)).toEqual([]);
  });

  // Whether a stub is needed is Rust's `schemasAreCompatible`: the rule a
  // deployment connects versions by. Rows of an enum gaining variants pass
  // unchanged, but the deployment needs the (empty) migration to reach them.
  it("writes an empty stub to add enum variants, and refuses dropping one", async () => {
    const root = await baseline({
      todos: 's.table({ title: s.string(), status: s.enum("todo", "done") }, {})',
    });
    await writeSchema(root, {
      todos: 's.table({ title: s.string(), status: s.enum("todo", "done", "archived") }, {})',
    });
    const added = await createAppMigration({ cwd: root, name: "archived" });
    expect(added.file).toMatch(/archived/);
    expect(added.todos).toEqual([]);
    await generateApp({ cwd: root });
    const app = JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
    expect(app.migrations).toHaveLength(1);

    await writeSchema(root, {
      todos: 's.table({ title: s.string(), status: s.enum("todo", "archived") }, {})',
    });
    await expect(createAppMigration({ cwd: root, name: "drop-done" })).rejects.toThrow(/status/);
  });

  it("writes a stub when a table gains branchBy, not for a default outside it", async () => {
    const notes = (options: { branchBy: boolean; titleDefault: string }) =>
      `s.table({ title: s.string().default(${JSON.stringify(options.titleDefault)}), ` +
      `branch: s.string().default("main") }, {})` +
      (options.branchBy ? '.branchBy("branch")' : "");
    const root = await baseline({ notes: notes({ branchBy: false, titleDefault: "x" }) });

    await writeSchema(root, { notes: notes({ branchBy: false, titleDefault: "y" }) });
    const titleDefault = await createAppMigration({ cwd: root, name: "title-default" });
    expect(titleDefault.file).toBeUndefined();
    expect(titleDefault.snapshot).toBeDefined();

    await writeSchema(root, { notes: notes({ branchBy: true, titleDefault: "y" }) });
    const branched = await createAppMigration({ cwd: root, name: "branch-by" });
    expect(branched.file).toMatch(/branch-by/);
    expect(await stubs(root)).toHaveLength(1);
  });

  // Rust's diff (jazz-napi `migrationDiff`) decides the steps; TS only renders them.
  it("refuses moving a kept column's reference to another table", async () => {
    const tables = (target: string) => ({
      lists: "s.table({ name: s.string() }, {})",
      boards: "s.table({ name: s.string() }, {})",
      todos: `s.table({ title: s.string(), parent: s.uuid() }, { parentRel: s.rel(${JSON.stringify(target)}, "parent") })`,
    });
    const root = await baseline(tables("lists"));
    await writeSchema(root, tables("boards"));
    await expect(createAppMigration({ cwd: root, name: "move-parent" })).rejects.toThrow(
      /Moving the reference of column `todos.parent` from lists to boards is not supported yet/,
    );
    expect(await stubs(root)).toEqual([]);
  });

  it("says what changed when rows pass unchanged, and the stub loads", async () => {
    const root = await baseline({
      notes: 's.table({ title: s.string(), branch: s.string().default("main") }, {})',
    });
    await writeSchema(root, {
      notes:
        's.table({ title: s.string(), branch: s.string().default("main") }, {}).branchBy("branch")',
    });
    const created = await createAppMigration({ cwd: root, name: "branch-by" });
    const stub = await stubText(root, created.file);
    expect(stub).toContain("// Rows pass unchanged: only branchBy changed.");
    expect(created.todos).toEqual([]);
    await generateApp({ cwd: root });
    const app = JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
    expect(app.migrations).toHaveLength(1);
  });

  it("writes createTables and dropTables for added and removed tables", async () => {
    const root = await baseline({
      todos: TODOS,
      tags: "s.table({ label: s.string() }, {})",
    });
    await writeSchema(root, {
      todos: TODOS,
      projects: "s.table({ name: s.string(), archived: s.boolean() }, {})",
    });
    const created = await createAppMigration({ cwd: root, name: "projects-not-tags" });
    const stub = await stubText(root, created.file);
    expect(stub).toMatch(/createTables: \{\s*"projects": true,?\s*\}/);
    expect(stub).toMatch(/dropTables: \{\s*"tags": true,?\s*\}/);
    expect(stub).not.toContain("renameTables");
  });

  it("suggests renameTables for a single exact table rename", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, { tasks: TODOS });
    const created = await createAppMigration({ cwd: root, name: "rename" });
    const stub = await stubText(root, created.file);
    expect(stub).toMatch(/renameTables: \{\s*tasks: m\.renameTableFrom\("todos"\),?\s*\}/);
    expect(stub).not.toContain("createTables");
    expect(stub).not.toContain("dropTables");
  });

  it("suggests renameTables for several exact, unambiguous table renames", async () => {
    const root = await baseline({
      todos: TODOS,
      tags: "s.table({ label: s.string() }, {})",
    });
    await writeSchema(root, {
      tasks: TODOS,
      labels: "s.table({ label: s.string() }, {})",
    });
    const created = await createAppMigration({ cwd: root, name: "renames" });
    const stub = await stubText(root, created.file);
    expect(stub).toContain('tasks: m.renameTableFrom("todos")');
    expect(stub).toContain('labels: m.renameTableFrom("tags")');
  });

  it("keeps duplicate-shape table changes as add and drop instead of guessing a rename", async () => {
    const root = await baseline({
      first: "s.table({ label: s.string() }, {})",
      second: "s.table({ label: s.string() }, {})",
    });
    await writeSchema(root, {
      third: "s.table({ label: s.string() }, {})",
      fourth: "s.table({ label: s.string() }, {})",
    });
    const created = await createAppMigration({ cwd: root, name: "ambiguous" });
    const stub = await stubText(root, created.file);
    expect(stub).not.toContain("renameTableFrom");
    expect(stub).toMatch(/createTables:/);
    expect(stub).toMatch(/dropTables:/);
  });

  it("names the stub after --name and refuses a name it can't use", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, {
      todos: "s.table({ title: s.string(), done: s.boolean(), due: s.timestamp().optional() }, {})",
    });
    const created = await createAppMigration({ cwd: root, name: "add-due-date" });
    expect(created.file).toMatch(/add-due-date[^/\\]*\.ts$/);
    await expect(createAppMigration({ cwd: root, name: "" })).rejects.toThrow(/--name <name>/);
  });

  it("refuses changing a kept column's type", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, { todos: "s.table({ title: s.int(), done: s.boolean() }, {})" });
    await expect(createAppMigration({ cwd: root, name: "int-title" })).rejects.toThrow(/title/);
    expect(await stubs(root)).toEqual([]);
  });

  it("asks for a dropped required column's backwardsDefault", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, { todos: "s.table({ title: s.string() }, {})" });
    const created = await createAppMigration({ cwd: root, name: "drop-done" });
    expect(created.todos).toEqual([
      expect.stringMatching(/Removed required column "done" needs an explicit backwardsDefault/),
    ]);
  });

  it("writes a complete migration for a defaulted bigint counter column", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, {
      todos:
        's.table({ title: s.string(), done: s.boolean(), views: s.bigint().default(0n).merge("counter") }, {})',
    });
    const created = await createAppMigration({ cwd: root, name: "views" });
    expect(created.todos).toEqual([]);
    expect(await stubText(root, created.file)).toMatch(
      /"?views"?: m\.add\.bigint\(\{ default: 0n \}\)/,
    );
    await generateApp({ cwd: root });
    const app = JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
    expect(app.migrations[0].migrate.todos.views).toEqual({ add: "bigint", default: "0" });
  });

  it("refuses in generate a step a deploy would refuse, naming the column", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, {
      todos: "s.table({ title: s.string(), done: s.boolean(), notes: s.string().optional() }, {})",
    });
    const created = await createAppMigration({ cwd: root, name: "notes" });
    const stub = await stubText(root, created.file);
    // Hand-edited to add the wrong column: the lens no longer leads to the schema.
    await writeFile(created.file!, stub.replace(/"?notes"?: m\.add/, '"memo": m.add'));
    await expect(generateApp({ cwd: root })).rejects.toThrow(/notes/);
  });

  it("offers a same-shape column as a rename, which keeps its values once chosen", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, { todos: "s.table({ name: s.string(), done: s.boolean() }, {})" });
    const created = await createAppMigration({ cwd: root, name: "rename-title" });
    expect(created.todos).toEqual([
      expect.stringContaining('Added column "name" may be a rename of "title"'),
    ]);
    const stub = await stubText(root, created.file);
    expect(stub).toContain('m.renameFrom("title")');
    expect(stub).not.toMatch(/m\.(add|drop)\.string\(/);
    await expect(generateApp({ cwd: root })).rejects.toThrow(/may be a rename of "title"/);

    await writeFile(
      created.file!,
      stub
        .split("\n")
        .map((line) =>
          UNFILLED_STUB_TODO.test(line) ? '      "name": m.renameFrom("title"),' : line,
        )
        .join("\n"),
    );
    await generateApp({ cwd: root });
    const app = JSON.parse(await readFile(join(root, "jazz.app.json"), "utf8"));
    expect(app.migrations[0].migrate.todos).toEqual({ name: { renameFrom: "title" } });
  });

  it("doesn't offer a rename for a column of another shape", async () => {
    const root = await baseline({ todos: TODOS });
    await writeSchema(root, { todos: "s.table({ count: s.int(), done: s.boolean() }, {})" });
    const created = await createAppMigration({ cwd: root, name: "count-not-title" });
    const stub = await stubText(root, created.file);
    expect(stub).not.toContain("renameFrom");
    expect(created.todos.join("\n")).not.toContain("may be a rename");
  });
});
