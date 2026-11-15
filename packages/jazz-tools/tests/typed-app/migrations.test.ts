// The lenses Jazz's Rust loader compiles from a jazz.app.json migration
// (`from`, `renameTables`, `createTables`, `dropTables`, `migrate`), as the
// bindings ship them and a deploy publishes them.
import { describe, expect, it } from "vitest";
import { compileTestApp, type TestAppFile } from "../../src/testing/test-app.js";

/** The forward lens of the app's one migration, from its `v1` to its current tables. */
function forwardLens(
  from: Record<string, unknown>,
  to: Record<string, unknown>,
  migration: Record<string, unknown> = {},
) {
  const app: TestAppFile = {
    tables: to,
    history: { v1: { tables: from } },
    migrations: [{ from: "v1", ...migration }],
  };
  const migrations = compileTestApp(app).schema.migrations;
  expect(migrations).toHaveLength(1);
  return migrations[0]!.forward;
}

describe("migration lenses from jazz.app.json", () => {
  it("compiles add, drop, and rename operations from the migrate object", () => {
    const forward = forwardLens(
      {
        users: { columns: { email: "string", legacyPriority: "int?" } },
        todos: { columns: { title: "string", done: "boolean" } },
      },
      {
        users: {
          columns: { emailAddress: "string" },
          relations: { todosViaOwner: { reverse: "todos", relation: "owner" } },
        },
        todos: {
          columns: { title: "string", done: "boolean", description: "string?", ownerId: "uuid?" },
          relations: { owner: { rel: "users", column: "ownerId" } },
        },
      },
      {
        migrate: {
          users: {
            emailAddress: { renameFrom: "email" },
            legacyPriority: { drop: "int", backwardsDefault: null },
          },
          todos: {
            description: { add: "string", default: null },
            ownerId: { add: "uuid", default: null },
          },
        },
      },
    );

    expect(forward).toEqual([
      {
        table: "todos",
        added: false,
        removed: false,
        renamedFrom: null,
        operations: [
          {
            type: "introduce",
            column: "description",
            column_type: { type: "Text" },
            value: { type: "Null" },
          },
          {
            type: "introduce",
            column: "ownerId",
            column_type: { type: "Uuid" },
            value: { type: "Null" },
          },
        ],
      },
      {
        table: "users",
        added: false,
        removed: false,
        renamedFrom: null,
        operations: [
          { type: "rename", column: "email", value: "emailAddress" },
          {
            type: "drop",
            column: "legacyPriority",
            column_type: { type: "Integer" },
            value: { type: "Null" },
          },
        ],
      },
    ]);
  });

  it("compiles table renames", () => {
    const forward = forwardLens(
      { users: { columns: { email: "string" } } },
      { people: { columns: { emailAddress: "string", nickname: "string?" } } },
      {
        renameTables: { people: "users" },
        migrate: {
          people: {
            emailAddress: { renameFrom: "email" },
            nickname: { add: "string", default: null },
          },
        },
      },
    );

    expect(forward).toEqual([
      {
        table: "people",
        added: false,
        removed: false,
        renamedFrom: "users",
        operations: [
          { type: "rename", column: "email", value: "emailAddress" },
          {
            type: "introduce",
            column: "nickname",
            column_type: { type: "Text" },
            value: { type: "Null" },
          },
        ],
      },
    ]);
  });

  it("compiles table additions and removals", () => {
    const forward = forwardLens(
      {
        users: { columns: { email: "string" } },
        legacyProfiles: { columns: { bio: "string?" } },
      },
      {
        users: { columns: { email: "string" } },
        profiles: { columns: { bio: "string?" } },
      },
      { createTables: ["profiles"], dropTables: ["legacyProfiles"] },
    );

    expect(forward).toEqual([
      { table: "legacyProfiles", added: false, removed: true, renamedFrom: null, operations: [] },
      { table: "profiles", added: true, removed: false, renamedFrom: null, operations: [] },
    ]);
  });

  it("allows combining table renames with column migrations", () => {
    const forward = forwardLens(
      { users: { columns: { email: "string" } } },
      { people: { columns: { emailAddress: "string", age: "int" } } },
      {
        renameTables: { people: "users" },
        migrate: {
          people: {
            emailAddress: { renameFrom: "email" },
            age: { add: "int", default: 18 },
          },
        },
      },
    );

    expect(forward).toEqual([
      {
        table: "people",
        added: false,
        removed: false,
        renamedFrom: "users",
        operations: [
          { type: "rename", column: "email", value: "emailAddress" },
          {
            type: "introduce",
            column: "age",
            column_type: { type: "Integer" },
            value: { type: "Integer", value: 18 },
          },
        ],
      },
    ]);
  });

  it("cannot combine createTables/dropTables with column migrations", () => {
    expect(() =>
      forwardLens(
        { users: { columns: { email: "string" } } },
        { people: { columns: { emailAddress: "string" } } },
        {
          createTables: ["people"],
          dropTables: ["users"],
          migrate: { people: { emailAddress: { renameFrom: "email" } } },
        },
      ),
    ).toThrow(/table people cannot combine added\/removed markers with column operations/);
  });

  it("rejects explicit table renames that still do not match after applying column migrations", () => {
    expect(() =>
      forwardLens(
        { users: { columns: { email: "string" } } },
        { people: { columns: { emailAddress: "string", age: "int" } } },
        {
          renameTables: { people: "users" },
          migrate: { people: { emailAddress: { renameFrom: "email" } } },
        },
      ),
    ).toThrow(/the migration does not produce the columns of table "people" \("age" is missing\)/);
  });

  // #5056: an enum (or an array of enums) gaining variants at the end of its
  // list needs no step: the migration connects the versions with a lens of
  // no operations, also under a renamed table. Dropping a variant (or
  // renaming one), or moving one, is refused.
  it("connects a table whose enum columns gain variants with an empty lens", () => {
    const from = {
      tasks: {
        columns: { status: { enum: ["todo", "done"] }, labels: { array: { enum: ["bug"] } } },
      },
    };
    const to = {
      tasks: {
        columns: {
          status: { enum: ["todo", "done", "blocked"] },
          labels: { array: { enum: ["bug", "ui"] } },
        },
      },
    };
    // A table the migration leaves as it is gets no table lens at all.
    expect(forwardLens(from, to)).toEqual([]);
    expect(
      forwardLens({ legacyTasks: from.tasks }, to, { renameTables: { tasks: "legacyTasks" } }),
    ).toEqual([
      { table: "tasks", added: false, removed: false, renamedFrom: "legacyTasks", operations: [] },
    ]);

    const withColumns = (status: string[], labels: string[]) => ({
      tasks: { columns: { status: { enum: status }, labels: { array: { enum: labels } } } },
    });
    // A kept enum column cannot lose a variant.
    expect(() => forwardLens(from, withColumns(["todo"], ["bug"]))).toThrow(
      /"status" no longer declares enum variant "done"/,
    );
    // A new variant goes at the end of the list.
    expect(() => forwardLens(from, withColumns(["blocked", "todo", "done"], ["bug"]))).toThrow(
      /"status" reorders its enum variants/,
    );
    // Renaming a variant drops the old one.
    expect(() => forwardLens(from, withColumns(["todo", "done"], ["issue"]))).toThrow(
      /"labels" no longer declares enum variant "bug"/,
    );
  });
});
