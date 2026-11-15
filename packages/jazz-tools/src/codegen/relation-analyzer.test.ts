/**
 * The runtime's own relation checks. `analyzeRelations` runs on every
 * schema a query, include or row transform resolves relations against, so
 * it refuses a malformed schema even when it did not come through the Rust
 * loader (a hand-built or deserialized `WasmSchema`).
 */
import { describe, expect, it } from "vitest";
import type { ColumnDescriptor, ColumnType, WasmSchema } from "../drivers/types.js";
import type { Relationship } from "../relationships.js";
import {
  AmbiguousRelationNameError,
  DuplicateColumnNameError,
  analyzeRelations,
} from "./relation-analyzer.js";

const uuid: ColumnType = { type: "Uuid" };

function column(
  name: string,
  column_type: ColumnType = uuid,
  extra: Partial<ColumnDescriptor> = {},
): ColumnDescriptor {
  return { name, column_type, nullable: false, ...extra };
}

/** users(name) and todos(title, owner → users) with `todos.owner` as a forward relation. */
function schema(
  todos: { columns?: ColumnDescriptor[]; relations?: Record<string, unknown> } = {},
  users: { relations?: Record<string, unknown> } = {},
): WasmSchema {
  return {
    users: {
      columns: [column("name", { type: "Text" })],
      relations: users.relations as Record<string, Relationship> | undefined,
    },
    todos: {
      columns: todos.columns ?? [
        column("title", { type: "Text" }),
        column("ownerId", uuid, { references: "users" }),
      ],
      relations: (todos.relations ?? {
        owner: { kind: "forward", table: "users", column: "ownerId" },
      }) as Record<string, Relationship>,
    },
  };
}

describe("analyzeRelations", () => {
  it("resolves a forward relation and its declared reverse", () => {
    const relations = analyzeRelations(
      schema({}, { relations: { todos: { kind: "reverse", table: "todos", relation: "owner" } } }),
    );
    expect(relations.get("todos")).toEqual([
      {
        name: "owner",
        type: "forward",
        fromTable: "todos",
        toTable: "users",
        fromColumn: "ownerId",
        toColumn: "id",
        isArray: false,
        nullable: false,
      },
    ]);
    expect(relations.get("users")).toEqual([
      {
        name: "todos",
        type: "reverse",
        fromTable: "users",
        toTable: "todos",
        fromColumn: "id",
        toColumn: "ownerId",
        isArray: true,
        nullable: false,
      },
    ]);
  });

  it.each(["title", "id", "$owner", "__proto__", "constructor", "prototype"])(
    "refuses a relation named %s, which collides with an output column",
    (name) => {
      const run = () =>
        analyzeRelations(
          schema({
            relations: JSON.parse(
              JSON.stringify({ [name]: { kind: "forward", table: "users", column: "ownerId" } }),
            ),
          }),
        );
      expect(run).toThrow(AmbiguousRelationNameError);
      expect(run).toThrow(
        `Relationship "todos.${name}" collides with a stored/public output column.`,
      );
    },
  );

  it("refuses duplicate column descriptors", () => {
    const run = () =>
      analyzeRelations(schema({ columns: [column("ownerId"), column("ownerId")], relations: {} }));
    expect(run).toThrow(DuplicateColumnNameError);
    expect(run).toThrow('Table "todos" has duplicate column descriptor "ownerId".');
  });

  it("refuses a relation to an unknown table", () => {
    expect(() =>
      analyzeRelations(
        schema({ relations: { owner: { kind: "forward", table: "accounts", column: "ownerId" } } }),
      ),
    ).toThrow('Relationship "todos.owner" references unknown table "accounts".');
  });

  it("refuses a relation to an inherited Object property name as a table", () => {
    expect(() =>
      analyzeRelations(
        schema({
          relations: { owner: { kind: "forward", table: "toString", column: "ownerId" } },
        }),
      ),
    ).toThrow('Relationship "todos.owner" references unknown table "toString".');
  });

  it("refuses a reverse relation that names no forward relation", () => {
    expect(() =>
      analyzeRelations(
        schema(
          {},
          { relations: { todos: { kind: "reverse", table: "todos", relation: "title" } } },
        ),
      ),
    ).toThrow(
      'Reverse relationship "users.todos" must reference a named forward relation on "todos".',
    );
  });

  it("refuses a reverse relation whose forward relation targets another table", () => {
    const base = schema();
    base.projects = {
      columns: [column("name", { type: "Text" })],
      relations: { todos: { kind: "reverse", table: "todos", relation: "owner" } },
    };
    expect(() => analyzeRelations(base)).toThrow(
      'Reverse relationship "projects.todos" references a forward relation targeting "users", not "projects".',
    );
  });

  it("refuses a relation over an unknown column", () => {
    expect(() =>
      analyzeRelations(
        schema({ relations: { owner: { kind: "forward", table: "users", column: "authorId" } } }),
      ),
    ).toThrow('Relationship "todos.owner" references unknown column "authorId".');
  });

  it.each<[string, ColumnType]>([
    ["TEXT", { type: "Text" }],
    ["TEXT[]", { type: "Array", element: { type: "Text" } }],
    ["UUID[][]", { type: "Array", element: { type: "Array", element: uuid } }],
  ])("refuses a relation over a %s column: it needs UUID or UUID[]", (_, columnType) => {
    expect(() =>
      analyzeRelations(
        schema({
          columns: [column("ownerId", columnType)],
          relations: { owner: { kind: "forward", table: "users", column: "ownerId" } },
        }),
      ),
    ).toThrow('Relationship "todos.owner" requires UUID or UUID[] column "ownerId".');
  });

  it("accepts a UUID[] column and makes the relation an array", () => {
    const relations = analyzeRelations(
      schema({
        columns: [column("ownerIds", { type: "Array", element: uuid }, { references: "users" })],
        relations: { owners: { kind: "forward", table: "users", column: "ownerIds" } },
      }),
    );
    expect(relations.get("todos")?.[0]).toMatchObject({ name: "owners", isArray: true });
  });

  it("refuses a relation that conflicts with the column's reference target", () => {
    const base = schema({
      columns: [column("ownerId", uuid, { references: "projects" })],
    });
    base.projects = { columns: [column("name", { type: "Text" })] };
    expect(() => analyzeRelations(base)).toThrow(
      'Relationship "todos.owner" conflicts with column reference target "projects".',
    );
  });

  it.each([
    ["not an object", "users"],
    ["an unknown kind", { kind: "sideways", table: "users", column: "ownerId" }],
    ["an empty table", { kind: "forward", table: "", column: "ownerId" }],
    ["a non-string column", { kind: "forward", table: "users", column: 1 }],
  ])("refuses a declaration that is %s", (_, declaration) => {
    expect(() => analyzeRelations(schema({ relations: { owner: declaration } }))).toThrow(
      /Invalid relationship "todos\.owner"/,
    );
  });
});
