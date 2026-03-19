import { describe, expect, expectTypeOf, it } from "vitest";
import { col } from "../../src/dsl.js";
import type { QueryBuilder, TableProxy } from "../../src/runtime/db.js";
import {
  defineApp,
  type DefinedSchema,
  type InsertOf,
  type Query,
  type RowOf,
  type Table,
  type TypedApp,
  type WhereOf,
} from "../../src/typed-app.js";

interface ProjectRecord {
  id: string;
  name: string;
}

interface TodoTitleRecord {
  id: string;
  title: string;
}

const schemaDef = {
  users: {
    name: col.string(),
  },
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    done: col.boolean(),
    tags: col.array(col.string()),
    project: col.ref("projects"),
    owner: col.ref("users").optional(),
  },
};
type AppSchema = DefinedSchema<typeof schemaDef>;
const app: TypedApp<AppSchema> = defineApp(schemaDef);

describe("typed app prototype", () => {
  it("serializes select/include metadata without codegen", () => {
    expect(JSON.parse(app.todos.select("title").include({ project: true })._build())).toEqual({
      table: "todos",
      conditions: [],
      includes: { project: true },
      select: ["title"],
      orderBy: [],
      hops: [],
    });
  });

  it("serializes nested include builders as query objects", () => {
    expect(
      JSON.parse(app.projects.include({ todosViaProject: app.todos.select("title") })._build()),
    ).toEqual({
      table: "projects",
      conditions: [],
      includes: {
        todosViaProject: {
          table: "todos",
          conditions: [],
          includes: {},
          select: ["title"],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
      hops: [],
    });
  });

  it("infers rows, init payloads, where inputs, and include names from schema literals", () => {
    const todoWithProjectQuery = app.todos.include({ project: true });
    const projectWithTitlesQuery = app.projects.include({
      todosViaProject: app.todos.select("title"),
    });

    type TodoRow = RowOf<typeof app.todos>;
    type TodoInsert = InsertOf<typeof app.todos>;
    type TodoWhere = WhereOf<typeof app.todos>;
    type TodoWithProject = RowOf<typeof todoWithProjectQuery>;
    type ProjectWithTitles = RowOf<typeof projectWithTitlesQuery>;
    const todoRow = {} as TodoRow;
    const todoInsert = {} as TodoInsert;
    const todoWithProject = {} as TodoWithProject;
    const projectWithTitles = {} as ProjectWithTitles;

    expectTypeOf(todoRow.id).toEqualTypeOf<string>();
    expectTypeOf(todoRow.title).toEqualTypeOf<string>();
    expectTypeOf(todoRow.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoRow.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoRow.project).toEqualTypeOf<string>();
    expectTypeOf(todoRow.owner).toEqualTypeOf<string | undefined>();

    expectTypeOf(todoInsert.title).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoInsert.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoInsert.project).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.owner).toEqualTypeOf<string | undefined>();

    expectTypeOf(undefined as TodoWhere["project"]).toEqualTypeOf<
      string | { eq?: string; ne?: string } | undefined
    >();
    expectTypeOf(undefined as TodoWhere["owner"]).toEqualTypeOf<
      string | { eq?: string; ne?: string; isNull?: boolean } | undefined
    >();
    expectTypeOf(undefined as TodoWhere["tags"]).toEqualTypeOf<
      string[] | { eq?: string[]; contains?: string } | undefined
    >();

    const projectRecord: ProjectRecord | undefined = todoWithProject.project;
    expectTypeOf(todoWithProject.owner).toEqualTypeOf<string | undefined>();
    const todoTitleRecords: TodoTitleRecord[] = projectWithTitles.todosViaProject;
    const queryContract: QueryBuilder<TodoWithProject> = todoWithProjectQuery;
    const typedQueryContract: Query<"todos", { project: true }, any, AppSchema> =
      todoWithProjectQuery;
    const tableProxyContract: TableProxy<TodoRow, TodoInsert> = app.todos;
    const tableContract: Table<"todos", AppSchema> = app.todos;

    void projectRecord;
    void todoTitleRecords;
    void queryContract;
    void typedQueryContract;
    void tableProxyContract;
    void tableContract;

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error invalid root key
      app.unknown;

      // @ts-expect-error invalid where column
      app.todos.where({ missing: true });

      // @ts-expect-error invalid select column
      app.todos.select("missing");

      // @ts-expect-error invalid include relation
      app.todos.include({ todosViaProject: true });

      // @ts-expect-error invalid reverse include on wrong table
      app.users.include({ todosViaProject: true });

      const invalidScalarRefSchema = {
        users: {
          name: col.string(),
        },
        todos: {
          owner: col.ref("accounts"),
        },
      };

      // @ts-expect-error invalid ref target table name
      defineApp(invalidScalarRefSchema);

      const invalidArrayRefSchema = {
        users: {
          name: col.string(),
        },
        groups: {
          members: col.array(col.ref("accounts")),
        },
      };

      // @ts-expect-error invalid ref target table name inside array ref
      defineApp(invalidArrayRefSchema);
    }
  });
});
