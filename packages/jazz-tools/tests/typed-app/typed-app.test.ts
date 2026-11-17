import type { RowAuthor } from "../../src/magic-columns.js";
import { describe, expect, expectTypeOf, it } from "vitest";
import type {
  InsertOf,
  LargeValueUpdateOf,
  RowOf,
  StreamingInsertOf,
  StreamingUpdateOf,
  WhereOf,
} from "../../src/index.js";
import type { Db, QueryBuilder, TableProxy } from "../../src/runtime/db.js";
import { testApp } from "../../src/testing/test-app.js";
import type { Query, Table } from "../../src/typed-app.js";
import { app as chatApp } from "./apps/chat-indexes/jazz.generated.js";
import { app as indexedApp } from "./apps/composite-indexes/jazz.generated.js";
import { app as defaultedApp } from "./apps/defaulted-todos/jazz.generated.js";
import { app as historyApp } from "./apps/history-table/jazz.generated.js";
import { app as largeValueUpdateApp } from "./apps/large-values/jazz.generated.js";
import { app as graphApp } from "./apps/team-graph/jazz.generated.js";
import {
  app as transformedColumnApp,
  type TaskUrgency as Urgency,
} from "./apps/transformed-column/jazz.generated.js";
import {
  app,
  type Todo,
  type TodoInsert as GeneratedTodoInsert,
} from "./apps/typed-todos/jazz.generated.js";

interface ProjectRecord {
  id: string;
  name: string;
}

interface TodoTitleRecord {
  id: string;
  title: string;
}

// The generated module keeps its `Types` map private; read it back off a table.
type AppSchema = typeof app.todos extends Table<"todos", infer TSchema> ? TSchema : never;

describe("typed app prototype", () => {
  it("exposes history queries without reserving the table name history", () => {
    expect(historyApp.history._table).toBe("history");
    expect(historyApp.$history.history._table).toBe("history.history");
    expect(historyApp.$history.history._schema).toBe(historyApp.wasmSchema);
    expect(Object.keys(historyApp.wasmSchema)).toEqual(["history"]);
    expect(Object.keys(historyApp.$history)).toEqual(["history"]);
  });

  it("reserves $history for the history namespace", () => {
    expect(() => testApp({ tables: { $history: { columns: { title: "string" } } } })).toThrow(
      /tables\.\$history: `\$history` is not a name/,
    );
  });

  it.each(["union", "wasmSchema", "schemaAst", "_schema", "exists", "constructor", "toString"])(
    "rejects a table named %s instead of masking an app control",
    (tableName) => {
      expect(() => testApp({ tables: { [tableName]: { columns: { value: "string" } } } })).toThrow(
        `Schema name "${tableName}" is reserved for Jazz schema controls.`,
      );
    },
  );

  it("allows a table-inferred variable to be reassigned to a refined query", () => {
    let query = app.todos;

    query = query.where({ done: true });

    expect(JSON.parse(query._build()).conditions).toEqual([
      { column: "done", op: "eq", value: true },
    ]);
  });

  it("serializes select/include metadata without codegen", () => {
    expect(
      JSON.parse(app.todos.select("title").include({ projectRelation: true })._build()),
    ).toEqual({
      table: "todos",
      conditions: [],
      includes: { projectRelation: true },
      select: ["title"],
      orderBy: [],
      hops: [],
    });
  });

  it("serializes partial large-value select descriptors", () => {
    const query = app.todos.select({
      attachment: { from: 1_000_000, to: 2_000_000 },
      title: { fromUtf8: 4, toUtf8: 67 },
    });

    expect(JSON.parse(query._build())).toMatchObject({
      table: "todos",
      select: {
        attachment: { from: 1_000_000, to: 2_000_000 },
        title: { fromUtf8: 4, toUtf8: 67 },
      },
    });
    expectTypeOf(query).toMatchTypeOf<
      QueryBuilder<{ id: string; attachment: Uint8Array; title: string }>
    >();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // The object form is a schema-derived partial projection, rather than a
      // generic descriptor bag. In particular, JSON pointers cannot leak onto
      // an arbitrary scalar just because its JS representation is primitive.
      // @ts-expect-error BOOLEAN columns do not support partial projections.
      app.todos.select({ done: { at: "/" } });
      // @ts-expect-error bytes use byte ranges, not text UTF-8 coordinates.
      app.todos.select({ attachment: { fromUtf8: 0, toUtf8: 1 } });
      // @ts-expect-error TEXT cannot use the JSON-pointer projection form.
      app.todos.select({ title: { at: "/" } });
    }
  });

  it("mixes column names and partial selections in any order (#5192)", () => {
    const rangeFirst = app.todos.select({ title: { from: 0, to: 10 } }, "done");
    const namesFirst = app.todos.select("done", { title: { from: 0, to: 10 } });
    const interleaved = app.todos.select({ attachment: { from: 1, to: 3 } }, "done", {
      title: { fromUtf8: 0, toUtf8: 4 },
    });

    expect(JSON.parse(rangeFirst._build())).toMatchObject({
      select: ["title", "done"],
      partialSelect: { title: { from: 0, to: 10 } },
    });
    expect(JSON.parse(namesFirst._build())).toMatchObject({
      select: ["done", "title"],
      partialSelect: { title: { from: 0, to: 10 } },
    });
    expect(JSON.parse(interleaved._build())).toMatchObject({
      select: ["attachment", "done", "title"],
      partialSelect: { attachment: { from: 1, to: 3 }, title: { fromUtf8: 0, toUtf8: 4 } },
    });
    expectTypeOf<RowOf<typeof rangeFirst>>().toEqualTypeOf<{
      id: string;
      title: string;
      done: boolean;
    }>();
    expectTypeOf<RowOf<typeof namesFirst>>().toEqualTypeOf<{
      id: string;
      title: string;
      done: boolean;
    }>();
    expectTypeOf<RowOf<typeof interleaved>>().toEqualTypeOf<{
      id: string;
      attachment: Uint8Array;
      title: string;
      done: boolean;
    }>();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error a partial selection after a name is still schema-checked.
      app.todos.select("done", { done: { from: 0, to: 1 } });
      // @ts-expect-error a name after a partial selection is still checked.
      app.todos.select({ title: { from: 0, to: 1 } }, "missing");
    }
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

  it("serializes provenance magic columns and infers their projected types", () => {
    const author: RowAuthor = {
      account: "00000000-0000-0000-0000-000000000001",
      identity: { issuer: "https://issuer.example", subject: "alice" },
    };
    const provenanceQuery = app.todos
      .where({ $createdBy: author })
      .select("title", "$createdBy", "$updatedAt");

    expect(JSON.parse(provenanceQuery._build())).toEqual({
      table: "todos",
      conditions: [{ column: "$createdBy", op: "eq", value: author }],
      includes: {},
      select: ["title", "$createdBy", "$updatedAt"],
      orderBy: [],
      hops: [],
    });

    type ProvenanceRow = RowOf<typeof provenanceQuery>;
    const row = {} as ProvenanceRow;

    expectTypeOf(row.title).toEqualTypeOf<string>();
    expectTypeOf(row.$createdBy).toEqualTypeOf<RowAuthor>();
    expectTypeOf(row.$updatedAt).toEqualTypeOf<Date>();
  });

  it("does not expose permission introspection columns through typed queries", () => {
    // @ts-expect-error Permission introspection columns are not selectable query columns.
    app.todos.select("$canRead");
    // @ts-expect-error Permission introspection columns are not filterable query columns.
    app.todos.where({ $canRead: true });
    // @ts-expect-error Permission introspection columns are not orderable query columns.
    app.todos.orderBy("$canRead");

    app.todos
      .select("$createdAt")
      .where({ $updatedAt: { lte: new Date() } })
      .orderBy("$createdBy");
  });

  it("emits indexOnly metadata into the runtime schema", () => {
    expect(app.wasmSchema.todos?.indexed_columns).toEqual(["done"]);
    expect(app.wasmSchema.users?.indexed_columns).toBeUndefined();
  });

  it("emits ordered composite indexes through jazz.app.json", () => {
    expect(indexedApp.wasmSchema.documents?.indexed_columns).toEqual(["ownerId"]);
    expect(indexedApp.wasmSchema.documents?.composite_indexes).toEqual([["ownerId", "updatedAt"]]);
  });

  it("lets a composite index use the provenance timestamps", () => {
    expect(chatApp.wasmSchema.messages?.composite_indexes).toEqual([
      ["roomId", "$createdAt"],
      ["roomId", "$updatedAt"],
    ]);
  });

  it("rejects composite index columns that are neither declared nor provenance timestamps", () => {
    const withIndex = (index: string[]) => ({
      tables: { messages: { columns: { roomId: "uuid", text: "string" }, indexes: [index] } },
    });
    // `$createdBy` is metadata, not an indexable timestamp.
    expect(() => testApp(withIndex(["roomId", "$createdBy"]))).toThrow(
      /indexes\.composite\[0\]: table messages has no column `\$createdBy`/,
    );
    // Not a column of this table.
    expect(() => testApp(withIndex(["roomId", "sentAt"]))).toThrow(
      /indexes\.composite\[0\]: table messages has no column `sentAt`/,
    );
    expect(() => testApp(withIndex(["$createdAt", "$createdAt"]))).toThrow(
      /distinct declared columns or provenance timestamps/,
    );
  });

  it("serializes gather seeded from the current relation", () => {
    const directParents = graphApp.team_edges
      .where({ child_team: "team-a" })
      .hopTo("parent_teamRelation");
    const reachableTeams = directParents.gather({
      step: ({ current }) =>
        graphApp.team_edges.where({ child_team: current }).hopTo("parent_teamRelation"),
      maxDepth: 0,
    });

    expect(JSON.parse(reachableTeams._build())).toEqual({
      table: "team_edges",
      conditions: [],
      includes: {},
      orderBy: [],
      hops: [],
      gather: {
        seed: {
          table: "team_edges",
          conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
          hops: ["parent_teamRelation"],
        },
        max_depth: 0,
        step_table: "team_edges",
        step_current_column: "child_team",
        step_conditions: [],
        step_hops: ["parent_teamRelation"],
      },
    });
  });

  it("serializes union gather seeds", () => {
    const directParents = graphApp.team_edges
      .where({ child_team: "team-a" })
      .hopTo("parent_teamRelation");
    const adminReachableTeams = graphApp.teams.gather({
      start: { name: "admins" },
      step: ({ current }) =>
        graphApp.team_edges.where({ child_team: current }).hopTo("parent_teamRelation"),
      maxDepth: 2,
    });
    const reachableTeams = graphApp.union([directParents, adminReachableTeams]).gather({
      step: ({ current }) =>
        graphApp.team_edges.where({ child_team: current }).hopTo("parent_teamRelation"),
      maxDepth: 4,
    });

    expect(JSON.parse(reachableTeams._build())).toEqual({
      table: "team_edges",
      conditions: [],
      includes: {},
      orderBy: [],
      hops: [],
      gather: {
        seed: {
          union: {
            inputs: [
              {
                label: "",
                input: {
                  table: "team_edges",
                  conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
                  hops: ["parent_teamRelation"],
                },
              },
              {
                label: "",
                input: {
                  table: "teams",
                  conditions: [],
                  hops: [],
                  gather: {
                    seed: {
                      table: "teams",
                      conditions: [{ column: "name", op: "eq", value: "admins" }],
                      hops: [],
                    },
                    max_depth: 2,
                    step_table: "team_edges",
                    step_current_column: "child_team",
                    step_conditions: [],
                    step_hops: ["parent_teamRelation"],
                  },
                },
              },
            ],
          },
        },
        max_depth: 4,
        step_table: "team_edges",
        step_current_column: "child_team",
        step_conditions: [],
        step_hops: ["parent_teamRelation"],
      },
    });
  });

  it("infers rows, init payloads, where inputs, and include names from schema literals", () => {
    const todoWithProjectQuery = app.todos.include({ projectRelation: true });
    const projectWithTitlesQuery = app.projects.include({
      todosViaProject: app.todos.select("title"),
    });

    type TodoRow = RowOf<typeof app.todos>;
    type TodoInsert = InsertOf<typeof app.todos>;
    type TodoStreamingInsert = StreamingInsertOf<typeof app.todos>;
    type TodoStreamingUpdate = StreamingUpdateOf<typeof app.todos>;
    type TodoWhere = WhereOf<typeof app.todos>;
    type TodoWithProject = RowOf<typeof todoWithProjectQuery>;
    type ProjectWithTitles = RowOf<typeof projectWithTitlesQuery>;
    const todoRow = {} as TodoRow;
    const todoInsert = {} as TodoInsert;
    const streamedTitle = {
      title: new ReadableStream<string>(),
      done: false,
      tags: [],
      attachment: new Uint8Array(),
      project: "project-id",
    } satisfies TodoStreamingInsert;
    const streamedAttachment = {
      title: "todo",
      done: false,
      tags: [],
      attachment: new ReadableStream<Uint8Array>(),
      project: "project-id",
    } satisfies TodoStreamingInsert;
    const streamedTitleUpdate = {
      title: new ReadableStream<string>(),
    } satisfies TodoStreamingUpdate;
    const todoWithProject = {} as TodoWithProject;
    const projectWithTitles = {} as ProjectWithTitles;

    // The table's row and insert types are the generated interfaces.
    expectTypeOf<TodoRow>().toMatchTypeOf<Todo>();
    expectTypeOf<Todo>().toMatchTypeOf<TodoRow>();
    expectTypeOf<TodoInsert>().toEqualTypeOf<GeneratedTodoInsert>();
    expectTypeOf(todoRow.id).toEqualTypeOf<string>();
    expectTypeOf(todoRow.title).toEqualTypeOf<string>();
    expectTypeOf(todoRow.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoRow.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoRow.attachment).toEqualTypeOf<Uint8Array>();
    expectTypeOf(todoRow.project).toEqualTypeOf<string>();
    expectTypeOf(todoRow.owner).toEqualTypeOf<string | null>();

    expectTypeOf(todoInsert.title).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.done).toEqualTypeOf<boolean>();
    expectTypeOf(todoInsert.tags).toEqualTypeOf<string[]>();
    expectTypeOf(todoInsert.attachment).toEqualTypeOf<Uint8Array>();
    expectTypeOf(todoInsert.project).toEqualTypeOf<string>();
    expectTypeOf(todoInsert.owner).toEqualTypeOf<string | null | undefined>();
    expectTypeOf(streamedTitle.title).toEqualTypeOf<ReadableStream<string>>();
    expectTypeOf(streamedAttachment.attachment).toEqualTypeOf<ReadableStream<Uint8Array>>();
    expectTypeOf(streamedTitleUpdate.title).toEqualTypeOf<ReadableStream<string>>();

    expectTypeOf<TodoWhere["project"]>().branded.toEqualTypeOf<
      string | { eq?: string; ne?: string; in?: string[]; notIn?: string[] } | undefined
    >();
    expectTypeOf<TodoWhere["owner"]>().branded.toEqualTypeOf<
      | string
      | null
      | {
          eq?: string | null;
          ne?: string | null;
          in?: string[];
          notIn?: string[];
          isNull?: boolean;
        }
      | undefined
    >();
    expectTypeOf<TodoWhere["tags"]>().branded.toEqualTypeOf<
      | string[]
      | { eq?: string[]; ne?: string[]; contains?: string; in?: string[][]; notIn?: string[][] }
      | undefined
    >();
    expectTypeOf<TodoWhere["attachment"]>().branded.toEqualTypeOf<
      | Uint8Array
      | {
          eq?: Uint8Array;
          ne?: Uint8Array;
          in?: (Uint8Array | number[])[];
          notIn?: (Uint8Array | number[])[];
        }
      | undefined
    >();

    app.todos.where({
      optionalTitle: { isNull: true },
      optionalRank: { isNull: false },
      optionalTags: { isNull: true },
      optionalAttachment: { isNull: false },
      optionalStatus: { isNull: true },
    });
    // @ts-expect-error required scalar columns do not support isNull
    app.todos.where({ done: { isNull: true } });

    // Membership is deliberately non-nullable. Express null handling with
    // isNull/isNotNull rather than SQL-style null membership semantics.
    // @ts-expect-error null is not a valid membership value
    app.todos.where({ owner: { notIn: [null] } });

    const projectRecord: ProjectRecord | null = todoWithProject.projectRelation;
    expectTypeOf(todoWithProject.owner).toEqualTypeOf<string | null>();
    const todoTitleRecords: TodoTitleRecord[] = projectWithTitles.todosViaProject;
    const queryContract: QueryBuilder<TodoWithProject> = todoWithProjectQuery;
    const typedQueryContract: Query<"todos", { projectRelation: true }, any, AppSchema> =
      todoWithProjectQuery;
    const tableProxyContract: TableProxy<TodoRow, TodoInsert> = app.todos;
    const tableContract: Table<"todos", AppSchema> = app.todos;

    void projectRecord;
    void todoTitleRecords;
    void streamedTitle;
    void streamedAttachment;
    void streamedTitleUpdate;
    void queryContract;
    void typedQueryContract;
    void tableProxyContract;
    void tableContract;

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error invalid root key
      void app.unknown;
      const invalidStreamedReference: TodoStreamingInsert = {
        title: "todo",
        done: false,
        tags: [],
        attachment: new Uint8Array(),
        // @ts-expect-error UUID references are not streamable despite being strings in TypeScript.
        project: new ReadableStream<string>(),
      };
      void invalidStreamedReference;

      // @ts-expect-error invalid where column
      app.todos.where({ missing: true });

      // @ts-expect-error invalid select column
      app.todos.select("missing");

      // @ts-expect-error invalid include relation
      app.todos.include({ todosViaProject: true });

      // @ts-expect-error invalid reverse include on wrong table
      app.users.include({ todosViaProject: true });
    }
  });

  it("rejects relations to an unknown target table", () => {
    // Was a `@ts-expect-error` on `defineApp`; jazz.app.json is checked when it loads.
    expect(() =>
      testApp({
        tables: {
          users: { columns: { name: "string" } },
          todos: {
            columns: { owner: "uuid" },
            relations: { ownerRelation: { rel: "accounts", column: "owner" } },
          },
        },
      }),
    ).toThrow(
      /^jazz\.app\.json: migrations: invalid schema [0-9a-f]{64}: \$\.todos\.owner: reference target table 'accounts' does not exist$/,
    );
    // The same inside an array reference.
    expect(() =>
      testApp({
        tables: {
          users: { columns: { name: "string" } },
          groups: {
            columns: { members: { array: "uuid" } },
            relations: { membersRelation: { rel: "accounts", column: "members" } },
          },
        },
      }),
    ).toThrow(
      /^jazz\.app\.json: migrations: invalid schema [0-9a-f]{64}: \$\.groups\.members: reference target table 'accounts' does not exist$/,
    );
  });

  it("infers fields with defaults as optional for init payloads", () => {
    type TodoInsert = InsertOf<typeof defaultedApp.todos>;
    const minimalInsert: TodoInsert = {
      title: "Ship defaults",
      projectId: "00000000-0000-0000-0000-000000000001",
    };
    const explicitOptionalValues: TodoInsert = {
      title: "Ship defaults",
      projectId: "00000000-0000-0000-0000-000000000001",
      ownerId: null,
      assigneesIds: ["00000000-0000-0000-0000-000000000002"],
    };

    expectTypeOf(minimalInsert.title).toEqualTypeOf<string>();
    expectTypeOf(minimalInsert.projectId).toEqualTypeOf<string>();
    expectTypeOf(minimalInsert.done).toEqualTypeOf<boolean | undefined>();
    expectTypeOf(minimalInsert.tags).toEqualTypeOf<string[] | undefined>();
    expectTypeOf(explicitOptionalValues.ownerId).toEqualTypeOf<string | null | undefined>();
    expectTypeOf(explicitOptionalValues.assigneesIds).toEqualTypeOf<string[] | undefined>();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      const invalidDefaultedNull: TodoInsert = {
        title: "Broken",
        projectId: "00000000-0000-0000-0000-000000000001",
        // @ts-expect-error non-nullable defaulted columns still reject null
        done: null,
      };
      void invalidDefaultedNull;
    }
  });

  it("infers update payloads with column-specific large-value descriptors", () => {
    type DocumentUpdate = LargeValueUpdateOf<typeof largeValueUpdateApp.documents>;
    const update = {
      title: {
        within: { from: 0, to: 4 },
        splices: [{ at: 1, delete: 2, insert: "ee" }],
      },
      payload: {
        within: { from: 0, to: 3 },
        splices: [{ at: 1, delete: 1, insert: new Uint8Array([9]) }],
      },
      metadata: {
        edits: [{ op: "set", at: "/selected/answer", value: 43 }],
      },
    } satisfies DocumentUpdate;

    const utf8TextUpdate = {
      title: {
        within: { fromUtf8: 0, toUtf8: 4 },
        splices: [{ atUtf8: 0, deleteUtf8: 4, insert: "text" }],
      },
    } satisfies DocumentUpdate;

    void update;
    void utf8TextUpdate;

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // Whole-column replacements and column-specific diffs share Db.update.
      const db = null as unknown as Db;
      db.update(
        largeValueUpdateApp.documents,
        "00000000-0000-0000-0000-000000000001",
        {
          done: true,
        },
        {
          applyDiffs: {
            title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
          },
        },
      );
      // @ts-expect-error Db no longer exposes a separate applyDiffs method
      db.applyDiffs(largeValueUpdateApp.documents, "00000000-0000-0000-0000-000000000001", {});
      // @ts-expect-error a column cannot be both replaced and diffed
      db.update(
        largeValueUpdateApp.documents,
        "00000000-0000-0000-0000-000000000001",
        { title: "replacement" },
        {
          applyDiffs: {
            title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
          },
        },
      );
      // Transactions take the same applyDiffs option (#2087).
      void db.transaction((tx) => {
        tx.update(
          largeValueUpdateApp.documents,
          "00000000-0000-0000-0000-000000000001",
          { done: true },
          {
            applyDiffs: {
              title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
            },
          },
        );
        // @ts-expect-error a column cannot be both replaced and diffed in a transaction either
        tx.update(
          largeValueUpdateApp.documents,
          "00000000-0000-0000-0000-000000000001",
          { title: "replacement" },
          {
            applyDiffs: {
              title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
            },
          },
        );
      });
      db.upsert(largeValueUpdateApp.documents, "00000000-0000-0000-0000-000000000001", {
        // @ts-expect-error partial descriptors belong exclusively to update's applyDiffs option
        title: { within: { from: 0, to: 1 }, splices: [{ at: 0, delete: 0, insert: "x" }] },
      });

      const byteUpdateWithText = {
        payload: {
          within: { from: 0, to: 1 },
          splices: [
            {
              at: 0,
              delete: 0,
              // @ts-expect-error byte splice inserts must be Uint8Array
              insert: "x",
            },
          ],
        },
      } satisfies DocumentUpdate;
      void byteUpdateWithText;

      const textUpdateWithBytes = {
        title: {
          within: { fromUtf8: 0, toUtf8: 1 },
          splices: [
            {
              atUtf8: 0,
              deleteUtf8: 0,
              // @ts-expect-error text splice inserts must be strings
              insert: new Uint8Array([1]),
            },
          ],
        },
      } satisfies DocumentUpdate;
      void textUpdateWithBytes;
    }
  });

  it("infers in filters for boolean, bytes, and array columns", () => {
    expectTypeOf<WhereOf<typeof app.todos>["done"]>().branded.toEqualTypeOf<
      | boolean
      | {
          eq?: boolean;
          ne?: boolean;
          in?: boolean[];
          notIn?: boolean[];
        }
      | undefined
    >();
    expectTypeOf<WhereOf<typeof app.todos>["tags"]>().branded.toEqualTypeOf<
      | string[]
      | {
          eq?: string[];
          ne?: string[];
          contains?: string;
          in?: string[][];
          notIn?: string[][];
        }
      | undefined
    >();
    expectTypeOf<WhereOf<typeof app.todos>["attachment"]>().branded.toEqualTypeOf<
      | Uint8Array
      | {
          eq?: Uint8Array;
          ne?: Uint8Array;
          in?: (Uint8Array | number[])[];
          notIn?: (Uint8Array | number[])[];
        }
      | undefined
    >();
  });

  it("infers transformed column row and write types while keeping where raw", () => {
    expectTypeOf<RowOf<typeof transformedColumnApp.tasks>>().toEqualTypeOf<{
      id: string;
      title: string;
      urgency: Urgency;
    }>();
    expectTypeOf<InsertOf<typeof transformedColumnApp.tasks>>().toEqualTypeOf<{
      title: string;
      urgency: Urgency;
    }>();
    expectTypeOf<WhereOf<typeof transformedColumnApp.tasks>["urgency"]>().branded.toEqualTypeOf<
      | number
      | {
          eq?: number;
          ne?: number;
          gt?: number;
          gte?: number;
          lt?: number;
          lte?: number;
          in?: number[];
          notIn?: number[];
        }
      | undefined
    >();
  });
});
