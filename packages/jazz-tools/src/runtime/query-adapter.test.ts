import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { translateQuery } from "./query-adapter.js";

const app = s.defineApp({
  users: s.table({
    name: s.string(),
  }),
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    body: s.string(),
    attachment: s.bytes(),
    metadata: s.json(),
    done: s.boolean(),
    projectId: s.ref("projects"),
    ownerId: s.ref("users").optional(),
  }),
  events: s.table({
    event: s.enum({
      message: { text: s.string(), level: s.int() },
      closed: { code: s.int() },
    }),
  }),
});

describe("translateQuery", () => {
  it("rejects public unions instead of silently scanning the whole table", () => {
    const union = app.union([
      app.users.where({ name: "first" }),
      app.users.where({ name: "second" }),
    ]);
    expect(() => translateQuery(union._build(), app.wasmSchema)).toThrow(
      "Public union queries are not supported by canonical query lowering yet.",
    );
  });

  // https://github.com/garden-co/jazz/issues/2571
  // Known shared-API regression: union is currently omitted from native-feature
  // routing, so browser and native adapters receive an unfiltered table scan.
  // Keep this executable counterexample until canonical union lowering exists.
  it.fails("preserves public union membership in the shared runtime query", () => {
    const union = app.union([
      app.users.where({ name: "first" }),
      app.users.where({ name: "second" }),
    ]);
    const translated = JSON.parse(translateQuery(union._build(), app.wasmSchema));
    expect(translated).toHaveProperty("relation_ir");
    expect(JSON.stringify(translated.relation_ir)).toContain('"Union"');
  });

  it("rejects colliding externally supplied relation schemas during query lowering", () => {
    const ambiguousRelationsSchema = {
      users: {
        columns: [{ name: "name", column_type: { type: "Text" as const }, nullable: false }],
      },
      todos: {
        columns: [
          {
            name: "ownerId",
            column_type: { type: "Uuid" as const },
            nullable: false,
            references: "users",
          },
          {
            name: "owner_id",
            column_type: { type: "Uuid" as const },
            nullable: false,
            references: "users",
          },
        ],
      },
    };

    expect(() => translateQuery(app.todos._build(), ambiguousRelationsSchema)).toThrow(
      /Generated relation name "owner" is ambiguous on table "todos".*"todos.ownerId".*"todos.owner_id"/,
    );
  });

  it("rejects duplicate external descriptors before allowing a reference-name alias", () => {
    const duplicateDescriptorSchema = {
      users: {
        columns: [{ name: "name", column_type: { type: "Text" as const }, nullable: false }],
      },
      todos: {
        columns: [
          { name: "owner", column_type: { type: "Text" as const }, nullable: false },
          {
            name: "owner",
            column_type: { type: "Uuid" as const },
            nullable: false,
            references: "users",
          },
        ],
      },
    };

    expect(() => translateQuery(app.todos._build(), duplicateDescriptorSchema)).toThrow(
      /Table "todos" has duplicate column descriptor "owner": descriptor #1 \(Text\) conflicts with descriptor #2 \(Uuid referencing "users"\)/,
    );
  });

  it("rejects a forward relation that would shadow a stored output column", () => {
    expect(() =>
      s.defineApp({
        users: s.table({ name: s.string() }),
        todos: s.table({
          owner: s.string(),
          ownerId: s.ref("users"),
        }),
      }),
    ).toThrow(
      /Generated relation name "owner" on table "todos".*forward relation generated from reference column "todos.ownerId".*stored\/public output column "todos.owner"/,
    );
  });

  it("rejects a generated relation that would shadow the implicit public id", () => {
    expect(() =>
      s.defineApp({
        users: s.table({ name: s.string() }),
        todos: s.table({
          idId: s.ref("users"),
        }),
      }),
    ).toThrow(
      /Generated relation name "id" on table "todos".*forward relation generated from reference column "todos.idId".*stored\/public output column "todos.id"/,
    );
  });

  it("rejects a nested reverse relation that would shadow a stored output column", () => {
    expect(() =>
      s.defineApp({
        users: s.table({
          todosViaOwner: s.string(),
        }),
        todos: s.table({
          ownerId: s.ref("users"),
        }),
      }),
    ).toThrow(
      /Generated relation name "todosViaOwner" on table "users".*reverse relation generated from reference column "todos.ownerId".*stored\/public output column "users.todosViaOwner"/,
    );
  });

  it("preserves the established reference-column relation alias", () => {
    expect(() =>
      s.defineApp({
        users: s.table({ name: s.string() }),
        todos: s.table({
          owner: s.ref("users"),
        }),
      }),
    ).not.toThrow();
  });

  it("emits ordinary table queries on the flat Query path", () => {
    const query = app.todos
      .includeDeleted()
      .where({ done: false, ownerId: { isNull: true } })
      .include({ owner: true })
      .select("title")
      .orderBy("title", "desc")
      .limit(5)
      .offset(2);

    const translated = JSON.parse(translateQuery(query._build(), app.wasmSchema));

    expect(translated).toMatchObject({
      table: "todos",
      include_deleted: true,
      conditions: [
        {
          Cmp: {
            left: { column: "done" },
            op: "Eq",
            right: { Literal: { type: "Boolean", value: false } },
          },
        },
        { IsNull: { column: { column: "ownerId" } } },
      ],
      select_columns: [{ kind: "full", column: "title" }],
      order_by: [{ column: "title", direction: "Desc" }],
      limit: 5,
      offset: 2,
    });
    expect(translated.relation_ir).toBeUndefined();
    expect(translated.array_subqueries).toHaveLength(1);
  });

  it("uses the same canonical predicate IR for root and included membership filters", () => {
    const translated = JSON.parse(
      translateQuery(
        app.projects
          .where({ id: { notIn: ["00000000-0000-0000-0000-000000000001"] } })
          .include({ todosViaProject: app.todos.where({ title: { notIn: ["hidden"] } }) })
          ._build(),
        app.wasmSchema,
      ),
    );

    const rootPredicate = translated.conditions[0];
    const includePredicate = translated.array_subqueries[0].filters[0];
    expect(rootPredicate).toEqual({
      Not: {
        In: {
          left: { column: "id" },
          values: [{ Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" } }],
        },
      },
    });
    expect(includePredicate).toEqual({
      Not: {
        In: {
          left: { column: "title" },
          values: [{ Literal: { type: "Text", value: "hidden" } }],
        },
      },
    });
  });

  it("lowers partial select descriptors to the native projection contract", () => {
    const translated = JSON.parse(
      translateQuery(
        app.todos
          .select({
            attachment: { from: 1_000_000, to: 2_000_000 },
            body: { from: 4, to: 124 },
            title: { fromUtf8: 4, toUtf8: 67 },
            metadata: { at: "/someKey/11/otherKey" },
          })
          ._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.select_columns).toEqual([
      { kind: "bytes", column: "attachment", from: 1_000_000, to: 2_000_000 },
      { kind: "text_utf16", column: "body", from: 4, to: 124 },
      { kind: "text_utf8", column: "title", from: 4, to: 67 },
      { kind: "json_pointer", column: "metadata", at: "/someKey/11/otherKey" },
    ]);
  });

  it("rejects partial large-value selections in include builders without rejecting named columns", () => {
    const namedColumnInclude = JSON.parse(
      translateQuery(
        app.projects.include({ todosViaProject: app.todos.select("body") })._build(),
        app.wasmSchema,
      ),
    );
    expect(namedColumnInclude.array_subqueries).toMatchObject([
      { column_name: "todosViaProject", select_columns: ["body"] },
    ]);

    expect(() =>
      translateQuery(
        app.projects
          .include({
            todosViaProject: app.todos.select({ body: { from: 4, to: 124 } }),
          })
          ._build(),
        app.wasmSchema,
      ),
    ).toThrow(
      'Include builder for relation "todosViaProject" does not support partial large-value selections.',
    );

    expect(() =>
      translateQuery(
        app.projects
          .include({
            todosViaProject: app.todos.include({
              project: app.projects.select({ name: { from: 0, to: 1 } }),
            }),
          })
          ._build(),
        app.wasmSchema,
      ),
    ).toThrow(
      'Include builder for relation "project" does not support partial large-value selections.',
    );
  });
  it("keeps native relation IR for relation traversal queries", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.where({ done: false }).hopTo("owner")._build(), app.wasmSchema),
    );

    expect(translated.relation_ir).toBeDefined();
    expect(translated.conditions).toBeUndefined();
  });

  it("lowers a payload enum match to the first-class relation predicate", () => {
    const translated = JSON.parse(
      translateQuery(
        app.events.where({ event: { match: { type: "message", where: { level: 2 } } } })._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.relation_ir).toEqual({
      Project: {
        input: {
          Filter: {
            input: { TableScan: { table: "events" } },
            predicate: {
              EnumMatch: {
                column: { column: "event", scope: "events" },
                case: "message",
                payload: {
                  Cmp: {
                    left: { column: "level" },
                    op: "Eq",
                    right: { Literal: { type: "Integer", value: 2 } },
                  },
                },
              },
            },
          },
        },
        columns: [{ alias: "event", expr: { Column: { column: "event", scope: "events" } } }],
      },
    });
  });

  it("rejects a payload enum match against an absent case field", () => {
    const built = JSON.parse(app.events._build());
    built.conditions = [
      { column: "event", op: "match", value: { type: "closed", where: { level: 2 } } },
    ];
    expect(() => translateQuery(JSON.stringify(built), app.wasmSchema)).toThrow(
      'unknown payload enum field "level" for case "closed"',
    );
  });

  it("treats an omitted include limit as unbounded", () => {
    const translated = JSON.parse(
      translateQuery(app.users.include({ todosViaOwner: app.todos })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null },
    ]);
  });

  it("preserves an omitted limit across subsequent query-builder clones", () => {
    const translated = JSON.parse(
      translateQuery(
        app.users
          .include({
            todosViaOwner: app.todos.select("title").orderBy("title"),
          })
          ._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null },
    ]);
  });

  it("treats an omitted forward-relation limit as unbounded", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.include({ owner: app.users })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([{ column_name: "owner", limit: null }]);
  });

  it("treats include shorthand as an explicit whole-relation request", () => {
    const translated = JSON.parse(
      translateQuery(app.users.include({ todosViaOwner: true })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null },
    ]);
  });

  it("keeps projected include fields in their public terminal namespace", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.select("title").include({ owner: true })._build(), app.wasmSchema),
    );

    // Query bytes are ShapeAst v0-compatible: do not add a positional codec
    // field just to recover this name later. The collector descriptor carries
    // the public relation field directly.
    expect(translated.array_subqueries).toMatchObject([{ column_name: "owner" }]);
    expect(translated.array_subqueries[0]).not.toHaveProperty("public_name");
  });

  it("leaves required-include pagination at the core query boundary", () => {
    const translated = JSON.parse(
      translateQuery(
        app.todos.include({ project: true }).requireIncludes().offset(2).limit(1)._build(),
        app.wasmSchema,
      ),
    );

    expect(translated).toMatchObject({ limit: 1, offset: 2 });
    expect(translated.array_subqueries).toMatchObject([{ requirement: "AtLeastOne" }]);
    expect(translated).not.toHaveProperty("__jazz_client_limit");
    expect(translated).not.toHaveProperty("__jazz_client_offset");
  });
});
