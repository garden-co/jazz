import { describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { translateQuery } from "./query-adapter.js";

const app = testApp({
  tables: {
    users: {
      columns: { name: "string" },
      relations: { todosViaOwner: { reverse: "todos", relation: "owner" } },
    },
    projects: {
      columns: { name: "string" },
      relations: { todosViaProject: { reverse: "todos", relation: "project" } },
    },
    todos: {
      columns: {
        title: "string",
        body: "string",
        rank: "bigint",
        attachment: "bytes",
        metadata: { json: true },
        done: "boolean",
        projectId: "uuid",
        ownerId: "uuid?",
      },
      relations: {
        project: { rel: "projects", column: "projectId" },
        owner: { rel: "users", column: "ownerId" },
      },
    },
  },
});
const scalarApp = testApp({
  tables: {
    filters: {
      columns: {
        timestamp: "timestamp",
        bytes: "bytes",
        nullableBytes: "bytes?",
        metadata: { json: true },
        tags: { array: "string" },
        nullableTags: { array: "string", optional: true },
      },
    },
  },
});

function translatedConditions(query: { _build(): string }) {
  return JSON.parse(translateQuery(query._build(), scalarApp.wasmSchema)).conditions;
}

describe("translateQuery", () => {
  it("uses total structured author equality in both flat and relation predicates", () => {
    const author = {
      account: "00000000-0000-4000-8000-000000000001",
      identity: { issuer: "issuer", subject: "subject" },
    };
    const eq = app.todos.where({ $createdBy: author });
    const ne = app.todos.where({ $createdBy: { ne: author } });
    const flatEq = JSON.parse(translateQuery(eq._build(), app.wasmSchema)).conditions[0];
    const flatNe = JSON.parse(translateQuery(ne._build(), app.wasmSchema)).conditions[0];
    expect(flatEq.And[0]).toEqual({
      Cmp: {
        left: { column: "$createdBy.account" },
        op: "Eq",
        right: { Literal: { type: "Uuid", value: author.account } },
      },
    });
    expect(flatNe).toEqual({ Not: flatEq });
    const relation = JSON.parse(
      translateQuery(app.union([eq, ne])._build(), app.wasmSchema),
    ).relation_ir;
    const text = JSON.stringify(relation);
    for (const path of [
      "$createdBy.account",
      "$createdBy.identity.issuer",
      "$createdBy.identity.subject",
    ])
      expect(text).toContain(path);
    expect(text).not.toContain('"IsNotNull"');
    expect(text).toContain('"Not"');
  });

  it.each([
    {},
    { account: null },
    { account: null, identity: { issuer: "issuer", subject: "subject" } },
    { account: "not-a-uuid", identity: { issuer: "issuer", subject: "subject" } },
    { account: null, identity: { issuer: "issuer", subject: " " } },
    { account: null, identity: { issuer: "issuer", subject: "\ud800" } },
    { account: null, identity: { issuer: "issuer", subject: "subject", extra: true } },
    { account: null, identity: { issuer: "issuer", subject: "subject" }, extra: true },
  ])("rejects malformed whole-author filters without dropping constraints: %j", (author) => {
    expect(() =>
      translateQuery(
        app.todos.where({ $createdBy: { eq: author } } as never)._build(),
        app.wasmSchema,
      ),
    ).toThrow();
  });

  // https://github.com/garden-co/jazz/issues/2571
  it("preserves public union membership in the shared runtime query", () => {
    const union = app.union([
      app.users.where({ name: "first" }),
      app.users.where({ name: "second" }),
    ]);
    const translated = JSON.parse(translateQuery(union._build(), app.wasmSchema));
    expect(translated).toHaveProperty("relation_ir");
    expect(JSON.stringify(translated.relation_ir)).toContain('"Union"');
  });

  it("preserves signed bigint union literals through relation JSON", () => {
    const query = app.union([
      app.todos.where({ rank: 9_007_199_254_740_993n }),
      app.todos.where({ rank: -9_007_199_254_740_993n }),
    ]);
    const translated = JSON.parse(translateQuery(query._build(), app.wasmSchema));
    const relationJson = JSON.stringify(translated.relation_ir);
    expect(relationJson).toContain('"9007199254740993"');
    expect(relationJson).toContain('"-9007199254740993"');
  });

  it("leaves unnamed union arms for Rust to label and keeps named ones", () => {
    const first = app.todos.where({ title: "first" });
    const second = app.todos.where({ title: "second" });
    const unnamed = app.union([first, second, first]) as any;
    expect(unnamed._unionVal.union.inputs.map((arm: any) => arm.label)).toEqual(["", "", ""]);
    const named = app.union({ duplicate: first, second }) as any;
    expect(named._unionVal.union.inputs.map((arm: any) => arm.label)).toEqual([
      "duplicate",
      "second",
    ]);
    const translated = JSON.parse(translateQuery(unnamed._build(), app.wasmSchema));
    expect(translated.relation_ir.Union.inputs.map((arm: { label: string }) => arm.label)).toEqual([
      "",
      "",
      "",
    ]);
  });

  it("rejects public union modifiers that retained relation lowering cannot compose", () => {
    const union = app.union([app.todos.where({ done: false }), app.todos.where({ title: "a" })]);
    expect(() => union.select("title")).toThrow(/select\(\.\.\.\) is not supported/);
    expect(() => union.include({})).toThrow(/include\(\.\.\.\) is not supported/);
    expect(() => union.requireIncludes()).toThrow(/requireIncludes\(\) is not supported/);
    expect(() => union.includeDeleted()).toThrow(/includeDeleted\(\) is not supported/);
  });

  it("rejects duplicate external descriptors before allowing a reference-name alias", () => {
    const duplicateDescriptorSchema = {
      users: {
        relations: {
          todosViaOwner: { kind: "reverse" as const, table: "todos", relation: "ownerRelation" },
        },
        columns: [{ name: "name", column_type: { type: "Text" as const }, nullable: false }],
      },
      todos: {
        relations: { ownerRelation: { kind: "forward" as const, table: "users", column: "owner" } },
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
      /Table "todos" has duplicate column descriptor "owner"/,
    );
  });

  it("rejects a forward relation that would shadow a stored output column", () => {
    expect(() =>
      testApp({
        tables: {
          users: {
            columns: { name: "string" },
            relations: { todosViaOwner: { reverse: "todos", relation: "owner" } },
          },
          todos: {
            columns: { owner: "string", ownerId: "uuid" },
            relations: { owner: { rel: "users", column: "ownerId" } },
          },
        },
      }),
    ).toThrow("tables.todos.relations.owner: table todos already has a column owner");
  });

  it("rejects a generated relation that would shadow the implicit public id", () => {
    expect(() =>
      testApp({
        tables: {
          users: {
            columns: { name: "string" },
            relations: { todosViaId: { reverse: "todos", relation: "id" } },
          },
          todos: {
            columns: { idId: "uuid" },
            relations: { id: { rel: "users", column: "idId" } },
          },
        },
      }),
    ).toThrow("tables.todos.relations.id: table todos already has a column id");
  });

  it("rejects a nested reverse relation that would shadow a stored output column", () => {
    expect(() =>
      testApp({
        tables: {
          users: {
            columns: { todosViaOwner: "string" },
            relations: { todosViaOwner: { reverse: "todos", relation: "owner" } },
          },
          todos: {
            columns: { ownerId: "uuid" },
            relations: { owner: { rel: "users", column: "ownerId" } },
          },
        },
      }),
    ).toThrow(
      "tables.users.relations.todosViaOwner: table users already has a column todosViaOwner",
    );
  });

  it("uses a distinct declared alias for an unsuffixed UUID column", () => {
    expect(() =>
      testApp({
        tables: {
          users: {
            columns: { name: "string" },
            relations: { todosViaOwner: { reverse: "todos", relation: "ownerRelation" } },
          },
          todos: {
            columns: { owner: "uuid" },
            relations: { ownerRelation: { rel: "users", column: "owner" } },
          },
        },
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
  it("orders relation hops by the projected output scope", () => {
    const translated = JSON.parse(
      translateQuery(
        app.todos.where({ done: false }).hopTo("owner").orderBy("name")._build(),
        app.wasmSchema,
      ),
    );
    const { input, terms } = translated.relation_ir.OrderBy;
    const projectedName = input.Project.columns.find(
      (column: { alias: string }) => column.alias === "name",
    );
    expect(terms).toEqual([{ column: projectedName.expr.Column, direction: "Asc" }]);
    expect(terms[0].column).toEqual({ scope: "__hop_0", column: "name" });
  });

  it("keeps native relation IR for relation traversal queries", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.where({ done: false }).hopTo("owner")._build(), app.wasmSchema),
    );

    expect(translated.table).toBe("users");
    expect(translated.relation_ir).toBeDefined();
    expect(translated.conditions).toBeUndefined();
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

  it("lowers direct Date values as one equality", () => {
    const timestamp = new Date("2026-01-02T03:04:05.678Z");
    expect(translatedConditions(scalarApp.filters.where({ timestamp }))).toEqual([
      {
        Cmp: {
          left: { column: "timestamp" },
          op: "Eq",
          right: { Literal: { type: "Timestamp", value: timestamp.getTime() } },
        },
      },
    ]);
  });

  it("lowers direct Uint8Array values as one equality", () => {
    expect(
      translatedConditions(scalarApp.filters.where({ bytes: new Uint8Array([1, 2, 3]) })),
    ).toEqual([
      {
        Cmp: {
          left: { column: "bytes" },
          op: "Eq",
          right: { Literal: { type: "Bytea", value: [1, 2, 3] } },
        },
      },
    ]);
  });

  it("lowers nullable Bytea null checks without converting flags as bytes", () => {
    expect(
      translatedConditions(scalarApp.filters.where({ nullableBytes: { isNull: true } })),
    ).toEqual([{ IsNull: { column: { column: "nullableBytes" } } }]);
    expect(
      translatedConditions(scalarApp.filters.where({ nullableBytes: { isNull: false } })),
    ).toEqual([{ IsNotNull: { column: { column: "nullableBytes" } } }]);
  });
  it("lowers nullable Array null checks", () => {
    expect(
      translatedConditions(scalarApp.filters.where({ nullableTags: { isNull: true } })),
    ).toEqual([{ IsNull: { column: { column: "nullableTags" } } }]);
    expect(
      translatedConditions(scalarApp.filters.where({ nullableTags: { isNull: false } })),
    ).toEqual([{ IsNotNull: { column: { column: "nullableTags" } } }]);
  });

  it("lowers direct JSON objects as one equality", () => {
    const metadata = { nested: { answer: 42 } };
    expect(translatedConditions(scalarApp.filters.where({ metadata }))).toEqual([
      {
        Cmp: {
          left: { column: "metadata" },
          op: "Eq",
          right: { Literal: { type: "Text", value: JSON.stringify(metadata) } },
        },
      },
    ]);
  });

  it("lowers direct arrays as one equality", () => {
    expect(translatedConditions(scalarApp.filters.where({ tags: ["urgent"] }))).toEqual([
      {
        Cmp: {
          left: { column: "tags" },
          op: "Eq",
          right: {
            Literal: {
              type: "Array",
              value: [{ type: "Text", value: "urgent" }],
            },
          },
        },
      },
    ]);
  });

  it("treats typo-only JSON objects as direct equality literals", () => {
    const metadata = { misspelled: true };
    expect(translatedConditions(scalarApp.filters.where({ metadata }))).toEqual([
      {
        Cmp: {
          left: { column: "metadata" },
          op: "Eq",
          right: { Literal: { type: "Text", value: JSON.stringify(metadata) } },
        },
      },
    ]);
  });

  it("treats empty JSON objects as direct equality literals", () => {
    const metadata = {};
    expect(translatedConditions(scalarApp.filters.where({ metadata }))).toEqual([
      {
        Cmp: {
          left: { column: "metadata" },
          op: "Eq",
          right: { Literal: { type: "Text", value: JSON.stringify(metadata) } },
        },
      },
    ]);
  });

  it("lowers provenance Date fields as direct equality", () => {
    const createdAt = new Date("2026-02-03T04:05:06.789Z");
    expect(translatedConditions(scalarApp.filters.where({ $createdAt: createdAt }))).toEqual([
      {
        Cmp: {
          left: { column: "$createdAt" },
          op: "Eq",
          right: { Literal: { type: "Timestamp", value: createdAt.getTime() } },
        },
      },
    ]);
  });

  it("leaves operator checks on JSON operator maps to the engine", () => {
    // The engine refuses `gt` on JSON at prepare (db.in-memory.test.ts).
    expect(
      translatedConditions(scalarApp.filters.where({ metadata: { gt: 1 } } as never)),
    ).toHaveLength(1);
  });
});
