import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { translateQuery } from "./query-adapter.js";

const app = s.defineApp({
  users: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    ownerId: s.ref("users").optional(),
  }),
});

describe("translateQuery", () => {
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
        { column: "done", op: "eq", value: false },
        { column: "ownerId", op: "isNull", value: true },
      ],
      select_columns: ["title"],
      order_by: [{ column: "title", direction: "Desc" }],
      limit: 5,
      offset: 2,
    });
    expect(translated.relation_ir).toBeUndefined();
    expect(translated.array_subqueries).toHaveLength(1);
  });

  it("keeps native relation IR for relation traversal queries", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.where({ done: false }).hopTo("owner")._build(), app.wasmSchema),
    );

    expect(translated.relation_ir).toBeDefined();
    expect(translated.conditions).toBeUndefined();
  });

  it("preserves explicit unbounded intent for query-builder includes", () => {
    const translated = JSON.parse(
      translateQuery(
        app.users.include({ todosViaOwner: app.todos.unbounded() })._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null, unbounded: true },
    ]);
  });

  it("preserves unbounded intent across subsequent query-builder clones", () => {
    const translated = JSON.parse(
      translateQuery(
        app.users
          .include({
            todosViaOwner: app.todos.unbounded().select("title").orderBy("title"),
          })
          ._build(),
        app.wasmSchema,
      ),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null, unbounded: true },
    ]);
  });

  it("preserves explicit unbounded intent for forward relations", () => {
    const translated = JSON.parse(
      translateQuery(app.todos.include({ owner: app.users.unbounded() })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "owner", limit: null, unbounded: true },
    ]);
  });

  it("treats include shorthand as an explicit whole-relation request", () => {
    const translated = JSON.parse(
      translateQuery(app.users.include({ todosViaOwner: true })._build(), app.wasmSchema),
    );

    expect(translated.array_subqueries).toMatchObject([
      { column_name: "todosViaOwner", limit: null, unbounded: true },
    ]);
  });

  it("rejects query-builder includes without an explicit bound", () => {
    expect(() =>
      translateQuery(app.users.include({ todosViaOwner: app.todos })._build(), app.wasmSchema),
    ).toThrow('relation "todosViaOwner" must specify limit(...) or unbounded()');
  });

  it("rejects malformed query JSON declaring finite and unbounded together", () => {
    const built = JSON.parse(
      app.users.include({ todosViaOwner: app.todos.limit(2) })._build(),
    ) as Record<string, any>;
    built.includes.todosViaOwner.unbounded = true;

    expect(() => translateQuery(JSON.stringify(built), app.wasmSchema)).toThrow(
      'relation "todosViaOwner" cannot specify both limit(...) and unbounded()',
    );
  });
});
