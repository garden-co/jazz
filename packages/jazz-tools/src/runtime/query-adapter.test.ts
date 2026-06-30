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
});
