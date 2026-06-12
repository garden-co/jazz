import { defineSuite } from "../runner/harness";
import { app, type Todo } from "../schema";

export default defineSuite("queries", ({ test }) => {
  test("filters by a condition", async ({ db, expect, waitForQuery }) => {
    db.insert(app.todos, { title: "a", done: false });
    db.insert(app.todos, { title: "b", done: true });
    db.insert(app.todos, { title: "c", done: false });

    const open = await waitForQuery<Todo>(
      db,
      app.todos.where({ done: false }),
      (r) => r.length === 2,
      "two open todos",
    );
    expect(open.every((t) => t.done === false)).toBe(true);
  });

  test("orders and limits", async ({ db, expect, waitForQuery }) => {
    db.insert(app.todos, { title: "zeta", done: false });
    db.insert(app.todos, { title: "alpha", done: false });
    db.insert(app.todos, { title: "mike", done: false });

    // Wait until all three have landed, then assert deterministic order + limit.
    await waitForQuery<Todo>(db, app.todos, (r) => r.length === 3, "all three land");
    const rows = await db.all(app.todos.orderBy("title", "asc").limit(2));
    expect(rows).toHaveLength(2);
    expect(rows[0]!.title).toBe("alpha");
    expect(rows[1]!.title).toBe("mike");
  });
});
