import { defineSuite } from "../runner/harness";
import { app, type Todo } from "../schema";

export default defineSuite("crud", ({ test }) => {
  test("insert, update and delete round-trip", async ({ db, expect, waitForQuery }) => {
    const { value: created } = db.insert(app.todos, { title: "original", done: false });
    expect(created.id).toBeDefined();

    db.update(app.todos, created.id, { done: true });
    const updated = await waitForQuery<Todo>(
      db,
      app.todos.where({ id: { eq: created.id } }),
      (r) => r.length === 1 && r[0]!.done === true,
      "todo marked done",
    );
    expect(updated[0]!.title).toBe("original");

    db.delete(app.todos, created.id);
    await waitForQuery<Todo>(
      db,
      app.todos.where({ id: { eq: created.id } }),
      (r) => r.length === 0,
      "todo deleted",
    );
  });
});
