import { defineSuite } from "../runner/harness";
import { app, type Todo } from "../schema";

export default defineSuite("bulk writes", ({ test }) => {
  test("writes 100 todos and reads them all back", async ({ db, expect, waitForQuery }) => {
    for (let i = 0; i < 100; i++) {
      db.insert(app.todos, { title: `t-${i}`, done: false });
    }
    const rows = await waitForQuery<Todo>(
      db,
      app.todos,
      (r) => r.length >= 100,
      "100 todos land",
      15_000,
    );
    expect(rows).toHaveLength(100);
  });
});
