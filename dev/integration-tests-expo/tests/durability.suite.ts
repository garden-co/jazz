import { defineSuite } from "../runner/harness";
import { app } from "../schema";

export default defineSuite("durability", ({ test }) => {
  test("insert resolves at the local tier and is queryable", async ({ db, expect }) => {
    const row = await db
      .insert(app.todos, { title: "durable", done: false })
      .wait({ tier: "local" });
    expect(row.id).toBeDefined();

    const found = await db.one(app.todos.where({ id: { eq: row.id } }));
    expect(found).not.toBeNull();
    expect(found?.title).toBe("durable");
  });
});
