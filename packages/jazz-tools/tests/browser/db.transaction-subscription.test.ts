import { expect, it } from "vitest";
import { generateAuthSecret, schema, type RowOf } from "../../src/index.js";
import { createBrowserTestDb as createDb } from "./account-fixtures.js";
import { waitForCondition } from "./support.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

it("delivers a queued memory transaction to an existing local subscription", async () => {
  const db = await createDb({
    appId: crypto.randomUUID(),
    secret: generateAuthSecret(),
    driver: { type: "memory" },
  });
  let stop = () => {};
  let visible: RowOf<typeof app.todos>[] = [];
  const errors: Error[] = [];
  const subscribe = () =>
    db.subscribe(
      app.todos,
      {
        onUpdate(rows) {
          visible = rows;
        },
        onError(error) {
          errors.push(error);
        },
      },
      { tier: "local" },
    );
  try {
    const seeded = await db.transaction((tx) => {
      tx.insert(app.todos, { title: "pending task", done: false });
    });
    await seeded.wait({ tier: "local" });
    const [row] = await db.all(app.todos, { tier: "local" });

    // Reopening the same query mirrors application subscription lifetimes.
    stop = subscribe();
    await waitForCondition(
      async () => visible.length === 1,
      2_000,
      "setup subscription did not open",
    );
    stop();
    visible = [];
    stop = subscribe();
    await waitForCondition(
      async () => visible.length === 1,
      2_000,
      "active subscription did not open",
    );
    expect(visible[0].done).toBe(false);

    const tx = db.beginTransaction();
    tx.update(app.todos, row.id, { done: true });
    await Promise.resolve();
    await tx.commit().wait({ tier: "local" });
    await waitForCondition(
      async () => visible.length === 1 && visible[0].done,
      2_000,
      "committed memory transaction did not reach its local subscription",
    );
    expect(errors).toEqual([]);
    expect(await db.all(app.todos, { tier: "local" })).toEqual(visible);
  } finally {
    stop();
    await db.shutdown();
  }
});
