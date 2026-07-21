import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../src/runtime/db.js";
import { schema as s } from "../../src/index.js";

const schemaDefinition = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schemaDefinition>;
const app: s.App<AppSchema> = s.defineApp(schemaDefinition);

describe("count aggregate over the browser worker", () => {
  let db: Db;

  beforeAll(async () => {
    db = await createDb({
      appId: "count-aggregate-test",
      driver: { type: "persistent", dbName: `count-agg-${Date.now()}` },
    });
    for (let i = 0; i < 25; i++) {
      db.insert(app.todos, { title: `todo-${i}`, done: i % 5 === 0 });
    }
  }, 60_000);

  afterAll(async () => {
    await db.shutdown();
  });

  it("answers a one-shot count", async () => {
    const rows = await db.all(app.todos.count(), { propagation: "local-only" });
    expect(rows).toHaveLength(1);
    expect(rows[0]!.count).toBe(25);
  });

  it("answers a filtered count", async () => {
    const rows = await db.all(app.todos.where({ done: true }).count(), {
      propagation: "local-only",
    });
    expect(rows).toHaveLength(1);
    expect(rows[0]!.count).toBe(5);
  });

  it("maintains a live count subscription across inserts", async () => {
    const counts: number[] = [];
    const unsubscribe = db.subscribeAll(app.todos.count(), (delta) => {
      const row = delta.all[0];
      if (row) counts.push((row as { count: number }).count);
    });
    try {
      await waitFor(() => counts.at(-1) === 25, "initial count");
      db.insert(app.todos, { title: "one more", done: false });
      await waitFor(() => counts.at(-1) === 26, "count after insert");
    } finally {
      unsubscribe();
    }
  }, 30_000);
});

async function waitFor(check: () => boolean, label: string): Promise<void> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for ${label}`);
}
