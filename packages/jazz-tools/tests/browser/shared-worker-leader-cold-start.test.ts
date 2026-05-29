import { describe, expect, it } from "vitest";
import { createDb } from "../../src/runtime/db.js";
import { schema as s } from "../../src/index.js";
import { leaderSupported } from "./fixtures/leader-support.js";

// Public-API schema (no JSON-like literals, per repo convention).
const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};
type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
const { todos } = app;
type Todo = s.RowOf<typeof todos>;
const allTodos = app.todos;

describe.skipIf(!leaderSupported)("shared-worker-leader cold start", () => {
  it("single tab opens DB through the SharedWorker leader", async () => {
    const db = await createDb({
      appId: `leader-cold-${Math.random().toString(36).slice(2, 8)}`,
      driver: { type: "persistent", dbName: `cold-${Date.now()}` },
    });

    const {
      value: { id },
    } = db.insert(todos, { title: "Alice", done: false });

    const rows = await db.all<Todo>(allTodos);
    expect(rows.map((r) => r.id)).toEqual([id]);

    await db.shutdown();
  }, 30000);
});
