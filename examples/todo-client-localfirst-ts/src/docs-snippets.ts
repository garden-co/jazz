import type { Db } from "jazz-ts";
import { app } from "../schema/app.js";

const EXAMPLE_PROJECT_ID = "00000000-0000-0000-0000-000000000000";

// #region reading-oneshot-ts
export async function readTodosOneshot(db: Db) {
  return db.all(app.todos.where({ done: false }));
}
// #endregion reading-oneshot-ts

// #region reading-subscriptions-ts
export function subscribeTodos(db: Db, onCount: (count: number) => void) {
  return db.subscribeAll(app.todos.where({ done: false }), ({ all }) => onCount(all.length));
}
// #endregion reading-subscriptions-ts

// #region reading-settled-tier-ts
export async function readTodosSettledAtEdge(db: Db) {
  return db.all(app.todos.where({ done: false }), "edge");
}
// #endregion reading-settled-tier-ts

// #region writing-crud-ts
export function writeTodoCrud(db: Db, todoId: string) {
  db.insert(app.todos, {
    title: "Write docs",
    done: false,
    project: EXAMPLE_PROJECT_ID,
  });
  db.update(app.todos, todoId, { done: true });
  db.deleteFrom(app.todos, todoId);
}
// #endregion writing-crud-ts

// #region writing-ack-tier-ts
export async function writeTodoWithAckTiers(db: Db) {
  const id = await db.insertPersisted(
    app.todos,
    {
      title: "Write docs with ack",
      done: false,
      project: EXAMPLE_PROJECT_ID,
    },
    "edge",
  );

  await db.updatePersisted(app.todos, id, { done: true }, "core");
  await db.deleteFromPersisted(app.todos, id, "core");
}
// #endregion writing-ack-tier-ts
