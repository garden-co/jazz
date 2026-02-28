import type { Db } from "jazz-tools";
import { app } from "../schema/app.js";

const EXAMPLE_PROJECT_ID = "00000000-0000-0000-0000-000000000000";
const EXAMPLE_OWNER_ID = "local:example-owner";

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

// #region reading-filters-ts
export async function readTodosWithFilters(db: Db) {
  return db.all(app.todos.where({ done: false, title: { contains: "docs" } }));
}
// #endregion reading-filters-ts

// #region reading-where-operators-ts
export async function readTodosWithWhereOperators(db: Db) {
  await db.all(app.todos.where({ done: false }));
  await db.all(app.todos.where({ title: { contains: "milk" } }));
  await db.all(app.todos.where({ project: { ne: EXAMPLE_PROJECT_ID } }));
}
// #endregion reading-where-operators-ts

// #region reading-sorting-ts
export async function readTodosSortedByTitle(db: Db) {
  return db.all(app.todos.where({ done: false }).orderBy("title", "asc"));
}
// #endregion reading-sorting-ts

// #region reading-pagination-ts
export async function readTodoPage(db: Db, page: number, pageSize = 20) {
  const offset = Math.max(0, (page - 1) * pageSize);
  return db.all(
    app.todos.where({ done: false }).orderBy("title", "asc").limit(pageSize).offset(offset),
  );
}
// #endregion reading-pagination-ts

// #region reading-includes-ts
export async function readTodosWithIncludes(db: Db) {
  return db.all(
    app.todos.where({ done: false }).include({ project: true, parent: { project: true } }),
  );
}
// #endregion reading-includes-ts

// #region reading-recursive-ts
export function buildTodoLineageQuery() {
  return app.todos.gather({
    start: { done: false },
    step: ({ current }) => app.todos.where({ parent: current }).hopTo("parent"),
    maxDepth: 10,
  });
}
// #endregion reading-recursive-ts

// #region writing-crud-ts
export function writeTodoCrud(db: Db, todoId: string) {
  db.insert(app.todos, {
    title: "Write docs",
    done: false,
    owner_id: EXAMPLE_OWNER_ID,
    project: EXAMPLE_PROJECT_ID,
  });
  db.update(app.todos, todoId, { done: true });
  db.deleteFrom(app.todos, todoId);
}
// #endregion writing-crud-ts

// #region writing-ack-tier-ts
export async function writeTodoWithAckTiers(db: Db) {
  const id = await db.insertWithAck(
    app.todos,
    {
      title: "Write docs with ack",
      done: false,
      owner_id: EXAMPLE_OWNER_ID,
      project: EXAMPLE_PROJECT_ID,
    },
    "edge",
  );

  await db.updateWithAck(app.todos, id, { done: true }, "core");
  await db.deleteFromWithAck(app.todos, id, "core");
}
// #endregion writing-ack-tier-ts
