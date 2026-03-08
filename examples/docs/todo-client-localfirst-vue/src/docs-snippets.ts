import type { Db } from "jazz-tools";
import { app } from "../schema/app.js";

const EXAMPLE_TODO_ID = "00000000-0000-0000-0000-000000000000";
const EXAMPLE_PROJECT_ID = "00000000-0000-0000-0000-000000000000";

// #region oneshot-vue
export async function readTodosOneshot(db: Db) {
  const todos = await db.all(app.todos.where({ done: false }));
  const todo = await db.one(app.todos.where({ id: EXAMPLE_TODO_ID }));
  return { todos, todo };
}
// #endregion oneshot-vue

// #region subscribe-vue
export function subscribeTodos(db: Db, onUpdate: (results: unknown[]) => void) {
  const unsubscribe = db.subscribeAll(app.todos.where({ done: false }), ({ all }) => {
    onUpdate(all);
  });

  return unsubscribe;
}
// #endregion subscribe-vue

// #region where-operators-vue
export async function whereExamples(db: Db) {
  await db.all(app.todos.where({ done: false }));
  await db.all(app.todos.where({ title: { contains: "milk" } }));
  await db.all(app.todos.where({ project: { ne: EXAMPLE_PROJECT_ID } }));
}
// #endregion where-operators-vue

// #region includes-vue
export async function includeExamples(db: Db) {
  const todos = await db.all(app.todos.include({ project: true, parent: true }));
  const projects = await db.all(app.projects.include({ todosViaProject: true }));
  const nested = await db.all(
    app.todos.include({
      project: {
        todosViaProject: true,
      },
    }),
  );
  const filtered = await db.all(
    app.todos.include({
      todosViaParent: app.todos.where({ done: false }),
    }),
  );

  return { todos, projects, nested, filtered };
}
// #endregion includes-vue

// #region chaining-vue
export async function chainingExamples(db: Db) {
  return db.all(app.todos.where({ done: false }).where({ title: { contains: "docs" } }));
}
// #endregion chaining-vue

// #region combining-vue
export async function combinedQuery(db: Db) {
  return db.all(
    app.todos
      .where({ done: false, title: { contains: "docs" } })
      .include({ project: true, parent: true })
      .orderBy("title", "asc")
      .limit(20)
      .offset(0),
  );
}
// #endregion combining-vue

// #region reading-tier-vue
export function subscribeTodosAtEdge(db: Db, onCount: (count: number) => void) {
  return db.subscribeAll(app.todos.where({ done: false }), ({ all }) => onCount(all.length), {
    tier: "edge",
    localUpdates: "immediate",
  });
}
// #endregion reading-tier-vue

// #region writing-durability-vue
export async function writeWithDurabilityTier(db: Db, todoTitle: string) {
  const { id } = await db.insertDurable(
    app.todos,
    { title: todoTitle, done: false },
    { tier: "edge" },
  );
  await db.updateDurable(app.todos, id, { done: true }, { tier: "edge" });
  await db.deleteDurable(app.todos, id, { tier: "global" });
}
// #endregion writing-durability-vue
