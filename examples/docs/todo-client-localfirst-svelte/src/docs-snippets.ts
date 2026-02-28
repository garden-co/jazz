import type { Db } from "jazz-tools";
import { app } from "../schema/app.js";

// #region oneshot-svelte
export async function readTodosOneshot(db: Db) {
  const todos = await db.all(app.todos.where({ done: false }));
  const todo = await db.one(app.todos.where({ id: "00000000-0000-0000-0000-000000000000" }));
  return { todos, todo };
}
// #endregion oneshot-svelte

// #region subscribe-svelte
export function subscribeTodos(db: Db, onUpdate: (results: unknown[]) => void) {
  const unsubscribe = db.subscribeAll(app.todos.where({ done: false }), (delta) => {
    // delta.all       — Todo[] full current result set
    // delta.delta     — RowDelta<Todo>[] granular row-level changes
    onUpdate(delta.all);
  });

  return unsubscribe;
}
// #endregion subscribe-svelte

// #region where-operators-svelte
export async function whereExamples(db: Db) {
  // Equality (shorthand)
  await db.all(app.todos.where({ done: false }));

  // Explicit operators
  await db.all(app.todos.where({ title: { contains: "milk" } }));
  await db.all(app.todos.where({ project: { ne: "00000000-0000-0000-0000-000000000000" } }));
}
// #endregion where-operators-svelte

// #region includes-svelte
export async function includeExamples(db: Db) {
  // Load each todo's project and parent in one shot
  const todos = await db.all(app.todos.include({ project: true, parent: true }));

  // Reverse relations: project FK on todos creates todosViaProject on projects
  const projects = await db.all(app.projects.include({ todosViaProject: true }));

  // Nested: load project, and for each project, load its todos
  const nested = await db.all(
    app.todos.include({
      project: {
        todosViaProject: true,
      },
    }),
  );

  // Filtered: only include incomplete child todos
  const filtered = await db.all(
    app.todos.include({
      todosViaParent: app.todos.where({ done: false }),
    }),
  );

  return { todos, projects, nested, filtered };
}
// #endregion includes-svelte

// #region chaining-svelte
export async function chainingExamples(db: Db) {
  // Multiple where calls produce AND semantics
  const results = await db.all(
    app.todos.where({ done: false }).where({ title: { contains: "docs" } }),
  );

  return results;
}
// #endregion chaining-svelte

// #region combining-svelte
export async function combinedQuery(db: Db) {
  const results = await db.all(
    app.todos
      .where({ done: false, title: { contains: "docs" } })
      .include({ project: true, parent: true })
      .orderBy("title", "asc")
      .limit(20)
      .offset(0),
  );

  return results;
}
// #endregion combining-svelte
