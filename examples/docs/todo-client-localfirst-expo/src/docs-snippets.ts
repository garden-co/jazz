import type { Db } from "jazz-tools/react-native";
import { app } from "../schema/app";

const EXAMPLE_TODO_ID = "00000000-0000-0000-0000-000000000000";
const EXAMPLE_PROJECT_ID = "00000000-0000-0000-0000-000000000000";
const EXAMPLE_OWNER_ID = "local:example-owner";

// #region oneshot-expo
export async function readTodosOneshot(db: Db) {
  const todos = await db.all(app.todos.where({ done: false }));
  const todo = await db.one(app.todos.where({ id: EXAMPLE_TODO_ID }));
  return { todos, todo };
}
// #endregion oneshot-expo

// #region subscribe-expo
export function subscribeTodos(db: Db, onUpdate: (results: unknown[]) => void) {
  const unsubscribe = db.subscribeAll(app.todos.where({ done: false }), ({ all }) => {
    onUpdate(all);
  });

  return unsubscribe;
}
// #endregion subscribe-expo

// #region where-operators-expo
export async function whereExamples(db: Db) {
  // Equality (shorthand)
  await db.all(app.todos.where({ done: false }));

  // Explicit operators
  await db.all(app.todos.where({ title: { contains: "milk" } }));
  await db.all(app.todos.where({ project: { ne: EXAMPLE_PROJECT_ID } }));
}
// #endregion where-operators-expo

// #region includes-expo
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
// #endregion includes-expo

// #region chaining-expo
export async function chainingExamples(db: Db) {
  // Multiple where calls produce AND semantics
  const results = await db.all(
    app.todos.where({ done: false }).where({ title: { contains: "docs" } }),
  );

  return results;
}
// #endregion chaining-expo

// #region combining-expo
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
// #endregion combining-expo

// #region reading-tier-expo
export function subscribeTodosAtEdge(db: Db, onCount: (count: number) => void) {
  return db.subscribeAll(app.todos.where({ done: false }), ({ all }) => onCount(all.length), {
    tier: "edge",
    localUpdates: "immediate",
  });
}
// #endregion reading-tier-expo

// #region writing-durability-expo
export async function writeWithDurabilityTier(db: Db, todoTitle: string) {
  const { id } = await db.insertDurable(
    app.todos,
    {
      title: todoTitle,
      done: false,
      owner_id: EXAMPLE_OWNER_ID,
      project: EXAMPLE_PROJECT_ID,
    },
    { tier: "worker" },
  );

  await db.updateDurable(app.todos, id, { done: true }, { tier: "worker" });
  await db.deleteDurable(app.todos, id, { tier: "worker" });
}
// #endregion writing-durability-expo
