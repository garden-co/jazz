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

// #region reading-durability-tier-ts
export async function readTodosAtEdgeDurability(db: Db) {
  return db.all(app.todos.where({ done: false }), { tier: "edge", localUpdates: "immediate" });
}
// #endregion reading-durability-tier-ts

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

// #region reading-select-ts
export async function readTodoTitlesWithSelectedProject(db: Db) {
  return db.all(
    app.todos
      .select("title")
      .where({ done: false })
      .include({ project: app.projects.select("name") }),
  );
}
// #endregion reading-select-ts

// #region reading-magic-columns-ts
export async function readTodoPermissionIntrospection(db: Db) {
  return db.all(
    app.todos.select("title", "$canRead", "$canEdit", "$canDelete").orderBy("title", "asc"),
  );
}

export async function readEditableTodos(db: Db) {
  return db.all(app.todos.where({ $canEdit: true }).select("title", "$canEdit"));
}

export async function readDeletableTodos(db: Db) {
  return db.all(app.todos.where({ $canDelete: true }).select("title", "$canDelete"));
}
// #endregion reading-magic-columns-ts

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
export async function writeTodoCrud(db: Db, todoId: string) {
  db.insert(app.todos, {
    title: "Write docs",
    done: false,
    owner_id: EXAMPLE_OWNER_ID,
    project: EXAMPLE_PROJECT_ID,
  });
  db.update(app.todos, todoId, { done: true });
  db.delete(app.todos, todoId);
}
// #endregion writing-crud-ts

// #region writing-durability-tier-ts
export async function writeTodoWithDurabilityTiers(db: Db) {
  const { id } = await db.insertDurable(
    app.todos,
    {
      title: "Write docs with durability tier",
      done: false,
      owner_id: EXAMPLE_OWNER_ID,
      project: EXAMPLE_PROJECT_ID,
    },
    { tier: "edge" },
  );

  await db.updateDurable(app.todos, id, { done: true }, { tier: "global" });
  await db.deleteDurable(app.todos, id, { tier: "global" });
}
// #endregion writing-durability-tier-ts
