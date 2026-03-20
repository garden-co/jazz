import type { Db } from "jazz-tools";
import { app } from "../schema.js";

const EXAMPLE_PROJECT_ID = "00000000-0000-0000-0000-000000000000";
const EXAMPLE_OWNER_ID = "local:example-owner";
const todoIdA = "00000000-0000-0000-0000-000000000001";
const todoIdB = "00000000-0000-0000-0000-000000000002";

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

// #region where-subscription-ts
export function subscribeOpenTodos(db: Db, onChange: (todos: unknown[]) => void) {
  return db.subscribeAll(app.todos.where({ done: false }), ({ all }) => onChange(all));
}
// #endregion where-subscription-ts

// #region reading-durability-tier-ts
export async function readTodosAtEdgeDurability(db: Db) {
  return db.all(app.todos.where({ done: false }), { tier: "edge", localUpdates: "immediate" });
}
// #endregion reading-durability-tier-ts

// #region reading-composing-queries-ts
// Store a base query and reuse it for different views.
const openTodos = app.todos.where({ done: false });

const byNewest = openTodos.orderBy("id", "desc");
const byTitle = openTodos.orderBy("title", "asc").limit(20);
const urgent = openTodos.where({ title: { contains: "urgent" } });
// #endregion reading-composing-queries-ts

// #region reading-chained-query-ts
const incompleteTodos = app.todos.where({ done: false }).orderBy("title", "asc").limit(50);
// #endregion reading-chained-query-ts

// #region reading-filters-ts
export async function readTodosWithFilters(db: Db) {
  return db.all(app.todos.where({ done: false, title: { contains: "docs" } }));
}
// #endregion reading-filters-ts

// #region reading-where-operators-ts
export async function readTodosWithWhereOperators(db: Db) {
  await db.all(app.todos.where({ done: false }));
  await db.all(app.todos.where({ title: { contains: "milk" } }));
  await db.all(app.todos.where({ projectId: { ne: EXAMPLE_PROJECT_ID } }));
}
// #endregion reading-where-operators-ts

export async function whereOperatorExamples(db: Db) {
  const searchTerm = "milk";

  // #region where-eq-ne-ts
  // Exact match (shorthand — no operator object needed)
  const incompleteTodos = await db.all(app.todos.where({ done: false }));

  // Not equal
  const nonDraftTodos = await db.all(app.todos.where({ title: { ne: "Draft" } }));

  // One of a set
  const selectedTodos = await db.all(app.todos.where({ id: { in: [todoIdA, todoIdB] } }));
  // #endregion where-eq-ne-ts

  // #region where-numeric-ts
  const oneWeekAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

  const recentTodos = await db.all(app.todos.where({ created_at: { gt: oneWeekAgo } }));
  const highPriority = await db.all(app.todos.where({ priority: { gte: 3 } }));
  const lowPriority = await db.all(app.todos.where({ priority: { lt: 10 } }));
  // #endregion where-numeric-ts

  // #region where-contains-ts
  // Substring match (case-sensitive)
  const matches = await db.all(app.todos.where({ title: { contains: searchTerm } }));
  // #endregion where-contains-ts

  // #region where-null-ts
  // Rows where the optional ref is not set
  const unlinkedTodos = await db.all(app.todos.where({ parentId: { isNull: true } }));

  // Rows where it is set
  const linkedTodos = await db.all(app.todos.where({ parentId: { isNull: false } }));
  // #endregion where-null-ts

  // #region where-and-ts
  // done AND assigned to a project
  const doneWithProject = await db.all(
    app.todos.where({
      done: true,
      projectId: { isNull: false },
    }),
  );
  // #endregion where-and-ts

  // #region where-order-limit-ts
  const recentIncomplete = await db.all(
    app.todos.where({ done: false }).orderBy("created_at", "asc").limit(50),
  );
  // #endregion where-order-limit-ts

  return {
    incompleteTodos,
    nonDraftTodos,
    selectedTodos,
    recentTodos,
    highPriority,
    lowPriority,
    matches,
    unlinkedTodos,
    linkedTodos,
    doneWithProject,
    recentIncomplete,
  };
}

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

export async function readTodosWithDeletePermission(db: Db) {
  return db.all(app.todos.select("*", "$canDelete").orderBy("title", "asc"));
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
    step: ({ current }) => app.todos.where({ parentId: current }).hopTo("parent"),
    maxDepth: 10,
  });
}
// #endregion reading-recursive-ts

// #region reading-seeding-ts
export async function seedDefaultProject(db: Db) {
  // Wait for the global core before reading — prevents duplicate seeding
  // from concurrent fresh clients on first visit.
  const existing = await db.all(app.projects, { tier: "global" });

  if (existing.length === 0) {
    db.insert(app.projects, { name: "Default" });
  }
}
// #endregion reading-seeding-ts

// #region writing-crud-ts
export async function writeTodoCrud(db: Db, todoId: string) {
  db.insert(app.todos, {
    title: "Write docs",
    done: false,
    ownerId: EXAMPLE_OWNER_ID,
    projectId: EXAMPLE_PROJECT_ID,
  });
  db.update(app.todos, todoId, { done: true });
  db.delete(app.todos, todoId);
}
// #endregion writing-crud-ts

// #region writing-nullable-update-ts
export function clearNullableTodoFields(db: Db, todoId: string) {
  db.update(app.todos, todoId, { ownerId: null }); // clears the nullable FK
  db.update(app.todos, todoId, { description: undefined }); // leaves the field unchanged
}
// #endregion writing-nullable-update-ts

// #region writing-durability-tier-ts
export async function writeTodoWithDurabilityTiers(db: Db) {
  const { id } = await db.insertDurable(
    app.todos,
    {
      title: "Write docs with durability tier",
      done: false,
      ownerId: EXAMPLE_OWNER_ID,
      projectId: EXAMPLE_PROJECT_ID,
    },
    { tier: "edge" },
  );

  await db.updateDurable(app.todos, id, { done: true }, { tier: "global" });
  await db.deleteDurable(app.todos, id, { tier: "global" });
}
// #endregion writing-durability-tier-ts

// #region chaining-ts
export async function chainingExamples(db: Db) {
  // Multiple where calls produce AND semantics
  const results = await db.all(
    app.todos.where({ done: false }).where({ title: { contains: "docs" } }),
  );

  return results;
}
// #endregion chaining-ts

// #region combining-ts
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
// #endregion combining-ts
