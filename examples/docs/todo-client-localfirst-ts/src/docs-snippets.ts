import type { Db, DbTransaction } from "jazz-tools";
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

// The query objects above are illustrative snippet fragments not referenced
// elsewhere; re-export them so they count as used.
export { byNewest, byTitle, urgent, incompleteTodos };

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

// #region reading-magic-columns-include-ts
export async function readProjectsWithTodoPermissions(db: Db) {
  return db.all(
    app.projects.include({
      todosViaProject: app.todos.select("title", "$canEdit", "$canDelete").orderBy("title", "asc"),
    }),
  );
}
// #endregion reading-magic-columns-include-ts

// #region reading-edit-metadata-magic-columns-ts
export async function readTodoEditMetadata(db: Db, currentUserId: string, updatedSinceMs: number) {
  return db.all(
    app.todos
      .where({
        $createdBy: currentUserId,
        $updatedAt: { gt: updatedSinceMs },
      })
      .select("title", "$createdBy", "$createdAt", "$updatedBy", "$updatedAt"),
  );
}
// #endregion reading-edit-metadata-magic-columns-ts

// #region reading-reverse-relation-ts
export async function readProjectsWithTodos(db: Db) {
  return db.all(app.projects.include({ todosViaProject: app.todos.where({ done: false }) }));
}
// #endregion reading-reverse-relation-ts

// #region reading-require-includes-ts
export async function readTodosWithRequiredProject(db: Db) {
  return db.all(app.todos.where({ done: false }).include({ project: true }).requireIncludes());
}
// #endregion reading-require-includes-ts

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
    owner_id: EXAMPLE_OWNER_ID,
    projectId: EXAMPLE_PROJECT_ID,
  });
  db.update(app.todos, todoId, { done: true });
  db.delete(app.todos, todoId);
}
// #endregion writing-crud-ts

// #region writing-nullable-update-ts
export function clearNullableTodoFields(db: Db, todoId: string) {
  db.update(app.todos, todoId, { owner_id: null }); // clears the nullable FK
  db.update(app.todos, todoId, { description: undefined }); // leaves the field unchanged
}
// #endregion writing-nullable-update-ts

// #region writing-durability-tier-ts
export async function writeTodoWithDurabilityTiers(db: Db) {
  const { id } = await db
    .insert(app.todos, {
      title: "Write docs with durability tier",
      done: false,
      owner_id: EXAMPLE_OWNER_ID,
      projectId: EXAMPLE_PROJECT_ID,
    })
    .wait({ tier: "edge" });

  await db.update(app.todos, id, { done: true }).wait({ tier: "global" });
  await db.delete(app.todos, id).wait({ tier: "global" });
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

// #region writing-tx-isolation-ts
export async function transactionIsolation(db: Db) {
  const tx = db.beginTransaction();
  tx.insert(app.todos, { title: "Draft launch post", done: false });

  // Using the tx handle here means we can read the staged write
  const stagedInTx = await tx.all(app.todos.where({ done: false }));

  // Using the db handle only shows us the committed state, so the new todo is still invisible.
  const visibleOutside = await db.all(app.todos.where({ done: false }));

  tx.commit();

  // After commit the write is visible to ordinary reads too.
  const visibleAfterCommit = await db.all(app.todos.where({ done: false }));

  return { stagedInTx, visibleOutside, visibleAfterCommit };
}
// #endregion writing-tx-isolation-ts

// #region writing-tx-concurrent-ts
export async function concurrentTransactionIsolation(db: Db) {
  const aliceTx = db.beginTransaction();
  const bobTx = db.beginTransaction();

  aliceTx.insert(app.todos, { title: "Alice draft", done: false });
  bobTx.insert(app.todos, { title: "Bob draft", done: false });

  // Open transactions never observe each other's staged writes —
  // each list contains only that transaction's own draft.
  const aliceSees = await aliceTx.all(app.todos.where({ done: false }));
  const bobSees = await bobTx.all(app.todos.where({ done: false }));

  aliceTx.rollback(); // staged write discarded — visible to no reader
  bobTx.commit();

  // Only Bob's committed draft is visible to indexed reads.
  const visible = await db.all(app.todos.where({ done: false }));

  return { aliceSees, bobSees, visible };
}
// #endregion writing-tx-concurrent-ts

// #region writing-tx-commit-visibility-ts
export async function directBatchCommitVisibility(db: Db) {
  const batch = db.beginBatch();
  batch.insert(app.todos, { title: "Grouped write", done: false });

  // Staged writes are invisible to indexed reads while the batch is open.
  const beforeCommit = await db.all(app.todos.where({ done: false }));

  batch.commit();

  // A direct batch is visible to indexed reads as soon as it commits —
  // there is no authority round-trip. A transaction instead becomes
  // globally visible only once the authority accepts it; when connected
  // to a sync server, await result.wait({ tier }) before relying on that.
  const afterCommit = await db.all(app.todos.where({ done: false }));

  return { beforeCommit, afterCommit };
}
// #endregion writing-tx-commit-visibility-ts

// #region writing-tx-callback-ts
export async function markAllOpenToDosAsDone(db: Db) {
  // The callback may be async. Return a value and it flows through
  // the WriteResult. Throw and the whole transaction rolls back.
  const result = await db.transaction(async (tx) => {
    const open = await tx.all(app.todos.where({ done: false }));
    for (const todo of open) {
      tx.update(app.todos, todo.id, { done: true });
    }
    return open.length;
  });

  return { closedCount: result.value, batchId: result.batchId };
}
// #endregion writing-tx-callback-ts

// #region writing-tx-explicit-ts
export function importProjectPlan(db: Db, plan: { name: string; tasks: string[] }) {
  const tx = db.beginTransaction();
  try {
    const projectId = stageProject(tx, plan.name);
    for (const title of plan.tasks) {
      stageTask(tx, projectId, title);
    }
    return tx.commit().batchId;
  } catch (error) {
    tx.rollback();
    throw error;
  }
}

function stageProject(tx: DbTransaction, name: string) {
  return tx.insert(app.projects, { name }).id;
}

function stageTask(tx: DbTransaction, projectId: string, title: string) {
  tx.insert(app.todos, { title, done: false, projectId });
}
// #endregion writing-tx-explicit-ts

// #region writing-tx-multitable-ts
export async function createProjectWithFirstTodo(db: Db) {
  // Both rows settle together: neither the project nor its first todo
  // is visible to other reads until the transaction commits.
  const result = db.transaction((tx) => {
    const project = tx.insert(app.projects, { name: "Launch" });
    const todo = tx.insert(app.todos, {
      title: "Write the brief",
      done: false,
      projectId: project.id,
    });
    return { projectId: project.id, todoId: todo.id };
  });

  await result.wait({ tier: "edge" });
  return result.value;
}
// #endregion writing-tx-multitable-ts

// #region writing-tx-test-fixture-ts
// An explicit transaction doubles as a test-isolation primitive: open one
// before each test, roll it back after, and a test's writes never reach the
// next. Wire `open` into beforeEach and `discard` into afterEach.
export function transactionTestFixture(db: Db) {
  let tx: DbTransaction;

  return {
    open: () => {
      tx = db.beginTransaction();
    },
    discard: () => {
      tx.rollback(); // every staged write from this test is thrown away
    },
    addTask: () => {
      const task = tx.insert(app.todos, { title: "Test task", done: false });
      return task.title;
    },
  };
}
// #endregion writing-tx-test-fixture-ts
