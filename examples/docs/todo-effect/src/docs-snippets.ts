import { Data, Effect, Option, Schedule, Stream } from "effect";
import { schema as s, ReadTier, type RowAuthor } from "jazz-tools";
import { Jazz } from "jazz-tools/effect";
import { app } from "../schema.js";

const EXAMPLE_PROJECT_ID = "00000000-0000-0000-0000-000000000000";
const EXAMPLE_OWNER_ID = "00000000-0000-0000-0000-00000000000a";
const todoIdA = "00000000-0000-0000-0000-000000000001";
const todoIdB = "00000000-0000-0000-0000-000000000002";

class TodoNotFound extends Data.TaggedError("TodoNotFound")<{ readonly id: string }> {}

// #region reading-oneshot-effect
export const readTodosOneshot = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(app.todos.where({ done: false }));
});
// #endregion reading-oneshot-effect

// #region reading-subscriptions-effect
// Every emission is the complete current result. The subscription is removed
// when the stream ends or the fiber running it is interrupted.
export const logOpenTodoCount = Effect.gen(function* () {
  const jazz = yield* Jazz;
  yield* jazz
    .stream(app.todos.where({ done: false }))
    .pipe(Stream.runForEach((todos) => Effect.log(`${todos.length} open todos`)));
});
// #endregion reading-subscriptions-effect

// #region where-subscription-effect
// A Stream of the open todos, re-emitted whenever they change.
export const openTodos = Stream.unwrap(
  Jazz.useSync((jazz) => jazz.stream(app.todos.where({ done: false }))),
);
// #endregion where-subscription-effect

// #region reading-durability-tier-effect
export const readTodosAtEdgeDurability = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(app.todos.where({ done: false }), { tier: ReadTier.Remote });
});
// #endregion reading-durability-tier-effect

// #region reading-filters-effect
export const readTodosWithFilters = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(app.todos.where({ done: false, title: { contains: "docs" } }));
});
// #endregion reading-filters-effect

// #region reading-where-operators-effect
export const readTodosWithWhereOperators = Effect.gen(function* () {
  const jazz = yield* Jazz;
  yield* jazz.all(app.todos.where({ done: false }));
  yield* jazz.all(app.todos.where({ title: { contains: "milk" } }));
  yield* jazz.all(app.todos.where({ projectId: { ne: EXAMPLE_PROJECT_ID } }));
});
// #endregion reading-where-operators-effect

export const whereOperatorExamples = Effect.gen(function* () {
  const jazz = yield* Jazz;
  const searchTerm = "milk";

  // #region where-eq-ne-effect
  // Exact match (shorthand — no operator object needed)
  const incompleteTodos = yield* jazz.all(app.todos.where({ done: false }));

  // Not equal
  const nonDraftTodos = yield* jazz.all(app.todos.where({ title: { ne: "Draft" } }));

  // One of a set
  const selectedTodos = yield* jazz.all(app.todos.where({ id: { in: [todoIdA, todoIdB] } }));
  // #endregion where-eq-ne-effect

  // #region where-numeric-effect
  const oneWeekAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

  const recentTodos = yield* jazz.all(app.todos.where({ $createdAt: { gt: oneWeekAgo } }));
  const highPriority = yield* jazz.all(app.todos.where({ priority: { gte: 3 } }));
  const lowPriority = yield* jazz.all(app.todos.where({ priority: { lt: 10 } }));
  // #endregion where-numeric-effect

  // #region where-contains-effect
  // Substring match (case-sensitive)
  const matches = yield* jazz.all(app.todos.where({ title: { contains: searchTerm } }));
  // #endregion where-contains-effect

  // #region where-null-effect
  // Rows where the optional ref is not set
  const unlinkedTodos = yield* jazz.all(app.todos.where({ parentId: { isNull: true } }));

  // Rows where it is set
  const linkedTodos = yield* jazz.all(app.todos.where({ parentId: { isNull: false } }));
  // #endregion where-null-effect

  // #region where-and-effect
  // done AND assigned to a project
  const doneWithProject = yield* jazz.all(
    app.todos.where({
      done: true,
      projectId: { isNull: false },
    }),
  );
  // #endregion where-and-effect

  // #region where-order-limit-effect
  const recentIncomplete = yield* jazz.all(
    app.todos.where({ done: false }).orderBy("$createdAt", "asc").limit(50),
  );
  // #endregion where-order-limit-effect

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
});

// #region reading-sorting-effect
export const readTodosSortedByTitle = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(app.todos.where({ done: false }).orderBy("title", "asc"));
});
// #endregion reading-sorting-effect

// #region reading-pagination-effect
export const readTodoPage = Effect.fn("readTodoPage")(function* (page: number, pageSize = 20) {
  const jazz = yield* Jazz;
  const offset = Math.max(0, (page - 1) * pageSize);
  return yield* jazz.all(
    app.todos
      .where({ done: false })
      .orderBy("title", "asc")
      .orderBy("id", "asc")
      .limit(pageSize)
      .offset(offset),
  );
});
// #endregion reading-pagination-effect

// #region reading-includes-effect
export const readTodosWithIncludes = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(
    app.todos.where({ done: false }).include({ project: true, parent: { project: true } }),
  );
});
// #endregion reading-includes-effect

// #region reading-select-effect
export const readTodoTitlesWithSelectedProject = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(
    app.todos
      .select("title")
      .where({ done: false })
      .include({ project: app.projects.select("name") }),
  );
});
// #endregion reading-select-effect

// #region dry-run-permissions-effect
export const canReadTodo = (todoId: string) => Jazz.use((jazz) => jazz.canRead(app.todos, todoId));

export const readTodosWithDeletePermission = Effect.gen(function* () {
  const jazz = yield* Jazz;
  const todos = yield* jazz.all(app.todos.select("id", "title").orderBy("title", "asc"));
  return yield* Effect.filter(
    todos,
    (todo) => Effect.map(jazz.canDelete(app.todos, todo.id), (advice) => advice === "allowed"),
    { concurrency: "unbounded" },
  );
});

export const readEditableTodos = Effect.gen(function* () {
  const jazz = yield* Jazz;
  const todos = yield* jazz.all(app.todos.select("id", "title").orderBy("title", "asc"));
  return yield* Effect.filter(
    todos,
    (todo) =>
      Effect.map(
        jazz.canUpdate(app.todos, todo.id, { title: todo.title }),
        (advice) => advice === "allowed",
      ),
    { concurrency: "unbounded" },
  );
});

export const canCreateTodo = (title: string) =>
  Jazz.use((jazz) => jazz.canInsert(app.todos, { title, done: false }));
// #endregion dry-run-permissions-effect

// #region reading-edit-metadata-magic-columns-effect
export const readTodoEditMetadata = Effect.fn("readTodoEditMetadata")(function* (
  author: RowAuthor,
  updatedSinceMs: number,
) {
  const jazz = yield* Jazz;
  return yield* jazz.all(
    app.todos
      .where({
        $createdBy: author,
        $updatedAt: { gt: updatedSinceMs },
      })
      .select("title", "$createdBy", "$createdAt", "$updatedBy", "$updatedAt"),
  );
});
// #endregion reading-edit-metadata-magic-columns-effect

// #region reading-reverse-relation-effect
export const readProjectsWithTodos = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(app.projects.include({ todos: app.todos.where({ done: false }) }));
});
// #endregion reading-reverse-relation-effect

// #region reading-require-includes-effect
const requiredReferences = s.defineApp({
  customers: s.table({ name: s.string() }, { orders: s.reverse("orders", "customer") }),
  orders: s.table({ customerId: s.uuid() }, { customer: s.rel("customers", "customerId") }),
});

export const readOrdersWithRequiredCustomer = Effect.gen(function* () {
  const jazz = yield* Jazz;
  return yield* jazz.all(requiredReferences.orders.include({ customer: true }).requireIncludes());
});
// #endregion reading-require-includes-effect

// #region reading-seeding-effect
export const seedDefaultProject = Effect.gen(function* () {
  const jazz = yield* Jazz;
  // Wait for the global core before reading — prevents duplicate seeding
  // from concurrent fresh clients on first visit.
  const existing = yield* jazz.all(app.projects, { tier: "global" });

  if (existing.length === 0) {
    yield* jazz.insert(app.projects, { name: "Default" });
  }
});
// #endregion reading-seeding-effect

// #region writing-crud-effect
export const writeTodoCrud = Effect.fn("writeTodoCrud")(function* (todoId: string) {
  const jazz = yield* Jazz;
  yield* jazz.insert(app.todos, {
    title: "Write docs",
    done: false,
    owner_id: EXAMPLE_OWNER_ID,
    projectId: EXAMPLE_PROJECT_ID,
  });
  yield* jazz.update(app.todos, todoId, { done: true });
  yield* jazz.delete(app.todos, todoId);
});
// #endregion writing-crud-effect

// #region writing-upsert-effect
export const upsertTodo = Effect.fn("upsertTodo")(function* (importedTodoId: string) {
  const jazz = yield* Jazz;
  yield* jazz.upsert(
    app.todos,
    importedTodoId,
    { title: "Imported task", done: false },
    { wait: "global" },
  );
});
// #endregion writing-upsert-effect

// #region writing-restore-effect
export const restoreDeletedTodo = Effect.fn("restoreDeletedTodo")(function* (todoId: string) {
  const jazz = yield* Jazz;
  yield* jazz.delete(app.todos, todoId);

  const deletedTodo = yield* jazz.one(app.todos.where({ id: todoId }).includeDeleted());
  if (Option.isNone(deletedTodo)) return yield* new TodoNotFound({ id: todoId });

  return yield* jazz.restore(app.todos, todoId, {
    title: "Restored task",
    done: false,
    owner_id: EXAMPLE_OWNER_ID,
    projectId: EXAMPLE_PROJECT_ID,
  });
});
// #endregion writing-restore-effect

// #region writing-nullable-update-effect
export const clearNullableTodoFields = Effect.fn("clearNullableTodoFields")(function* (
  todoId: string,
) {
  const jazz = yield* Jazz;
  yield* jazz.update(app.todos, todoId, { owner_id: null }); // clears the nullable FK
  yield* jazz.update(app.todos, todoId, { description: undefined }); // leaves the field unchanged
});
// #endregion writing-nullable-update-effect

// #region writing-durability-tier-effect
export const writeTodoWithDurabilityTiers = Effect.gen(function* () {
  const jazz = yield* Jazz;
  const { id } = yield* jazz.insert(
    app.todos,
    {
      title: "Write docs with durability tier",
      done: false,
      owner_id: EXAMPLE_OWNER_ID,
      projectId: EXAMPLE_PROJECT_ID,
    },
    { wait: "global" },
  );

  yield* jazz.update(app.todos, id, { done: true }, { wait: "global" });
  yield* jazz.delete(app.todos, id, { wait: "global" });
});
// #endregion writing-durability-tier-effect

// #region writing-mutation-errors-effect
export const insertTodoAndWait = Effect.gen(function* () {
  const jazz = yield* Jazz;
  const row = yield* jazz.insert(
    app.todos,
    {
      title: "Ship review fixes",
      done: false,
      owner_id: EXAMPLE_OWNER_ID,
      projectId: EXAMPLE_PROJECT_ID,
    },
    { wait: "global" },
  );
  yield* Effect.log(row.id);
}).pipe(
  // The server rejected the write; it has already been undone locally.
  Effect.catchTag("JazzWriteRejected", (rejected) =>
    Effect.logError(rejected.code, rejected.reason),
  ),
);
// #endregion writing-mutation-errors-effect

// #region writing-mutation-error-listener-effect
// Rejections of writes nobody waited for. Fork this for the app's lifetime.
export const logMutationErrors = Effect.gen(function* () {
  const jazz = yield* Jazz;
  yield* jazz.mutationErrors.pipe(
    Stream.runForEach((event) => Effect.logError("DB mutation failed:", event.code, event.reason)),
  );
});
// #endregion writing-mutation-error-listener-effect

// #region writing-transaction-effect
export const groupTodoWrites = Effect.fn("groupTodoWrites")(function* (existingTodoId: string) {
  const jazz = yield* Jazz;
  // Commits when the inner Effect succeeds; rolls back if it fails, dies or is interrupted.
  return yield* jazz.transaction(
    (tx) =>
      Effect.gen(function* () {
        const created = yield* tx.insert(app.todos, {
          title: "Write transaction docs",
          done: false,
          owner_id: EXAMPLE_OWNER_ID,
          projectId: EXAMPLE_PROJECT_ID,
        });

        yield* tx.update(app.todos, existingTodoId, { done: true });

        const staged = yield* tx.one(app.todos.where({ id: created.id }));
        if (Option.isNone(staged)) return yield* new TodoNotFound({ id: created.id });

        return staged.value.id;
      }),
    { wait: "global" },
  );
});
// #endregion writing-transaction-effect

// #region writing-exclusive-transaction-effect
export const finishTodoExclusively = Effect.fn("finishTodoExclusively")(function* (todoId: string) {
  const jazz = yield* Jazz;
  // Waits for the authority's verdict. A conflict fails with JazzWriteRejected,
  // so the whole transaction can simply be retried.
  return yield* jazz
    .exclusiveTransaction((tx) =>
      Effect.gen(function* () {
        const todo = yield* tx.one(app.todos.where({ id: todoId }));
        if (Option.isNone(todo)) return yield* new TodoNotFound({ id: todoId });

        yield* tx.update(app.todos, todo.value.id, { done: true });
        return todo.value.id;
      }),
    )
    .pipe(
      Effect.retry({
        while: (error) => error._tag === "JazzWriteRejected",
        schedule: Schedule.recurs(3),
      }),
    );
});
// #endregion writing-exclusive-transaction-effect

// #region writing-transaction-errors-effect
export const completeTodoInTransaction = Effect.fn("completeTodoInTransaction")(function* (
  todoId: string,
) {
  const jazz = yield* Jazz;
  yield* jazz
    .transaction((tx) => tx.update(app.todos, todoId, { done: true }), { wait: "global" })
    .pipe(
      Effect.catchTag("JazzWriteRejected", (rejected) =>
        Effect.logError(rejected.code, rejected.reason),
      ),
    );
});
// #endregion writing-transaction-errors-effect
