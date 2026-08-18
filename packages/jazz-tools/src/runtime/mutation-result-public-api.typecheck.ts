import type { Db, MutationResult, TableProxy, TransactionKind } from "../index.js";

type Todo = { id: string; title: string; done: boolean };
type TodoInit = { title: string; done: boolean };

declare const db: Db;
declare const todos: TableProxy<Todo, TodoInit>;

async function assertMutationResultContract() {
  const inserted: MutationResult<Todo> = db.insert(todos, { title: "todo", done: false });
  const restored: MutationResult<Todo> = db.restore(todos, "todo-1", {
    title: "todo",
    done: false,
  });
  const updated: MutationResult<void> = db.update(todos, "todo-1", { done: true });
  const upserted: MutationResult<void> = db.upsert(todos, "todo-1", { title: "todo", done: false });
  const deleted: MutationResult<void> = db.delete(todos, "todo-1");

  inserted.wait({ tier: "local" });
  // @ts-expect-error Mergeable mutations require a durability tier when waiting.
  inserted.wait();

  const callbackResult: MutationResult<string> = await db.transaction((tx) => {
    const row: Todo = tx.insert(todos, { title: "todo", done: false });
    const _voidUpdate: void = tx.update(todos, row.id, { done: true });
    return row.id;
  });
  callbackResult.wait({ tier: "edge" });

  const exclusiveResult: MutationResult<string, "exclusive"> = await db.exclusiveTransaction(
    () => "committed",
  );
  exclusiveResult.wait();
  // @ts-expect-error Exclusive mutations are confirmed by the authority without a tier.
  exclusiveResult.wait({ tier: "global" });

  const mergeableCommit: MutationResult<void> = await db.beginTransaction().commit();
  const exclusiveCommit: MutationResult<void, "exclusive"> = await db
    .beginExclusiveTransaction()
    .commit();

  void restored;
  void updated;
  void upserted;
  void deleted;
  void mergeableCommit;
  void exclusiveCommit;
}

declare const genericResult: MutationResult<void, TransactionKind>;
// @ts-expect-error A widened transaction kind must supply a durability tier.
genericResult.wait();
genericResult.wait({ tier: "global" });

void assertMutationResultContract;
