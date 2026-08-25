import type {
  Db,
  ExclusiveWriteHandle,
  ExclusiveWriteResult,
  TableProxy,
  WriteHandle,
  WriteResult,
} from "../index.js";

type Todo = { id: string; title: string; done: boolean };
type TodoInit = { title: string; done: boolean };
type TodoStreamingInit =
  | { title: ReadableStream<string | Uint8Array>; done: boolean }
  | { title: AsyncIterable<string | Uint8Array>; done: boolean };
type TodoStreamingUpdate =
  | { title: ReadableStream<string | Uint8Array>; done?: boolean }
  | { title: AsyncIterable<string | Uint8Array>; done?: boolean };

declare const db: Db;
declare const todos: TableProxy<Todo, TodoInit, TodoStreamingInit, TodoStreamingUpdate>;

async function assertWriteHandleContract() {
  const inserted: WriteResult<Todo> = db.insert(todos, { title: "todo", done: false });
  const restored: WriteResult<Todo> = db.restore(todos, "todo-1", {
    title: "todo",
    done: false,
  });
  const updated: WriteHandle = db.update(todos, "todo-1", { done: true });
  const upserted: WriteHandle = db.upsert(todos, "todo-1", { title: "todo", done: false });
  const deleted: WriteHandle = db.delete(todos, "todo-1");
  const streamed: Promise<WriteHandle<{ id: string }>> = db.insertStreaming(todos, {
    done: false,
    title: (async function* () {
      yield "streamed ";
      yield new TextEncoder().encode("title");
    })(),
  });
  const streamedUpdate: Promise<WriteHandle<{ id: string }>> = db.updateStreaming(todos, "todo-1", {
    title: new ReadableStream<string>(),
  });
  const streamedUpsert: Promise<WriteHandle<{ id: string }>> = db.upsertStreaming(todos, "todo-1", {
    title: new ReadableStream<string>(),
    done: true,
  });

  // @ts-expect-error Every required non-streamed column remains required.
  db.insertStreaming(todos, { title: new ReadableStream() });
  // @ts-expect-error A streaming insert must put its source in the derived streamed column.
  db.insertStreaming(todos, { title: "todo", done: false });

  const batchId: Promise<string> = inserted.batchId;

  inserted.wait({ tier: "local" });
  // @ts-expect-error Mergeable mutations require a durability tier when waiting.
  inserted.wait();

  const callbackResult: WriteResult<string> = await db.transaction((tx) => {
    const row: Todo = tx.insert(todos, { title: "todo", done: false });
    const _voidUpdate: void = tx.update(todos, row.id, { done: true });
    return row.id;
  });
  callbackResult.wait({ tier: "edge" });

  const exclusiveResult: ExclusiveWriteResult<string> = await db.exclusiveTransaction(
    () => "committed",
  );
  exclusiveResult.wait();
  // @ts-expect-error Exclusive mutations are confirmed by the authority without a tier.
  exclusiveResult.wait({ tier: "global" });

  const mergeableCommit: WriteHandle = db.beginTransaction().commit();
  const exclusiveCommit: ExclusiveWriteHandle = db.beginExclusiveTransaction().commit();

  void restored;
  void updated;
  void upserted;
  void deleted;
  void streamed;
  void streamedUpdate;
  void streamedUpsert;
  void batchId;
  void mergeableCommit;
  void exclusiveCommit;
}

void assertWriteHandleContract;
