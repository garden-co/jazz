import type {
  Db,
  ExclusiveWriteHandle,
  ExclusiveWriteResult,
  TableProxy,
  WriteHandle,
  WriteResult,
  StreamingWritePlan,
} from "../index.js";
// @ts-expect-error `BatchId` was removed from the runtime transaction API.
import type { BatchId as _RemovedBatchId } from "./client.js";
// @ts-expect-error `OpenBatchId` was removed from the runtime transaction API.
import type { OpenBatchId as _RemovedOpenBatchId } from "./client.js";

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
  const planned: ExclusiveWriteResult<string> = await db.streamingTransaction(
    (plan: StreamingWritePlan) => {
      const ordinary: { id: string } = plan.insert(todos, { title: "scope", done: false });
      const file: { id: string } = plan.insertStreaming(todos, {
        title: new ReadableStream<string>(),
        done: false,
      });
      plan.updateStreaming(todos, ordinary.id, { title: new ReadableStream<string>() });
      plan.upsertStreaming(todos, file.id, { title: new ReadableStream<string>(), done: true });
      // @ts-expect-error Declaration plans have no reads or open native transactions.
      plan.one(todos);
      // @ts-expect-error Declaration plans cannot be committed by application code.
      plan.commit();
      // @ts-expect-error Declarations expose stable IDs, not provisional plaintext rows.
      void ordinary.title;
      // @ts-expect-error Required ordinary insert fields remain required.
      plan.insertStreaming(todos, { title: new ReadableStream<string>() });
      return file.id;
    },
  );
  const plannedValue: string = await planned.wait({ tier: "global" });
  void plannedValue;

  // @ts-expect-error Every required non-streamed column remains required.
  db.insertStreaming(todos, { title: new ReadableStream() });
  // @ts-expect-error A streaming insert must put its source in the derived streamed column.
  db.insertStreaming(todos, { title: "todo", done: false });

  const txId: Promise<string> = inserted.txId;
  // @ts-expect-error `txId` is the single public committed-write identity.
  void inserted.batchId;
  // @ts-expect-error `transactionId` was an obsolete compatibility alias.
  void inserted.transactionId;

  inserted.wait({ tier: "local" });
  // @ts-expect-error Mergeable mutations require a durability tier when waiting.
  inserted.wait();

  const callbackResult: WriteResult<string> = await db.transaction((tx) => {
    const row: Todo = tx.insert(todos, { title: "todo", done: false });
    const _voidUpdate: void = tx.update(todos, row.id, { done: true });
    return row.id;
  });
  callbackResult.wait({ tier: "global" });

  const exclusiveResult: ExclusiveWriteResult<string> = await db.exclusiveTransaction(
    () => "committed",
  );
  exclusiveResult.wait();
  // Explicit global waits require authority confirmation, including while offline.
  exclusiveResult.wait({ tier: "global" });

  const mergeableCommit: WriteHandle = db.beginTransaction().commit();
  const exclusiveCommit: ExclusiveWriteHandle = db.beginExclusiveTransaction().commit();
  exclusiveCommit.wait();
  exclusiveCommit.wait({ tier: "global" });

  void restored;
  void updated;
  void upserted;
  void deleted;
  void streamed;
  void streamedUpdate;
  void streamedUpsert;
  void txId;
  void mergeableCommit;
  void exclusiveCommit;
}

void assertWriteHandleContract;
