import {
  BrowserWasmAbiSmokeClient,
  openConfig,
  PostcardWriter,
  queryFromTable,
  type DbHandle,
  type Handle,
  type QueryHandle,
  type SubscriptionHandle,
  type WriteHandle,
} from "./abi-smoke-worker-client.js";
import {
  encodedFileCells,
  encodedTodoCells,
  encodedTodoPatch,
  fileViews,
  formatTodos,
  todosSchema,
  todoViews,
  type TodoView,
} from "jazz-tools";
import { openBrowserWebSocketTransport } from "./browser-websocket-transport.js";

export type ScenarioLogger = (line: string) => void;
export type ScenarioSummary = { message: string };

export type ScenarioProgress =
  | { type: "worker-starting" }
  | { type: "worker-ready" }
  | { type: "db-opened"; db: Handle }
  | { type: "query-prepared"; query: Handle }
  | { type: "insert-permission"; allowed: boolean }
  | {
      type: "todo-transition";
      label: "initial" | "insert" | "update" | "delete";
      todos: TodoView[];
    }
  | { type: "write-state"; fate: string; durability: string }
  | { type: "write-durable"; durability: string }
  | { type: "watch-opened"; watch: SubscriptionHandle; current: TodoView[] }
  | { type: "worker-shutdown" };

export type ScenarioProgressHandler = (event: ScenarioProgress) => void;
export type ReloadPersistenceSmokeMode = "reload-write" | "reload-verify";

type StartedWorkerClient = {
  client: BrowserWasmAbiSmokeClient;
};

export function todoSchemaHex(): string {
  return hex(todosSchema());
}

export async function runWorkerBackedTour(
  log: ScenarioLogger,
  progress: ScenarioProgressHandler = () => {},
): Promise<ScenarioSummary> {
  const started = await startWorkerClient(log, "tour", progress);
  try {
    const schema = todosSchema();
    const accountAuthor = new Uint8Array(16).fill(0x77);
    const config = openConfig(new Uint8Array(16).fill(0x42), accountAuthor, 0x2026);
    const db = await openMemoryTodoDb(started.client, schema, config, log, "tour");
    progress({ type: "db-opened", db });

    const query = await prepareTodosQuery(started.client, db, log, "tour");
    progress({ type: "query-prepared", query });

    const opened = await started.client.subscribe(db, query);
    const openedCurrent = todoViews(opened.current);
    progress({ type: "watch-opened", watch: opened.subscription, current: openedCurrent });
    progress({ type: "todo-transition", label: "initial", todos: openedCurrent });
    assertTodoSummaries("initial", openedCurrent, []);

    const todoRowId = new Uint8Array(16).fill(0x7a);
    const initialCells = encodedTodoCells({
      title: "Ship direct WasmDb",
      done: false,
      owner: accountAuthor,
    });
    const allowed = await started.client.canInsertEncoded(db, "todos", initialCells);
    progress({ type: "insert-permission", allowed });
    if (!allowed) throw new Error("expected insert to be allowed");

    await waitForLocalWrite(
      started.client,
      await started.client.insertWithIdEncoded(db, "todos", todoRowId, initialCells),
      "insert",
      progress,
      log,
    );
    const afterInsert = await subscriptionTodos(started.client, opened.subscription);
    progress({ type: "todo-transition", label: "insert", todos: afterInsert });
    assertTodoSummaries("insert", afterInsert, ["Ship direct WasmDb:open"]);

    await waitForLocalWrite(
      started.client,
      await started.client.updateEncoded(db, "todos", todoRowId, encodedTodoPatch({ done: true })),
      "update",
      progress,
      log,
    );
    const afterUpdate = await subscriptionTodos(started.client, opened.subscription);
    progress({ type: "todo-transition", label: "update", todos: afterUpdate });
    assertTodoSummaries("update", afterUpdate, ["Ship direct WasmDb:done"]);

    const ownerRead = await readTodosAs(started.client, db, query, accountAuthor);
    assertTodoSummaries("identity owner read", ownerRead, ["Ship direct WasmDb:done"]);

    const otherAuthor = new Uint8Array(16).fill(0x88);
    const otherRead = await readTodosAs(started.client, db, query, otherAuthor);
    assertTodoSummaries("identity other read", otherRead, []);

    const ownerCanUpdate = await started.client.canUpdateEncodedForIdentity(
      db,
      "todos",
      todoRowId,
      encodedTodoPatch({ title: "Owner dry-run" }),
      accountAuthor,
    );
    if (!ownerCanUpdate) throw new Error("expected owner update dry-run to be allowed");
    const otherCanUpdate = await started.client.canUpdateEncodedForIdentity(
      db,
      "todos",
      todoRowId,
      encodedTodoPatch({ title: "Other dry-run" }),
      otherAuthor,
    );
    if (otherCanUpdate) throw new Error("expected other update dry-run to be denied");

    await waitForLocalWrite(
      started.client,
      await started.client.deleteRow(db, "todos", todoRowId),
      "delete",
      progress,
      log,
    );
    const afterDelete = await subscriptionTodos(started.client, opened.subscription);
    progress({ type: "todo-transition", label: "delete", todos: afterDelete });
    assertTodoSummaries("delete", afterDelete, []);

    await started.client.unsubscribe(opened.subscription);
    await started.client.release(query);
    await started.client.closeDb(db);
    await shutdownWorkerClient(started, log, "tour");
    progress({ type: "worker-shutdown" });
    return { message: "Ready: browser worker ran the direct WasmDb todo flow." };
  } catch (error) {
    await shutdownWorkerClient(started, log, "tour").catch(() => undefined);
    throw error;
  }
}

export async function runReloadPersistenceSmoke(
  mode: ReloadPersistenceSmokeMode,
  namespace: string,
  log: ScenarioLogger,
): Promise<ScenarioSummary> {
  if (namespace.length === 0) throw new Error("reload persistence namespace must not be empty");
  const schema = todosSchema();
  const accountAuthor = new Uint8Array(16).fill(0x77);
  const config = openConfig(new Uint8Array(16).fill(0x42), accountAuthor, 0x2026);
  const todoRowId = new Uint8Array(16).fill(0x7a);

  if (mode === "reload-write") {
    const first = await startWorkerClient(log, "reload write");
    try {
      const db = await openBrowserTodoDb(
        first.client,
        namespace,
        schema,
        config,
        log,
        "reload write",
      );
      const query = await prepareTodosQuery(first.client, db, log, "reload write");
      const cells = encodedTodoCells({
        title: "Survive reload",
        done: false,
        owner: accountAuthor,
      });
      await waitForLocalWrite(
        first.client,
        await first.client.insertWithIdEncoded(db, "todos", todoRowId, cells),
        "reload insert",
        () => {},
        log,
      );
      assertTodoSummaries(
        "reload first read",
        await readTodosAs(first.client, db, query, accountAuthor),
        ["Survive reload:open"],
      );
      await first.client.release(query);
      await first.client.closeDb(db);
      await shutdownWorkerClient(first, log, "reload write");
      return { message: "Ready: reload persistence write flushed through openBrowserDb." };
    } catch (error) {
      await shutdownWorkerClient(first, log, "reload write").catch(() => undefined);
      throw error;
    }
  }

  const second = await startWorkerClient(log, "reload verify");
  try {
    const db = await openBrowserTodoDb(
      second.client,
      namespace,
      schema,
      config,
      log,
      "reload verify",
    );
    const query = await prepareTodosQuery(second.client, db, log, "reload verify");
    const opened = await second.client.subscribe(db, query);
    assertTodoSummaries("reload restored watch", todoViews(opened.current), [
      "Survive reload:open",
    ]);
    await waitForLocalWrite(
      second.client,
      await second.client.updateEncoded(db, "todos", todoRowId, encodedTodoPatch({ done: true })),
      "reload update",
      () => {},
      log,
    );
    assertTodoSummaries(
      "reload updated watch",
      await subscriptionTodos(second.client, opened.subscription),
      ["Survive reload:done"],
    );
    await second.client.unsubscribe(opened.subscription);
    await second.client.release(query);
    await second.client.closeDb(db);
    await second.client.destroyBrowserStorage(namespace);
    await shutdownWorkerClient(second, log, "reload verify");
    return {
      message: "Ready: openBrowserDb reload persistence smoke restored and watched the todo.",
    };
  } catch (error) {
    await second.client.destroyBrowserStorage(namespace).catch(() => undefined);
    await shutdownWorkerClient(second, log, "reload verify").catch(() => undefined);
    throw error;
  }
}

export async function runBrowserStorageConcurrencySmoke(
  namespace: string,
  log: ScenarioLogger,
): Promise<ScenarioSummary> {
  if (namespace.length === 0)
    throw new Error("browser storage concurrency namespace must not be empty");
  const schema = todosSchema();
  const config = openConfig(new Uint8Array(16).fill(0x42), new Uint8Array(16).fill(0x77), 0x2026);
  const first = await startWorkerClient(log, "concurrency first");
  let second: StartedWorkerClient | undefined;
  try {
    const firstDb = await openBrowserTodoDb(
      first.client,
      namespace,
      schema,
      config,
      log,
      "concurrency first",
    );
    second = await startWorkerClient(log, "concurrency second");
    const secondOpen = openBrowserTodoDb(
      second.client,
      namespace,
      schema,
      config,
      log,
      "concurrency second",
    );
    await delay(75);
    await first.client.closeDb(firstDb);
    await shutdownWorkerClient(first, log, "concurrency first");
    const secondDb = await secondOpen;
    await second.client.closeDb(secondDb);
    await second.client.destroyBrowserStorage(namespace);
    await shutdownWorkerClient(second, log, "concurrency second");
    return { message: "Ready: openBrowserDb handled a same-namespace worker handoff." };
  } catch (error) {
    await first.client.destroyBrowserStorage(namespace).catch(() => undefined);
    await shutdownWorkerClient(first, log, "concurrency first").catch(() => undefined);
    if (second)
      await shutdownWorkerClient(second, log, "concurrency second").catch(() => undefined);
    throw error;
  }
}

export async function runBrowserBatchDurabilitySmoke(
  namespace: string,
  log: ScenarioLogger,
): Promise<ScenarioSummary> {
  if (namespace.length === 0)
    throw new Error("browser batch durability namespace must not be empty");
  const schema = todosSchema();
  const accountAuthor = new Uint8Array(16).fill(0x77);
  const config = openConfig(new Uint8Array(16).fill(0x42), accountAuthor, 0x2026);
  const fixtures = [
    { id: new Uint8Array(16).fill(0xb1), title: "Batch durable alpha", done: false },
    { id: new Uint8Array(16).fill(0xb2), title: "Batch durable beta", done: false },
    { id: new Uint8Array(16).fill(0xb3), title: "Batch durable gamma", done: true },
  ];

  const first = await startWorkerClient(log, "batch");
  try {
    const db = await openBrowserTodoDb(first.client, namespace, schema, config, log, "batch");
    const query = await prepareTodosQuery(first.client, db, log, "batch");
    for (const fixture of fixtures) {
      await waitForLocalWrite(
        first.client,
        await first.client.insertWithIdEncoded(
          db,
          "todos",
          fixture.id,
          encodedTodoCells({ title: fixture.title, done: fixture.done, owner: accountAuthor }),
        ),
        `batch insert ${fixture.title}`,
        () => {},
        log,
      );
    }
    assertTodoSummariesIgnoringOrder(
      "batch read",
      await readTodosAs(first.client, db, query, accountAuthor),
      ["Batch durable alpha:open", "Batch durable beta:open", "Batch durable gamma:done"],
    );
    await first.client.release(query);
    await first.client.closeDb(db);
    await first.client.destroyBrowserStorage(namespace);
    await shutdownWorkerClient(first, log, "batch");
    return { message: "Ready: openBrowserDb preserved a multi-write batch." };
  } catch (error) {
    await first.client.destroyBrowserStorage(namespace).catch(() => undefined);
    await shutdownWorkerClient(first, log, "batch").catch(() => undefined);
    throw error;
  }
}

export async function runDbAllByteaOrderSmoke(log: ScenarioLogger): Promise<ScenarioSummary> {
  const started = await startWorkerClient(log, "db.all bytea/order");
  try {
    const schema = todosSchema();
    const accountAuthor = new Uint8Array(16).fill(0x77);
    const config = openConfig(new Uint8Array(16).fill(0x42), accountAuthor, 0x2026);
    const db = await openMemoryTodoDb(started.client, schema, config, log, "db.all bytea/order");
    const fixtures = [
      { id: new Uint8Array(16).fill(0xa1), name: "alpha.bin", data: new Uint8Array([1, 2]) },
      { id: new Uint8Array(16).fill(0xa2), name: "bravo.bin", data: new Uint8Array([3, 4, 5, 6]) },
      { id: new Uint8Array(16).fill(0xa3), name: "charlie.bin", data: new Uint8Array([7, 8, 9]) },
      { id: new Uint8Array(16).fill(0xa4), name: "delta.bin", data: new Uint8Array([10]) },
    ];
    for (const fixture of fixtures) {
      await waitForLocalWrite(
        started.client,
        await started.client.insertWithIdEncoded(
          db,
          "files",
          fixture.id,
          encodedFileCells({
            name: fixture.name,
            mimeType: "application/octet-stream",
            data: fixture.data,
            size: fixture.data.length,
            owner: accountAuthor,
          }),
        ),
        `file insert ${fixture.name}`,
        () => {},
        log,
      );
    }
    const query = await prepareQueryBytes(
      started.client,
      db,
      queryFilesOrderBySizeDescLimitOffset(),
      log,
      "db.all bytea/order",
    );
    const files = fileViews(await started.client.readAllForIdentity(db, query, accountAuthor));
    const actualNames = files.map((file) => file.name);
    const actualBytea = files.map((file) => [...file.data].join(","));
    if (
      actualNames.join("|") !== "charlie.bin|alpha.bin" ||
      actualBytea.join("|") !== "7,8,9|1,2"
    ) {
      throw new Error(
        `unexpected bytea/order limited files: ${actualNames.join(", ")} / ${actualBytea.join(" | ")}`,
      );
    }
    await started.client.release(query);
    await started.client.closeDb(db);
    await shutdownWorkerClient(started, log, "db.all bytea/order");
    return {
      message:
        "Ready: browser direct db.all read returned Bytea rows with order, limit, and offset.",
    };
  } catch (error) {
    await shutdownWorkerClient(started, log, "db.all bytea/order").catch(() => undefined);
    throw error;
  }
}

export async function runWebSocketBoundarySmoke(log: ScenarioLogger): Promise<ScenarioSummary> {
  const started = await startWorkerClient(log, "websocket boundary");
  const socket = new RecordingWebSocket("ws://127.0.0.1:8787/todos/sync");
  try {
    const schema = todosSchema();
    const accountAuthor = new Uint8Array(16).fill(0x77);
    const config = openConfig(new Uint8Array(16).fill(0x42), accountAuthor, 0x2026);
    const db = await openMemoryTodoDb(started.client, schema, config, log, "websocket boundary");
    const sync = await openBrowserWebSocketTransport({
      url: "ws://127.0.0.1:8787/todos/sync",
      client: started.client,
      db,
      identity: accountAuthor,
      WebSocket: class extends RecordingWebSocket {
        constructor(url: string) {
          super(url, socket);
        }
      },
      tickMs: 60_000,
    });
    const cells = encodedTodoCells({
      title: "Browser websocket boundary",
      done: false,
      owner: accountAuthor,
    });
    await waitForLocalWrite(
      started.client,
      await started.client.insertWithIdEncoded(db, "todos", new Uint8Array(16).fill(0x91), cells),
      "websocket insert",
      () => {},
      log,
    );
    for (let attempt = 0; attempt < 20 && socket.sent.length === 0; attempt += 1) {
      await sync.flush();
      await delay(10);
    }
    if (socket.sent.length === 0 || sync.stats.sentFrames === 0)
      throw new Error("browser websocket boundary did not emit any binary wire frames");
    await sync.close();
    await started.client.closeDb(db);
    await shutdownWorkerClient(started, log, "websocket boundary");
    return { message: "Ready: browser websocket boundary emitted opaque direct wire frames." };
  } catch (error) {
    await shutdownWorkerClient(started, log, "websocket boundary").catch(() => undefined);
    throw error;
  }
}

export async function runWebSocketRustSmoke(
  wsUrl: string,
  log: ScenarioLogger,
): Promise<ScenarioSummary> {
  if (!wsUrl.startsWith("ws://") && !wsUrl.startsWith("wss://"))
    throw new Error("websocket rust smoke requires a ws:// or wss:// URL");
  const started = await startWorkerClient(log, "websocket rust");
  try {
    const schema = todosSchema();
    const accountAuthor = new Uint8Array(16).fill(0x77);
    const config = openConfig(new Uint8Array(16).fill(0x42), accountAuthor, 0x2026);
    const db = await openMemoryTodoDb(started.client, schema, config, log, "websocket rust");
    const sync = await openBrowserWebSocketTransport({
      url: wsUrl,
      client: started.client,
      db,
      identity: accountAuthor,
      tickMs: 20,
    });
    const cells = encodedTodoCells({
      title: "Browser to Rust websocket",
      done: false,
      owner: accountAuthor,
    });
    await waitForLocalWrite(
      started.client,
      await started.client.insertWithIdEncoded(db, "todos", new Uint8Array(16).fill(0x92), cells),
      "websocket rust insert",
      () => {},
      log,
    );
    for (
      let attempt = 0;
      attempt < 50 && (sync.stats.sentFrames === 0 || sync.stats.receivedFrames === 0);
      attempt += 1
    ) {
      await sync.flush();
      await delay(20);
    }
    if (sync.stats.sentFrames === 0)
      throw new Error("browser websocket rust smoke did not send any binary wire frames");
    if (sync.stats.receivedFrames === 0)
      throw new Error("browser websocket rust smoke did not receive any binary wire frames");
    await sync.close();
    await started.client.closeDb(db);
    await shutdownWorkerClient(started, log, "websocket rust");
    return { message: "Ready: browser WebSocket connected to real Rust sync listener." };
  } catch (error) {
    await shutdownWorkerClient(started, log, "websocket rust").catch(() => undefined);
    throw error;
  }
}

async function startWorkerClient(
  log: ScenarioLogger,
  label: string,
  progress?: ScenarioProgressHandler,
): Promise<StartedWorkerClient> {
  progress?.({ type: "worker-starting" });
  log(`${label} worker: starting browser WASM worker`);
  const worker = new Worker(new URL("./abi-smoke-worker.ts", import.meta.url), { type: "module" });
  const client = new BrowserWasmAbiSmokeClient(worker);
  worker.addEventListener("error", (event) => client.rejectPending(new Error(event.message)));
  worker.addEventListener("messageerror", () =>
    client.rejectPending(new Error("worker message deserialization failed")),
  );
  await client.init();
  progress?.({ type: "worker-ready" });
  log(`${label} worker: ready`);
  return { client };
}

async function shutdownWorkerClient(
  started: StartedWorkerClient,
  log: ScenarioLogger,
  label: string,
): Promise<void> {
  await started.client.shutdown();
  log(`${label} worker: shutdown`);
}

async function openMemoryTodoDb(
  client: BrowserWasmAbiSmokeClient,
  schema: Uint8Array,
  config: Uint8Array,
  log: ScenarioLogger,
  label: string,
): Promise<DbHandle> {
  const db = await client.openMemoryDb(schema, config);
  log(`${label} worker: memory db opened ${db.kind}#${db.id}`);
  return db;
}

async function openBrowserTodoDb(
  client: BrowserWasmAbiSmokeClient,
  namespace: string,
  schema: Uint8Array,
  config: Uint8Array,
  log: ScenarioLogger,
  label: string,
): Promise<DbHandle> {
  const db = await client.openBrowserDb(namespace, schema, config);
  log(`${label} worker: browser db opened ${db.kind}#${db.id}`);
  return db;
}

async function prepareTodosQuery(
  client: BrowserWasmAbiSmokeClient,
  db: DbHandle,
  log: ScenarioLogger,
  label: string,
): Promise<QueryHandle> {
  const query = await client.prepareQuery(db, queryFromTable("todos"));
  log(`${label} worker: todos query prepared ${query.kind}#${query.id}`);
  return query;
}

async function prepareQueryBytes(
  client: BrowserWasmAbiSmokeClient,
  db: DbHandle,
  queryBytes: Uint8Array,
  log: ScenarioLogger,
  label: string,
): Promise<QueryHandle> {
  const query = await client.prepareQuery(db, queryBytes);
  log(`${label} worker: query prepared ${query.kind}#${query.id}`);
  return query;
}

async function waitForLocalWrite(
  client: BrowserWasmAbiSmokeClient,
  write: WriteHandle,
  operation: string,
  progress: ScenarioProgressHandler,
  log: ScenarioLogger,
): Promise<void> {
  const writeState = await client.writeState(write);
  if (
    !["Pending", "Accepted"].includes(writeState.fate) ||
    writeState.durability !== "Local" ||
    writeState.rejection !== undefined
  ) {
    throw new Error(`unexpected ${operation} write state: ${JSON.stringify(writeState)}`);
  }
  progress({ type: "write-state", fate: writeState.fate, durability: writeState.durability });
  await client.waitWrite(write, "Local");
  progress({ type: "write-durable", durability: "Local" });
  log(`${operation} write durability: Local`);
}

async function subscriptionTodos(
  client: BrowserWasmAbiSmokeClient,
  subscription: SubscriptionHandle,
): Promise<TodoView[]> {
  return todoViews(await client.subscriptionCurrent(subscription));
}

async function readTodosAs(
  client: BrowserWasmAbiSmokeClient,
  db: DbHandle,
  query: QueryHandle,
  identity: Uint8Array,
): Promise<TodoView[]> {
  return todoViews(await client.readAllForIdentity(db, query, identity));
}

function queryFilesOrderBySizeDescLimitOffset(): Uint8Array {
  const writer = new PostcardWriter();
  writer.string("files");
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec((order: PostcardWriter) => {
    order.string("size");
    order.enumUnit(1);
  }, 1);
  writer.none();
  writer.some((limit: PostcardWriter) => limit.u64(2));
  writer.u64(1);
  return writer.finish();
}

function assertTodoSummaries(label: string, todos: TodoView[], expected: string[]): void {
  const actual = todos.map((todo) => `${todo.title}:${todo.done ? "done" : "open"}`);
  if (actual.join("\n") !== expected.join("\n"))
    throw new Error(`unexpected ${label} todos: ${formatTodos(todos)}`);
}

function assertTodoSummariesIgnoringOrder(
  label: string,
  todos: TodoView[],
  expected: string[],
): void {
  assertTodoSummaries(label, [...todos].sort(compareTodoSummary), [...expected].sort());
}

function compareTodoSummary(left: TodoView, right: TodoView): number {
  return `${left.title}:${left.done ? "done" : "open"}`.localeCompare(
    `${right.title}:${right.done ? "done" : "open"}`,
  );
}

class RecordingWebSocket {
  binaryType = "arraybuffer" as const;
  readyState = 0;
  readonly sent: Uint8Array[];
  private readonly listeners = new Map<string, Set<(...args: unknown[]) => void>>();

  constructor(
    readonly url: string,
    shared?: RecordingWebSocket,
  ) {
    this.sent = shared?.sent ?? [];
    queueMicrotask(() => {
      this.readyState = 1;
      this.emit("open");
    });
  }

  send(data: Uint8Array): void {
    if (this.readyState !== 1) throw new Error("recording websocket is not open");
    this.sent.push(new Uint8Array(data));
  }

  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.emit("close");
  }

  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
  addEventListener(type: "close", listener: () => void): void;
  addEventListener(
    type: "open" | "message" | "error" | "close",
    listener: (() => void) | ((event: { data: unknown }) => void) | ((event: unknown) => void),
  ): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener as (...args: unknown[]) => void);
    this.listeners.set(type, listeners);
  }

  private emit(type: string, event?: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
