import assert from "node:assert/strict";
import test from "node:test";
import { createAuthSecretStore, type AuthSecretStorage } from "./auth-secret-store.js";
import { createJazzClient, createJazzHooks, JazzProvider, type JazzClient } from "./jazz-client.js";
import { defineApp, schema, type AuthState, type Db, type InsertOptions, type Subscription, type Table, type Transaction, type UpsertOptions, type WriteResult, type WriteTimestampOptions } from "./jazz-tools.js";

const emptySchema = {};
const app = defineApp({
  todos: schema.table({
    title: schema.text(),
    done: schema.boolean(),
  }),
});
type Todo = { id: string; title: string; done: boolean };

class MemoryStorage implements AuthSecretStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

class FakeWasmDb {
  static openMemory(): FakeWasmDb {
    return new FakeWasmDb();
  }
}

function toBase64Url(value: unknown): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function makeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "none", typ: "JWT" })}.${toBase64Url(payload)}.`;
}

test("createJazzClient opens a local-first DB from the auth secret store", async () => {
  const storage = new MemoryStorage();
  const store = createAuthSecretStore({ appId: "client-app", storage });
  const client = await createJazzClient({
    schema: emptySchema,
    appId: "client-app",
    Runtime: FakeWasmDb,
    authSecretStore: store,
  });

  assert.equal(client.db, client.db);
  assert.equal(client.auth.authMode, "local-first");
  assert.equal(client.getAuthState().session?.authMode, "local-first");
  assert.equal(storage.values.size, 1);
});

test("createJazzClient keeps explicit JWT auth instead of creating a secret", async () => {
  const storage = new MemoryStorage();
  const store = createAuthSecretStore({ appId: "client-app", storage });
  const client = await createJazzClient({
    schema: emptySchema,
    appId: "client-app",
    jwtToken: makeJwt({ sub: "alice", claims: { role: "writer" } }),
    Runtime: FakeWasmDb,
    authSecretStore: store,
  });

  assert.equal(client.auth.authMode, "external");
  assert.equal(client.auth.session?.user_id, "alice");
  assert.equal(storage.values.size, 0);
});

test("createJazzClient subscriptions observe real WasmDb writes", async () => {
  const client = await createJazzClient({
    schema: app._schema,
    appId: "client-runtime-subscription",
    authSecretStore: createAuthSecretStore({ storage: null }),
  });
  const snapshots: Todo[][] = [];
  const subscription = client.db.subscribe(app.todos as Table<Todo, Omit<Todo, "id">>, (rows) => {
    snapshots.push(rows.map((row) => ({ ...row })));
  });

  try {
    await waitForSnapshots(snapshots, 1);
    assert.deepEqual(todoSnapshots(snapshots), [[]]);

    const inserted = client.db.insert(app.todos as Table<Todo, Omit<Todo, "id">>, { title: "Real client callback", done: false });
    client.db.update(app.todos as Table<Todo, Omit<Todo, "id">>, inserted.id, { done: true });
    client.db.delete(app.todos as Table<Todo, Omit<Todo, "id">>, inserted.id);

    await waitForSnapshots(snapshots, 4);
    assert.deepEqual(todoSnapshots(snapshots), [
      [],
      [[inserted.id, "Real client callback", false]],
      [[inserted.id, "Real client callback", true]],
      [],
    ]);
  } finally {
    subscription.unsubscribe();
    await (client.db as { close?: () => Promise<void> }).close?.();
  }
});

async function waitForSnapshots<Row>(snapshots: Row[][], count: number): Promise<void> {
  for (let attempt = 0; attempt < 20 && snapshots.length < count; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

function todoSnapshots(snapshots: Todo[][]): Array<Array<[string, string, boolean]>> {
  return snapshots.map((rows) => rows.map((todo) => [todo.id, todo.title, todo.done]));
}

test("JazzProvider passes the thin client to function children", async () => {
  const client = await createJazzClient({
    schema: emptySchema,
    appId: "provider-app",
    Runtime: FakeWasmDb,
    authSecretStore: createAuthSecretStore({ storage: null }),
  });

  assert.equal(JazzProvider({ client }), client);
  assert.equal(JazzProvider({ client, children: "ready" }), "ready");
  assert.equal(JazzProvider({ client, children: (provided) => provided.getAuthState().authMode }), "local-first");
});

test("createJazzHooks binds useDb, useTable, and useAll to DB subscription callbacks", () => {
  const db = new MemoryDb({ todos: [] });
  const client = makeClient(db);
  const { useJazzClient, useDb, useTable, useAll } = createJazzHooks(client);

  assert.equal(useJazzClient(), client);
  assert.equal(useDb(), db);

  const todos = useTable<Todo, Omit<Todo, "id">>("todos");
  const liveTodos = useAll<Todo>(todos);
  assert.equal(liveTodos.current.length, 0);

  db.insert(todos, { id: "todo-1", title: "Ship hooks", done: false });
  assert.equal(liveTodos.current.length, 0);
  db.emit(todos);
  assert.deepEqual(liveTodos.current.map((todo) => todo.title), ["Ship hooks"]);
  assert.deepEqual(liveTodos.current.map((todo) => todo.done), [false]);

  db.update(todos, "todo-1", { done: true });
  assert.deepEqual(liveTodos.current.map((todo) => todo.done), [false]);
  db.emit(todos);
  assert.deepEqual(liveTodos.current.map((todo) => todo.done), [true]);

  liveTodos.unsubscribe();
  assert.equal(db.unsubscribeCount, 1);
});

test("MemoryDb write compatibility handles stay row-like and expose value/wait", async () => {
  type Todo = { id: string; title: string; done: boolean; updatedAt?: string };
  const db = new MemoryDb({ todos: [] });
  const todos = db.table<Todo, Omit<Todo, "id">>("todos");

  const inserted = db.insert(todos, { title: "Alpha write shape", done: false }, { id: "todo-1" });
  assert.equal(inserted.id, "todo-1");
  assert.equal(inserted.value.title, "Alpha write shape");
  assert.equal(await inserted.wait({ tier: "local" }), inserted.value);
  assert.deepEqual(Object.keys(inserted).sort(), ["done", "id", "title"]);

  const updated = db.update(todos, "todo-1", { done: true }, { updatedAt: "2026-06-24T00:00:00.000Z" });
  assert.equal(updated.done, true);
  assert.equal(updated.value.done, true);

  const patched = db.upsert(todos, { title: "Patched", done: true }, { id: "todo-1" });
  assert.equal(patched.id, "todo-1");
  assert.equal(patched.title, "Patched");
  assert.equal(db.one(todos)?.title, "Patched");

  const created = db.upsert(todos, { title: "Created", done: false }, { id: "todo-2", updatedAt: 0 });
  assert.equal(created.id, "todo-2");
  assert.equal(db.all(todos).length, 2);

  const deleted = db.delete(todos, "todo-2", { updatedAt: new Date(0) });
  assert.equal(deleted.value, undefined);
  assert.equal(await deleted.wait(), undefined);
  assert.equal(db.all(todos).length, 1);

  const restored = db.restore(todos, "todo-2", { title: "Restored", done: false });
  assert.equal(restored.id, "todo-2");
  assert.equal(restored.value.title, "Restored");
  assert.equal(await restored.wait({ tier: "local" }), restored.value);
  assert.throws(() => db.restore(todos, "todo-2", { title: "Still visible", done: false }), /row not deleted/);
});

test("createJazzHooks can read a provider-backed current client", () => {
  const db = new MemoryDb({ todos: [] });
  const client = makeClient(db);
  let currentClient: JazzClient | null = null;
  const hooks = createJazzHooks(() => currentClient);

  assert.throws(() => hooks.useDb(), /Jazz client is not available/);
  assert.equal(JazzProvider({ client, children: (provided) => {
    currentClient = provided;
    return hooks.useDb();
  } }), db);
});

class MemoryDb implements Db {
  unsubscribeCount = 0;
  private readonly subscribersByTable = new Map<string, Set<(rows: unknown[]) => void>>();

  constructor(private readonly rowsByTable: Record<string, Array<Record<string, unknown>>>) {}

  beginTransaction(): Transaction {
    throw new Error("transactions are not implemented by this test fixture");
  }

  transaction<Value>(): Value {
    throw new Error("transactions are not implemented by this test fixture");
  }

  table<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(name: string): Table<Row, Init> {
    this.rowsByTable[name] ??= [];
    return {
      _table: name,
      _schema: {},
      _rowType: undefined as unknown as Row,
      _initType: undefined as unknown as Init,
    } as Table<Row, Init>;
  }

  insert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: InsertOptions<Row> = {},
  ): WriteResult<Row> & Row {
    const next = { id: options.id ?? row.id ?? `row-${this.rowsByTable[table._table].length + 1}`, ...row };
    this.rowsByTable[table._table].push(next as Record<string, unknown>);
    return makeMemoryWriteResult(next as unknown as Row);
  }

  update<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    patch: Partial<Omit<Row, "id">>,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    const rows = this.rowsByTable[table._table];
    const index = rows.findIndex((row) => row.id === id);
    if (index < 0) throw new Error(`missing row ${String(id)}`);
    rows[index] = { ...rows[index], ...patch };
    return makeMemoryWriteResult(rows[index] as Row);
  }

  upsert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: UpsertOptions<Row>,
  ): WriteResult<Row> & Row {
    const id = options.id ?? row.id;
    if (id == null) throw new Error("upsert requires id");
    const rows = this.rowsByTable[table._table];
    const index = rows.findIndex((existing) => existing.id === id);
    if (index < 0) {
      const next = { id, ...row };
      rows.push(next as Record<string, unknown>);
      return makeMemoryWriteResult(next as unknown as Row);
    }
    rows[index] = { ...rows[index], ...row, id };
    return makeMemoryWriteResult(rows[index] as Row);
  }

  delete<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    _options: WriteTimestampOptions = {},
  ): WriteResult<void> {
    const rows = this.rowsByTable[table._table];
    const index = rows.findIndex((row) => row.id === id);
    if (index >= 0) rows.splice(index, 1);
    return makeMemoryWriteResult(undefined) as WriteResult<void>;
  }

  restore<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    const rows = this.rowsByTable[table._table];
    if (rows.some((existing) => existing.id === id)) throw new Error(`Restore failed: row not deleted: ${String(id)}`);
    const restored = { id, ...row };
    rows.push(restored as Record<string, unknown>);
    return makeMemoryWriteResult(restored as unknown as Row);
  }

  all<Row>(tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string }): Row[] {
    return [...this.rowsByTable[tableOrQuery._table]] as Row[];
  }

  one<Row>(tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string }): Row | null {
    return this.all(tableOrQuery)[0] ?? null;
  }

  allForIdentity<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string },
    _identity: string | Uint8Array,
  ): Row[] {
    return this.all(tableOrQuery);
  }

  subscribe<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string },
    callback: (rows: Row[]) => void,
  ): Subscription<Row> {
    const subscribers = this.subscribersByTable.get(tableOrQuery._table) ?? new Set<(rows: unknown[]) => void>();
    this.subscribersByTable.set(tableOrQuery._table, subscribers);
    subscribers.add(callback as (rows: unknown[]) => void);
    callback(this.all(tableOrQuery));
    return {
      unsubscribe: () => {
        subscribers.delete(callback as (rows: unknown[]) => void);
        this.unsubscribeCount += 1;
      },
    };
  }

  emit<Row extends { id: string | Uint8Array }>(tableOrQuery: Table<Row, unknown> | { readonly _table: string }): void {
    const rows = this.all(tableOrQuery);
    for (const callback of this.subscribersByTable.get(tableOrQuery._table) ?? []) {
      callback(rows);
    }
  }

  getAuthState(): AuthState {
    return testAuthState;
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    listener(testAuthState);
    return () => undefined;
  }

  updateAuthToken(): void {}
}

const testAuthState: AuthState = {
  authMode: "local-first",
  session: null,
};

function makeClient(db: Db): JazzClient {
  return {
    db,
    get auth() {
      return db.getAuthState();
    },
    getAuthState() {
      return db.getAuthState();
    },
    onAuthChanged(listener) {
      return db.onAuthChanged(listener);
    },
    updateAuthToken(jwtToken) {
      db.updateAuthToken(jwtToken);
    },
  };
}

function makeMemoryWriteResult<Value extends object>(value: Value): WriteResult<Value> & Value;
function makeMemoryWriteResult(value: void): WriteResult<void>;
function makeMemoryWriteResult<Value>(value: Value): WriteResult<Value> | (WriteResult<Value> & object) {
  const target = value && typeof value === "object" ? value as object : {};
  Object.defineProperties(target, {
    value: {
      value,
      enumerable: false,
    },
    handle: {
      value: null,
      enumerable: false,
    },
    wait: {
      value: async () => value,
      enumerable: false,
    },
  });
  return target as WriteResult<Value> & object;
}
