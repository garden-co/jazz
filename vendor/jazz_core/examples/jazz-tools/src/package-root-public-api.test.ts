import assert from "node:assert/strict";
import test from "node:test";
import {
  createDb,
  createLocalFirstJwtAsync,
  defineApp,
  isDeleted,
  schema,
  type AuthState,
  type Db,
  type InsertOptions,
  type QueryBuilder,
  type ReadOptions,
  type Subscription,
  type Table,
  type Transaction,
  type UpsertOptions,
  type WriteResult,
  type WriteTimestampOptions,
  type WriteWaitOptions,
} from "jazz-tools";

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

const app = defineApp({
  todos: schema.table({
    title: schema.text(),
    done: schema.boolean(),
  }),
});

test("package root exports newest core WasmDb public APIs", async () => {
  assert.equal(typeof createLocalFirstJwtAsync, "function");
  const db = new PackageRootFixtureDb();
  const todos = db.table<Todo, Omit<Todo, "id">>("todos");
  const readOptions: ReadOptions = { includeDeleted: true };

  const inserted = db.insert(
    todos,
    { title: "Import by package name", done: false },
    { id: "todo-1" },
  );
  assert.equal(inserted.handle?.kind, "insert");
  assert.equal(await inserted.wait({ tier: "local" }), inserted);

  db.delete(todos, "todo-1");
  const deletedRows = db.all(todos, readOptions);
  assert.equal(deletedRows.length, 1);

  const restored = db.restore(todos, "todo-1", {
    title: "Restored from package root",
    done: false,
  });
  assert.equal(restored.handle?.kind, "restore");
  assert.equal(isDeleted(restored), false);
});

test("package root createDb import runs a real WasmDb subscription flow", async () => {
  const db = await createDb({ schema: app._schema, appId: "package-root-runtime-subscription" });
  const snapshots: Todo[][] = [];
  const subscription = db.subscribe(app.todos as Table<Todo, Omit<Todo, "id">>, (rows) => {
    snapshots.push(rows.map((row) => ({ ...row })));
  });

  try {
    await waitForSnapshots(snapshots, 1);
    db.insert(app.todos as Table<Todo, Omit<Todo, "id">>, {
      title: "Package runtime flow",
      done: false,
    });

    await waitForSnapshots(snapshots, 2);
    assert.deepEqual(
      snapshots.map((rows) => rows.map((row) => row.title)),
      [[], ["Package runtime flow"]],
    );
  } finally {
    subscription.unsubscribe();
    await (db as { close?: () => Promise<void> }).close?.();
  }
});

async function waitForSnapshots<Row>(snapshots: Row[][], count: number): Promise<void> {
  for (let attempt = 0; attempt < 20 && snapshots.length < count; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

test("auth package export exposes local-first JWT helpers", async () => {
  const auth = await import("jazz-tools/auth");

  assert.equal(typeof auth.createLocalFirstJwt, "function");
  assert.equal(typeof auth.createLocalFirstJwtAsync, "function");
  assert.equal(typeof auth.localFirstJwtPublicKeyPem, "function");
  assert.equal(auth.LOCAL_FIRST_JWT_ISSUER, "urn:jazz:local-first");
});

type PackageRootWriteHandle = {
  kind: "insert" | "update" | "upsert" | "delete" | "restore";
};

type StoredTodo = Todo & {
  readonly __deleted?: true;
};

class PackageRootFixtureDb implements Db {
  #rows: StoredTodo[] = [];

  beginTransaction(): Transaction {
    throw new Error("transactions are not implemented by this test fixture");
  }

  transaction<Value>(): Value {
    throw new Error("transactions are not implemented by this test fixture");
  }

  table<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(
    name: string,
  ): Table<Row, Init> {
    return {
      _table: name,
      _schema: {},
      _rowType: undefined as unknown as Row,
      _initType: undefined as Init,
    } as Table<Row, Init>;
  }

  insert<Row extends { id: string | Uint8Array }, Init>(
    _table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: InsertOptions<Row> = {},
  ): WriteResult<Row> & Row {
    const next = {
      ...row,
      id: options.id ?? row.id ?? `todo-${this.#rows.length + 1}`,
    } as unknown as StoredTodo;
    this.#rows.push(next);
    return writeObjectResult(next as unknown as Row, { kind: "insert" });
  }

  update<Row extends { id: string | Uint8Array }>(
    _table: Table<Row, unknown>,
    id: Row["id"],
    patch: Partial<Omit<Row, "id">>,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    const index = this.#findIndex(id);
    this.#rows[index] = { ...this.#rows[index], ...patch };
    return writeObjectResult(this.#rows[index] as unknown as Row, { kind: "update" });
  }

  upsert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: UpsertOptions<Row>,
  ): WriteResult<Row> & Row {
    const id = options.id ?? row.id;
    if (id == null) throw new Error("upsert requires id");
    const index = this.#rows.findIndex((existing) => existing.id === id);
    if (index < 0) return this.insert(table, row, { id });
    this.#rows[index] = { ...this.#rows[index], ...row, id };
    return writeObjectResult(this.#rows[index] as unknown as Row, { kind: "upsert" });
  }

  delete<Row extends { id: string | Uint8Array }>(
    _table: Table<Row, unknown>,
    id: Row["id"],
    _options: WriteTimestampOptions = {},
  ): WriteResult<void> {
    this.#rows[this.#findIndex(id)] = { ...this.#rows[this.#findIndex(id)], __deleted: true };
    return writeResult(undefined, { kind: "delete" });
  }

  restore<Row extends { id: string | Uint8Array }, Init>(
    _table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    const index = this.#findIndex(id);
    this.#rows[index] = { ...row, id } as unknown as StoredTodo;
    return writeObjectResult(this.#rows[index] as unknown as Row, { kind: "restore" });
  }

  all<Row>(
    _tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options: ReadOptions = {},
  ): Row[] {
    const rows = options.includeDeleted
      ? this.#rows
      : this.#rows.filter((row) => row.__deleted !== true);
    return [...rows] as Row[];
  }

  one<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    options: ReadOptions = {},
  ): Row | null {
    return this.all(tableOrQuery, options)[0] ?? null;
  }

  allForIdentity<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    _identity: string | Uint8Array,
    options: ReadOptions = {},
  ): Row[] {
    return this.all(tableOrQuery, options);
  }

  subscribe<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    callback: (rows: Row[]) => void,
  ): Subscription<Row> {
    callback(this.all(tableOrQuery));
    return {
      unsubscribe: () => undefined,
    };
  }

  getAuthState(): AuthState {
    return { authMode: "anonymous", session: null };
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    listener(this.getAuthState());
    return () => undefined;
  }

  updateAuthToken(_jwtToken: string | null): void {}

  #findIndex(id: string | Uint8Array): number {
    const index = this.#rows.findIndex((row) => row.id === id);
    if (index < 0) throw new Error(`missing row ${String(id)}`);
    return index;
  }
}

function writeObjectResult<Value extends object>(
  value: Value,
  handle: PackageRootWriteHandle,
): WriteResult<Value> & Value {
  const result = value;
  Object.defineProperties(result, {
    value: { value, enumerable: false },
    handle: { value: handle, enumerable: false },
    wait: {
      value: async (_options: WriteWaitOptions = {}) => value,
      enumerable: false,
    },
  });
  return result as WriteResult<Value> & Value;
}

function writeResult<Value>(value: Value, handle: PackageRootWriteHandle): WriteResult<Value> {
  return {
    value,
    handle: handle as never,
    wait: async (_options: WriteWaitOptions = {}) => value,
  };
}
