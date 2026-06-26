import assert from "node:assert/strict";
import test from "node:test";
import { act, create } from "react-test-renderer";
import { createElement, useEffect } from "react";
import {
  createJazzClient,
  JazzProvider,
  useAll,
  useDb,
  useTable,
  type JazzClient,
} from "./react.js";
import { createAuthSecretStore } from "./auth-secret-store.js";
import type {
  AuthState,
  Db,
  InsertOptions,
  QueryBuilder,
  Subscription,
  Table,
  Transaction,
  UpsertOptions,
  WriteResult,
  WriteTimestampOptions,
} from "./jazz-tools.js";
import { defineApp, schema } from "./jazz-tools.js";

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

type Todo = { id: string; title: string; done: boolean };
const app = defineApp({
  todos: schema.table({
    title: schema.text(),
    done: schema.boolean(),
  }),
});

test("React JazzProvider/useAll rerenders from real WasmDb subscription callbacks", async () => {
  const client = await createJazzClient({
    schema: app._schema,
    appId: "react-runtime-subscription",
    authSecretStore: createAuthSecretStore({ storage: null }),
  });
  let latest: { db: Db; table: Table<Todo, Omit<Todo, "id">>; titles: string[] } | undefined;

  function TodoList() {
    const table = useTable<Todo, Omit<Todo, "id">>("todos");
    const rows = useAll(table);
    const currentDb = useDb();

    useEffect(() => {
      latest = { db: currentDb, table, titles: rows.map((todo) => todo.title) };
    }, [currentDb, rows, table]);

    return createElement("output", null, rows.map((todo) => todo.title).join(","));
  }

  let renderer: ReturnType<typeof create> | undefined;
  try {
    await act(async () => {
      renderer = create(createElement(JazzProvider, { client }, createElement(TodoList)));
    });

    assert.deepEqual(latest?.titles, []);

    await act(async () => {
      latest?.db.insert(latest.table, { title: "Real React callback", done: false });
    });

    assert.deepEqual(latest?.titles, ["Real React callback"]);
    const json = renderer!.toJSON();
    assert.ok(json && !Array.isArray(json));
    assert.equal(json.children?.join(""), "Real React callback");
  } finally {
    await act(async () => {
      renderer?.unmount();
    });
    await (client.db as { close?: () => Promise<void> }).close?.();
  }
});

test("React JazzProvider/useDb/useAll fixture covers lifecycle unsubscribe only", () => {
  const db = new ObservableMemoryDb({ todos: [] });
  const client = makeClient(db);
  let latest: { db: Db; table: Table<Todo, Omit<Todo, "id">>; titles: string[] } | undefined;

  function TodoList() {
    const table = useTable<Todo, Omit<Todo, "id">>("todos");
    const rows = useAll(table);
    const currentDb = useDb();

    useEffect(() => {
      latest = { db: currentDb, table, titles: rows.map((todo) => todo.title) };
    }, [currentDb, rows, table]);

    return createElement("output", null, rows.map((todo) => todo.title).join(","));
  }

  let renderer: ReturnType<typeof create>;
  act(() => {
    renderer = create(createElement(JazzProvider, { client }, createElement(TodoList)));
  });

  assert.deepEqual(latest?.titles, []);

  act(() => {
    latest?.db.insert(latest.table, { title: "Ship React compat", done: false }, { id: "todo-1" });
  });

  assert.deepEqual(latest?.titles, []);

  act(() => {
    db.emit(latest!.table);
  });

  assert.deepEqual(latest?.titles, ["Ship React compat"]);
  const json = renderer!.toJSON();
  assert.ok(json && !Array.isArray(json));
  assert.equal(json.children?.join(""), "Ship React compat");

  act(() => {
    renderer!.unmount();
  });
  assert.equal(db.activeListenerCount, 0);
});

class ObservableMemoryDb implements Db {
  unsubscribeCount = 0;
  private readonly subscribersByTable = new Map<string, Set<(rows: unknown[]) => void>>();
  private snapshotVersion = 0;
  private cachedRowsByTable = new Map<string, { version: number; rows: Record<string, unknown>[] }>();

  constructor(private readonly rowsByTable: Record<string, Array<Record<string, unknown>>>) {}

  get activeListenerCount(): number {
    let count = 0;
    for (const subscribers of this.subscribersByTable.values()) count += subscribers.size;
    return count;
  }

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
    this.notify();
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
    this.notify();
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
    if (index < 0) return this.insert(table, row, { id } as InsertOptions<Row>);
    rows[index] = { ...rows[index], ...row, id };
    this.notify();
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
    this.notify();
    return makeMemoryWriteResult(undefined) as WriteResult<void>;
  }

  restore<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    const restored = { id, ...row };
    this.rowsByTable[table._table].push(restored as Record<string, unknown>);
    this.notify();
    return makeMemoryWriteResult(restored as unknown as Row);
  }

  all<Row>(tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>): Row[] {
    const cached = this.cachedRowsByTable.get(tableOrQuery._table);
    if (cached?.version === this.snapshotVersion) return cached.rows as Row[];

    const rows = [...this.rowsByTable[tableOrQuery._table]];
    this.cachedRowsByTable.set(tableOrQuery._table, { version: this.snapshotVersion, rows });
    return rows as Row[];
  }

  one<Row>(tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>): Row | null {
    return this.all(tableOrQuery)[0] ?? null;
  }

  allForIdentity<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
    _identity: string | Uint8Array,
  ): Row[] {
    return this.all(tableOrQuery);
  }

  subscribe<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
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

  getAuthState(): AuthState {
    return testAuthState;
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    listener(testAuthState);
    return () => undefined;
  }

  updateAuthToken(): void {}

  emit<Row extends { id: string | Uint8Array }>(tableOrQuery: Table<Row, unknown> | QueryBuilder<Row>): void {
    this.snapshotVersion += 1;
    const rows = this.all(tableOrQuery);
    for (const callback of this.subscribersByTable.get(tableOrQuery._table) ?? []) {
      callback(rows);
    }
  }

  private notify(): void {
    this.snapshotVersion += 1;
  }
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
