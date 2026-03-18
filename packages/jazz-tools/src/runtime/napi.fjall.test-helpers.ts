import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeAll } from "vitest";
import type { Value, WasmSchema } from "../drivers/types.js";
import { JazzClient, type Row } from "./client.js";
import type { QueryBuilder } from "./db.js";
import { createPersistentNapiRuntime, loadNapiModule } from "./testing/napi-runtime-test-utils.js";

export type Todo = {
  id: string;
  title: string;
  done: boolean;
};

type PersistentStore = {
  appId: string;
  dataPath: string;
  cleanup(): Promise<void>;
};

type PersistentClientHandle = {
  client: JazzClient;
  shutdown(): Promise<void>;
};

export const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

export const allTodosQuery: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: TEST_SCHEMA,
  _rowType: undefined as unknown as Todo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
      offset: 0,
    });
  },
};

export function todosByDone(done: boolean): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: TEST_SCHEMA,
    _rowType: undefined as unknown as Todo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [{ column: "done", op: "eq", value: done }],
        includes: {},
        orderBy: [],
        offset: 0,
      });
    },
  };
}

export function todoValues(title: string, done: boolean): Value[] {
  return [
    { type: "Text", value: title },
    { type: "Boolean", value: done },
  ];
}

function readText(values: Value[], index: number): string {
  const value = values[index];
  if (value?.type !== "Text") {
    throw new Error(`expected Text at column ${index}, got ${JSON.stringify(value)}`);
  }
  return value.value;
}

function readBoolean(values: Value[], index: number): boolean {
  const value = values[index];
  if (value?.type !== "Boolean") {
    throw new Error(`expected Boolean at column ${index}, got ${JSON.stringify(value)}`);
  }
  return value.value;
}

export function readRowTitle(row: { values: Value[] }): string {
  return readText(row.values, 0);
}

export function readRowDone(row: { values: Value[] }): boolean {
  return readBoolean(row.values, 1);
}

export function createNapiFjallTestEnv(): {
  createPersistentStore(label: string): Promise<PersistentStore>;
  openPersistentClient(store: PersistentStore): Promise<PersistentClientHandle>;
  waitForRows(
    client: JazzClient,
    query: QueryBuilder<Todo>,
    predicate: (rows: Row[]) => boolean,
    timeoutMs?: number,
  ): Promise<Row[]>;
} {
  const stores: PersistentStore[] = [];
  const clients: PersistentClientHandle[] = [];

  beforeAll(async () => {
    await loadNapiModule();
  });

  afterEach(async () => {
    for (const client of clients.splice(0).reverse()) {
      try {
        await client.shutdown();
      } catch {
        // Best effort cleanup for native runtime handles.
      }
    }

    for (const store of stores.splice(0).reverse()) {
      try {
        await store.cleanup();
      } catch {
        // Best effort cleanup for temporary Fjall directories.
      }
    }
  });

  async function createPersistentStore(label: string): Promise<PersistentStore> {
    const dataRoot = await mkdtemp(join(tmpdir(), `jazz-napi-fjall-${label}-`));
    let cleaned = false;
    const store: PersistentStore = {
      appId: `napi-fjall-${label}-${randomUUID()}`,
      dataPath: join(dataRoot, "runtime.skv"),
      async cleanup() {
        if (cleaned) {
          return;
        }
        cleaned = true;
        await rm(dataRoot, { recursive: true, force: true });
      },
    };

    stores.push(store);
    return store;
  }

  async function openPersistentClient(store: PersistentStore): Promise<PersistentClientHandle> {
    const runtime = await createPersistentNapiRuntime(TEST_SCHEMA, store.dataPath, {
      appId: store.appId,
      env: "test",
      userBranch: "main",
      tier: "worker",
    });

    const client = JazzClient.connectWithRuntime(runtime, {
      appId: store.appId,
      schema: TEST_SCHEMA,
      env: "test",
      userBranch: "main",
      tier: "worker",
      defaultDurabilityTier: "worker",
    });

    let closed = false;
    const handle: PersistentClientHandle = {
      client,
      async shutdown() {
        if (closed) {
          return;
        }
        closed = true;
        await client.shutdown();
      },
    };

    clients.push(handle);
    return handle;
  }

  async function waitForRows(
    client: JazzClient,
    query: QueryBuilder<Todo>,
    predicate: (rows: Row[]) => boolean,
    timeoutMs = 10_000,
  ): Promise<Row[]> {
    const deadline = Date.now() + timeoutMs;
    let lastRows: Row[] = [];
    let lastError: unknown = undefined;

    while (Date.now() < deadline) {
      try {
        const rows = await client.query(query, { tier: "worker" });
        if (predicate(rows)) {
          return rows;
        }
        lastRows = rows;
      } catch (error) {
        lastError = error;
      }

      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    const lastErrorMessage =
      lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
    throw new Error(
      `timed out waiting for rows; lastRows=${JSON.stringify(
        lastRows.map((row) => ({
          id: row.id,
          title: row.values[0],
          done: row.values[1],
        })),
      )}; lastError=${lastErrorMessage}`,
    );
  }

  return {
    createPersistentStore,
    openPersistentClient,
    waitForRows,
  };
}
