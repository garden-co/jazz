import { createDb, type Db, type QueryBuilder } from "../../src/runtime/db.js";
import type { DbConfig } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";

export interface RemoteBrowserDbCreateInput {
  id: string;
  appId: string;
  dbName: string;
  table: string;
  queryJson?: string;
  schemaJson: string;
  serverUrl?: string;
  adminSecret?: string;
  localFirstSecret?: string;
  logLevel?: DbConfig["logLevel"];
  initialize?: boolean;
  tabCount?: number;
  initialRow?: Record<string, unknown>;
}

export interface RemoteBrowserDbWaitForTitleInput {
  id: string;
  title: string;
  timeoutMs: number;
  tier?: "local" | "edge";
}

interface RemoteBrowserDbState {
  db: Db;
  schema: WasmSchema;
  query: QueryBuilder<Record<string, unknown>>;
  table: QueryBuilder<Record<string, unknown>>;
}

declare global {
  interface Window {
    __jazzRemoteBrowserDbs__?: Map<string, RemoteBrowserDbState>;
  }
}

function getRemoteStateStore(): Map<string, RemoteBrowserDbState> {
  if (!window.__jazzRemoteBrowserDbs__) {
    window.__jazzRemoteBrowserDbs__ = new Map();
  }
  return window.__jazzRemoteBrowserDbs__;
}

function makeAllRowsQuery(
  table: string,
  schema: WasmSchema,
): QueryBuilder<Record<string, unknown>> {
  return {
    _table: table,
    _schema: schema,
    _rowType: {} as Record<string, unknown>,
    _build() {
      return JSON.stringify({
        table,
        conditions: [],
        includes: {},
        orderBy: [],
      });
    },
  };
}

export async function createRemoteBrowserDb(input: RemoteBrowserDbCreateInput): Promise<void> {
  const store = getRemoteStateStore();
  const existing = store.get(input.id);
  if (existing) {
    await existing.db.shutdown();
    store.delete(input.id);
  }

  const schema = JSON.parse(input.schemaJson) as WasmSchema;
  const db = await createDb({
    appId: input.appId,
    driver: { type: "persistent", dbName: input.dbName },
    serverUrl: input.serverUrl,
    ...(input.localFirstSecret
      ? { secret: input.localFirstSecret }
      : { adminSecret: input.adminSecret }),
    logLevel: input.logLevel,
  });

  const query = input.queryJson
    ? {
        _table: input.table,
        _schema: schema,
        _rowType: {} as Record<string, unknown>,
        _build: () => input.queryJson!,
      }
    : makeAllRowsQuery(input.table, schema);
  const table = {
    _table: input.table,
    _schema: schema,
    _rowType: {} as Record<string, unknown>,
    _initType: {} as Record<string, unknown>,
  };
  if (input.initialize) await db.all(query, { tier: "local" });
  if (input.initialRow) {
    await db.insert(table, input.initialRow).wait({ tier: "local" });
  }

  store.set(input.id, {
    db,
    schema,
    query,
    table,
  });
}

export async function insertRemoteBrowserDbRow(input: {
  id: string;
  row: Record<string, unknown>;
  table?: string;
}): Promise<string> {
  const state = getRemoteStateStore().get(input.id);
  if (!state) throw new Error(`Remote browser db "${input.id}" was not initialized`);
  const table = input.table
    ? {
        _table: input.table,
        _schema: state.schema,
        _rowType: {} as Record<string, unknown>,
        _initType: {} as Record<string, unknown>,
      }
    : state.table;
  const result = state.db.insert(table, input.row);
  await result.wait({ tier: "local" });
  return result.value.id;
}

export async function updateRemoteBrowserDbRow(input: {
  id: string;
  rowId: string;
  patch: Record<string, unknown>;
  table?: string;
}): Promise<void> {
  const state = getRemoteStateStore().get(input.id);
  if (!state) throw new Error(`Remote browser db "${input.id}" was not initialized`);
  const table = input.table
    ? {
        _table: input.table,
        _schema: state.schema,
        _rowType: {} as Record<string, unknown>,
        _initType: {} as Record<string, unknown>,
      }
    : state.table;
  await state.db.update(table, input.rowId, input.patch).wait({ tier: "local" });
}

export async function queryRemoteBrowserDbRows(input: {
  id: string;
  tier?: "local" | "edge";
}): Promise<Record<string, unknown>[]> {
  const state = getRemoteStateStore().get(input.id);
  if (!state) throw new Error(`Remote browser db "${input.id}" was not initialized`);
  return state.db.all(state.query, { tier: input.tier ?? "local" });
}

export async function waitForRemoteBrowserDbTitle(
  input: RemoteBrowserDbWaitForTitleInput,
): Promise<Record<string, unknown>[]> {
  const store = getRemoteStateStore();
  const state = store.get(input.id);
  if (!state) {
    throw new Error(`Remote browser db "${input.id}" was not initialized`);
  }

  return await new Promise<Record<string, unknown>[]>((resolve, reject) => {
    let lastRows: Record<string, unknown>[] = [];
    let unsubscribe: () => void = () => {};
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(
        new Error(
          `Remote browser db "${input.id}" did not observe title "${input.title}" within ${input.timeoutMs}ms; ` +
            `lastRows=${JSON.stringify(lastRows.slice(0, 10))}`,
        ),
      );
    }, input.timeoutMs);
    unsubscribe = state.db.subscribe(
      state.query,
      (rows) => {
        lastRows = [...rows];
        if (lastRows.some((row) => row.title === input.title)) {
          clearTimeout(timeout);
          unsubscribe();
          resolve(lastRows);
        }
      },
      input.tier ? { tier: input.tier } : undefined,
    );
  });
}

export async function closeRemoteBrowserDb(id: string): Promise<void> {
  const store = getRemoteStateStore();
  const state = store.get(id);
  if (!state) {
    return;
  }

  await state.db.shutdown();
  store.delete(id);
}
