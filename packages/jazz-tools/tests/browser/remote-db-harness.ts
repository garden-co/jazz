import { createDb, type Db, type QueryBuilder } from "../../src/runtime/db.js";
import type { DbConfig } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";

export interface RemoteBrowserDbCreateInput {
  id: string;
  appId: string;
  dbName: string;
  table: string;
  schemaJson: string;
  serverUrl?: string;
  adminSecret?: string;
  localFirstSecret?: string;
  logLevel?: DbConfig["logLevel"];
}

export interface RemoteBrowserDbWaitForTitleInput {
  id: string;
  title: string;
  timeoutMs: number;
  tier?: "local" | "edge";
}

interface RemoteBrowserDbState {
  db: Db;
  query: QueryBuilder<Record<string, unknown>>;
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
    ...(input.localFirstSecret ? { secret: input.localFirstSecret } : {}),
    adminSecret: input.adminSecret,
    logLevel: input.logLevel,
  });

  store.set(input.id, {
    db,
    query: makeAllRowsQuery(input.table, schema),
  });
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
    unsubscribe = state.db.subscribeAll(
      state.query,
      (delta) => {
        lastRows = [...delta.all];
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
