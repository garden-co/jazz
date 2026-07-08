import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Session } from "../runtime/context.js";
import type { CreateOptions, DeleteOptions, UpdateOptions } from "../runtime/client.js";
import type { QueryBuilder, QueryOptions, TableProxy } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import {
  decodeEncodedSubscriptionDelta,
  type EncodedSubscriptionDelta,
  type EncodedSubscriptionRow,
  type SubscriptionChannel,
  type SubscriptionRowCodec,
} from "../runtime/subscription-channel.js";

const mocks = vi.hoisted(() => {
  const createDb = vi.fn();
  const registerWindowJazzStorageClient = vi.fn(() => () => {});
  return {
    createDb,
    registerWindowJazzStorageClient,
    reset() {
      createDb.mockReset();
      registerWindowJazzStorageClient.mockClear();
      registerWindowJazzStorageClient.mockReturnValue(() => {});
    },
  };
});

vi.mock("../runtime/db.js", () => ({
  Db: class {},
  createDb: mocks.createDb,
}));

vi.mock("../window-client-storage.js", () => ({
  registerWindowJazzStorageClient: mocks.registerWindowJazzStorageClient,
}));

vi.mock("../dev-tools/index.js", () => ({
  createDbFromInspectedPage: vi.fn(),
}));

import { createJazzClient, type JazzClient } from "./create-jazz-client.js";
import {
  __browserWorkerSubscriptionChannelDiagnostics,
  __resetBrowserWorkerSubscriptionChannelsForTests,
  createBrowserWorkerSubscriptionChannel,
} from "./browser-subscription-channel.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";
import type { AuthState } from "../runtime/auth-state.js";

type TestRow = { id: string; value: string };
type TestInit = { value: string };

const query = {
  _table: "items",
  _schema: {},
  _build() {
    return JSON.stringify({ table: "items" });
  },
} as QueryBuilder<TestRow>;

const table = query as QueryBuilder<TestRow> & TableProxy<TestRow, TestInit>;

function delta(rows: TestRow[]): SubscriptionDelta<TestRow> {
  return {
    all: rows,
    delta: rows.map((item, index) => ({
      kind: 0,
      id: item.id,
      index,
      item,
    })),
  };
}

function encodeRows(rows: TestRow[]): EncodedSubscriptionDelta {
  const encoder = new TextEncoder();
  const encodedRows = rows.map((row) => ({
    id: row.id,
    bytes: encoder.encode(JSON.stringify(row)),
  }));
  return {
    all: encodedRows,
    delta: encodedRows.map((row, index) => ({
      kind: 0 as const,
      id: row.id,
      index,
      row,
    })),
  };
}

function createChannel(rows: TestRow[]): SubscriptionChannel & {
  calls: Array<{ options?: QueryOptions; session?: Session }>;
  writes: Array<{ operation: string; id?: string; session?: Session }>;
  unsubscribeCount: number;
  decodeCount: number;
} {
  const encoded = encodeRows(rows);
  const codec: SubscriptionRowCodec<TestRow> = {
    decode: (row: EncodedSubscriptionRow) => {
      channel.decodeCount++;
      return JSON.parse(new TextDecoder().decode(row.bytes)) as TestRow;
    },
  };
  const channel = {
    calls: [],
    writes: [],
    unsubscribeCount: 0,
    decodeCount: 0,
    subscribeAll(_query, callback, options, session) {
      this.calls.push({ options, session });
      callback(decodeEncodedSubscriptionDelta(encoded, codec) as SubscriptionDelta<any>);
      return () => {
        this.unsubscribeCount++;
      };
    },
    async insert<T, Init>(
      _table: TableProxy<T, Init>,
      data: Init,
      _options?: CreateOptions,
      session?: Session,
    ) {
      this.writes.push({ operation: "insert", session });
      const value = { id: "async-inserted", ...(data as Record<string, unknown>) } as T;
      return {
        value,
        transactionId: "tx-insert",
        wait: async () => value,
      };
    },
    async update<T, Init>(
      _table: TableProxy<T, Init>,
      id: string,
      _data: Partial<Init>,
      _options?: UpdateOptions,
      session?: Session,
    ) {
      this.writes.push({ operation: "update", id, session });
      return {
        transactionId: "tx-update",
        wait: async () => undefined,
      };
    },
    async delete<T, Init>(
      _table: TableProxy<T, Init>,
      id: string,
      _options?: DeleteOptions,
      session?: Session,
    ) {
      this.writes.push({ operation: "delete", id, session });
      return {
        transactionId: "tx-delete",
        wait: async () => undefined,
      };
    },
    async canInsert() {
      return true;
    },
    async canUpdate() {
      return true;
    },
    async canDelete() {
      return true;
    },
    async getAuthState() {
      return TEST_AUTH_STATE;
    },
    onAuthChanged(listener) {
      listener(TEST_AUTH_STATE);
      return () => {};
    },
    async updateAuthToken(_token) {},
    async getConfig() {
      return { appId: "test-app" };
    },
    async createFileFromBlob(_app, blob) {
      return { id: "async-file", data: new Uint8Array(await blob.arrayBuffer()) } as any;
    },
    async loadFileAsBlob() {
      return new Blob(["async-file"]);
    },
  } satisfies SubscriptionChannel & {
    calls: Array<{ options?: QueryOptions; session?: Session }>;
    writes: Array<{ operation: string; id?: string; session?: Session }>;
    unsubscribeCount: number;
    decodeCount: number;
  };
  return channel;
}

function createTypedChannel(rows: TestRow[]): SubscriptionChannel & {
  calls: Array<{ options?: QueryOptions; session?: Session }>;
  writes: Array<{ operation: string; id?: string; session?: Session }>;
  unsubscribeCount: number;
} {
  return {
    calls: [],
    writes: [],
    unsubscribeCount: 0,
    subscribeAll(_query, callback, options, session) {
      this.calls.push({ options, session });
      callback(delta(rows) as SubscriptionDelta<any>);
      return () => {
        this.unsubscribeCount++;
      };
    },
    async insert<T, Init>(
      _table: TableProxy<T, Init>,
      data: Init,
      _options?: CreateOptions,
      session?: Session,
    ) {
      this.writes.push({ operation: "insert", session });
      const value = { id: "async-inserted", ...(data as Record<string, unknown>) } as T;
      return {
        value,
        transactionId: "tx-insert",
        wait: async () => value,
      };
    },
    async update<T, Init>(
      _table: TableProxy<T, Init>,
      id: string,
      _data: Partial<Init>,
      _options?: UpdateOptions,
      session?: Session,
    ) {
      this.writes.push({ operation: "update", id, session });
      return {
        transactionId: "tx-update",
        wait: async () => undefined,
      };
    },
    async delete<T, Init>(
      _table: TableProxy<T, Init>,
      id: string,
      _options?: DeleteOptions,
      session?: Session,
    ) {
      this.writes.push({ operation: "delete", id, session });
      return {
        transactionId: "tx-delete",
        wait: async () => undefined,
      };
    },
    async canInsert() {
      return true;
    },
    async canUpdate() {
      return true;
    },
    async canDelete() {
      return true;
    },
    async getAuthState() {
      return TEST_AUTH_STATE;
    },
    onAuthChanged(listener) {
      listener(TEST_AUTH_STATE);
      return () => {};
    },
    async updateAuthToken(_token) {},
    async getConfig() {
      return { appId: "test-app" };
    },
    async createFileFromBlob(_app, blob) {
      return { id: "async-file", data: new Uint8Array(await blob.arrayBuffer()) } as any;
    },
    async loadFileAsBlob() {
      return new Blob(["async-file"]);
    },
  };
}

function createMockDb(rows: TestRow[] = []) {
  let runtimeContext: { session?: Session } | null = null;
  return {
    getAuthState: vi.fn(() => ({
      status: "unauthenticated",
      session: null,
    })),
    onAuthChanged: vi.fn(() => () => {}),
    updateAuthToken: vi.fn(),
    insert: vi.fn((_table, data) => ({
      value: { id: "worker-inserted", ...(data as Record<string, unknown>) },
      transactionId: "tx-worker-insert",
      session: runtimeContext?.session,
      wait: vi.fn(async () => undefined),
    })),
    update: vi.fn(() => ({
      transactionId: "tx-worker-update",
      session: runtimeContext?.session,
      wait: vi.fn(async () => undefined),
    })),
    delete: vi.fn(() => ({
      transactionId: "tx-worker-delete",
      session: runtimeContext?.session,
      wait: vi.fn(async () => undefined),
    })),
    canInsert: vi.fn(() => !runtimeContext?.session || runtimeContext.session.user_id === "user-1"),
    canUpdate: vi.fn(() => !runtimeContext?.session || runtimeContext.session.user_id === "user-1"),
    canDelete: vi.fn(() => !runtimeContext?.session || runtimeContext.session.user_id === "user-1"),
    __withRuntimeOperationContext: vi.fn((context, operation) => {
      const previous = runtimeContext;
      runtimeContext = context;
      try {
        return operation();
      } finally {
        runtimeContext = previous;
      }
    }),
    createFileFromBlob: vi.fn(async (_app, blob) => ({
      id: "worker-file",
      data: new Uint8Array(await blob.arrayBuffer()),
    })),
    loadFileAsBlob: vi.fn(async () => new Blob(["worker-file"])),
    deleteClientStorage: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
    getConfig: vi.fn(() => ({ appId: "test-app" })),
    subscribeAll: vi.fn((_query, callback) => {
      callback(delta(rows));
      return () => {};
    }),
  };
}

const TEST_AUTH_STATE: AuthState = {
  authMode: "local-first",
  session: {
    user_id: "user-1",
    claims: { subject: "user-1" },
    authMode: "local-first",
  },
};

async function readSubscriptionRows(client: object, options?: QueryOptions): Promise<TestRow[]> {
  const store = getSubscriptionStore(client);
  const key = store.makeQueryKey(query, options);
  const entry = store.getCacheEntry<TestRow>(key);
  return await new Promise<TestRow[]>((resolve, reject) => {
    entry.subscribe({
      onfulfilled: resolve,
      onError: reject,
    });
  });
}

async function typeSurfaceChecks(channel: SubscriptionChannel) {
  const acceptsSyncDb = (_db: unknown) => undefined;
  const asyncClient = await createJazzClient({
    appId: "types-async",
    subscriptionChannel: channel,
  });
  // @ts-expect-error asyncSubscriptionsOnly defaults to true and does not expose sync Db APIs.
  void asyncClient.db.all(query);
  void asyncClient.db.insert(table, { value: "ok" });

  const explicitAsyncClient = await createJazzClient({
    appId: "types-explicit-async",
    asyncSubscriptionsOnly: true,
    subscriptionChannel: channel,
  });
  // @ts-expect-error explicit async-only clients do not expose sync Db APIs.
  void explicitAsyncClient.db.one(query);

  const syncClient = await createJazzClient({
    appId: "types-sync",
    asyncSubscriptionsOnly: false,
  });
  acceptsSyncDb(syncClient.db);

  const narrowed: JazzClient<false> = syncClient;
  acceptsSyncDb(narrowed.db);
}
void typeSurfaceChecks;

describe("web/createJazzClient async subscription channel", () => {
  beforeEach(() => {
    mocks.reset();
  });

  afterEach(async () => {
    await __resetBrowserWorkerSubscriptionChannelsForTests();
    vi.restoreAllMocks();
  });

  it("serves async-only subscriptions through the channel without creating a Db", async () => {
    const channel = createChannel([{ id: "row-1", value: "from-channel" }]);

    const client = await createJazzClient({
      appId: "async-only-channel",
      subscriptionChannel: channel,
    });

    expect(client.db).toBeDefined();
    expect("all" in client.db).toBe(false);
    expect(mocks.createDb).not.toHaveBeenCalled();

    await expect(readSubscriptionRows(client)).resolves.toEqual([
      { id: "row-1", value: "from-channel" },
    ]);
    expect(channel.calls).toHaveLength(1);
    expect(channel.decodeCount).toBe(1);

    await client.shutdown();
    expect(channel.unsubscribeCount).toBe(1);
  });

  it("routes async-only writes and dry-run checks through the channel", async () => {
    const channel = createChannel([]);

    const client = await createJazzClient({
      appId: "async-only-writes",
      subscriptionChannel: channel,
    });

    await expect(client.db.insert(table, { value: "created" })).resolves.toMatchObject({
      value: { id: "async-inserted", value: "created" },
      transactionId: "tx-insert",
    });
    await expect(client.db.update(table, "row-1", { value: "updated" })).resolves.toMatchObject({
      transactionId: "tx-update",
    });
    await expect(client.db.delete(table, "row-1")).resolves.toMatchObject({
      transactionId: "tx-delete",
    });
    await expect(client.db.canInsert(table, { value: "created" })).resolves.toBe(true);
    await expect(client.db.canUpdate(table, "row-1", { value: "updated" })).resolves.toBe(true);
    await expect(client.db.canDelete(table, "row-1")).resolves.toBe(true);

    expect(channel.writes).toEqual([
      { operation: "insert", session: TEST_AUTH_STATE.session },
      { operation: "update", id: "row-1", session: TEST_AUTH_STATE.session },
      { operation: "delete", id: "row-1", session: TEST_AUTH_STATE.session },
    ]);

    await client.shutdown();
  });

  it("keeps channel rows encoded until a field is read", async () => {
    const channel = createChannel([{ id: "row-1", value: "lazy-channel" }]);

    const client = await createJazzClient({
      appId: "async-lazy-channel",
      subscriptionChannel: channel,
    });

    const rows = await readSubscriptionRows(client);
    expect(channel.decodeCount).toBe(0);
    expect(rows[0]?.id).toBe("row-1");
    expect(channel.decodeCount).toBe(0);
    expect(rows[0]?.value).toBe("lazy-channel");
    expect(channel.decodeCount).toBe(1);

    await client.shutdown();
  });

  it("creates a browser worker channel for async-only clients without an explicit channel", async () => {
    const db = createMockDb([{ id: "worker-row", value: "from-worker-channel" }]);
    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient({ appId: "default-worker-channel" });

    expect(client.db).toBeDefined();
    expect("all" in client.db).toBe(false);
    expect(mocks.createDb).toHaveBeenCalledWith(
      expect.objectContaining({
        appId: "default-worker-channel",
        asyncSubscriptionsOnly: false,
      }),
    );
    await expect(readSubscriptionRows(client)).resolves.toEqual([
      { id: "worker-row", value: "from-worker-channel" },
    ]);

    await client.shutdown();
    expect(db.shutdown).toHaveBeenCalledTimes(1);
  });

  it("routes synchronous-mode subscriptions to the local node by default", async () => {
    const db = createMockDb([{ id: "local-row", value: "from-local-node" }]);
    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient({
      appId: "sync-default",
      asyncSubscriptionsOnly: false,
    });

    await expect(readSubscriptionRows(client)).resolves.toEqual([
      { id: "local-row", value: "from-local-node" },
    ]);
    expect(db.subscribeAll).toHaveBeenCalledTimes(1);
  });

  it("routes async-declared synchronous-mode subscriptions through the channel", async () => {
    const db = createMockDb([{ id: "local-row", value: "from-local-node" }]);
    const channel = createTypedChannel([{ id: "async-row", value: "from-channel" }]);
    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient({
      appId: "sync-with-channel",
      asyncSubscriptionsOnly: false,
      subscriptionChannel: channel,
    });

    await expect(readSubscriptionRows(client, { subscriptionMode: "async" })).resolves.toEqual([
      { id: "async-row", value: "from-channel" },
    ]);
    expect(db.subscribeAll).not.toHaveBeenCalled();
    expect(channel.calls).toHaveLength(1);
  });

  it("exposes the browser worker subscription channel directly", async () => {
    const db = createMockDb([{ id: "direct-worker-row", value: "from-direct-channel" }]);
    mocks.createDb.mockResolvedValue(db);

    const channel = createBrowserWorkerSubscriptionChannel({ appId: "direct-channel" });
    const updates: TestRow[][] = [];
    const unsubscribe = channel.subscribeAll(query, (next) => {
      updates.push(next.all);
    });

    await vi.waitFor(() => {
      expect(updates).toHaveLength(1);
    });
    expect(updates[0]).toEqual([{ id: "direct-worker-row", value: "from-direct-channel" }]);
    expect(db.subscribeAll).toHaveBeenCalledTimes(1);

    unsubscribe();
    await channel.shutdown();
    expect(db.shutdown).toHaveBeenCalledTimes(1);
  });

  it("routes browser worker channel writes to the worker-owned Db", async () => {
    const db = createMockDb([]);
    mocks.createDb.mockResolvedValue(db);

    const channel = createBrowserWorkerSubscriptionChannel({ appId: "direct-write-channel" });

    await expect(channel.insert(table, { value: "worker-created" })).resolves.toMatchObject({
      value: { id: "worker-inserted", value: "worker-created" },
      transactionId: "tx-worker-insert",
    });
    await expect(
      channel.update(table, "row-1", { value: "worker-updated" }),
    ).resolves.toMatchObject({
      transactionId: "tx-worker-update",
    });
    await expect(channel.delete(table, "row-1")).resolves.toMatchObject({
      transactionId: "tx-worker-delete",
    });
    await expect(channel.canInsert(table, { value: "worker-created" })).resolves.toBe(true);
    await expect(channel.canUpdate(table, "row-1", { value: "worker-updated" })).resolves.toBe(
      true,
    );
    await expect(channel.canDelete(table, "row-1")).resolves.toBe(true);

    expect(db.insert).toHaveBeenCalledWith(table, { value: "worker-created" }, undefined);
    expect(db.update).toHaveBeenCalledWith(table, "row-1", { value: "worker-updated" }, undefined);
    expect(db.delete).toHaveBeenCalledWith(table, "row-1", undefined);
    expect(db.canInsert).toHaveBeenCalledWith(table, { value: "worker-created" });
    expect(db.canUpdate).toHaveBeenCalledWith(table, "row-1", { value: "worker-updated" });
    expect(db.canDelete).toHaveBeenCalledWith(table, "row-1");

    await expect(
      channel.insert(table, { value: "session-created" }, undefined, TEST_AUTH_STATE.session!),
    ).resolves.toMatchObject({
      session: TEST_AUTH_STATE.session,
    });
    await expect(
      channel.canInsert(table, { value: "session-created" }, TEST_AUTH_STATE.session!),
    ).resolves.toBe(true);

    await channel.shutdown();
  });

  it("shares one worker-owned node across multiple async-only tabs", async () => {
    const db = createMockDb([{ id: "shared-worker-row", value: "from-shared-owner" }]);
    mocks.createDb.mockResolvedValue(db);

    const tabA = createBrowserWorkerSubscriptionChannel({
      appId: "multi-tab-app",
      dbName: "multi-tab-db",
    });
    const tabB = createBrowserWorkerSubscriptionChannel({
      appId: "multi-tab-app",
      dbName: "multi-tab-db",
    });
    const updatesA: TestRow[][] = [];
    const updatesB: TestRow[][] = [];

    const unsubscribeA = tabA.subscribeAll(query, (next) => {
      updatesA.push(next.all);
    });
    const unsubscribeB = tabB.subscribeAll(query, (next) => {
      updatesB.push(next.all);
    });

    await vi.waitFor(() => {
      expect(updatesA).toHaveLength(1);
      expect(updatesB).toHaveLength(1);
    });
    expect(mocks.createDb).toHaveBeenCalledTimes(1);
    expect(db.subscribeAll).toHaveBeenCalledTimes(2);
    expect(__browserWorkerSubscriptionChannelDiagnostics().owners).toEqual([
      expect.objectContaining({ refCount: 2, closed: false }),
    ]);

    unsubscribeA();
    await tabA.shutdown();
    expect(db.shutdown).not.toHaveBeenCalled();
    expect(__browserWorkerSubscriptionChannelDiagnostics().owners).toEqual([
      expect.objectContaining({ refCount: 1, closed: false }),
    ]);

    unsubscribeB();
    await tabB.shutdown();
    expect(db.shutdown).toHaveBeenCalledTimes(1);
    expect(__browserWorkerSubscriptionChannelDiagnostics().owners).toEqual([]);
  });

  it("reopens the owning worker and resubscribes after leader handoff", async () => {
    const firstDb = createMockDb([{ id: "before-handoff", value: "old-leader" }]);
    const secondDb = createMockDb([{ id: "after-handoff", value: "new-leader" }]);
    mocks.createDb.mockResolvedValueOnce(firstDb).mockResolvedValueOnce(secondDb);

    const firstTab = createBrowserWorkerSubscriptionChannel({
      appId: "handoff-app",
      dbName: "handoff-db",
    });
    const firstUpdates: TestRow[][] = [];
    const unsubscribeFirst = firstTab.subscribeAll(query, (next) => {
      firstUpdates.push(next.all);
    });
    await vi.waitFor(() => {
      expect(firstUpdates).toEqual([[{ id: "before-handoff", value: "old-leader" }]]);
    });
    unsubscribeFirst();
    await firstTab.shutdown();

    const replacementTab = createBrowserWorkerSubscriptionChannel({
      appId: "handoff-app",
      dbName: "handoff-db",
    });
    const replacementUpdates: TestRow[][] = [];
    const unsubscribeReplacement = replacementTab.subscribeAll(query, (next) => {
      replacementUpdates.push(next.all);
    });

    await vi.waitFor(() => {
      expect(replacementUpdates).toEqual([[{ id: "after-handoff", value: "new-leader" }]]);
    });
    expect(mocks.createDb).toHaveBeenCalledTimes(2);
    expect(firstDb.shutdown).toHaveBeenCalledTimes(1);
    expect(secondDb.subscribeAll).toHaveBeenCalledTimes(1);

    unsubscribeReplacement();
    await replacementTab.shutdown();
    expect(secondDb.shutdown).toHaveBeenCalledTimes(1);
  });
});
