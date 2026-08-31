import { afterEach, describe, expect, it, vi } from "vitest";
import { ReadTier, type JazzClient } from "./client.js";
import { createDbWithRuntimeSource, type Db, type DbConfig, type QueryBuilder } from "./db.js";
import { RuntimeSource, type RuntimeClientContext } from "./runtime-source.js";
import type { RuntimeSubscriptionDelta, WasmSchema } from "../drivers/types.js";

const schema: WasmSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
};

function query(): QueryBuilder<{ id: string; title: string }> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as { id: string; title: string },
    _build: () => JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] }),
  };
}

class TestRuntimeSource extends RuntimeSource<DbConfig> {
  constructor(private readonly client: JazzClient) {
    super();
  }

  override createClient(_context: RuntimeClientContext<DbConfig>): JazzClient {
    return this.client;
  }
}

function makeClient() {
  let nextSubscription = 1;
  const subscriptionCallbacks = new Map<number, (delta: RuntimeSubscriptionDelta) => void>();
  return {
    connectTransport: vi.fn(),
    disconnectTransport: vi.fn(async () => undefined),
    onMutationError: vi.fn(),
    queryInternal: vi.fn(async () => []),
    subscribeInternal: vi.fn((_query, callback: (delta: RuntimeSubscriptionDelta) => void) => {
      const id = nextSubscription++;
      subscriptionCallbacks.set(id, callback);
      return id;
    }),
    unsubscribe: vi.fn((id: number) => subscriptionCallbacks.delete(id)),
    shutdown: vi.fn(async () => undefined),
    subscriptionCallbacks,
  } as unknown as JazzClient & {
    connectTransport: ReturnType<typeof vi.fn>;
    disconnectTransport: ReturnType<typeof vi.fn>;
    queryInternal: ReturnType<typeof vi.fn>;
    subscribeInternal: ReturnType<typeof vi.fn>;
    unsubscribe: ReturnType<typeof vi.fn>;
    subscriptionCallbacks: Map<number, (delta: RuntimeSubscriptionDelta) => void>;
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function added(id: string, title: string): RuntimeSubscriptionDelta {
  const rowId = new Uint8Array(16);
  rowId.set(new TextEncoder().encode(id).subarray(0, rowId.length));
  const hex = Array.from(rowId, (byte) => byte.toString(16).padStart(2, "0")).join("");
  const sourceId = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(
    16,
    20,
  )}-${hex.slice(20)}`;
  return {
    added: [
      {
        sourceId,
        occurrenceKey: Uint8Array.from([1, ...rowId]),
        index: 0,
        row: { id: sourceId, values: [{ type: "Text", value: title }] },
      },
    ],
    removed: [],
    updated: [],
  };
}

function publicationTitles(rows: Array<{ title: string }>): string[] {
  return rows.map((row) => row.title);
}

const dbs: Db[] = [];

afterEach(async () => {
  while (dbs.length > 0) await dbs.pop()!.shutdown();
});

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("Db ReadTier.RemoteIfPossible", () => {
  it("chooses local once for one-shot reads during an explicit disconnect", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-one-shot",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    await db.disconnect();
    await db.all(query(), { tier: ReadTier.RemoteIfPossible });
    await db.reconnect();

    expect(client.queryInternal).toHaveBeenCalledOnce();
    expect(client.queryInternal.mock.calls[0]?.[1]).toMatchObject({ tier: "local" });
  });

  it("does not fall back to local when an edge read fails or times out", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-transport-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const connection = (
      db as unknown as { connection: { ensureReady: (tier?: string) => Promise<void> } }
    ).connection;
    const timeout = new Error("edge transport timed out");
    vi.spyOn(connection, "ensureReady").mockRejectedValue(timeout);

    await expect(db.all(query(), { tier: ReadTier.RemoteIfPossible })).rejects.toBe(timeout);
    expect(client.queryInternal).not.toHaveBeenCalled();
  });

  it("does not publish a local seed while a connected remote subscription is slow", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-slow-subscription",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const callback = vi.fn();

    const unsubscribe = db.subscribe(query(), callback, {
      tier: ReadTier.RemoteIfPossible,
    });
    await settle();

    expect(client.subscribeInternal).toHaveBeenCalledOnce();
    expect(client.subscribeInternal.mock.calls[0]?.[2]).toMatchObject({
      tier: "remote-if-possible",
    });
    expect(client.queryInternal).not.toHaveBeenCalled();
    expect(callback).not.toHaveBeenCalled();
    unsubscribe();
  });

  it("does not change an outstanding remote one-shot after an explicit disconnect", async () => {
    const client = makeClient();
    const result = deferred<never[]>();
    client.queryInternal.mockImplementationOnce(() => result.promise);
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-outstanding-one-shot",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    const read = db.all(query(), { tier: ReadTier.RemoteIfPossible });
    await settle();
    expect(client.queryInternal).toHaveBeenCalledOnce();
    expect(client.queryInternal.mock.calls[0]?.[1]).toMatchObject({ tier: "remote-if-possible" });

    await db.disconnect();
    result.resolve([]);
    await expect(read).resolves.toEqual([]);
    expect(client.queryInternal).toHaveBeenCalledOnce();
  });

  it("chooses remote once for concurrent reads already waiting for connection", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-connecting-one-shots",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const connectionReady = deferred<void>();
    const connection = (
      db as unknown as { connection: { ensureReady: (tier?: string) => Promise<void> } }
    ).connection;
    vi.spyOn(connection, "ensureReady").mockImplementation(() => connectionReady.promise);

    const first = db.all(query(), { tier: ReadTier.RemoteIfPossible });
    const second = db.all(query(), { tier: ReadTier.RemoteIfPossible });
    await settle();
    expect(client.queryInternal).not.toHaveBeenCalled();

    await db.disconnect();
    connectionReady.resolve();
    await expect(Promise.all([first, second])).resolves.toEqual([[], []]);
    expect(client.queryInternal).toHaveBeenCalledTimes(2);
    for (const call of client.queryInternal.mock.calls) {
      expect(call[1]).toMatchObject({ tier: "remote-if-possible" });
    }
  });

  it("serializes disconnect and reconnect while preserving the last requested state", async () => {
    const client = makeClient();
    const disconnected = deferred<void>();
    client.disconnectTransport.mockImplementationOnce(() => disconnected.promise);
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-transport-race",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    // Materialize the client and its initial transport before racing controls.
    await db.all(query(), { tier: ReadTier.LocalFirst });
    expect(client.connectTransport).toHaveBeenCalledOnce();

    const disconnect = db.disconnect();
    const reconnect = db.reconnect();
    await settle();
    expect(client.connectTransport).toHaveBeenCalledOnce();

    disconnected.resolve();
    await Promise.all([disconnect, reconnect]);
    expect(client.connectTransport).toHaveBeenCalledTimes(2);
    expect(
      (
        db as unknown as {
          connection: { isExplicitlyOffline: () => boolean };
        }
      ).connection.isExplicitlyOffline(),
    ).toBe(false);
  });

  it("does not treat a failed explicit disconnect as permission to fall back", async () => {
    const client = makeClient();
    const failure = new Error("disconnect transport failed");
    const disconnectResult = deferred<void>();
    client.disconnectTransport.mockImplementationOnce(() => disconnectResult.promise);
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-failed-disconnect",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.all(query(), { tier: ReadTier.LocalFirst });

    const disconnect = db.disconnect();
    const read = db.all(query(), { tier: ReadTier.RemoteIfPossible });
    await settle();
    expect(client.queryInternal).toHaveBeenCalledOnce();

    disconnectResult.reject(failure);
    await expect(disconnect).rejects.toBe(failure);
    await expect(read).resolves.toEqual([]);
    expect(client.queryInternal.mock.calls.at(-1)?.[1]).toMatchObject({
      tier: "remote-if-possible",
    });
  });

  it("disconnects a client created while offline before an immediate reconnect", async () => {
    const client = makeClient();
    const newClientDisconnect = deferred<void>();
    client.disconnectTransport.mockImplementationOnce(() => newClientDisconnect.promise);
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-offline-client-creation",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.disconnect();

    const unsubscribe = db.subscribe(query(), () => undefined, {
      tier: ReadTier.RemoteIfPossible,
    });
    const reconnect = db.reconnect();
    await settle();
    expect(client.disconnectTransport).toHaveBeenCalledOnce();
    expect(client.connectTransport).not.toHaveBeenCalled();

    newClientDisconnect.resolve();
    await reconnect;
    expect(client.connectTransport).toHaveBeenCalledOnce();
    expect(
      (
        db as unknown as {
          connection: { isExplicitlyOffline: () => boolean };
        }
      ).connection.isExplicitlyOffline(),
    ).toBe(false);
    unsubscribe();
  });

  it("replaces an explicitly-offline local subscription with edge exactly on reconnect", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-subscription",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    await db.disconnect();
    const unsubscribe = db.subscribe(query(), () => undefined, {
      tier: ReadTier.RemoteIfPossible,
    });

    expect(client.subscribeInternal).toHaveBeenCalledOnce();
    expect(client.subscribeInternal.mock.calls[0]?.[2]).toMatchObject({ tier: "local" });

    await db.reconnect();
    await settle();

    expect(client.unsubscribe).toHaveBeenCalledWith(1);
    expect(client.subscribeInternal).toHaveBeenCalledTimes(2);
    expect(client.subscribeInternal.mock.calls[1]?.[2]).toMatchObject({ tier: "edge" });

    unsubscribe();
  });

  it("keeps local updates live during handoff and rejects callbacks from the retired stream", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-atomic-handoff",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.disconnect();

    const publications: string[][] = [];
    const unsubscribe = db.subscribe(
      query(),
      (delta) => publications.push(publicationTitles(delta)),
      { tier: ReadTier.RemoteIfPossible },
    );
    const localCallback = client.subscriptionCallbacks.get(1)!;
    const edgeReady = deferred<void>();
    const connection = (
      db as unknown as { connection: { ensureReady: (tier?: string) => Promise<void> } }
    ).connection;
    const originalEnsureReady = connection.ensureReady.bind(connection);
    vi.spyOn(connection, "ensureReady").mockImplementation(async (tier?: string) => {
      if (tier === "edge") await edgeReady.promise;
      return originalEnsureReady(tier);
    });

    await db.reconnect();
    await settle();
    expect(client.subscribeInternal).toHaveBeenCalledOnce();
    expect(client.unsubscribe).not.toHaveBeenCalled();

    localCallback(added("during", "during handoff"));
    expect(publications.at(-1)).toEqual(["during handoff"]);

    edgeReady.resolve();
    await vi.waitFor(() => expect(client.subscribeInternal).toHaveBeenCalledTimes(2));
    expect(client.unsubscribe).toHaveBeenCalledWith(1);
    const publicationCount = publications.length;
    localCallback(added("stale", "retired local"));
    expect(publications).toHaveLength(publicationCount);

    client.subscriptionCallbacks.get(2)!(added("remote", "remote"));
    expect(publications.at(-1)).toEqual(["remote", "during handoff"]);
    unsubscribe();
  });

  it("publishes a synchronous replacement snapshot only after owning its handle", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-synchronous-replacement",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.disconnect();
    const publications: string[][] = [];
    const unsubscribe = db.subscribe(
      query(),
      (delta) => publications.push(publicationTitles(delta)),
      { tier: ReadTier.RemoteIfPossible },
    );
    client.subscribeInternal.mockImplementationOnce((_query, callback) => {
      callback(added("remote", "synchronous remote"));
      return 2;
    });

    await db.reconnect();
    await vi.waitFor(() => expect(client.subscribeInternal).toHaveBeenCalledTimes(2));

    expect(publications).toEqual([[], ["synchronous remote"]]);
    expect(client.unsubscribe).toHaveBeenCalledWith(1);
    unsubscribe();
  });

  it("does not publish a synchronous replacement callback when subscribe then throws", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-throwing-replacement",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.disconnect();
    const publications: string[][] = [];
    const unsubscribe = db.subscribe(
      query(),
      (delta) => publications.push(publicationTitles(delta)),
      { tier: ReadTier.RemoteIfPossible },
    );
    const localCallback = client.subscriptionCallbacks.get(1)!;
    const failure = new Error("replacement subscribe failed after callback");
    client.subscribeInternal.mockImplementationOnce((_query, callback) => {
      callback(added("remote", "must stay buffered"));
      throw failure;
    });
    const deferredErrors: Array<() => void> = [];
    const timeout = vi.spyOn(globalThis, "setTimeout").mockImplementation((callback: any) => {
      deferredErrors.push(callback);
      return 0 as unknown as ReturnType<typeof setTimeout>;
    });

    try {
      await db.reconnect();
      for (let turn = 0; turn < 8; turn++) await Promise.resolve();

      expect(client.subscribeInternal).toHaveBeenCalledTimes(2);
      expect(deferredErrors).toHaveLength(1);
      expect(publications).toEqual([[]]);
      localCallback(added("local", "local resumes"));
      expect(publications.at(-1)).toEqual(["local resumes"]);
    } finally {
      timeout.mockRestore();
      unsubscribe();
    }
  });

  it("hands off multiple concurrent subscriptions once across repeated offline cycles", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-concurrent-cycles",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    for (let cycle = 0; cycle < 3; cycle++) {
      await db.disconnect();
      const first = db.subscribe(query(), () => undefined, {
        tier: ReadTier.RemoteIfPossible,
      });
      const second = db.subscribe(query(), () => undefined, {
        tier: ReadTier.RemoteIfPossible,
      });
      const firstLocal = cycle * 4 + 1;
      const secondLocal = firstLocal + 1;

      await db.reconnect();
      await settle();
      expect(client.unsubscribe).toHaveBeenCalledWith(firstLocal);
      expect(client.unsubscribe).toHaveBeenCalledWith(secondLocal);
      first();
      second();
    }

    expect(client.subscribeInternal).toHaveBeenCalledTimes(12);
    expect(client.unsubscribe).toHaveBeenCalledTimes(12);
  });

  it("does not install a remote replacement when unsubscribe races reconnect", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-unsubscribe-race",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.disconnect();
    const unsubscribe = db.subscribe(query(), () => undefined, {
      tier: ReadTier.RemoteIfPossible,
    });

    const reconnect = db.reconnect();
    unsubscribe();
    await reconnect;
    await settle();

    expect(client.subscribeInternal).toHaveBeenCalledOnce();
    expect(client.unsubscribe).toHaveBeenCalledOnce();
    expect(client.unsubscribe).toHaveBeenCalledWith(1);
  });
});
