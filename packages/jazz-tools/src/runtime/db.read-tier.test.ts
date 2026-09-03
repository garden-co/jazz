import { afterEach, describe, expect, it, vi } from "vitest";
import { ReadTier, type JazzClient, type SubscriptionCallbacks } from "./client.js";
import {
  createDbWithRuntimeSource,
  getDbSubscriptionSource,
  type Db,
  type DbConfig,
  type QueryBuilder,
} from "./db.js";
import { RuntimeSource, type RuntimeClientContext } from "./runtime-source.js";
import type { RuntimeSubscriptionDelta, WasmSchema } from "../drivers/types.js";
import type { DbSubscriptionCallbacks as PublicDbSubscriptionCallbacks } from "../index.js";

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
  const subscriptionErrorCallbacks = new Map<number, (error: Error) => void>();
  const query = vi.fn(async () => []);
  const subscribe = vi.fn(
    (
      _query,
      callbackOrCallbacks: ((delta: RuntimeSubscriptionDelta) => void) | SubscriptionCallbacks,
    ) => {
      const callbacks =
        typeof callbackOrCallbacks === "function"
          ? { onUpdate: callbackOrCallbacks, onError: undefined }
          : callbackOrCallbacks;
      const id = nextSubscription++;
      subscriptionCallbacks.set(id, callbacks.onUpdate);
      if (callbacks.onError) subscriptionErrorCallbacks.set(id, callbacks.onError);
      return id;
    },
  );
  return {
    connectTransport: vi.fn(),
    disconnectTransport: vi.fn(async () => undefined),
    onMutationError: vi.fn(),
    query,
    queryInternal: query,
    subscribe,
    subscribeInternal: subscribe,
    unsubscribe: vi.fn((id: number) => {
      subscriptionCallbacks.delete(id);
      subscriptionErrorCallbacks.delete(id);
    }),
    shutdown: vi.fn(async () => undefined),
    subscriptionCallbacks,
    subscriptionErrorCallbacks,
  } as unknown as JazzClient & {
    connectTransport: ReturnType<typeof vi.fn>;
    disconnectTransport: ReturnType<typeof vi.fn>;
    query: ReturnType<typeof vi.fn>;
    subscribe: ReturnType<typeof vi.fn>;
    unsubscribe: ReturnType<typeof vi.fn>;
    shutdown: ReturnType<typeof vi.fn>;
    subscriptionCallbacks: Map<number, (delta: RuntimeSubscriptionDelta) => void>;
    subscriptionErrorCallbacks: Map<number, (error: Error) => void>;
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
  it("keeps explicit Local reads propagating whether connected or explicitly offline", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-public-local-propagation",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    await db.all(query(), { tier: ReadTier.LocalFirst });
    expect(client.query.mock.calls.at(-1)?.[1]).toMatchObject({ tier: ReadTier.LocalFirst });
    expect(client.query.mock.calls.at(-1)?.[1]).not.toHaveProperty("propagation");

    await db.disconnect();
    await db.all(query(), { tier: "local" });
    expect(client.query.mock.calls.at(-1)?.[1]).toMatchObject({ tier: "local" });
    expect(client.query.mock.calls.at(-1)?.[1]).not.toHaveProperty("propagation");

    await db.reconnect();
    await db.all(query(), { tier: ReadTier.Remote });
    expect(client.query.mock.calls.at(-1)?.[1]).toMatchObject({ tier: ReadTier.Remote });
    expect(client.query.mock.calls.at(-1)?.[1]).not.toHaveProperty("propagation");
  });

  it("keeps explicit Local subscriptions propagating without changing connected Edge", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-public-local-subscription-propagation",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    const stopLocal = db.subscribe(query(), () => undefined, { tier: ReadTier.LocalFirst });
    expect(client.subscribe.mock.calls.at(-1)?.[2]).toMatchObject({ tier: ReadTier.LocalFirst });
    expect(client.subscribe.mock.calls.at(-1)?.[2]).not.toHaveProperty("propagation");

    const stopRemote = db.subscribe(query(), () => undefined, { tier: ReadTier.Remote });
    expect(client.subscribe.mock.calls.at(-1)?.[2]).toMatchObject({ tier: ReadTier.Remote });
    expect(client.subscribe.mock.calls.at(-1)?.[2]).not.toHaveProperty("propagation");

    stopLocal();
    stopRemote();
  });

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

    expect(client.query).toHaveBeenCalledOnce();
    expect(client.query.mock.calls[0]?.[1]).toMatchObject({ tier: "local" });
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
    expect(client.query).not.toHaveBeenCalled();
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

    expect(client.subscribe).toHaveBeenCalledOnce();
    expect(client.subscribe.mock.calls[0]?.[2]).toMatchObject({
      tier: "remote-if-possible",
    });
    expect(client.query).not.toHaveBeenCalled();
    expect(callback).not.toHaveBeenCalled();
    unsubscribe();
  });

  it("delivers deferred-start readiness failure through the subscription owner", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-deferred-subscription-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const readiness = deferred<void>();
    // Test-only access to force the deferred worker-start branch deterministically.
    const dbInternals = db as unknown as {
      connection: {
        ensureReady: (tier?: string) => Promise<void>;
        shouldDeferSubscriptionStart: (tier: string) => boolean;
      };
    };
    const connection = dbInternals.connection;
    vi.spyOn(connection, "shouldDeferSubscriptionStart").mockReturnValue(true);
    vi.spyOn(connection, "ensureReady").mockImplementation(() => readiness.promise);
    const onUpdate = vi.fn();
    const onError = vi.fn();

    const unsubscribe = db.subscribe(query(), { onUpdate, onError }, { tier: ReadTier.Remote });
    expect(client.subscribe).not.toHaveBeenCalled();

    const failure = new Error("deferred authority readiness failed");
    readiness.reject(failure);
    await vi.waitFor(() => expect(onError).toHaveBeenCalledWith(failure));

    expect(onError).toHaveBeenCalledOnce();
    expect(client.subscribe).not.toHaveBeenCalled();
    expect(client.unsubscribe).not.toHaveBeenCalled();
    unsubscribe();
    expect(client.unsubscribe).not.toHaveBeenCalled();
  });

  it("does not change an outstanding remote one-shot after an explicit disconnect", async () => {
    const client = makeClient();
    const result = deferred<never[]>();
    client.query.mockImplementationOnce(() => result.promise);
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
    expect(client.query).toHaveBeenCalledOnce();
    expect(client.query.mock.calls[0]?.[1]).toMatchObject({ tier: "remote-if-possible" });

    await db.disconnect();
    result.resolve([]);
    await expect(read).resolves.toEqual([]);
    expect(client.query).toHaveBeenCalledOnce();
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
    expect(client.query).not.toHaveBeenCalled();

    await db.disconnect();
    connectionReady.resolve();
    await expect(Promise.all([first, second])).resolves.toEqual([[], []]);
    expect(client.query).toHaveBeenCalledTimes(2);
    for (const call of client.query.mock.calls) {
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
    expect(client.query).toHaveBeenCalledOnce();

    disconnectResult.reject(failure);
    await expect(disconnect).rejects.toBe(failure);
    await expect(read).resolves.toEqual([]);
    expect(client.query.mock.calls.at(-1)?.[1]).toMatchObject({
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

    expect(client.subscribe).toHaveBeenCalledOnce();
    expect(client.subscribe.mock.calls[0]?.[2]).toMatchObject({ tier: "local" });

    await db.reconnect();
    await settle();

    expect(client.unsubscribe).toHaveBeenCalledWith(1);
    expect(client.subscribe).toHaveBeenCalledTimes(2);
    expect(client.subscribe.mock.calls[1]?.[2]).toMatchObject({ tier: ReadTier.RemoteIfPossible });

    unsubscribe();
  });

  it("waits for fresh remote inputs on reconnect and rejects retired local callbacks", async () => {
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
    expect(client.subscribe).toHaveBeenCalledOnce();
    expect(client.unsubscribe).toHaveBeenCalledWith(1);

    localCallback(added("during", "during handoff"));
    expect(publications.at(-1)).toEqual([]);

    edgeReady.resolve();
    await vi.waitFor(() => expect(client.subscribe).toHaveBeenCalledTimes(2));
    expect(client.unsubscribe).toHaveBeenCalledWith(1);
    const publicationCount = publications.length;
    localCallback(added("stale", "retired local"));
    expect(publications).toHaveLength(publicationCount);

    client.subscriptionCallbacks.get(2)!(added("remote", "remote"));
    expect(publications.at(-1)).toEqual(["remote"]);
    unsubscribe();
  });

  it("switches an existing remote-if-possible stream repeatedly without admitting retired callbacks", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-repeated-transition",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const onDelta = vi.fn();
    const unsubscribe = getDbSubscriptionSource(db).subscribeDelta(
      query(),
      { onDelta },
      {
        tier: ReadTier.RemoteIfPossible,
      },
    );
    for (const [index, offline] of [true, false, true, false].entries()) {
      const previous = client.subscriptionCallbacks.get(index + 1)!;
      if (offline) await db.disconnect();
      else await db.reconnect();
      await vi.waitFor(() => expect(client.subscribe).toHaveBeenCalledTimes(index + 2));
      expect(client.subscribe.mock.calls[index + 1]?.[2]).toMatchObject({
        tier: offline ? "local" : ReadTier.RemoteIfPossible,
      });
      onDelta.mockClear();
      previous(added("stale", "retired generation"));
      expect(onDelta).not.toHaveBeenCalled();
      client.subscriptionCallbacks.get(index + 2)!(added(`live-${index}`, "current generation"));
      expect(onDelta).toHaveBeenCalledOnce();
    }
    unsubscribe();
    const count = client.subscribe.mock.calls.length;
    await db.disconnect();
    await db.reconnect();
    await settle();
    expect(client.subscribe).toHaveBeenCalledTimes(count);
    expect(client.subscriptionCallbacks.size).toBe(0);
  });

  it("terminalizes the local generation when reconnect handoff readiness fails", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-handoff-readiness-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.disconnect();

    const onDelta = vi.fn();
    const onError = vi.fn();
    const unsubscribe = getDbSubscriptionSource(db).subscribeDelta(
      query(),
      { onDelta, onError },
      { tier: ReadTier.RemoteIfPossible },
    );
    const localCallback = client.subscriptionCallbacks.get(1)!;
    const connectionInternals = db as unknown as {
      connection: { ensureReady: (tier?: string) => Promise<void> };
    };
    const connection = connectionInternals.connection;
    const originalEnsureReady = connection.ensureReady.bind(connection);
    const failure = new Error("edge handoff readiness failed");
    vi.spyOn(connection, "ensureReady").mockImplementation(async (tier?: string) => {
      if (tier === "edge") throw failure;
      return originalEnsureReady(tier);
    });

    await db.reconnect();
    await vi.waitFor(() => expect(onError).toHaveBeenCalledWith(failure));

    expect(onError).toHaveBeenCalledOnce();
    expect(client.subscribe).toHaveBeenCalledOnce();
    expect(client.unsubscribe.mock.calls).toEqual([[1]]);
    onDelta.mockClear();
    localCallback(added("late", "late after handoff failure"));
    expect(onDelta).not.toHaveBeenCalled();

    unsubscribe();
    expect(client.unsubscribe.mock.calls).toEqual([[1]]);
  });

  it("terminalizes the active generation once and suppresses every retired or late callback", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-retired-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    await db.disconnect();

    const onDelta = vi.fn();
    const errors: Error[] = [];
    const unsubscribe = getDbSubscriptionSource(db).subscribeDelta(
      query(),
      {
        onDelta,
        onError: (error) => errors.push(error),
      },
      { tier: ReadTier.RemoteIfPossible },
    );
    const retiredOnDelta = client.subscriptionCallbacks.get(1)!;
    const retiredOnError = client.subscriptionErrorCallbacks.get(1)!;

    await db.reconnect();
    await vi.waitFor(() => expect(client.subscribe).toHaveBeenCalledTimes(2));
    const activeOnDelta = client.subscriptionCallbacks.get(2)!;
    const activeOnError = client.subscriptionErrorCallbacks.get(2)!;
    onDelta.mockClear();

    retiredOnDelta(added("retired", "retired local stream"));
    retiredOnError(new Error("retired local stream failed late"));
    expect(onDelta).not.toHaveBeenCalled();
    expect(errors).toEqual([]);

    const activeFailure = new Error("active edge stream failed");
    activeOnError(activeFailure);
    activeOnDelta(added("late", "late active delta"));
    activeOnError(new Error("active stream failed again"));

    expect(errors).toEqual([activeFailure]);
    expect(onDelta).not.toHaveBeenCalled();
    expect(client.unsubscribe.mock.calls).toEqual([[1], [2]]);

    unsubscribe();
    expect(client.unsubscribe.mock.calls).toEqual([[1], [2]]);
  });

  it("buffers a synchronous opening error until its native handle can be detached", async () => {
    const client = makeClient();
    const failure = new Error("opening stream failed");
    let openingCallbacks!: SubscriptionCallbacks;
    client.subscribe.mockImplementationOnce((_query, callbacks: SubscriptionCallbacks) => {
      openingCallbacks = callbacks;
      callbacks.onError?.(failure);
      callbacks.onUpdate(added("opening-late", "must not publish"));
      callbacks.onError?.(new Error("duplicate opening failure"));
      return 1;
    });
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-synchronous-opening-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);

    const onDelta = vi.fn();
    const onError = vi.fn();
    const unsubscribe = getDbSubscriptionSource(db).subscribeDelta(query(), { onDelta, onError });

    expect(onError).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(failure);
    expect(onDelta).toHaveBeenCalledOnce();
    expect(publicationTitles(onDelta.mock.calls[0]![0].all)).toEqual([]);
    expect(client.unsubscribe.mock.calls).toEqual([[1]]);

    openingCallbacks.onUpdate(added("late", "must stay terminal"));
    openingCallbacks.onError?.(new Error("late error"));
    unsubscribe();

    expect(onDelta).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledOnce();
    expect(client.unsubscribe.mock.calls).toEqual([[1]]);
  });

  it("clears buffered admission deltas when the subscription terminalizes", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-buffered-admission-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const admission = deferred<void>();
    const connection = (
      db as unknown as {
        connection: { initialExplicitOfflineState: () => Promise<void> | null };
      }
    ).connection;
    vi.spyOn(connection, "initialExplicitOfflineState").mockReturnValue(admission.promise);
    const updates = vi.fn();
    const errors: Error[] = [];

    const unsubscribe = db.subscribe(query(), {
      onUpdate: updates,
      onError: (error) => errors.push(error),
    });
    const onDelta = client.subscriptionCallbacks.get(1)!;
    const onNativeError = client.subscriptionErrorCallbacks.get(1)!;
    onDelta(added("buffered", "must never publish"));
    const failure = new Error("subscription failed before admission");
    onNativeError(failure);
    onNativeError(new Error("duplicate terminal failure"));
    admission.resolve();
    await settle();

    expect(errors).toEqual([failure]);
    expect(updates).not.toHaveBeenCalled();
    onDelta(added("late", "must stay terminal"));
    expect(updates).not.toHaveBeenCalled();
    unsubscribe();
  });

  it("fences an already-running local seed after a native terminal error", async () => {
    const client = makeClient();
    const localSeed = deferred<
      Array<{ id: string; values: Array<{ type: "Text"; value: string }> }>
    >();
    client.query.mockImplementationOnce(() => localSeed.promise);
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-running-seed-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const updates: string[][] = [];
    const errors: Error[] = [];
    const unsubscribe = db.subscribe(query(), {
      onUpdate: (rows) => updates.push(publicationTitles(rows)),
      onError: (error) => errors.push(error),
    });
    await vi.waitFor(() => expect(client.query).toHaveBeenCalledOnce());
    const onNativeError = client.subscriptionErrorCallbacks.get(1)!;
    const failure = new Error("native stream failed while seed was reading");
    onNativeError(failure);

    localSeed.resolve([
      {
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "late local seed" }],
      },
    ]);
    await settle();

    expect(errors).toEqual([failure]);
    expect(updates).toEqual([[]]);
    unsubscribe();
  });

  it("routes public update callback failures once and contains error callback failures", async () => {
    const client = makeClient();
    const db = await createDbWithRuntimeSource(
      {
        appId: "read-tier-public-callback-error",
        serverUrl: "https://example.test",
        adminSecret: "test-admin-secret",
      },
      new TestRuntimeSource(client),
    );
    dbs.push(db);
    const updateFailure = new Error("public subscription update failed");
    const errorCallbackFailure = new Error("public subscription onError failed");
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const updates = vi.fn((rows: Array<{ id: string; title: string }>) => {
      if (rows.length > 0) throw updateFailure;
    });
    const onError = vi.fn(() => {
      throw errorCallbackFailure;
    });
    const callbacks: PublicDbSubscriptionCallbacks<{ id: string; title: string }> = {
      onUpdate: updates,
      onError,
    };
    const unsubscribe = db.subscribe(query(), callbacks);
    const onDelta = client.subscriptionCallbacks.get(1)!;

    expect(() => onDelta(added("callback", "throws"))).not.toThrow();
    onDelta(added("late", "must stay terminal"));

    expect(updates).toHaveBeenCalledTimes(2);
    expect(onError).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(updateFailure);
    expect(consoleError).toHaveBeenCalledWith(
      "Jazz subscription error callback failed",
      errorCallbackFailure,
    );
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
    client.subscribe.mockImplementationOnce((_query, callbacks: SubscriptionCallbacks) => {
      callbacks.onUpdate(added("remote", "synchronous remote"));
      return 2;
    });

    await db.reconnect();
    await vi.waitFor(() => expect(client.subscribe).toHaveBeenCalledTimes(2));

    expect(publications).toEqual([[], ["synchronous remote"]]);
    expect(client.unsubscribe).toHaveBeenCalledWith(1);
    unsubscribe();
  });

  it("terminalizes through onError when synchronous replacement installation fails", async () => {
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
    const errors: Error[] = [];
    const unsubscribe = db.subscribe(
      query(),
      {
        onUpdate: (rows) => publications.push(publicationTitles(rows)),
        onError: (error) => errors.push(error),
      },
      { tier: ReadTier.RemoteIfPossible },
    );
    await vi.waitFor(() => expect(client.subscribe).toHaveBeenCalledTimes(1));
    const localCallback = client.subscriptionCallbacks.get(1)!;
    const failure = new Error("replacement subscribe failed after callback");
    client.subscribe.mockImplementationOnce((_query, callbacks: SubscriptionCallbacks) => {
      callbacks.onUpdate(added("remote", "must stay buffered"));
      throw failure;
    });

    await db.reconnect();
    await vi.waitFor(() => expect(client.subscribe).toHaveBeenCalledTimes(2));

    expect(errors).toEqual([failure]);
    expect(publications).toEqual([[]]);
    expect(client.unsubscribe.mock.calls).toEqual([[1]]);
    localCallback(added("local", "retired after replacement failure"));
    expect(publications).toEqual([[]]);

    unsubscribe();
    expect(client.unsubscribe.mock.calls).toEqual([[1]]);
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

    expect(client.subscribe).toHaveBeenCalledTimes(12);
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

    expect(client.subscribe).toHaveBeenCalledOnce();
    expect(client.unsubscribe).toHaveBeenCalledOnce();
    expect(client.unsubscribe).toHaveBeenCalledWith(1);
  });
});
