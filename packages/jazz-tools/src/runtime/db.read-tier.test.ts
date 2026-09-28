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
import type { RemoteLinkState } from "./remote-link-state.js";

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

function makeClient(initialLink: RemoteLinkState = "connected") {
  let nextSubscription = 1;
  let linkState = initialLink;
  const linkListeners = new Set<(state: RemoteLinkState) => void>();
  const runtime = {
    setRemoteLinkHint: vi.fn(),
    remoteLinkState: () => linkState,
    onRemoteLinkStateChange: (listener: (state: RemoteLinkState) => void, signal: AbortSignal) => {
      linkListeners.add(listener);
      signal.addEventListener("abort", () => linkListeners.delete(listener), { once: true });
    },
  };
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
    getRuntime: () => runtime,
    linkHints: runtime.setRemoteLinkHint,
    setLink: (state: RemoteLinkState) => {
      linkState = state;
      for (const listener of [...linkListeners]) listener(state);
    },
    subscriptionCallbacks,
    subscriptionErrorCallbacks,
  } as unknown as JazzClient & {
    linkHints: ReturnType<typeof vi.fn>;
    setLink: (state: RemoteLinkState) => void;
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
        occurrenceKey: Uint8Array.from([1, ...rowId, 0, 0, 0, 0, 0, 0, 0, 0]),
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

describe("Db read tiers and connection controls", () => {
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

  it("keeps explicit Local subscriptions propagating without changing connected remote subscriptions", async () => {
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
      tier: ReadTier.LocalFirstUnlessEmpty,
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
    expect(onDelta).not.toHaveBeenCalled();
    expect(client.unsubscribe.mock.calls).toEqual([[1]]);

    openingCallbacks.onUpdate(added("late", "must stay terminal"));
    openingCallbacks.onError?.(new Error("late error"));
    unsubscribe();

    expect(onDelta).not.toHaveBeenCalled();
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

  it("uses only the native stream for its opening and later results", async () => {
    const client = makeClient();
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
    await settle();
    expect(client.query).not.toHaveBeenCalled();
    expect(updates).toEqual([]);
    const onDelta = client.subscriptionCallbacks.get(1)!;
    onDelta(added("opening", "native opening"));
    expect(updates).toEqual([["native opening"]]);
    const onNativeError = client.subscriptionErrorCallbacks.get(1)!;
    const failure = new Error("native stream failed");
    onNativeError(failure);
    onDelta(added("late", "must stay terminal"));
    await settle();

    expect(errors).toEqual([failure]);
    expect(updates).toEqual([["native opening"]]);
    expect(client.query).not.toHaveBeenCalled();
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

    expect(updates).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(updateFailure);
    expect(consoleError).toHaveBeenCalledWith(
      "Jazz subscription error callback failed",
      errorCallbackFailure,
    );
    unsubscribe();
  });
});

function emptyOpening(): RuntimeSubscriptionDelta {
  return { added: [], removed: [], updated: [], reset: true };
}

async function openUnlessEmptyDb(
  client: ReturnType<typeof makeClient>,
  appId: string,
  serverUrl: string | undefined = "https://example.test",
) {
  const db = await createDbWithRuntimeSource(
    {
      appId,
      ...(serverUrl ? { serverUrl } : {}),
      adminSecret: "test-admin-secret",
    },
    new TestRuntimeSource(client),
  );
  dbs.push(db);
  return db;
}

describe("Db ReadTier.LocalFirstUnlessEmpty", () => {
  // The core Db owns the opening gate. Db passes the tier through unchanged
  // and keeps the core informed of the server link; it never withholds,
  // probes or re-reads on its own.

  it("passes the tier to one runtime subscription and publishes its opening as delivered", async () => {
    const client = makeClient("connected");
    const db = await openUnlessEmptyDb(client, "unless-empty-subscription");
    const updates: string[][] = [];

    const unsubscribe = db.subscribe(query(), (rows) => updates.push(publicationTitles(rows)), {
      tier: ReadTier.LocalFirstUnlessEmpty,
    });
    expect(client.subscribe).toHaveBeenCalledOnce();
    expect(client.subscribe.mock.calls[0]?.[2]).toMatchObject({
      tier: "local-first-unless-empty",
    });
    client.subscriptionCallbacks.get(1)!(emptyOpening());
    await settle();
    expect(updates).toEqual([[]]);

    client.subscriptionCallbacks.get(1)!(added("synced", "synced row"));
    expect(updates).toEqual([[], ["synced row"]]);
    expect(client.subscribe).toHaveBeenCalledOnce();
    unsubscribe();
  });

  it("passes the tier to one runtime read and returns its rows", async () => {
    const client = makeClient("connecting");
    client.query.mockResolvedValueOnce([
      {
        id: "22222222-2222-2222-2222-222222222222",
        values: [{ type: "Text", value: "remote" }],
      },
    ]);
    const db = await openUnlessEmptyDb(client, "unless-empty-one-shot");

    const rows = await db.all(query(), { tier: ReadTier.LocalFirstUnlessEmpty });

    expect(publicationTitles(rows)).toEqual(["remote"]);
    expect(client.query).toHaveBeenCalledOnce();
    expect(client.query.mock.calls[0]?.[1]).toMatchObject({ tier: "local-first-unless-empty" });
  });

  it("reports the server link to the runtime as it changes", async () => {
    const client = makeClient("connecting");
    const db = await openUnlessEmptyDb(client, "unless-empty-link-hints");
    // The runtime client is created by the first operation.
    await db.all(query());
    expect(client.linkHints.mock.calls.at(-1)).toEqual(["connecting"]);

    client.setLink("connected");
    expect(client.linkHints.mock.calls.at(-1)).toEqual(["connected"]);

    await db.disconnect();
    expect(client.linkHints.mock.calls.at(-1)).toEqual(["unavailable"]);

    client.setLink("unavailable");
    await db.reconnect();
    expect(client.linkHints.mock.calls.at(-1)).toEqual(["unavailable"]);
    client.setLink("connecting");
    expect(client.linkHints.mock.calls.at(-1)).toEqual(["connecting"]);
  });

  it("reports no server when none is configured", async () => {
    const client = makeClient("connected");
    const db = await openUnlessEmptyDb(client, "unless-empty-no-server", "");
    await db.all(query());

    expect(client.linkHints.mock.calls).toEqual([["none"]]);
  });
});
