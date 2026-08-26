import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { canonicalAuthorSubject } from "../runtime/author-id.js";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";

const mocks = vi.hoisted(() => {
  const createDb = vi.fn();
  const trackPromise = vi.fn(<T>(promise: Promise<T>) => promise);
  const orchestratorInstances: Array<{
    config: { appId: string };
    db: unknown;
    init: ReturnType<typeof vi.fn>;
    shutdown: ReturnType<typeof vi.fn>;
  }> = [];
  let initError: Error | null = null;

  class MockSubscriptionsOrchestrator {
    readonly init = vi.fn(async () => {
      if (initError) {
        throw initError;
      }
    });
    readonly setSession = vi.fn();
    readonly shutdown = vi.fn(async () => undefined);

    constructor(
      readonly config: { appId: string },
      readonly db: unknown,
    ) {
      orchestratorInstances.push(this);
    }
  }

  return {
    createDb,
    trackPromise,
    orchestratorInstances,
    MockSubscriptionsOrchestrator,
    setInitError(error: Error | null) {
      initError = error;
    },
    reset() {
      createDb.mockReset();
      trackPromise.mockReset();
      orchestratorInstances.length = 0;
      initError = null;
    },
  };
});

vi.mock("../runtime/db.js", () => ({
  Db: class {},
  createDb: mocks.createDb,
  getDbSubscriptionSource: (db: unknown) => db,
  resolveDefaultPersistentDbName: (config: DbConfig) => {
    const driver = config.driver;
    if (driver?.type === "persistent" && driver.dbName?.trim()) {
      return driver.dbName.trim();
    }
    return config.dbName?.trim() || config.appId;
  },
}));

vi.mock("../subscriptions-orchestrator.js", () => ({
  SubscriptionsOrchestrator: mocks.MockSubscriptionsOrchestrator,
  trackPromise: mocks.trackPromise,
}));

import { createJazzClient, type JazzClientConfig } from "./create-jazz-client.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";

const originalWindow = (globalThis as { window?: unknown }).window;

function createMockDb(
  appId = "test-app",
  session: Session | null = null,
  config: DbConfig = { appId },
) {
  return {
    getAuthState: vi.fn(() => ({
      status: session ? "authenticated" : "unauthenticated",
      session: session
        ? {
            user: JSON.stringify([session.issuer, session.user_id]),
            claims: { ...session.claims, iss: session.issuer, sub: session.user_id },
            authMode: session.authMode,
          }
        : null,
    })),
    getInternalSession: vi.fn(() => session),
    onAuthChanged: vi.fn(() => () => {}),
    deleteClientStorage: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
    getConfig: vi.fn(() => config),
  };
}

describe("framework-agnostic/createAgnosticJazzClient", () => {
  beforeEach(() => {
    mocks.reset();
    mocks.trackPromise.mockImplementation((promise) => promise);
  });

  afterEach(() => {
    if (originalWindow === undefined) {
      delete (globalThis as { window?: unknown }).window;
    } else {
      (globalThis as { window?: unknown }).window = originalWindow;
    }
  });

  it("AGC-01: initialises orchestrator and shuts down cleanly", async () => {
    const config: JazzClientConfig = {
      appId: "solid-unit-1",
    };
    const session: Session = {
      user_id: "local:alice",
      claims: {},
      issuer: "urn:jazz:local-first",
      authMode: "local-first",
    };
    const db = createMockDb("test-app", session);

    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient(config);

    expect(mocks.trackPromise).toHaveBeenCalledTimes(1);
    expect(mocks.createDb).toHaveBeenCalledWith({ appId: "solid-unit-1" });

    expect(mocks.orchestratorInstances).toHaveLength(1);
    const manager = mocks.orchestratorInstances[0]!;
    expect(manager.config).toEqual({ appId: config.appId });
    expect(manager.db).toBe(db);
    expect(manager.init).toHaveBeenCalledTimes(1);

    expect(client.db).toBe(db);
    expect(client.session).toEqual({
      user: canonicalAuthorSubject(session.issuer, session.user_id),
      claims: { iss: session.issuer, sub: session.user_id },
      authMode: session.authMode,
    });
    expect("manager" in client).toBe(false);
    expect(getSubscriptionStore(client)).toBe(manager);

    await client.shutdown();
    expect(manager.shutdown).toHaveBeenCalledTimes(1);
    expect(db.shutdown).toHaveBeenCalledTimes(1);
    expect(manager.shutdown.mock.invocationCallOrder[0]!).toBeLessThan(
      db.shutdown.mock.invocationCallOrder[0]!,
    );
  });

  it("continues database teardown after orchestrator shutdown fails", async () => {
    const managerError = new Error("orchestrator shutdown failed");
    const dbError = new Error("database shutdown failed");
    const db = createMockDb("shutdown-failure");
    db.shutdown.mockRejectedValueOnce(dbError);
    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient({ appId: "shutdown-failure" });
    const manager = mocks.orchestratorInstances[0]!;
    manager.shutdown.mockRejectedValueOnce(managerError);

    await expect(client.shutdown()).resolves.toBeUndefined();
    expect(manager.shutdown).toHaveBeenCalledOnce();
    expect(db.shutdown).toHaveBeenCalledOnce();
  });

  it("AGC-02: rejects when db creation fails", async () => {
    const config: JazzClientConfig = {
      appId: "solid-unit-2",
    };
    const dbError = new Error("createDb failed");

    mocks.createDb.mockRejectedValue(dbError);

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(mocks.orchestratorInstances).toHaveLength(0);
  });

  it("AGC-03: rejects when orchestrator init fails", async () => {
    const config: JazzClientConfig = {
      appId: "solid-unit-3",
    };
    const initError = new Error("orchestrator init failed");
    const db = createMockDb();

    mocks.createDb.mockResolvedValue(db);
    mocks.setInitError(initError);

    await expect(createJazzClient(config)).rejects.toBe(initError);
    expect(mocks.orchestratorInstances).toHaveLength(1);
    expect(mocks.orchestratorInstances[0]!.init).toHaveBeenCalledTimes(1);
  });

  it("AGC-04: forwards runtimeSources through framework client creation", async () => {
    const config: JazzClientConfig = {
      appId: "solid-unit-4",
      runtimeSources: {
        baseUrl: "/assets/jazz/",
        wasmUrl: "/assets/jazz/custom.wasm",
      },
    };
    const db = createMockDb();

    mocks.createDb.mockResolvedValue(db);

    await createJazzClient(config);

    expect(mocks.createDb).toHaveBeenCalledWith({
      appId: "solid-unit-4",
      runtimeSources: {
        baseUrl: "/assets/jazz/",
        wasmUrl: "/assets/jazz/custom.wasm",
      },
    });
  });

  it("AGC-05: collapses same-identity clients onto one runtime", async () => {
    const config: JazzClientConfig = {
      appId: "web-client-dedup-shared",
      serverUrl: "https://jazz.example.com",
    };
    mocks.createDb.mockResolvedValue(createMockDb(config.appId, null, config));

    const first = await createJazzClient(config);
    const second = await createJazzClient({ ...config });

    expect(mocks.createDb).toHaveBeenCalledTimes(1);
    expect(first.db).toBe(second.db);
    expect("manager" in first).toBe(false);
    expect(getSubscriptionStore(first)).toBe(getSubscriptionStore(second));

    await first.shutdown();
    expect(mocks.orchestratorInstances[0]!.shutdown).not.toHaveBeenCalled();

    await second.shutdown();
    expect(mocks.orchestratorInstances[0]!.shutdown).toHaveBeenCalledTimes(1);
    expect(first.db.shutdown).toHaveBeenCalledTimes(1);
  });

  it("AGC-06: keeps distinct identities on separate runtimes", async () => {
    mocks.createDb.mockImplementation(async (config: DbConfig) =>
      createMockDb(config.appId, null, config),
    );

    const first = await createJazzClient({
      appId: "web-client-dedup-multi",
      secret: "principal-A",
    } satisfies JazzClientConfig);
    const second = await createJazzClient({
      appId: "web-client-dedup-multi",
      secret: "principal-B",
    } satisfies JazzClientConfig);

    expect(mocks.createDb).toHaveBeenCalledTimes(2);
    expect(first.db).not.toBe(second.db);

    await first.shutdown();
    await second.shutdown();
  });

  it("AGC-07: exposes window.__jazz.clearStorage for the only live namespace", async () => {
    (globalThis as { window?: unknown }).window = {} as unknown;

    const config: JazzClientConfig = {
      appId: "web-client-unit-5",
      driver: { type: "persistent", dbName: "alice-cache" },
    };
    const db = createMockDb(config.appId, null, config);
    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient(config);

    const api = (
      window as {
        __jazz?: {
          clearStorage(namespace?: string): Promise<void>;
          listLiveStorageNamespaces(): string[];
        };
      }
    ).__jazz;

    expect(api?.listLiveStorageNamespaces()).toEqual(["alice-cache"]);

    await api?.clearStorage();

    expect(db.deleteClientStorage).toHaveBeenCalledTimes(1);

    await client.shutdown();
    expect(api?.listLiveStorageNamespaces()).toEqual([]);
  });

  it("AGC-08: requires a namespace when multiple live contexts exist", async () => {
    (globalThis as { window?: unknown }).window = {} as unknown;

    const aliceConfig: JazzClientConfig = {
      appId: "web-client-unit-6-alice",
      driver: { type: "persistent", dbName: "alice-cache" },
    };
    const bobConfig: JazzClientConfig = {
      appId: "web-client-unit-6-bob",
      driver: { type: "persistent", dbName: "bob-cache" },
    };
    const aliceDb = createMockDb(aliceConfig.appId, null, aliceConfig);
    const bobDb = createMockDb(bobConfig.appId, null, bobConfig);
    mocks.createDb.mockResolvedValueOnce(aliceDb).mockResolvedValueOnce(bobDb);

    const aliceClient = await createJazzClient(aliceConfig);
    const bobClient = await createJazzClient(bobConfig);

    const api = (
      window as {
        __jazz?: {
          clearStorage(namespace?: string): Promise<void>;
          listLiveStorageNamespaces(): string[];
        };
      }
    ).__jazz;

    await expect(api?.clearStorage()).rejects.toThrow(
      /Multiple live Jazz storage contexts.*alice-cache, bob-cache/u,
    );

    await api?.clearStorage("bob-cache");

    expect(aliceDb.deleteClientStorage).not.toHaveBeenCalled();
    expect(bobDb.deleteClientStorage).toHaveBeenCalledTimes(1);
    expect(api?.listLiveStorageNamespaces()).toEqual(["alice-cache", "bob-cache"]);

    await aliceClient.shutdown();
    await bobClient.shutdown();
  });
});
