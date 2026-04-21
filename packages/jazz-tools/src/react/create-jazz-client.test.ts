import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { createJazzClient } from "./create-jazz-client.js";

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
}));

vi.mock("../subscriptions-orchestrator.js", () => ({
  SubscriptionsOrchestrator: mocks.MockSubscriptionsOrchestrator,
  trackPromise: mocks.trackPromise,
}));

const originalWindow = (globalThis as { window?: unknown }).window;

function createMockDb(session: Session | null = null, config: DbConfig = { appId: "test-app" }) {
  let authSession = session;
  const listeners = new Set<(state: { session: Session | null }) => void>();

  return {
    getConfig: vi.fn(() => config),
    getAuthState: vi.fn(() => ({
      status: authSession ? "authenticated" : "unauthenticated",
      session: authSession,
    })),
    onAuthChanged: vi.fn((listener: (state: { session: Session | null }) => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    }),
    emitAuthChange(nextSession: Session | null) {
      authSession = nextSession;
      for (const listener of listeners) {
        listener({ session: authSession });
      }
    },
    deleteClientStorage: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
  };
}

describe("react/create-jazz-client unit", () => {
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

  it("RC-U01: initializes and shuts down cleanly", async () => {
    const config: DbConfig = { appId: "react-client-unit-1" };
    const session: Session = {
      user_id: "local:test",
      claims: {},
      authMode: "local-first",
    };
    const db = createMockDb(session);

    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient(config);

    expect(mocks.trackPromise).toHaveBeenCalledTimes(1);
    expect(mocks.createDb).toHaveBeenCalledWith(config);

    expect(mocks.orchestratorInstances).toHaveLength(1);
    const manager = mocks.orchestratorInstances[0]!;
    expect(manager.config).toEqual({ appId: config.appId });
    expect(manager.db).toBe(db);
    expect(manager.init).toHaveBeenCalledTimes(1);

    expect(client.db).toBe(db);
    expect(client.session).toEqual(session);
    expect(client.manager).toBe(manager);

    await client.shutdown();
    expect(manager.shutdown).toHaveBeenCalledTimes(1);
    expect(db.shutdown).toHaveBeenCalledTimes(1);
    expect(manager.shutdown.mock.invocationCallOrder[0]!).toBeLessThan(
      db.shutdown.mock.invocationCallOrder[0]!,
    );
  });

  it("RC-U02: rejects when db creation fails", async () => {
    const config: DbConfig = { appId: "react-client-unit-2" };
    const dbError = new Error("createDb failed");

    mocks.createDb.mockRejectedValue(dbError);

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(mocks.orchestratorInstances).toHaveLength(0);
  });

  it("RC-U03: tracks auth session updates from the db", async () => {
    const config: DbConfig = { appId: "react-client-unit-3" };
    const db = createMockDb({
      user_id: "alice",
      claims: { role: "reader" },
      authMode: "external" as const,
    });

    mocks.createDb.mockResolvedValue(db);

    const client = await createJazzClient(config);

    expect(client.session).toEqual({
      user_id: "alice",
      claims: { role: "reader" },
      authMode: "external",
    });

    db.emitAuthChange({
      user_id: "alice",
      claims: { role: "writer" },
      authMode: "external" as const,
    });

    expect(client.session).toEqual({
      user_id: "alice",
      claims: { role: "writer" },
      authMode: "external",
    });
  });

  it("RC-U04: rejects when orchestrator init fails", async () => {
    const config: DbConfig = { appId: "react-client-unit-4" };
    const initError = new Error("orchestrator init failed");
    const db = createMockDb();

    mocks.createDb.mockResolvedValue(db);
    mocks.setInitError(initError);

    await expect(createJazzClient(config)).rejects.toBe(initError);
    expect(mocks.orchestratorInstances).toHaveLength(1);
    expect(mocks.orchestratorInstances[0]!.init).toHaveBeenCalledTimes(1);
  });

  it("RC-U05: forwards runtimeSources through framework client creation", async () => {
    const config: DbConfig = {
      appId: "react-client-unit-5",
      runtimeSources: {
        baseUrl: "/assets/jazz/",
        wasmUrl: "/assets/jazz/custom.wasm",
        workerUrl: "/assets/jazz/custom-worker.js",
      },
    };
    const db = createMockDb();

    mocks.createDb.mockResolvedValue(db);

    await createJazzClient(config);

    expect(mocks.createDb).toHaveBeenCalledWith(config);
  });

  it("RC-U06: exposes window.__jazz.clearStorage for the only live namespace", async () => {
    (globalThis as { window?: unknown }).window = {} as unknown;

    const config: DbConfig = {
      appId: "react-client-unit-6",
      driver: { type: "persistent", dbName: "alice-cache" },
    };
    const db = createMockDb(null, config);
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

  it("RC-U07: requires a namespace when multiple live contexts exist", async () => {
    (globalThis as { window?: unknown }).window = {} as unknown;

    const aliceConfig: DbConfig = {
      appId: "react-client-unit-7-alice",
      driver: { type: "persistent", dbName: "alice-cache" },
    };
    const bobConfig: DbConfig = {
      appId: "react-client-unit-7-bob",
      driver: { type: "persistent", dbName: "bob-cache" },
    };
    const aliceDb = createMockDb(null, aliceConfig);
    const bobDb = createMockDb(null, bobConfig);
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
