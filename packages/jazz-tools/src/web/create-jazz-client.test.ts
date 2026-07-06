import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";

const mocks = vi.hoisted(() => {
  const createDb = vi.fn();
  const createDbFromInspectedPage = vi.fn();
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
    createDbFromInspectedPage,
    trackPromise,
    orchestratorInstances,
    MockSubscriptionsOrchestrator,
    setInitError(error: Error | null) {
      initError = error;
    },
    reset() {
      createDb.mockReset();
      createDbFromInspectedPage.mockReset();
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

vi.mock("../dev-tools/index.js", () => ({
  createDbFromInspectedPage: mocks.createDbFromInspectedPage,
}));

import {
  createExtensionJazzClient,
  createJazzClient,
  type JazzClientConfig,
} from "./create-jazz-client.js";
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
      session,
    })),
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
    const config: JazzClientConfig<false> = {
      appId: "solid-unit-1",
      asyncSubscriptionsOnly: false,
    };
    const session: Session = {
      user_id: "local:alice",
      claims: {},
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
    expect(manager.db).not.toBe(db);
    expect(manager.init).toHaveBeenCalledTimes(1);

    expect(client.db).toBe(db);
    expect(client.session).toEqual(session);
    expect("manager" in client).toBe(false);
    expect(getSubscriptionStore(client)).toBe(manager);

    await client.shutdown();
    expect(manager.shutdown).toHaveBeenCalledTimes(1);
    expect(db.shutdown).toHaveBeenCalledTimes(1);
    expect(manager.shutdown.mock.invocationCallOrder[0]!).toBeLessThan(
      db.shutdown.mock.invocationCallOrder[0]!,
    );
  });

  it("AGC-02: rejects when db creation fails", async () => {
    const config: JazzClientConfig<false> = {
      appId: "solid-unit-2",
      asyncSubscriptionsOnly: false,
    };
    const dbError = new Error("createDb failed");

    mocks.createDb.mockRejectedValue(dbError);

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(mocks.orchestratorInstances).toHaveLength(0);
  });

  it("AGC-03: rejects when orchestrator init fails", async () => {
    const config: JazzClientConfig<false> = {
      appId: "solid-unit-3",
      asyncSubscriptionsOnly: false,
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
    const config: JazzClientConfig<false> = {
      appId: "solid-unit-4",
      asyncSubscriptionsOnly: false,
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
    const config: JazzClientConfig<false> = {
      appId: "web-client-dedup-shared",
      asyncSubscriptionsOnly: false,
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
      asyncSubscriptionsOnly: false,
      secret: "principal-A",
    } satisfies JazzClientConfig<false>);
    const second = await createJazzClient({
      appId: "web-client-dedup-multi",
      asyncSubscriptionsOnly: false,
      secret: "principal-B",
    } satisfies JazzClientConfig<false>);

    expect(mocks.createDb).toHaveBeenCalledTimes(2);
    expect(first.db).not.toBe(second.db);

    await first.shutdown();
    await second.shutdown();
  });

  it("AGC-07: exposes window.__jazz.clearStorage for the only live namespace", async () => {
    (globalThis as { window?: unknown }).window = {} as unknown;

    const config: JazzClientConfig<false> = {
      appId: "web-client-unit-5",
      asyncSubscriptionsOnly: false,
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

    const aliceConfig: JazzClientConfig<false> = {
      appId: "web-client-unit-6-alice",
      asyncSubscriptionsOnly: false,
      driver: { type: "persistent", dbName: "alice-cache" },
    };
    const bobConfig: JazzClientConfig<false> = {
      appId: "web-client-unit-6-bob",
      asyncSubscriptionsOnly: false,
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

describe("framework-agnostic/createAgnosticExtensionJazzClient", () => {
  beforeEach(() => {
    mocks.reset();
    mocks.trackPromise.mockImplementation((promise) => promise);
  });

  it("AGC-EXT-01: creates client from inspected page", async () => {
    const db = createMockDb("devtools-app");
    mocks.createDbFromInspectedPage.mockResolvedValue(db);

    const client = await createExtensionJazzClient();

    expect(mocks.createDbFromInspectedPage).toHaveBeenCalledTimes(1);
    expect(db.getConfig).toHaveBeenCalledTimes(1);
    expect(mocks.orchestratorInstances).toHaveLength(1);
    expect(mocks.orchestratorInstances[0]!.config).toEqual({
      appId: "devtools-app",
    });
    expect(client.db).toBe(db);
    expect(client.session).toBeNull();
    expect("manager" in client).toBe(false);
    expect(getSubscriptionStore(client)).toBe(mocks.orchestratorInstances[0]!);
  });

  it("AGC-EXT-02: rejects when config is missing", async () => {
    const db = { shutdown: vi.fn(), getConfig: vi.fn(() => null) };
    mocks.createDbFromInspectedPage.mockResolvedValue(db);

    await expect(createExtensionJazzClient()).rejects.toThrow(
      "DevTools bridge did not provide an inspected page config.",
    );
  });

  it("AGC-EXT-03: wraps with trackPromise", async () => {
    const db = createMockDb();
    mocks.createDbFromInspectedPage.mockResolvedValue(db);

    await createExtensionJazzClient();
    expect(mocks.trackPromise).toHaveBeenCalledTimes(1);
  });
});
