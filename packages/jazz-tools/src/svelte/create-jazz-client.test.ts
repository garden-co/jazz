import { beforeEach, describe, expect, it, vi } from "vitest";
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

import { createJazzClient, createExtensionJazzClient } from "./create-jazz-client.js";

function createMockDb(appId = "test-app", session: Session | null = null) {
  return {
    getAuthState: vi.fn(() => ({
      status: session ? "authenticated" : "unauthenticated",
      session,
    })),
    onAuthChanged: vi.fn(() => () => {}),
    shutdown: vi.fn(async () => undefined),
    getConfig: vi.fn(() => ({ appId })),
  };
}

describe("svelte/createJazzClient", () => {
  beforeEach(() => {
    mocks.reset();
    mocks.trackPromise.mockImplementation((promise) => promise);
  });

  it("SV-U01: initialises orchestrator and shuts down cleanly", async () => {
    const config: DbConfig = { appId: "svelte-unit-1" };
    const session: Session = {
      user_id: "local:alice",
      claims: { auth_mode: "local-first" },
    };
    const db = createMockDb("test-app", session);

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

  it("SV-U02: rejects when db creation fails", async () => {
    const config: DbConfig = { appId: "svelte-unit-2" };
    const dbError = new Error("createDb failed");

    mocks.createDb.mockRejectedValue(dbError);

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(mocks.orchestratorInstances).toHaveLength(0);
  });

  it("SV-U03: rejects when orchestrator init fails", async () => {
    const config: DbConfig = { appId: "svelte-unit-3" };
    const initError = new Error("orchestrator init failed");
    const db = createMockDb();

    mocks.createDb.mockResolvedValue(db);
    mocks.setInitError(initError);

    await expect(createJazzClient(config)).rejects.toBe(initError);
    expect(mocks.orchestratorInstances).toHaveLength(1);
    expect(mocks.orchestratorInstances[0]!.init).toHaveBeenCalledTimes(1);
  });

  it("SV-U04: forwards runtimeSources through framework client creation", async () => {
    const config: DbConfig = {
      appId: "svelte-unit-4",
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
});

describe("svelte/createExtensionJazzClient", () => {
  beforeEach(() => {
    mocks.reset();
    mocks.trackPromise.mockImplementation((promise) => promise);
  });

  it("SV-EXT-01: creates client from inspected page", async () => {
    const db = createMockDb("devtools-app");
    mocks.createDbFromInspectedPage.mockResolvedValue(db);

    const client = await createExtensionJazzClient();

    expect(mocks.createDbFromInspectedPage).toHaveBeenCalledTimes(1);
    expect(db.getConfig).toHaveBeenCalledTimes(1);
    expect(mocks.orchestratorInstances).toHaveLength(1);
    expect(mocks.orchestratorInstances[0]!.config).toEqual({ appId: "devtools-app" });
    expect(client.db).toBe(db);
    expect(client.session).toBeNull();
    expect(client.manager).toBe(mocks.orchestratorInstances[0]!);
  });

  it("SV-EXT-02: rejects when config is missing", async () => {
    const db = { shutdown: vi.fn(), getConfig: vi.fn(() => null) };
    mocks.createDbFromInspectedPage.mockResolvedValue(db);

    await expect(createExtensionJazzClient()).rejects.toThrow(
      "DevTools bridge did not provide an inspected page config.",
    );
  });

  it("SV-EXT-03: wraps with trackPromise", async () => {
    const db = createMockDb();
    mocks.createDbFromInspectedPage.mockResolvedValue(db);

    await createExtensionJazzClient();
    expect(mocks.trackPromise).toHaveBeenCalledTimes(1);
  });
});
