import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { createJazzClient } from "./create-jazz-client.js";

const mocks = vi.hoisted(() => {
  const resolveLocalAuthDefaults = vi.fn();
  const createDb = vi.fn();
  const resolveClientSession = vi.fn();
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
    readonly shutdown = vi.fn(async () => undefined);

    constructor(
      readonly config: { appId: string },
      readonly db: unknown,
    ) {
      orchestratorInstances.push(this);
    }
  }

  return {
    resolveLocalAuthDefaults,
    createDb,
    resolveClientSession,
    trackPromise,
    orchestratorInstances,
    MockSubscriptionsOrchestrator,
    setInitError(error: Error | null) {
      initError = error;
    },
    reset() {
      resolveLocalAuthDefaults.mockReset();
      createDb.mockReset();
      resolveClientSession.mockReset();
      trackPromise.mockReset();
      orchestratorInstances.length = 0;
      initError = null;
    },
  };
});

vi.mock("../runtime/local-auth.js", () => ({
  resolveLocalAuthDefaults: mocks.resolveLocalAuthDefaults,
}));

vi.mock("../runtime/db.js", () => ({
  Db: class {},
  createDb: mocks.createDb,
}));

vi.mock("../runtime/client-session.js", () => ({
  resolveClientSession: mocks.resolveClientSession,
}));

vi.mock("../subscriptions-orchestrator.js", () => ({
  SubscriptionsOrchestrator: mocks.MockSubscriptionsOrchestrator,
  trackPromise: mocks.trackPromise,
}));

function createMockDb() {
  return {
    shutdown: vi.fn(async () => undefined),
  };
}

describe("vue/create-jazz-client unit", () => {
  beforeEach(() => {
    mocks.reset();
    mocks.trackPromise.mockImplementation((promise) => promise);
  });

  it("VU-U01: initializes and shuts down cleanly", async () => {
    const config: DbConfig = { appId: "vue-client-unit-1" };
    const resolvedConfig: DbConfig = {
      appId: "vue-client-unit-1",
      localAuthMode: "anonymous",
      localAuthToken: "test-token",
    };
    const session: Session = {
      user_id: "local:test",
      claims: { auth_mode: "local", local_mode: "anonymous" },
    };
    const db = createMockDb();

    mocks.resolveLocalAuthDefaults.mockReturnValue(resolvedConfig);
    mocks.createDb.mockResolvedValue(db);
    mocks.resolveClientSession.mockResolvedValue(session);

    const client = await createJazzClient(config);

    expect(mocks.trackPromise).toHaveBeenCalledTimes(1);
    expect(mocks.resolveLocalAuthDefaults).toHaveBeenCalledWith(config);
    expect(mocks.createDb).toHaveBeenCalledWith(resolvedConfig);
    expect(mocks.resolveClientSession).toHaveBeenCalledWith(resolvedConfig);

    expect(mocks.orchestratorInstances).toHaveLength(1);
    const manager = mocks.orchestratorInstances[0]!;
    expect(manager.config).toEqual({ appId: resolvedConfig.appId });
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

  it("VU-U02: rejects when db creation fails", async () => {
    const config: DbConfig = { appId: "vue-client-unit-2" };
    const dbError = new Error("createDb failed");

    mocks.resolveLocalAuthDefaults.mockReturnValue(config);
    mocks.createDb.mockRejectedValue(dbError);
    mocks.resolveClientSession.mockResolvedValue(null);

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(mocks.orchestratorInstances).toHaveLength(0);
  });

  it("VU-U03: rejects when session resolution fails", async () => {
    const config: DbConfig = { appId: "vue-client-unit-3" };
    const sessionError = new Error("resolveClientSession failed");
    const db = createMockDb();

    mocks.resolveLocalAuthDefaults.mockReturnValue(config);
    mocks.createDb.mockResolvedValue(db);
    mocks.resolveClientSession.mockRejectedValue(sessionError);

    await expect(createJazzClient(config)).rejects.toBe(sessionError);
    expect(mocks.orchestratorInstances).toHaveLength(0);
  });

  it("VU-U04: rejects when orchestrator init fails", async () => {
    const config: DbConfig = { appId: "vue-client-unit-4" };
    const initError = new Error("orchestrator init failed");
    const db = createMockDb();

    mocks.resolveLocalAuthDefaults.mockReturnValue(config);
    mocks.createDb.mockResolvedValue(db);
    mocks.resolveClientSession.mockResolvedValue(null);
    mocks.setInitError(initError);

    await expect(createJazzClient(config)).rejects.toBe(initError);
    expect(mocks.orchestratorInstances).toHaveLength(1);
    expect(mocks.orchestratorInstances[0]!.init).toHaveBeenCalledTimes(1);
  });

  it("VU-U05: forwards runtimeSources through framework client creation", async () => {
    const config: DbConfig = {
      appId: "vue-client-unit-5",
      runtimeSources: {
        baseUrl: "/assets/jazz/",
        wasmUrl: "/assets/jazz/custom.wasm",
        workerUrl: "/assets/jazz/custom-worker.js",
      },
    };
    const db = createMockDb();

    mocks.resolveLocalAuthDefaults.mockReturnValue(config);
    mocks.createDb.mockResolvedValue(db);
    mocks.resolveClientSession.mockResolvedValue(null);

    await createJazzClient(config);

    expect(mocks.createDb).toHaveBeenCalledWith(config);
    expect(mocks.resolveClientSession).toHaveBeenCalledWith(config);
  });
});
