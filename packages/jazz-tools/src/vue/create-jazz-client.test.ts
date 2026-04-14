import { beforeEach, describe, expect, it, vi } from "vitest";
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

function createMockDb(session: Session | null = null) {
  return {
    getAuthState: vi.fn(() => ({
      status: session ? "authenticated" : "unauthenticated",
      session,
    })),
    onAuthChanged: vi.fn(() => () => {}),
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
    const session: Session = {
      user_id: "local:test",
      claims: { auth_mode: "local-first" },
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

  it("VU-U02: rejects when db creation fails", async () => {
    const config: DbConfig = { appId: "vue-client-unit-2" };
    const dbError = new Error("createDb failed");

    mocks.createDb.mockRejectedValue(dbError);

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(mocks.orchestratorInstances).toHaveLength(0);
  });

  it("VU-U03: rejects when orchestrator init fails", async () => {
    const config: DbConfig = { appId: "vue-client-unit-3" };
    const initError = new Error("orchestrator init failed");
    const db = createMockDb();

    mocks.createDb.mockResolvedValue(db);
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

    mocks.createDb.mockResolvedValue(db);

    await createJazzClient(config);

    expect(mocks.createDb).toHaveBeenCalledWith(config);
  });
});
