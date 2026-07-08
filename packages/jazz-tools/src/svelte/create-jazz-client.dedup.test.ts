import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DbConfig } from "../runtime/db.js";

const mocks = vi.hoisted(() => {
  const createDb = vi.fn();
  const trackPromise = vi.fn(<T>(promise: Promise<T>) => promise);

  class MockSubscriptionsOrchestrator {
    readonly init = vi.fn(async () => undefined);
    readonly setSession = vi.fn();
    readonly shutdown = vi.fn(async () => undefined);
    constructor(
      readonly config: { appId: string },
      readonly db: unknown,
    ) {}
  }

  return {
    createDb,
    trackPromise,
    MockSubscriptionsOrchestrator,
    reset() {
      createDb.mockReset();
      trackPromise.mockReset();
      trackPromise.mockImplementation(<T>(promise: Promise<T>) => promise);
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

import { createJazzClient } from "./create-jazz-client.js";

function mockDb(appId: string) {
  return {
    getAuthState: vi.fn(() => ({ status: "unauthenticated", session: null })),
    onAuthChanged: vi.fn(() => () => {}),
    shutdown: vi.fn(async () => undefined),
    getConfig: vi.fn(() => ({ appId })),
  };
}

describe("svelte/createJazzClient runtime dedup", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("collapses two same-identity clients onto a single runtime", async () => {
    const config: DbConfig = { appId: "dedup-shared", serverUrl: "https://jazz.example.com" };
    mocks.createDb.mockResolvedValue(mockDb("dedup-shared"));

    const a = await createJazzClient(config);
    const b = await createJazzClient({ ...config });

    // Two components, one identity → one runtime (one createDb, shared Db).
    expect(mocks.createDb).toHaveBeenCalledTimes(1);
    expect(a.db).toBe(b.db);
    expect(a.manager).toBe(b.manager);
  });

  it("keeps distinct identities on separate runtimes (single-screen multi-principal)", async () => {
    mocks.createDb.mockImplementation(async () => mockDb("dedup-multi"));

    await createJazzClient({ appId: "dedup-multi", secret: "principal-A" } as DbConfig);
    await createJazzClient({ appId: "dedup-multi", secret: "principal-B" } as DbConfig);

    // Different secret → different identity → must NOT be merged.
    expect(mocks.createDb).toHaveBeenCalledTimes(2);
  });
});
