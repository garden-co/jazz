import { expect, it, type Mock, vi } from "vitest";

const mocks = vi.hoisted(() => {
  const createDb = vi.fn();
  const orchestratorInstances: Array<{
    init: Mock;
    shutdown: Mock;
    setSession: Mock;
  }> = [];

  class MockSubscriptionsOrchestrator {
    readonly init = vi.fn(async () => undefined);
    readonly shutdown = vi.fn(async () => undefined);
    readonly setSession = vi.fn();

    constructor() {
      orchestratorInstances.push(this);
    }
  }

  return { createDb, orchestratorInstances, MockSubscriptionsOrchestrator };
});

vi.mock("./create-db.js", () => ({
  createDb: mocks.createDb,
}));

vi.mock("../runtime/db.js", () => ({
  getDbSubscriptionSource: (db: unknown) => db,
}));

vi.mock("../subscriptions-orchestrator.js", () => ({
  SubscriptionsOrchestrator: mocks.MockSubscriptionsOrchestrator,
  trackPromise: <T>(promise: Promise<T>) => promise,
}));

import { createJazzClient } from "./create-jazz-client.js";

it("continues React Native database teardown after orchestrator shutdown fails", async () => {
  const managerError = new Error("orchestrator shutdown failed");
  const dbError = new Error("database shutdown failed");
  const db = {
    getAuthState: vi.fn(() => ({ status: "unauthenticated", session: null })),
    onAuthChanged: vi.fn(() => () => undefined),
    shutdown: vi.fn(async () => {
      throw dbError;
    }),
  };
  mocks.createDb.mockResolvedValueOnce(db);

  const client = await createJazzClient({ appId: "react-native-shutdown-failure" });
  const manager = mocks.orchestratorInstances[0]!;
  manager.shutdown.mockRejectedValueOnce(managerError);

  await expect(client.shutdown()).rejects.toBe(managerError);
  expect(manager.shutdown).toHaveBeenCalledOnce();
  expect(db.shutdown).toHaveBeenCalledOnce();
});
