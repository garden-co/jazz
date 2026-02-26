import { afterEach, describe, expect, it, vi } from "vitest";
import type { DbConfig } from "./db.js";

type SetupOptions = {
  dbError?: Error;
  orchestratorInitError?: Error;
};

async function setupCreateClient(options: SetupOptions = {}) {
  vi.resetModules();

  const dbShutdown = vi.fn(async () => {});
  const db = { shutdown: dbShutdown };

  const createDb = vi.fn(async () => {
    if (options.dbError) {
      throw options.dbError;
    }
    return db;
  });

  const managerCtor = vi.fn();
  const managerInit = vi.fn(async () => {
    if (options.orchestratorInitError) {
      throw options.orchestratorInitError;
    }
  });
  const managerShutdown = vi.fn(async () => {});

  vi.doMock("./db.js", () => ({
    createDb,
  }));

  vi.doMock("../subscriptions-orchestrator.js", async () => {
    const actual = await vi.importActual<typeof import("../subscriptions-orchestrator.js")>(
      "../subscriptions-orchestrator.js",
    );

    class MockSubscriptionsOrchestrator {
      constructor(config: { appId: string }, instanceDb: unknown) {
        managerCtor(config, instanceDb);
      }

      init = managerInit;
      shutdown = managerShutdown;
    }

    return {
      ...actual,
      SubscriptionsOrchestrator: MockSubscriptionsOrchestrator,
    };
  });

  const { createJazzClient } = await import("./create-jazz-client.js");

  return {
    createJazzClient,
    createDb,
    db,
    dbShutdown,
    managerCtor,
    managerInit,
    managerShutdown,
  };
}

afterEach(() => {
  vi.clearAllMocks();
  vi.resetModules();
  vi.doUnmock("./db.js");
  vi.doUnmock("../subscriptions-orchestrator.js");
});

describe("react-native/create-jazz-client", () => {
  it("RNC-U01 initializes a client and shuts down manager + db cleanly", async () => {
    const {
      createJazzClient,
      createDb,
      db,
      dbShutdown,
      managerCtor,
      managerInit,
      managerShutdown,
    } = await setupCreateClient();
    const config: DbConfig = { appId: "rn-create-client-happy" };

    const client = await createJazzClient(config);

    expect(createDb).toHaveBeenCalledWith(config);
    expect(managerCtor).toHaveBeenCalledWith({ appId: config.appId }, db);
    expect(managerInit).toHaveBeenCalledTimes(1);
    expect(client.db).toBe(db);

    await client.shutdown();

    expect(managerShutdown).toHaveBeenCalledTimes(1);
    expect(dbShutdown).toHaveBeenCalledTimes(1);
  });

  it("RNC-U02 rejects when db creation fails", async () => {
    const dbError = new Error("db creation failed");
    const { createJazzClient, createDb, managerCtor, managerInit } = await setupCreateClient({
      dbError,
    });
    const config: DbConfig = { appId: "rn-create-client-db-failure" };

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(createDb).toHaveBeenCalledWith(config);
    expect(managerCtor).not.toHaveBeenCalled();
    expect(managerInit).not.toHaveBeenCalled();
  });

  it("RNC-U03 rejects when orchestrator init fails", async () => {
    const initError = new Error("orchestrator init failed");
    const {
      createJazzClient,
      createDb,
      db,
      dbShutdown,
      managerCtor,
      managerInit,
      managerShutdown,
    } = await setupCreateClient({
      orchestratorInitError: initError,
    });
    const config: DbConfig = { appId: "rn-create-client-manager-failure" };

    await expect(createJazzClient(config)).rejects.toBe(initError);
    expect(createDb).toHaveBeenCalledWith(config);
    expect(managerCtor).toHaveBeenCalledWith({ appId: config.appId }, db);
    expect(managerInit).toHaveBeenCalledTimes(1);
    expect(managerShutdown).not.toHaveBeenCalled();
    expect(dbShutdown).not.toHaveBeenCalled();
  });
});
