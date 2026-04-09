import { afterEach, describe, expect, it, vi } from "vitest";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "./db.js";
import { createJazzClient } from "./create-jazz-client.js";

const mocks = vi.hoisted(() => {
  const createDb = vi.fn();
  const managerCtor = vi.fn();
  const managerInit = vi.fn();
  const managerShutdown = vi.fn();

  class MockSubscriptionsOrchestrator {
    constructor(config: { appId: string }, instanceDb: unknown, sessionArg: Session | null) {
      managerCtor(config, instanceDb, sessionArg);
    }

    init = managerInit;
    setSession = vi.fn();
    shutdown = managerShutdown;
  }

  return {
    createDb,
    managerCtor,
    managerInit,
    managerShutdown,
    MockSubscriptionsOrchestrator,
  };
});

vi.mock("./db.js", async () => {
  const actual = await vi.importActual<typeof import("./db.js")>("./db.js");
  return {
    ...actual,
    createDb: mocks.createDb,
  };
});

vi.mock("../subscriptions-orchestrator.js", async () => {
  const actual = await vi.importActual<typeof import("../subscriptions-orchestrator.js")>(
    "../subscriptions-orchestrator.js",
  );

  return {
    ...actual,
    SubscriptionsOrchestrator: mocks.MockSubscriptionsOrchestrator,
  };
});

type SetupOptions = {
  config?: DbConfig;
  session?: Session | null;
  dbError?: Error;
  orchestratorInitError?: Error;
};

async function setupCreateClient(options: SetupOptions = {}) {
  const config: DbConfig = options.config ?? { appId: "rn-create-client-resolved" };

  const dbShutdown = vi.fn(async () => {});
  const db = {
    getAuthState: vi.fn(() => ({
      status: options.session ? "authenticated" : "unauthenticated",
      session: options.session ?? null,
    })),
    onAuthChanged: vi.fn(() => () => {}),
    shutdown: dbShutdown,
  };

  mocks.createDb.mockImplementation(async (_config: DbConfig) => {
    if (options.dbError) {
      throw options.dbError;
    }
    return db;
  });

  mocks.managerInit.mockImplementation(async () => {
    if (options.orchestratorInitError) {
      throw options.orchestratorInitError;
    }
  });
  mocks.managerShutdown.mockImplementation(async () => {});

  return {
    createJazzClient,
    createDb: mocks.createDb,
    config,
    db,
    dbShutdown,
    managerCtor: mocks.managerCtor,
    managerInit: mocks.managerInit,
    managerShutdown: mocks.managerShutdown,
  };
}

afterEach(() => {
  mocks.createDb.mockReset();
  mocks.managerCtor.mockReset();
  mocks.managerInit.mockReset();
  mocks.managerShutdown.mockReset();
  vi.clearAllMocks();
});

describe("react-native/create-jazz-client", () => {
  it("RNC-U01 initializes client session + manager and shuts down cleanly", async () => {
    const session: Session = {
      user_id: "alice",
      claims: { auth_mode: "external", subject: "alice" },
    };
    const appConfig: DbConfig = { appId: "rn-create-client-happy" };
    const {
      createJazzClient,
      createDb,
      db,
      dbShutdown,
      managerCtor,
      managerInit,
      managerShutdown,
    } = await setupCreateClient({ config: appConfig, session });
    const config: DbConfig = { appId: "rn-create-client-happy" };

    const client = await createJazzClient(config);

    expect(createDb).toHaveBeenCalledWith(config);
    expect(managerCtor).toHaveBeenCalledWith({ appId: config.appId }, db, session);
    expect(managerInit).toHaveBeenCalledTimes(1);
    expect(client.db).toBe(db);
    expect(client.session).toEqual(session);

    await client.shutdown();

    expect(managerShutdown).toHaveBeenCalledTimes(1);
    expect(dbShutdown).toHaveBeenCalledTimes(1);
  });

  it("RNC-U02 rejects when db creation fails", async () => {
    const dbError = new Error("db creation failed");
    const appConfig: DbConfig = { appId: "rn-create-client-db-failure" };
    const { createJazzClient, createDb, managerCtor, managerInit } = await setupCreateClient({
      dbError,
      config: appConfig,
    });
    const config: DbConfig = { appId: "rn-create-client-db-failure" };

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(createDb).toHaveBeenCalledWith(config);
    expect(managerCtor).not.toHaveBeenCalled();
    expect(managerInit).not.toHaveBeenCalled();
  });

  it("RNC-U03 rejects when orchestrator init fails", async () => {
    const initError = new Error("orchestrator init failed");
    const appConfig: DbConfig = { appId: "rn-create-client-manager-failure" };
    const session: Session = {
      user_id: "alice",
      claims: { auth_mode: "external", subject: "alice" },
    };
    const {
      createJazzClient,
      createDb,
      db,
      dbShutdown,
      managerCtor,
      managerInit,
      managerShutdown,
    } = await setupCreateClient({
      config: appConfig,
      session,
      orchestratorInitError: initError,
    });
    const config: DbConfig = { appId: "rn-create-client-manager-failure" };

    await expect(createJazzClient(config)).rejects.toBe(initError);
    expect(createDb).toHaveBeenCalledWith(config);
    expect(managerCtor).toHaveBeenCalledWith({ appId: config.appId }, db, session);
    expect(managerInit).toHaveBeenCalledTimes(1);
    expect(managerShutdown).not.toHaveBeenCalled();
    expect(dbShutdown).not.toHaveBeenCalled();
  });
});
