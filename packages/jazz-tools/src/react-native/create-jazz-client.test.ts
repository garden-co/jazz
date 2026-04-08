import { afterEach, describe, expect, it, vi } from "vitest";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "./db.js";
import { createJazzClient } from "./create-jazz-client.js";

const mocks = vi.hoisted(() => {
  const resolveLocalAuthDefaults = vi.fn();
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
    resolveLocalAuthDefaults,
    createDb,
    managerCtor,
    managerInit,
    managerShutdown,
    MockSubscriptionsOrchestrator,
  };
});

vi.mock("../runtime/local-auth.js", () => ({
  resolveLocalAuthDefaults: mocks.resolveLocalAuthDefaults,
}));

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
  resolvedConfig?: DbConfig;
  session?: Session | null;
  dbError?: Error;
  orchestratorInitError?: Error;
};

async function setupCreateClient(options: SetupOptions = {}) {
  const resolvedConfig: DbConfig = options.resolvedConfig ?? {
    appId: "rn-create-client-resolved",
    localAuthMode: "anonymous",
    localAuthToken: "rn-local-token",
  };

  const dbShutdown = vi.fn(async () => {});
  const db = {
    getAuthState: vi.fn(() => ({
      status: options.session ? "authenticated" : "unauthenticated",
      session: options.session ?? null,
    })),
    onAuthChanged: vi.fn(() => () => {}),
    shutdown: dbShutdown,
  };

  mocks.resolveLocalAuthDefaults.mockImplementation((_input: DbConfig) => resolvedConfig);

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
    resolveLocalAuthDefaults: mocks.resolveLocalAuthDefaults,
    resolvedConfig,
    db,
    dbShutdown,
    managerCtor: mocks.managerCtor,
    managerInit: mocks.managerInit,
    managerShutdown: mocks.managerShutdown,
  };
}

afterEach(() => {
  mocks.resolveLocalAuthDefaults.mockReset();
  mocks.createDb.mockReset();
  mocks.managerCtor.mockReset();
  mocks.managerInit.mockReset();
  mocks.managerShutdown.mockReset();
  vi.clearAllMocks();
});

describe("react-native/create-jazz-client", () => {
  it("RNC-U01 initializes client session + manager and shuts down cleanly", async () => {
    const session: Session = {
      user_id: "local:rn-user",
      claims: { auth_mode: "local", local_mode: "anonymous" },
    };
    const resolvedConfig: DbConfig = {
      appId: "rn-create-client-happy",
      localAuthMode: "anonymous",
      localAuthToken: "rn-token-1",
    };
    const {
      createJazzClient,
      createDb,
      resolveLocalAuthDefaults,
      resolvedConfig: actualResolvedConfig,
      db,
      dbShutdown,
      managerCtor,
      managerInit,
      managerShutdown,
    } = await setupCreateClient({ resolvedConfig, session });
    const config: DbConfig = { appId: "rn-create-client-happy" };

    const client = await createJazzClient(config);

    expect(resolveLocalAuthDefaults).toHaveBeenCalledWith(config);
    expect(actualResolvedConfig).toEqual(resolvedConfig);
    expect(createDb).toHaveBeenCalledWith(resolvedConfig);
    expect(managerCtor).toHaveBeenCalledWith({ appId: resolvedConfig.appId }, db, session);
    expect(managerInit).toHaveBeenCalledTimes(1);
    expect(client.db).toBe(db);
    expect(client.session).toEqual(session);

    await client.shutdown();

    expect(managerShutdown).toHaveBeenCalledTimes(1);
    expect(dbShutdown).toHaveBeenCalledTimes(1);
  });

  it("RNC-U02 rejects when db creation fails", async () => {
    const dbError = new Error("db creation failed");
    const resolvedConfig: DbConfig = {
      appId: "rn-create-client-db-failure",
      localAuthMode: "demo",
      localAuthToken: "rn-token-2",
    };
    const { createJazzClient, createDb, resolveLocalAuthDefaults, managerCtor, managerInit } =
      await setupCreateClient({ dbError, resolvedConfig });
    const config: DbConfig = { appId: "rn-create-client-db-failure" };

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(resolveLocalAuthDefaults).toHaveBeenCalledWith(config);
    expect(createDb).toHaveBeenCalledWith(resolvedConfig);
    expect(managerCtor).not.toHaveBeenCalled();
    expect(managerInit).not.toHaveBeenCalled();
  });

  it("RNC-U03 rejects when orchestrator init fails", async () => {
    const initError = new Error("orchestrator init failed");
    const resolvedConfig: DbConfig = {
      appId: "rn-create-client-manager-failure",
      localAuthMode: "anonymous",
      localAuthToken: "rn-token-4",
    };
    const session: Session = {
      user_id: "local:rn-user-4",
      claims: { auth_mode: "local", local_mode: "anonymous" },
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
      resolvedConfig,
      session,
      orchestratorInitError: initError,
    });
    const config: DbConfig = { appId: "rn-create-client-manager-failure" };

    await expect(createJazzClient(config)).rejects.toBe(initError);
    expect(createDb).toHaveBeenCalledWith(resolvedConfig);
    expect(managerCtor).toHaveBeenCalledWith({ appId: resolvedConfig.appId }, db, session);
    expect(managerInit).toHaveBeenCalledTimes(1);
    expect(managerShutdown).not.toHaveBeenCalled();
    expect(dbShutdown).not.toHaveBeenCalled();
  });
});
