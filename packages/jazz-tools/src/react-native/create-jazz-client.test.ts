import { afterEach, describe, expect, it, vi } from "vitest";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "./db.js";

type SetupOptions = {
  resolvedConfig?: DbConfig;
  session?: Session | null;
  dbError?: Error;
  sessionError?: Error;
  orchestratorInitError?: Error;
};

async function setupCreateClient(options: SetupOptions = {}) {
  vi.resetModules();

  const resolvedConfig: DbConfig = options.resolvedConfig ?? {
    appId: "rn-create-client-resolved",
    localAuthMode: "anonymous",
    localAuthToken: "rn-local-token",
  };

  const dbShutdown = vi.fn(async () => {});
  const db = { shutdown: dbShutdown };

  const resolveLocalAuthDefaults = vi.fn((input: DbConfig) => resolvedConfig);
  const resolveClientSession = vi.fn(async () => {
    if (options.sessionError) {
      throw options.sessionError;
    }
    return options.session ?? null;
  });

  const createDb = vi.fn(async (_config: DbConfig) => {
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

  vi.doMock("../runtime/local-auth.js", () => ({
    resolveLocalAuthDefaults,
  }));

  vi.doMock("../runtime/client-session.js", () => ({
    resolveClientSession,
  }));

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
    resolveLocalAuthDefaults,
    resolveClientSession,
    resolvedConfig,
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
  vi.doUnmock("../runtime/local-auth.js");
  vi.doUnmock("../runtime/client-session.js");
  vi.doUnmock("./db.js");
  vi.doUnmock("../subscriptions-orchestrator.js");
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
      resolveClientSession,
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
    expect(resolveClientSession).toHaveBeenCalledWith(resolvedConfig);
    expect(managerCtor).toHaveBeenCalledWith({ appId: resolvedConfig.appId }, db);
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
    const {
      createJazzClient,
      createDb,
      resolveLocalAuthDefaults,
      resolveClientSession,
      managerCtor,
      managerInit,
    } = await setupCreateClient({ dbError, resolvedConfig });
    const config: DbConfig = { appId: "rn-create-client-db-failure" };

    await expect(createJazzClient(config)).rejects.toBe(dbError);
    expect(resolveLocalAuthDefaults).toHaveBeenCalledWith(config);
    expect(createDb).toHaveBeenCalledWith(resolvedConfig);
    expect(resolveClientSession).toHaveBeenCalledWith(resolvedConfig);
    expect(managerCtor).not.toHaveBeenCalled();
    expect(managerInit).not.toHaveBeenCalled();
  });

  it("RNC-U03 rejects when session resolution fails", async () => {
    const sessionError = new Error("session resolution failed");
    const resolvedConfig: DbConfig = {
      appId: "rn-create-client-session-failure",
      localAuthMode: "anonymous",
      localAuthToken: "rn-token-3",
    };
    const { createJazzClient, createDb, resolveClientSession, managerCtor, managerInit } =
      await setupCreateClient({
        resolvedConfig,
        sessionError,
      });
    const config: DbConfig = { appId: "rn-create-client-session-failure" };

    await expect(createJazzClient(config)).rejects.toBe(sessionError);
    expect(createDb).toHaveBeenCalledWith(resolvedConfig);
    expect(resolveClientSession).toHaveBeenCalledWith(resolvedConfig);
    expect(managerCtor).not.toHaveBeenCalled();
    expect(managerInit).not.toHaveBeenCalled();
  });

  it("RNC-U04 rejects when orchestrator init fails", async () => {
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
      resolveClientSession,
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
    expect(resolveClientSession).toHaveBeenCalledWith(resolvedConfig);
    expect(managerCtor).toHaveBeenCalledWith({ appId: resolvedConfig.appId }, db);
    expect(managerInit).toHaveBeenCalledTimes(1);
    expect(managerShutdown).not.toHaveBeenCalled();
    expect(dbShutdown).not.toHaveBeenCalled();
  });
});
