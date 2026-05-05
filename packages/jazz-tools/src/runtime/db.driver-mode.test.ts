import { afterEach, describe, expect, it, vi } from "vitest";
import { Db, createDb } from "./db.js";

const originalWindow = (globalThis as Record<string, unknown>).window;
const originalWorker = (globalThis as Record<string, unknown>).Worker;

afterEach(() => {
  vi.restoreAllMocks();
  if (originalWindow === undefined) {
    delete (globalThis as Record<string, unknown>).window;
  } else {
    (globalThis as Record<string, unknown>).window = originalWindow;
  }
  if (originalWorker === undefined) {
    delete (globalThis as Record<string, unknown>).Worker;
  } else {
    (globalThis as Record<string, unknown>).Worker = originalWorker;
  }
});

describe("runtime/createDb driver mode", () => {
  it("throws when memory driver is used without serverUrl", async () => {
    await expect(
      createDb({
        appId: "driver-mode-no-server",
        driver: { type: "memory" },
      }),
    ).rejects.toThrow("driver.type='memory' requires serverUrl.");
  });

  it("uses the runtime supplied in DbConfig", async () => {
    const configuredRuntime = {
      load: vi.fn(async () => undefined),
      mintAnonymousToken: vi.fn(() => "anonymous-jwt"),
    };
    const configuredDb = {} as Db;
    const createSpy = vi.spyOn(Db, "create").mockResolvedValue(configuredDb);

    const result = await createDb({
      appId: "driver-mode-configured-runtime",
      runtime: configuredRuntime as never,
    });

    expect(result).toBe(configuredDb);
    expect(configuredRuntime.load).toHaveBeenCalledTimes(1);
    expect(configuredRuntime.load).toHaveBeenCalledWith(
      expect.objectContaining({
        appId: "driver-mode-configured-runtime",
      }),
    );
    expect(createSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        appId: "driver-mode-configured-runtime",
        jwtToken: "anonymous-jwt",
      }),
      configuredRuntime,
    );
  });

  it("initializes the per-Db backend through Db.create", async () => {
    const dbBackend = {
      hasWorker: false,
      createClient: vi.fn(),
      ensureReady: vi.fn(async () => undefined),
      waitForUpstreamServerConnection: vi.fn(async () => undefined),
      updateAuth: vi.fn(),
      acknowledgeRejectedBatch: vi.fn(),
      deleteClientStorage: vi.fn(async () => undefined),
      shutdown: vi.fn(async () => undefined),
    };
    const runtime = {
      supportsPolicyBypass: true,
      createBackend: vi.fn(async () => dbBackend),
    };

    const db = await Db.create(
      {
        appId: "driver-mode-backend-manager",
      },
      runtime as never,
    );

    await db.shutdown();

    expect(runtime.createBackend).toHaveBeenCalledWith(
      expect.objectContaining({
        config: expect.objectContaining({
          appId: "driver-mode-backend-manager",
        }),
        host: expect.objectContaining({
          isShuttingDown: expect.any(Function),
          onAuthFailure: expect.any(Function),
        }),
      }),
    );
    expect(dbBackend.shutdown).toHaveBeenCalledTimes(1);
  });

  it("returns configs with runtimes without trying to clone the runtime", async () => {
    const dbBackend = {
      hasWorker: false,
      createClient: vi.fn(),
      ensureReady: vi.fn(async () => undefined),
      waitForUpstreamServerConnection: vi.fn(async () => undefined),
      updateAuth: vi.fn(),
      acknowledgeRejectedBatch: vi.fn(),
      deleteClientStorage: vi.fn(async () => undefined),
      shutdown: vi.fn(async () => undefined),
    };
    const runtime = {
      load: vi.fn(async () => undefined),
      mintAnonymousToken: vi.fn(() => "anonymous-jwt"),
      createBackend: vi.fn(async () => dbBackend),
    };

    const db = await createDb({
      appId: "driver-mode-config-copy",
      runtime: runtime as never,
    });

    expect(db.getConfig()).toEqual(
      expect.objectContaining({
        appId: "driver-mode-config-copy",
        runtime,
      }),
    );

    await db.shutdown();
  });

  it("uses worker-backed path in browser when driver is persistent", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};

    const workerDb = {} as Db;
    const createSpy = vi.spyOn(Db, "create").mockResolvedValue(workerDb);

    const result = await createDb({
      appId: "driver-mode-persistent",
      driver: { type: "persistent", dbName: "driver-mode-db" },
    });

    expect(result).toBe(workerDb);
    expect(createSpy).toHaveBeenCalledTimes(1);
  });

  it("uses direct in-memory path in browser when driver is memory", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};

    const directDb = {} as Db;
    const createSpy = vi.spyOn(Db, "create").mockResolvedValue(directDb);

    const result = await createDb({
      appId: "driver-mode-memory",
      driver: { type: "memory" },
      serverUrl: "http://localhost:1625",
    });

    expect(result).toBe(directDb);
    expect(createSpy).toHaveBeenCalledTimes(1);
  });
});
