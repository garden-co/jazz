import { afterEach, describe, expect, it, vi } from "vitest";
import type { JazzClient } from "./client.js";
import {
  IncompatibleBrowserBrokerConfigurationError,
  type IncompatibleBrowserBrokerConfigurationHandler,
} from "./browser-broker-errors.js";
import { Db, createDb, createDbWithRuntimeModule, type DbConfig } from "./db.js";
import { DbRuntimeModule, type DbRuntimeClientContext } from "./db-runtime-module.js";

const originalWindow = (globalThis as Record<string, unknown>).window;
const originalWorker = (globalThis as Record<string, unknown>).Worker;

class TestRuntimeModule extends DbRuntimeModule<DbConfig> {
  protected override async loadRuntime(): Promise<void> {
    return;
  }

  override createClient(_context: DbRuntimeClientContext<DbConfig>): JazzClient {
    throw new Error("createClient should not be called by driver mode tests");
  }
}

function installFakeBrowserWindow(confirm: (message?: string) => boolean): {
  confirm: ReturnType<typeof vi.fn<(message?: string) => boolean>>;
  reload: ReturnType<typeof vi.fn<() => void>>;
} {
  const confirmMock = vi.fn(confirm);
  const reload = vi.fn<() => void>();
  (globalThis as Record<string, unknown>).window = {
    confirm: confirmMock,
    location: { reload },
  };
  (globalThis as Record<string, unknown>).Worker = class {};
  return { confirm: confirmMock, reload };
}

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

  it("uses worker-backed path in browser when driver is persistent", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};

    const workerDb = {} as Db;
    const createWithWorkerSpy = vi.spyOn(Db, "createWithWorker").mockResolvedValue(workerDb);
    const createSpy = vi.spyOn(Db, "create").mockResolvedValue({} as Db);

    const result = await createDb({
      appId: "driver-mode-persistent",
      driver: { type: "persistent", dbName: "driver-mode-db" },
    });

    expect(result).toBe(workerDb);
    expect(createWithWorkerSpy).toHaveBeenCalledTimes(1);
    expect(createSpy).not.toHaveBeenCalled();
  });

  it("uses direct in-memory path in browser when driver is memory", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};

    const directDb = {} as Db;
    const createWithWorkerSpy = vi.spyOn(Db, "createWithWorker").mockResolvedValue({} as Db);
    const createSpy = vi.spyOn(Db, "create").mockResolvedValue(directDb);

    const result = await createDb({
      appId: "driver-mode-memory",
      driver: { type: "memory" },
      serverUrl: "http://localhost:1625",
    });

    expect(result).toBe(directDb);
    expect(createSpy).toHaveBeenCalledTimes(1);
    expect(createWithWorkerSpy).not.toHaveBeenCalled();
  });

  it("shows the default browser broker compatibility prompt and reloads when confirmed", async () => {
    const { confirm, reload } = installFakeBrowserWindow(() => true);
    const error = new IncompatibleBrowserBrokerConfigurationError();
    vi.spyOn(Db, "createWithWorker").mockRejectedValue(error);

    await expect(
      createDbWithRuntimeModule(
        {
          appId: "driver-mode-incompatible-broker",
          driver: { type: "persistent", dbName: "driver-mode-db" },
          jwtToken: "jwt",
        },
        new TestRuntimeModule(),
      ),
    ).rejects.toBe(error);

    expect(confirm).toHaveBeenCalledTimes(1);
    expect(confirm.mock.calls[0]?.[0]).toContain("different version of this app");
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it("lets apps override the browser broker compatibility prompt", async () => {
    const { confirm, reload } = installFakeBrowserWindow(() => true);
    const error = new IncompatibleBrowserBrokerConfigurationError();
    const onIncompatibleBrowserBrokerConfiguration =
      vi.fn<IncompatibleBrowserBrokerConfigurationHandler>();
    vi.spyOn(Db, "createWithWorker").mockRejectedValue(error);

    await expect(
      createDbWithRuntimeModule(
        {
          appId: "driver-mode-incompatible-broker-override",
          driver: { type: "persistent", dbName: "driver-mode-db" },
          jwtToken: "jwt",
          onIncompatibleBrowserBrokerConfiguration,
        },
        new TestRuntimeModule(),
      ),
    ).rejects.toBe(error);

    expect(onIncompatibleBrowserBrokerConfiguration).toHaveBeenCalledTimes(1);
    expect(onIncompatibleBrowserBrokerConfiguration).toHaveBeenCalledWith(error);
    expect(confirm).not.toHaveBeenCalled();
    expect(reload).not.toHaveBeenCalled();
  });
});
