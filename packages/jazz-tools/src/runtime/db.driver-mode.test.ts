import { afterEach, describe, expect, it, vi } from "vitest";
import { Db, createDb } from "./db.js";

const originalWindow = (globalThis as Record<string, unknown>).window;
const originalWorker = (globalThis as Record<string, unknown>).Worker;
const originalSharedWorker = (globalThis as Record<string, unknown>).SharedWorker;
const originalNavigator = (globalThis as Record<string, unknown>).navigator;

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
  if (originalSharedWorker === undefined) {
    delete (globalThis as Record<string, unknown>).SharedWorker;
  } else {
    (globalThis as Record<string, unknown>).SharedWorker = originalSharedWorker;
  }
  if (originalNavigator === undefined) {
    delete (globalThis as Record<string, unknown>).navigator;
  } else {
    Object.defineProperty(globalThis, "navigator", {
      value: originalNavigator,
      configurable: true,
    });
  }
});

function stubNavigatorLocks(): void {
  Object.defineProperty(globalThis, "navigator", {
    value: { locks: { request: async () => undefined } },
    configurable: true,
  });
}

describe("runtime/createDb driver mode", () => {
  it("throws when memory driver is used without serverUrl", async () => {
    await expect(
      createDb({
        appId: "driver-mode-no-server",
        driver: { type: "memory" },
      }),
    ).rejects.toThrow("driver.type='memory' requires serverUrl.");
  });

  it("uses the SharedWorker-backed path in browser when driver is persistent", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};
    (globalThis as Record<string, unknown>).SharedWorker = class {};
    stubNavigatorLocks();

    const sharedWorkerDb = {} as Db;
    const createWithSharedWorkerSpy = vi
      .spyOn(Db, "createWithSharedWorker")
      .mockResolvedValue(sharedWorkerDb);
    const createSpy = vi.spyOn(Db, "create").mockResolvedValue({} as Db);

    const result = await createDb({
      appId: "driver-mode-shared-worker",
      driver: { type: "persistent", dbName: "driver-mode-shared-worker-db" },
    });

    expect(result).toBe(sharedWorkerDb);
    expect(createWithSharedWorkerSpy).toHaveBeenCalledTimes(1);
    expect(createSpy).not.toHaveBeenCalled();
  });

  it("throws in browser with persistent driver when SharedWorker is unavailable", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};
    delete (globalThis as Record<string, unknown>).SharedWorker;
    stubNavigatorLocks();

    await expect(
      createDb({
        appId: "driver-mode-shared-worker-missing",
        driver: { type: "persistent", dbName: "driver-mode-shared-worker-missing-db" },
      }),
    ).rejects.toThrow(/This browser does not support SharedWorker.*Please update your browser/);
  });

  it("throws in browser with persistent driver when navigator.locks is unavailable", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};
    (globalThis as Record<string, unknown>).SharedWorker = class {};
    delete (globalThis as Record<string, unknown>).navigator;

    await expect(
      createDb({
        appId: "driver-mode-locks-missing",
        driver: { type: "persistent", dbName: "driver-mode-locks-missing-db" },
      }),
    ).rejects.toThrow(/navigator\.locks/);
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
