import { afterEach, describe, expect, it, vi } from "vitest";
import { Db, createDb } from "./db.js";

vi.mock("./local-auth.js", () => ({
  resolveLocalAuthDefaults: vi.fn((config) => config),
}));

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
});
