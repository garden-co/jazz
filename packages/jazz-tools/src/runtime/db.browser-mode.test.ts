import { afterEach, describe, expect, it, vi } from "vitest";
import { Db } from "./db.js";
import { createDb } from "./default-create-db.js";

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

describe("createDb browser mode", () => {
  it("uses the dedicated-worker connection path in browser when driver is persistent", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};

    const createdDb = {} as Db;
    const createSpy = vi.spyOn(Db, "createWithBrowserWorker").mockResolvedValue(createdDb);

    const result = await createDb({
      appId: "driver-mode-persistent",
      driver: { type: "persistent", dbName: "driver-mode-db" },
    });

    expect(result).toBe(createdDb);
    expect(createSpy).toHaveBeenCalledTimes(1);
  });

  it("uses the in-memory native runtime path in browser when driver is memory", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).Worker = class {};

    const createdDb = {} as Db;
    const createSpy = vi.spyOn(Db, "create").mockReturnValue(createdDb);

    const result = await createDb({
      appId: "driver-mode-memory",
      driver: { type: "memory" },
    });

    expect(result).toBe(createdDb);
    expect(createSpy).toHaveBeenCalledTimes(1);
  });
});
