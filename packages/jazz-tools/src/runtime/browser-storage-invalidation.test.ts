import { describe, expect, it, vi } from "vitest";
import { reloadAfterStorageInvalidation } from "./browser-storage-invalidation.js";

describe("reloadAfterStorageInvalidation", () => {
  it("reloads once and suppresses a rapid reload loop", () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => void values.set(key, value),
    };
    const reload = vi.fn();

    expect(reloadAfterStorageInvalidation({ now: () => 100_000, reload, storage })).toBe(true);
    expect(reloadAfterStorageInvalidation({ now: () => 100_001, reload, storage })).toBe(false);
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it("reloads when session storage is unavailable", () => {
    const reload = vi.fn();
    const storage = {
      getItem: () => {
        throw new Error("storage unavailable");
      },
      setItem: () => undefined,
    };

    expect(reloadAfterStorageInvalidation({ reload, storage })).toBe(true);
    expect(reload).toHaveBeenCalledOnce();
  });
});
