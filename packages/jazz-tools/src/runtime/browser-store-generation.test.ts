import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  browserStoreGeneration,
  browserStoreGenerationChanged,
  renewBrowserStoreGeneration,
} from "./browser-store-generation.js";

let storage: Map<string, string>;
beforeEach(() => {
  storage = new Map();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => void storage.set(key, value),
    removeItem: (key: string) => void storage.delete(key),
  });
});
afterEach(() => vi.unstubAllGlobals());

describe("browser store generation", () => {
  it("is unchanged until the store is deleted, reset or invalidated", () => {
    const recorded = browserStoreGeneration("db-a");
    expect(browserStoreGeneration("db-a")).toBe(recorded);
    expect(browserStoreGenerationChanged("db-a", recorded)).toBe(false);
    renewBrowserStoreGeneration("db-b");
    expect(browserStoreGenerationChanged("db-a", recorded)).toBe(false);
    renewBrowserStoreGeneration("db-a");
    expect(browserStoreGenerationChanged("db-a", recorded)).toBe(true);
  });

  it("counts as changed once site data is cleared", () => {
    const recorded = browserStoreGeneration("db-a");
    storage.clear();
    expect(browserStoreGenerationChanged("db-a", recorded)).toBe(true);
  });

  it("counts as changed once localStorage, which worked when the tab left, throws", () => {
    const recorded = browserStoreGeneration("db-a");
    vi.stubGlobal("localStorage", {
      getItem: () => {
        throw new Error("SecurityError");
      },
    });
    expect(browserStoreGenerationChanged("db-a", recorded)).toBe(true);
  });

  it("can't tell without localStorage, and says unchanged", () => {
    vi.stubGlobal("localStorage", undefined);
    const recorded = browserStoreGeneration("db-a");
    expect(recorded).toBeNull();
    expect(browserStoreGenerationChanged("db-a", recorded)).toBe(false);
  });
});
