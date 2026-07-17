import { afterEach, describe, expect, it } from "vitest";
import { createBrowserTabId } from "./browser-broker-utils.js";

const originalSessionStorage = (globalThis as Record<string, unknown>).sessionStorage;

afterEach(() => {
  if (originalSessionStorage === undefined) {
    delete (globalThis as Record<string, unknown>).sessionStorage;
  } else {
    (globalThis as Record<string, unknown>).sessionStorage = originalSessionStorage;
  }
});

describe("createBrowserTabId", () => {
  it("reuses the tab identity across same-tab reloads", () => {
    const values = new Map<string, string>();
    (globalThis as Record<string, unknown>).sessionStorage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    };

    expect(createBrowserTabId()).toBe(createBrowserTabId());
  });
});
