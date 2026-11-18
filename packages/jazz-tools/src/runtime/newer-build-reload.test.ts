import { afterEach, describe, expect, it, vi } from "vitest";
import {
  newerBuildReloadAllowed,
  reloadForNewerBuild,
  setNewerBuildHandler,
} from "./newer-build-reload.js";

function sessionStorageStub() {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => void values.set(key, value),
  };
}

afterEach(() => setNewerBuildHandler(undefined));

describe("reloading for a newer build", () => {
  it("reloads at once without a handler, and not again within the loop window", () => {
    const storage = sessionStorageStub();
    const reload = vi.fn();
    let now = 1_000_000;
    expect(newerBuildReloadAllowed({ storage, now: () => now })).toBe(true);
    reloadForNewerBuild({ storage, reload, now: () => now });
    expect(reload).toHaveBeenCalledOnce();
    // Reloaded, and still an older build (a CDN serving it): don't loop.
    now += 5_000;
    expect(newerBuildReloadAllowed({ storage, now: () => now })).toBe(false);
    now += 10_000;
    expect(newerBuildReloadAllowed({ storage, now: () => now })).toBe(true);
  });

  it("hands the reload to the app's onNewerBuild, which calls it when it suits it", () => {
    const storage = sessionStorageStub();
    const reload = vi.fn();
    let later!: () => void;
    setNewerBuildHandler((go) => (later = go));
    reloadForNewerBuild({ storage, reload });
    expect(reload).not.toHaveBeenCalled();
    later();
    expect(reload).toHaveBeenCalledOnce();
  });

  it("reloads anyway when the app's handler throws", () => {
    const reload = vi.fn();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    setNewerBuildHandler(() => {
      throw new Error("app bug");
    });
    reloadForNewerBuild({ storage: sessionStorageStub(), reload });
    expect(reload).toHaveBeenCalledOnce();
    warn.mockRestore();
  });
});
