import { afterEach, describe, expect, it } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { INSPECTOR_HOST_GLOBAL } from "jazz-tools";
import {
  readInspectorHostConfig,
  readInspectorHostSchema,
  useHostSubscriptions,
} from "./host-link.js";

// In jsdom, window.parent === window, so installing on window.parent installs here.
function installHost(subs: unknown[] = []) {
  (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] = {
    getConnectionConfig: () => ({
      appId: "app1",
      serverUrl: "http://server",
      env: "dev",
      adminSecret: "sek",
    }),
    getWasmSchema: () => ({ todos: { columns: [] } }),
    getActiveSubscriptions: () => subs,
  };
}

afterEach(() => {
  delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
});

describe("host-link", () => {
  it("reads config + schema from the host handle, null when absent", () => {
    expect(readInspectorHostConfig()).toBeNull();
    installHost();
    expect(readInspectorHostConfig()).toMatchObject({ appId: "app1", serverUrl: "http://server" });
    expect(readInspectorHostSchema()).toEqual({ todos: { columns: [] } });
  });

  it("seeds subscriptions from the handle and updates on push", () => {
    installHost([{ id: "s1", table: "todos" }]);
    const { result } = renderHook(() => useHostSubscriptions());
    expect(result.current).toEqual([{ id: "s1", table: "todos" }]);

    act(() => {
      window.dispatchEvent(
        new MessageEvent("message", {
          data: { type: "jazz-inspector:subscriptions", list: [{ id: "s2", table: "projects" }] },
          // Real cross-window postMessage sets event.source to the sender;
          // window.parent === window in jsdom, so that's `window` here.
          source: window,
        }),
      );
    });
    expect(result.current).toEqual([{ id: "s2", table: "projects" }]);
  });

  it("ignores a subscriptions push whose source isn't the host window", () => {
    installHost([{ id: "s1", table: "todos" }]);
    const { result } = renderHook(() => useHostSubscriptions());

    act(() => {
      window.dispatchEvent(
        new MessageEvent("message", {
          data: { type: "jazz-inspector:subscriptions", list: [{ id: "s2", table: "projects" }] },
        }),
      );
    });
    expect(result.current).toEqual([{ id: "s1", table: "todos" }]);
  });

  it("returns [] when there is no host", () => {
    const { result } = renderHook(() => useHostSubscriptions());
    expect(result.current).toEqual([]);
  });

  it("ignores unrelated messages", () => {
    installHost([]);
    const { result } = renderHook(() => useHostSubscriptions());
    act(() => {
      window.dispatchEvent(new MessageEvent("message", { data: { type: "other" } }));
    });
    expect(result.current).toEqual([]);
  });
});
