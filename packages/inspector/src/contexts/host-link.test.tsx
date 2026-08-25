import { afterEach, describe, expect, it, vi } from "vitest";
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
  Object.defineProperty(window, "opener", { configurable: true, value: null });
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
          origin: window.location.origin,
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

  it("reads from and registers a detached window with its opener host", () => {
    const registerInspectorWindow = vi.fn();
    const unregisterInspectorWindow = vi.fn();
    const opener = {
      [INSPECTOR_HOST_GLOBAL]: {
        getConnectionConfig: () => ({ appId: "detached" }),
        getWasmSchema: () => ({ todos: { columns: [] } }),
        getActiveSubscriptions: () => [{ id: "s1", table: "todos" }],
        registerInspectorWindow,
        unregisterInspectorWindow,
      },
    } as unknown as Window;
    const previousOpener = window.opener;
    Object.defineProperty(window, "opener", { configurable: true, value: opener });

    const { result, unmount } = renderHook(() => useHostSubscriptions());

    expect(readInspectorHostConfig()).toMatchObject({ appId: "detached" });
    expect(result.current).toEqual([{ id: "s1", table: "todos" }]);
    expect(registerInspectorWindow).toHaveBeenCalledWith(window);

    act(() => {
      window.dispatchEvent(
        new MessageEvent("message", {
          data: { type: "jazz-inspector:subscriptions", list: [{ id: "s2", table: "projects" }] },
          origin: window.location.origin,
          source: opener,
        }),
      );
    });
    expect(result.current).toEqual([{ id: "s2", table: "projects" }]);

    unmount();
    expect(unregisterInspectorWindow).toHaveBeenCalledWith(window);
    Object.defineProperty(window, "opener", { configurable: true, value: previousOpener });
  });

  it("falls back to the parent host when an opener's parent is cross-origin", () => {
    installHost();
    const opener = Object.create(null, {
      parent: {
        get() {
          throw new DOMException("Blocked", "SecurityError");
        },
      },
    }) as Window;
    Object.defineProperty(window, "opener", { configurable: true, value: opener });

    expect(readInspectorHostConfig()).toMatchObject({ appId: "app1" });
  });
});
