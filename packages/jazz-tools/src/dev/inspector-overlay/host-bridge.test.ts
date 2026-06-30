// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { installInspectorHost } from "./host-bridge.js";
import { INSPECTOR_HOST_GLOBAL } from "./inspector-host-types.js";

function makeFakeDb(overrides: Record<string, unknown> = {}) {
  let changeCb: () => void = () => {};
  return {
    db: {
      setDevMode: vi.fn(),
      getConfig: () => ({
        appId: "app1",
        serverUrl: "http://server",
        env: "dev",
        userBranch: "main",
        adminSecret: "sek",
      }),
      getRuntimeSchema: () => ({ todos: { columns: [] } }),
      getActiveQuerySubscriptions: () => [
        {
          id: "s1",
          query: "{}",
          table: "todos",
          branches: [],
          tier: "edge",
          propagation: "full",
          createdAt: "2026-06-30T00:00:00.000Z",
          stack: "Error\n at X",
        },
      ],
      // Mirror the real Db: the listener is invoked immediately on register.
      onActiveQuerySubscriptionsChange: (cb: () => void) => {
        changeCb = cb;
        cb();
        return () => {
          changeCb = () => {};
        };
      },
      ...overrides,
    } as unknown as import("../../runtime/db.js").Db,
    fireChange: () => changeCb(),
  };
}

afterEach(() => {
  delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
});

describe("installInspectorHost", () => {
  it("enables devMode, publishes the handle, and pushes a stack-less initial snapshot", () => {
    const posts: any[] = [];
    const iframeWindow = { postMessage: (m: any) => posts.push(m) } as unknown as Window;
    const { db } = makeFakeDb();

    installInspectorHost(db, iframeWindow, "http://localhost");

    expect((db as any).setDevMode).toHaveBeenCalledWith(true);
    const handle = (window as any)[INSPECTOR_HOST_GLOBAL];
    expect(handle.getConnectionConfig()).toMatchObject({
      appId: "app1",
      serverUrl: "http://server",
      adminSecret: "sek",
    });
    expect(handle.getWasmSchema()).toEqual({ todos: { columns: [] } });
    expect(handle.getActiveSubscriptions()[0].id).toBe("s1");
    expect("stack" in handle.getActiveSubscriptions()[0]).toBe(false);
    expect(posts).toHaveLength(1);
    expect(posts[0]).toMatchObject({ type: "jazz-inspector:subscriptions" });
    expect(posts[0].list[0].id).toBe("s1");
    expect("stack" in posts[0].list[0]).toBe(false);
  });

  it("pushes again on subscription change", () => {
    const posts: any[] = [];
    const iframeWindow = { postMessage: (m: any) => posts.push(m) } as unknown as Window;
    const fake = makeFakeDb();
    installInspectorHost(fake.db, iframeWindow, "http://localhost");
    expect(posts).toHaveLength(1);
    fake.fireChange();
    expect(posts).toHaveLength(2);
  });

  it("dispose() removes the listener and the global", () => {
    const iframeWindow = { postMessage: () => {} } as unknown as Window;
    const stop = vi.fn();
    const fake = makeFakeDb({
      onActiveQuerySubscriptionsChange: (cb: () => void) => {
        cb();
        return stop;
      },
    });
    const dispose = installInspectorHost(fake.db, iframeWindow, "http://localhost");
    expect((window as any)[INSPECTOR_HOST_GLOBAL]).toBeDefined();
    dispose();
    expect(stop).toHaveBeenCalled();
    expect((window as any)[INSPECTOR_HOST_GLOBAL]).toBeUndefined();
  });

  it("omits serverUrl when the host has none (overlay runs on local storage)", () => {
    const iframeWindow = { postMessage: () => {} } as unknown as Window;
    const fake = makeFakeDb({ getConfig: () => ({ appId: "a", dbName: "a" }) });
    installInspectorHost(fake.db, iframeWindow, "http://localhost");
    const handle = (window as any)[INSPECTOR_HOST_GLOBAL];
    const config = handle.getConnectionConfig();
    expect(config.serverUrl).toBeUndefined();
    expect(config).toMatchObject({ appId: "a", dbName: "a" });
    // Always resolves a broker URL so the overlay can join the host's store.
    expect(typeof config.brokerWorkerUrl).toBe("string");
  });
});
