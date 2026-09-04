// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { installInspectorHost } from "./host-bridge.js";
import { INSPECTOR_HOST_GLOBAL } from "./inspector-host-types.js";
import { resolveDefaultPersistentDbName } from "../../runtime/db.js";

function makeFakeDb(overrides: Record<string, unknown> = {}) {
  let changeCb: () => void = () => {};
  return {
    db: {
      setDevMode: vi.fn(),
      getConfig: () => ({
        appId: "app1",
        serverUrl: "http://server",
        env: "dev",
        adminSecret: "sek",
      }),
      getRuntimeSchema: () => ({ todos: { columns: [] } }),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
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
    expect(handle.getConnectionConfig().appId).toBe("app1");
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

  it("pushes subscriptions to a registered detached inspector window", () => {
    const iframePosts: any[] = [];
    const popupPosts: any[] = [];
    const iframeWindow = { postMessage: (m: any) => iframePosts.push(m) } as unknown as Window;
    const popupWindow = { postMessage: (m: any) => popupPosts.push(m) } as unknown as Window;
    const fake = makeFakeDb();

    installInspectorHost(fake.db, iframeWindow, "http://localhost");
    const handle = (window as any)[INSPECTOR_HOST_GLOBAL];
    handle.registerInspectorWindow(popupWindow);
    fake.fireChange();

    expect(iframePosts).toHaveLength(2);
    expect(popupPosts).toHaveLength(1);

    handle.unregisterInspectorWindow(popupWindow);
    fake.fireChange();
    expect(popupPosts).toHaveLength(1);
  });

  it("keeps detached inspector windows across host rebinding", () => {
    const inspectorWindows = new Set<Window>();
    const iframeWindow = { postMessage: vi.fn() } as unknown as Window;
    const popupWindow = { postMessage: vi.fn() } as unknown as Window;
    const first = makeFakeDb();
    const dispose = installInspectorHost(
      first.db,
      iframeWindow,
      "http://localhost",
      inspectorWindows,
    );
    const handle = (window as any)[INSPECTOR_HOST_GLOBAL];
    handle.registerInspectorWindow(popupWindow);

    dispose();
    const second = makeFakeDb({
      onActiveQuerySubscriptionsChange: () => vi.fn(),
    });
    installInspectorHost(second.db, iframeWindow, "http://localhost", inspectorWindows);

    expect(popupWindow.postMessage).toHaveBeenCalledOnce();
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

  it("publishes the exact non-secret host physical namespace for the overlay", async () => {
    const iframeWindow = { postMessage: () => {} } as unknown as Window;
    const fake = makeFakeDb({
      getConfig: () => ({
        appId: "a",
        dbName: "a",
        serverUrl: "http://server",
        secret: "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        cookieSession: { user_id: "u1" },
        adminSecret: "adm",
      }),
    });
    installInspectorHost(fake.db, iframeWindow, "http://localhost");
    const config = (window as any)[INSPECTOR_HOST_GLOBAL].getConnectionConfig();
    expect(config).toMatchObject({
      appId: "a",
      serverUrl: "http://server",
      secret: "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      adminSecret: "adm",
      driver: { type: "persistent", dbName: "a" },
    });
    expect(config.cookieSession).toBeUndefined();
    const expectedPhysicalDbName = resolveDefaultPersistentDbName((fake.db as any).getConfig());
    expect(config.runtimeSources).toEqual({
      inspectorHostPhysicalDbName: expectedPhysicalDbName,
    });
    expect(resolveDefaultPersistentDbName(config)).toBe(expectedPhysicalDbName);
    expect(decodeURIComponent(config.runtimeSources!.inspectorHostPhysicalDbName!)).toContain(
      '"auth":{"kind":"system"}',
    );
    expect(JSON.stringify(config.runtimeSources)).not.toContain(config.secret);
    expect(JSON.stringify(config.runtimeSources)).not.toContain(config.adminSecret);
    await (window as any)[INSPECTOR_HOST_GLOBAL].openControlPort();
    expect((fake.db as any).openInspectorControlPort).toHaveBeenCalledOnce();
  });

  it("forwards a resolved local-first session from the host JWT when private session state is unavailable", () => {
    const iframeWindow = { postMessage: () => {} } as unknown as Window;
    const fake = makeFakeDb({
      getConfig: () => ({
        appId: "a",
        serverUrl: "http://server",
        jwtToken:
          "header.eyJzdWIiOiJpbnNwZWN0b3ItdGVzdC11c2VyIiwiaXNzIjoidXJuOmpheno6bG9jYWwtZmlyc3QiLCJjbGFpbXMiOnsicm9sZSI6Imluc3BlY3Rvci10ZXN0In19.signature",
      }),
    });

    installInspectorHost(fake.db, iframeWindow, "http://localhost");

    expect((window as any)[INSPECTOR_HOST_GLOBAL].getConnectionConfig()).toMatchObject({
      jwtToken:
        "header.eyJzdWIiOiJpbnNwZWN0b3ItdGVzdC11c2VyIiwiaXNzIjoidXJuOmpheno6bG9jYWwtZmlyc3QiLCJjbGFpbXMiOnsicm9sZSI6Imluc3BlY3Rvci10ZXN0In19.signature",
      runtimeSources: {
        browserWorkerSession: {
          issuer: "urn:jazz:local-first",
          user_id: "inspector-test-user",
          authMode: "local-first",
          claims: { role: "inspector-test" },
        },
      },
    });
  });
});
