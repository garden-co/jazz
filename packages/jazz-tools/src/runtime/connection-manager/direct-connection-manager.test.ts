import { describe, expect, it } from "vitest";
import type { RemoteLinkState } from "../remote-link-state.js";
import { DirectConnectionManager } from "./direct-connection-manager.js";
import type { DbForConnection } from "./types.js";
import { getTrustedReservedSession, setTrustedReservedSession } from "../db-internal-session.js";
import type { Session } from "../context.js";

const host = { config: { serverUrl: "ws://example.invalid" } } as DbForConnection;
const nextTick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("DirectConnectionManager explicit offline state", () => {
  it("preserves the private reserved-session capability in the runtime config clone", () => {
    const config = { appId: "local-first-app" };
    const session: Session = {
      issuer: "urn:jazz:local-first",
      user_id: "alice",
      claims: {},
      authMode: "local-first",
    };
    setTrustedReservedSession(config, session);
    let runtimeConfig: object | undefined;
    const manager = new DirectConnectionManager({
      config,
      runtimeSource: {
        createClient: ({ config: receivedConfig }: { config: object }) => {
          runtimeConfig = receivedConfig;
          return { onMutationError() {} };
        },
      },
      isShuttingDown: false,
      markUnauthenticated() {},
      clearAuthError() {},
      onMutationError() {},
    } as unknown as DbForConnection);

    manager.getClient({});

    expect(runtimeConfig).not.toBe(config);
    expect(getTrustedReservedSession(runtimeConfig!)).toBe(session);
  });

  it("never mistakes connecting, timeout, or slowness for explicit offline", async () => {
    const manager = new DirectConnectionManager(host);
    expect(manager.isExplicitlyOffline()).toBe(false);
    await expect(manager.ensureReady("server")).resolves.toBeUndefined();
    await expect(manager.waitForReconnect()).resolves.toBeUndefined();
  });

  it("keeps remote waits pending while explicitly offline, but local reads choose once", async () => {
    const manager = new DirectConnectionManager(host);
    await manager.disconnect();
    expect(manager.isExplicitlyOffline()).toBe(true);
    await expect(manager.ensureReady("runtime")).resolves.toBeUndefined();

    let remoteSettled = false;
    void manager.ensureReady("server").then(() => (remoteSettled = true));
    await nextTick();
    expect(remoteSettled).toBe(false);
    await manager.reconnect();
    await nextTick();
    expect(remoteSettled).toBe(true);
  });

  it("reconnect releases every waiter exactly once", async () => {
    const manager = new DirectConnectionManager(host);
    await manager.disconnect();
    let wakes = 0;
    void manager.waitForReconnect().then(() => wakes++);
    void manager.waitForReconnect().then(() => wakes++);
    await manager.reconnect();
    await nextTick();
    expect(wakes).toBe(2);
    await manager.reconnect();
    await nextTick();
    expect(wakes).toBe(2);
  });
});

describe("DirectConnectionManager lazy transport observation", () => {
  function fixture() {
    let state: RemoteLinkState = "connecting";
    const observers = new Set<() => void>();
    let bindings = 0;
    const runtime = {
      remoteLinkState: () => state,
      onRemoteLinkStateChange(listener: () => void, signal: AbortSignal) {
        bindings++;
        observers.add(listener);
        signal.addEventListener("abort", () => observers.delete(listener), { once: true });
      },
    };
    const manager = new DirectConnectionManager({
      config: { serverUrl: "ws://example.invalid", appId: "lazy-observer" },
      runtimeSource: {
        nativeConnection: { configured: () => true },
        createClient: () => ({ onMutationError() {}, getRuntime: () => runtime }),
      },
      isShuttingDown: false,
      markUnauthenticated() {},
      clearAuthError() {},
      onMutationError() {},
    } as unknown as DbForConnection);
    return {
      manager,
      bindings: () => bindings,
      emit(next: RemoteLinkState) {
        state = next;
        for (const listener of [...observers]) listener();
      },
    };
  }

  it("observes real connection loss and recovery after a listener precedes client creation", () => {
    const f = fixture();
    const controller = new AbortController();
    const states: RemoteLinkState[] = [];
    try {
      f.manager.onRemoteLinkStateChange((state) => states.push(state), controller.signal);
      expect(f.bindings()).toBe(0);
      f.manager.getClient({});
      expect(f.bindings()).toBe(1);
      expect(states).toEqual([]);
      f.emit("connected");
      f.emit("unavailable");
      f.emit("connected");
      expect(states).toEqual(["connected", "unavailable", "connected"]);
      f.manager.getClient({});
      expect(f.bindings()).toBe(1);
    } finally {
      controller.abort();
    }
  });

  it.each(["before creation", "after binding"])(
    "does not deliver after cancellation %s",
    (when) => {
      const f = fixture();
      const controller = new AbortController();
      const states: RemoteLinkState[] = [];
      f.manager.onRemoteLinkStateChange((state) => states.push(state), controller.signal);
      if (when === "after binding") f.manager.getClient({});
      controller.abort();
      f.manager.getClient({});
      f.emit("connected");
      expect(states).toEqual([]);
      expect(f.bindings()).toBe(when === "after binding" ? 1 : 0);
    },
  );
});
