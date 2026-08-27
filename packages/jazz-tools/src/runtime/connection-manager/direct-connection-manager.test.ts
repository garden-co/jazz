import { describe, expect, it } from "vitest";
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
    await expect(manager.ensureReady("edge")).resolves.toBeUndefined();
    await expect(manager.waitForReconnect()).resolves.toBeUndefined();
  });

  it("keeps remote waits pending while explicitly offline, but local reads choose once", async () => {
    const manager = new DirectConnectionManager(host);
    await manager.disconnect();
    expect(manager.isExplicitlyOffline()).toBe(true);
    await expect(manager.ensureReady("local")).resolves.toBeUndefined();

    let remoteSettled = false;
    void manager.ensureReady("edge").then(() => (remoteSettled = true));
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
