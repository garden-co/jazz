import { describe, expect, it, vi } from "vitest";
import type { BrowserWorkerConnectionContext } from "../../runtime-source.js";
import type { DbForConnection } from "../types.js";
import {
  AuthRefreshRejectedError,
  LeaderWorkerConnectionRole,
} from "./leader-worker-connection-role.js";

describe("LeaderWorkerConnectionRole auth refresh", () => {
  it("uses the ordered worker auth-update RPC and waits for server confirmation", async () => {
    const harness = createHarness();
    const refresh = harness.role.performAuthRefresh(3, '{"jwt_token":"fresh"}', {
      sub: "alice",
    });
    await expect(refresh).resolves.toBe("authenticated");
    expect(harness.worker.updateAuth).toHaveBeenCalledTimes(1);
    expect(harness.worker.waitForServerConnection).toHaveBeenCalledTimes(1);
    expect(harness.worker.reconnect).not.toHaveBeenCalled();
  });

  it("rejects the exact active generation while server confirmation is delayed", async () => {
    let resolveServer!: () => void;
    const serverConfirmation = new Promise<void>((resolve) => {
      resolveServer = resolve;
    });
    const harness = createHarness(serverConfirmation);
    const refresh = harness.role.performAuthRefresh(4, '{"jwt_token":"bad"}', {
      sub: "alice",
    });
    await vi.waitFor(() => {
      expect(harness.worker.waitForServerConnection).toHaveBeenCalledTimes(1);
    });
    harness.workerContext.onAuthFailure("invalid");
    resolveServer();
    await expect(refresh).rejects.toEqual(new AuthRefreshRejectedError("invalid"));
  });

  it("reports auth expiry after a successful refresh as an unsolicited failure", async () => {
    const harness = createHarness();
    await expect(
      harness.role.performAuthRefresh(5, '{"jwt_token":"fresh"}', { sub: "alice" }),
    ).resolves.toBe("authenticated");

    harness.workerContext.onAuthFailure("expired");

    expect(harness.onAuthFailure).toHaveBeenCalledWith("expired", null);
  });

  it("defers a refresh disconnected before confirmation and allows replay after reconnect", async () => {
    let rejectUpdate!: (error: Error) => void;
    const blockedUpdate = new Promise<void>((_, reject) => {
      rejectUpdate = reject;
    });
    const harness = createHarness(Promise.resolve(), blockedUpdate);
    const refresh = harness.role.performAuthRefresh(6, '{"jwt_token":"fresh"}', {
      sub: "alice",
    });
    await vi.waitFor(() => expect(harness.worker.updateAuth).toHaveBeenCalledTimes(1));

    const disconnect = harness.role.disconnect();
    rejectUpdate(new Error("Auth confirmation cancelled by disconnect"));
    await disconnect;
    await expect(refresh).resolves.toBe("deferred");

    harness.worker.updateAuth.mockResolvedValueOnce(undefined);
    await harness.role.reconnect();
    await expect(
      harness.role.performAuthRefresh(6, '{"jwt_token":"fresh"}', { sub: "alice" }),
    ).resolves.toBe("authenticated");
  });
});

function createHarness(
  serverConfirmation: Promise<void> = Promise.resolve(),
  authUpdate: Promise<void> = Promise.resolve(),
) {
  let workerContext!: BrowserWorkerConnectionContext;
  const worker = {
    ready: vi.fn(async () => undefined),
    updateAuth: vi.fn(() => authUpdate),
    reconnect: vi.fn(async () => undefined),
    waitForServerConnection: vi.fn(() => serverConfirmation),
    attachFollowerPort: vi.fn(async () => undefined),
    detachFollowerPort: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    deleteStorage: vi.fn(async () => undefined),
    simulateCrash: vi.fn(async () => undefined),
    simulatePendingAuthConfirmation: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
  };
  const host = {
    config: { serverUrl: "ws://server" },
    runtimeSource: {
      createBrowserWorkerConnection(context: BrowserWorkerConnectionContext) {
        workerContext = context;
        return worker;
      },
    },
  } as unknown as DbForConnection;
  const onAuthFailure = vi.fn();
  const role = new LeaderWorkerConnectionRole(
    host,
    1,
    "worker-lock",
    () => "{}",
    () => ({}),
    {
      onFollowerPortAttached: vi.fn(),
      onFollowerPortClosed: vi.fn(),
      onReady: vi.fn(),
      onFailure: vi.fn(),
      onAuthFailure,
    },
  );
  role.onClientCreated({ schema: {}, schemaKey: "schema", client: {} } as never);
  return {
    role,
    worker,
    onAuthFailure,
    get workerContext() {
      return workerContext;
    },
  };
}
