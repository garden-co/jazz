import { describe, expect, it } from "vitest";

import type { Runtime } from "./client.js";
import { LeaderMigratedError, WorkerBridge, type WorkerBridgeOptions } from "./worker-bridge.js";

class FakeMessagePortEndpoint {
  readonly posted: unknown[] = [];
  readonly listeners = new Set<(event: MessageEvent) => void>();

  postMessage(message: unknown): void {
    this.posted.push(message);
  }

  addEventListener(type: "message", listener: (event: MessageEvent) => void): void {
    if (type === "message") {
      this.listeners.add(listener);
    }
  }

  removeEventListener(type: "message", listener: (event: MessageEvent) => void): void {
    if (type === "message") {
      this.listeners.delete(listener);
    }
  }

  start(): void {}

  close(): void {}
}

function workerBridgeOptions(): WorkerBridgeOptions {
  return {
    schemaJson: "{}",
    appId: "test-app",
    env: "dev",
    userBranch: "main",
    dbName: "test-db",
  };
}

function fakeRuntime(overrides: Partial<Record<string, unknown>> = {}): Runtime {
  return {
    createWorkerBridge() {
      return {
        init: async () => ({ clientId: "worker-client" }),
        updateAuth() {},
        sendLifecycleHint() {},
        setServerPayloadForwarder() {},
        waitForUpstreamServerConnection: async () => {},
        waitForLocalSyncFlush: async () => {},
        replayServerConnection() {},
        disconnectUpstream() {},
        reconnectUpstream() {},
        acknowledgeRejectedBatch() {},
        simulateCrash: async () => {},
        setListeners() {},
        shutdown: async () => {},
        getWorkerClientId: () => "worker-client",
        ...overrides,
      };
    },
  } as unknown as Runtime;
}

describe("WorkerBridge endpoint", () => {
  it("accepts a MessagePort-shaped endpoint", async () => {
    const endpoint = new FakeMessagePortEndpoint();
    let attachedEndpoint: unknown = null;

    const runtime = {
      createWorkerBridge(target: unknown) {
        attachedEndpoint = target;
        return {
          init: async () => ({ clientId: "worker-client" }),
          updateAuth() {},
          sendLifecycleHint() {},
          setServerPayloadForwarder() {},
          waitForUpstreamServerConnection: async () => {},
          waitForLocalSyncFlush: async () => {},
          replayServerConnection() {},
          disconnectUpstream() {},
          reconnectUpstream() {},
          acknowledgeRejectedBatch() {},
          simulateCrash: async () => {},
          setListeners() {},
          shutdown: async () => {},
          getWorkerClientId: () => "worker-client",
        };
      },
    } as unknown as Runtime;

    const bridge = new WorkerBridge(endpoint, runtime);

    await expect(bridge.init(workerBridgeOptions())).resolves.toBe("worker-client");
    expect(attachedEndpoint).toBe(endpoint);
  });
});

describe("WorkerBridge leader migration", () => {
  it("rejects in-flight waitForLocalSyncFlush with LeaderMigratedError on notifyMigrated()", async () => {
    const endpoint = new FakeMessagePortEndpoint();
    // The Rust-side waiter never settles in this fake; only `notifyMigrated`
    // should resolve the JS-visible promise.
    const stuck = new Promise<void>(() => undefined);
    const runtime = fakeRuntime({ waitForLocalSyncFlush: () => stuck });
    const bridge = new WorkerBridge(endpoint, runtime);
    await bridge.init(workerBridgeOptions());

    const inflight = bridge.waitForLocalSyncFlush("batch-1");
    let settled = false;
    inflight
      .catch(() => undefined)
      .finally(() => {
        settled = true;
      });
    await new Promise((r) => setTimeout(r, 0));
    expect(settled).toBe(false);

    bridge.notifyMigrated();

    await expect(inflight).rejects.toBeInstanceOf(LeaderMigratedError);
    await expect(inflight).rejects.toMatchObject({ code: "leader-migrated" });
  });

  it("rejects subsequent waits immediately after notifyMigrated()", async () => {
    const endpoint = new FakeMessagePortEndpoint();
    const runtime = fakeRuntime();
    const bridge = new WorkerBridge(endpoint, runtime);
    await bridge.init(workerBridgeOptions());
    bridge.notifyMigrated();

    await expect(bridge.waitForLocalSyncFlush("batch-2")).rejects.toBeInstanceOf(
      LeaderMigratedError,
    );
    await expect(bridge.waitForUpstreamServerConnection()).rejects.toBeInstanceOf(
      LeaderMigratedError,
    );
  });

  it("notifyMigrated() is idempotent and exposes isMigrated()", async () => {
    const endpoint = new FakeMessagePortEndpoint();
    const bridge = new WorkerBridge(endpoint, fakeRuntime());
    await bridge.init(workerBridgeOptions());

    expect(bridge.isMigrated()).toBe(false);
    bridge.notifyMigrated();
    expect(bridge.isMigrated()).toBe(true);
    bridge.notifyMigrated(); // no-op
    expect(bridge.isMigrated()).toBe(true);
  });

  it("shutdown() also drains pending waiters with LeaderMigratedError", async () => {
    const endpoint = new FakeMessagePortEndpoint();
    const stuck = new Promise<void>(() => undefined);
    const runtime = fakeRuntime({ waitForLocalSyncFlush: () => stuck });
    const bridge = new WorkerBridge(endpoint, runtime);
    await bridge.init(workerBridgeOptions());

    const inflight = bridge.waitForLocalSyncFlush("batch-3");
    void bridge.shutdown();

    await expect(inflight).rejects.toBeInstanceOf(LeaderMigratedError);
  });
});
