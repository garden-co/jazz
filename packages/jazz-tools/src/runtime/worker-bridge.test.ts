import { describe, expect, it } from "vitest";

import type { Runtime } from "./client.js";
import { WorkerBridge, type WorkerBridgeOptions } from "./worker-bridge.js";

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
