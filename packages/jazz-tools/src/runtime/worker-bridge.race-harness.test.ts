import { describe, expect, it, vi } from "vitest";
import type { Runtime } from "./client.js";
import { WorkerBridge, type WorkerBridgeOptions } from "./worker-bridge.js";
import type { MainToWorkerMessage, WorkerToMainMessage } from "../worker/worker-protocol.js";

type ScriptOptions = {
  dropSyncBeforeInit?: boolean;
  synchronousInitOk?: boolean;
  synchronousShutdownOk?: boolean;
};

type WorkerMessageHandler = (event: MessageEvent<WorkerToMainMessage>) => void;

class FakeWorkerScript {
  private initialized = false;
  private pendingSyncPayloads: string[] = [];
  readonly receivedSyncPayloads: string[] = [];
  readonly droppedSyncPayloads: string[] = [];

  constructor(
    private readonly worker: FakeWorker,
    private readonly options: ScriptOptions = {},
  ) {}

  onMainMessage(message: MainToWorkerMessage): void {
    switch (message.type) {
      case "init":
        if (this.options.synchronousInitOk) {
          this.completeInit();
        }
        return;
      case "sync":
        if (!this.initialized) {
          if (this.options.dropSyncBeforeInit) {
            this.droppedSyncPayloads.push(message.payload);
          } else {
            this.pendingSyncPayloads.push(message.payload);
          }
          return;
        }
        this.receivedSyncPayloads.push(message.payload);
        return;
      case "shutdown": {
        const emitShutdownOk = () => {
          this.worker.emitToMain({ type: "shutdown-ok" });
        };

        if (this.options.synchronousShutdownOk) {
          emitShutdownOk();
        } else {
          queueMicrotask(emitShutdownOk);
        }
        return;
      }
      case "update-auth":
      case "simulate-crash":
        return;
    }
  }

  completeInit(clientId = "worker-client"): void {
    if (this.initialized) return;
    this.initialized = true;

    this.worker.emitToMain({ type: "init-ok", clientId });

    const pending = this.pendingSyncPayloads;
    this.pendingSyncPayloads = [];
    for (const payload of pending) {
      this.receivedSyncPayloads.push(payload);
    }
  }

  emitSyncToMain(payload: string): void {
    this.worker.emitToMain({ type: "sync", payload });
  }
}

class FakeWorker {
  onmessage: ((event: MessageEvent<WorkerToMainMessage>) => void) | null = null;
  readonly script: FakeWorkerScript;
  private readonly listeners = new Set<WorkerMessageHandler>();

  constructor(options: ScriptOptions = {}) {
    this.script = new FakeWorkerScript(this, options);
  }

  postMessage(message: MainToWorkerMessage): void {
    this.script.onMainMessage(message);
  }

  addEventListener(type: string, handler: WorkerMessageHandler): void {
    if (type === "message") {
      this.listeners.add(handler);
    }
  }

  removeEventListener(type: string, handler: WorkerMessageHandler): void {
    if (type === "message") {
      this.listeners.delete(handler);
    }
  }

  emitToMain(message: WorkerToMainMessage): void {
    const event = { data: message } as MessageEvent<WorkerToMainMessage>;
    this.onmessage?.(event);
    for (const handler of this.listeners) {
      handler(event);
    }
  }
}

function createRuntimeHarness() {
  let outboundHandler: ((envelope: string) => void) | null = null;
  const receivedFromWorker: string[] = [];

  const runtime = {
    onSyncMessageToSend(handler: (envelope: string) => void) {
      outboundHandler = handler;
    },
    onSyncMessageReceived(payload: string) {
      receivedFromWorker.push(payload);
    },
    addServer() {},
  } as unknown as Runtime;

  return {
    runtime,
    receivedFromWorker,
    emitServerPayload(payload: unknown) {
      if (!outboundHandler) {
        throw new Error("Runtime sync handler is not installed");
      }
      outboundHandler(
        JSON.stringify({
          destination: { Server: {} },
          payload,
        }),
      );
    },
  };
}

function makeBridgeOptions(): WorkerBridgeOptions {
  return {
    schemaJson: JSON.stringify({ tables: {} }),
    appId: "race-harness-app",
    env: "dev",
    userBranch: "main",
    dbName: "race-harness-db",
  };
}

describe("WorkerBridge race harness", () => {
  it("queues outbound sync until init completes", async () => {
    const worker = new FakeWorker({ dropSyncBeforeInit: true });
    const { runtime, emitServerPayload } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());

    emitServerPayload({ kind: "sub", seq: 1 });
    emitServerPayload({ kind: "sub", seq: 2 });

    expect(worker.script.droppedSyncPayloads).toEqual([]);
    expect(worker.script.receivedSyncPayloads).toEqual([]);

    worker.script.completeInit("worker-client-1");
    await expect(initPromise).resolves.toBe("worker-client-1");
    expect(bridge.getWorkerClientId()).toBe("worker-client-1");

    expect(worker.script.receivedSyncPayloads).toEqual([
      JSON.stringify({ kind: "sub", seq: 1 }),
      JSON.stringify({ kind: "sub", seq: 2 }),
    ]);
  });

  it("preserves outbound ordering across init boundary", async () => {
    const worker = new FakeWorker({ dropSyncBeforeInit: true });
    const { runtime, emitServerPayload } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());

    emitServerPayload({ kind: "sub", seq: 1 });
    emitServerPayload({ kind: "sub", seq: 2 });
    worker.script.completeInit("worker-client-2");
    await initPromise;
    emitServerPayload({ kind: "sub", seq: 3 });

    expect(worker.script.receivedSyncPayloads).toEqual([
      JSON.stringify({ kind: "sub", seq: 1 }),
      JSON.stringify({ kind: "sub", seq: 2 }),
      JSON.stringify({ kind: "sub", seq: 3 }),
    ]);
  });

  it("does not miss synchronous init-ok responses", async () => {
    vi.useFakeTimers();
    try {
      const worker = new FakeWorker({ synchronousInitOk: true });
      const { runtime } = createRuntimeHarness();
      const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

      const initPromise = bridge.init(makeBridgeOptions());
      await vi.runAllTimersAsync();

      await expect(initPromise).resolves.toBe("worker-client");
    } finally {
      vi.useRealTimers();
    }
  });

  it("forwards worker->main sync while init is pending", async () => {
    const worker = new FakeWorker();
    const { runtime, receivedFromWorker } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());
    worker.script.emitSyncToMain(JSON.stringify({ kind: "from-worker", seq: 1 }));

    expect(receivedFromWorker).toEqual([JSON.stringify({ kind: "from-worker", seq: 1 })]);

    worker.script.completeInit("worker-client-3");
    await initPromise;
  });
});
