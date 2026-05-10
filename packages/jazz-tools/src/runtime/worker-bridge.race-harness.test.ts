import { describe, expect, it, vi } from "vitest";
import type { Runtime } from "./client.js";
import { WorkerBridge, type WorkerBridgeOptions } from "./worker-bridge.js";
import type { MainToWorkerMessage, WorkerToMainMessage } from "../worker/worker-protocol.js";

type ScriptOptions = {
  dropSyncBeforeInit?: boolean;
  initAckMode?: "manual" | "sync-ok" | "async-ok" | "sync-error" | "async-error";
  shutdownAckMode?: "sync-ok" | "async-ok" | "timeout";
  initErrorMessage?: string;
};

type WorkerMessageHandler = (event: MessageEvent<WorkerToMainMessage>) => void;

class FakeWorkerScript {
  private initialized = false;
  private pendingSyncPayloads: Uint8Array[] = [];
  readonly receivedSyncPayloads: Uint8Array[] = [];
  readonly droppedSyncPayloads: Uint8Array[] = [];
  initMessageCount = 0;
  shutdownMessageCount = 0;

  constructor(
    private readonly worker: FakeWorker,
    private readonly options: ScriptOptions = {},
  ) {}

  onMainMessage(message: MainToWorkerMessage): void {
    switch (message.type) {
      case "init":
        this.initMessageCount += 1;
        this.handleInit();
        return;
      case "sync":
        if (!this.initialized) {
          if (this.options.dropSyncBeforeInit) {
            this.droppedSyncPayloads.push(...message.payload);
          } else {
            this.pendingSyncPayloads.push(...message.payload);
          }
          return;
        }
        this.receivedSyncPayloads.push(...message.payload);
        return;
      case "shutdown":
        this.shutdownMessageCount += 1;
        this.handleShutdown();
        return;
      case "update-auth":
      case "simulate-crash":
        return;
    }
  }

  private handleInit(): void {
    const mode = this.options.initAckMode ?? "manual";
    switch (mode) {
      case "sync-ok":
        this.completeInit();
        return;
      case "async-ok":
        queueMicrotask(() => this.completeInit());
        return;
      case "sync-error":
        this.failInit();
        return;
      case "async-error":
        queueMicrotask(() => this.failInit());
        return;
      case "manual":
        return;
    }
  }

  private handleShutdown(): void {
    const mode = this.options.shutdownAckMode ?? "async-ok";
    if (mode === "timeout") {
      return;
    }

    const emitShutdownOk = () => {
      this.worker.emitToMain({ type: "shutdown-ok" });
    };

    if (mode === "sync-ok") {
      emitShutdownOk();
      return;
    }

    queueMicrotask(emitShutdownOk);
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

  failInit(message = this.options.initErrorMessage ?? "init failed"): void {
    this.worker.emitToMain({ type: "error", message });
  }

  emitSyncToMain(...payloads: Uint8Array[]): void {
    this.worker.emitToMain({ type: "sync", payload: payloads });
  }

  emitUpstreamConnected(): void {
    this.worker.emitToMain({ type: "upstream-connected" });
  }

  emitUpstreamDisconnected(): void {
    this.worker.emitToMain({ type: "upstream-disconnected" });
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
  let outboundHandler: ((...args: unknown[]) => void) | null = null;
  const receivedFromWorker: Uint8Array[] = [];
  const receivedSequences: Array<number | null | undefined> = [];
  let batchedTickCount = 0;

  const runtime = {
    onSyncMessageToSend(handler: (...args: unknown[]) => void) {
      outboundHandler = handler;
    },
    onSyncMessageReceived(payload: Uint8Array, sequence?: number | null) {
      receivedFromWorker.push(payload);
      receivedSequences.push(sequence);
    },
    batchedTick() {
      batchedTickCount += 1;
    },
    addServer() {},
    removeServer() {},
  } as unknown as Runtime;

  return {
    runtime,
    receivedFromWorker,
    receivedSequences,
    getBatchedTickCount: () => batchedTickCount,
    emitServerPayload(payload: unknown) {
      if (!outboundHandler) {
        throw new Error("Runtime sync handler is not installed");
      }
      outboundHandler(
        "server",
        "server-1",
        new TextEncoder().encode(JSON.stringify(payload)),
        false,
      );
    },
  };
}

function makeBridgeOptions(): WorkerBridgeOptions {
  return {
    schemaJson: JSON.stringify({}),
    appId: "race-harness-app",
    env: "dev",
    userBranch: "main",
    dbName: "race-harness-db",
  };
}

describe("WorkerBridge race harness", () => {
  const enc = (value: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(value));

  it("WB-U01 queues outbound sync until init completes", async () => {
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
      enc({ kind: "sub", seq: 1 }),
      enc({ kind: "sub", seq: 2 }),
    ]);
  });

  it("WB-U02 preserves outbound ordering across init boundary", async () => {
    const worker = new FakeWorker({ dropSyncBeforeInit: true });
    const { runtime, emitServerPayload } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());

    emitServerPayload({ kind: "sub", seq: 1 });
    emitServerPayload({ kind: "sub", seq: 2 });
    worker.script.completeInit("worker-client-2");
    await initPromise;
    emitServerPayload({ kind: "sub", seq: 3 });
    await Promise.resolve();

    expect(worker.script.receivedSyncPayloads).toEqual([
      enc({ kind: "sub", seq: 1 }),
      enc({ kind: "sub", seq: 2 }),
      enc({ kind: "sub", seq: 3 }),
    ]);
  });

  it("WB-U03 does not miss synchronous init-ok responses", async () => {
    vi.useFakeTimers();
    try {
      const worker = new FakeWorker({ initAckMode: "sync-ok" });
      const { runtime } = createRuntimeHarness();
      const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

      const initPromise = bridge.init(makeBridgeOptions());
      await vi.runAllTimersAsync();

      await expect(initPromise).resolves.toBe("worker-client");
    } finally {
      vi.useRealTimers();
    }
  });

  it("WB-U04 forwards worker->main sync while init is pending", async () => {
    const worker = new FakeWorker();
    const { runtime, receivedFromWorker } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());
    worker.script.emitSyncToMain(enc({ kind: "from-worker", seq: 1 }));

    expect(receivedFromWorker).toEqual([enc({ kind: "from-worker", seq: 1 })]);

    worker.script.completeInit("worker-client-3");
    await initPromise;
  });

  it("WB-U04a wakes the main runtime after worker->main sync batches", () => {
    const worker = new FakeWorker();
    const { runtime, receivedFromWorker, receivedSequences, getBatchedTickCount } =
      createRuntimeHarness();
    new WorkerBridge(worker as unknown as Worker, runtime);

    const first = enc({ kind: "from-worker", seq: 1 });
    const second = enc({ kind: "from-worker", seq: 2 });

    worker.emitToMain({
      type: "sync",
      payload: [first, { payload: second, sequence: 7 }],
    });

    expect(receivedFromWorker).toEqual([first, second]);
    expect(receivedSequences).toEqual([undefined, 7]);
    expect(getBatchedTickCount()).toBe(1);
  });

  it("WB-U04b waits for a direct upstream connection before resolving edge readiness", async () => {
    const worker = new FakeWorker({ initAckMode: "sync-ok" });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    await bridge.init({
      ...makeBridgeOptions(),
      serverUrl: "https://example.test",
    });

    let resolved = false;
    const waitPromise = bridge.waitForUpstreamServerConnection().then(() => {
      resolved = true;
    });

    await Promise.resolve();
    expect(resolved).toBe(false);

    worker.script.emitUpstreamConnected();
    await waitPromise;
    expect(resolved).toBe(true);
  });

  it("WB-U04c skips upstream waiting when server payloads route through another tab", async () => {
    const worker = new FakeWorker({ initAckMode: "sync-ok" });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    await bridge.init({
      ...makeBridgeOptions(),
      serverUrl: "https://example.test",
    });

    bridge.setServerPayloadForwarder(() => {});
    await expect(bridge.waitForUpstreamServerConnection()).resolves.toBeUndefined();
  });

  it("WB-U05 init memoizes and returns the same in-flight promise", async () => {
    const worker = new FakeWorker();
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromiseA = bridge.init(makeBridgeOptions());
    const initPromiseB = bridge.init(makeBridgeOptions());

    expect(initPromiseA).toBe(initPromiseB);
    expect(worker.script.initMessageCount).toBe(1);

    worker.script.completeInit("worker-client-5");
    await expect(initPromiseA).resolves.toBe("worker-client-5");
    await expect(initPromiseB).resolves.toBe("worker-client-5");
  });

  it("WB-U06 init failure transitions state and preserves queued sync", async () => {
    const worker = new FakeWorker();
    const { runtime, emitServerPayload } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());
    emitServerPayload({ kind: "sub", seq: 1 });
    emitServerPayload({ kind: "sub", seq: 2 });

    worker.script.failInit("boom");
    await expect(initPromise).rejects.toThrow("Worker init failed: boom");

    expect((bridge as any).state.phase).toBe("failed");
    expect((bridge as any).state.pendingSyncPayloadsForWorker).toEqual([
      enc({ kind: "sub", seq: 1 }),
      enc({ kind: "sub", seq: 2 }),
    ]);
    expect(worker.script.receivedSyncPayloads).toEqual([]);
  });

  it("WB-U09 init times out after the bridge timeout window", async () => {
    vi.useFakeTimers();
    try {
      const worker = new FakeWorker({ initAckMode: "manual" });
      const { runtime } = createRuntimeHarness();
      const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

      const initErrorPromise = bridge.init(makeBridgeOptions()).then(
        () => new Error("Expected init to timeout"),
        (error: unknown) => (error instanceof Error ? error : new Error(String(error))),
      );
      await vi.advanceTimersByTimeAsync(12_001);

      const initError = await initErrorPromise;
      expect(initError.message).toContain("Worker init timeout");
      expect((bridge as any).state.phase).toBe("failed");
    } finally {
      vi.useRealTimers();
    }
  });

  it("WB-U07 handles synchronous shutdown-ok acknowledgements", async () => {
    const worker = new FakeWorker({ shutdownAckMode: "sync-ok" });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    await expect(bridge.shutdown(worker as unknown as Worker)).resolves.toBeUndefined();
    expect(worker.script.shutdownMessageCount).toBe(1);
  });

  it("WB-U08 does not throw when shutdown acknowledgement times out", async () => {
    vi.useFakeTimers();
    try {
      const worker = new FakeWorker({ shutdownAckMode: "timeout" });
      const { runtime } = createRuntimeHarness();
      const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

      const shutdownPromise = bridge.shutdown(worker as unknown as Worker);
      await vi.advanceTimersByTimeAsync(5001);

      await expect(shutdownPromise).resolves.toBeUndefined();
      expect(worker.script.shutdownMessageCount).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
