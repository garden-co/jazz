import { afterEach, describe, expect, it, vi } from "vitest";

const directWebSocketCarrierMock = vi.hoisted(() => {
  const instances: Array<{
    options: any;
    close: ReturnType<typeof vi.fn>;
    sendBatch: ReturnType<typeof vi.fn>;
    resolveReady(): void;
    rejectReady(error: unknown): void;
  }> = [];
  const autoReady = { current: true };

  class DirectWebSocketCarrier {
    readonly options: any;
    readonly close = vi.fn();
    readonly sendBatchMock = vi.fn((_frames: Uint8Array[]) => Promise.resolve());
    private readonly readyPromise: Promise<DirectWebSocketCarrier>;
    private resolveReady!: () => void;
    private rejectReady!: (error: unknown) => void;

    constructor(options: any) {
      this.options = options;
      this.readyPromise = new Promise((resolve, reject) => {
        this.resolveReady = () => resolve(this);
        this.rejectReady = reject;
      });
      instances.push({
        options,
        close: this.close,
        sendBatch: this.sendBatchMock,
        resolveReady: this.resolveReady,
        rejectReady: this.rejectReady,
      });
      if (autoReady.current) {
        this.resolveReady();
      }
    }

    ready(): Promise<DirectWebSocketCarrier> {
      return this.readyPromise;
    }

    sendBatch(frames: Uint8Array[]): Promise<void> {
      return this.sendBatchMock(frames);
    }
  }

  return { DirectWebSocketCarrier, autoReady, instances };
});

vi.mock("./core-runtime/direct-websocket.js", () => ({
  DirectWebSocketCarrier: directWebSocketCarrierMock.DirectWebSocketCarrier,
  directWireAuthFailureReason: (error: { code: string; message: string }) => {
    if (error.code !== "auth_failed") return null;
    if (error.message.includes("expired")) return "expired";
    return "invalid";
  },
}));

import { MessagePortRuntimeBridge, WorkerBridge } from "./worker-bridge.js";
import type { Runtime } from "./client.js";
import type { Session } from "./context.js";

function testPort(): MessagePort & { sent: unknown[]; emit(message: unknown): void } {
  let listener: ((event: MessageEvent) => void) | null = null;
  const sent: unknown[] = [];
  return {
    sent,
    postMessage: vi.fn((message: unknown) => {
      sent.push(message);
    }),
    addEventListener: vi.fn((_type: string, next: (event: MessageEvent) => void) => {
      listener = next;
    }),
    start: vi.fn(),
    emit(message: unknown) {
      listener?.({ data: message } as MessageEvent);
    },
  } as unknown as MessagePort & { sent: unknown[]; emit(message: unknown): void };
}

function testRuntime() {
  const transport = {
    close: vi.fn(() => true),
    recvWireFrames: vi.fn(() => []),
    sendWireFrame: vi.fn(),
    tick: vi.fn(() => 0),
  };
  const runtime = {
    connectUpstreamPeer: vi.fn(() => transport),
  } as unknown as Runtime;
  return { runtime, transport };
}

type DirectRuntimeForTest = Runtime & {
  connectUpstreamPeer: ReturnType<typeof vi.fn>;
  getDirectOpenPayload: ReturnType<typeof vi.fn>;
};

function testDirectRuntime(): {
  runtime: DirectRuntimeForTest;
  transport: ReturnType<typeof testRuntime>["transport"];
} {
  const { runtime, transport } = testRuntime();
  return {
    runtime: {
      ...runtime,
      connectUpstreamPeer: vi.fn(() => transport),
      getDirectOpenPayload: vi.fn(() => ({
        schema: new Uint8Array([1]),
        config: new Uint8Array([2]),
        peerIdentity: new Uint8Array([3, 4]),
      })),
    } as unknown as DirectRuntimeForTest,
    transport,
  };
}

function workerBridgeOptions(overrides: Partial<Parameters<WorkerBridge["init"]>[0]> = {}) {
  return {
    schemaJson: "{}",
    appId: "test-app",
    env: "dev",
    userBranch: "main",
    dbName: "test-db",
    ...overrides,
  };
}

async function flushMicrotasks() {
  await Promise.resolve();
  await Promise.resolve();
}

function testWorker(): Worker & { emit(message: unknown): void; sent: unknown[] } {
  const listeners = new Set<(event: MessageEvent) => void>();
  const sent: unknown[] = [];
  return {
    sent,
    postMessage: vi.fn((message: unknown) => {
      sent.push(message);
    }),
    addEventListener: vi.fn((_type: string, next: (event: MessageEvent) => void) => {
      listeners.add(next);
    }),
    removeEventListener: vi.fn((_type: string, next: (event: MessageEvent) => void) => {
      listeners.delete(next);
    }),
    terminate: vi.fn(),
    emit(message: unknown) {
      for (const listener of listeners) {
        listener({ data: message } as MessageEvent);
      }
    },
  } as unknown as Worker & { emit(message: unknown): void; sent: unknown[] };
}

describe("WorkerBridge", () => {
  afterEach(() => {
    directWebSocketCarrierMock.autoReady.current = true;
    directWebSocketCarrierMock.instances.splice(0);
  });

  it("memoizes in-flight init and resolves every caller with the worker client id", async () => {
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const firstInit = bridge.init(workerBridgeOptions({ appId: "memoized-app" }));
    const secondInit = bridge.init(workerBridgeOptions({ appId: "ignored-app" }));

    expect(firstInit).toBe(secondInit);
    expect(runtime.connectUpstreamPeer).toHaveBeenCalledTimes(1);
    expect(
      worker.sent.filter((message) => (message as { type?: string }).type === "init"),
    ).toHaveLength(1);

    worker.emit({ type: "init-ok", clientId: "worker-client" });

    await expect(firstInit).resolves.toBe("worker-client");
    await expect(secondInit).resolves.toBe("worker-client");
    expect(bridge.getWorkerClientId()).toBe("worker-client");
  });

  it("propagates worker init errors to every memoized init caller", async () => {
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const firstInit = bridge.init(workerBridgeOptions({ appId: "error-app" }));
    const secondInit = bridge.init(workerBridgeOptions({ appId: "ignored-error-app" }));

    worker.emit({ type: "error", message: "direct runtime failed to open" });

    await expect(firstInit).rejects.toThrow("direct runtime failed to open");
    await expect(secondInit).rejects.toThrow("direct runtime failed to open");
    expect(bridge.getWorkerClientId()).toBeNull();
  });

  it("rejects init on shutdown and ignores a stale init-ok afterwards", async () => {
    const { runtime, transport } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const initPromise = bridge.init(workerBridgeOptions({ appId: "shutdown-during-init-app" }));
    const shutdownPromise = bridge.shutdown();
    worker.emit({ type: "shutdown-ok" });
    worker.emit({ type: "init-ok", clientId: "stale-worker-client" });

    await expect(initPromise).rejects.toThrow("WorkerBridge init was shut down");
    await shutdownPromise;
    expect(bridge.getWorkerClientId()).toBeNull();
    expect(transport.close).toHaveBeenCalledTimes(1);
  });

  it("delivers worker sync frames into the main-thread runtime transport", async () => {
    const { runtime, transport } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const initPromise = bridge.init(workerBridgeOptions({ appId: "worker-to-main-sync-app" }));
    worker.emit({ type: "init-ok", clientId: "worker-client" });
    await initPromise;

    const frame = new Uint8Array([11, 12, 13]);
    worker.emit({ type: "sync", frames: [frame] });
    await flushMicrotasks();

    expect(transport.sendWireFrame).toHaveBeenCalledWith(frame);
  });

  it("posts main-thread runtime transport frames to the worker", async () => {
    const { runtime, transport } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const initPromise = bridge.init(workerBridgeOptions({ appId: "main-to-worker-sync-app" }));
    worker.emit({ type: "init-ok", clientId: "worker-client" });
    await initPromise;
    await flushMicrotasks();
    worker.sent.splice(0);
    vi.mocked(transport.recvWireFrames as () => unknown[]).mockReturnValueOnce([
      new Uint8Array([21, 22, 23]),
    ]);

    worker.emit({ type: "sync", frames: [new Uint8Array([31])] });
    await flushMicrotasks();

    expect(worker.sent).toContainEqual({
      type: "sync",
      frames: [new Uint8Array([21, 22, 23])],
    });
  });

  it("queues server frames until the page-owned carrier is ready", async () => {
    directWebSocketCarrierMock.autoReady.current = false;
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const initPromise = bridge.init(
      workerBridgeOptions({
        appId: "queued-server-frames-app",
        serverUrl: "http://localhost:4200",
      }),
    );
    worker.emit({ type: "init-ok", clientId: "worker-client" });
    await initPromise;

    const firstFrame = new Uint8Array([1, 2, 3]);
    const secondFrame = new Uint8Array([4, 5, 6]);
    worker.emit({ type: "server-out", frames: [firstFrame] });

    expect(directWebSocketCarrierMock.instances).toHaveLength(1);
    expect(directWebSocketCarrierMock.instances[0]!.sendBatch).not.toHaveBeenCalled();

    worker.emit({ type: "server-out", frames: [secondFrame] });
    expect(directWebSocketCarrierMock.instances[0]!.sendBatch).not.toHaveBeenCalled();

    directWebSocketCarrierMock.instances[0]!.resolveReady();
    await flushMicrotasks();

    expect(directWebSocketCarrierMock.instances[0]!.sendBatch).toHaveBeenCalledTimes(1);
    expect(directWebSocketCarrierMock.instances[0]!.sendBatch).toHaveBeenCalledWith([
      firstFrame,
      secondFrame,
    ]);
  });

  it("flushes queued server frames to the refreshed-auth carrier after reopen readiness", async () => {
    directWebSocketCarrierMock.autoReady.current = false;
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const initPromise = bridge.init(
      workerBridgeOptions({
        appId: "auth-refresh-queued-server-frames-app",
        serverUrl: "http://localhost:4200",
        jwtToken: "jwt-initial",
      }),
    );
    worker.emit({ type: "init-ok", clientId: "worker-client" });
    await initPromise;
    directWebSocketCarrierMock.instances[0]!.resolveReady();
    await flushMicrotasks();

    bridge.updateAuth({ jwtToken: "jwt-refresh" });
    expect(directWebSocketCarrierMock.instances).toHaveLength(2);
    expect(directWebSocketCarrierMock.instances[0]!.close).toHaveBeenCalledTimes(1);

    const frame = new Uint8Array([7, 8, 9]);
    worker.emit({ type: "server-out", frames: [frame] });

    expect(directWebSocketCarrierMock.instances[0]!.sendBatch).not.toHaveBeenCalled();
    expect(directWebSocketCarrierMock.instances[1]!.sendBatch).not.toHaveBeenCalled();

    directWebSocketCarrierMock.instances[0]!.resolveReady();
    await flushMicrotasks();
    expect(directWebSocketCarrierMock.instances[1]!.sendBatch).not.toHaveBeenCalled();

    directWebSocketCarrierMock.instances[1]!.resolveReady();
    await flushMicrotasks();

    expect(directWebSocketCarrierMock.instances[1]!.options.authJson).toBe(
      JSON.stringify({ jwt_token: "jwt-refresh" }),
    );
    expect(directWebSocketCarrierMock.instances[1]!.sendBatch).toHaveBeenCalledWith([frame]);
  });

  it("passes worker bridge auth JSON to direct websocket carriers", async () => {
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);
    const cookieSession: Session = {
      user_id: "auth-forwarding-user",
      claims: { role: "member" },
      authMode: "external",
    };

    const initPromise = bridge.init({
      schemaJson: "{}",
      appId: "auth-forwarding-app",
      env: "dev",
      userBranch: "main",
      dbName: "auth-forwarding-db",
      serverUrl: "http://localhost:4200",
      jwtToken: "jwt-initial",
      adminSecret: "admin-secret",
      backendSecret: "backend-secret",
      cookieSession,
    });
    worker.emit({ type: "init-ok", clientId: "worker-client" });

    await expect(initPromise).resolves.toBe("worker-client");
    expect(directWebSocketCarrierMock.instances).toHaveLength(1);
    expect(directWebSocketCarrierMock.instances[0]!.options).toMatchObject({
      serverUrl: "http://localhost:4200",
      appId: "auth-forwarding-app",
      peerIdentity: new Uint8Array([3, 4]),
      authJson: JSON.stringify({
        jwt_token: "jwt-initial",
        admin_secret: "admin-secret",
        backend_secret: "backend-secret",
        backend_session: cookieSession,
      }),
    });
  });

  it("uses a null jwt in direct websocket auth JSON when no token is configured", async () => {
    directWebSocketCarrierMock.instances.splice(0);
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);

    const initPromise = bridge.init({
      schemaJson: "{}",
      appId: "anonymous-app",
      env: "dev",
      userBranch: "main",
      dbName: "anonymous-db",
      serverUrl: "http://localhost:4200",
    });
    worker.emit({ type: "init-ok", clientId: "worker-client" });

    await expect(initPromise).resolves.toBe("worker-client");
    expect(directWebSocketCarrierMock.instances[0]!.options.authJson).toBe(
      JSON.stringify({ jwt_token: null }),
    );
  });

  it("reopens the page-owned direct websocket carrier with refreshed auth", async () => {
    directWebSocketCarrierMock.instances.splice(0);
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);
    const initialSession: Session = {
      user_id: "auth-refresh-user",
      claims: { role: "member" },
      authMode: "external",
    };
    const refreshedSession: Session = {
      user_id: "auth-refresh-user",
      claims: { role: "member", refresh: 1 },
      authMode: "external",
    };

    const initPromise = bridge.init({
      schemaJson: "{}",
      appId: "auth-refresh-app",
      env: "dev",
      userBranch: "main",
      dbName: "auth-refresh-db",
      serverUrl: "http://localhost:4200",
      jwtToken: "jwt-initial",
      adminSecret: "admin-secret",
      backendSecret: "backend-secret",
      cookieSession: initialSession,
    });
    worker.emit({ type: "init-ok", clientId: "worker-client" });
    await initPromise;
    worker.sent.splice(0);

    bridge.updateAuth({
      jwtToken: "jwt-refresh",
      backendSecret: "backend-secret",
      cookieSession: refreshedSession,
    });

    expect(directWebSocketCarrierMock.instances).toHaveLength(2);
    expect(directWebSocketCarrierMock.instances[0]!.close).toHaveBeenCalledTimes(1);
    expect(directWebSocketCarrierMock.instances[1]!.options.authJson).toBe(
      JSON.stringify({
        jwt_token: "jwt-refresh",
        admin_secret: "admin-secret",
        backend_secret: "backend-secret",
        backend_session: refreshedSession,
      }),
    );
    expect(worker.sent).not.toContainEqual({ type: "update-auth", jwtToken: "jwt-refresh" });
  });

  it("reports direct websocket auth errors from the page-owned carrier", async () => {
    directWebSocketCarrierMock.instances.splice(0);
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);
    const onAuthFailure = vi.fn();
    bridge.onAuthFailure(onAuthFailure);

    const initPromise = bridge.init({
      schemaJson: "{}",
      appId: "auth-error-app",
      env: "dev",
      userBranch: "main",
      dbName: "auth-error-db",
      serverUrl: "http://localhost:4200",
      jwtToken: "jwt-invalid",
    });
    worker.emit({ type: "init-ok", clientId: "worker-client" });
    await initPromise;

    directWebSocketCarrierMock.instances[0]!.options.onError({
      code: "auth_failed",
      retry: "after_auth",
      message: "token expired",
    });

    expect(onAuthFailure).toHaveBeenCalledWith("expired");
  });

  it("does not report non-auth direct websocket errors as auth failures", async () => {
    directWebSocketCarrierMock.instances.splice(0);
    const { runtime } = testDirectRuntime();
    const worker = testWorker();
    const bridge = new WorkerBridge(worker, runtime);
    const onAuthFailure = vi.fn();
    bridge.onAuthFailure(onAuthFailure);

    const initPromise = bridge.init({
      schemaJson: "{}",
      appId: "protocol-error-app",
      env: "dev",
      userBranch: "main",
      dbName: "protocol-error-db",
      serverUrl: "http://localhost:4200",
    });
    worker.emit({ type: "init-ok", clientId: "worker-client" });
    await initPromise;

    directWebSocketCarrierMock.instances[0]!.options.onError({
      code: "internal",
      retry: "later",
      message: "conflicting commit unit",
    });

    expect(onAuthFailure).not.toHaveBeenCalled();
  });
});

describe("MessagePortRuntimeBridge", () => {
  it("detaches for reconnect without shutting down the runtime sender", () => {
    const { runtime, transport } = testRuntime();
    const port = testPort();

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.detachForReconnect();

    expect(transport.close).toHaveBeenCalledTimes(1);
    expect(port.postMessage).not.toHaveBeenCalledWith({ type: "close" });
  });

  it("registers auth failure callbacks on follower data port bridges", () => {
    const { runtime } = testRuntime();
    const port = testPort();
    const onAuthFailure = vi.fn();

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.onAuthFailure(onAuthFailure);
    port.emit({ type: "auth-failure", reason: "unauthenticated" });

    expect(onAuthFailure).toHaveBeenCalledWith("unauthenticated");
  });
});
