import { describe, expect, it, vi } from "vitest";

const directWebSocketCarrierMock = vi.hoisted(() => {
  const instances: Array<{ options: any; close: ReturnType<typeof vi.fn> }> = [];

  class DirectWebSocketCarrier {
    readonly options: any;
    readonly close = vi.fn();

    constructor(options: any) {
      this.options = options;
      instances.push({ options, close: this.close });
    }

    ready(): Promise<DirectWebSocketCarrier> {
      return Promise.resolve(this);
    }

    sendBatch(): Promise<void> {
      return Promise.resolve();
    }
  }

  return { DirectWebSocketCarrier, instances };
});

vi.mock("./direct-wasm/direct-websocket.js", () => ({
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

function testDirectRuntime() {
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
    } as unknown as Runtime,
    transport,
  };
}

function testWorker(): Worker & { emit(message: unknown): void; sent: unknown[] } {
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
    removeEventListener: vi.fn(),
    terminate: vi.fn(),
    emit(message: unknown) {
      listener?.({ data: message } as MessageEvent);
    },
  } as unknown as Worker & { emit(message: unknown): void; sent: unknown[] };
}

describe("WorkerBridge", () => {
  it("passes worker bridge auth JSON to direct websocket carriers", async () => {
    directWebSocketCarrierMock.instances.splice(0);
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
  it("forwards auth updates over the follower data port bridge", () => {
    const { runtime } = testRuntime();
    const port = testPort();

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.updateAuth({ jwtToken: "jwt-refresh" });

    expect(port.postMessage).toHaveBeenCalledWith({
      type: "update-auth",
      jwtToken: "jwt-refresh",
    });
  });

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
