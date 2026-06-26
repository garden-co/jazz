import type { Runtime } from "./client.js";
import type { RuntimeSourcesConfig } from "./context.js";
import type { AuthFailureReason } from "./sync-transport.js";

/** Page lifecycle hint forwarded to the worker runtime. */
export type WorkerLifecycleEvent =
  | "visibility-hidden"
  | "visibility-visible"
  | "pagehide"
  | "freeze"
  | "resume";

export interface WorkerBridgeOptions {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  workerLockName?: string;
  leadershipId?: number;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
  directOpen?: DirectOpenPayload;
}

export interface PeerSyncBatch {
  peerId: string;
  leadershipId: number;
  payload: Uint8Array[];
}

export interface FollowerPortEvent {
  peerId: string;
  leadershipId: number;
}

interface ListenerSlots {
  onPeerSync?: (batch: PeerSyncBatch) => void;
  onAuthFailure?: (reason: AuthFailureReason) => void;
  onFollowerPortAttached?: (event: FollowerPortEvent) => void;
  onFollowerPortClosed?: (event: FollowerPortEvent) => void;
}

type ServerPayloadForwarder = (payload: Uint8Array) => void;

export interface DirectOpenPayload {
  schema: Uint8Array;
  config: Uint8Array;
}

interface DirectTransport {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  tick(): number;
}

interface RuntimeWithDirectTransport extends Runtime {
  connectUpstreamPeer?(): DirectTransport;
  getDirectOpenPayload?(): DirectOpenPayload;
}

type WorkerInbound =
  | { type: "init"; options: WorkerBridgeOptions }
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "server-in"; frame: Uint8Array }
  | { type: "update-auth"; jwtToken?: string | null }
  | { type: "lifecycle"; event: WorkerLifecycleEvent }
  | { type: "attach-follower-port"; peerId: string; leadershipId: number; port: MessagePort }
  | { type: "detach-follower-port"; peerId: string; leadershipId: number }
  | { type: "simulate-crash" }
  | { type: "shutdown" };

type WorkerOutbound =
  | { type: "ready" }
  | { type: "init-ok"; clientId: string }
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "server-out"; frames: Uint8Array[] }
  | { type: "auth-failure"; reason: AuthFailureReason }
  | { type: "follower-port-attached"; peerId: string; leadershipId: number }
  | { type: "follower-port-closed"; peerId: string; leadershipId: number }
  | { type: "shutdown-ok" }
  | { type: "error"; message: string };

type PortInbound =
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "update-auth"; jwtToken?: string | null }
  | { type: "close" };

type PortOutbound =
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "auth-failure"; reason: AuthFailureReason }
  | { type: "close" };

function isUint8Array(value: unknown): value is Uint8Array {
  return value instanceof Uint8Array;
}

function normalizeFrames(frames: unknown): Uint8Array[] {
  return Array.isArray(frames) ? frames.filter(isUint8Array) : [];
}

export class WorkerBridge {
  private readonly worker: Worker;
  private readonly runtime: RuntimeWithDirectTransport;
  private transport: DirectTransport | null = null;
  private readonly listeners: ListenerSlots = {};
  private pendingForwarder: ServerPayloadForwarder | null = null;
  private clientIdPromise: Promise<string> | null = null;
  private workerClientId: string | null = null;
  private disposed = false;

  private pumpScheduled = false;
  private shutdownResolve: (() => void) | null = null;

  constructor(worker: Worker, runtime: Runtime) {
    this.worker = worker;
    this.runtime = runtime as RuntimeWithDirectTransport;
    this.worker.addEventListener("message", (event: MessageEvent<WorkerOutbound>) => {
      this.handleWorkerMessage(event.data);
    });
  }

  init(options: WorkerBridgeOptions): Promise<string> {
    if (this.clientIdPromise) return this.clientIdPromise;
    if (this.disposed) {
      this.clientIdPromise = Promise.reject(new Error("WorkerBridge has been disposed"));
      return this.clientIdPromise;
    }

    const connectUpstreamPeer = this.runtime.connectUpstreamPeer;
    const getDirectOpenPayload = this.runtime.getDirectOpenPayload;
    if (typeof connectUpstreamPeer !== "function" || typeof getDirectOpenPayload !== "function") {
      this.clientIdPromise = Promise.reject(
        new Error("WorkerBridge requires a direct WasmDb runtime"),
      );
      return this.clientIdPromise;
    }

    this.transport = connectUpstreamPeer.call(this.runtime);
    const initOptions: WorkerBridgeOptions = {
      ...options,
      directOpen: getDirectOpenPayload.call(this.runtime),
    };

    this.clientIdPromise = new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("WorkerBridge init timed out")), 30_000);
      const onMessage = (event: MessageEvent<WorkerOutbound>) => {
        const msg = event.data;
        if (msg.type === "init-ok") {
          clearTimeout(timeout);
          this.worker.removeEventListener("message", onMessage);
          this.workerClientId = msg.clientId;
          this.schedulePump();
          resolve(msg.clientId);
        } else if (msg.type === "error") {
          clearTimeout(timeout);
          this.worker.removeEventListener("message", onMessage);
          reject(new Error(msg.message));
        }
      };
      this.worker.addEventListener("message", onMessage);
      this.postToWorker({ type: "init", options: initOptions });
      this.schedulePump();
    });
    return this.clientIdPromise;
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.postToWorker({ type: "update-auth", jwtToken: auth.jwtToken ?? null });
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    this.postToWorker({ type: "lifecycle", event });
  }

  async shutdown(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    this.transport?.close();
    this.transport = null;
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(resolve, 1_000);
      this.shutdownResolve = () => {
        clearTimeout(timeout);
        resolve();
      };
      this.postToWorker({ type: "shutdown" });
    });
  }

  getWorkerClientId(): string | null {
    return this.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ServerPayloadForwarder | null): void {
    this.pendingForwarder = forwarder;
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    return;
  }

  applyIncomingServerPayload(payload: Uint8Array): void {
    this.postToWorker({ type: "server-in", frame: payload });
  }

  replayServerConnection(): void {
    this.schedulePump();
  }

  disconnectUpstream(): void {
    this.transport?.close();
  }

  reconnectUpstream(): void {
    this.schedulePump();
  }

  replayWorkerUpstreamConnection(): void {
    this.reconnectUpstream();
  }

  /** Test-only: posts `simulate-crash` so the worker releases OPFS handles
   * without a clean snapshot, and resolves on `shutdown-ok` (or after the
   * shutdown-ack timeout). Used to validate WAL replay. */
  async simulateCrash(): Promise<void> {
    this.postToWorker({ type: "simulate-crash" });
  }

  onPeerSync(listener: (batch: PeerSyncBatch) => void): void {
    this.listeners.onPeerSync = listener;
  }

  onAuthFailure(listener: (reason: AuthFailureReason) => void): void {
    this.listeners.onAuthFailure = listener;
  }

  onFollowerPortAttached(listener: (event: FollowerPortEvent) => void): void {
    this.listeners.onFollowerPortAttached = listener;
  }

  onFollowerPortClosed(listener: (event: FollowerPortEvent) => void): void {
    this.listeners.onFollowerPortClosed = listener;
  }

  openPeer(peerId: string): void {
    void peerId;
  }

  sendPeerSync(peerId: string, leadershipId: number, payload: Uint8Array[]): void {
    this.listeners.onPeerSync?.({ peerId, leadershipId, payload });
  }

  closePeer(peerId: string): void {
    void peerId;
  }

  attachFollowerPort(peerId: string, leadershipId: number, port: MessagePort): void {
    this.postToWorker({ type: "attach-follower-port", peerId, leadershipId, port }, [port]);
  }

  detachFollowerPort(peerId: string, leadershipId: number): void {
    this.postToWorker({ type: "detach-follower-port", peerId, leadershipId });
  }

  private handleWorkerMessage(message: WorkerOutbound): void {
    if (!message || typeof message !== "object") return;
    switch (message.type) {
      case "sync":
        for (const frame of normalizeFrames(message.frames)) {
          this.transport?.sendWireFrame(frame);
        }
        this.schedulePump();
        return;
      case "server-out":
        for (const frame of normalizeFrames(message.frames)) {
          this.pendingForwarder?.(frame);
        }
        return;
      case "auth-failure":
        this.listeners.onAuthFailure?.(message.reason);
        return;
      case "follower-port-attached":
        this.listeners.onFollowerPortAttached?.(message);
        return;
      case "follower-port-closed":
        this.listeners.onFollowerPortClosed?.(message);
        return;
      case "shutdown-ok":
        this.shutdownResolve?.();
        this.shutdownResolve = null;
        return;
      case "error":
        console.error("Jazz worker bridge error", message.message);
        return;
      default:
        return;
    }
  }

  private schedulePump(): void {
    if (!this.transport || this.pumpScheduled || this.disposed) return;
    this.pumpScheduled = true;
    queueMicrotask(() => {
      this.pumpScheduled = false;
      this.pumpTransport();
    });
  }

  private pumpTransport(): void {
    const transport = this.transport;
    if (!transport || this.disposed) return;
    transport.tick();
    const frames = normalizeFrames(transport.recvWireFrames());
    if (frames.length > 0) {
      this.postToWorker({ type: "sync", frames }, frames.map((frame) => frame.buffer));
    }
  }

  private postToWorker(message: WorkerInbound, transfer: Transferable[] = []): void {
    this.worker.postMessage(message, transfer);
  }
}

export class MessagePortRuntimeBridge {
  private readonly port: MessagePort;
  private readonly runtime: RuntimeWithDirectTransport;
  private transport: DirectTransport | null = null;
  private authFailureCallback: ((reason: AuthFailureReason) => void) | null = null;
  private pumpScheduled = false;

  constructor(port: MessagePort, runtime: Runtime) {
    this.port = port;
    this.runtime = runtime as RuntimeWithDirectTransport;
  }

  init(): void {
    if (this.transport) return;
    const connectUpstreamPeer = this.runtime.connectUpstreamPeer;
    if (typeof connectUpstreamPeer !== "function") {
      throw new Error("MessagePortRuntimeBridge requires a direct WasmDb runtime");
    }
    this.transport = connectUpstreamPeer.call(this.runtime);
    this.port.addEventListener("message", (event: MessageEvent<PortOutbound>) => {
      this.handlePortMessage(event.data);
    });
    this.port.start?.();
    this.schedulePump();
  }

  shutdown(): void {
    this.transport?.close();
    this.transport = null;
    this.port.postMessage({ type: "close" } satisfies PortInbound);
  }

  detachForReconnect(): void {
    this.transport?.close();
    this.transport = null;
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.port.postMessage({ type: "update-auth", jwtToken: auth.jwtToken ?? null } satisfies PortInbound);
  }

  onAuthFailure(callback: (reason: AuthFailureReason) => void): void {
    this.authFailureCallback = callback;
  }

  private handlePortMessage(message: PortOutbound): void {
    if (!message || typeof message !== "object") return;
    if (message.type === "sync") {
      for (const frame of normalizeFrames(message.frames)) {
        this.transport?.sendWireFrame(frame);
      }
      this.schedulePump();
    } else if (message.type === "auth-failure") {
      this.authFailureCallback?.(message.reason);
    } else if (message.type === "close") {
      this.detachForReconnect();
    }
  }

  private schedulePump(): void {
    if (!this.transport || this.pumpScheduled) return;
    this.pumpScheduled = true;
    queueMicrotask(() => {
      this.pumpScheduled = false;
      this.pumpTransport();
    });
  }

  private pumpTransport(): void {
    const transport = this.transport;
    if (!transport) return;
    transport.tick();
    const frames = normalizeFrames(transport.recvWireFrames());
    if (frames.length > 0) {
      this.port.postMessage({ type: "sync", frames } satisfies PortInbound, frames.map((frame) => frame.buffer));
    }
  }
}
