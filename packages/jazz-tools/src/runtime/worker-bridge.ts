/**
 * `WorkerBridge` — thin TS adapter over the Rust-owned `WasmWorkerBridge`.
 *
 * The bridge state machine, init/shutdown handshakes, listener slots, peer
 * routing, lifecycle hint forwarding, and outbox buffering all live in
 * `crates/jazz-wasm/src/worker_bridge.rs`. The Rust API takes options at
 * `attach()` time and `init()` is parameter-less. This adapter preserves
 * the historical TS surface (`new WorkerBridge(worker, runtime)` + listener
 * registration before `init(options)`) by deferring Rust attach until
 * `init()`.
 */

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

interface WasmBridgeHandle {
  init(): Promise<{ clientId: string }>;
  updateAuth(jwtToken?: string | null): void;
  sendLifecycleHint(event: string): void;
  openPeer(peerId: string): void;
  sendPeerSync(peerId: string, leadershipId: number, payload: Uint8Array[]): void;
  closePeer(peerId: string): void;
  attachFollowerPort(peerId: string, leadershipId: number, port: MessagePort): void;
  detachFollowerPort(peerId: string, leadershipId: number): void;
  setServerPayloadForwarder(
    callback:
      | ((payload: Uint8Array | string, isCatalogue: boolean, sequence: number | null) => void)
      | null,
  ): void;
  applyIncomingServerPayload(payload: Uint8Array): void;
  waitForUpstreamServerConnection(): Promise<void>;
  replayServerConnection(): void;
  disconnectUpstream(): void;
  reconnectUpstream(): void;
  simulateCrash(): Promise<void>;
  setListeners(listeners: ListenerSlots): void;
  shutdown(): Promise<void>;
  getWorkerClientId(): string | null;
}

interface RuntimeWithWorkerBridge extends Runtime {
  createWorkerBridge?(worker: Worker, options: object): WasmBridgeHandle;
  createMessagePortBridge?(port: MessagePort): WasmMessagePortBridgeHandle;
}

interface ListenerSlots {
  onPeerSync?: (batch: PeerSyncBatch) => void;
  onAuthFailure?: (reason: AuthFailureReason) => void;
  onFollowerPortAttached?: (event: FollowerPortEvent) => void;
  onFollowerPortClosed?: (event: FollowerPortEvent) => void;
}

type ServerPayloadForwarder = (payload: Uint8Array) => void;

interface WasmMessagePortBridgeHandle {
  updateAuth(jwtToken?: string | null): void;
  onAuthFailure?(callback: (reason: AuthFailureReason) => void): void;
  detachForReconnect(): void;
  shutdown(): void;
}

export class WorkerBridge {
  private readonly worker: Worker;
  private readonly runtime: Runtime;
  private bridge: WasmBridgeHandle | null = null;
  private readonly listeners: ListenerSlots = {};
  private pendingForwarder: ServerPayloadForwarder | null = null;
  private clientIdPromise: Promise<string> | null = null;
  private workerClientId: string | null = null;
  private disposed = false;

  constructor(worker: Worker, runtime: Runtime) {
    this.worker = worker;
    this.runtime = runtime;
  }

  init(options: WorkerBridgeOptions): Promise<string> {
    if (this.clientIdPromise) return this.clientIdPromise;
    if (this.disposed) {
      this.clientIdPromise = Promise.reject(new Error("WorkerBridge has been disposed"));
      return this.clientIdPromise;
    }

    const create = (this.runtime as RuntimeWithWorkerBridge).createWorkerBridge;
    if (typeof create !== "function") {
      this.clientIdPromise = Promise.reject(
        new Error("WorkerBridge requires a WasmRuntime with `createWorkerBridge`"),
      );
      return this.clientIdPromise;
    }

    let bridge: WasmBridgeHandle;
    try {
      bridge = create.call(this.runtime, this.worker, options as unknown as object);
    } catch (e: unknown) {
      const err = e instanceof Error ? e : new Error(String(e));
      this.clientIdPromise = Promise.reject(err);
      return this.clientIdPromise;
    }
    this.bridge = bridge;
    bridge.setListeners(this.listeners);
    if (this.pendingForwarder) {
      this.installForwarderInternal(this.pendingForwarder);
    }

    this.clientIdPromise = bridge
      .init()
      .then((result) => {
        this.workerClientId = result.clientId;
        return result.clientId;
      })
      .catch((error: unknown) => {
        if (error instanceof Error) throw error;
        if (typeof error === "string") throw new Error(error);
        throw new Error(String(error));
      });
    return this.clientIdPromise;
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.bridge?.updateAuth(auth.jwtToken ?? null);
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    this.bridge?.sendLifecycleHint(event);
  }

  async shutdown(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    if (this.bridge) {
      try {
        await this.bridge.shutdown();
      } finally {
        this.bridge = null;
      }
    }
  }

  getWorkerClientId(): string | null {
    if (this.bridge) return this.bridge.getWorkerClientId();
    return this.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ServerPayloadForwarder | null): void {
    this.pendingForwarder = forwarder;
    if (!this.bridge) return;
    if (forwarder) this.installForwarderInternal(forwarder);
    else this.bridge.setServerPayloadForwarder(null);
  }

  private installForwarderInternal(forwarder: ServerPayloadForwarder): void {
    // Server-bound payloads are always binary postcard; the Rust outbox sender
    // calls the forwarder with a single `Uint8Array`.
    this.bridge?.setServerPayloadForwarder((payload) => {
      forwarder(payload as Uint8Array);
    });
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    if (!this.bridge) return;
    await this.bridge.waitForUpstreamServerConnection();
  }

  applyIncomingServerPayload(payload: Uint8Array): void {
    this.bridge?.applyIncomingServerPayload(payload);
  }

  replayServerConnection(): void {
    this.bridge?.replayServerConnection();
  }

  disconnectUpstream(): void {
    this.bridge?.disconnectUpstream();
  }

  reconnectUpstream(): void {
    this.bridge?.reconnectUpstream();
  }

  replayWorkerUpstreamConnection(): void {
    this.reconnectUpstream();
  }

  /** Test-only: posts `simulate-crash` so the worker releases OPFS handles
   * without a clean snapshot, and resolves on `shutdown-ok` (or after the
   * shutdown-ack timeout). Used to validate WAL replay. */
  async simulateCrash(): Promise<void> {
    if (!this.bridge) return;
    await this.bridge.simulateCrash();
  }

  onPeerSync(listener: (batch: PeerSyncBatch) => void): void {
    this.listeners.onPeerSync = listener;
    this.bridge?.setListeners(this.listeners);
  }

  onAuthFailure(listener: (reason: AuthFailureReason) => void): void {
    this.listeners.onAuthFailure = listener;
    this.bridge?.setListeners(this.listeners);
  }

  onFollowerPortAttached(listener: (event: FollowerPortEvent) => void): void {
    this.listeners.onFollowerPortAttached = listener;
    this.bridge?.setListeners(this.listeners);
  }

  onFollowerPortClosed(listener: (event: FollowerPortEvent) => void): void {
    this.listeners.onFollowerPortClosed = listener;
    this.bridge?.setListeners(this.listeners);
  }

  openPeer(peerId: string): void {
    this.bridge?.openPeer(peerId);
  }

  sendPeerSync(peerId: string, leadershipId: number, payload: Uint8Array[]): void {
    this.bridge?.sendPeerSync(peerId, leadershipId, payload);
  }

  closePeer(peerId: string): void {
    this.bridge?.closePeer(peerId);
  }

  attachFollowerPort(peerId: string, leadershipId: number, port: MessagePort): void {
    this.bridge?.attachFollowerPort(peerId, leadershipId, port);
  }

  detachFollowerPort(peerId: string, leadershipId: number): void {
    this.bridge?.detachFollowerPort(peerId, leadershipId);
  }
}

export class MessagePortRuntimeBridge {
  private readonly port: MessagePort;
  private readonly runtime: Runtime;
  private bridge: WasmMessagePortBridgeHandle | null = null;

  constructor(port: MessagePort, runtime: Runtime) {
    this.port = port;
    this.runtime = runtime;
  }

  init(): void {
    if (this.bridge) return;
    const create = (this.runtime as RuntimeWithWorkerBridge).createMessagePortBridge;
    if (typeof create !== "function") {
      throw new Error(
        "MessagePortRuntimeBridge requires a WasmRuntime with `createMessagePortBridge`",
      );
    }
    this.bridge = create.call(this.runtime, this.port);
  }

  shutdown(): void {
    this.bridge?.shutdown();
    this.bridge = null;
  }

  detachForReconnect(): void {
    this.bridge?.detachForReconnect();
    this.bridge = null;
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.bridge?.updateAuth(auth.jwtToken ?? null);
  }

  onAuthFailure(callback: (reason: AuthFailureReason) => void): void {
    this.bridge?.onAuthFailure?.(callback);
  }
}
