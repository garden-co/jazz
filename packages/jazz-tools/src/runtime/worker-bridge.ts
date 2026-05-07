/**
 * `WorkerBridge` — thin TS adapter over the Rust-owned `WasmWorkerBridge`.
 *
 * Per `specs/todo/projects/rust-owned-worker-bridge/spec.md` Stage 3, the
 * bridge state machine, init/shutdown handshakes, listener slots, peer
 * routing, lifecycle hint forwarding, and outbox buffering all live in
 * `crates/jazz-wasm/src/worker_bridge.rs`. The Rust API takes options at
 * `attach()` time and `init()` is parameter-less. This adapter preserves
 * the historical TS surface (`new WorkerBridge(worker, runtime)` + listener
 * registration before `init(options)`) by deferring Rust attach until
 * `init()`.
 */

import type { LocalBatchRecord, Runtime } from "./client.js";
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
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
}

export interface PeerSyncBatch {
  peerId: string;
  term: number;
  payload: Uint8Array[];
}

interface WasmBridgeHandle {
  init(): Promise<{ clientId: string }>;
  updateAuth(jwtToken?: string | null): void;
  sendLifecycleHint(event: string): void;
  openPeer(peerId: string): void;
  sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void;
  closePeer(peerId: string): void;
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
  acknowledgeRejectedBatch(batchId: string): void;
  simulateCrash(): Promise<void>;
  setListeners(listeners: ListenerSlots): void;
  shutdown(): Promise<void>;
  getWorkerClientId(): string | null;
}

interface RuntimeWithWorkerBridge extends Runtime {
  createWorkerBridge?(worker: Worker, options: object): WasmBridgeHandle;
}

interface ListenerSlots {
  onPeerSync?: (batch: PeerSyncBatch) => void;
  onAuthFailure?: (reason: AuthFailureReason) => void;
  onLocalBatchRecordsSync?: (batches: LocalBatchRecord[]) => void;
  onMutationErrorReplay?: (batch: LocalBatchRecord) => void;
}

type ServerPayloadForwarder = (payload: Uint8Array) => void;

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

  acknowledgeRejectedBatch(batchId: string): void {
    this.bridge?.acknowledgeRejectedBatch(batchId);
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

  onLocalBatchRecordsSync(listener: (batches: LocalBatchRecord[]) => void): void {
    this.listeners.onLocalBatchRecordsSync = listener;
    this.bridge?.setListeners(this.listeners);
  }

  onMutationErrorReplay(listener: (batch: LocalBatchRecord) => void): void {
    this.listeners.onMutationErrorReplay = listener;
    this.bridge?.setListeners(this.listeners);
  }

  openPeer(peerId: string): void {
    this.bridge?.openPeer(peerId);
  }

  sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void {
    this.bridge?.sendPeerSync(peerId, term, payload);
  }

  closePeer(peerId: string): void {
    this.bridge?.closePeer(peerId);
  }
}
