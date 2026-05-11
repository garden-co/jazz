/**
 * WorkerBridge — thin TS adapter over the Rust `WasmWorkerBridge`.
 *
 * Preserves the historical TS surface so `db.ts` does not need to change in
 * this round. Internally defers Rust attach until `init()`.
 */

import type { Runtime } from "./client.js";
import type { RuntimeSourcesConfig } from "./context.js";
import type { AuthFailureReason } from "./sync-transport.js";
import type { LocalBatchRecord, MutationErrorEvent } from "./client.js";

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

interface ListenerSlots {
  onPeerSync?: (batch: PeerSyncBatch) => void;
  onAuthFailure?: (reason: AuthFailureReason) => void;
  onLocalBatchRecordsSync?: (batches: LocalBatchRecord[]) => void;
  onMutationErrorReplay?: (event: MutationErrorEvent) => void;
}

/** Subset of the Rust bridge handle this adapter relies on. */
interface RustWorkerBridge {
  init(): Promise<{ clientId: string }>;
  updateAuth(jwtToken?: string): void;
  sendLifecycleHint(event: string): void;
  openPeer(peerId: string): void;
  sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void;
  closePeer(peerId: string): void;
  setServerPayloadForwarder(callback: ((payload: Uint8Array) => void) | null): void;
  applyIncomingServerPayload(payload: Uint8Array): void;
  waitForUpstreamServerConnection(): Promise<void>;
  replayServerConnection(): void;
  disconnectUpstream(): void;
  reconnectUpstream(): void;
  acknowledgeRejectedBatch(batchId: string): void;
  simulateCrash(): Promise<void>;
  setListeners(listeners: ListenerSlots): void;
  getWorkerClientId(): string | null;
  shutdown(): Promise<void>;
}

/** Shape of the WasmRuntime piece this adapter needs. */
type RuntimeWithBridgeFactory = Runtime & {
  createWorkerBridge?: (worker: Worker, options: WorkerBridgeOptions) => RustWorkerBridge;
};

export class WorkerBridge {
  private bridge: RustWorkerBridge | null = null;
  private listeners: ListenerSlots = {};
  private pendingForwarder: ((payload: Uint8Array) => void) | null = null;
  private clientIdPromise: Promise<string> | null = null;
  private disposed = false;

  constructor(
    private readonly worker: Worker,
    private readonly runtime: Runtime,
  ) {}

  async init(options: WorkerBridgeOptions): Promise<string> {
    if (this.clientIdPromise) return this.clientIdPromise;
    if (this.disposed) {
      return Promise.reject(new Error("WorkerBridge has been disposed"));
    }

    const runtime = this.runtime as RuntimeWithBridgeFactory;
    if (typeof runtime.createWorkerBridge !== "function") {
      return Promise.reject(
        new Error("WorkerBridge requires a WasmRuntime with `createWorkerBridge`"),
      );
    }

    let bridge: RustWorkerBridge;
    try {
      bridge = runtime.createWorkerBridge(this.worker, options) as RustWorkerBridge;
    } catch (err) {
      this.clientIdPromise = Promise.reject(coerceError(err));
      return this.clientIdPromise;
    }

    this.bridge = bridge;
    bridge.setListeners(this.listeners);
    if (this.pendingForwarder) {
      bridge.setServerPayloadForwarder(this.pendingForwarder);
    }

    this.clientIdPromise = bridge.init().then(
      (res) => res.clientId,
      (err) => {
        throw coerceError(err);
      },
    );
    return this.clientIdPromise;
  }

  async shutdown(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    const bridge = this.bridge;
    this.bridge = null;
    if (!bridge) return;
    try {
      await bridge.shutdown();
    } catch {
      // shutdown errors are swallowed — the bridge is already disposed
    }
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.bridge?.updateAuth(auth.jwtToken);
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    this.bridge?.sendLifecycleHint(event);
  }

  getWorkerClientId(): string | null {
    return this.bridge?.getWorkerClientId() ?? null;
  }

  setServerPayloadForwarder(fwd: ((payload: Uint8Array) => void) | null): void {
    this.pendingForwarder = fwd;
    this.bridge?.setServerPayloadForwarder(fwd);
  }

  applyIncomingServerPayload(payload: Uint8Array): void {
    this.bridge?.applyIncomingServerPayload(payload);
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    if (!this.bridge) return;
    await this.bridge.waitForUpstreamServerConnection();
  }

  /**
   * Reset the main-runtime server edge so a fresh catalogue exchange runs.
   * Mirrors the legacy adapter; the new bridge offers `replayServerConnection`
   * directly.
   */
  replayServerConnection(): void {
    this.bridge?.replayServerConnection();
  }

  /**
   * Re-arm the worker's upstream WS. Today this is just `reconnectUpstream`;
   * kept under the legacy name so existing callers in `db.ts` keep working
   * without renames.
   */
  replayWorkerUpstreamConnection(): void {
    this.bridge?.reconnectUpstream();
  }

  /**
   * Legacy hook used to wait for the worker to durably persist a local batch.
   * In the new architecture durability is handled inline by the worker host's
   * `batched_tick` + `flush_wal`, so this resolves immediately.
   */
  async waitForLocalSyncFlush(_batchId?: string): Promise<void> {
    // Intentionally no-op: durability is enforced inside the worker host.
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

  onMutationErrorReplay(listener: (event: MutationErrorEvent) => void): void {
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

function coerceError(value: unknown): Error {
  if (value instanceof Error) return value;
  if (typeof value === "string") return new Error(value);
  if (value && typeof value === "object" && "message" in value) {
    return new Error(String((value as { message: unknown }).message));
  }
  return new Error(String(value));
}
