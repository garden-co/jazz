/**
 * WorkerBridge — Main-thread side of the worker communication bridge.
 *
 * Wires a main-thread WasmRuntime (in-memory) to a dedicated worker
 * (OPFS-persistent) via the Rust WorkerClient façade. The worker acts
 * as the "server" for the main thread's runtime.
 *
 * The WorkerClient owns the postMessage/onmessage wire protocol (binary
 * WorkerFrame encoding). This shim preserves:
 *   - Phase state machine (idle → initializing → ready → …)
 *   - pendingSyncPayloadsForWorker + flushPendingSyncToWorker
 *   - upstreamServerReady deferred promise
 *   - serverPayloadForwarder callback
 */

import type { Runtime } from "./client.js";
import type { RuntimeSourcesConfig } from "./context.js";
import type { AuthFailureReason } from "./sync-transport.js";
import { createSyncOutboxRouter } from "./sync-transport.js";
import { WorkerClient } from "jazz-wasm";

/**
 * Page-lifecycle event names forwarded to the worker.
 */
export type WorkerLifecycleEvent =
  | "visibility-hidden"
  | "visibility-visible"
  | "pagehide"
  | "freeze"
  | "resume";

/**
 * Options for initializing the worker bridge.
 */
export interface WorkerBridgeOptions {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  serverUrl?: string;
  serverPathPrefix?: string;
  jwtToken?: string;
  adminSecret?: string;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
}

export interface PeerSyncBatch {
  peerId: string;
  term: number;
  payload: Uint8Array[];
}

type BridgePhase = "idle" | "initializing" | "ready" | "failed" | "shutting-down" | "disposed";
type BridgeEvent =
  | { type: "INIT_CALLED" }
  | { type: "INIT_OK"; clientId: string }
  | { type: "INIT_FAILED" }
  | { type: "SHUTDOWN_CALLED" }
  | { type: "SHUTDOWN_FINISHED" };

interface WorkerBridgeState {
  phase: BridgePhase;
  workerClientId: string | null;
  initPromise: Promise<string> | null;
  expectsUpstreamServer: boolean;
  upstreamServerConnected: boolean;
  upstreamServerReady: Promise<void>;
  resolveUpstreamServerReady: (() => void) | null;
  pendingSyncPayloadsForWorker: Uint8Array[];
  syncBatchFlushQueued: boolean;
  peerSyncListener: ((batch: PeerSyncBatch) => void) | null;
  authFailureListener: ((reason: AuthFailureReason) => void) | null;
  serverPayloadForwarder: ((payload: Uint8Array) => void) | null;
}

function createDeferredPromise(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((resolver) => {
    resolve = resolver;
  });
  return { promise, resolve };
}

/**
 * Bridge between main-thread runtime and dedicated worker.
 *
 * The bridge:
 * - Forwards outgoing sync messages from the main runtime to the worker
 * - Forwards incoming sync messages from the worker to the main runtime
 * - The worker is treated as the main thread's "server" for sync purposes
 */
export class WorkerBridge {
  private client: WorkerClient;
  private runtime: Runtime;
  private state: WorkerBridgeState;

  constructor(worker: Worker, runtime: Runtime) {
    const upstreamReady = createDeferredPromise();
    this.runtime = runtime;
    this.client = new WorkerClient(worker);
    this.state = {
      phase: "idle",
      workerClientId: null,
      initPromise: null,
      expectsUpstreamServer: false,
      upstreamServerConnected: false,
      upstreamServerReady: upstreamReady.promise,
      resolveUpstreamServerReady: upstreamReady.resolve,
      pendingSyncPayloadsForWorker: [],
      syncBatchFlushQueued: false,
      peerSyncListener: null,
      authFailureListener: null,
      serverPayloadForwarder: null,
    };

    // Wire worker → main: incoming sync messages from worker
    this.client.set_on_sync((bytes: Uint8Array) => {
      this.runtime.onSyncMessageReceived(bytes);
    });

    this.client.set_on_peer_sync((peerId: string, term: number, bytes: Uint8Array) => {
      this.state.peerSyncListener?.({
        peerId,
        term,
        payload: [bytes],
      });
    });

    this.client.set_on_upstream_status((connected: boolean) => {
      if (connected) {
        this.markUpstreamServerConnected();
      } else {
        this.markUpstreamServerDisconnected();
      }
    });

    this.client.set_on_auth_failed((reason: string) => {
      this.state.authFailureListener?.(reason as AuthFailureReason);
    });

    // Wire main → worker: outgoing sync messages from runtime via WorkerClient outbox.
    // The WorkerClient.installOnRuntime installs a ClientOutboxHandle on the Rust core
    // so server-bound payloads are encoded as WorkerFrame::Sync and sent to the worker.
    // We still need the TS-level router for the serverPayloadForwarder path.
    this.runtime.onSyncMessageToSend?.(
      createSyncOutboxRouter({
        onServerPayload: (payload) => {
          if (this.isDisposedLike()) return;

          if (this.state.serverPayloadForwarder) {
            this.state.serverPayloadForwarder(payload as Uint8Array);
          } else {
            this.enqueueSyncMessageForWorker(payload as Uint8Array);
          }
        },
      }),
    );

    // Register a server so the runtime sends sync messages to it
    this.runtime.addServer();
  }

  /**
   * Initialize the worker with schema and config.
   *
   * Sends InitPayload via WorkerClient and updates the phase state machine.
   */
  init(options: WorkerBridgeOptions): Promise<string> {
    if (this.state.initPromise) {
      return this.state.initPromise;
    }

    if (this.isDisposedLike()) {
      const disposedError = Promise.reject(new Error("WorkerBridge has been disposed"));
      this.state.initPromise = disposedError;
      return disposedError;
    }

    this.transition({ type: "INIT_CALLED" });

    this.state.expectsUpstreamServer = Boolean(options.serverUrl);
    if (!this.state.expectsUpstreamServer) {
      this.markUpstreamServerConnected();
    } else {
      this.markUpstreamServerDisconnected();
    }

    const initPayload = {
      schema_json: options.schemaJson,
      app_id: options.appId,
      env: options.env,
      user_branch: options.userBranch,
      db_name: options.dbName,
      server_url: options.serverUrl,
      server_path_prefix: options.serverPathPrefix,
      jwt_token: options.jwtToken,
      admin_secret: options.adminSecret,
      log_level: options.logLevel,
      fallback_wasm_url: options.fallbackWasmUrl,
    };

    const initPromise = this.client
      .init(initPayload)
      .then((clientId: string) => {
        if (this.isDisposedLike()) {
          throw new Error("WorkerBridge has been disposed");
        }
        if (this.state.phase !== "initializing") {
          throw new Error("Worker init response arrived after bridge left initializing state");
        }
        this.transition({ type: "INIT_OK", clientId });
        this.flushPendingSyncToWorker();
        return clientId;
      })
      .catch((error: unknown) => {
        if (this.state.phase !== "disposed") {
          this.transition({ type: "INIT_FAILED" });
        }
        throw error;
      });

    this.state.initPromise = initPromise;
    return initPromise;
  }

  /**
   * Update auth credentials in the worker.
   */
  updateAuth(auth: { jwtToken?: string }): void {
    if (this.isDisposedLike()) return;
    this.client.update_auth(auth.jwtToken);
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.isDisposedLike()) return;
    this.client.lifecycle_hint(event, Date.now());
  }

  /**
   * Shut down the worker and wait for OPFS handles to be released.
   */
  async shutdown(worker: Worker): Promise<void> {
    if (this.isDisposedLike()) return;

    this.transition({ type: "SHUTDOWN_CALLED" });

    try {
      await this.client.shutdown();
      this.transition({ type: "SHUTDOWN_FINISHED" });
    } catch {
      this.transition({ type: "SHUTDOWN_FINISHED" });
      // Timeout — worker may have already closed
    }
    worker.terminate();
  }

  /**
   * Get the client ID the worker assigned to the main thread.
   */
  getWorkerClientId(): string | null {
    return this.state.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ((payload: Uint8Array) => void) | null): void {
    if (this.isDisposedLike()) return;
    this.state.serverPayloadForwarder = forwarder;
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    if (!this.state.expectsUpstreamServer) return;
    if (this.state.serverPayloadForwarder) return;
    if (this.state.upstreamServerConnected) return;
    await this.state.upstreamServerReady;
  }

  applyIncomingServerPayload(payload: Uint8Array): void {
    if (this.isDisposedLike()) return;
    this.runtime.onSyncMessageReceived(payload);
  }

  replayServerConnection(): void {
    if (this.isDisposedLike()) return;
    this.runtime.removeServer();
    this.runtime.addServer();
  }

  disconnectUpstream(): void {
    if (this.isDisposedLike()) return;
    this.client.disconnect_upstream();
  }

  reconnectUpstream(): void {
    if (this.isDisposedLike()) return;
    this.client.reconnect_upstream();
  }

  onPeerSync(listener: (batch: PeerSyncBatch) => void): void {
    this.state.peerSyncListener = listener;
  }

  onAuthFailure(listener: (reason: AuthFailureReason) => void): void {
    this.state.authFailureListener = listener;
  }

  openPeer(peerId: string): void {
    if (this.isDisposedLike()) return;
    this.client.peer_open(peerId);
  }

  sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void {
    if (this.isDisposedLike()) return;
    if (payload.length === 0) return;
    for (const bytes of payload) {
      this.client.send_peer_sync(peerId, term, bytes);
    }
  }

  closePeer(peerId: string): void {
    if (this.isDisposedLike()) return;
    this.client.peer_close(peerId);
  }

  private enqueueSyncMessageForWorker(payload: Uint8Array): void {
    if (this.isDisposedLike()) return;

    this.state.pendingSyncPayloadsForWorker.push(payload);
    if (this.state.syncBatchFlushQueued) return;

    this.state.syncBatchFlushQueued = true;
    queueMicrotask(() => {
      if (this.isDisposedLike()) {
        this.state.syncBatchFlushQueued = false;
        this.state.pendingSyncPayloadsForWorker = [];
        return;
      }
      this.state.syncBatchFlushQueued = false;
      this.flushPendingSyncToWorker();
    });
  }

  private flushPendingSyncToWorker(): void {
    if (this.state.phase !== "ready" || this.state.pendingSyncPayloadsForWorker.length === 0) {
      return;
    }

    const payloads = this.state.pendingSyncPayloadsForWorker;
    this.state.pendingSyncPayloadsForWorker = [];

    for (const bytes of payloads) {
      this.client.send_sync(bytes);
    }
  }

  private markUpstreamServerConnected(): void {
    this.state.upstreamServerConnected = true;
    const resolver = this.state.resolveUpstreamServerReady;
    this.state.resolveUpstreamServerReady = null;
    resolver?.();
  }

  private markUpstreamServerDisconnected(): void {
    if (!this.state.expectsUpstreamServer) {
      this.state.upstreamServerConnected = true;
      return;
    }
    if (!this.state.upstreamServerConnected && this.state.resolveUpstreamServerReady) {
      return;
    }
    const deferred = createDeferredPromise();
    this.state.upstreamServerConnected = false;
    this.state.upstreamServerReady = deferred.promise;
    this.state.resolveUpstreamServerReady = deferred.resolve;
  }

  private isDisposedLike(): boolean {
    return this.state.phase === "disposed" || this.state.phase === "shutting-down";
  }

  private transition(event: BridgeEvent): void {
    switch (event.type) {
      case "INIT_CALLED":
        if (this.state.phase === "idle" || this.state.phase === "failed") {
          this.state.phase = "initializing";
        }
        return;
      case "INIT_OK":
        if (this.state.phase !== "initializing") return;
        this.state.workerClientId = event.clientId;
        this.state.phase = "ready";
        return;
      case "INIT_FAILED":
        if (this.state.phase !== "initializing") return;
        this.state.phase = "failed";
        this.state.syncBatchFlushQueued = false;
        return;
      case "SHUTDOWN_CALLED":
        if (this.state.phase === "disposed" || this.state.phase === "shutting-down") return;
        this.state.phase = "shutting-down";
        // Detach upstream edge so the next bridge attach performs a clean replay.
        this.runtime.removeServer();
        return;
      case "SHUTDOWN_FINISHED":
        if (this.state.phase === "disposed") return;
        this.state.phase = "disposed";
        this.disposeInternals();
        return;
    }
  }

  private disposeInternals(): void {
    this.state.pendingSyncPayloadsForWorker = [];
    this.state.serverPayloadForwarder = null;
    this.state.peerSyncListener = null;
    this.state.syncBatchFlushQueued = false;
    this.runtime.onSyncMessageToSend?.(() => undefined);
  }
}
