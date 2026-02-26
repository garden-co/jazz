/**
 * WorkerBridge — Main-thread side of the worker communication bridge.
 *
 * Wires a main-thread WasmRuntime (in-memory) to a dedicated worker
 * (OPFS-persistent) via postMessage. The worker acts as the "server"
 * for the main thread's runtime.
 */

import type { Runtime } from "./client.js";
import type {
  InitMessage,
  WorkerLifecycleEvent,
  WorkerToMainMessage,
} from "../worker/worker-protocol.js";

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
  localAuthMode?: "anonymous" | "demo";
  localAuthToken?: string;
  adminSecret?: string;
}

export interface PeerSyncBatch {
  peerId: string;
  term: number;
  payload: string[];
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
  pendingSyncPayloadsForWorker: string[];
  syncBatchFlushQueued: boolean;
  peerSyncListener: ((batch: PeerSyncBatch) => void) | null;
  serverPayloadForwarder: ((payload: string) => void) | null;
}

const INIT_RESPONSE_TIMEOUT_MS = 12_000;
const SHUTDOWN_ACK_TIMEOUT_MS = 5_000;

/**
 * Bridge between main-thread runtime and dedicated worker.
 *
 * The bridge:
 * - Forwards outgoing sync messages from the main runtime to the worker
 * - Forwards incoming sync messages from the worker to the main runtime
 * - The worker is treated as the main thread's "server" for sync purposes
 */
export class WorkerBridge {
  private worker: Worker;
  private runtime: Runtime;
  private state: WorkerBridgeState;

  constructor(worker: Worker, runtime: Runtime) {
    this.worker = worker;
    this.runtime = runtime;
    this.state = {
      phase: "idle",
      workerClientId: null,
      initPromise: null,
      pendingSyncPayloadsForWorker: [],
      syncBatchFlushQueued: false,
      peerSyncListener: null,
      serverPayloadForwarder: null,
    };

    // Wire worker → main: incoming sync messages from worker
    this.worker.onmessage = (event: MessageEvent<WorkerToMainMessage>) => {
      const msg = event.data;
      if (msg.type === "sync") {
        for (const payload of msg.payload) {
          this.runtime.onSyncMessageReceived(payload);
        }
      } else if (msg.type === "peer-sync") {
        this.state.peerSyncListener?.({
          peerId: msg.peerId,
          term: msg.term,
          payload: msg.payload,
        });
      }
    };

    // Wire main → worker: outgoing sync messages from runtime
    this.runtime.onSyncMessageToSend((envelope: string) => {
      if (this.isDisposedLike()) return;
      const parsed = JSON.parse(envelope);
      if (parsed.destination && "Server" in parsed.destination) {
        const payload = JSON.stringify(parsed.payload);
        if (this.state.serverPayloadForwarder) {
          this.state.serverPayloadForwarder(payload);
        } else {
          this.enqueueSyncMessageForWorker(payload);
        }
      }
    });

    // Register a server so the runtime sends sync messages to it
    this.runtime.addServer();
  }

  /**
   * Initialize the worker with schema and config.
   *
   * Waits for the worker to respond with init-ok.
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

    const initMsg: InitMessage = {
      type: "init",
      schemaJson: options.schemaJson,
      appId: options.appId,
      env: options.env,
      userBranch: options.userBranch,
      dbName: options.dbName,
      serverUrl: options.serverUrl,
      serverPathPrefix: options.serverPathPrefix,
      jwtToken: options.jwtToken,
      localAuthMode: options.localAuthMode,
      localAuthToken: options.localAuthToken,
      adminSecret: options.adminSecret,
      clientId: "", // Worker generates its own client ID for main thread
    };

    const responsePromise = waitForMessage<WorkerToMainMessage>(
      this.worker,
      (msg) => msg.type === "init-ok" || msg.type === "error",
      INIT_RESPONSE_TIMEOUT_MS,
      "Worker init timeout",
    );

    this.worker.postMessage(initMsg);

    const initPromise = responsePromise
      .then((response) => {
        if (this.isDisposedLike()) {
          throw new Error("WorkerBridge has been disposed");
        }

        if (response.type === "error") {
          this.transition({ type: "INIT_FAILED" });
          throw new Error(`Worker init failed: ${response.message}`);
        }

        if (response.type === "init-ok") {
          if (this.state.phase !== "initializing") {
            // Ignore late init-ok after a terminal transition.
            throw new Error("Worker init response arrived after bridge left initializing state");
          }
          this.transition({ type: "INIT_OK", clientId: response.clientId });
          this.flushPendingSyncToWorker();
          return response.clientId;
        }

        throw new Error("Unexpected worker response");
      })
      .catch((error) => {
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
  updateAuth(auth: {
    jwtToken?: string;
    localAuthMode?: "anonymous" | "demo";
    localAuthToken?: string;
  }): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "update-auth", ...auth });
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({
      type: "lifecycle-hint",
      event,
      sentAtMs: Date.now(),
    });
  }

  /**
   * Shut down the worker and wait for OPFS handles to be released.
   *
   * @param worker The Worker instance (needed for listening to shutdown-ok)
   */
  async shutdown(worker: Worker): Promise<void> {
    if (this.isDisposedLike()) return;

    this.transition({ type: "SHUTDOWN_CALLED" });

    const shutdownAckPromise = waitForMessage<WorkerToMainMessage>(
      worker,
      (msg) => msg.type === "shutdown-ok",
      SHUTDOWN_ACK_TIMEOUT_MS,
      "Worker shutdown timeout",
    );
    this.worker.postMessage({ type: "shutdown" });
    try {
      await shutdownAckPromise;
      this.transition({ type: "SHUTDOWN_FINISHED" });
    } catch {
      this.transition({ type: "SHUTDOWN_FINISHED" });
      // Timeout — worker may have already closed
    }
  }

  /**
   * Get the client ID the worker assigned to the main thread.
   */
  getWorkerClientId(): string | null {
    return this.state.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ((payload: string) => void) | null): void {
    if (this.isDisposedLike()) return;
    this.state.serverPayloadForwarder = forwarder;
  }

  applyIncomingServerPayload(payload: string): void {
    if (this.isDisposedLike()) return;
    this.runtime.onSyncMessageReceived(payload);
  }

  replayServerConnection(): void {
    if (this.isDisposedLike()) return;
    this.runtime.removeServer();
    this.runtime.addServer();
  }

  onPeerSync(listener: (batch: PeerSyncBatch) => void): void {
    this.state.peerSyncListener = listener;
  }

  openPeer(peerId: string): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "peer-open", peerId });
  }

  sendPeerSync(peerId: string, term: number, payload: string[]): void {
    if (this.isDisposedLike()) return;
    if (payload.length === 0) return;
    this.worker.postMessage({
      type: "peer-sync",
      peerId,
      term,
      payload,
    });
  }

  closePeer(peerId: string): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "peer-close", peerId });
  }

  private enqueueSyncMessageForWorker(payload: string): void {
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

    this.worker.postMessage({
      type: "sync",
      payload: payloads,
    });
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
    this.runtime.onSyncMessageToSend(() => undefined);
  }
}

/**
 * Wait for a specific message type from a worker.
 */
function waitForMessage<T>(
  worker: Worker,
  predicate: (msg: T) => boolean,
  timeoutMs: number,
  timeoutMessage: string,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(timeoutMessage));
    }, timeoutMs);

    const handler = (event: MessageEvent<T>) => {
      if (predicate(event.data)) {
        cleanup();
        resolve(event.data);
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
  });
}
