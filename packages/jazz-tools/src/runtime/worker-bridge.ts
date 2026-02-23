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
  private workerClientId: string | null = null;
  private initState: "idle" | "pending" | "ready" | "failed" = "idle";
  private initPromise: Promise<string> | null = null;
  private pendingSyncPayloadsForWorker: string[] = [];
  private syncBatchFlushQueued = false;
  private disposed = false;
  private peerSyncListener: ((batch: PeerSyncBatch) => void) | null = null;
  private serverPayloadForwarder: ((payload: string) => void) | null = null;

  constructor(worker: Worker, runtime: Runtime) {
    this.worker = worker;
    this.runtime = runtime;

    // Wire worker → main: incoming sync messages from worker
    this.worker.onmessage = (event: MessageEvent<WorkerToMainMessage>) => {
      const msg = event.data;
      if (msg.type === "sync") {
        // Worker sends payload-only (it's the "server" for main thread)
        for (const payload of msg.payload) {
          this.runtime.onSyncMessageReceived(payload);
        }
      } else if (msg.type === "peer-sync") {
        this.peerSyncListener?.({
          peerId: msg.peerId,
          term: msg.term,
          payload: msg.payload,
        });
      }
    };

    // Wire main → worker: outgoing sync messages from runtime
    this.runtime.onSyncMessageToSend((envelope: string) => {
      if (this.disposed) return;
      const parsed = JSON.parse(envelope);
      // Only forward server-bound messages (worker IS the server)
      if (parsed.destination && "Server" in parsed.destination) {
        const payload = JSON.stringify(parsed.payload);
        if (this.serverPayloadForwarder) {
          this.serverPayloadForwarder(payload);
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
    if (this.initPromise) {
      return this.initPromise;
    }

    this.initState = "pending";

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
    );
    this.worker.postMessage(initMsg);

    this.initPromise = responsePromise
      .then((response) => {
        if (response.type === "error") {
          throw new Error(`Worker init failed: ${response.message}`);
        }

        if (response.type === "init-ok") {
          this.workerClientId = response.clientId;
          this.initState = "ready";
          this.flushPendingSyncToWorker();
          return response.clientId;
        }

        throw new Error("Unexpected worker response");
      })
      .catch((error) => {
        this.initState = "failed";
        this.pendingSyncPayloadsForWorker = [];
        throw error;
      });

    return this.initPromise;
  }

  /**
   * Update auth credentials in the worker.
   */
  updateAuth(auth: {
    jwtToken?: string;
    localAuthMode?: "anonymous" | "demo";
    localAuthToken?: string;
  }): void {
    this.worker.postMessage({ type: "update-auth", ...auth });
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.disposed) return;
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
    if (this.disposed) return;
    this.disposed = true;

    // Detach upstream edge so the next bridge attach performs a clean replay.
    this.runtime.removeServer();

    const shutdownAckPromise = waitForMessage<WorkerToMainMessage>(
      worker,
      (msg) => msg.type === "shutdown-ok",
      5000,
    );
    this.worker.postMessage({ type: "shutdown" });
    try {
      await shutdownAckPromise;
    } catch {
      // Timeout — worker may have already closed
    }

    // Drop any buffered payloads and stop forwarding from stale callbacks.
    this.pendingSyncPayloadsForWorker = [];
    this.serverPayloadForwarder = null;
    this.runtime.onSyncMessageToSend(() => undefined);
  }

  /**
   * Get the client ID the worker assigned to the main thread.
   */
  getWorkerClientId(): string | null {
    return this.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ((payload: string) => void) | null): void {
    if (this.disposed) return;
    this.serverPayloadForwarder = forwarder;
  }

  applyIncomingServerPayload(payload: string): void {
    if (this.disposed) return;
    this.runtime.onSyncMessageReceived(payload);
  }

  replayServerConnection(): void {
    if (this.disposed) return;
    this.runtime.removeServer();
    this.runtime.addServer();
  }

  onPeerSync(listener: (batch: PeerSyncBatch) => void): void {
    this.peerSyncListener = listener;
  }

  openPeer(peerId: string): void {
    if (this.disposed) return;
    this.worker.postMessage({ type: "peer-open", peerId });
  }

  sendPeerSync(peerId: string, term: number, payload: string[]): void {
    if (this.disposed) return;
    if (payload.length === 0) return;
    this.worker.postMessage({
      type: "peer-sync",
      peerId,
      term,
      payload,
    });
  }

  closePeer(peerId: string): void {
    if (this.disposed) return;
    this.worker.postMessage({ type: "peer-close", peerId });
  }

  private enqueueSyncMessageForWorker(payload: string): void {
    if (this.disposed) return;
    this.pendingSyncPayloadsForWorker.push(payload);
    if (this.syncBatchFlushQueued) return;

    this.syncBatchFlushQueued = true;
    queueMicrotask(() => {
      if (this.disposed) {
        this.syncBatchFlushQueued = false;
        this.pendingSyncPayloadsForWorker = [];
        return;
      }
      this.syncBatchFlushQueued = false;
      this.flushPendingSyncToWorker();
    });
  }

  private flushPendingSyncToWorker(): void {
    if (this.initState !== "ready" || this.pendingSyncPayloadsForWorker.length === 0) {
      return;
    }

    const payloads = this.pendingSyncPayloadsForWorker;
    this.pendingSyncPayloadsForWorker = [];

    this.worker.postMessage({
      type: "sync",
      payload: payloads,
    });
  }
}

/**
 * Wait for a specific message type from a worker.
 */
function waitForMessage<T>(
  worker: Worker,
  predicate: (msg: T) => boolean,
  timeoutMs = 10000,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("Worker message timeout"));
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
