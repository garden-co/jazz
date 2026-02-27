/**
 * WorkerBridge — Main-thread side of the worker communication bridge.
 *
 * Wires a main-thread WasmRuntime (in-memory) to a dedicated worker
 * (OPFS-persistent) via postMessage. The worker acts as the "server"
 * for the main thread's runtime.
 */
import type { Runtime } from "./client.js";
import type { WorkerLifecycleEvent } from "../worker/worker-protocol.js";
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
export declare class WorkerBridge {
  private worker;
  private runtime;
  private workerClientId;
  private initState;
  private initPromise;
  private pendingSyncPayloadsForWorker;
  private syncBatchFlushQueued;
  private disposed;
  private peerSyncListener;
  private serverPayloadForwarder;
  constructor(worker: Worker, runtime: Runtime);
  /**
   * Initialize the worker with schema and config.
   *
   * Waits for the worker to respond with init-ok.
   */
  init(options: WorkerBridgeOptions): Promise<string>;
  /**
   * Update auth credentials in the worker.
   */
  updateAuth(auth: {
    jwtToken?: string;
    localAuthMode?: "anonymous" | "demo";
    localAuthToken?: string;
  }): void;
  sendLifecycleHint(event: WorkerLifecycleEvent): void;
  /**
   * Shut down the worker and wait for OPFS handles to be released.
   *
   * @param worker The Worker instance (needed for listening to shutdown-ok)
   */
  shutdown(worker: Worker): Promise<void>;
  /**
   * Get the client ID the worker assigned to the main thread.
   */
  getWorkerClientId(): string | null;
  setServerPayloadForwarder(forwarder: ((payload: string) => void) | null): void;
  applyIncomingServerPayload(payload: string): void;
  replayServerConnection(): void;
  onPeerSync(listener: (batch: PeerSyncBatch) => void): void;
  openPeer(peerId: string): void;
  sendPeerSync(peerId: string, term: number, payload: string[]): void;
  closePeer(peerId: string): void;
  private enqueueSyncMessageForWorker;
  private flushPendingSyncToWorker;
}
//# sourceMappingURL=worker-bridge.d.ts.map
