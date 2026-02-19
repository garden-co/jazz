/**
 * WorkerBridge — Main-thread side of the worker communication bridge.
 *
 * Wires a main-thread WasmRuntime (in-memory) to a dedicated worker
 * (OPFS-persistent) via postMessage. The worker acts as the "server"
 * for the main thread's runtime.
 */

import type { Runtime } from "./client.js";
import type { InitMessage, WorkerToMainMessage } from "../worker/worker-protocol.js";

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

  constructor(worker: Worker, runtime: Runtime) {
    this.worker = worker;
    this.runtime = runtime;

    // Wire worker → main: incoming sync messages from worker
    this.worker.onmessage = (event: MessageEvent<WorkerToMainMessage>) => {
      const msg = event.data;
      if (msg.type === "sync") {
        // Worker sends payload-only (it's the "server" for main thread)
        this.runtime.onSyncMessageReceived(msg.payload);
      }
    };

    // Wire main → worker: outgoing sync messages from runtime
    this.runtime.onSyncMessageToSend((envelope: string) => {
      const parsed = JSON.parse(envelope);
      // Only forward server-bound messages (worker IS the server)
      if (parsed.destination && "Server" in parsed.destination) {
        this.worker.postMessage({
          type: "sync",
          payload: JSON.stringify(parsed.payload),
        });
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
  async init(options: WorkerBridgeOptions): Promise<string> {
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
      adminSecret: options.adminSecret,
      clientId: "", // Worker generates its own client ID for main thread
    };

    this.worker.postMessage(initMsg);

    const response = await waitForMessage<WorkerToMainMessage>(
      this.worker,
      (msg) => msg.type === "init-ok" || msg.type === "error",
    );

    if (response.type === "error") {
      throw new Error(`Worker init failed: ${response.message}`);
    }

    if (response.type === "init-ok") {
      this.workerClientId = response.clientId;
      return response.clientId;
    }

    throw new Error("Unexpected worker response");
  }

  /**
   * Update auth credentials in the worker.
   */
  updateAuth(jwtToken: string): void {
    this.worker.postMessage({ type: "update-auth", jwtToken });
  }

  /**
   * Shut down the worker and wait for OPFS handles to be released.
   *
   * @param worker The Worker instance (needed for listening to shutdown-ok)
   */
  async shutdown(worker: Worker): Promise<void> {
    this.worker.postMessage({ type: "shutdown" });
    try {
      await waitForMessage<WorkerToMainMessage>(worker, (msg) => msg.type === "shutdown-ok", 5000);
    } catch {
      // Timeout — worker may have already closed
    }
  }

  /**
   * Get the client ID the worker assigned to the main thread.
   */
  getWorkerClientId(): string | null {
    return this.workerClientId;
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
