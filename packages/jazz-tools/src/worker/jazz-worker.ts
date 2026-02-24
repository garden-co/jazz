/**
 * Dedicated Worker entry point for Jazz.
 *
 * Runs a WasmRuntime with OPFS persistence inside a web worker.
 * Communicates with the main thread via postMessage and optionally
 * syncs with an upstream server via binary HTTP streaming.
 */

import type { InitMessage, MainToWorkerMessage, WorkerToMainMessage } from "./worker-protocol.js";
import {
  createRuntimeSyncStreamController,
  createSyncOutboxRouter,
  sendSyncPayload,
  generateClientId,
} from "../runtime/sync-transport.js";

// Worker globals — minimal type for DedicatedWorkerGlobalScope
// (Cannot use lib "WebWorker" as it conflicts with DOM types in the main tsconfig)
declare const self: {
  postMessage(msg: unknown): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close(): void;
};

let runtime: any = null; // WasmRuntime instance
let mainClientId: string | null = null;
let jwtToken: string | undefined;
let localAuthMode: "anonymous" | "demo" | undefined;
let localAuthToken: string | undefined;
let adminSecret: string | undefined;
let serverClientId: string = generateClientId();
let pendingSyncMessages: string[] = []; // Buffer sync messages until init completes
let pendingSyncPayloadsForMain: string[] = [];
let syncBatchFlushQueued = false;
let initComplete = false;

const streamController = createRuntimeSyncStreamController({
  logPrefix: "[worker] ",
  getRuntime: () => runtime,
  getAuth: () => ({ jwtToken, localAuthMode, localAuthToken }),
  getClientId: () => serverClientId,
  setClientId: (clientId) => {
    serverClientId = clientId;
  },
});

function enqueueSyncMessageForMain(payload: string): void {
  pendingSyncPayloadsForMain.push(payload);
  if (syncBatchFlushQueued) return;

  syncBatchFlushQueued = true;
  queueMicrotask(() => {
    syncBatchFlushQueued = false;
    const payloads = pendingSyncPayloadsForMain;
    pendingSyncPayloadsForMain = [];
    if (payloads.length === 0) return;
    post({ type: "sync", payload: payloads });
  });
}

function post(msg: WorkerToMainMessage): void {
  self.postMessage(msg);
}

// ============================================================================
// Startup: Load WASM
// ============================================================================

async function startup(): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    // With vite-plugin-wasm, init happens at import time and default is not a function.
    // Without it, default is the init function that must be called.
    if (typeof wasmModule.default === "function") {
      await wasmModule.default();
    }
    post({ type: "ready" });
  } catch (e: any) {
    post({ type: "error", message: `WASM load failed: ${e.message}` });
  }
}

// ============================================================================
// Init: Open persistent runtime, register main thread as client
// ============================================================================

async function handleInit(msg: InitMessage): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    initComplete = false;
    streamController.stop();
    serverClientId = generateClientId();

    // Open persistent OPFS-backed runtime with Worker tier
    runtime = await wasmModule.WasmRuntime.openPersistent(
      msg.schemaJson,
      msg.appId,
      msg.env,
      msg.userBranch,
      msg.dbName,
      "worker",
    );

    // Store auth
    jwtToken = msg.jwtToken;
    localAuthMode = msg.localAuthMode;
    localAuthToken = msg.localAuthToken;
    adminSecret = msg.adminSecret;

    // Register main thread as a Peer client
    mainClientId = runtime.addClient();
    runtime.setClientRole(mainClientId, "peer");

    // Set up outbox routing
    runtime.onSyncMessageToSend(
      createSyncOutboxRouter({
        logPrefix: "[worker] ",
        onClientPayload: enqueueSyncMessageForMain,
        onServerPayload: (payload) => sendToServer(payload),
        onServerPayloadError: (error) => {
          console.error("[worker] Sync POST error:", error);
          streamController.notifyTransportFailure();
        },
      }),
    );

    // Runtime is now fully ready to ingest client sync traffic.
    const bufferedSyncMessages = pendingSyncMessages;
    pendingSyncMessages = [];
    initComplete = true;

    // Drain sync messages that arrived before init completed.
    for (const payload of bufferedSyncMessages) {
      runtime.onSyncMessageReceivedFromClient(mainClientId!, payload);
    }

    post({ type: "init-ok", clientId: mainClientId! });

    // Connect upstream in background (do not block init).
    if (msg.serverUrl) {
      streamController.start(msg.serverUrl, msg.serverPathPrefix);
    }
  } catch (e: any) {
    post({ type: "error", message: `Init failed: ${e.message}` });
  }
}

// ============================================================================
// Upstream server communication
// ============================================================================

/** POST a sync payload to the upstream server. */
async function sendToServer(payload: unknown): Promise<void> {
  const serverUrl = streamController.getServerUrl();
  if (!serverUrl) return;

  await sendSyncPayload(
    serverUrl,
    payload,
    {
      jwtToken,
      localAuthMode,
      localAuthToken,
      adminSecret,
      clientId: serverClientId,
      pathPrefix: streamController.getPathPrefix(),
    },
    "[worker] ",
  );
}

// ============================================================================
// Message handler
// ============================================================================

self.onmessage = async (event: MessageEvent<MainToWorkerMessage>) => {
  const msg = event.data;

  switch (msg.type) {
    case "init":
      await handleInit(msg);
      break;

    case "sync": {
      const payloads = msg.payload;
      if (runtime && mainClientId && initComplete) {
        for (const payload of payloads) {
          runtime.onSyncMessageReceivedFromClient(mainClientId, payload);
        }
      } else {
        pendingSyncMessages.push(...payloads);
      }
      break;
    }

    case "update-auth":
      jwtToken = msg.jwtToken;
      localAuthMode = msg.localAuthMode;
      localAuthToken = msg.localAuthToken;
      // Reconnect stream to bind the new token.
      streamController.updateAuth();
      break;

    case "shutdown":
      initComplete = false;
      streamController.stop();
      if (runtime) {
        runtime.flush();
        runtime.free(); // Triggers Rust Drop → closes OPFS exclusive handles
        runtime = null;
      }
      post({ type: "shutdown-ok" });
      self.close();
      break;

    case "simulate-crash":
      // Flush WAL buffer to OPFS but do NOT write snapshot.
      // This simulates a crash where writes reached the WAL but no
      // clean checkpoint happened. Recovery must replay the WAL.
      initComplete = false;
      streamController.stop();
      if (runtime) {
        runtime.flushWal(); // WAL buffer → OPFS, but no snapshot
        runtime.free(); // Drop → releases OPFS exclusive handles
        runtime = null;
      }
      post({ type: "shutdown-ok" });
      self.close();
      break;
  }
};

// Start loading WASM immediately
startup();
