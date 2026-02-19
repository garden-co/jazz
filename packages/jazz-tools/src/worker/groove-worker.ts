/**
 * Dedicated Worker entry point for Jazz.
 *
 * Runs a WasmRuntime with OPFS persistence inside a web worker.
 * Communicates with the main thread via postMessage and optionally
 * syncs with an upstream server via binary HTTP streaming.
 */

import type { InitMessage, MainToWorkerMessage, WorkerToMainMessage } from "./worker-protocol.js";
import {
  sendSyncPayload,
  readBinaryFrames,
  generateClientId,
  buildEventsUrl,
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
let adminSecret: string | undefined;
let streamAbortController: AbortController | null = null;
let serverClientId: string = generateClientId();
let activeServerUrl: string | null = null;
let activeServerPathPrefix: string | undefined;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let reconnectAttempt = 0;
let streamConnecting = false;
let streamAttached = false;
let isShuttingDown = false;
let pendingSyncMessages: string[] = []; // Buffer sync messages until init completes
let initComplete = false;

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
    isShuttingDown = false;
    activeServerUrl = msg.serverUrl ?? null;
    activeServerPathPrefix = msg.serverPathPrefix;
    reconnectAttempt = 0;
    streamAttached = false;
    streamConnecting = false;
    serverClientId = generateClientId();
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (streamAbortController) {
      streamAbortController.abort();
      streamAbortController = null;
    }

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
    adminSecret = msg.adminSecret;

    // Register main thread as a Peer client
    mainClientId = runtime.addClient();
    runtime.setClientRole(mainClientId, "peer");

    // Set up outbox routing
    runtime.onSyncMessageToSend((envelope: string) => {
      const parsed = JSON.parse(envelope);

      if (parsed.destination && "Client" in parsed.destination) {
        // Client-bound → send payload to main thread
        post({ type: "sync", payload: JSON.stringify(parsed.payload) });
      } else if (parsed.destination && "Server" in parsed.destination) {
        // Server-bound → HTTP POST to upstream
        if (activeServerUrl) {
          void sendToServer(activeServerUrl, parsed.payload).catch((error) => {
            console.error("[worker] Sync POST error:", error);
            detachServer();
            scheduleReconnect();
          });
        }
      }
    });

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
    if (activeServerUrl) {
      void connectStream();
    }
  } catch (e: any) {
    post({ type: "error", message: `Init failed: ${e.message}` });
  }
}

// ============================================================================
// Upstream server communication
// ============================================================================

/** POST a sync payload to the upstream server. */
async function sendToServer(serverUrl: string, payload: any): Promise<void> {
  await sendSyncPayload(
    serverUrl,
    payload,
    { jwtToken, adminSecret, clientId: serverClientId, pathPrefix: activeServerPathPrefix },
    "[worker] ",
  );
}

function attachServer(): void {
  if (!runtime) return;
  // Re-attach every time the stream reconnects so query subscriptions replay.
  if (streamAttached) {
    runtime.removeServer();
  }
  runtime.addServer();
  streamAttached = true;
  reconnectAttempt = 0;
}

function detachServer(): void {
  if (!runtime || !streamAttached) return;
  runtime.removeServer();
  streamAttached = false;
}

function scheduleReconnect(): void {
  if (isShuttingDown || !activeServerUrl) return;
  if (reconnectTimer) return;

  const baseMs = 300;
  const maxMs = 10_000;
  const jitterMs = Math.floor(Math.random() * 200);
  const delayMs = Math.min(maxMs, baseMs * 2 ** reconnectAttempt) + jitterMs;
  reconnectAttempt += 1;

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    void connectStream();
  }, delayMs);
}

/** Connect to the server's binary streaming endpoint. */
async function connectStream(): Promise<void> {
  if (streamConnecting || !activeServerUrl || isShuttingDown) return;
  streamConnecting = true;

  const headers: Record<string, string> = {
    Accept: "application/octet-stream",
  };
  if (jwtToken) {
    headers["Authorization"] = `Bearer ${jwtToken}`;
  }

  streamAbortController = new AbortController();

  try {
    const eventsUrl = buildEventsUrl(activeServerUrl, serverClientId, activeServerPathPrefix);

    const response = await fetch(eventsUrl, {
      headers,
      signal: streamAbortController.signal,
    });

    if (!response.ok) {
      console.error(`[worker] Stream connect failed: ${response.status}`);
      detachServer();
      streamConnecting = false;
      scheduleReconnect();
      return;
    }

    const reader = response.body!.getReader();
    let connected = false;
    await readBinaryFrames(
      reader,
      {
        onSyncMessage: (json) => runtime?.onSyncMessageReceived(json),
        onConnected: (clientId) => {
          serverClientId = clientId;
          if (!connected) {
            connected = true;
            attachServer();
          }
        },
      },
      "[worker] ",
    );
  } catch (e: any) {
    if (e?.name === "AbortError") return;
    console.error("[worker] Stream connect error:", e);
  } finally {
    streamConnecting = false;
  }

  if (streamAbortController && !streamAbortController.signal.aborted) {
    detachServer();
    scheduleReconnect();
  }
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
      if (runtime && mainClientId && initComplete) {
        runtime.onSyncMessageReceivedFromClient(mainClientId, msg.payload);
      } else {
        pendingSyncMessages.push(msg.payload);
      }
      break;
    }

    case "update-auth":
      jwtToken = msg.jwtToken;
      // Reconnect stream to bind the new token.
      if (streamAbortController) {
        streamAbortController.abort();
        streamAbortController = null;
      }
      detachServer();
      if (activeServerUrl && !isShuttingDown) {
        scheduleReconnect();
      }
      break;

    case "shutdown":
      isShuttingDown = true;
      initComplete = false;
      activeServerUrl = null;
      activeServerPathPrefix = undefined;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (streamAbortController) {
        streamAbortController.abort();
        streamAbortController = null;
      }
      if (runtime) {
        detachServer();
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
      isShuttingDown = true;
      initComplete = false;
      activeServerUrl = null;
      activeServerPathPrefix = undefined;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (streamAbortController) {
        streamAbortController.abort();
        streamAbortController = null;
      }
      if (runtime) {
        detachServer();
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
