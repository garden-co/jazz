/**
 * Dedicated Worker entry point for Jazz.
 *
 * Runs a WasmRuntime with OPFS persistence inside a web worker.
 * Communicates with the main thread via postMessage and optionally
 * syncs with an upstream server via binary HTTP streaming.
 */

import type { InitMessage, MainToWorkerMessage, WorkerToMainMessage } from "./worker-protocol.js";
import { sendSyncPayload, readBinaryFrames } from "../runtime/sync-transport.js";
import { resolveClientId } from "../runtime/client-id.js";

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
let serverClientId: string | null = null; // Client ID assigned by server (from Connected frame)
let streamClientId: string | null = null; // Stable ID we use for /events and /sync
let upstreamServerUrl: string | null = null;
let pendingSyncMessages: string[] = []; // Buffer sync messages until init completes

function post(msg: WorkerToMainMessage): void {
  self.postMessage(msg);
}

// ============================================================================
// Startup: Load WASM
// ============================================================================

async function startup(): Promise<void> {
  try {
    const wasmModule: any = await import("groove-wasm");
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
    const wasmModule: any = await import("groove-wasm");

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
    streamClientId = resolveClientId(msg.clientId);
    upstreamServerUrl = msg.serverUrl ?? null;

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
        if (msg.serverUrl) {
          sendToServer(msg.serverUrl, parsed.payload);
        }
      }
    });

    // Connect to upstream server if URL provided.
    // IMPORTANT: connectStream first to set serverClientId, THEN addServer.
    // addServer() flushes the outbox synchronously (including catalogue sync),
    // and those POSTs need serverClientId to be set.
    if (msg.serverUrl) {
      await connectStream(msg.serverUrl);
      runtime.addServer();
    }

    // Drain any sync messages that arrived before init completed
    for (const payload of pendingSyncMessages) {
      runtime.onSyncMessageReceivedFromClient(mainClientId!, payload);
    }
    pendingSyncMessages = [];

    post({ type: "init-ok", clientId: mainClientId! });
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
    { jwtToken, adminSecret, clientId: serverClientId ?? streamClientId ?? undefined },
    "[worker] ",
  );
}

/**
 * Connect to the server's binary streaming endpoint.
 * Resolves once the Connected frame is received (serverClientId is set).
 * Stream reading continues in the background after resolution.
 */
async function connectStream(serverUrl: string): Promise<void> {
  const headers: Record<string, string> = {
    Accept: "application/octet-stream",
  };
  if (jwtToken) {
    headers["Authorization"] = `Bearer ${jwtToken}`;
  }

  const abortController = new AbortController();
  streamAbortController = abortController;

  const params = streamClientId ? `?client_id=${encodeURIComponent(streamClientId)}` : "";
  const streamUrl = `${serverUrl}/events${params}`;

  try {
    const response = await fetch(streamUrl, {
      headers,
      signal: abortController.signal,
    });

    if (!response.ok) {
      console.error(`[worker] Stream connect failed: ${response.status}`);
      if (streamAbortController === abortController && !abortController.signal.aborted) {
        setTimeout(() => connectStream(serverUrl), 5000);
      }
      return;
    }

    const reader = response.body!.getReader();

    // Read frames in background, resolve once Connected is received
    await new Promise<void>((resolveConnected) => {
      let resolved = false;

      const resolve = () => {
        if (!resolved) {
          resolved = true;
          resolveConnected();
        }
      };

      const readLoop = async () => {
        try {
          await readBinaryFrames(
            reader,
            {
              onSyncMessage: (json) => runtime?.onSyncMessageReceived(json),
              onConnected: (clientId) => {
                serverClientId = clientId;
                streamClientId = clientId;
                resolve();
              },
            },
            "[worker] ",
          );
        } catch (e: any) {
          if (e?.name === "AbortError") {
            resolve();
            return;
          }
          console.error("[worker] Stream error:", e);
        }

        // Reconnect unless aborted
        if (streamAbortController === abortController && !abortController.signal.aborted) {
          setTimeout(() => connectStream(serverUrl), 5000);
        }
        resolve();
      };

      readLoop();
    });
  } catch (e: any) {
    if (e?.name === "AbortError") return;
    console.error("[worker] Stream connect error:", e);
    if (streamAbortController === abortController && !abortController.signal.aborted) {
      setTimeout(() => connectStream(serverUrl), 5000);
    }
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
      if (runtime && mainClientId) {
        runtime.onSyncMessageReceivedFromClient(mainClientId, msg.payload);
      } else {
        pendingSyncMessages.push(msg.payload);
      }
      break;
    }

    case "update-auth":
      jwtToken = msg.jwtToken;
      if (upstreamServerUrl) {
        if (streamAbortController) {
          streamAbortController.abort();
          streamAbortController = null;
        }
        void connectStream(upstreamServerUrl);
      }
      break;

    case "shutdown":
      if (streamAbortController) {
        streamAbortController.abort();
        streamAbortController = null;
      }
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
      if (streamAbortController) {
        streamAbortController.abort();
        streamAbortController = null;
      }
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
