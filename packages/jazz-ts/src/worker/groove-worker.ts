/**
 * Dedicated Worker entry point for Jazz.
 *
 * Runs a WasmRuntime with OPFS persistence inside a web worker.
 * Communicates with the main thread via postMessage and optionally
 * syncs with an upstream server via binary HTTP streaming.
 */

import type { InitMessage, MainToWorkerMessage, WorkerToMainMessage } from "./worker-protocol.js";

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
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    // Check if catalogue payload → admin header
    if (isCataloguePayload(payload)) {
      if (adminSecret) {
        headers["X-Jazz-Admin-Secret"] = adminSecret;
      }
    } else if (jwtToken) {
      headers["Authorization"] = `Bearer ${jwtToken}`;
    }

    const body = JSON.stringify({
      payload,
      client_id: serverClientId ?? "00000000-0000-0000-0000-000000000000",
    });

    const response = await fetch(`${serverUrl}/sync`, {
      method: "POST",
      headers,
      body,
    });

    if (!response.ok) {
      console.error("[worker] Sync POST error:", response.statusText);
    }
  } catch (e) {
    console.error("[worker] Sync POST error:", e);
  }
}

function isCataloguePayload(payload: any): boolean {
  const metadata = payload?.ObjectUpdated?.metadata?.metadata;
  if (metadata) {
    const t = metadata["type"];
    return t === "catalogue_schema" || t === "catalogue_lens";
  }
  return false;
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

  streamAbortController = new AbortController();

  try {
    const response = await fetch(`${serverUrl}/events`, {
      headers,
      signal: streamAbortController.signal,
    });

    if (!response.ok) {
      console.error(`[worker] Stream connect failed: ${response.status}`);
      setTimeout(() => connectStream(serverUrl), 5000);
      return;
    }

    const reader = response.body!.getReader();

    // Read frames in background, resolve once Connected is received
    await new Promise<void>((resolveConnected) => {
      let resolved = false;
      let buffer = new Uint8Array(0);

      const readLoop = async () => {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            // Append chunk to buffer
            const newBuffer = new Uint8Array(buffer.length + value.length);
            newBuffer.set(buffer);
            newBuffer.set(value, buffer.length);
            buffer = newBuffer;

            // Read complete frames
            while (buffer.length >= 4) {
              const len = new DataView(buffer.buffer, buffer.byteOffset).getUint32(0, false);
              if (buffer.length < 4 + len) break;
              const json = new TextDecoder().decode(buffer.slice(4, 4 + len));
              buffer = buffer.slice(4 + len);
              try {
                const event = JSON.parse(json);
                if (event.type === "Connected" && event.client_id) {
                  serverClientId = event.client_id;
                  if (!resolved) {
                    resolved = true;
                    resolveConnected();
                  }
                } else if (event.type === "SyncUpdate" && runtime) {
                  runtime.onSyncMessageReceived(JSON.stringify(event.payload));
                }
              } catch (e) {
                console.error("[worker] Stream parse error:", e);
              }
            }
          }
        } catch (e: any) {
          if (e?.name === "AbortError") {
            if (!resolved) {
              resolved = true;
              resolveConnected();
            }
            return;
          }
          console.error("[worker] Stream error:", e);
        }

        // Reconnect unless aborted
        if (streamAbortController && !streamAbortController.signal.aborted) {
          setTimeout(() => connectStream(serverUrl), 5000);
        }
        if (!resolved) {
          resolved = true;
          resolveConnected();
        }
      };

      readLoop();
    });
  } catch (e: any) {
    if (e?.name === "AbortError") return;
    console.error("[worker] Stream connect error:", e);
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
      // TODO: Reconnect stream with new token if needed
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
