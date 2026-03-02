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
  applyUserAuthHeaders,
  isExpectedFetchAbortError,
  OutboxDestinationKind,
} from "../runtime/sync-transport.js";
import { normalizeRuntimeSchemaJson } from "../drivers/schema-wire.js";

// Worker globals — minimal type for DedicatedWorkerGlobalScope
// (Cannot use lib "WebWorker" as it conflicts with DOM types in the main tsconfig)
declare const self: {
  postMessage(msg: unknown): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close(): void;
  location?: { origin?: string };
};

let runtime: any = null; // WasmRuntime instance
let mainClientId: string | null = null;
let jwtToken: string | undefined;
let localAuthMode: "anonymous" | "demo" | undefined;
let localAuthToken: string | undefined;
let adminSecret: string | undefined;
let streamAbortController: AbortController | null = null;
let serverClientId: string = generateClientId();
let activeServerUrl: string | null = null;
let activeServerPathPrefix: string | undefined;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let reconnectAttempt = 0;
let streamConnecting = false;
let streamAttached = false;
const streamConnectTimeoutMs = 10_000;
let isShuttingDown = false;
let pendingSyncMessages: string[] = []; // Buffer sync messages until init completes
let pendingPeerSyncMessages: Array<{ peerId: string; term: number; payload: string[] }> = [];
let pendingSyncPayloadsForMain: string[] = [];
let syncBatchFlushQueued = false;
let initComplete = false;
let bootstrapCatalogueForwarding = false;
let peerRuntimeClientByPeerId = new Map<string, string>();
let peerIdByRuntimeClient = new Map<string, string>();
let peerTermByPeerId = new Map<string, number>();

function resolveAbsoluteWasmUrlFromInitError(error: unknown): string | null {
  const origin = self.location?.origin;
  if (!origin) return null;

  const message = error instanceof Error ? error.message : String(error ?? "");
  const match = message.match(/(\/[^"'\s]+\.wasm)/);
  if (!match) return null;

  return new URL(match[1], origin).href;
}

async function runWithRootRelativeFetchSupport<T>(operation: () => Promise<T>): Promise<T> {
  const globalRef = globalThis as typeof globalThis & {
    fetch?: typeof fetch;
  };
  const originalFetch = globalRef.fetch;
  const origin = self.location?.origin;

  if (typeof originalFetch !== "function" || !origin) {
    return operation();
  }

  const patchedFetch: typeof fetch = (input, init) =>
    originalFetch(
      typeof input === "string" && input.startsWith("/")
        ? new URL(input, origin).toString()
        : input,
      init,
    );
  globalRef.fetch = patchedFetch;

  try {
    return await operation();
  } finally {
    globalRef.fetch = originalFetch;
  }
}

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
      try {
        await runWithRootRelativeFetchSupport(() => wasmModule.default());
      } catch (error) {
        const absoluteWasmUrl = resolveAbsoluteWasmUrlFromInitError(error);
        if (!absoluteWasmUrl) {
          throw error;
        }
        await wasmModule.default({ module_or_path: absoluteWasmUrl });
      }
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
    const schemaJson = normalizeRuntimeSchemaJson(msg.schemaJson);
    initComplete = false;
    isShuttingDown = false;
    activeServerUrl = msg.serverUrl ?? null;
    activeServerPathPrefix = msg.serverPathPrefix;
    reconnectAttempt = 0;
    streamAttached = false;
    streamConnecting = false;
    serverClientId = generateClientId();
    peerRuntimeClientByPeerId.clear();
    peerIdByRuntimeClient.clear();
    peerTermByPeerId.clear();
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
      schemaJson,
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
      (
        destinationKind: OutboxDestinationKind,
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ) => {
        if (destinationKind === "client") {
          const destinationClientId = destinationId;
          if (destinationClientId === mainClientId) {
            // Local main-thread client-bound payload.
            enqueueSyncMessageForMain(payloadJson);
            return;
          }

          // Follower peer client-bound payload.
          const peerId = peerIdByRuntimeClient.get(destinationClientId);
          if (!peerId) {
            return;
          }
          const term = peerTermByPeerId.get(peerId) ?? 0;
          post({
            type: "peer-sync",
            peerId,
            term,
            payload: [payloadJson],
          });
        } else if (destinationKind === "server") {
          if (bootstrapCatalogueForwarding) {
            if (isCatalogue) {
              enqueueSyncMessageForMain(payloadJson);
            }
            return;
          }

          // Server-bound → HTTP POST to upstream
          if (activeServerUrl) {
            void sendToServer(activeServerUrl, payloadJson, isCatalogue).catch((error) => {
              if (!isExpectedFetchAbortError(error)) {
                console.error("[worker] Sync POST error:", error);
              }
              detachServer();
              scheduleReconnect();
            });
          }
        }
      },
    );

    // Runtime is now fully ready to ingest client sync traffic.
    const bufferedSyncMessages = pendingSyncMessages;
    pendingSyncMessages = [];
    initComplete = true;

    // Drain sync messages that arrived before init completed.
    for (const payload of bufferedSyncMessages) {
      runtime.onSyncMessageReceivedFromClient(mainClientId!, payload);
    }

    const bufferedPeerSyncMessages = pendingPeerSyncMessages;
    pendingPeerSyncMessages = [];
    for (const buffered of bufferedPeerSyncMessages) {
      const peerClientId = ensurePeerClient(buffered.peerId);
      if (!peerClientId) continue;
      peerTermByPeerId.set(buffered.peerId, buffered.term);
      for (const payload of buffered.payload) {
        runtime.onSyncMessageReceivedFromClient(peerClientId, payload);
      }
    }

    // Bootstrap catalogue-only sync from worker to main runtime.
    // This sends persisted schema/lens objects (including rehydrated ones)
    // without syncing user data rows.
    bootstrapCatalogueForwarding = true;
    try {
      runtime.addServer();
      runtime.removeServer();
    } finally {
      bootstrapCatalogueForwarding = false;
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
async function sendToServer(
  serverUrl: string,
  payloadJson: string,
  isCatalogue: boolean,
): Promise<void> {
  await sendSyncPayload(
    serverUrl,
    payloadJson,
    isCatalogue,
    {
      jwtToken,
      localAuthMode,
      localAuthToken,
      adminSecret,
      clientId: serverClientId,
      pathPrefix: activeServerPathPrefix,
    },
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
  applyUserAuthHeaders(headers, { jwtToken, localAuthMode, localAuthToken });

  streamAbortController = new AbortController();
  let streamConnectTimedOut = false;
  const streamConnectTimeout = setTimeout(() => {
    if (streamAbortController && !streamAbortController.signal.aborted) {
      streamConnectTimedOut = true;
      streamAbortController.abort();
    }
  }, streamConnectTimeoutMs);

  try {
    const eventsUrl = buildEventsUrl(activeServerUrl, serverClientId, activeServerPathPrefix);
    console.log("[worker] Stream connect attempt", { eventsUrl });

    const response = await fetch(eventsUrl, {
      headers,
      signal: streamAbortController.signal,
    });
    clearTimeout(streamConnectTimeout);

    if (!response.ok) {
      console.error(`[worker] Stream connect failed: ${response.status}`);
      detachServer();
      streamConnecting = false;
      scheduleReconnect();
      return;
    }

    if (!response.body || typeof response.body.getReader !== "function") {
      console.error("[worker] Stream connect failed: fetch response body stream unavailable", {
        hasBody: Boolean(response.body),
        bodyType: response.body ? typeof response.body : "undefined",
        url: eventsUrl,
      });
      detachServer();
      streamConnecting = false;
      scheduleReconnect();
      return;
    }

    const reader = response.body.getReader();
    let connected = false;
    await readBinaryFrames(
      reader,
      {
        onSyncMessage: (json) => runtime?.onSyncMessageReceived(json),
        onConnected: (clientId) => {
          console.log("[worker] Stream connected", { clientId });
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
    if (e?.name === "AbortError") {
      if (streamConnectTimedOut) {
        console.error(`[worker] Stream connect timeout after ${streamConnectTimeoutMs}ms`);
        const fetchBaseHint = (globalThis.fetch as { __jazzRnFetchBaseHint?: string } | undefined)
          ?.__jazzRnFetchBaseHint;
        if (fetchBaseHint === "whatwg-fetch/xhr") {
          console.error(
            "[worker] Stream connect likely stalled because fetch is backed by whatwg-fetch/XHR, which does not handle long-lived binary streams.",
          );
        }
        detachServer();
        scheduleReconnect();
      }
      return;
    }
    console.error("[worker] Stream connect error:", e);
  } finally {
    clearTimeout(streamConnectTimeout);
    streamConnecting = false;
  }

  if (streamAbortController && !streamAbortController.signal.aborted) {
    detachServer();
    scheduleReconnect();
  }
}

function ensurePeerClient(peerId: string): string | null {
  if (!runtime) return null;
  const existing = peerRuntimeClientByPeerId.get(peerId);
  if (existing) return existing;

  const clientId = runtime.addClient();
  runtime.setClientRole(clientId, "peer");
  peerRuntimeClientByPeerId.set(peerId, clientId);
  peerIdByRuntimeClient.set(clientId, peerId);
  return clientId;
}

function closePeer(peerId: string): void {
  const runtimeClientId = peerRuntimeClientByPeerId.get(peerId);
  if (!runtimeClientId) return;
  peerRuntimeClientByPeerId.delete(peerId);
  peerIdByRuntimeClient.delete(runtimeClientId);
  peerTermByPeerId.delete(peerId);
}

function flushWalBestEffort(): void {
  if (!runtime || !initComplete) return;
  try {
    runtime.flushWal();
  } catch (error) {
    console.warn("[worker] flushWal on lifecycle hint failed:", error);
  }
}

function nudgeReconnectAfterResume(): void {
  if (!activeServerUrl || isShuttingDown) return;
  if (streamAttached || streamConnecting) return;
  if (reconnectTimer) return;
  reconnectAttempt = 0;
  scheduleReconnect();
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

    case "peer-open":
      if (runtime && initComplete) {
        ensurePeerClient(msg.peerId);
      }
      break;

    case "peer-sync": {
      if (!runtime || !mainClientId || !initComplete) {
        pendingPeerSyncMessages.push({
          peerId: msg.peerId,
          term: msg.term,
          payload: msg.payload,
        });
        break;
      }

      const peerClientId = ensurePeerClient(msg.peerId);
      if (!peerClientId) break;
      peerTermByPeerId.set(msg.peerId, msg.term);
      for (const payload of msg.payload) {
        runtime.onSyncMessageReceivedFromClient(peerClientId, payload);
      }
      break;
    }

    case "peer-close":
      closePeer(msg.peerId);
      break;

    case "lifecycle-hint":
      if (msg.event === "visibility-hidden" || msg.event === "pagehide" || msg.event === "freeze") {
        flushWalBestEffort();
      } else if (msg.event === "visibility-visible" || msg.event === "resume") {
        nudgeReconnectAfterResume();
      }
      break;

    case "update-auth":
      jwtToken = msg.jwtToken;
      localAuthMode = msg.localAuthMode;
      localAuthToken = msg.localAuthToken;
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
      peerRuntimeClientByPeerId.clear();
      peerIdByRuntimeClient.clear();
      peerTermByPeerId.clear();
      pendingPeerSyncMessages = [];
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
      peerRuntimeClientByPeerId.clear();
      peerIdByRuntimeClient.clear();
      peerTermByPeerId.clear();
      pendingPeerSyncMessages = [];
      post({ type: "shutdown-ok" });
      self.close();
      break;

    case "debug-schema-state":
      if (!runtime || !initComplete) {
        post({
          type: "error",
          message: "debug-schema-state requested before worker init complete",
        });
        break;
      }
      try {
        const state = runtime.__debugSchemaState();
        post({ type: "debug-schema-state-ok", state });
      } catch (error: any) {
        post({ type: "error", message: `debug-schema-state failed: ${error?.message ?? error}` });
      }
      break;

    case "debug-seed-live-schema":
      if (!runtime || !initComplete) {
        post({
          type: "error",
          message: "debug-seed-live-schema requested before worker init complete",
        });
        break;
      }
      try {
        runtime.__debugSeedLiveSchema(normalizeRuntimeSchemaJson(msg.schemaJson));
        post({ type: "debug-seed-live-schema-ok" });
      } catch (error: any) {
        post({
          type: "error",
          message: `debug-seed-live-schema failed: ${error?.message ?? error}`,
        });
      }
      break;
  }
};

// Start loading WASM immediately
startup();
