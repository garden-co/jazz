/**
 * Dedicated Worker entry point for Jazz.
 *
 * Runs a WasmRuntime with OPFS persistence inside a web worker.
 * Communicates with the main thread via postMessage (worker-bridge path).
 * Server sync is handled by the Rust-owned WebSocket transport via
 * `runtime.connect()` — no HTTP/SSE code lives here.
 */

import type {
  InitMessage,
  MainToWorkerMessage,
  SequencedSyncPayload,
  WorkerToMainMessage,
} from "./worker-protocol.js";
import { OutboxDestinationKind } from "../runtime/sync-transport.js";
import { mapAuthReason } from "../runtime/auth-state.js";
import { normalizeRuntimeSchemaJson } from "../drivers/schema-wire.js";
import {
  readWorkerRuntimeWasmUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "../runtime/runtime-config.js";
import { installWasmTelemetry } from "../runtime/sync-telemetry.js";
import { httpUrlToWs } from "../runtime/url.js";

// Worker globals — minimal type for DedicatedWorkerGlobalScope
// (Cannot use lib "WebWorker" as it conflicts with DOM types in the main tsconfig)
declare const self: {
  postMessage(msg: unknown, transfer?: Transferable[]): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close(): void;
  location?: { origin?: string; href?: string };
};

type VitestBrowserRunner = {
  wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T>;
};

function ensureVitestWorkerImportShim(): void {
  const globalRef = globalThis as typeof globalThis & {
    __vitest_browser_runner__?: VitestBrowserRunner;
  };

  if (globalRef.__vitest_browser_runner__) {
    return;
  }

  // Vitest browser mode installs this on the page global, but dedicated workers
  // can miss that setup. Provide the same no-op wrapper so transformed worker
  // imports still resolve through the bundler.
  globalRef.__vitest_browser_runner__ = {
    wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T> {
      return loader();
    },
  };
}

ensureVitestWorkerImportShim();

let runtime: any = null; // WasmRuntime instance
let mainClientId: string | null = null;
let initComplete = false;
let wasmInitialized = false;
let pendingSyncMessages: Uint8Array[] = []; // Buffer sync messages until init completes
let pendingPeerSyncMessages: Array<{ peerId: string; term: number; payload: Uint8Array[] }> = [];
let pendingSyncPayloadsForMain: (Uint8Array | string | SequencedSyncPayload)[] = [];
let syncBatchFlushQueued = false;
let rejectedBatchReplayQueued = false;
let bootstrapCatalogueForwarding = false;
const DEFAULT_WASM_LOG_LEVEL = "warn";
let peerRuntimeClientByPeerId = new Map<string, string>();
let peerIdByRuntimeClient = new Map<string, string>();
let peerTermByPeerId = new Map<string, number>();
let currentAuth: Record<string, string> = {};
let disposeWasmTelemetry: (() => void) | null = null;
// Stored after init so reconnect-upstream can re-establish the WS.
let currentWsUrl: string | null = null;

function syncRetainedLocalBatchRecordsToMain(): void {
  if (!runtime) {
    return;
  }
  try {
    const batches = (runtime.loadLocalBatchRecords?.() ?? []).map(attachEncodedLocalBatchRecord);
    post({ type: "local-batch-records-sync", batches });
  } catch (error) {
    console.warn("[worker] loadLocalBatchRecords failed:", error);
  }
}

function attachEncodedLocalBatchRecord(batch: any): any {
  if (!runtime?.loadLocalBatchRecordStorageRow || !batch?.batchId) {
    return batch;
  }
  const encodedRecord = runtime.loadLocalBatchRecordStorageRow(batch.batchId);
  return encodedRecord ? { ...batch, encodedRecord } : batch;
}

function replayNewlyRejectedBatchesToMain(): void {
  if (!runtime) {
    return;
  }
  try {
    const batchIds = runtime.drainRejectedBatchIds?.() ?? [];
    for (const batchId of batchIds) {
      const fate = runtime.loadBatchFate?.(batchId) ?? null;
      if (fate?.kind !== "rejected") {
        continue;
      }
      const batch = attachEncodedLocalBatchRecord(
        runtime.loadLocalBatchRecord?.(batchId) ?? {
          batchId,
          mode: "direct",
          sealed: true,
          latestSettlement: fate,
        },
      );
      post({ type: "mutation-error-replay", batch });
    }
  } catch (error) {
    console.warn("[worker] drainRejectedBatchIds failed:", error);
  }
}

function queueRejectedBatchReplayToMain(): void {
  if (rejectedBatchReplayQueued) {
    return;
  }
  rejectedBatchReplayQueued = true;
  queueMicrotask(() => {
    rejectedBatchReplayQueued = false;
    replayNewlyRejectedBatchesToMain();
  });
}

function resolveAbsoluteWasmUrlFromInitError(error: unknown): string | null {
  const origin = self.location?.origin;
  if (!origin) return null;

  const message = error instanceof Error ? error.message : String(error ?? "");
  const match = message.match(/(\/[^"'\s]+\.wasm)/);
  const wasmPath = match?.[1];
  if (!wasmPath) return null;

  return new URL(wasmPath, origin).href;
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

async function ensureWorkerWasmInitialized(
  wasmModule: any,
  msg: Pick<InitMessage, "runtimeSources" | "fallbackWasmUrl"> | undefined,
): Promise<void> {
  if (wasmInitialized) {
    return;
  }

  const syncInitInput = resolveRuntimeConfigSyncInitInput(msg?.runtimeSources);
  if (syncInitInput) {
    wasmModule.initSync(syncInitInput);
    wasmInitialized = true;
    return;
  }

  if (typeof wasmModule.default !== "function") {
    wasmInitialized = true;
    return;
  }

  const locationHref = self.location?.href;
  const wasmUrl =
    resolveRuntimeConfigWasmUrl(import.meta.url, locationHref, msg?.runtimeSources) ??
    readWorkerRuntimeWasmUrl(locationHref);

  if (wasmUrl) {
    await wasmModule.default({ module_or_path: wasmUrl });
    wasmInitialized = true;
    return;
  }

  try {
    await runWithRootRelativeFetchSupport(() => wasmModule.default());
  } catch (error) {
    const absoluteWasmUrl =
      resolveAbsoluteWasmUrlFromInitError(error) ?? msg?.fallbackWasmUrl ?? null;
    if (!absoluteWasmUrl) {
      throw error;
    }
    await wasmModule.default({ module_or_path: absoluteWasmUrl });
  }

  wasmInitialized = true;
}

function enqueueSyncMessageForMain(payload: Uint8Array | string, sequence?: number | null): void {
  pendingSyncPayloadsForMain.push(typeof sequence === "number" ? { payload, sequence } : payload);
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
  const transfer =
    msg.type === "sync" || msg.type === "peer-sync"
      ? collectPayloadTransferables(msg.payload)
      : undefined;
  self.postMessage(msg, transfer);
}

function collectPayloadTransferables(
  payloads: (Uint8Array | string | SequencedSyncPayload)[],
): Transferable[] {
  const transferables = [];
  for (const entry of payloads) {
    const payload = isSequencedSyncPayload(entry) ? entry.payload : entry;
    if (payload instanceof Uint8Array) {
      transferables.push(payload.buffer);
    }
  }
  return transferables;
}

function isSequencedSyncPayload(value: unknown): value is SequencedSyncPayload {
  return (
    typeof value === "object" &&
    value !== null &&
    "payload" in value &&
    "sequence" in value &&
    typeof (value as { sequence?: unknown }).sequence === "number"
  );
}

// ============================================================================
// Startup: Load WASM
// ============================================================================

async function startup(): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    // Eager init only when the worker URL already carries an explicit wasm URL.
    // Otherwise wait for init so runtimeSources.wasmSource/wasmModule can win.
    if (readWorkerRuntimeWasmUrl(self.location?.href)) {
      await ensureWorkerWasmInitialized(wasmModule, undefined);
    }
    post({ type: "ready" });
  } catch (e: any) {
    post({ type: "error", message: `WASM load failed: ${e.message}` });
  }
}

// ============================================================================
// Pure helpers (exported for unit tests)
// ============================================================================

// mapAuthReason is re-exported from auth-state for backward compatibility with
// any tests that may import it from this module.
export { mapAuthReason } from "../runtime/auth-state.js";

/**
 * Build the WebSocket URL for runtime.connect() from the init message fields.
 */
export function composeConnectUrl(serverUrl: string, appId: string): string {
  return httpUrlToWs(serverUrl, appId);
}

/**
 * Return a new auth object that merges an incoming JWT token (or its absence)
 * into the existing cached auth record.
 *
 * - If `incomingJwtToken` is a non-empty string, it replaces/sets `jwt_token`.
 * - If `incomingJwtToken` is absent/undefined, `jwt_token` is removed.
 * - All other fields (e.g. `admin_secret`) are preserved unchanged.
 */
export function mergeAuth(
  currentAuth: Record<string, string>,
  incomingJwtToken?: string,
): Record<string, string> {
  const next = { ...currentAuth };
  if (incomingJwtToken) {
    next.jwt_token = incomingJwtToken;
  } else {
    delete next.jwt_token;
  }
  return next;
}

/**
 * Open the upstream WebSocket via the Rust transport and notify the main
 * thread whether the bridge should treat the upstream edge as live.
 *
 * Posts `upstream-connected` on success so the bridge releases any queries
 * gated on `waitForUpstreamServerConnection`. Posts `upstream-disconnected`
 * if `runtime.connect` throws synchronously so the bridge keeps the edge
 * marked as down instead of optimistically assuming it is up.
 */
export function performUpstreamConnect(
  runtime: {
    connect?: (url: string, auth: string) => void;
    batchedTick?: () => void;
  },
  post: (msg: WorkerToMainMessage) => void,
  wsUrl: string,
  authJson: string,
): void {
  try {
    runtime.connect?.(wsUrl, authJson);
    runtime.batchedTick?.();
    post({ type: "upstream-connected" });
  } catch (err) {
    console.error("[worker] runtime.connect failed:", err);
    post({ type: "upstream-disconnected" });
  }
}

export function handleUpdateAuth(
  runtime: { updateAuth?: (auth: string) => void },
  authJson: string,
  post: (msg: WorkerToMainMessage) => void,
): void {
  try {
    runtime.updateAuth?.(authJson);
  } catch (e) {
    console.error("[worker] runtime.updateAuth failed:", e);
    post({ type: "auth-failed", reason: "invalid" });
  }
}

// ============================================================================
// Init: Open persistent runtime, register main thread as client
// ============================================================================

async function handleInit(msg: InitMessage): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    (globalThis as any).__JAZZ_WASM_LOG_LEVEL = msg.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
    await ensureWorkerWasmInitialized(wasmModule, msg);
    disposeWasmTelemetry?.();
    disposeWasmTelemetry = installWasmTelemetry({
      wasmModule,
      collectorUrl: msg.telemetryCollectorUrl,
      appId: msg.appId,
      runtimeThread: "worker",
    });
    const schemaJson = normalizeRuntimeSchemaJson(msg.schemaJson);
    initComplete = false;
    currentAuth = {};
    currentWsUrl = null;
    peerRuntimeClientByPeerId.clear();
    peerIdByRuntimeClient.clear();
    peerTermByPeerId.clear();

    // Open persistent OPFS-backed runtime, falling back to ephemeral in-memory
    // storage if OPFS is blocked (e.g. Firefox private browsing raises SecurityError).
    try {
      runtime = await wasmModule.WasmRuntime.openPersistent(
        schemaJson,
        msg.appId,
        msg.env,
        msg.userBranch,
        msg.dbName,
        "local",
        false,
      );
    } catch (e: any) {
      if (e?.name === "SecurityError") {
        console.warn("[jazz] OPFS unavailable (SecurityError) — falling back to ephemeral storage");
        runtime = await wasmModule.WasmRuntime.openEphemeral(
          schemaJson,
          msg.appId,
          msg.env,
          msg.userBranch,
          msg.dbName,
          "local",
          false,
        );
      } else {
        throw e;
      }
    }

    // Register main thread as a Peer client
    mainClientId = runtime.addClient();
    runtime.setClientRole(mainClientId, "peer");

    // Register auth failure callback so the worker can notify the main thread
    // when the Rust transport is rejected by the server (e.g. expired JWT).
    runtime.onAuthFailure?.((reason: string) => {
      post({ type: "upstream-disconnected" });
      post({ type: "auth-failed", reason: mapAuthReason(reason) });
    });

    // Set up outbox routing — only the worker-bridge (client-bound) path.
    // Server sync is handled by the Rust-owned WebSocket transport below.
    runtime.onSyncMessageToSend(
      (
        destinationKind: OutboxDestinationKind,
        destinationId: string,
        payload: Uint8Array | string,
        isCatalogue: boolean,
        sequence?: number | null,
      ) => {
        if (destinationKind === "client") {
          const destinationClientId = destinationId;
          if (destinationClientId === mainClientId) {
            // Local main-thread client-bound payload.
            enqueueSyncMessageForMain(payload, sequence);
            queueRejectedBatchReplayToMain();
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
            payload: [payload as Uint8Array],
          });
        } else if (destinationKind === "server") {
          if (bootstrapCatalogueForwarding) {
            if (isCatalogue) {
              enqueueSyncMessageForMain(payload, sequence);
            }
          }
          // Server-bound payloads are delivered by the Rust transport; no TS action needed.
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
    if (bufferedSyncMessages.length > 0 || bufferedPeerSyncMessages.length > 0) {
      runtime.batchedTick?.();
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

    syncRetainedLocalBatchRecordsToMain();
    replayNewlyRejectedBatchesToMain();

    post({ type: "init-ok", clientId: mainClientId! });

    // Connect to upstream server via Rust-owned WebSocket transport.
    if (msg.serverUrl) {
      if (msg.adminSecret) {
        currentAuth.admin_secret = msg.adminSecret;
      }
      currentAuth = mergeAuth(currentAuth, msg.jwtToken);
      const wsUrl = composeConnectUrl(msg.serverUrl, msg.appId);
      currentWsUrl = wsUrl;
      performUpstreamConnect(runtime, post, wsUrl, JSON.stringify(currentAuth));
    }
  } catch (e: any) {
    post({ type: "error", message: `Init failed: ${e.message}` });
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
  // With the Rust-owned transport, reconnect is handled automatically.
  // No TS-side action needed.
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
        runtime.batchedTick?.();
        if (msg.ackId !== undefined) {
          replayNewlyRejectedBatchesToMain();
          try {
            runtime.flushWal?.();
          } catch (error) {
            console.warn("[worker] flushWal on sync ack failed:", error);
          }
          let hasBatchRecord: boolean | undefined;
          if (msg.ackBatchId) {
            try {
              const replayPayloads = runtime.replayLocalBatchPayloads?.(msg.ackBatchId) ?? [];
              hasBatchRecord = replayPayloads.length > 1;
              if (hasBatchRecord) {
                const sealPayload = replayPayloads[replayPayloads.length - 1];
                if (sealPayload) {
                  runtime.onSyncMessageReceivedFromClient(mainClientId, sealPayload);
                  runtime.batchedTick?.();
                }
                runtime.addServer?.();
                runtime.reconcileLocalBatchWithServer?.(msg.ackBatchId);
                runtime.batchedTick?.();
              }
            } catch (error) {
              hasBatchRecord = false;
              console.warn("[worker] local batch reconciliation failed:", error);
            }
          }
          let batchReconciled: boolean | undefined;
          if (msg.ackBatchId) {
            try {
              const fate = runtime.loadBatchFate?.(msg.ackBatchId);
              batchReconciled =
                fate?.kind === "rejected" ||
                fate?.kind === "acceptedTransaction" ||
                (fate?.kind === "durableDirect" && fate.confirmedTier !== "local");
            } catch {
              batchReconciled = false;
            }
          }
          post({
            type: "sync-ack",
            ackId: msg.ackId,
            hasBatchRecord,
            batchReconciled,
          });
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
      runtime.batchedTick?.();
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

    case "update-auth": {
      currentAuth = mergeAuth(currentAuth, msg.jwtToken);
      if (runtime) {
        handleUpdateAuth(runtime, JSON.stringify(currentAuth), post);
      }
      break;
    }

    case "disconnect-upstream": {
      if (runtime) {
        try {
          runtime.disconnect?.();
          runtime.removeServer?.();
          runtime.batchedTick?.();
          post({ type: "upstream-disconnected" });
        } catch (e) {
          console.error("[worker] disconnect-upstream failed:", e);
        }
      }
      break;
    }

    case "reconnect-upstream": {
      if (runtime && currentWsUrl) {
        runtime.removeServer?.();
        runtime.addServer?.();
        runtime.batchedTick?.();
      }
      break;
    }

    case "shutdown":
      initComplete = false;
      disposeWasmTelemetry?.();
      disposeWasmTelemetry = null;
      if (runtime) {
        runtime.batchedTick?.();
        runtime.flushWal?.();
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

    case "acknowledge-rejected-batch":
      try {
        runtime?.acknowledgeRejectedBatch?.(msg.batchId);
      } catch (error) {
        console.warn("[worker] acknowledgeRejectedBatch failed:", error);
      }
      break;

    case "simulate-crash":
      // Flush WAL buffer to OPFS but do NOT write snapshot.
      // This simulates a crash where writes reached the WAL but no
      // clean checkpoint happened. Recovery must replay the WAL.
      initComplete = false;
      disposeWasmTelemetry?.();
      disposeWasmTelemetry = null;
      if (runtime) {
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
        // Flush the BTree to OPFS so the seeded catalogue entries survive shutdown.
        runtime.flushWal?.();
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
