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
  isCataloguePayload,
} from "../runtime/sync-transport.js";
import {
  openPersistentWithRetry,
  isRetryableOpfsInitError,
  OpfsInitRetryCancelled,
  OpfsInitRetryFailure,
} from "./init-retry.js";

// Worker globals — minimal type for DedicatedWorkerGlobalScope
// (Cannot use lib "WebWorker" as it conflicts with DOM types in the main tsconfig)
declare const self: {
  postMessage(msg: unknown): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close(): void;
};

type WorkerPhase =
  | "booting"
  | "ready-for-init"
  | "initializing"
  | "running"
  | "shutting-down"
  | "closed";

type WorkerEvent =
  | { type: "WASM_LOADED" }
  | { type: "WASM_LOAD_FAILED" }
  | { type: "INIT_REQUESTED" }
  | { type: "INIT_SUCCEEDED" }
  | { type: "INIT_FAILED" }
  | { type: "SHUTDOWN_REQUESTED" }
  | { type: "SHUTDOWN_COMPLETED" };

interface WorkerState {
  phase: WorkerPhase;
  runtime: any | null;
  mainClientId: string | null;
  jwtToken: string | undefined;
  localAuthMode: "anonymous" | "demo" | undefined;
  localAuthToken: string | undefined;
  adminSecret: string | undefined;
  streamAbortController: AbortController | null;
  serverClientId: string;
  activeServerUrl: string | null;
  activeServerPathPrefix: string | undefined;
  reconnectTimer: ReturnType<typeof setTimeout> | null;
  reconnectAttempt: number;
  streamConnecting: boolean;
  streamAttached: boolean;
  pendingSyncMessages: string[];
  pendingPeerSyncMessages: Array<{ peerId: string; term: number; payload: string[] }>;
  pendingSyncPayloadsForMain: string[];
  syncBatchFlushQueued: boolean;
  initComplete: boolean;
  bootstrapCatalogueForwarding: boolean;
  peerRuntimeClientByPeerId: Map<string, string>;
  peerIdByRuntimeClient: Map<string, string>;
  peerTermByPeerId: Map<string, number>;
  initSessionId: number;
}

interface FreeableRuntime {
  free: () => void;
}

function isFreeableRuntime(value: unknown): value is FreeableRuntime {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { free?: unknown }).free === "function"
  );
}

class JazzWorkerRuntime {
  private state: WorkerState = {
    phase: "booting",
    runtime: null,
    mainClientId: null,
    jwtToken: undefined,
    localAuthMode: undefined,
    localAuthToken: undefined,
    adminSecret: undefined,
    streamAbortController: null,
    serverClientId: generateClientId(),
    activeServerUrl: null,
    activeServerPathPrefix: undefined,
    reconnectTimer: null,
    reconnectAttempt: 0,
    streamConnecting: false,
    streamAttached: false,
    pendingSyncMessages: [],
    pendingPeerSyncMessages: [],
    pendingSyncPayloadsForMain: [],
    syncBatchFlushQueued: false,
    initComplete: false,
    bootstrapCatalogueForwarding: false,
    peerRuntimeClientByPeerId: new Map<string, string>(),
    peerIdByRuntimeClient: new Map<string, string>(),
    peerTermByPeerId: new Map<string, number>(),
    initSessionId: 0,
  };

  transition(event: WorkerEvent): void {
    switch (event.type) {
      case "WASM_LOADED":
      case "WASM_LOAD_FAILED":
        if (this.state.phase === "booting") {
          this.state.phase = "ready-for-init";
        }
        return;
      case "INIT_REQUESTED":
        if (this.state.phase === "ready-for-init" || this.state.phase === "running") {
          this.state.phase = "initializing";
        }
        return;
      case "INIT_SUCCEEDED":
        if (this.state.phase === "initializing") {
          this.state.phase = "running";
        }
        return;
      case "INIT_FAILED":
        if (this.state.phase === "initializing") {
          this.state.phase = "ready-for-init";
        }
        return;
      case "SHUTDOWN_REQUESTED":
        if (this.state.phase !== "closed") {
          this.state.phase = "shutting-down";
          this.state.initSessionId += 1;
        }
        return;
      case "SHUTDOWN_COMPLETED":
        this.state.phase = "closed";
        return;
    }
  }

  async startup(): Promise<void> {
    try {
      const wasmModule: any = await import("jazz-wasm");
      if (typeof wasmModule.default === "function") {
        await wasmModule.default();
      }
      this.transition({ type: "WASM_LOADED" });
      this.post({ type: "ready" });
    } catch (e: any) {
      this.transition({ type: "WASM_LOAD_FAILED" });
      this.post({ type: "error", message: `WASM load failed: ${e.message}` });
    }
  }

  async onMessage(event: MessageEvent<MainToWorkerMessage>): Promise<void> {
    const msg = event.data;

    switch (msg.type) {
      case "init":
        await this.handleInit(msg);
        break;

      case "sync": {
        const payloads = msg.payload;
        if (this.state.runtime && this.state.mainClientId && this.state.initComplete) {
          for (const payload of payloads) {
            this.state.runtime.onSyncMessageReceivedFromClient(this.state.mainClientId, payload);
          }
        } else {
          this.state.pendingSyncMessages.push(...payloads);
        }
        break;
      }

      case "peer-open":
        if (this.state.runtime && this.state.initComplete) {
          this.ensurePeerClient(msg.peerId);
        }
        break;

      case "peer-sync": {
        if (!this.state.runtime || !this.state.mainClientId || !this.state.initComplete) {
          this.state.pendingPeerSyncMessages.push({
            peerId: msg.peerId,
            term: msg.term,
            payload: msg.payload,
          });
          break;
        }

        const peerClientId = this.ensurePeerClient(msg.peerId);
        if (!peerClientId) break;
        this.state.peerTermByPeerId.set(msg.peerId, msg.term);
        for (const payload of msg.payload) {
          this.state.runtime.onSyncMessageReceivedFromClient(peerClientId, payload);
        }
        break;
      }

      case "peer-close":
        this.closePeer(msg.peerId);
        break;

      case "lifecycle-hint":
        if (
          msg.event === "visibility-hidden" ||
          msg.event === "pagehide" ||
          msg.event === "freeze"
        ) {
          this.flushWalBestEffort();
        } else if (msg.event === "visibility-visible" || msg.event === "resume") {
          this.nudgeReconnectAfterResume();
        }
        break;

      case "update-auth":
        this.state.jwtToken = msg.jwtToken;
        this.state.localAuthMode = msg.localAuthMode;
        this.state.localAuthToken = msg.localAuthToken;
        if (this.state.streamAbortController) {
          this.state.streamAbortController.abort();
          this.state.streamAbortController = null;
        }
        this.detachServer();
        if (this.state.activeServerUrl && !this.isShuttingDownLike()) {
          this.scheduleReconnect();
        }
        break;

      case "shutdown":
        this.completeShutdown("flush");
        break;

      case "simulate-crash":
        this.completeShutdown("flushWal");
        break;

      case "debug-schema-state":
        if (!this.state.runtime || !this.state.initComplete) {
          this.post({
            type: "error",
            message: "debug-schema-state requested before worker init complete",
          });
          break;
        }
        try {
          const statePayload = this.state.runtime.__debugSchemaState();
          this.post({ type: "debug-schema-state-ok", state: statePayload });
        } catch (error: any) {
          this.post({
            type: "error",
            message: `debug-schema-state failed: ${error?.message ?? error}`,
          });
        }
        break;

      case "debug-seed-live-schema":
        if (!this.state.runtime || !this.state.initComplete) {
          this.post({
            type: "error",
            message: "debug-seed-live-schema requested before worker init complete",
          });
          break;
        }
        try {
          const runtimeAny = this.state.runtime as Record<string, unknown>;
          const seedMethod =
            runtimeAny.__debugSeedLiveSchema ??
            runtimeAny.debugSeedLiveSchema ??
            runtimeAny.debug_seed_live_schema;
          if (typeof seedMethod !== "function") {
            throw new Error("worker runtime does not expose a debug seed method");
          }
          (seedMethod as (schemaJson: string) => void).call(this.state.runtime, msg.schemaJson);
          this.post({ type: "debug-seed-live-schema-ok" });
        } catch (error: any) {
          this.post({
            type: "error",
            message: `debug-seed-live-schema failed: ${error?.message ?? error}`,
          });
        }
        break;
    }
  }

  private post(msg: WorkerToMainMessage): void {
    self.postMessage(msg);
  }

  private isShuttingDownLike(): boolean {
    return this.state.phase === "shutting-down" || this.state.phase === "closed";
  }

  private resetBeforeInit(msg: InitMessage): void {
    this.state.initComplete = false;
    this.state.activeServerUrl = msg.serverUrl ?? null;
    this.state.activeServerPathPrefix = msg.serverPathPrefix;
    this.state.reconnectAttempt = 0;
    this.state.streamAttached = false;
    this.state.streamConnecting = false;
    this.state.serverClientId = generateClientId();
    this.state.peerRuntimeClientByPeerId.clear();
    this.state.peerIdByRuntimeClient.clear();
    this.state.peerTermByPeerId.clear();
    this.state.mainClientId = null;
    this.state.bootstrapCatalogueForwarding = false;

    if (this.state.reconnectTimer) {
      clearTimeout(this.state.reconnectTimer);
      this.state.reconnectTimer = null;
    }
    if (this.state.streamAbortController) {
      this.state.streamAbortController.abort();
      this.state.streamAbortController = null;
    }
  }

  private resetPeerState(): void {
    this.state.peerRuntimeClientByPeerId.clear();
    this.state.peerIdByRuntimeClient.clear();
    this.state.peerTermByPeerId.clear();
    this.state.pendingPeerSyncMessages = [];
  }

  private disposeRuntime(clean: "flush" | "flushWal"): void {
    if (!this.state.runtime) return;
    this.detachServer();
    if (clean === "flush") {
      this.state.runtime.flush();
    } else {
      this.state.runtime.flushWal();
    }
    this.state.runtime.free();
    this.state.runtime = null;
    this.state.mainClientId = null;
  }

  private teardownConnectionState(): void {
    this.state.activeServerUrl = null;
    this.state.activeServerPathPrefix = undefined;
    if (this.state.reconnectTimer) {
      clearTimeout(this.state.reconnectTimer);
      this.state.reconnectTimer = null;
    }
    if (this.state.streamAbortController) {
      this.state.streamAbortController.abort();
      this.state.streamAbortController = null;
    }
    this.state.streamAttached = false;
    this.state.streamConnecting = false;
    this.state.reconnectAttempt = 0;
  }

  private completeShutdown(clean: "flush" | "flushWal"): void {
    this.transition({ type: "SHUTDOWN_REQUESTED" });
    this.state.initComplete = false;
    this.teardownConnectionState();
    this.disposeRuntime(clean);
    this.resetPeerState();
    this.post({ type: "shutdown-ok" });
    this.transition({ type: "SHUTDOWN_COMPLETED" });
    self.close();
  }

  private enqueueSyncMessageForMain(payload: string): void {
    this.state.pendingSyncPayloadsForMain.push(payload);
    if (this.state.syncBatchFlushQueued) return;

    this.state.syncBatchFlushQueued = true;
    queueMicrotask(() => {
      this.state.syncBatchFlushQueued = false;
      const payloads = this.state.pendingSyncPayloadsForMain;
      this.state.pendingSyncPayloadsForMain = [];
      if (payloads.length === 0) return;
      this.post({ type: "sync", payload: payloads });
    });
  }

  private ensurePeerClient(peerId: string): string | null {
    if (!this.state.runtime) return null;
    const existing = this.state.peerRuntimeClientByPeerId.get(peerId);
    if (existing) return existing;

    const clientId = this.state.runtime.addClient();
    this.state.runtime.setClientRole(clientId, "peer");
    this.state.peerRuntimeClientByPeerId.set(peerId, clientId);
    this.state.peerIdByRuntimeClient.set(clientId, peerId);
    return clientId;
  }

  private closePeer(peerId: string): void {
    const runtimeClientId = this.state.peerRuntimeClientByPeerId.get(peerId);
    if (!runtimeClientId) return;
    this.state.peerRuntimeClientByPeerId.delete(peerId);
    this.state.peerIdByRuntimeClient.delete(runtimeClientId);
    this.state.peerTermByPeerId.delete(peerId);
  }

  private async sendToServer(serverUrl: string, payload: any): Promise<void> {
    await sendSyncPayload(
      serverUrl,
      payload,
      {
        jwtToken: this.state.jwtToken,
        localAuthMode: this.state.localAuthMode,
        localAuthToken: this.state.localAuthToken,
        adminSecret: this.state.adminSecret,
        clientId: this.state.serverClientId,
        pathPrefix: this.state.activeServerPathPrefix,
      },
      "[worker] ",
    );
  }

  private attachServer(): void {
    if (!this.state.runtime) return;
    if (this.state.streamAttached) {
      this.state.runtime.removeServer();
    }
    this.state.runtime.addServer();
    this.state.streamAttached = true;
    this.state.reconnectAttempt = 0;
  }

  private detachServer(): void {
    if (!this.state.runtime || !this.state.streamAttached) return;
    this.state.runtime.removeServer();
    this.state.streamAttached = false;
  }

  private scheduleReconnect(): void {
    if (this.isShuttingDownLike() || !this.state.activeServerUrl) return;
    if (this.state.reconnectTimer) return;

    const baseMs = 300;
    const maxMs = 10_000;
    const jitterMs = Math.floor(Math.random() * 200);
    const delayMs = Math.min(maxMs, baseMs * 2 ** this.state.reconnectAttempt) + jitterMs;
    this.state.reconnectAttempt += 1;

    this.state.reconnectTimer = setTimeout(() => {
      this.state.reconnectTimer = null;
      void this.connectStream();
    }, delayMs);
  }

  private async connectStream(): Promise<void> {
    if (this.state.streamConnecting || !this.state.activeServerUrl || this.isShuttingDownLike())
      return;
    this.state.streamConnecting = true;

    const headers: Record<string, string> = {
      Accept: "application/octet-stream",
    };
    applyUserAuthHeaders(headers, {
      jwtToken: this.state.jwtToken,
      localAuthMode: this.state.localAuthMode,
      localAuthToken: this.state.localAuthToken,
    });

    this.state.streamAbortController = new AbortController();

    try {
      const eventsUrl = buildEventsUrl(
        this.state.activeServerUrl,
        this.state.serverClientId,
        this.state.activeServerPathPrefix,
      );

      const response = await fetch(eventsUrl, {
        headers,
        signal: this.state.streamAbortController.signal,
      });

      if (!response.ok) {
        console.error(`[worker] Stream connect failed: ${response.status}`);
        this.detachServer();
        this.state.streamConnecting = false;
        this.scheduleReconnect();
        return;
      }

      const reader = response.body!.getReader();
      let connected = false;
      await readBinaryFrames(
        reader,
        {
          onSyncMessage: (json) => this.state.runtime?.onSyncMessageReceived(json),
          onConnected: (clientId) => {
            this.state.serverClientId = clientId;
            if (!connected) {
              connected = true;
              this.attachServer();
            }
          },
        },
        "[worker] ",
      );
    } catch (e: any) {
      if (e?.name === "AbortError") return;
      console.error("[worker] Stream connect error:", e);
    } finally {
      this.state.streamConnecting = false;
    }

    if (this.state.streamAbortController && !this.state.streamAbortController.signal.aborted) {
      this.detachServer();
      this.scheduleReconnect();
    }
  }

  private flushWalBestEffort(): void {
    if (!this.state.runtime || !this.state.initComplete) return;
    try {
      this.state.runtime.flushWal();
    } catch (error) {
      console.warn("[worker] flushWal on lifecycle hint failed:", error);
    }
  }

  private nudgeReconnectAfterResume(): void {
    if (!this.state.activeServerUrl || this.isShuttingDownLike()) return;
    if (this.state.streamAttached || this.state.streamConnecting) return;
    if (this.state.reconnectTimer) return;
    this.state.reconnectAttempt = 0;
    this.scheduleReconnect();
  }

  private async handleInit(msg: InitMessage): Promise<void> {
    this.transition({ type: "INIT_REQUESTED" });
    const thisInitSessionId = ++this.state.initSessionId;

    try {
      const wasmModule: any = await import("jazz-wasm");
      this.resetBeforeInit(msg);

      const initResult = await openPersistentWithRetry({
        open: () =>
          wasmModule.WasmRuntime.openPersistent(
            msg.schemaJson,
            msg.appId,
            msg.env,
            msg.userBranch,
            msg.dbName,
            "worker",
          ),
        isRetryable: isRetryableOpfsInitError,
        isCancelled: () =>
          this.state.initSessionId !== thisInitSessionId || this.isShuttingDownLike(),
      });

      if (this.state.initSessionId !== thisInitSessionId || this.isShuttingDownLike()) {
        if (isFreeableRuntime(initResult?.value)) {
          try {
            initResult.value.free();
          } catch {
            // Best effort if cancellation raced after open succeeded.
          }
        }
        return;
      }

      this.state.runtime = initResult.value;

      this.state.jwtToken = msg.jwtToken;
      this.state.localAuthMode = msg.localAuthMode;
      this.state.localAuthToken = msg.localAuthToken;
      this.state.adminSecret = msg.adminSecret;

      this.state.mainClientId = this.state.runtime.addClient();
      this.state.runtime.setClientRole(this.state.mainClientId, "peer");

      this.state.runtime.onSyncMessageToSend((envelope: string) => {
        const parsed = JSON.parse(envelope);

        if (parsed.destination && "Client" in parsed.destination) {
          const destinationClientId = parsed.destination.Client as string;
          if (destinationClientId === this.state.mainClientId) {
            this.enqueueSyncMessageForMain(JSON.stringify(parsed.payload));
            return;
          }

          const peerId = this.state.peerIdByRuntimeClient.get(destinationClientId);
          if (!peerId) {
            return;
          }
          const term = this.state.peerTermByPeerId.get(peerId) ?? 0;
          this.post({
            type: "peer-sync",
            peerId,
            term,
            payload: [JSON.stringify(parsed.payload)],
          });
        } else if (parsed.destination && "Server" in parsed.destination) {
          if (this.state.bootstrapCatalogueForwarding) {
            if (isCataloguePayload(parsed.payload)) {
              this.enqueueSyncMessageForMain(JSON.stringify(parsed.payload));
            }
            return;
          }

          if (this.state.activeServerUrl) {
            void this.sendToServer(this.state.activeServerUrl, parsed.payload).catch((error) => {
              console.error("[worker] Sync POST error:", error);
              this.detachServer();
              this.scheduleReconnect();
            });
          }
        }
      });

      const bufferedSyncMessages = this.state.pendingSyncMessages;
      this.state.pendingSyncMessages = [];
      this.state.initComplete = true;

      for (const payload of bufferedSyncMessages) {
        this.state.runtime.onSyncMessageReceivedFromClient(this.state.mainClientId!, payload);
      }

      const bufferedPeerSyncMessages = this.state.pendingPeerSyncMessages;
      this.state.pendingPeerSyncMessages = [];
      for (const buffered of bufferedPeerSyncMessages) {
        const peerClientId = this.ensurePeerClient(buffered.peerId);
        if (!peerClientId) continue;
        this.state.peerTermByPeerId.set(buffered.peerId, buffered.term);
        for (const payload of buffered.payload) {
          this.state.runtime.onSyncMessageReceivedFromClient(peerClientId, payload);
        }
      }

      this.state.bootstrapCatalogueForwarding = true;
      try {
        this.state.runtime.addServer();
        this.state.runtime.removeServer();
      } finally {
        this.state.bootstrapCatalogueForwarding = false;
      }

      this.transition({ type: "INIT_SUCCEEDED" });
      this.post({ type: "init-ok", clientId: this.state.mainClientId! });

      if (this.state.activeServerUrl) {
        void this.connectStream();
      }
    } catch (e: unknown) {
      if (e instanceof OpfsInitRetryCancelled) {
        return;
      }

      if (this.state.initSessionId !== thisInitSessionId || this.isShuttingDownLike()) {
        return;
      }

      this.transition({ type: "INIT_FAILED" });

      if (e instanceof OpfsInitRetryFailure) {
        this.post({
          type: "error",
          message: `Init failed: ${e.message}`,
        });
        return;
      }

      const message = e instanceof Error ? e.message : String(e);
      this.post({ type: "error", message: `Init failed: ${message}` });
    }
  }
}

const runtime = new JazzWorkerRuntime();
self.onmessage = (event: MessageEvent) => {
  void runtime.onMessage(event as MessageEvent<MainToWorkerMessage>);
};
void runtime.startup();
