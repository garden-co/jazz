import { loadWasmModule, type WasmModule } from "../runtime/client.js";
import { IndexedDbPageStore } from "../runtime/indexeddb-page-store.js";
import { installWasmTelemetry } from "../runtime/sync-telemetry.js";
import {
  BrowserWorkerTransportPump,
  transferableFrames,
} from "../runtime/native-runtime/browser-worker-transport.js";
import type {
  BrowserFollowerPortEvent,
  BrowserFollowerPortRequest,
  BrowserSharedWorkerConnectRequest,
  BrowserSharedWorkerConnectResponse,
  BrowserWorkerInitOptions,
} from "../runtime/native-runtime/browser-worker-protocol.js";
import { openConfig } from "../runtime/native-runtime/native-codec.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
};

type TabPeer = {
  tabId: string;
  port: MessagePort;
  pump: BrowserWorkerTransportPump | null;
  subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]> | null;
  onMessage: (event: MessageEvent<BrowserFollowerPortRequest>) => void;
  onMessageError: () => void;
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const peers = new Map<string, TabPeer>();
let fingerprint: string | null = null;
let initPromise: Promise<void> | null = null;
let initOptions: BrowserWorkerInitOptions | null = null;
let wasmModule: WasmModule | null = null;
let runtime: NativeRuntimeAdapter | null = null;
let pageStore: IndexedDbPageStore | null = null;
let disposeTelemetry: (() => void) | null = null;

workerGlobal.onconnect = (event) => {
  const port = event.ports[0];
  if (!port) return;
  const onBootstrapMessage = (messageEvent: MessageEvent<BrowserSharedWorkerConnectRequest>) => {
    const message = messageEvent.data;
    if (message?.type !== "connect-runtime") return;
    port.removeEventListener("message", onBootstrapMessage);
    void connectTab(port, message);
  };
  port.addEventListener("message", onBootstrapMessage);
  port.start();
};

async function connectTab(
  port: MessagePort,
  message: BrowserSharedWorkerConnectRequest,
): Promise<void> {
  try {
    if (fingerprint !== null && fingerprint !== message.fingerprint) {
      throw new Error("incompatible persistent browser configuration");
    }
    if (!initPromise) {
      fingerprint = message.fingerprint;
      initOptions = message.options;
      initPromise = initialize(message.options);
    }
    await initPromise;
    post(port, { type: "runtime-ready" });
    attachTab(message.tabId, port);
  } catch (error) {
    post(port, { type: "runtime-error", message: asError(error).message });
    port.close();
  }
}

async function initialize(options: BrowserWorkerInitOptions): Promise<void> {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = options.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
  wasmModule = await loadWasmModule(options.runtimeSources);
  disposeTelemetry = installWasmTelemetry({
    wasmModule,
    collectorUrl: options.telemetryCollectorUrl,
    appId: options.appId,
    runtimeThread: "worker",
  });
  pageStore = await IndexedDbPageStore.open(options.dbName);
  const db = await wasmModule.WasmDb.openBrowser(
    pageStore,
    encodeSchema(options.schema),
    openConfig(options.node, options.author, 1, false, options.initialSyncFlushEvery),
  );
  runtime = NativeRuntimeAdapter.fromDb(
    db as never,
    options.schema,
    options.node,
    options.author,
    1,
    false,
  );
  runtime.onAuthFailure((reason) => broadcast({ type: "auth-failure", reason }));
  if (options.serverUrl) runtime.connect(options.serverUrl, options.authJson);
}

function attachTab(tabId: string, port: MessagePort): void {
  closeTab(tabId);
  let peer!: TabPeer;
  const onMessage = (event: MessageEvent<BrowserFollowerPortRequest>) => {
    void handleTabMessage(peer, event.data);
  };
  const onMessageError = () => closeTab(tabId);
  peer = { tabId, port, pump: null, subscriber: null, onMessage, onMessageError };
  peers.set(tabId, peer);
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", onMessageError);
}

async function handleTabMessage(peer: TabPeer, message: BrowserFollowerPortRequest): Promise<void> {
  if (peers.get(peer.tabId) !== peer) return;
  if (message.type === "frames") {
    if (!peer.pump) {
      failPeer(peer, new Error("Browser tab sent frames before initializing session claims"));
      return;
    }
    peer.pump.receive(message.frames);
    return;
  }
  if (message.type === "close") {
    closeTab(peer.tabId);
    return;
  }

  try {
    const activeRuntime = requireRuntime();
    if (message.type === "init") {
      if (peer.pump || peer.subscriber) throw new Error("Browser tab is already initialized");
      peer.subscriber = activeRuntime.acceptPeer(message.sessionClaims);
      peer.pump = new BrowserWorkerTransportPump(
        activeRuntime,
        peer.subscriber,
        (frames) => {
          const copies = transferableFrames(frames);
          peer.port.postMessage(
            { type: "frames", frames: copies } satisfies BrowserFollowerPortEvent,
            copies.map((frame) => frame.buffer),
          );
        },
        (error) => failPeer(peer, asError(error)),
      );
      result(peer, message.id);
      return;
    }
    if (message.type === "update-auth") {
      if (!peer.subscriber) throw new Error("Browser tab is not initialized");
      await peer.subscriber.updateAuthenticatedClaims?.(message.sessionClaims);
      await activeRuntime.updateAuth(message.authJson);
      broadcast({ type: "auth-restored" });
      return;
    }
    if (message.type === "disconnect") {
      await activeRuntime.disconnect({ rejectWaiters: false });
      result(peer, message.id);
      return;
    }
    if (message.type === "reconnect") {
      const serverUrl = initOptions?.serverUrl;
      if (!serverUrl) throw new Error("Browser runtime reconnect requires a serverUrl");
      await peer.subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
      activeRuntime.connect(serverUrl, message.authJson);
      result(peer, message.id);
      return;
    }
    await activeRuntime.waitForUpstreamServerConnection();
    result(peer, message.id);
  } catch (error) {
    if ("id" in message) result(peer, message.id, asError(error));
    else failPeer(peer, asError(error));
  }
}

function result(peer: TabPeer, id: number, error?: Error): void {
  post(peer.port, { type: "result", id, ...(error ? { error: error.message } : {}) });
}

function failPeer(peer: TabPeer, error: Error): void {
  post(peer.port, { type: "error", message: error.message });
  closeTab(peer.tabId);
}

function closeTab(tabId: string): void {
  const peer = peers.get(tabId);
  if (!peer) return;
  peers.delete(tabId);
  peer.port.removeEventListener("message", peer.onMessage);
  peer.port.removeEventListener("messageerror", peer.onMessageError);
  peer.pump?.close();
  peer.subscriber?.free?.();
  peer.port.close();
}

function broadcast(event: BrowserFollowerPortEvent): void {
  for (const peer of peers.values()) post(peer.port, event);
}

function requireRuntime(): NativeRuntimeAdapter {
  if (!runtime) throw new Error("Shared browser runtime is closed");
  return runtime;
}

function post(
  port: MessagePort,
  event: BrowserFollowerPortEvent | BrowserSharedWorkerConnectResponse,
): void {
  port.postMessage(event);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

void disposeTelemetry;
