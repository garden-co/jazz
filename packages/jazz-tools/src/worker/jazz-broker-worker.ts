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
  BrowserInspectorControlEvent,
  BrowserInspectorControlRequest,
  BrowserSharedWorkerConnectRequest,
  BrowserSharedWorkerConnectResponse,
  BrowserWorkerInitOptions,
} from "../runtime/native-runtime/browser-worker-protocol.js";

// Worker failures cross a MessagePort boundary, so retain enough WASM frames to
// identify the Rust call site before serializing them for the owning tab.
(Error as ErrorConstructor & { stackTraceLimit?: number }).stackTraceLimit = 50;
import { openConfig } from "../runtime/native-runtime/native-codec.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
};

type TabPeer = {
  tabId: string;
  context: RuntimeContext;
  port: MessagePort;
  pump: BrowserWorkerTransportPump | null;
  subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]> | null;
  pendingFrames: Uint8Array[];
  onMessage: (event: MessageEvent<BrowserFollowerPortRequest>) => void;
  onMessageError: () => void;
};

type RuntimeContext = {
  key: string;
  fingerprint: string;
  options: BrowserWorkerInitOptions;
  peers: Map<string, TabPeer>;
  runtime: NativeRuntimeAdapter | null;
  initialize: Promise<void>;
  pageStore: IndexedDbPageStore | null;
  disposeTelemetry: (() => void) | null;
  resetBarrier: { id: number; pending: Set<string>; resolve: () => void } | null;
  storageInvalidated: boolean;
  serverUrl: string | null;
  serverAuthJson: string;
  serverConnectionStarted: boolean;
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const contexts = new Map<string, RuntimeContext>();
let wasmModulePromise: Promise<WasmModule> | null = null;
let contextInitializationTail: Promise<void> = Promise.resolve();
let nextResetId = 1;

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
    const key = runtimeKey(message.options);
    let context = contexts.get(key);
    if (context && context.fingerprint !== message.fingerprint) {
      throw new Error("incompatible persistent browser configuration");
    }
    if (!context) {
      context = createContext(key, message.fingerprint, message.options);
      contexts.set(key, context);
    }
    configureServer(context, message.options);
    await context.initialize;
    post(port, { type: "runtime-ready" });
    attachTab(context, message.tabId, port);
  } catch (error) {
    post(port, { type: "runtime-error", message: asError(error).message });
    port.close();
  }
}

function createContext(
  key: string,
  fingerprint: string,
  options: BrowserWorkerInitOptions,
): RuntimeContext {
  const context: RuntimeContext = {
    key,
    fingerprint,
    options,
    peers: new Map(),
    runtime: null,
    pageStore: null,
    disposeTelemetry: null,
    resetBarrier: null,
    storageInvalidated: false,
    serverUrl: options.serverUrl ?? null,
    serverAuthJson: options.authJson,
    serverConnectionStarted: false,
    initialize: Promise.resolve(),
  };
  const initializeContext = contextInitializationTail.then(() => initialize(context));
  contextInitializationTail = initializeContext.catch(() => undefined);
  context.initialize = initializeContext;
  return context;
}

async function initialize(context: RuntimeContext): Promise<void> {
  const { options } = context;
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = options.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
  wasmModulePromise ??= loadWasmModule(options.runtimeSources);
  const wasmModule = await wasmModulePromise;
  context.disposeTelemetry = installWasmTelemetry({
    wasmModule,
    collectorUrl: options.telemetryCollectorUrl,
    appId: options.appId,
    runtimeThread: "worker",
  });
  context.pageStore = await IndexedDbPageStore.open(options.dbName, () =>
    handleStorageInvalidation(context),
  );
  const db = await wasmModule.WasmDb.openBrowser(
    context.pageStore,
    encodeSchema(options.schema),
    openConfig(options.node, options.author, 1, false, options.initialSyncFlushEvery),
  );
  context.runtime = NativeRuntimeAdapter.fromDb(
    db as never,
    options.schema,
    options.node,
    options.author,
    1,
    false,
  );
  context.runtime.onAuthFailure((reason) => broadcast(context, { type: "auth-failure", reason }));
}

function configureServer(context: RuntimeContext, options: BrowserWorkerInitOptions): void {
  const requestedUrl = options.serverUrl;
  if (!requestedUrl) return;
  if (context.serverUrl && context.serverUrl !== requestedUrl) {
    throw new Error("incompatible persistent browser server configuration");
  }
  context.serverUrl = requestedUrl;
  context.serverAuthJson = options.authJson;
}

function ensureServerConnection(context: RuntimeContext): void {
  const requestedUrl = context.serverUrl;
  if (!requestedUrl) return;
  if (context.serverConnectionStarted) return;
  requireRuntime(context).connect(requestedUrl, context.serverAuthJson);
  context.serverConnectionStarted = true;
}

function attachTab(context: RuntimeContext, tabId: string, port: MessagePort): void {
  closeTab(context, tabId);
  let peer!: TabPeer;
  const onMessage = (event: MessageEvent<BrowserFollowerPortRequest>) => {
    void handleTabMessage(peer, event.data);
  };
  const onMessageError = () => closeTab(context, tabId);
  peer = {
    tabId,
    context,
    port,
    pump: null,
    subscriber: null,
    pendingFrames: [],
    onMessage,
    onMessageError,
  };
  context.peers.set(tabId, peer);
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", onMessageError);
}

async function handleTabMessage(peer: TabPeer, message: BrowserFollowerPortRequest): Promise<void> {
  if (peer.context.peers.get(peer.tabId) !== peer) return;
  if (message.type === "frames") {
    if (!peer.pump) {
      // The init handler may be awaiting an evaluator pass while MessagePort
      // delivery continues. Preserve ordering instead of treating those
      // already-staged follower frames as a protocol violation.
      peer.pendingFrames.push(...message.frames);
      return;
    }
    peer.pump.receive(message.frames);
    return;
  }
  if (message.type === "close") {
    closeTab(peer.context, peer.tabId);
    return;
  }
  if (message.type === "storage-reset-observed") {
    observeStorageReset(peer, message.resetId);
    return;
  }
  if (message.type === "open-inspector-control") {
    attachInspectorControl(peer.context.options.authSessionKey, message.port);
    result(peer, message.id);
    return;
  }

  try {
    const activeRuntime = requireRuntime(peer.context);
    if (message.type === "init") {
      if (peer.pump || peer.subscriber) throw new Error("Browser tab is already initialized");
      // Peer installation is synchronous once the shared evaluator is idle.
      // If hydration currently owns node state, join that evaluator pass first
      // so attachment cannot use a binding-time borrow during evaluation.
      await activeRuntime.progressPeerTransport();
      attachPeerTransport(peer, activeRuntime, message.sessionClaims);
      if (peer.pendingFrames.length > 0) {
        const pending = peer.pendingFrames.splice(0);
        peer.pump?.receive(pending);
      }
      ensureServerConnection(peer.context);
      result(peer, message.id);
      return;
    }
    if (message.type === "update-auth") {
      if (!peer.subscriber) throw new Error("Browser tab is not initialized");
      await peer.subscriber.updateAuthenticatedClaims?.(message.sessionClaims);
      peer.context.serverAuthJson = message.authJson;
      await activeRuntime.updateAuth(message.authJson);
      broadcast(peer.context, { type: "auth-restored" });
      return;
    }
    if (message.type === "disconnect") {
      await activeRuntime.disconnect({ rejectWaiters: false });
      peer.context.serverConnectionStarted = false;
      result(peer, message.id);
      return;
    }
    if (message.type === "delete-storage") {
      await deleteContextStorage(peer.context);
      await notifyStorageReset(peer.context);
      result(peer, message.id);
      setTimeout(() => closeContextPeers(peer.context), 0);
      return;
    }
    if (message.type === "reconnect") {
      const serverUrl = peer.context.serverUrl;
      if (!serverUrl) throw new Error("Browser runtime reconnect requires a serverUrl");
      await peer.subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
      peer.context.serverAuthJson = message.authJson;
      activeRuntime.connect(serverUrl, message.authJson);
      peer.context.serverConnectionStarted = true;
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

function attachInspectorControl(authSessionKey: string, port: MessagePort): void {
  const onMessage = (event: MessageEvent<BrowserInspectorControlRequest>) => {
    const message = event.data;
    if (message.type === "close") {
      dispose();
      return;
    }
    if (message.type === "list-contexts") {
      const available = [...contexts.values()]
        .filter(
          (context) =>
            context.options.authSessionKey === authSessionKey &&
            !context.storageInvalidated &&
            context.runtime !== null,
        )
        .map((context) => ({
          key: context.key,
          appId: context.options.appId,
          dbName: context.options.dbName,
          schema: context.options.schema,
        }));
      port.postMessage({
        type: "contexts",
        id: message.id,
        contexts: available,
      } satisfies BrowserInspectorControlEvent);
      return;
    }
    const context = contexts.get(message.contextKey);
    if (
      !context ||
      context.options.authSessionKey !== authSessionKey ||
      context.storageInvalidated ||
      !context.runtime
    ) {
      port.postMessage({
        type: "result",
        id: message.id,
        error: "Inspector context is no longer available",
      } satisfies BrowserInspectorControlEvent);
      return;
    }
    attachTab(context, message.tabId, message.port);
    port.postMessage({ type: "result", id: message.id } satisfies BrowserInspectorControlEvent);
  };
  const dispose = () => {
    port.removeEventListener("message", onMessage);
    port.removeEventListener("messageerror", dispose);
    port.close();
  };
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", dispose);
  port.start();
}

function result(peer: TabPeer, id: number, error?: Error): void {
  post(peer.port, { type: "result", id, ...(error ? { error: errorDetails(error) } : {}) });
}

function errorDetails(error: Error): string {
  const cause = error.cause;
  if (!(cause instanceof Error) || !cause.stack) return error.stack ?? error.message;
  return `${error.stack ?? error.message}\nCaused by: ${cause.stack}`;
}

function failPeer(peer: TabPeer, error: Error): void {
  post(peer.port, { type: "error", message: error.message });
  closeTab(peer.context, peer.tabId);
}

function closeTab(context: RuntimeContext, tabId: string): void {
  const peer = context.peers.get(tabId);
  if (!peer) return;
  context.peers.delete(tabId);
  acknowledgeReset(context, tabId);
  peer.port.removeEventListener("message", peer.onMessage);
  peer.port.removeEventListener("messageerror", peer.onMessageError);
  peer.pump?.close();
  peer.subscriber?.free?.();
  peer.port.close();
}

function broadcast(context: RuntimeContext, event: BrowserFollowerPortEvent): void {
  for (const peer of context.peers.values()) post(peer.port, event);
}

function requireRuntime(context: RuntimeContext): NativeRuntimeAdapter {
  if (!context.runtime) throw new Error("Shared browser runtime is closed");
  return context.runtime;
}

async function deleteContextStorage(context: RuntimeContext): Promise<void> {
  await context.runtime?.close();
  context.runtime = null;
  await context.pageStore?.clear();
  context.pageStore?.close();
  context.pageStore = null;
  context.disposeTelemetry?.();
  context.disposeTelemetry = null;
  contexts.delete(context.key);
}

function closeContextPeers(context: RuntimeContext): void {
  for (const tabId of context.peers.keys()) closeTab(context, tabId);
}

function handleStorageInvalidation(context: RuntimeContext): void {
  if (context.storageInvalidated) return;
  context.storageInvalidated = true;
  contexts.delete(context.key);
  context.pageStore = null;
  context.runtime?.discard();
  context.runtime = null;
  context.disposeTelemetry?.();
  context.disposeTelemetry = null;
  broadcast(context, { type: "storage-invalidated" });
  setTimeout(() => closeContextPeers(context), 0);
}

async function notifyStorageReset(context: RuntimeContext): Promise<void> {
  const pending = new Set(context.peers.keys());
  if (pending.size === 0) return;
  const id = nextResetId++;
  await new Promise<void>((resolve) => {
    context.resetBarrier = { id, pending, resolve };
    broadcast(context, { type: "storage-reset", resetId: id });
  });
}

function observeStorageReset(peer: TabPeer, resetId: number): void {
  if (peer.context.resetBarrier?.id !== resetId) return;
  acknowledgeReset(peer.context, peer.tabId);
}

function acknowledgeReset(context: RuntimeContext, tabId: string): void {
  const barrier = context.resetBarrier;
  if (!barrier || !barrier.pending.delete(tabId) || barrier.pending.size !== 0) return;
  context.resetBarrier = null;
  barrier.resolve();
}

function attachPeerTransport(
  peer: TabPeer,
  activeRuntime: NativeRuntimeAdapter,
  sessionClaims: Record<string, unknown>,
): void {
  peer.subscriber = activeRuntime.acceptPeer(sessionClaims);
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
}

function runtimeKey(options: BrowserWorkerInitOptions): string {
  return JSON.stringify([options.appId, options.dbName]);
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
