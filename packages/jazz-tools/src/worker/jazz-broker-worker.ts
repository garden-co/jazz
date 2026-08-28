import type { WasmDb } from "jazz-wasm";
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
import { deliverMutationErrorToAttachedPeers } from "./mutation-error-delivery.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

type SharedWorkerGlobal = typeof globalThis & {
  __JAZZ_WASM_LOG_LEVEL?: BrowserWorkerInitOptions["logLevel"];
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
  close(): void;
};

type TabPeer = {
  tabId: string;
  context: RuntimeContext;
  port: MessagePort;
  pump: BrowserWorkerTransportPump | null;
  subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]> | null;
  pendingFrames: Uint8Array[];
  flushedLocal: boolean;
  flushRequestId: number | null;
  flushPumpComplete: boolean;
  flushObserved: boolean;
  transportWaitAbort: AbortController;
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
  closing: Promise<void> | null;
  idleReleaseTimer: ReturnType<typeof setTimeout> | null;
  pageStore: IndexedDbPageStore | null;
  disposeTelemetry: (() => void) | null;
  disposeAuxiliaryTrace: (() => void) | null;
  resetBarrier: { id: number; pending: Set<string>; resolve: () => void } | null;
  storageInvalidated: boolean;
  intentionalStorageReset: boolean;
  serverUrl: string | null;
  serverAuthJson: string;
  serverConnectionStarted: boolean;
  explicitlyDisconnected: boolean;
  transportTransition: Promise<void>;
  transportStateEpoch: number;
  transportStateWaiters: Set<() => void>;
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const workerRealmId = crypto.randomUUID();
const contexts = new Map<string, RuntimeContext>();
const inspectorControlPorts = new Set<MessagePort>();
let wasmModulePromise: Promise<WasmModule> | null = null;
let wasmModuleSource: string | null = null;
let contextInitializationTail: Promise<void> = Promise.resolve();
let nextResetId = 1;

workerGlobal.onconnect = (event) => {
  const port = event.ports[0];
  if (!port) return;
  const onBootstrapMessage = (messageEvent: MessageEvent<BrowserSharedWorkerConnectRequest>) => {
    const message = messageEvent.data;
    if (message?.type !== "connect-runtime") return;
    port.removeEventListener("message", onBootstrapMessage);
    post(port, { type: "worker-alive" });
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
    if (context?.idleReleaseTimer) {
      clearTimeout(context.idleReleaseTimer);
      context.idleReleaseTimer = null;
    }
    if (context?.closing) {
      await context.closing;
      context = contexts.get(key);
    }
    if (context && context.fingerprint !== message.fingerprint) {
      throw new Error("incompatible persistent browser configuration");
    }
    if (!context) {
      context = createContext(key, message.fingerprint, message.options);
      contexts.set(key, context);
    }
    await context.initialize;
    await enqueueTransportTransition(context, () => configureServer(context, message.options));
    attachTab(context, message.tabId, port);
    post(port, { type: "runtime-ready" });
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
    disposeAuxiliaryTrace: null,
    resetBarrier: null,
    storageInvalidated: false,
    intentionalStorageReset: false,
    serverUrl: options.serverUrl ?? null,
    serverAuthJson: options.authJson,
    serverConnectionStarted: false,
    explicitlyDisconnected: false,
    transportTransition: Promise.resolve(),
    transportStateEpoch: 0,
    transportStateWaiters: new Set(),
    initialize: Promise.resolve(),
    closing: null,
    idleReleaseTimer: null,
  };
  const initializeContext = contextInitializationTail.then(() => initialize(context));
  contextInitializationTail = initializeContext.catch(() => undefined);
  context.initialize = initializeContext;
  return context;
}

async function initialize(context: RuntimeContext): Promise<void> {
  let unownedDb: WasmDb | null = null;
  try {
    const { options } = context;
    workerGlobal.__JAZZ_WASM_LOG_LEVEL = options.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
    const wasmModule = await loadWorkerWasmModule(options.runtimeSources);
    context.disposeTelemetry = installWasmTelemetry({
      wasmModule,
      collectorUrl: options.telemetryCollectorUrl,
      appId: options.appId,
      runtimeThread: "worker",
    });
    context.pageStore = await IndexedDbPageStore.open(options.dbName, () =>
      handleStorageInvalidation(context),
    );
    const schema = encodeSchema(options.schema);
    const proof = options.selfSignedClientProof;
    const config = openConfig(
      options.node,
      options.author,
      1,
      false,
      options.initialSyncFlushEvery,
      proof,
    );
    if (proof && typeof wasmModule.WasmDb.openBrowserWithSelfSignedProof !== "function") {
      throw new Error(
        "WASM runtime does not support self-signed client opens; rebuild the matching Jazz WASM artifact",
      );
    }
    unownedDb = proof
      ? await wasmModule.WasmDb.openBrowserWithSelfSignedProof(
          context.pageStore,
          schema,
          config,
          proof.token,
          proof.appId,
          proof.claimedAuthor,
        )
      : await wasmModule.WasmDb.openBrowser(context.pageStore, schema, config);
    const runtime = NativeRuntimeAdapter.fromDb(
      unownedDb as never,
      options.schema,
      options.node,
      options.author,
      1,
      false,
      { selfSignedClientProof: proof },
    );
    if (!unownedDb?.setRelayAuthoritySessionOwner) {
      throw new Error(
        "Browser worker artifact does not support relay authority-session bindings; rebuild the matching Jazz WASM artifact",
      );
    }
    unownedDb.setRelayAuthoritySessionOwner();
    context.runtime = runtime;
    unownedDb = null;
    context.runtime.onAuthFailure((reason) => broadcast(context, { type: "auth-failure", reason }));
    context.runtime.onMutationError((event) => {
      // A mutation error is a notification for foreground runtimes that are
      // attached now. Durable reconciliation belongs to the worker's database;
      // persisting this event would instead surface an old application's toast
      // to an unrelated future tab.
      deliverMutationErrorToAttachedPeers(context.peers.values(), event, (peer, received) =>
        post(peer.port, { type: "mutation-error", event: received }),
      );
    });
    context.runtime.onServerTransportError((error) => {
      // Transport failures are foreground events, not authority fates or
      // durable notifications. Only peers that have completed admission own a
      // tab runtime capable of rejecting an active remote wait.
      for (const peer of context.peers.values()) {
        if (!peer.subscriber || !peer.pump) continue;
        post(peer.port, { type: "transport-error", message: error.message });
      }
    });
    if (options.logLevel === "trace") {
      context.disposeAuxiliaryTrace = context.runtime.onAuxiliaryTrace((entries) => {
        broadcast(context, {
          type: "relay-trace",
          entries: entries.map((entry) => ({ ...entry, hop: "worker-server" })),
        });
      });
    }
  } catch (error) {
    try {
      await unownedDb?.close();
    } catch {
      // The initialisation error is the actionable failure.
    }
    try {
      cleanupFailedContext(context);
    } catch {
      // Cleanup must not replace the initialisation error reported to the tab.
    }
    throw error;
  }
}

async function loadWorkerWasmModule(
  runtimeSources: BrowserWorkerInitOptions["runtimeSources"],
): Promise<WasmModule> {
  const source = workerWasmSource(runtimeSources);
  if (wasmModulePromise && (!source || wasmModuleSource !== source)) {
    throw new Error(
      "incompatible WASM asset source for this SharedWorker; start a worker scoped to the new asset URL",
    );
  }
  const load = wasmModulePromise ?? loadWasmModule(runtimeSources);
  wasmModulePromise = load;
  wasmModuleSource = source;
  try {
    return await load;
  } catch (error) {
    if (wasmModulePromise === load) {
      wasmModulePromise = null;
      wasmModuleSource = null;
    }
    throw error;
  }
}

function workerWasmSource(
  runtimeSources: BrowserWorkerInitOptions["runtimeSources"],
): string | null {
  // The page assigns an opaque identity before structured-cloning an in-memory
  // source into this worker. A raw worker caller without that identity is
  // deliberately unshareable: treating every supplied byte array/module as
  // the same source would silently reuse the first wasm-bindgen realm.
  if (runtimeSources?.wasmModule) {
    return runtimeSources.workerWasmAssetIdentity
      ? `module:${runtimeSources.workerWasmAssetIdentity}`
      : null;
  }
  if (runtimeSources?.wasmSource) {
    return runtimeSources.workerWasmAssetIdentity
      ? `source:${runtimeSources.workerWasmAssetIdentity}`
      : null;
  }
  return runtimeSources?.wasmUrl ?? "worker-local";
}

function cleanupFailedContext(context: RuntimeContext): void {
  const runtime = context.runtime;
  const pageStore = context.pageStore;
  const disposeTelemetry = context.disposeTelemetry;
  const disposeAuxiliaryTrace = context.disposeAuxiliaryTrace;
  context.runtime = null;
  context.pageStore = null;
  context.disposeTelemetry = null;
  context.disposeAuxiliaryTrace = null;
  if (contexts.get(context.key) === context) contexts.delete(context.key);
  try {
    runtime?.discard();
  } finally {
    try {
      pageStore?.close();
    } finally {
      try {
        disposeTelemetry?.();
      } finally {
        disposeAuxiliaryTrace?.();
      }
    }
  }
}

async function configureServer(
  context: RuntimeContext,
  options: BrowserWorkerInitOptions,
): Promise<void> {
  const requestedUrl = options.serverUrl ?? null;
  if (context.serverUrl === requestedUrl) {
    context.serverAuthJson = options.authJson;
    if (context.serverConnectionStarted) await requireRuntime(context).updateAuth(options.authJson);
    return;
  }
  if (context.peers.size > 0) {
    throw new Error("incompatible persistent browser server configuration");
  }
  if (context.serverConnectionStarted) {
    await requireRuntime(context).disconnect({ rejectWaiters: false });
    context.serverConnectionStarted = false;
  }
  context.serverUrl = requestedUrl;
  context.serverAuthJson = options.authJson;
}

function ensureServerConnection(context: RuntimeContext): void {
  const requestedUrl = context.serverUrl;
  if (!requestedUrl || context.explicitlyDisconnected) return;
  if (context.serverConnectionStarted) return;
  requireRuntime(context).connect(requestedUrl, context.serverAuthJson);
  context.serverConnectionStarted = true;
}

/**
 * The durable worker has one upstream transport, while every MessagePort is
 * serviced independently. Serialize mutations and observations of that shared
 * transport so an older disconnect cannot complete after a newer reconnect.
 */
function enqueueTransportTransition(
  context: RuntimeContext,
  transition: () => void | Promise<void>,
): Promise<void> {
  const queued = context.transportTransition.then(transition, transition);
  context.transportTransition = queued.catch(() => undefined);
  return queued;
}

function publishExplicitOffline(context: RuntimeContext): void {
  context.transportStateEpoch += 1;
  broadcast(context, {
    type: "transport-state",
    explicitlyDisconnected: context.explicitlyDisconnected,
  });
  const waiters = [...context.transportStateWaiters];
  context.transportStateWaiters.clear();
  for (const wake of waiters) wake();
}

function waitForTransportStateChange(
  context: RuntimeContext,
  observedEpoch: number,
  signal?: AbortSignal,
): Promise<void> {
  if (context.transportStateEpoch !== observedEpoch || signal?.aborted) return Promise.resolve();
  return new Promise<void>((resolve) => {
    const finish = () => {
      context.transportStateWaiters.delete(finish);
      signal?.removeEventListener("abort", finish);
      resolve();
    };
    signal?.addEventListener("abort", finish, { once: true });
    context.transportStateWaiters.add(finish);
  });
}

async function waitForServerConnection(
  context: RuntimeContext,
  runtime: NativeRuntimeAdapter,
  signal?: AbortSignal,
): Promise<void> {
  for (;;) {
    if (signal?.aborted) return;
    await context.transportTransition;
    if (signal?.aborted) return;
    if (context.explicitlyDisconnected) {
      const epoch = context.transportStateEpoch;
      await waitForTransportStateChange(context, epoch, signal);
      continue;
    }
    await runtime.waitForUpstreamServerConnection();
    // A disconnect can be queued while carrier negotiation is pending. Do not
    // tell a caller that remote readiness succeeded until it observes the
    // resulting namespace state.
    if (!context.explicitlyDisconnected) return;
  }
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
    flushedLocal: false,
    flushRequestId: null,
    flushPumpComplete: false,
    flushObserved: false,
    transportWaitAbort: new AbortController(),
    onMessage,
    onMessageError,
  };
  context.peers.set(tabId, peer);
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", onMessageError);
  // SharedWorker connection ports are started by `onconnect`, but inspector
  // peers arrive as freshly transferred MessageChannel ports. Starting is
  // idempotent and required before addEventListener-based delivery can begin.
  port.start();
}

async function handleTabMessage(peer: TabPeer, message: BrowserFollowerPortRequest): Promise<void> {
  if (peer.context.peers.get(peer.tabId) !== peer) return;
  if (message.type === "frames") {
    peer.flushedLocal = false;
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
    const releaseWhenIdle = peer.context.peers.size === 1 && message.releaseContext;
    if (message.id !== undefined) result(peer, message.id);
    // The requester owns graceful port closure. Closing this endpoint in the
    // same task as the acknowledgement can discard that queued message in
    // WebKit, leaving shutdown pending forever. Detach runtime ownership now;
    // the client closes both ends after it observes the result.
    closeTab(peer.context, peer.tabId, false);
    if (releaseWhenIdle) scheduleIdleContextRelease(peer.context);
    return;
  }
  if (message.type === "storage-reset-observed") {
    observeStorageReset(peer, message.resetId);
    return;
  }
  if (message.type === "flush-local-observed") {
    peer.flushObserved = true;
    completeLocalFlush(peer);
    return;
  }
  if (message.type === "open-inspector-control") {
    attachInspectorControl(peer.context.options.authSessionKey, message.port);
    result(peer, message.id);
    return;
  }
  if (message.type === "prepare-storage-reset") {
    peer.context.intentionalStorageReset = true;
    result(peer, message.id);
    return;
  }
  if (message.type === "abort-storage-reset") {
    peer.context.intentionalStorageReset = false;
    result(peer, message.id);
    return;
  }
  if (message.type === "finish-storage-reset") {
    await finalizeContextStorageReset(peer.context);
    void notifyStorageReset(peer.context);
    result(peer, message.id);
    return;
  }

  try {
    const activeRuntime = requireRuntime(peer.context);
    if (message.type === "init") {
      if (peer.pump || peer.subscriber) throw new Error("Browser tab is already initialized");
      // Peer admission mutates the connection registry. A running evaluator
      // may hold that registry across storage suspension, so install only at
      // the owner-wide evaluator boundary. Storage progress is independent of
      // this new peer and therefore continues while admission waits.
      const subscriber = await activeRuntime.acceptPeerWhenIdle(message.sessionClaims);
      const pump = attachPeerTransport(peer, activeRuntime, subscriber);
      if (peer.pendingFrames.length > 0) {
        const pending = peer.pendingFrames.splice(0);
        pump.receive(pending);
      }
      await enqueueTransportTransition(peer.context, () => {
        if (peer.context.explicitlyDisconnected) {
          post(peer.port, { type: "transport-state", explicitlyDisconnected: true });
        } else {
          ensureServerConnection(peer.context);
        }
      });
      result(peer, message.id);
      return;
    }
    if (message.type === "update-auth") {
      if (!peer.subscriber) throw new Error("Browser tab is not initialized");
      await enqueueTransportTransition(peer.context, async () => {
        await peer.subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
        peer.context.serverAuthJson = message.authJson;
        if (!peer.context.explicitlyDisconnected) {
          await activeRuntime.updateAuth(message.authJson);
        }
      });
      broadcast(peer.context, { type: "auth-restored" });
      return;
    }
    if (message.type === "disconnect") {
      await enqueueTransportTransition(peer.context, async () => {
        const wasExplicitlyDisconnected = peer.context.explicitlyDisconnected;
        peer.context.explicitlyDisconnected = true;
        peer.context.serverConnectionStarted = false;
        try {
          await activeRuntime.disconnect({ rejectWaiters: false });
        } catch (error) {
          // Match the public Db contract: a failed explicit disconnect must
          // not silently change RemoteIfPossible behavior for any tab. The
          // adapter may already have detached the old carrier before reporting
          // a retirement error, so restore normal connection ownership.
          peer.context.explicitlyDisconnected = wasExplicitlyDisconnected;
          if (!wasExplicitlyDisconnected) ensureServerConnection(peer.context);
          throw error;
        }
        publishExplicitOffline(peer.context);
      });
      result(peer, message.id);
      return;
    }
    if (message.type === "flush-local") {
      if (peer.flushRequestId !== null) {
        result(peer, message.id, new Error("Browser tab already has a local flush in progress"));
        return;
      }
      peer.flushRequestId = message.id;
      peer.flushPumpComplete = false;
      peer.flushObserved = false;
      peer.pump?.drainOutboundFrames();
      // The page reports `flush-local-observed` only after every pending local
      // settlement handle has resolved. Those handles are acknowledged by the
      // durable worker after persistence, so evaluator quiescence is neither
      // necessary nor relevant to this durability barrier.
      peer.flushPumpComplete = true;
      completeLocalFlush(peer);
      return;
    }
    if (message.type === "reconnect") {
      const serverUrl = peer.context.serverUrl;
      if (!serverUrl) throw new Error("Browser runtime reconnect requires a serverUrl");
      await enqueueTransportTransition(peer.context, async () => {
        await peer.subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
        peer.context.serverAuthJson = message.authJson;
        activeRuntime.connect(serverUrl, message.authJson);
        peer.context.explicitlyDisconnected = false;
        peer.context.serverConnectionStarted = true;
        publishExplicitOffline(peer.context);
      });
      result(peer, message.id);
      return;
    }
    await waitForServerConnection(peer.context, activeRuntime, peer.transportWaitAbort.signal);
    if (peer.transportWaitAbort.signal.aborted) return;
    result(peer, message.id);
  } catch (error) {
    if ("id" in message) result(peer, message.id, asError(error));
    else failPeer(peer, asError(error));
  }
}

function attachInspectorControl(authSessionKey: string, port: MessagePort): void {
  inspectorControlPorts.add(port);
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
          workerRealmId,
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
    if (message.type === "terminate-worker") {
      if (contexts.size > 0) {
        port.postMessage({
          type: "result",
          id: message.id,
          error: "Worker still has live runtime contexts",
        } satisfies BrowserInspectorControlEvent);
        return;
      }
      port.postMessage({ type: "result", id: message.id } satisfies BrowserInspectorControlEvent);
      // Termination happens in a later task so the acknowledgement crosses the
      // MessagePort before this realm disappears. A subsequent connection's
      // bootstrap retry advances to a distinct SharedWorker generation.
      setTimeout(() => workerGlobal.close(), 0);
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
    inspectorControlPorts.delete(port);
    port.close();
    maybeCloseWorker();
  };
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", dispose);
  port.start();
}

function result(peer: TabPeer, id: number, error?: Error): void {
  if (peer.context.peers.get(peer.tabId) !== peer) return;
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

function closeTab(context: RuntimeContext, tabId: string, closePort = true): void {
  const peer = context.peers.get(tabId);
  if (!peer) return;
  context.peers.delete(tabId);
  peer.transportWaitAbort.abort();
  acknowledgeReset(context, tabId);
  peer.port.removeEventListener("message", peer.onMessage);
  peer.port.removeEventListener("messageerror", peer.onMessageError);
  detachPeerRuntime(peer);
  if (closePort) peer.port.close();
}

function detachPeerRuntime(peer: TabPeer): void {
  peer.pump?.close();
  peer.pump = null;
  // The pump exclusively owns logical closure of the WASM transport receiver
  // and defers it until an in-flight evaluator pass has unwound. Do not call
  // `free()` here: Rust futures may retain the wasm-bindgen wrapper beyond the
  // visible peer lifetime, so its registered finalizer owns physical release.
  peer.subscriber = null;
  peer.pendingFrames.length = 0;
}

function broadcast(context: RuntimeContext, event: BrowserFollowerPortEvent): void {
  for (const peer of context.peers.values()) post(peer.port, event);
}

function requireRuntime(context: RuntimeContext): NativeRuntimeAdapter {
  if (!context.runtime) throw new Error("Shared browser runtime is closed");
  return context.runtime;
}

async function finalizeContextStorageReset(context: RuntimeContext): Promise<void> {
  context.intentionalStorageReset = false;
  for (const peer of context.peers.values()) {
    // The persistence epoch is already gone. Do not call into transport or
    // subscriber wrappers whose WASM receiver may still be unwinding the IDB
    // versionchange; abandon them with the discarded runtime instead.
    peer.pump = null;
    peer.subscriber = null;
    peer.pendingFrames.length = 0;
  }
  context.runtime?.discard();
  context.runtime = null;
  context.pageStore?.close();
  context.pageStore = null;
  context.disposeTelemetry?.();
  context.disposeTelemetry = null;
  context.disposeAuxiliaryTrace?.();
  context.disposeAuxiliaryTrace = null;
  contexts.delete(context.key);
}

async function releaseIdleContext(context: RuntimeContext): Promise<void> {
  if (!context.closing) {
    context.closing = (async () => {
      for (const peer of context.peers.values()) {
        peer.pump?.close();
        peer.pump = null;
        peer.subscriber = null;
        peer.pendingFrames.length = 0;
      }
      // The last peer's flush barrier already drained evaluator persistence.
      // Do not retain a graceful close future after every page has gone: a
      // suspended cold/query lifecycle cannot add durability at this point.
      context.runtime?.discard();
      context.runtime = null;
      context.pageStore?.close();
      context.pageStore = null;
      context.disposeTelemetry?.();
      context.disposeTelemetry = null;
      context.disposeAuxiliaryTrace?.();
      context.disposeAuxiliaryTrace = null;
      if (contexts.get(context.key) === context) contexts.delete(context.key);
    })();
  }
  await context.closing;
}

function scheduleIdleContextRelease(context: RuntimeContext): void {
  if (context.idleReleaseTimer) clearTimeout(context.idleReleaseTimer);
  context.idleReleaseTimer = setTimeout(() => {
    context.idleReleaseTimer = null;
    if (context.peers.size !== 0) return;
    void releaseIdleContext(context).then(() => {
      maybeCloseWorker();
    });
  }, 50);
}

function maybeCloseWorker(): void {
  if (contexts.size === 0 && inspectorControlPorts.size === 0) workerGlobal.close();
}

function closeContextPeers(context: RuntimeContext): void {
  for (const tabId of context.peers.keys()) closeTab(context, tabId);
}

function handleStorageInvalidation(context: RuntimeContext): void {
  if (context.intentionalStorageReset) {
    context.runtime?.discard();
    context.runtime = null;
    context.pageStore = null;
    return;
  }
  if (context.storageInvalidated) return;
  context.storageInvalidated = true;
  contexts.delete(context.key);
  context.pageStore = null;
  context.runtime?.discard();
  context.runtime = null;
  context.disposeTelemetry?.();
  context.disposeTelemetry = null;
  context.disposeAuxiliaryTrace?.();
  context.disposeAuxiliaryTrace = null;
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
  subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]>,
): BrowserWorkerTransportPump {
  peer.subscriber = subscriber;
  peer.pump = new BrowserWorkerTransportPump(
    activeRuntime,
    subscriber,
    (frames) => {
      const copies = transferableFrames(frames);
      peer.port.postMessage(
        { type: "frames", frames: copies } satisfies BrowserFollowerPortEvent,
        copies.map((frame) => frame.buffer),
      );
    },
    (error) => failPeer(peer, asError(error)),
    peer.context.options.logLevel === "trace"
      ? (entries) => {
          post(peer.port, {
            type: "relay-trace",
            entries: entries.map((entry) => ({ ...entry, hop: "worker-tab" })),
          });
        }
      : undefined,
  );
  return peer.pump;
}

function completeLocalFlush(peer: TabPeer): void {
  const requestId = peer.flushRequestId;
  if (requestId === null || !peer.flushPumpComplete || !peer.flushObserved) return;
  peer.flushRequestId = null;
  peer.flushedLocal = true;
  result(peer, requestId);
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
