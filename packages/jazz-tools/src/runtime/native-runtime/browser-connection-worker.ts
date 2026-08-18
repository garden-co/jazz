import { loadWasmModule, type WasmModule } from "../client.js";
import { installWasmTelemetry } from "../sync-telemetry.js";
import { tryAcquireWebLock, type LeaderLockLease } from "../leader-lock.js";
import { BrowserWorkerTransportPump, transferableFrames } from "./browser-worker-transport.js";
import type {
  BrowserWorkerEvent,
  BrowserFollowerPortEvent,
  BrowserFollowerPortRequest,
  BrowserWorkerInitOptions,
  BrowserWorkerMessage,
} from "./browser-worker-protocol.js";
import { openConfig } from "./native-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { encodeSchema } from "./schema-codec.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

const workerScope = self as unknown as {
  onmessage: ((event: MessageEvent<BrowserWorkerMessage>) => void) | null;
  postMessage(message: BrowserWorkerEvent, transfer?: Transferable[]): void;
  close(): void;
};

let wasmModule: WasmModule | null = null;
let runtime: NativeRuntimeAdapter | null = null;
let relayPump: BrowserWorkerTransportPump | null = null;
let subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]> | null = null;
let initOptions: BrowserWorkerInitOptions | null = null;
let initPromise: Promise<void> | null = null;
let disposeTelemetry: (() => void) | null = null;
let workerLockLease: LeaderLockLease | null = null;
let activeLeadershipId: number | null = null;
let suppressOutboundFrames = false;
let authConnectionEpoch = 0;
let simulatePendingAuthConfirmation = false;
const pendingAuthConfirmationCancellations = new Set<() => void>();

type FollowerPeer = {
  followerTabId: string;
  leadershipId: number;
  port: MessagePort;
  pump: BrowserWorkerTransportPump | null;
  subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]> | null;
  onMessage: (event: MessageEvent<BrowserFollowerPortRequest>) => void;
  onMessageError: () => void;
};

const followerPeers = new Map<string, FollowerPeer>();

function broadcastAuthRestored(): void {
  postEvent({ type: "auth-restored" });
  for (const peer of followerPeers.values()) {
    peer.port.postMessage({ type: "auth-restored" } satisfies BrowserFollowerPortEvent);
  }
}

workerScope.onmessage = (event) => {
  const message = event.data;
  if (message.type === "init") {
    if (initPromise) {
      postResult(message.id, new Error("Browser persistence worker is already initialized"));
      return;
    }
    initPromise = initialize(message);
    void initPromise.then(
      () => postResult(message.id),
      (error: unknown) => postResult(message.id, asError(error)),
    );
    return;
  }
  void handleAfterInitialization(message).catch((error: unknown) => {
    if ("id" in message) postResult(message.id, asError(error));
    else postEvent({ type: "error", message: asError(error).message });
  });
};

async function initialize(options: BrowserWorkerInitOptions): Promise<void> {
  initOptions = options;
  activeLeadershipId = options.leadershipId;
  workerLockLease = await tryAcquireWebLock(options.workerLockName, {
    onLost: (reason) => {
      const message = asError(reason).message;
      postEvent({ type: "error", message: `Browser persistence worker lock was lost: ${message}` });
      void closeRuntime();
    },
  });
  if (!workerLockLease) {
    throw new Error(`Unable to acquire ${options.workerLockName}`);
  }

  try {
    (globalThis as any).__JAZZ_WASM_LOG_LEVEL = options.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
    wasmModule = await loadWasmModule(options.runtimeSources);
    disposeTelemetry = installWasmTelemetry({
      wasmModule,
      collectorUrl: options.telemetryCollectorUrl,
      appId: options.appId,
      runtimeThread: "worker",
    });
    const db = await wasmModule.WasmDb.openBrowser(
      options.dbName,
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
    runtime.onAuthFailure((reason) => {
      postEvent({ type: "auth-failure", reason });
      for (const peer of followerPeers.values()) {
        peer.port.postMessage({ type: "auth-failure", reason } satisfies BrowserFollowerPortEvent);
      }
    });
    subscriber = runtime.acceptPeer(options.sessionClaims);
    relayPump = new BrowserWorkerTransportPump(runtime, subscriber, postFrames);
    if (options.serverUrl) runtime.connect(options.serverUrl, options.authJson);
  } catch (error) {
    workerLockLease.release();
    workerLockLease = null;
    activeLeadershipId = null;
    throw error;
  }
}

async function handleAfterInitialization(message: Exclude<BrowserWorkerMessage, { type: "init" }>) {
  if (!initPromise) throw new Error("Browser persistence worker has not been initialized");
  await initPromise;
  const activeRuntime = requireRuntime();
  if (message.type === "frames") {
    relayPump?.receive(message.frames);
    return;
  }
  switch (message.type) {
    case "wait-server":
      await activeRuntime.waitForUpstreamServerConnection();
      break;
    case "update-auth": {
      const connectionEpoch = authConnectionEpoch;
      subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
      await activeRuntime.updateAuth(message.authJson);
      await waitForAuthConfirmation(activeRuntime, connectionEpoch);
      broadcastAuthRestored();
      break;
    }
    case "disconnect":
      authConnectionEpoch += 1;
      for (const cancel of pendingAuthConfirmationCancellations) cancel();
      await activeRuntime.disconnect({ rejectWaiters: false });
      break;
    case "reconnect": {
      const options = initOptions;
      const serverUrl = options?.serverUrl;
      if (!serverUrl) throw new Error("Browser worker reconnect requires a serverUrl");
      options.authJson = message.authJson;
      options.sessionClaims = message.sessionClaims;
      subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
      activeRuntime.connect(serverUrl, message.authJson);
      break;
    }
    case "attach-follower":
      attachFollower(activeRuntime, message.followerTabId, message.leadershipId, message.port);
      break;
    case "detach-follower":
      if (message.leadershipId === activeLeadershipId) {
        closeFollower(message.followerTabId, false);
      }
      break;
    case "delete-storage": {
      const dbName = initOptions?.dbName;
      if (!dbName || !wasmModule) throw new Error("Browser storage namespace is unavailable");
      await closeRuntime();
      await wasmModule.WasmDb.destroyBrowserStorage(dbName);
      break;
    }
    case "simulate-crash":
      // Test-only: let already-sent peer frames reach the runtime and persist,
      // while dropping the returning BatchFate. This reproduces a worker dying
      // after WAL durability but before the main-thread wait is acknowledged.
      suppressOutboundFrames = true;
      await new Promise((resolve) => setTimeout(resolve, 25));
      await closeRuntime(false);
      break;
    case "simulate-pending-auth-confirmation":
      simulatePendingAuthConfirmation = true;
      break;
    case "close":
      await closeRuntime();
      break;
  }
  postResult(message.id);
  if (
    message.type === "close" ||
    message.type === "delete-storage" ||
    message.type === "simulate-crash"
  ) {
    queueMicrotask(() => workerScope.close());
  }
}

async function waitForAuthConfirmation(
  activeRuntime: NativeRuntimeAdapter,
  connectionEpoch: number,
): Promise<void> {
  if (connectionEpoch !== authConnectionEpoch) {
    throw new Error("Auth confirmation cancelled by disconnect");
  }
  let cancel!: () => void;
  const cancelled = new Promise<never>((_, reject) => {
    cancel = () => reject(new Error("Auth confirmation cancelled by disconnect"));
  });
  pendingAuthConfirmationCancellations.add(cancel);
  try {
    if (connectionEpoch !== authConnectionEpoch) cancel();
    const serverConfirmation = simulatePendingAuthConfirmation
      ? new Promise<never>(() => undefined)
      : activeRuntime.waitForUpstreamServerConnection();
    simulatePendingAuthConfirmation = false;
    await Promise.race([serverConfirmation, cancelled]);
  } finally {
    pendingAuthConfirmationCancellations.delete(cancel);
  }
}

async function closeRuntime(graceful = true): Promise<void> {
  for (const followerTabId of [...followerPeers.keys()]) {
    closeFollower(followerTabId, false);
  }
  relayPump?.close();
  relayPump = null;
  subscriber?.free?.();
  subscriber = null;
  if (graceful) await runtime?.close();
  else await runtime?.simulateCrash();
  runtime = null;
  disposeTelemetry?.();
  disposeTelemetry = null;
  activeLeadershipId = null;
  workerLockLease?.release();
  workerLockLease = null;
}

function attachFollower(
  activeRuntime: NativeRuntimeAdapter,
  followerTabId: string,
  leadershipId: number,
  port: MessagePort,
): void {
  if (leadershipId !== activeLeadershipId) {
    port.close();
    throw new Error(`Refusing follower port for stale leadership ${leadershipId}`);
  }

  closeFollower(followerTabId, false);
  let peer!: FollowerPeer;
  const onMessage = (event: MessageEvent<BrowserFollowerPortRequest>) => {
    void handleFollowerMessage(peer, event.data);
  };
  const onMessageError = () => closeFollower(followerTabId, true);
  peer = {
    followerTabId,
    leadershipId,
    port,
    pump: null,
    subscriber: null,
    onMessage,
    onMessageError,
  };
  followerPeers.set(followerTabId, peer);
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", onMessageError);
  port.start();
}

async function handleFollowerMessage(
  peer: FollowerPeer,
  message: BrowserFollowerPortRequest,
): Promise<void> {
  if (followerPeers.get(peer.followerTabId) !== peer) return;
  if (message.type === "frames") {
    if (!peer.pump) {
      peer.port.postMessage({
        type: "error",
        message: "Browser follower sent frames before initializing its session claims",
      } satisfies BrowserFollowerPortEvent);
      closeFollower(peer.followerTabId, true);
      return;
    }
    peer.pump.receive(message.frames);
    return;
  }
  if (message.type === "close") {
    closeFollower(peer.followerTabId, true);
    return;
  }

  const activeRuntime = requireRuntime();
  try {
    if (message.type === "init") {
      if (peer.pump || peer.subscriber) {
        throw new Error("Browser follower port is already initialized");
      }
      const followerSubscriber = activeRuntime.acceptPeer(message.sessionClaims);
      peer.subscriber = followerSubscriber;
      peer.pump = new BrowserWorkerTransportPump(activeRuntime, followerSubscriber, (frames) => {
        if (suppressOutboundFrames) return;
        const copies = transferableFrames(frames);
        peer.port.postMessage(
          { type: "frames", frames: copies } satisfies BrowserFollowerPortEvent,
          copies.map((frame) => frame.buffer),
        );
      });
      peer.port.postMessage({ type: "result", id: message.id } satisfies BrowserFollowerPortEvent);
      return;
    }
    if (message.type === "update-auth") {
      if (!peer.subscriber) {
        throw new Error("Browser follower port is not initialized");
      }
      const connectionEpoch = authConnectionEpoch;
      peer.subscriber.updateAuthenticatedClaims?.(message.sessionClaims);
      await activeRuntime.updateAuth(message.authJson);
      await waitForAuthConfirmation(activeRuntime, connectionEpoch);
      broadcastAuthRestored();
      return;
    }
    await activeRuntime.waitForUpstreamServerConnection();
    peer.port.postMessage({ type: "result", id: message.id } satisfies BrowserFollowerPortEvent);
  } catch (error) {
    if (message.type === "init" || message.type === "wait-server") {
      peer.port.postMessage({
        type: "result",
        id: message.id,
        error: asError(error).message,
      } satisfies BrowserFollowerPortEvent);
      return;
    }
    peer.port.postMessage({
      type: "error",
      message: asError(error).message,
    } satisfies BrowserFollowerPortEvent);
  }
}

function closeFollower(followerTabId: string, notify: boolean): void {
  const peer = followerPeers.get(followerTabId);
  if (!peer) return;
  followerPeers.delete(followerTabId);
  peer.port.removeEventListener("message", peer.onMessage);
  peer.port.removeEventListener("messageerror", peer.onMessageError);
  peer.pump?.close();
  peer.subscriber?.free?.();
  peer.port.close();
  if (notify) {
    postEvent({
      type: "follower-port-closed",
      followerTabId,
      leadershipId: peer.leadershipId,
    });
  }
}

function requireRuntime(): NativeRuntimeAdapter {
  if (!runtime) throw new Error("Browser persistence worker runtime is closed");
  return runtime;
}

function postFrames(frames: Uint8Array[]): void {
  if (suppressOutboundFrames) return;
  const copies = transferableFrames(frames);
  postEvent(
    { type: "frames", frames: copies },
    copies.map((frame) => frame.buffer),
  );
}

function postResult(id: number, error?: Error): void {
  postEvent({ type: "result", id, ...(error ? { error: error.message } : {}) });
}

function postEvent(event: BrowserWorkerEvent, transfer?: Transferable[]): void {
  workerScope.postMessage(event, transfer);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
