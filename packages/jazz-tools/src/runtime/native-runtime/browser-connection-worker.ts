import { loadWasmModule, type WasmModule } from "../client.js";
import { installWasmTelemetry } from "../sync-telemetry.js";
import { BrowserWorkerTransportPump, transferableFrames } from "./browser-worker-transport.js";
import type {
  BrowserWorkerEvent,
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
  runtime.onAuthFailure((reason) => postEvent({ type: "auth-failure", reason }));
  subscriber = runtime.acceptPeer(options.sessionClaims);
  relayPump = new BrowserWorkerTransportPump(runtime, subscriber, postFrames);
  if (options.serverUrl) runtime.connect(options.serverUrl, options.authJson);
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
    case "update-auth":
      subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
      await activeRuntime.updateAuth(message.authJson);
      break;
    case "disconnect":
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
    case "delete-storage": {
      const dbName = initOptions?.dbName;
      if (!dbName || !wasmModule) throw new Error("Browser storage namespace is unavailable");
      await closeRuntime();
      await wasmModule.WasmDb.destroyBrowserStorage(dbName);
      break;
    }
    case "close":
      await closeRuntime();
      break;
  }
  postResult(message.id);
  if (message.type === "close" || message.type === "delete-storage") {
    queueMicrotask(() => workerScope.close());
  }
}

async function closeRuntime(): Promise<void> {
  relayPump?.close();
  relayPump = null;
  await runtime?.close();
  runtime = null;
  disposeTelemetry?.();
  disposeTelemetry = null;
}

function requireRuntime(): NativeRuntimeAdapter {
  if (!runtime) throw new Error("Browser persistence worker runtime is closed");
  return runtime;
}

function postFrames(frames: Uint8Array[]): void {
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
