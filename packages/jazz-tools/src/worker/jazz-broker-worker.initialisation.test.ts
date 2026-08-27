import type { Mock } from "vitest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  BrowserFollowerPortEvent,
  BrowserFollowerPortRequest,
  BrowserSharedWorkerConnectRequest,
  BrowserSharedWorkerConnectResponse,
  BrowserWorkerInitOptions,
} from "../runtime/native-runtime/browser-worker-protocol.js";

const mocks = vi.hoisted(() => {
  const loadWasmModule = vi.fn();
  const installWasmTelemetry = vi.fn();
  const openPageStore = vi.fn();
  const openBrowser = vi.fn();
  const openBrowserWithSelfSignedProof = vi.fn();
  const fromDb = vi.fn();
  const encodeSchema = vi.fn(() => new Uint8Array());
  const openConfig = vi.fn(() => new Uint8Array());
  const telemetryDisposers: Mock[] = [];
  const pageStores: Array<{ close: Mock }> = [];
  const runtimes: Array<Record<string, Mock>> = [];
  const wasmModule = {
    WasmDb: {
      openBrowser,
      openBrowserWithSelfSignedProof,
    },
  };

  return {
    loadWasmModule,
    installWasmTelemetry,
    openPageStore,
    openBrowser,
    openBrowserWithSelfSignedProof,
    fromDb,
    encodeSchema,
    openConfig,
    telemetryDisposers,
    pageStores,
    runtimes,
    wasmModule,
    reset() {
      loadWasmModule.mockReset().mockResolvedValue(wasmModule);
      installWasmTelemetry.mockReset().mockImplementation(() => {
        const dispose = vi.fn();
        telemetryDisposers.push(dispose);
        return dispose;
      });
      openPageStore.mockReset().mockImplementation(async () => {
        const pageStore = { close: vi.fn() };
        pageStores.push(pageStore);
        return pageStore;
      });
      openBrowser.mockReset().mockResolvedValue({});
      openBrowserWithSelfSignedProof.mockReset().mockResolvedValue({});
      fromDb.mockReset().mockImplementation(() => {
        const subscriber = {
          setAuxiliaryTraceEnabled: vi.fn(),
          setOutboundScheduler: vi.fn(),
          clearOutboundScheduler: vi.fn(),
          recvWireFrames: vi.fn(() => []),
          sendWireFrame: vi.fn(),
        };
        const runtime = {
          discard: vi.fn(),
          onAuthFailure: vi.fn(),
          onMutationError: vi.fn(),
          onPeerTransportWork: vi.fn(() => () => {}),
          progressPeerTransport: vi.fn(async () => undefined),
          retirePeerTransport: vi.fn(async () => undefined),
          acceptPeerWhenIdle: vi.fn(async () => subscriber),
          connect: vi.fn(),
          disconnect: vi.fn(async () => undefined),
          updateAuth: vi.fn(async () => undefined),
          waitForUpstreamServerConnection: vi.fn(async () => undefined),
        };
        runtimes.push(runtime);
        return runtime;
      });
      encodeSchema.mockClear();
      openConfig.mockClear();
      telemetryDisposers.length = 0;
      pageStores.length = 0;
      runtimes.length = 0;
    },
  };
});

vi.mock("../runtime/client.js", () => ({
  loadWasmModule: mocks.loadWasmModule,
}));

vi.mock("../runtime/indexeddb-page-store.js", () => ({
  IndexedDbPageStore: { open: mocks.openPageStore },
}));

vi.mock("../runtime/sync-telemetry.js", () => ({
  installWasmTelemetry: mocks.installWasmTelemetry,
}));

vi.mock("../runtime/native-runtime/native-codec.js", () => ({
  openConfig: mocks.openConfig,
}));

vi.mock("../runtime/native-runtime/native-runtime-adapter.js", () => ({
  NativeRuntimeAdapter: { fromDb: mocks.fromDb },
}));

vi.mock("../runtime/native-runtime/schema-codec.js", () => ({
  encodeSchema: mocks.encodeSchema,
}));

type RuntimeOutcome = Extract<
  BrowserSharedWorkerConnectResponse,
  { type: "runtime-ready" | "runtime-error" }
>;

type WorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
};

class TestPort {
  readonly close = vi.fn();
  readonly start = vi.fn();
  private readonly listeners = new Map<string, Set<(event: MessageEvent) => void>>();
  private readonly outcomes: RuntimeOutcome[] = [];
  private readonly outcomeWaiters: Array<(outcome: RuntimeOutcome) => void> = [];
  private readonly events: BrowserFollowerPortEvent[] = [];
  private readonly eventWaiters: Array<{
    predicate: (event: BrowserFollowerPortEvent) => boolean;
    resolve: (event: BrowserFollowerPortEvent) => void;
  }> = [];

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: BrowserSharedWorkerConnectResponse | BrowserFollowerPortEvent): void {
    if (message.type === "runtime-ready" || message.type === "runtime-error") {
      const waiter = this.outcomeWaiters.shift();
      if (waiter) waiter(message);
      else this.outcomes.push(message);
      return;
    }
    // Bootstrap liveness is intentionally not a follower event or a runtime
    // connection outcome; tests only retain the latter two protocol classes.
    if (message.type === "worker-alive") return;
    const waiterIndex = this.eventWaiters.findIndex(({ predicate }) => predicate(message));
    if (waiterIndex >= 0) {
      this.eventWaiters.splice(waiterIndex, 1)[0]!.resolve(message);
    } else this.events.push(message);
  }

  emitMessage(message: BrowserSharedWorkerConnectRequest | BrowserFollowerPortRequest): void {
    for (const listener of this.listeners.get("message") ?? []) {
      listener({ data: message } as MessageEvent);
    }
  }

  waitForOutcome(): Promise<RuntimeOutcome> {
    const outcome = this.outcomes.shift();
    if (outcome) return Promise.resolve(outcome);
    const waiter = deferred<RuntimeOutcome>();
    this.outcomeWaiters.push(waiter.resolve);
    return waiter.promise;
  }

  waitForEvent(
    predicate: (event: BrowserFollowerPortEvent) => boolean,
  ): Promise<BrowserFollowerPortEvent> {
    const index = this.events.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.events.splice(index, 1)[0]!);
    const waiter = deferred<BrowserFollowerPortEvent>();
    this.eventWaiters.push({ predicate, resolve: waiter.resolve });
    return waiter.promise;
  }

  hasEvent(predicate: (event: BrowserFollowerPortEvent) => boolean): boolean {
    return this.events.some(predicate);
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function options(dbName: string): BrowserWorkerInitOptions {
  return {
    schema: {},
    dbName,
    node: new Uint8Array([1]),
    author: new Uint8Array([2]),
    initialSyncFlushEvery: 1,
    appId: "worker-initialization-test",
    authSessionKey: "session",
    authJson: "{}",
    sessionClaims: {},
  };
}

function enabledTelemetryOptions(dbName: string): BrowserWorkerInitOptions {
  return {
    ...options(dbName),
    telemetryCollectorUrl: "http://localhost:4318",
  };
}

async function connect(
  initOptions: BrowserWorkerInitOptions,
  tabId: string,
): Promise<{ outcome: RuntimeOutcome; port: TestPort }> {
  const port = new TestPort();
  const outcome = port.waitForOutcome();
  const onconnect = (globalThis as WorkerGlobal).onconnect;
  if (!onconnect) throw new Error("broker worker did not install its connect handler");
  onconnect({ ports: [port as unknown as MessagePort] } as MessageEvent & {
    ports: MessagePort[];
  });
  port.emitMessage({
    type: "connect-runtime",
    tabId,
    fingerprint: "shared-fingerprint",
    options: initOptions,
  });
  return { outcome: await outcome, port };
}

async function initializeFollower(port: TestPort, id: number): Promise<void> {
  const result = port.waitForEvent((event) => event.type === "result" && event.id === id);
  port.emitMessage({ type: "init", id, sessionClaims: {} });
  await result;
}

describe("broker worker context initialization", () => {
  beforeEach(async () => {
    mocks.reset();
    vi.resetModules();
    // The worker owns process-global state, so each case must evaluate a fresh module instance.
    await import("./jazz-broker-worker.js");
  });

  it("recovers a rejected process-wide WASM load and retains the successful replacement", async () => {
    const failedLoad = deferred<typeof mocks.wasmModule>();
    const successfulLoad = deferred<typeof mocks.wasmModule>();
    mocks.loadWasmModule
      .mockReturnValueOnce(failedLoad.promise)
      .mockReturnValueOnce(successfulLoad.promise);

    const failedConnection = connect(options("failed-wasm"), "failed-tab");
    const successfulConnection = connect(options("successful-wasm"), "successful-tab");
    failedLoad.reject(new Error("WASM load failed"));

    const failed = await failedConnection;
    expect(failed.outcome).toEqual({ type: "runtime-error", message: "WASM load failed" });
    expect(failed.port.close).toHaveBeenCalledOnce();
    await vi.waitFor(() => expect(mocks.loadWasmModule).toHaveBeenCalledTimes(2));

    successfulLoad.resolve(mocks.wasmModule);
    expect((await successfulConnection).outcome).toEqual({ type: "runtime-ready" });
    expect((await connect(options("failed-wasm"), "retry-tab")).outcome).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledTimes(2);
  });

  it("does not let a second context repoint the process-wide WASM realm at another origin", async () => {
    const firstOptions = {
      ...options("first-origin"),
      runtimeSources: { wasmUrl: "http://vite-first.test/assets/jazz_wasm_bg.wasm" },
    };
    const secondOptions = {
      ...options("second-origin"),
      runtimeSources: { wasmUrl: "http://vite-second.test/assets/jazz_wasm_bg.wasm" },
    };

    expect((await connect(firstOptions, "first-tab")).outcome).toEqual({ type: "runtime-ready" });
    expect(mocks.loadWasmModule).toHaveBeenCalledWith(firstOptions.runtimeSources);

    // Different persistent contexts can reach one long-lived SharedWorker
    // process. Its wasm-bindgen module is initialized exactly once, so a
    // later page must not silently inherit an asset URL from a Vite origin
    // which may already have been torn down.
    expect((await connect(secondOptions, "second-tab")).outcome).toEqual({
      type: "runtime-error",
      message:
        "incompatible WASM asset source for this SharedWorker; start a worker scoped to the new asset URL",
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledTimes(1);
  });

  it("never aliases distinct supplied WASM byte arrays to the first worker realm", async () => {
    const firstOptions = {
      ...options("first-source"),
      runtimeSources: { wasmSource: new Uint8Array([0, 97, 115, 109]) },
    };
    const secondOptions = {
      ...options("second-source"),
      runtimeSources: { wasmSource: new Uint8Array([0, 97, 115, 109]) },
    };

    expect((await connect(firstOptions, "first-tab")).outcome).toEqual({ type: "runtime-ready" });
    expect((await connect(secondOptions, "second-tab")).outcome).toEqual({
      type: "runtime-error",
      message:
        "incompatible WASM asset source for this SharedWorker; start a worker scoped to the new asset URL",
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledTimes(1);
  });

  it("evicts a context when telemetry installation fails so the same key can retry", async () => {
    mocks.installWasmTelemetry.mockImplementationOnce(() => {
      throw new Error("telemetry installation failed");
    });

    const failed = await connect(enabledTelemetryOptions("telemetry-failure"), "failed-tab");
    expect(failed.outcome).toEqual({
      type: "runtime-error",
      message: "telemetry installation failed",
    });
    expect(failed.port.close).toHaveBeenCalledOnce();
    expect(mocks.openPageStore).not.toHaveBeenCalled();

    expect(
      (await connect(enabledTelemetryOptions("telemetry-failure"), "retry-tab")).outcome,
    ).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledOnce();
    expect(mocks.installWasmTelemetry).toHaveBeenCalledTimes(2);
  });

  it("disposes telemetry when page-store opening fails and retries the same key", async () => {
    mocks.openPageStore.mockRejectedValueOnce(new Error("page-store open failed"));

    const failed = await connect(enabledTelemetryOptions("page-store-failure"), "failed-tab");
    expect(failed.outcome).toEqual({
      type: "runtime-error",
      message: "page-store open failed",
    });
    expect(mocks.telemetryDisposers[0]).toHaveBeenCalledOnce();
    expect(mocks.openBrowser).not.toHaveBeenCalled();

    expect(
      (await connect(enabledTelemetryOptions("page-store-failure"), "retry-tab")).outcome,
    ).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.telemetryDisposers).toHaveLength(2);
    expect(mocks.telemetryDisposers[1]).not.toHaveBeenCalled();
    expect(mocks.openPageStore).toHaveBeenCalledTimes(2);
  });

  it("closes the page store and telemetry when browser DB opening fails, then retries", async () => {
    mocks.openBrowser.mockRejectedValueOnce(new Error("browser DB open failed"));

    const failed = await connect(enabledTelemetryOptions("browser-db-failure"), "failed-tab");
    expect(failed.outcome).toEqual({
      type: "runtime-error",
      message: "browser DB open failed",
    });
    expect(mocks.pageStores[0]?.close).toHaveBeenCalledOnce();
    expect(mocks.telemetryDisposers[0]).toHaveBeenCalledOnce();

    expect(
      (await connect(enabledTelemetryOptions("browser-db-failure"), "retry-tab")).outcome,
    ).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.pageStores).toHaveLength(2);
    expect(mocks.pageStores[1]?.close).not.toHaveBeenCalled();
    expect(mocks.telemetryDisposers[1]).not.toHaveBeenCalled();
    expect(mocks.openBrowser).toHaveBeenCalledTimes(2);
  });

  it("carries a verified local-first proof from worker open to follower admission", async () => {
    const selfSignedClientProof = {
      token: "verified-token",
      appId: "worker-initialization-test",
      claimedAuthor: '["urn:jazz:local-first","alice"]',
    };
    const initOptions = { ...options("self-signed-worker"), selfSignedClientProof };

    expect((await connect(initOptions, "proof-tab")).outcome).toEqual({ type: "runtime-ready" });
    expect(mocks.openBrowser).not.toHaveBeenCalled();
    expect(mocks.openBrowserWithSelfSignedProof).toHaveBeenCalledWith(
      expect.anything(),
      expect.any(Uint8Array),
      expect.any(Uint8Array),
      selfSignedClientProof.token,
      selfSignedClientProof.appId,
      selfSignedClientProof.claimedAuthor,
    );
    expect(mocks.fromDb).toHaveBeenCalledWith(
      expect.anything(),
      initOptions.schema,
      initOptions.node,
      initOptions.author,
      1,
      false,
      { selfSignedClientProof },
    );
  });

  it("closes an unowned browser DB when adapter construction fails, cleans up, and retries", async () => {
    const rawDb = { close: vi.fn(async () => true) };
    mocks.openBrowser.mockResolvedValueOnce(rawDb);
    mocks.fromDb.mockImplementationOnce(() => {
      throw new Error("adapter construction failed");
    });

    const failed = await connect(enabledTelemetryOptions("adapter-failure"), "failed-tab");
    expect(failed.outcome).toEqual({
      type: "runtime-error",
      message: "adapter construction failed",
    });
    expect(failed.port.close).toHaveBeenCalledOnce();
    expect(rawDb.close).toHaveBeenCalledOnce();
    expect(mocks.pageStores[0]?.close).toHaveBeenCalledOnce();
    expect(mocks.telemetryDisposers[0]).toHaveBeenCalledOnce();

    expect(
      (await connect(enabledTelemetryOptions("adapter-failure"), "retry-tab")).outcome,
    ).toEqual({ type: "runtime-ready" });
    expect(rawDb.close).toHaveBeenCalledOnce();
    expect(mocks.pageStores).toHaveLength(2);
    expect(mocks.pageStores[1]?.close).not.toHaveBeenCalled();
    expect(mocks.telemetryDisposers).toHaveLength(2);
    expect(mocks.telemetryDisposers[1]).not.toHaveBeenCalled();
    expect(mocks.openBrowser).toHaveBeenCalledTimes(2);
    expect(mocks.fromDb).toHaveBeenCalledTimes(2);
  });

  it("shares one successful initialization between concurrent connections for the same key", async () => {
    const browserDb = deferred<object>();
    mocks.openBrowser.mockReturnValueOnce(browserDb.promise);
    const initOptions = options("concurrent-success");

    const first = connect(initOptions, "first-tab");
    const second = connect(initOptions, "second-tab");
    await vi.waitFor(() => expect(mocks.openBrowser).toHaveBeenCalledOnce());
    browserDb.resolve({});

    expect((await first).outcome).toEqual({ type: "runtime-ready" });
    expect((await second).outcome).toEqual({ type: "runtime-ready" });
    expect(mocks.loadWasmModule).toHaveBeenCalledOnce();
    expect(mocks.installWasmTelemetry).toHaveBeenCalledOnce();
    expect(mocks.openPageStore).toHaveBeenCalledOnce();
    expect(mocks.openBrowser).toHaveBeenCalledOnce();
    expect(mocks.fromDb).toHaveBeenCalledOnce();
  });

  it("serializes cross-port disconnect and reconnect before publishing state", async () => {
    const initOptions = { ...options("serialized-transport"), serverUrl: "ws://server.test" };
    const owner = await connect(initOptions, "owner-tab");
    const editor = await connect(initOptions, "editor-tab");
    await initializeFollower(owner.port, 1);
    await initializeFollower(editor.port, 1);
    const runtime = mocks.runtimes[0]!;
    runtime.connect.mockClear();

    const disconnectGate = deferred<void>();
    runtime.disconnect.mockImplementationOnce(() => disconnectGate.promise);
    owner.port.emitMessage({ type: "disconnect", id: 2 });
    await vi.waitFor(() => expect(runtime.disconnect).toHaveBeenCalledOnce());

    editor.port.emitMessage({
      type: "reconnect",
      id: 2,
      authJson: "{}",
      sessionClaims: {},
    });
    await Promise.resolve();
    expect(runtime.connect).not.toHaveBeenCalled();

    disconnectGate.resolve();
    await owner.port.waitForEvent((event) => event.type === "result" && event.id === 2);
    await editor.port.waitForEvent((event) => event.type === "result" && event.id === 2);
    expect(runtime.connect).toHaveBeenCalledOnce();

    for (const port of [owner.port, editor.port]) {
      expect(await port.waitForEvent((event) => event.type === "transport-state")).toMatchObject({
        explicitlyDisconnected: true,
      });
      expect(await port.waitForEvent((event) => event.type === "transport-state")).toMatchObject({
        explicitlyDisconnected: false,
      });
    }
  });

  it("reports the settled offline state before a late follower init succeeds", async () => {
    const initOptions = { ...options("late-offline-follower"), serverUrl: "ws://server.test" };
    const owner = await connect(initOptions, "owner-tab");
    await initializeFollower(owner.port, 1);
    owner.port.emitMessage({ type: "disconnect", id: 2 });
    await owner.port.waitForEvent((event) => event.type === "result" && event.id === 2);

    const late = await connect(initOptions, "late-tab");
    const offline = late.port.waitForEvent((event) => event.type === "transport-state");
    const initialized = late.port.waitForEvent(
      (event) => event.type === "result" && event.id === 1,
    );
    late.port.emitMessage({ type: "init", id: 1, sessionClaims: {} });
    expect(await offline).toMatchObject({ explicitlyDisconnected: true });
    await initialized;
  });

  it("cancels a closed peer's offline server wait", async () => {
    const initOptions = { ...options("closed-offline-waiter"), serverUrl: "ws://server.test" };
    const owner = await connect(initOptions, "owner-tab");
    const editor = await connect(initOptions, "editor-tab");
    await initializeFollower(owner.port, 1);
    await initializeFollower(editor.port, 1);

    owner.port.emitMessage({ type: "disconnect", id: 2 });
    await owner.port.waitForEvent((event) => event.type === "result" && event.id === 2);

    // The waiter parks on namespace state while explicitly offline. Closing its
    // port must cancel that wait, rather than retaining it until a later peer
    // reconnects and then posting a stale result to the dead port.
    editor.port.emitMessage({ type: "wait-server", id: 2 });
    await Promise.resolve();
    const closed = editor.port.waitForEvent((event) => event.type === "result" && event.id === 3);
    editor.port.emitMessage({ type: "close", id: 3 });
    await closed;

    owner.port.emitMessage({
      type: "reconnect",
      id: 3,
      authJson: "{}",
      sessionClaims: {},
    });
    await owner.port.waitForEvent((event) => event.type === "result" && event.id === 3);
    await Promise.resolve();
    expect(editor.port.hasEvent((event) => event.type === "result" && event.id === 2)).toBe(false);
  });
});
