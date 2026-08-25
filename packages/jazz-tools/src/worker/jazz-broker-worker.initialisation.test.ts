import type { Mock } from "vitest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
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
        const runtime = {
          discard: vi.fn(),
          onAuthFailure: vi.fn(),
          onMutationError: vi.fn(),
        };
        return runtime;
      });
      encodeSchema.mockClear();
      openConfig.mockClear();
      telemetryDisposers.length = 0;
      pageStores.length = 0;
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

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: BrowserSharedWorkerConnectResponse): void {
    if (message.type !== "runtime-ready" && message.type !== "runtime-error") return;
    const waiter = this.outcomeWaiters.shift();
    if (waiter) waiter(message);
    else this.outcomes.push(message);
  }

  emitMessage(message: BrowserSharedWorkerConnectRequest): void {
    for (const listener of this.listeners.get("message") ?? []) {
      listener({ data: message } as MessageEvent);
    }
  }

  waitForOutcome(): Promise<RuntimeOutcome> {
    const outcome = this.outcomes.shift();
    if (outcome) return Promise.resolve(outcome);
    const { promise, resolve } = Promise.withResolvers<RuntimeOutcome>();
    this.outcomeWaiters.push(resolve);
    return promise;
  }
}

function deferred<T>() {
  return Promise.withResolvers<T>();
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

  it("evicts a context when telemetry installation fails so the same key can retry", async () => {
    mocks.installWasmTelemetry.mockImplementationOnce(() => {
      throw new Error("telemetry installation failed");
    });

    const failed = await connect(options("telemetry-failure"), "failed-tab");
    expect(failed.outcome).toEqual({
      type: "runtime-error",
      message: "telemetry installation failed",
    });
    expect(failed.port.close).toHaveBeenCalledOnce();
    expect(mocks.openPageStore).not.toHaveBeenCalled();

    expect((await connect(options("telemetry-failure"), "retry-tab")).outcome).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledOnce();
    expect(mocks.installWasmTelemetry).toHaveBeenCalledTimes(2);
  });

  it("disposes telemetry when page-store opening fails and retries the same key", async () => {
    mocks.openPageStore.mockRejectedValueOnce(new Error("page-store open failed"));

    const failed = await connect(options("page-store-failure"), "failed-tab");
    expect(failed.outcome).toEqual({
      type: "runtime-error",
      message: "page-store open failed",
    });
    expect(mocks.telemetryDisposers[0]).toHaveBeenCalledOnce();
    expect(mocks.openBrowser).not.toHaveBeenCalled();

    expect((await connect(options("page-store-failure"), "retry-tab")).outcome).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.telemetryDisposers).toHaveLength(2);
    expect(mocks.telemetryDisposers[1]).not.toHaveBeenCalled();
    expect(mocks.openPageStore).toHaveBeenCalledTimes(2);
  });

  it("closes the page store and telemetry when browser DB opening fails, then retries", async () => {
    mocks.openBrowser.mockRejectedValueOnce(new Error("browser DB open failed"));

    const failed = await connect(options("browser-db-failure"), "failed-tab");
    expect(failed.outcome).toEqual({
      type: "runtime-error",
      message: "browser DB open failed",
    });
    expect(mocks.pageStores[0]?.close).toHaveBeenCalledOnce();
    expect(mocks.telemetryDisposers[0]).toHaveBeenCalledOnce();

    expect((await connect(options("browser-db-failure"), "retry-tab")).outcome).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.pageStores).toHaveLength(2);
    expect(mocks.pageStores[1]?.close).not.toHaveBeenCalled();
    expect(mocks.telemetryDisposers[1]).not.toHaveBeenCalled();
    expect(mocks.openBrowser).toHaveBeenCalledTimes(2);
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
});
