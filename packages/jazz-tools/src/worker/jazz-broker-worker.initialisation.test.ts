import type { Mock } from "vitest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  BrowserForegroundNodeLeaseAcquireRequest,
  BrowserForegroundNodeLeaseAcquireResponse,
  BrowserForegroundNodeLeaseCancelRequest,
  BrowserForegroundNodeLeasePortRequest,
  BrowserForegroundNodeLeaseProbeRequest,
  BrowserFollowerPortEvent,
  BrowserFollowerPortRequest,
  BrowserInspectorControlEvent,
  BrowserInspectorControlRequest,
  BrowserSharedWorkerConnectRequest,
  BrowserSharedWorkerConnectResponse,
  BrowserWorkerInitOptions,
} from "../runtime/native-runtime/browser-worker-protocol.js";
import {
  deserializeBrowserRelayError,
  type BrowserRelayError,
} from "../runtime/native-runtime/browser-worker-protocol.js";

const mocks = vi.hoisted(() => {
  const loadWasmModule = vi.fn();
  const installWasmTelemetry = vi.fn();
  const openPageStore = vi.fn();
  const openBrowser = vi.fn();
  const openBrowserWithSelfSignedProof = vi.fn();
  const fromDb = vi.fn();
  const encodeSchema = vi.fn(() => new Uint8Array());
  const openConfig = vi.fn((_node: Uint8Array, ..._rest: unknown[]) => new Uint8Array());
  const telemetryDisposers: Mock[] = [];
  const pageStores: Array<{
    close: Mock;
    claimBrowserWorkerEpoch: Mock;
    releaseBrowserWorkerEpoch: Mock;
    onInvalidated: Mock;
    acquireForegroundNodeLease: Mock;
    returnForegroundNodeLease: Mock;
    retireForegroundNodeLease: Mock;
    canonicalReplicaNode: Uint8Array;
    readonly replicaNode: Uint8Array;
  }> = [];
  const browserDbs: Array<{ close: Mock }> = [];
  const runtimes: Array<Record<string, Mock>> = [];
  const createBrowserDb = () => {
    const db = {
      close: vi.fn(async () => true),
    };
    browserDbs.push(db);
    return db;
  };
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
    browserDbs,
    runtimes,
    createBrowserDb,
    wasmModule,
    reset() {
      loadWasmModule.mockReset().mockResolvedValue(wasmModule);
      installWasmTelemetry.mockReset().mockImplementation(() => {
        const dispose = vi.fn();
        telemetryDisposers.push(dispose);
        return dispose;
      });
      openPageStore.mockReset().mockImplementation(async () => {
        const replicaNode = new Uint8Array(16);
        // IndexedDbPageStore deliberately returns a defensive copy: production
        // callers must not be able to mutate its persisted replica identity.
        // Keep this canonical byte string independently so the boundary tests
        // below cannot pass merely because mocks share one object reference.
        replicaNode.fill(pageStores.length + 3);
        const pageStore = {
          close: vi.fn(),
          claimBrowserWorkerEpoch: vi.fn(async () => undefined),
          releaseBrowserWorkerEpoch: vi.fn(async () => undefined),
          onInvalidated: vi.fn(() => () => undefined),
          acquireForegroundNodeLease: vi.fn(async () => ({
            leaseId: `lease-${pageStores.length}`,
            node: Uint8Array.from(replicaNode),
            confirmedTxTime: 0n,
          })),
          returnForegroundNodeLease: vi.fn(async () => undefined),
          retireForegroundNodeLease: vi.fn(async () => undefined),
          canonicalReplicaNode: replicaNode,
          get replicaNode() {
            return Uint8Array.from(replicaNode);
          },
        };
        pageStores.push(pageStore);
        return pageStore;
      });
      openBrowser.mockReset().mockImplementation(async () => createBrowserDb());
      openBrowserWithSelfSignedProof.mockReset().mockImplementation(async () => createBrowserDb());
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
          onServerTransportError: vi.fn(),
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
      browserDbs.length = 0;
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
  { type: "runtime-ready" | "runtime-error" | "worker-closing" }
>;

type ForegroundLeaseOutcome = Extract<
  BrowserForegroundNodeLeaseAcquireResponse,
  {
    type:
      | "foreground-node-lease-ready"
      | "foreground-node-lease-busy"
      | "foreground-node-lease-error";
  }
>;

type ForegroundLeaseProbeOutcome = Extract<
  BrowserForegroundNodeLeaseAcquireResponse,
  {
    type: "foreground-node-lease-worker-alive" | "foreground-node-lease-worker-closing";
  }
>;

type ForegroundLeaseCancellation = Extract<
  BrowserForegroundNodeLeaseAcquireResponse,
  { type: "foreground-node-lease-cancelled" }
>;

// A TestPort models the untyped MessagePort boundary used by both follower
// connections and Inspector control connections. Keep the protocol union here
// instead of narrowing control events away: an Inspector control port can emit
// `contexts`, `lifecycle-trace`, and its own `result` acknowledgement.
type TestPortEvent = BrowserFollowerPortEvent | BrowserInspectorControlEvent;
type TestPortRequest =
  | BrowserSharedWorkerConnectRequest
  | BrowserForegroundNodeLeaseProbeRequest
  | BrowserForegroundNodeLeaseAcquireRequest
  | BrowserForegroundNodeLeaseCancelRequest
  | BrowserForegroundNodeLeasePortRequest
  | BrowserFollowerPortRequest
  | BrowserInspectorControlRequest;
type TestPortResponse =
  | BrowserSharedWorkerConnectResponse
  | BrowserFollowerPortEvent
  | BrowserForegroundNodeLeaseAcquireResponse
  | BrowserInspectorControlEvent;

type WorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
  close?: Mock;
};

class TestPort {
  readonly close = vi.fn();
  readonly start = vi.fn();
  private readonly listeners = new Map<string, Set<(event: MessageEvent) => void>>();
  private readonly outcomes: RuntimeOutcome[] = [];
  private readonly outcomeWaiters: Array<(outcome: RuntimeOutcome) => void> = [];
  private readonly leaseOutcomes: ForegroundLeaseOutcome[] = [];
  private readonly leaseOutcomeWaiters: Array<(outcome: ForegroundLeaseOutcome) => void> = [];
  private readonly leaseProbeOutcomes: ForegroundLeaseProbeOutcome[] = [];
  private readonly leaseProbeOutcomeWaiters: Array<(outcome: ForegroundLeaseProbeOutcome) => void> =
    [];
  private readonly leaseCancellations: ForegroundLeaseCancellation[] = [];
  private readonly leaseCancellationWaiters: Array<(outcome: ForegroundLeaseCancellation) => void> =
    [];
  private readonly events: TestPortEvent[] = [];
  private readonly eventWaiters: Array<{
    predicate: (event: TestPortEvent) => boolean;
    resolve: (event: TestPortEvent) => void;
  }> = [];

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: TestPortResponse): void {
    if (
      message.type === "runtime-ready" ||
      message.type === "runtime-error" ||
      message.type === "worker-closing"
    ) {
      const waiter = this.outcomeWaiters.shift();
      if (waiter) waiter(message);
      else this.outcomes.push(message);
      return;
    }
    // Bootstrap liveness is intentionally not a follower event or a runtime
    // connection outcome; tests only retain the latter two protocol classes.
    if (message.type === "worker-alive") return;
    if (
      message.type === "foreground-node-lease-worker-alive" ||
      message.type === "foreground-node-lease-worker-closing"
    ) {
      const waiter = this.leaseProbeOutcomeWaiters.shift();
      if (waiter) waiter(message);
      else this.leaseProbeOutcomes.push(message);
      return;
    }
    if (
      message.type === "foreground-node-lease-ready" ||
      message.type === "foreground-node-lease-busy" ||
      message.type === "foreground-node-lease-error"
    ) {
      const waiter = this.leaseOutcomeWaiters.shift();
      if (waiter) waiter(message);
      else this.leaseOutcomes.push(message);
      return;
    }
    if (message.type === "foreground-node-lease-cancelled") {
      const waiter = this.leaseCancellationWaiters.shift();
      if (waiter) waiter(message);
      else this.leaseCancellations.push(message);
      return;
    }
    if (message.type === "foreground-node-lease-test-allocated") {
      throw new Error(`Unexpected lease bootstrap response: ${message.type}`);
    }
    const waiterIndex = this.eventWaiters.findIndex(({ predicate }) => predicate(message));
    if (waiterIndex >= 0) {
      this.eventWaiters.splice(waiterIndex, 1)[0]!.resolve(message);
    } else this.events.push(message);
  }

  emitMessage(message: TestPortRequest): void {
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

  waitForLeaseOutcome(): Promise<ForegroundLeaseOutcome> {
    const outcome = this.leaseOutcomes.shift();
    if (outcome) return Promise.resolve(outcome);
    const waiter = deferred<ForegroundLeaseOutcome>();
    this.leaseOutcomeWaiters.push(waiter.resolve);
    return waiter.promise;
  }

  waitForLeaseProbeOutcome(): Promise<ForegroundLeaseProbeOutcome> {
    const outcome = this.leaseProbeOutcomes.shift();
    if (outcome) return Promise.resolve(outcome);
    const waiter = deferred<ForegroundLeaseProbeOutcome>();
    this.leaseProbeOutcomeWaiters.push(waiter.resolve);
    return waiter.promise;
  }

  waitForLeaseCancellation(): Promise<ForegroundLeaseCancellation> {
    const outcome = this.leaseCancellations.shift();
    if (outcome) return Promise.resolve(outcome);
    const waiter = deferred<ForegroundLeaseCancellation>();
    this.leaseCancellationWaiters.push(waiter.resolve);
    return waiter.promise;
  }

  leaseCancellationCount(): number {
    return this.leaseCancellations.length;
  }

  hasLeaseProbeOutcome(): boolean {
    return this.leaseProbeOutcomes.length > 0;
  }

  waitForEvent(predicate: (event: TestPortEvent) => boolean): Promise<TestPortEvent> {
    const index = this.events.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.events.splice(index, 1)[0]!);
    const waiter = deferred<TestPortEvent>();
    this.eventWaiters.push({ predicate, resolve: waiter.resolve });
    return waiter.promise;
  }

  hasEvent(predicate: (event: TestPortEvent) => boolean): boolean {
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
    author: new Uint8Array([2]),
    initialSyncFlushEvery: 1,
    appId: "worker-initialization-test",
    storageOwner: "worker-initialization-test-owner",
    authSessionKey: "session",
    authJson: "{}",
    sessionClaims: {},
  };
}

function connectLease(request: BrowserForegroundNodeLeaseAcquireRequest): {
  outcome: Promise<ForegroundLeaseOutcome>;
  port: TestPort;
} {
  const port = new TestPort();
  const onconnect = (globalThis as WorkerGlobal).onconnect;
  if (!onconnect) throw new Error("broker worker did not install its connect handler");
  onconnect({ ports: [port as unknown as MessagePort] } as MessageEvent & {
    ports: MessagePort[];
  });
  const outcome = port.waitForLeaseOutcome();
  port.emitMessage(request);
  return { outcome, port };
}

function connectLeaseProbe(): { port: TestPort } {
  const port = new TestPort();
  const onconnect = (globalThis as WorkerGlobal).onconnect;
  if (!onconnect) throw new Error("broker worker did not install its connect handler");
  onconnect({ ports: [port as unknown as MessagePort] } as MessageEvent & {
    ports: MessagePort[];
  });
  return { port };
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

async function openInspector(port: TestPort, id: number): Promise<MessagePort> {
  const channel = new MessageChannel();
  const result = port.waitForEvent((event) => event.type === "result" && event.id === id);
  port.emitMessage({ type: "open-inspector-control", id, port: channel.port2 });
  await result;
  channel.port1.start();
  return channel.port1;
}

async function readLifecycle(
  port: MessagePort,
  id: number,
): Promise<Extract<BrowserInspectorControlEvent, { type: "lifecycle-trace" }>["entries"]> {
  return new Promise((resolve) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (event.data.type !== "lifecycle-trace" || event.data.id !== id) return;
      port.removeEventListener("message", onMessage);
      resolve(event.data.entries);
    };
    port.addEventListener("message", onMessage);
    port.postMessage({ type: "lifecycle-trace", id } satisfies BrowserInspectorControlRequest);
  });
}

async function terminateInspector(
  port: MessagePort,
  id: number,
): Promise<Extract<BrowserInspectorControlEvent, { type: "result" }>> {
  return new Promise((resolve, reject) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (event.data.type !== "result" || event.data.id !== id) return;
      port.removeEventListener("message", onMessage);
      if (event.data.error) reject(deserializeBrowserRelayError(event.data.error));
      else resolve(event.data);
    };
    port.addEventListener("message", onMessage);
    port.postMessage({ type: "terminate-worker", id } satisfies BrowserInspectorControlRequest);
  });
}

async function inspectorResult(
  port: MessagePort,
  id: number,
): Promise<Extract<BrowserInspectorControlEvent, { type: "result" }>> {
  return new Promise((resolve) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (event.data.type !== "result" || event.data.id !== id) return;
      port.removeEventListener("message", onMessage);
      resolve(event.data);
    };
    port.addEventListener("message", onMessage);
    port.postMessage({ type: "terminate-worker", id } satisfies BrowserInspectorControlRequest);
  });
}

describe("broker worker context initialization", () => {
  beforeEach(async () => {
    mocks.reset();
    (globalThis as WorkerGlobal).close = vi.fn();
    vi.resetModules();
    // The worker owns process-global state, so each case must evaluate a fresh module instance.
    await import("./jazz-broker-worker.js");
  });

  it("marks only control-port attached followers as Inspector peers", async () => {
    const host = await connect(options("inspector-authenticated-root"), "host-tab");
    expect(host.outcome).toEqual({ type: "runtime-ready" });
    await initializeFollower(host.port, 1);

    const control = new TestPort();
    const controlOpened = host.port.waitForEvent(
      (event) => event.type === "result" && event.id === 2,
    );
    host.port.emitMessage({
      type: "open-inspector-control",
      id: 2,
      port: control as unknown as MessagePort,
    });
    await controlOpened;

    const inspectorPeer = new TestPort();
    control.emitMessage({
      type: "attach-context",
      id: 3,
      contextKey: "inspector-authenticated-root",
      tabId: "inspector-tab",
      port: inspectorPeer as unknown as MessagePort,
    });
    const receipt = inspectorPeer.waitForEvent(
      (event) => event.type === "result" && event.id === 4,
    );
    inspectorPeer.emitMessage({ type: "init", id: 4, sessionClaims: {} });
    await expect(receipt).resolves.toMatchObject({
      inspectorAttachmentPhysicalDbName: "inspector-authenticated-root",
    });

    // A regular worker connection has the same storage coordinate but cannot
    // gain the receipt merely by knowing it.
    const ordinary = await connect(options("inspector-authenticated-root"), "ordinary-tab");
    await expect(ordinary.outcome).toEqual({ type: "runtime-ready" });
    const ordinaryReceipt = ordinary.port.waitForEvent(
      (event) => event.type === "result" && event.id === 5,
    );
    ordinary.port.emitMessage({ type: "init", id: 5, sessionClaims: {} });
    await expect(ordinaryReceipt).resolves.not.toHaveProperty("inspectorAttachmentPhysicalDbName");

    // A control port remains bound to the session that opened it. Selecting a
    // context from a later/different auth session cannot reuse its authority.
    const other = await connect(
      { ...options("inspector-other-session"), authSessionKey: "other-session" },
      "other-session-tab",
    );
    await initializeFollower(other.port, 6);
    const staleAttach = control.waitForEvent((event) => event.type === "result" && event.id === 7);
    control.emitMessage({
      type: "attach-context",
      id: 7,
      contextKey: "inspector-other-session",
      tabId: "stale-inspector-tab",
      port: new TestPort() as unknown as MessagePort,
    });
    await expect(staleAttach).resolves.toMatchObject({
      error: expect.objectContaining({ message: "Inspector context is no longer available" }),
    });
  });

  it("serializes direct inspector-control failures as bounded relay errors", async () => {
    const first = await connect(options("inspector-relay-error"), "first-tab");
    await initializeFollower(first.port, 1);
    const inspector = await openInspector(first.port, 2);
    try {
      const result = await inspectorResult(inspector, 3);
      expect(result.error).toEqual(
        expect.objectContaining({
          name: "Error",
          message: "Worker still has live runtime contexts",
        }),
      );
      expect(typeof result.error).toBe("object");
      expect(deserializeBrowserRelayError(result.error as BrowserRelayError)).toMatchObject({
        message: "Worker still has live runtime contexts",
      });
    } finally {
      inspector.close();
      first.port.emitMessage({ type: "close", id: 4, releaseContext: true });
    }
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
    expect(failed.outcome).toMatchObject({
      type: "runtime-error",
      error: { name: "Error", message: "WASM load failed", stack: expect.any(String) },
    });
    expect(failed.port.close).toHaveBeenCalledOnce();
    await vi.waitFor(() => expect(mocks.loadWasmModule).toHaveBeenCalledTimes(2));

    successfulLoad.resolve(mocks.wasmModule);
    expect((await successfulConnection).outcome).toEqual({ type: "runtime-ready" });
    expect((await connect(options("failed-wasm"), "retry-tab")).outcome).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledTimes(2);
  });

  it("acknowledges a lease probe before touching its durable root", async () => {
    const attemptId = "probe-before-durable-admission";
    const initOptions = options("probe-before-durable-admission");
    const { port } = connectLeaseProbe();
    const probeOutcome = port.waitForLeaseProbeOutcome();
    port.emitMessage({
      type: "probe-foreground-node-lease-worker",
      attemptId,
    });

    await expect(probeOutcome).resolves.toEqual({
      type: "foreground-node-lease-worker-alive",
      attemptId,
    });
    expect(mocks.openPageStore).not.toHaveBeenCalled();

    const leaseOutcome = port.waitForLeaseOutcome();
    port.emitMessage({
      type: "acquire-foreground-node-lease",
      attemptId,
      dbName: initOptions.dbName,
      storageOwner: initOptions.storageOwner,
    });
    await expect(leaseOutcome).resolves.toEqual(
      expect.objectContaining({ type: "foreground-node-lease-ready" }),
    );
    expect(mocks.openPageStore).toHaveBeenCalledOnce();
  });

  it("does not admit a lease probe after worker termination is acknowledged", async () => {
    const initOptions = options("terminate-before-successor-probe");
    const first = await connect(initOptions, "first-tab");
    await initializeFollower(first.port, 1);
    const inspector = await openInspector(first.port, 2);

    const closed = first.port.waitForEvent((event) => event.type === "result" && event.id === 3);
    first.port.emitMessage({ type: "close", id: 3, releaseContext: true });
    await closed;
    await new Promise<void>((resolve) => setTimeout(resolve, 60));
    await expect(terminateInspector(inspector, 4)).resolves.toEqual({
      type: "result",
      id: 4,
      workerTerminated: true,
    });
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect((globalThis as WorkerGlobal).close).toHaveBeenCalledOnce();

    const successor = connectLeaseProbe();
    const closing = successor.port.waitForLeaseProbeOutcome();
    successor.port.emitMessage({
      type: "probe-foreground-node-lease-worker",
      attemptId: "probe-after-termination-ack",
    });

    await expect(closing).resolves.toEqual({
      type: "foreground-node-lease-worker-closing",
      attemptId: "probe-after-termination-ack",
    });
    expect(successor.port.close).toHaveBeenCalledOnce();

    // The normal runtime bootstrap gets an equally explicit handoff signal.
    // It can advance its SharedWorker generation instead of waiting for a
    // doomed port's generic timeout, while ordinary idle close remains
    // cancelable through its bootstrap reservation.
    await expect(connect(initOptions, "runtime-after-termination")).resolves.toMatchObject({
      outcome: { type: "worker-closing" },
    });
    inspector.close();
  });

  it("rejects termination while preconnected lease bootstraps remain pending", async () => {
    const initOptions = options("terminate-preconnected-lease-bootstrap");
    const first = await connect(initOptions, "first-tab");
    await initializeFollower(first.port, 1);
    const inspector = await openInspector(first.port, 2);

    const closed = first.port.waitForEvent((event) => event.type === "result" && event.id === 3);
    first.port.emitMessage({ type: "close", id: 3, releaseContext: true });
    await closed;
    await new Promise<void>((resolve) => setTimeout(resolve, 60));

    // Both ports connect before termination. One has not sent its first
    // message; the other has received a liveness response but has not yet
    // requested durable allocation.
    const firstMessageAfterTermination = connectLeaseProbe();
    const acquireAfterTermination = connectLeaseProbe();
    const alive = acquireAfterTermination.port.waitForLeaseProbeOutcome();
    acquireAfterTermination.port.emitMessage({
      type: "probe-foreground-node-lease-worker",
      attemptId: "alive-before-termination",
    });
    await expect(alive).resolves.toEqual({
      type: "foreground-node-lease-worker-alive",
      attemptId: "alive-before-termination",
    });
    const durableOpenCount = mocks.openPageStore.mock.calls.length;

    await expect(terminateInspector(inspector, 4)).rejects.toThrow(
      "Worker still has pending bootstrap operations",
    );
    expect((globalThis as WorkerGlobal).close).not.toHaveBeenCalled();

    firstMessageAfterTermination.port.emitMessage({
      type: "cancel-foreground-node-lease",
    });
    acquireAfterTermination.port.emitMessage({
      type: "cancel-foreground-node-lease",
    });
    expect(firstMessageAfterTermination.port.close).toHaveBeenCalledOnce();
    expect(acquireAfterTermination.port.close).toHaveBeenCalledOnce();
    expect(mocks.openPageStore).toHaveBeenCalledTimes(durableOpenCount);
    expect(
      mocks.pageStores.some((store) => store.acquireForegroundNodeLease.mock.calls.length > 0),
    ).toBe(false);

    await terminateInspector(inspector, 5);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect((globalThis as WorkerGlobal).close).toHaveBeenCalledOnce();
    inspector.close();
  });

  it("only terminates after in-flight and active foreground leases are retired", async () => {
    const initOptions = options("terminate-during-lease-admission");
    const first = await connect(initOptions, "first-tab");
    await initializeFollower(first.port, 1);
    const inspector = await openInspector(first.port, 2);

    const closed = first.port.waitForEvent((event) => event.type === "result" && event.id === 3);
    first.port.emitMessage({ type: "close", id: 3, releaseContext: true });
    await closed;
    await new Promise<void>((resolve) => setTimeout(resolve, 60));

    const pageStore = mocks.pageStores[0]!;
    const physicalOwnerAdmission = deferred<typeof pageStore>();
    mocks.openPageStore.mockImplementationOnce(() => physicalOwnerAdmission.promise);
    const leaseDbName = `${initOptions.dbName}-lease-root`;
    const lease = connectLease({
      type: "acquire-foreground-node-lease",
      dbName: leaseDbName,
      storageOwner: initOptions.storageOwner,
    });
    await vi.waitFor(() => expect(mocks.openPageStore).toHaveBeenCalledTimes(2));

    await expect(terminateInspector(inspector, 4)).rejects.toThrow(
      "Worker still has pending bootstrap operations",
    );
    expect((globalThis as WorkerGlobal).close).not.toHaveBeenCalled();

    physicalOwnerAdmission.resolve(pageStore);
    await expect(lease.outcome).resolves.toEqual(
      expect.objectContaining({ type: "foreground-node-lease-ready" }),
    );
    await expect(terminateInspector(inspector, 5)).rejects.toThrow(
      "Worker still has pending or active foreground node leases",
    );
    expect((globalThis as WorkerGlobal).close).not.toHaveBeenCalled();

    const durableRetirement = deferred<void>();
    pageStore.retireForegroundNodeLease.mockImplementationOnce(() => durableRetirement.promise);
    lease.port.emitMessage({ type: "retire-foreground-node-lease" });
    await vi.waitFor(() => expect(pageStore.retireForegroundNodeLease).toHaveBeenCalledOnce());
    await expect(terminateInspector(inspector, 6)).rejects.toThrow(
      "Worker still has pending or active foreground node leases",
    );
    expect((globalThis as WorkerGlobal).close).not.toHaveBeenCalled();

    durableRetirement.resolve();
    await vi.waitFor(() => expect(lease.port.close).toHaveBeenCalledOnce());

    await terminateInspector(inspector, 7);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect((globalThis as WorkerGlobal).close).toHaveBeenCalledOnce();
    inspector.close();
  });

  it("cancels an idle worker close when a successor bootstrap arrives during physical release", async () => {
    const releasePhysicalOwner = deferred<void>();
    mocks.openPageStore.mockImplementationOnce(async () => {
      const replicaNode = new Uint8Array(16);
      replicaNode.fill(17);
      const pageStore = {
        close: vi.fn(),
        claimBrowserWorkerEpoch: vi.fn(async () => undefined),
        releaseBrowserWorkerEpoch: vi.fn(() => releasePhysicalOwner.promise),
        onInvalidated: vi.fn(() => () => undefined),
        acquireForegroundNodeLease: vi.fn(async () => ({
          leaseId: "successor-lease",
          node: Uint8Array.from(replicaNode),
          confirmedTxTime: 0n,
        })),
        returnForegroundNodeLease: vi.fn(async () => undefined),
        retireForegroundNodeLease: vi.fn(async () => undefined),
        canonicalReplicaNode: replicaNode,
        get replicaNode() {
          return Uint8Array.from(replicaNode);
        },
      };
      mocks.pageStores.push(pageStore);
      return pageStore;
    });

    const initOptions = options("successor-during-idle-release");
    const first = await connect(initOptions, "first-tab");
    await initializeFollower(first.port, 1);
    const closed = first.port.waitForEvent((event) => event.type === "result" && event.id === 2);
    first.port.emitMessage({ type: "close", id: 2, releaseContext: true });
    await closed;

    // The idle timer has discarded the context and is now blocked on its
    // physical Web-Lock/epoch release. Start a *lease-only* successor in
    // this exact window: no RuntimeContext can yet exist, so this is a
    // direct receipt that the bootstrap reservation (rather than a context
    // map entry) fences a stale `finally(close)`.
    await new Promise<void>((resolve) => setTimeout(resolve, 60));
    await vi.waitFor(() =>
      expect(mocks.pageStores[0]?.releaseBrowserWorkerEpoch).toHaveBeenCalledOnce(),
    );
    const successor = connectLease({
      type: "acquire-foreground-node-lease",
      dbName: initOptions.dbName,
      storageOwner: initOptions.storageOwner,
    });
    expect(mocks.runtimes).toHaveLength(1);
    releasePhysicalOwner.resolve();

    await expect(successor.outcome).resolves.toEqual(
      expect.objectContaining({
        type: "foreground-node-lease-ready",
        confirmedTxTime: "0",
        node: expect.any(Uint8Array),
      }),
    );
    expect((globalThis as WorkerGlobal).close).not.toHaveBeenCalled();
  });

  it("retries only a classified external physical-owner conflict", async () => {
    const { BrowserPhysicalDatabaseBusyError } =
      await import("../runtime/browser-physical-database-epoch.js");
    const busy = new BrowserPhysicalDatabaseBusyError("busy-lease-root");
    mocks.openPageStore.mockRejectedValueOnce(busy).mockRejectedValueOnce(new Error("bad storage"));

    const busyLease = connectLease({
      type: "acquire-foreground-node-lease",
      dbName: "busy-lease-root",
      storageOwner: "owner",
    });
    await expect(busyLease.outcome).resolves.toEqual({
      type: "foreground-node-lease-busy",
      message: busy.message,
    });
    expect(busyLease.port.close).toHaveBeenCalledOnce();

    const permanentFailure = connectLease({
      type: "acquire-foreground-node-lease",
      dbName: "terminal-lease-root",
      storageOwner: "owner",
    });
    await expect(permanentFailure.outcome).resolves.toEqual({
      type: "foreground-node-lease-error",
      error: expect.objectContaining({
        name: "Error",
        message: "bad storage",
      }),
    });
    expect(permanentFailure.port.close).toHaveBeenCalledOnce();
  });

  it("acknowledges cancelled owner-admission rejection exactly once and releases retry cleanup", async () => {
    const admission = deferred<never>();
    mocks.openPageStore.mockImplementationOnce(() => admission.promise);
    const port = connectLeaseProbe().port;
    const cancellation = port.waitForLeaseCancellation();
    port.emitMessage({
      type: "acquire-foreground-node-lease",
      dbName: "cancelled-owner-admission",
      storageOwner: "owner",
    });
    await vi.waitFor(() => expect(mocks.openPageStore).toHaveBeenCalledOnce());

    // The page can send both its timeout cancellation and a terminal port
    // error while the same physical-owner admission is still pending. Neither
    // may leave the retained cleanup port open or publish a second receipt.
    port.emitMessage({ type: "cancel-foreground-node-lease" });
    port.emitMessage({ type: "cancel-foreground-node-lease" });
    admission.reject(new Error("IndexedDB admission rejected"));

    await expect(cancellation).resolves.toMatchObject({
      type: "foreground-node-lease-cancelled",
      error: expect.objectContaining({
        name: "Error",
        message: "IndexedDB admission rejected",
      }),
    });
    await vi.waitFor(() => expect(port.close).toHaveBeenCalledOnce());
    await Promise.resolve();
    expect(port.leaseCancellationCount()).toBe(0);
    expect(port.close).toHaveBeenCalledOnce();

    // The failed owner never reached a durable lease; a fresh bootstrap must
    // not inherit an in-memory owner or reservation from the cancelled port.
    const retry = connectLease({
      type: "acquire-foreground-node-lease",
      dbName: "cancelled-owner-admission",
      storageOwner: "owner",
    });
    await expect(retry.outcome).resolves.toMatchObject({
      type: "foreground-node-lease-ready",
    });
    retry.port.emitMessage({ type: "retire-foreground-node-lease" });
    await vi.waitFor(() => expect(retry.port.close).toHaveBeenCalledOnce());
  });

  it("does not expose retained lifecycle entries across browser auth scopes", async () => {
    const firstOptions = {
      ...options("lifecycle-scope-a"),
      authSessionKey: "scope-a",
      storageOwner: "scope-a-owner",
    };
    const secondOptions = {
      ...options("lifecycle-scope-b"),
      authSessionKey: "scope-b",
      storageOwner: "scope-b-owner",
    };
    const first = await connect(firstOptions, "scope-a-tab");
    await initializeFollower(first.port, 1);
    const second = await connect(secondOptions, "scope-b-tab");
    await initializeFollower(second.port, 2);

    const firstInspector = await openInspector(first.port, 3);
    const secondInspector = await openInspector(second.port, 4);
    try {
      const firstEntries = await readLifecycle(firstInspector, 5);
      const secondEntries = await readLifecycle(secondInspector, 6);

      // Both sets remain physically retained in the one SharedWorker realm.
      // A scope-B inspector must nevertheless receive only entries that were
      // recorded under scope B; this rules out cross-account diagnostics when
      // a browser turns over to a different authenticated namespace.
      expect(firstEntries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ event: "bootstrap-start", dbName: firstOptions.dbName }),
        ]),
      );
      expect(secondEntries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ event: "bootstrap-start", dbName: secondOptions.dbName }),
        ]),
      );
      expect(secondEntries).not.toEqual(
        expect.arrayContaining([expect.objectContaining({ dbName: firstOptions.dbName })]),
      );
    } finally {
      firstInspector.postMessage({ type: "close" } satisfies BrowserInspectorControlRequest);
      secondInspector.postMessage({ type: "close" } satisfies BrowserInspectorControlRequest);
      firstInspector.close();
      secondInspector.close();
    }
  });

  it("filters same-root turnover evidence by scope and omits unscoped owner events", async () => {
    const { filterWorkerLifecycleEntriesForInspector } =
      await import("./jazz-broker-worker-core.js");
    const dbName = "retained-turnover-root";
    const entries = [
      {
        sequence: 1,
        event: "bootstrap-start" as const,
        dbName,
        authSessionKey: "scope-a",
        peerCount: 1,
        pendingBootstraps: 0,
        activeLeases: 0,
      },
      {
        sequence: 2,
        event: "bootstrap-start" as const,
        dbName,
        authSessionKey: "scope-b",
        peerCount: 1,
        pendingBootstraps: 0,
        activeLeases: 0,
      },
      {
        sequence: 3,
        event: "lease-admitted" as const,
        dbName,
        authSessionKey: null,
        peerCount: 0,
        pendingBootstraps: 0,
        activeLeases: 1,
      },
      {
        sequence: 4,
        event: "owner-release-finished" as const,
        dbName,
        authSessionKey: null,
        peerCount: 0,
        pendingBootstraps: 0,
        activeLeases: 0,
      },
    ];

    // `allowedDbNames` intentionally admits the same root for both scopes:
    // physical-root filtering alone would leak scope A's retained trace to
    // scope B after an auth turnover. Scope-less lease/owner events are also
    // deliberately diagnostic-ineligible rather than guessed-attributed.
    expect(filterWorkerLifecycleEntriesForInspector(entries, "scope-b", new Set([dbName]))).toEqual(
      [
        {
          sequence: 2,
          event: "bootstrap-start",
          dbName,
          peerCount: 1,
          pendingBootstraps: 0,
          activeLeases: 0,
        },
      ],
    );
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
    expect((await connect(secondOptions, "second-tab")).outcome).toMatchObject({
      type: "runtime-error",
      error: {
        name: "Error",
        message:
          "incompatible WASM asset source for this SharedWorker; start a worker scoped to the new asset URL",
      },
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
    expect((await connect(secondOptions, "second-tab")).outcome).toMatchObject({
      type: "runtime-error",
      error: {
        name: "Error",
        message:
          "incompatible WASM asset source for this SharedWorker; start a worker scoped to the new asset URL",
      },
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledTimes(1);
  });

  it("evicts a context when telemetry installation fails so the same key can retry", async () => {
    mocks.installWasmTelemetry.mockImplementationOnce(() => {
      throw new Error("telemetry installation failed");
    });

    const failed = await connect(enabledTelemetryOptions("telemetry-failure"), "failed-tab");
    expect(failed.outcome).toMatchObject({
      type: "runtime-error",
      error: { name: "Error", message: "telemetry installation failed" },
    });
    expect(failed.port.close).toHaveBeenCalledOnce();
    // Durable root admission deliberately precedes all process-wide WASM and
    // telemetry work. A telemetry failure therefore closes the already
    // admitted handle before allowing a retry.
    expect(mocks.openPageStore).toHaveBeenCalledOnce();
    expect(mocks.pageStores[0]?.close).toHaveBeenCalledOnce();

    expect(
      (await connect(enabledTelemetryOptions("telemetry-failure"), "retry-tab")).outcome,
    ).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.loadWasmModule).toHaveBeenCalledOnce();
    expect(mocks.installWasmTelemetry).toHaveBeenCalledTimes(2);
  });

  it("rejects a conflicting page-store owner before WASM, telemetry, native open, or peer admission", async () => {
    // This is the exact admission oracle. If initialization moves *any* of
    // these operations before IndexedDbPageStore.open, the test fails.
    mocks.openPageStore.mockRejectedValueOnce(
      new Error(
        "IndexedDB database explicit-owner is already owned by a different Jazz browser session",
      ),
    );

    const failed = await connect(options("explicit-owner"), "blocked-tab");
    expect(failed.outcome).toMatchObject({
      type: "runtime-error",
      error: {
        name: "Error",
        message:
          "IndexedDB database explicit-owner is already owned by a different Jazz browser session",
      },
    });
    expect(failed.port.close).toHaveBeenCalledOnce();
    expect(mocks.openPageStore).toHaveBeenCalledOnce();
    expect(mocks.loadWasmModule).not.toHaveBeenCalled();
    expect(mocks.installWasmTelemetry).not.toHaveBeenCalled();
    expect(mocks.openConfig).not.toHaveBeenCalled();
    expect(mocks.openBrowser).not.toHaveBeenCalled();
    expect(mocks.openBrowserWithSelfSignedProof).not.toHaveBeenCalled();
    expect(mocks.fromDb).not.toHaveBeenCalled();
    expect(mocks.runtimes).toEqual([]);

    // `connectTab` owns and serializes this rejection to the port. Yielding a
    // turn catches a regression that instead leaves a detached promise
    // rejection after posting the operation-level error.
    await Promise.resolve();
    expect(failed.port.hasEvent((event) => event.type === "result")).toBe(false);
  });

  it("does not start WASM or telemetry when page-store opening fails and retries the same key", async () => {
    mocks.openPageStore.mockRejectedValueOnce(new Error("page-store open failed"));

    const failed = await connect(enabledTelemetryOptions("page-store-failure"), "failed-tab");
    expect(failed.outcome).toMatchObject({
      type: "runtime-error",
      error: { name: "Error", message: "page-store open failed" },
    });
    expect(mocks.loadWasmModule).not.toHaveBeenCalled();
    expect(mocks.installWasmTelemetry).not.toHaveBeenCalled();
    expect(mocks.openBrowser).not.toHaveBeenCalled();

    expect(
      (await connect(enabledTelemetryOptions("page-store-failure"), "retry-tab")).outcome,
    ).toEqual({
      type: "runtime-ready",
    });
    expect(mocks.telemetryDisposers).toHaveLength(1);
    expect(mocks.telemetryDisposers[0]).not.toHaveBeenCalled();
    expect(mocks.openPageStore).toHaveBeenCalledTimes(2);
  });

  it("passes the persisted replica node through its exact config to a normal WASM open", async () => {
    const initOptions = options("replica-node-config-normal-open");
    // The config is opaque WASM input. A distinct sentinel makes this an
    // identity check, rather than merely proving a value-shaped config opened.
    const config = Uint8Array.from([0xa1, 0xb2, 0xc3]);
    mocks.openConfig.mockReturnValueOnce(config);

    expect((await connect(initOptions, "normal-open-tab")).outcome).toEqual({
      type: "runtime-ready",
    });

    expect(mocks.openConfig.mock.calls[0]?.[0]).toEqual(mocks.pageStores[0]?.canonicalReplicaNode);
    expect(mocks.openConfig.mock.calls[0]?.[0]).not.toBe(mocks.pageStores[0]?.canonicalReplicaNode);
    expect(mocks.openConfig.mock.calls[0]?.slice(1)).toEqual([
      initOptions.author,
      1,
      false,
      initOptions.initialSyncFlushEvery,
      undefined,
    ]);
    expect(mocks.openBrowser.mock.calls[0]?.[0]).toBe(mocks.pageStores[0]);
    expect(mocks.openBrowser.mock.calls[0]?.[2]).toBe(config);
    // Only the worker's physical-owner admission may grant relay serving.
    // It carries the exact admitted owner into the host-only WASM open.
    expect(mocks.openBrowser.mock.calls[0]?.[3]).toBe(initOptions.storageOwner);
    // NativeRuntimeAdapter is the final runtime boundary. It must receive the
    // persisted physical-replica identity, not a process constant or a node
    // intended for a different open attempt.
    expect(mocks.fromDb.mock.calls[0]?.[2]).toEqual(mocks.pageStores[0]?.canonicalReplicaNode);
    expect(mocks.fromDb.mock.calls[0]?.[2]).not.toBe(mocks.pageStores[0]?.canonicalReplicaNode);
  });

  it("closes the page store and telemetry when browser DB opening fails, then retries", async () => {
    const rejectedConfig = Uint8Array.from([0xd4]);
    const retryConfig = Uint8Array.from([0xe5]);
    mocks.openConfig.mockReturnValueOnce(rejectedConfig).mockReturnValueOnce(retryConfig);
    mocks.openBrowser.mockRejectedValueOnce(new Error("browser DB open failed"));

    const failed = await connect(enabledTelemetryOptions("browser-db-failure"), "failed-tab");
    expect(failed.outcome).toMatchObject({
      type: "runtime-error",
      error: { name: "Error", message: "browser DB open failed" },
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
    // A failed WASM open evicts its context and retries from a newly opened
    // page-store handle. Each attempt must derive its opaque config from that
    // attempt's admitted physical-replica identity.
    expect(mocks.openConfig.mock.calls[0]?.[0]).toEqual(mocks.pageStores[0]?.canonicalReplicaNode);
    expect(mocks.openConfig.mock.calls[1]?.[0]).toEqual(mocks.pageStores[1]?.canonicalReplicaNode);
    expect(mocks.openBrowser.mock.calls[0]?.[2]).toBe(rejectedConfig);
    expect(mocks.openBrowser.mock.calls[1]?.[2]).toBe(retryConfig);
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
      initOptions.storageOwner,
    );
    expect(mocks.fromDb.mock.calls[0]?.[2]).toEqual(mocks.pageStores[0]?.canonicalReplicaNode);
  });

  it("closes an unowned browser DB when adapter construction fails, cleans up, and retries", async () => {
    const rawDb = mocks.createBrowserDb();
    mocks.openBrowser.mockResolvedValueOnce(rawDb);
    const rejectedConfig = Uint8Array.from([0xd4]);
    const retryConfig = Uint8Array.from([0xe5]);
    mocks.openConfig.mockReturnValueOnce(rejectedConfig).mockReturnValueOnce(retryConfig);
    mocks.fromDb.mockImplementationOnce(() => {
      throw new Error("adapter construction failed");
    });

    const failed = await connect(enabledTelemetryOptions("adapter-failure"), "failed-tab");
    expect(failed.outcome).toMatchObject({
      type: "runtime-error",
      error: { name: "Error", message: "adapter construction failed" },
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
    // The failed context has a different persisted node and config from its
    // retry. Verify both native opens and both adapter constructions retain
    // their own pairing rather than silently substituting a constant node.
    expect(mocks.pageStores[0]?.canonicalReplicaNode).not.toEqual(
      mocks.pageStores[1]?.canonicalReplicaNode,
    );
    expect(mocks.openBrowser.mock.calls[0]?.[2]).toBe(rejectedConfig);
    expect(mocks.openBrowser.mock.calls[1]?.[2]).toBe(retryConfig);
    expect(mocks.fromDb.mock.calls[0]?.[2]).toEqual(mocks.pageStores[0]?.canonicalReplicaNode);
    expect(mocks.fromDb.mock.calls[1]?.[2]).toEqual(mocks.pageStores[1]?.canonicalReplicaNode);
    expect(mocks.fromDb.mock.calls[0]?.[2]).not.toEqual(mocks.fromDb.mock.calls[1]?.[2]);
  });

  it("shares one successful initialization between concurrent connections for the same key", async () => {
    const browserDb = deferred<ReturnType<typeof mocks.createBrowserDb>>();
    mocks.openBrowser.mockReturnValueOnce(browserDb.promise);
    const exactStorageOwner =
      '{"version":1,"appId":"worker-initialization-test","env":"dev","auth":{"kind":"principal","authMode":"external","user":"[\\"https://issuer.example\\",\\"alice\\"]"}}';
    const initOptions = {
      ...options("concurrent-success"),
      storageOwner: exactStorageOwner,
    };

    const first = connect(initOptions, "first-tab");
    const second = connect(initOptions, "second-tab");
    await vi.waitFor(() => expect(mocks.openBrowser).toHaveBeenCalledOnce());
    browserDb.resolve(mocks.createBrowserDb());

    expect((await first).outcome).toEqual({ type: "runtime-ready" });
    expect((await second).outcome).toEqual({ type: "runtime-ready" });
    expect(mocks.loadWasmModule).toHaveBeenCalledOnce();
    expect(mocks.installWasmTelemetry).toHaveBeenCalledOnce();
    expect(mocks.openPageStore).toHaveBeenCalledOnce();
    expect(mocks.openPageStore).toHaveBeenCalledWith("concurrent-success", {
      // This exact caller-supplied marker is the durable admission boundary.
      // Mutating production wiring to `owner: undefined` or a lossy surrogate
      // makes this mock receipt fail before a worker can open WASM.
      owner: exactStorageOwner,
    });
    expect(mocks.openBrowser).toHaveBeenCalledOnce();
    expect(mocks.fromDb).toHaveBeenCalledOnce();
  });

  it("rejects a tab whose policy claims differ from the worker upstream session", async () => {
    const initOptions = {
      ...options("single-upstream-claims"),
      sessionClaims: { role: "reader", teams: ["band-a"] },
    };
    const first = await connect(initOptions, "first-tab");
    const second = await connect(initOptions, "second-tab");
    const firstResult = first.port.waitForEvent(
      (event) => event.type === "result" && event.id === 1,
    );
    first.port.emitMessage({
      type: "init",
      id: 1,
      // Deliberately reorder keys: admission compares claim semantics rather
      // than the transport's incidental object insertion order.
      sessionClaims: { teams: ["band-a"], role: "reader" },
    });
    await firstResult;

    const secondResult = second.port.waitForEvent(
      (event) => event.type === "result" && event.id === 2,
    );
    second.port.emitMessage({
      type: "init",
      id: 2,
      sessionClaims: { role: "writer", teams: ["band-a"] },
    });
    await expect(secondResult).resolves.toMatchObject({
      type: "result",
      id: 2,
      error: {
        message:
          "Browser tab claims differ from the persistent worker's authenticated upstream session",
      },
    });
    expect(mocks.runtimes[0]?.acceptPeerWhenIdle).toHaveBeenCalledOnce();
  });

  it("keeps every tab attached after auth rejection so a sibling can refresh the session", async () => {
    const initOptions = {
      ...options("auth-recovery-owner"),
      serverUrl: "ws://authority.example",
    };
    const first = await connect(initOptions, "first-tab");
    const second = await connect(initOptions, "second-tab");
    await initializeFollower(first.port, 1);
    await initializeFollower(second.port, 2);

    const runtime = mocks.runtimes[0]!;
    const publishAuthFailure = runtime.onAuthFailure.mock.calls[0]![0] as (reason: string) => void;
    runtime.updateAuth.mockImplementationOnce(async () => {
      publishAuthFailure("invalid");
      throw new Error("authentication rejected");
    });
    const firstFailure = first.port.waitForEvent((event) => event.type === "auth-failure");
    const secondFailure = second.port.waitForEvent((event) => event.type === "auth-failure");
    first.port.emitMessage({ type: "update-auth", authJson: "invalid", sessionClaims: {} });
    await Promise.all([firstFailure, secondFailure]);

    expect(first.port.close).not.toHaveBeenCalled();
    expect(second.port.close).not.toHaveBeenCalled();

    runtime.updateAuth.mockResolvedValueOnce(undefined);
    const firstRestored = first.port.waitForEvent((event) => event.type === "auth-restored");
    const secondRestored = second.port.waitForEvent((event) => event.type === "auth-restored");
    second.port.emitMessage({ type: "update-auth", authJson: "valid", sessionClaims: {} });
    await Promise.all([firstRestored, secondRestored]);

    expect(runtime.updateAuth).toHaveBeenCalledTimes(2);
    expect(first.port.close).not.toHaveBeenCalled();
    expect(second.port.close).not.toHaveBeenCalled();
  });

  it("does not mistake an earlier queued auth rejection for a later runtime failure", async () => {
    const initOptions = { ...options("queued-auth-recovery"), serverUrl: "ws://authority.example" };
    const first = await connect(initOptions, "first-tab");
    const second = await connect(initOptions, "second-tab");
    await initializeFollower(first.port, 1);
    await initializeFollower(second.port, 2);
    const runtime = mocks.runtimes[0]!;
    const firstAttempt = deferred<void>();
    runtime.updateAuth.mockImplementationOnce(() => firstAttempt.promise);
    runtime.updateAuth.mockRejectedValueOnce(new Error("unrelated runtime failure"));
    first.port.emitMessage({ type: "update-auth", authJson: "invalid", sessionClaims: {} });
    await vi.waitFor(() => expect(runtime.updateAuth).toHaveBeenCalledTimes(1));
    const secondError = second.port.waitForEvent((event) => event.type === "error");
    second.port.emitMessage({ type: "update-auth", authJson: "replacement", sessionClaims: {} });

    const publishAuthFailure = runtime.onAuthFailure.mock.calls[0]![0] as (reason: string) => void;
    publishAuthFailure("invalid");
    firstAttempt.reject(new Error("authentication rejected"));
    await expect(secondError).resolves.toMatchObject({
      type: "error",
      error: { message: "unrelated runtime failure" },
    });
    expect(first.port.close).not.toHaveBeenCalled();
    expect(second.port.close).toHaveBeenCalledOnce();
  });

  it("rejects a second context with a different owner before it can reuse a live physical root", async () => {
    const alice = {
      ...options("shared-physical-root"),
      storageOwner: "owner:alice",
      authSessionKey: "session:alice",
    };
    const bob = {
      ...options("shared-physical-root"),
      storageOwner: "owner:bob",
      authSessionKey: "session:bob",
    };

    expect((await connect(alice, "alice-tab")).outcome).toEqual({ type: "runtime-ready" });

    // Planted positive: omitting the in-memory physical-owner comparison
    // reuses Alice's already-open page store and admits Bob without calling
    // IndexedDbPageStore.open, so this must fail before the fix.
    expect((await connect(bob, "bob-tab")).outcome).toMatchObject({
      type: "runtime-error",
      error: {
        name: "Error",
        message:
          "IndexedDB database shared-physical-root is already owned by a different Jazz browser session; choose a different driver.dbName or reset this database before changing accounts",
      },
    });
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

  it("publishes reconnected claims to every tab only after upstream admission", async () => {
    const initOptions = { ...options("reconnect-shared-claims"), serverUrl: "ws://server.test" };
    const owner = await connect(initOptions, "owner-tab");
    const editor = await connect(initOptions, "editor-tab");
    const runtime = mocks.runtimes[0]!;
    const subscriber = () => ({
      setAuxiliaryTraceEnabled: vi.fn(),
      setOutboundScheduler: vi.fn(),
      clearOutboundScheduler: vi.fn(),
      recvWireFrames: vi.fn(() => []),
      sendWireFrame: vi.fn(),
      updateAuthenticatedClaims: vi.fn(async () => undefined),
    });
    const ownerSubscriber = subscriber();
    const editorSubscriber = subscriber();
    runtime.acceptPeerWhenIdle
      .mockResolvedValueOnce(ownerSubscriber)
      .mockResolvedValueOnce(editorSubscriber);
    await initializeFollower(owner.port, 1);
    await initializeFollower(editor.port, 1);

    const upstreamAdmission = deferred<void>();
    runtime.connect.mockClear();
    runtime.waitForUpstreamServerConnection.mockImplementationOnce(() => upstreamAdmission.promise);
    const result = owner.port.waitForEvent((event) => event.type === "result" && event.id === 2);
    owner.port.emitMessage({
      type: "reconnect",
      id: 2,
      authJson: '{"jwt_token":"fresh"}',
      sessionClaims: { role: "writer" },
    });
    await vi.waitFor(() => expect(runtime.connect).toHaveBeenCalledOnce());
    expect(ownerSubscriber.updateAuthenticatedClaims).not.toHaveBeenCalled();
    expect(editorSubscriber.updateAuthenticatedClaims).not.toHaveBeenCalled();

    upstreamAdmission.resolve();
    await result;
    for (const attached of [ownerSubscriber, editorSubscriber]) {
      expect(attached.updateAuthenticatedClaims).toHaveBeenCalledExactlyOnceWith({
        role: "writer",
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

  it("relays a terminal server error only to active followers and never replays it", async () => {
    const initOptions = { ...options("terminal-error-continuity"), serverUrl: "ws://server.test" };
    const owner = await connect(initOptions, "owner-tab");
    const successor = await connect(initOptions, "successor-tab");
    await initializeFollower(owner.port, 1);
    await initializeFollower(successor.port, 1);

    const late = await connect(initOptions, "late-tab");
    const runtime = mocks.runtimes[0]!;
    const callback = runtime.onServerTransportError.mock.calls[0]?.[0] as
      | ((error: Error) => void)
      | undefined;
    expect(callback).toBeTypeOf("function");
    const terminalError = new Error("Protocol: terminal maintained view failure");
    callback?.(terminalError);

    for (const port of [owner.port, successor.port]) {
      await expect(
        port.waitForEvent((event) => event.type === "transport-error"),
      ).resolves.toMatchObject({
        type: "transport-error",
        error: {
          name: "Error",
          message: terminalError.message,
          stack: terminalError.stack,
        },
      });
    }
    expect(late.port.hasEvent((event) => event.type === "transport-error")).toBe(false);

    owner.port.emitMessage({
      type: "reconnect",
      id: 2,
      authJson: "{}",
      sessionClaims: {},
    });
    await owner.port.waitForEvent((event) => event.type === "result" && event.id === 2);
    await initializeFollower(late.port, 1);
    // A reconnect/admission completed after the terminal observation starts a
    // successor runtime, not a recipient for an old foreground error.
    await Promise.resolve();
    expect(late.port.hasEvent((event) => event.type === "transport-error")).toBe(false);
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
