import { describe, expect, it, vi } from "vitest";
import type { JazzClient } from "../client.js";
import { SharedBrowserForegroundNodeLease } from "../native-runtime/browser-shared-worker-connection.js";
import { BrowserWorkerUnresponsiveError } from "../native-runtime/browser-worker-protocol.js";
import type { BrowserWorkerConnection, BrowserWorkerConnectionContext } from "../runtime-source.js";
import { BrowserConnectionManager } from "./browser-connection-manager.js";
import type { DbForConnection } from "./types.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

async function foregroundLeaseFixture() {
  const messages: string[] = [];
  const leaseEvents = new EventTarget();
  const emit = (data: unknown) => leaseEvents.dispatchEvent(new MessageEvent("message", { data }));
  const finishStarted = deferred();
  let acknowledgeFinish = false;
  let liveClientAtRetirement: boolean | undefined;
  let clientLive = false;
  const port = Object.assign(leaseEvents, {
    start() {},
    close: vi.fn(),
    postMessage(message: { type: string; attemptId?: string }) {
      messages.push(message.type);
      if (message.type === "probe-foreground-node-lease-worker") {
        emit({ type: "foreground-node-lease-worker-alive", attemptId: message.attemptId });
      } else if (message.type === "acquire-foreground-node-lease") {
        emit({
          type: "foreground-node-lease-ready",
          leaseId: "00000000-0000-4000-8000-000000000002",
          node: new Uint8Array(16),
          confirmedTxTime: "0",
        });
      } else {
        if (message.type === "retire-foreground-node-lease") liveClientAtRetirement = clientLive;
        finishStarted.resolve();
        if (acknowledgeFinish) emit({ type: "foreground-node-lease-result" });
      }
    },
  });
  const lease = await SharedBrowserForegroundNodeLease.acquireFromPort(
    port as unknown as MessagePort,
    { dbName: "terminal-lease-controls", storageOwner: "owner" },
  );
  return {
    lease,
    port,
    messages,
    finishStarted,
    acknowledge: () => emit({ type: "foreground-node-lease-result" }),
    allowFinish: () => {
      acknowledgeFinish = true;
    },
    setClientLive: (live: boolean) => {
      clientLive = live;
    },
    liveClientAtRetirement: () => liveClientAtRetirement,
  };
}

async function leasedManagerFixture(admissionError?: Error, inspectorAttachment = false) {
  const leaseFixture = await foregroundLeaseFixture();
  const { lease } = leaseFixture;
  const connection: BrowserWorkerConnection = {
    ready: vi.fn(async () => {
      if (admissionError) throw admissionError;
    }),
    waitForServerConnection: vi.fn(async () => undefined),
    waitForPendingWrites: vi.fn(async () => undefined),
    updateAuth: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    reconnect: vi.fn(async () => undefined),
    deleteStorage: vi.fn(async () => undefined),
    flushLocal: vi.fn(async () => undefined),
    openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    shutdown: vi.fn(async () => undefined),
  };
  const client = {
    onMutationError: vi.fn(),
    getRuntime: () => undefined,
    discard: vi.fn(() => {
      leaseFixture.setClientLive(false);
    }),
  } as unknown as JazzClient;
  let onFailure!: BrowserWorkerConnectionContext["onFailure"];
  const contexts: BrowserWorkerConnectionContext[] = [];
  const disposeTelemetry = vi.fn();
  const createClient = vi.fn(() => {
    leaseFixture.setClientLive(true);
    return client;
  });
  const createConnection = vi.fn((context: BrowserWorkerConnectionContext) => {
    contexts.push(context);
    onFailure = context.onFailure;
    return { ...connection };
  });
  const host = {
    config: {
      serverUrl: "https://example.test",
      telemetryCollectorUrl: "https://example.test/telemetry",
      runtimeSources: inspectorAttachment
        ? {
            inspectorBinding: {
              appId: "app",
              physicalDbName: "root",
              authSessionKey: "session",
              storageOwner: "owner",
            },
          }
        : undefined,
    },
    isShuttingDown: false,
    runtimeSource: {
      acquireBrowserForegroundNodeLease: vi.fn(async () => lease),
      createClient,
      installTelemetry: () => disposeTelemetry,
      createBrowserWorkerConnection: createConnection,
    },
    markUnauthenticated: vi.fn(),
    clearAuthError: vi.fn(),
    onMutationError: vi.fn(),
    clearAuthenticatedInspectorLocalReads: vi.fn(),
  };
  const manager = new BrowserConnectionManager(host as unknown as DbForConnection);
  await manager.start();
  manager.getClient({});
  if (!admissionError) await manager.ensureReady("local");
  return {
    manager,
    host,
    ...leaseFixture,
    contexts,
    connection,
    client,
    disposeTelemetry,
    createClient,
    createConnection,
    fail: (error: Error) => onFailure(error),
  };
}

describe("BrowserConnectionManager acknowledged storage reset", () => {
  it.each([false, true])(
    "cancels an obsolete pending lease finish without losing an earlier flush error (%s)",
    async (flushFailed) => {
      vi.useFakeTimers();
      const fixture = await leasedManagerFixture();
      const flushError = new Error("independent flush failure");
      if (flushFailed) vi.mocked(fixture.connection.flushLocal).mockRejectedValue(flushError);
      fixture.host.isShuttingDown = true;
      let outcome: unknown = "pending";
      const shutdown = fixture.manager.shutdown().then(
        () => {
          outcome = "fulfilled";
        },
        (error) => {
          outcome = error;
        },
      );
      try {
        await fixture.finishStarted.promise;
        const terminalMessages = fixture.messages.slice();
        fixture.contexts[0]!.onStorageReset?.(1);
        fixture.contexts[0]!.onStorageReset?.(1);
        await vi.advanceTimersByTimeAsync(0);
        expect(outcome).toBe(flushFailed ? flushError : "fulfilled");
        expect(fixture.port.close).toHaveBeenCalledOnce();
        expect(fixture.messages).toEqual(terminalMessages);
        fixture.acknowledge();
        await vi.advanceTimersByTimeAsync(0);
        expect(outcome).toBe(flushFailed ? flushError : "fulfilled");
        await expect(fixture.lease.retire()).rejects.toThrow(/reset/i);
        expect(fixture.port.close).toHaveBeenCalledOnce();
      } finally {
        fixture.acknowledge();
        await shutdown;
        vi.useRealTimers();
      }
    },
  );

  it("waits for a new lease before same-Db reuse and ignores an old reset callback", async () => {
    vi.useFakeTimers();
    const fixture = await leasedManagerFixture();
    const successor = await foregroundLeaseFixture();
    successor.allowFinish();
    const acquire = deferred();
    fixture.host.runtimeSource.acquireBrowserForegroundNodeLease.mockImplementation(async () => {
      await acquire.promise;
      return successor.lease;
    });
    fixture.contexts[0]!.onStorageReset?.(1);
    let resetFinished = false;
    const reset = fixture.manager.deleteClientStorage().then(() => {
      resetFinished = true;
    });
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(resetFinished).toBe(false);
      expect(() => fixture.manager.getClient({})).toThrow(/reset/i);
      acquire.resolve();
      await reset;
      fixture.manager.getClient({});
      await fixture.manager.ensureReady("local");
      const discards = vi.mocked(fixture.client.discard).mock.calls.length;
      fixture.contexts[0]!.onStorageReset?.(2);
      await vi.advanceTimersByTimeAsync(0);
      expect(fixture.client.discard).toHaveBeenCalledTimes(discards);
      await fixture.manager.shutdown();
      expect(fixture.messages).not.toContain("return-foreground-node-lease");
      expect(successor.messages).toContain("return-foreground-node-lease");
      expect(successor.port.close).toHaveBeenCalledOnce();
    } finally {
      acquire.resolve();
      fixture.allowFinish();
      fixture.acknowledge();
      successor.acknowledge();
      await reset;
      await fixture.manager.shutdown();
      vi.useRealTimers();
    }
  });

  it("retires an acquired but unpublished successor when shutdown wins", async () => {
    vi.useFakeTimers();
    const fixture = await leasedManagerFixture();
    const successor = await foregroundLeaseFixture();
    successor.allowFinish();
    const acquire = deferred();
    fixture.host.runtimeSource.acquireBrowserForegroundNodeLease.mockImplementation(async () => {
      await acquire.promise;
      return successor.lease;
    });
    fixture.contexts[0]!.onStorageReset?.(1);
    await vi.advanceTimersByTimeAsync(0);
    let outcome = "pending";
    const shutdown = fixture.manager.shutdown().then(() => {
      outcome = "fulfilled";
    });
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(outcome).toBe("pending");
      acquire.resolve();
      await vi.advanceTimersByTimeAsync(0);
      expect(outcome).toBe("fulfilled");
      expect(successor.messages).toContain("retire-foreground-node-lease");
      expect(successor.messages).not.toContain("return-foreground-node-lease");
      expect(successor.port.close).toHaveBeenCalledOnce();
      expect(fixture.messages).not.toContain("return-foreground-node-lease");
      expect(() => fixture.manager.getClient({})).toThrow("shut down");
    } finally {
      acquire.resolve();
      fixture.acknowledge();
      successor.acknowledge();
      await shutdown;
      vi.useRealTimers();
    }
  });
  it("requires a fresh Inspector attachment after an acknowledged reset", async () => {
    const fixture = await leasedManagerFixture(undefined, true);
    fixture.contexts[0]!.onStorageReset?.(1);
    await fixture.manager.deleteClientStorage();
    let resetError: unknown;
    try {
      fixture.manager.getClient({});
    } catch (error) {
      resetError = error;
    }
    expect(resetError).toBeInstanceOf(Error);
    await expect(fixture.manager.ensureReady("local")).rejects.toBe(resetError);
    expect(fixture.host.runtimeSource.acquireBrowserForegroundNodeLease).toHaveBeenCalledOnce();
    expect(fixture.port.close).toHaveBeenCalledOnce();
    fixture.host.isShuttingDown = true;
    await fixture.manager.shutdown();
    expect(fixture.messages).not.toContain("return-foreground-node-lease");
  });
});

function admissionManager(retryable = true) {
  let pinned = true;
  const error = new Error("incompatible persistent browser configuration");
  const connections: Array<BrowserWorkerConnection & { shutdown: ReturnType<typeof vi.fn> }> = [];
  const host = {
    config: {},
    isShuttingDown: false,
    runtimeSource: {
      createBrowserWorkerConnection: vi.fn(() => {
        const rejected = pinned;
        const connection = {
          ready: async () => {
            if (rejected) throw error;
          },
          canRetryInitialConfigurationAdmission: () => rejected && retryable,
          shutdown: vi.fn(async () => undefined),
        } as unknown as BrowserWorkerConnection & { shutdown: ReturnType<typeof vi.fn> };
        connections.push(connection);
        return connection;
      }),
    },
    clearAuthenticatedInspectorLocalReads: vi.fn(),
  };
  const manager = new BrowserConnectionManager(host as unknown as DbForConnection);
  (manager as unknown as { onClientCreated(input: unknown): void }).onClientCreated({
    schemaKey: "empty",
    schema: {},
    client: {} as JazzClient,
  });
  return {
    manager,
    host,
    connections,
    error,
    unpin: () => {
      pinned = false;
    },
  };
}

describe("Browser configuration admission retries", () => {
  it("rejects each attempt visibly and only reattaches on a later explicit call", async () => {
    const { manager, connections, error, unpin } = admissionManager();
    const first = manager.ensureReady("local");
    const concurrent = manager.ensureReady("local");
    await expect(first).rejects.toBe(error);
    await expect(concurrent).rejects.toBe(error);
    expect(connections).toHaveLength(1);
    await expect(manager.ensureReady("local")).rejects.toBe(error);
    expect(connections).toHaveLength(2);
    expect(connections[0]!.shutdown).toHaveBeenCalledOnce();
    unpin();
    await Promise.resolve();
    expect(connections).toHaveLength(2);
    await expect(manager.ensureReady("local")).resolves.toBeUndefined();
    expect(connections).toHaveLength(3);
    expect(connections[1]!.shutdown).toHaveBeenCalledOnce();
  });

  it("does not retry other initial failures even with matching error text", async () => {
    const { manager, connections, error } = admissionManager(false);
    await expect(manager.ensureReady("local")).rejects.toBe(error);
    await expect(manager.ensureReady("local")).rejects.toBe(error);
    expect(connections).toHaveLength(1);
  });

  it("shares one candidate and cannot reopen while shutdown begins during retirement", async () => {
    const { manager, host, connections, error } = admissionManager();
    await expect(manager.ensureReady("local")).rejects.toBe(error);
    const retirement = deferred();
    connections[0]!.shutdown.mockImplementation(() => retirement.promise);
    const retry = manager.ensureReady("local");
    const concurrent = manager.ensureReady("local");
    await Promise.resolve();
    expect(connections[0]!.shutdown).toHaveBeenCalledOnce();
    host.isShuttingDown = true;
    retirement.resolve();
    await Promise.all([retry, concurrent]);
    expect(connections).toHaveLength(1);
  });
});

describe("BrowserConnectionManager.shutdown", () => {
  it("preserves a lease persistence failure while completing fallback and remaining teardown", async () => {
    const fixture = await leasedManagerFixture();
    vi.mocked(fixture.connection.shutdown).mockRejectedValue(
      new Error("later worker close failure"),
    );
    fixture.host.isShuttingDown = true;
    const shutdown = fixture.manager.shutdown();
    const rejected = expect(shutdown).rejects.toThrow("high-water persistence failed");
    await fixture.finishStarted.promise;
    fixture.port.dispatchEvent(
      new MessageEvent("message", {
        data: {
          type: "foreground-node-lease-result",
          error: { name: "Error", message: "high-water persistence failed" },
        },
      }),
    );
    await rejected;
    expect(fixture.client.discard).toHaveBeenCalledOnce();
    expect(fixture.disposeTelemetry).toHaveBeenCalledOnce();
    expect(fixture.connection.shutdown).toHaveBeenCalledOnce();
    expect(fixture.port.close).toHaveBeenCalledOnce();
  });

  it.each(["established follower", "initialization"] as const)(
    "abandons a latched %s failure only after disabling the foreground lifetime",
    async (phase) => {
      const error = new BrowserWorkerUnresponsiveError("worker stopped responding");
      const fixture = await leasedManagerFixture(phase === "initialization" ? error : undefined);
      if (phase === "established follower") fixture.fail(error);
      await expect(fixture.manager.ensureReady("local")).rejects.toBe(error);
      expect(fixture.client.discard).not.toHaveBeenCalled();
      expect(fixture.messages).not.toContain("retire-foreground-node-lease");
      fixture.host.isShuttingDown = true;
      let outcome: unknown = "pending";
      const shutdown = fixture.manager.shutdown().then(
        () => {
          outcome = "fulfilled";
        },
        (failure) => {
          outcome = failure;
        },
      );
      vi.useFakeTimers();
      try {
        await vi.advanceTimersByTimeAsync(0);
        expect(outcome).toBe(error);
        expect(fixture.liveClientAtRetirement()).toBe(false);
        expect(fixture.connection.flushLocal).not.toHaveBeenCalled();
        expect(fixture.connection.shutdown).toHaveBeenCalledOnce();
        expect(fixture.disposeTelemetry).toHaveBeenCalledOnce();
        expect(fixture.messages).not.toContain("return-foreground-node-lease");
        expect(() => fixture.manager.getClient({})).toThrow("shut down");
        expect(fixture.createClient).toHaveBeenCalledOnce();
        expect(fixture.port.close).not.toHaveBeenCalled();
      } finally {
        fixture.acknowledge();
        await shutdown;
        vi.useRealTimers();
      }
    },
  );

  it("interrupts an in-flight lease finish when the shutdown connection fails", async () => {
    const fixture = await leasedManagerFixture();
    const error = new BrowserWorkerUnresponsiveError("worker died during lease cleanup");
    fixture.host.isShuttingDown = true;
    let outcome: unknown = "pending";
    const shutdown = fixture.manager.shutdown().then(
      () => {
        outcome = "fulfilled";
      },
      (failure) => {
        outcome = failure;
      },
    );
    vi.useFakeTimers();
    try {
      await fixture.finishStarted.promise;
      expect(fixture.messages).toContain("return-foreground-node-lease");
      fixture.fail(error);
      fixture.fail(new BrowserWorkerUnresponsiveError("duplicate failure"));
      await vi.advanceTimersByTimeAsync(0);
      expect(outcome).toBe(error);
      expect(fixture.messages.filter((type) => type.endsWith("-foreground-node-lease"))).toEqual([
        "acquire-foreground-node-lease",
        "return-foreground-node-lease",
      ]);
      expect(fixture.client.discard).toHaveBeenCalledOnce();
      expect(fixture.disposeTelemetry).toHaveBeenCalledOnce();
      expect(fixture.connection.shutdown).toHaveBeenCalledOnce();
      expect(fixture.port.close).not.toHaveBeenCalled();
      await expect(fixture.lease.returnWithHighWater(0n)).rejects.toBe(error);
      await expect(fixture.lease.retire()).rejects.toBe(error);
    } finally {
      fixture.acknowledge();
      await shutdown;
      vi.useRealTimers();
    }
    expect(fixture.port.close).toHaveBeenCalledOnce();
  });

  it("keeps the same foreground lease live across an explicit follower reconnect", async () => {
    const fixture = await leasedManagerFixture();
    fixture.fail(new BrowserWorkerUnresponsiveError("replace only this follower"));
    await fixture.manager.reconnect();
    expect(fixture.createConnection).toHaveBeenCalledTimes(2);
    expect(fixture.manager.getClient({})).toBe(fixture.client);
    expect(fixture.createClient).toHaveBeenCalledOnce();
    expect(fixture.client.discard).not.toHaveBeenCalled();
    expect(fixture.messages).not.toContain("retire-foreground-node-lease");
    fixture.allowFinish();
    fixture.host.isShuttingDown = true;
    await fixture.manager.shutdown();
    expect(fixture.messages).toContain("return-foreground-node-lease");
    expect(fixture.messages).not.toContain("retire-foreground-node-lease");
  });

  it("returns the lease normally after a configuration error even with a lifecycle error name", async () => {
    const error = new Error("incompatible persistent browser configuration");
    error.name = "BrowserWorkerUnresponsiveError";
    const fixture = await leasedManagerFixture(error);
    await expect(fixture.manager.ensureReady("local")).rejects.toBe(error);
    fixture.allowFinish();
    fixture.host.isShuttingDown = true;
    await fixture.manager.shutdown();
    expect(fixture.connection.flushLocal).not.toHaveBeenCalled();
    expect(fixture.messages).toContain("return-foreground-node-lease");
    expect(fixture.messages).not.toContain("retire-foreground-node-lease");
    expect(fixture.client.discard).toHaveBeenCalledOnce();
  });

  it("awaits retirement after a responsive flush error before completing teardown", async () => {
    const fixture = await leasedManagerFixture();
    const flushError = new Error("local settlement failed");
    vi.mocked(fixture.connection.flushLocal).mockRejectedValue(flushError);
    const getRuntime = vi.spyOn(fixture.client, "getRuntime").mockImplementation(() => {
      throw new Error("foreground runtime cannot capture its high-water");
    });
    vi.mocked(fixture.connection.shutdown).mockRejectedValue(new Error("later close failure"));
    const abandon = vi.spyOn(fixture.lease, "abandonAfterWorkerFailure");
    fixture.host.isShuttingDown = true;
    let outcome: unknown = "pending";
    const shutdown = fixture.manager.shutdown().then(
      () => {
        outcome = "fulfilled";
      },
      (failure) => {
        outcome = failure;
      },
    );
    vi.useFakeTimers();
    try {
      // Drain without awaiting shutdown: premature abandonment must fail here,
      // rather than a withheld retirement result timing out the test.
      await vi.advanceTimersByTimeAsync(0);
      expect(outcome).toBe("pending");
      expect(abandon).not.toHaveBeenCalled();
      expect(fixture.messages.filter((type) => type.endsWith("-foreground-node-lease"))).toEqual([
        "acquire-foreground-node-lease",
        "retire-foreground-node-lease",
      ]);
      expect(fixture.client.discard).not.toHaveBeenCalled();
      expect(fixture.connection.shutdown).not.toHaveBeenCalled();
      expect(fixture.disposeTelemetry).not.toHaveBeenCalled();
      expect(fixture.port.close).not.toHaveBeenCalled();

      fixture.acknowledge();
      await shutdown;
      expect(outcome).toBe(flushError);
      expect(abandon).not.toHaveBeenCalled();
      expect(fixture.port.close).toHaveBeenCalledOnce();
      expect(fixture.client.discard).toHaveBeenCalledOnce();
      expect(fixture.disposeTelemetry).toHaveBeenCalledOnce();
      expect(fixture.connection.shutdown).toHaveBeenCalledOnce();
    } finally {
      // Always release the real lease adapter after observing the held result.
      fixture.allowFinish();
      fixture.acknowledge();
      await shutdown;
      getRuntime.mockRestore();
      abandon.mockRestore();
      vi.useRealTimers();
    }
  });

  it("continues teardown after flush fails and preserves the flush error", async () => {
    const flushError = new Error("flush failed");
    const workerShutdownError = new Error("worker shutdown failed");
    const connection: BrowserWorkerConnection = {
      ready: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      waitForPendingWrites: vi.fn(async () => undefined),
      updateAuth: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      deleteStorage: vi.fn(async () => undefined),
      flushLocal: vi.fn(async () => {
        throw flushError;
      }),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      shutdown: vi.fn(async () => {
        throw workerShutdownError;
      }),
    };
    const client = {
      discard: vi.fn(),
    } as unknown as JazzClient;
    const disposeRuntimeTelemetry = vi.fn();
    const manager = new BrowserConnectionManager({} as DbForConnection);
    Object.assign(
      manager as unknown as {
        connection: BrowserWorkerConnection;
        client: JazzClient;
        disposeRuntimeTelemetry: () => void;
      },
      { connection, client, disposeRuntimeTelemetry },
    );

    await expect(manager.shutdown()).rejects.toBe(flushError);

    expect(client.discard).toHaveBeenCalledOnce();
    expect(disposeRuntimeTelemetry).toHaveBeenCalledOnce();
    expect(connection.shutdown).toHaveBeenCalledOnce();
  });

  it("rejects a shutdown flush failure without waiting for the silent companion lease", async () => {
    const leaseMessages: string[] = [];
    const leaseEvents = new EventTarget();
    const emitLeaseMessage = (data: unknown) =>
      leaseEvents.dispatchEvent(new MessageEvent("message", { data }));
    const leasePort = Object.assign(leaseEvents, {
      start() {},
      close: vi.fn(),
      postMessage(message: { type: string; attemptId?: string }) {
        leaseMessages.push(message.type);
        if (message.type === "probe-foreground-node-lease-worker") {
          emitLeaseMessage({
            type: "foreground-node-lease-worker-alive",
            attemptId: message.attemptId,
          });
        } else if (message.type === "acquire-foreground-node-lease") {
          emitLeaseMessage({
            type: "foreground-node-lease-ready",
            leaseId: "00000000-0000-4000-8000-000000000001",
            node: new Uint8Array(16),
            confirmedTxTime: "0",
          });
        }
        // The companion channel supplies neither a finish result nor messageerror.
      },
    });
    const lease = await SharedBrowserForegroundNodeLease.acquireFromPort(
      leasePort as unknown as MessagePort,
      { dbName: "shutdown-flush-failure", storageOwner: "owner" },
    );
    const flushError = new BrowserWorkerUnresponsiveError(
      "worker became unresponsive during shutdown flush",
    );
    const flushStarted = deferred();
    const finishFlush = deferred();
    let onFailure!: BrowserWorkerConnectionContext["onFailure"];
    const connection: BrowserWorkerConnection = {
      ready: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      waitForPendingWrites: vi.fn(async () => undefined),
      updateAuth: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      deleteStorage: vi.fn(async () => undefined),
      flushLocal: vi.fn(async () => {
        flushStarted.resolve();
        await finishFlush.promise;
        onFailure(flushError);
        throw flushError;
      }),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      shutdown: vi.fn(async () => undefined),
    };
    const client = {
      onMutationError: vi.fn(),
      getRuntime: () => undefined,
      discard: vi.fn(),
    } as unknown as JazzClient;
    const disposeRuntimeTelemetry = vi.fn();
    const host = {
      config: { telemetryCollectorUrl: "https://example.test/telemetry" },
      isShuttingDown: false,
      runtimeSource: {
        acquireBrowserForegroundNodeLease: async () => lease,
        createClient: () => client,
        installTelemetry: () => disposeRuntimeTelemetry,
        createBrowserWorkerConnection: (context: BrowserWorkerConnectionContext) => {
          onFailure = context.onFailure;
          return connection;
        },
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      onMutationError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    };
    const manager = new BrowserConnectionManager(host as unknown as DbForConnection);
    await manager.start();
    manager.getClient({});
    await manager.ensureReady("local");
    expect(leasePort.close).not.toHaveBeenCalled();
    expect(leaseMessages).not.toContain("return-foreground-node-lease");
    expect(leaseMessages).not.toContain("retire-foreground-node-lease");

    host.isShuttingDown = true;
    let outcome: { status: "pending" | "fulfilled" } | { status: "rejected"; error: unknown } = {
      status: "pending",
    };
    void manager.shutdown().then(
      () => {
        outcome = { status: "fulfilled" };
      },
      (error: unknown) => {
        outcome = { status: "rejected", error };
      },
    );
    vi.useFakeTimers();
    try {
      await flushStarted.promise;
      expect(leasePort.close).not.toHaveBeenCalled();
      expect(leaseMessages).not.toContain("retire-foreground-node-lease");
      finishFlush.resolve();
      // Drain the synchronous fixture's cleanup promises without awaiting shutdown:
      // a missing lease result must fail this assertion, not time out the test.
      await vi.advanceTimersByTimeAsync(0);

      expect(outcome).toEqual({ status: "rejected", error: flushError });
      expect("error" in outcome ? outcome.error : undefined).toBe(flushError);
      expect(manager.getCurrentClient()).toBeNull();
      expect(client.discard).toHaveBeenCalledOnce();
      expect(disposeRuntimeTelemetry).toHaveBeenCalledOnce();
      expect(connection.shutdown).toHaveBeenCalledOnce();
      expect(leaseMessages).not.toContain("return-foreground-node-lease");
    } finally {
      // Release the baseline's pending finish only after observing shutdown.
      // This fixture acknowledgement is not evidence of durable return/retirement.
      finishFlush.resolve();
      await vi.advanceTimersByTimeAsync(0);
      emitLeaseMessage({ type: "foreground-node-lease-result" });
      await vi.advanceTimersByTimeAsync(0);
      leasePort.close();
      vi.useRealTimers();
    }
  });
});

describe("BrowserConnectionManager explicit transport transitions", () => {
  it("enables Inspector-local reads only from the worker attachment receipt", async () => {
    const connection = {
      ready: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(
        () => "jazz-inspector-authenticated-root",
      ),
    } as unknown as BrowserWorkerConnection;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: { createBrowserWorkerConnection: vi.fn(() => connection) },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      enableAuthenticatedInspectorLocalReads: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    await vi.waitFor(() =>
      expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledWith(
        "jazz-inspector-authenticated-root",
      ),
    );
  });

  it("revokes an Inspector receipt when a failed follower is replaced, and requires a fresh receipt", async () => {
    const firstReady = deferred();
    const secondReady = deferred();
    const first = {
      ready: vi.fn(() => firstReady.promise),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      waitForPendingWrites: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(() => "same-coordinate"),
    } as unknown as BrowserWorkerConnection;
    const second = {
      ready: vi.fn(() => secondReady.promise),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      waitForPendingWrites: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(() => "same-coordinate"),
    } as unknown as BrowserWorkerConnection;
    const callbacks: Array<{ onFailure(error: unknown): void }> = [];
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          callbacks.push(input);
          return callbacks.length === 1 ? first : second;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      enableAuthenticatedInspectorLocalReads: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    // The original connection fails before its init receipt resolves. Its late
    // receipt must not authorize the replacement merely because the physical
    // coordinate is identical.
    callbacks[0]?.onFailure(new Error("closed"));
    const reconnect = manager.reconnect();
    firstReady.resolve();
    await vi.waitFor(() =>
      expect(host.runtimeSource.createBrowserWorkerConnection).toHaveBeenCalledTimes(2),
    );
    expect(host.enableAuthenticatedInspectorLocalReads).not.toHaveBeenCalled();
    // Initial open, follower retirement, then the replacement opening each
    // revoke authority. In particular, removing the retirement clear makes
    // this assertion fail even though the replacement uses the same root.
    expect(host.clearAuthenticatedInspectorLocalReads).toHaveBeenCalledTimes(3);

    secondReady.resolve();
    await reconnect;
    await vi.waitFor(() =>
      expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledWith("same-coordinate"),
    );
    expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledTimes(1);
  });

  it("revokes an Inspector receipt before storage reset", async () => {
    const connection = {
      ready: vi.fn(async () => undefined),
      shutdown: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(() => "authenticated-root"),
    } as unknown as BrowserWorkerConnection;
    let onStorageReset: BrowserWorkerConnectionContext["onStorageReset"];
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          onStorageReset = input.onStorageReset;
          return connection;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      enableAuthenticatedInspectorLocalReads: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });
    await vi.waitFor(() =>
      expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledOnce(),
    );

    onStorageReset?.(1);
    await vi.waitFor(() =>
      expect(host.clearAuthenticatedInspectorLocalReads).toHaveBeenCalledTimes(2),
    );
  });

  it("serializes disconnect/reconnect and releases remote readiness after the last transition", async () => {
    const disconnectGate = deferred();
    const connection = {
      disconnect: vi.fn(() => disconnectGate.promise),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      waitForPendingWrites: vi.fn(async () => undefined),
    } as unknown as BrowserWorkerConnection;
    const manager = new BrowserConnectionManager({
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
    } as DbForConnection);
    Object.assign(
      manager as unknown as {
        connection: BrowserWorkerConnection;
        connectionReady: Promise<void>;
      },
      { connection, connectionReady: Promise.resolve() },
    );

    const disconnect = manager.disconnect();
    const ready = manager.ensureReady("edge");
    const reconnect = manager.reconnect();
    await Promise.resolve();
    expect(connection.reconnect).not.toHaveBeenCalled();

    disconnectGate.resolve();
    await Promise.all([disconnect, reconnect, ready]);
    expect(connection.disconnect).toHaveBeenCalledOnce();
    expect(connection.reconnect).toHaveBeenCalledOnce();
    expect(connection.waitForServerConnection).toHaveBeenCalledOnce();
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("does not retain explicit-offline state when disconnect fails", async () => {
    const failure = new Error("worker disconnect failed");
    const connection = {
      disconnect: vi.fn(async () => {
        throw failure;
      }),
    } as unknown as BrowserWorkerConnection;
    const manager = new BrowserConnectionManager({
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
    } as DbForConnection);
    Object.assign(
      manager as unknown as {
        connection: BrowserWorkerConnection;
        connectionReady: Promise<void>;
      },
      { connection, connectionReady: Promise.resolve() },
    );

    await expect(manager.disconnect()).rejects.toBe(failure);
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("reacquires a fresh follower after a terminal follower failure", async () => {
    const first = {
      ready: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      waitForPendingWrites: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const second = {
      ready: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      waitForPendingWrites: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const callbacks: Array<{ onFailure(error: unknown): void }> = [];
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          callbacks.push(input);
          return callbacks.length === 1 ? first : second;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });
    await Promise.resolve();

    const failure = new Error(
      "Protocol: maintained root occurrence sidecar length does not match root rows",
    );
    callbacks[0]?.onFailure(failure);

    await manager.reconnect();

    expect(host.runtimeSource.createBrowserWorkerConnection).toHaveBeenCalledTimes(2);
    expect(first.reconnect).not.toHaveBeenCalled();
    expect(second.reconnect).toHaveBeenCalledOnce();
    await expect(manager.ensureReady("edge")).resolves.toBeUndefined();
  });

  it("rejects remote readiness on terminal failure while explicitly offline", async () => {
    const fixture = await leasedManagerFixture();
    const failure = new Error("browser worker terminated");
    const bothWaitersParked = deferred();
    let waitForReconnectCalls = 0;
    const waitForReconnect = fixture.manager.waitForReconnect.bind(fixture.manager);
    vi.spyOn(fixture.manager, "waitForReconnect").mockImplementation((signal) => {
      waitForReconnectCalls += 1;
      if (waitForReconnectCalls === 2) bothWaitersParked.resolve();
      return waitForReconnect(signal);
    });

    fixture.contexts[0]?.onExplicitOfflineChange?.(true);
    expect(fixture.manager.isExplicitlyOffline()).toBe(true);

    let edgeResult: unknown;
    let globalResult: unknown;
    const edgeReady = fixture.manager.ensureReady("edge").then(
      () => {
        edgeResult = "resolved";
      },
      (error) => {
        edgeResult = error;
      },
    );
    const globalReady = fixture.manager.ensureReady("global").then(
      () => {
        globalResult = "resolved";
      },
      (error) => {
        globalResult = error;
      },
    );
    await bothWaitersParked.promise;
    fixture.fail(failure);

    await vi.waitFor(() => {
      expect(edgeResult).toBe(failure);
      expect(globalResult).toBe(failure);
    });
    await Promise.all([edgeReady, globalReady]);
    expect(fixture.manager.isExplicitlyOffline()).toBe(true);

    await expect(fixture.manager.reconnect()).resolves.toBeUndefined();
    expect(fixture.manager.isExplicitlyOffline()).toBe(false);
    await expect(fixture.manager.ensureReady("edge")).resolves.toBeUndefined();
  });

  it("disconnects a worker created while offline before an immediate reconnect", async () => {
    const disconnectGate = deferred();
    const connection = {
      ready: vi.fn(async () => undefined),
      disconnect: vi.fn(() => disconnectGate.promise),
      reconnect: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn(() => connection),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    await manager.disconnect();

    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });
    const reconnect = manager.reconnect();
    await vi.waitFor(() => expect(connection.disconnect).toHaveBeenCalledOnce());
    expect(connection.reconnect).not.toHaveBeenCalled();

    disconnectGate.resolve();
    await reconnect;
    expect(connection.reconnect).toHaveBeenCalledOnce();
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("adopts explicit offline state broadcast by another tab in the worker namespace", async () => {
    const connection = {
      ready: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    let callbacks:
      | {
          onExplicitOfflineChange?: (offline: boolean) => void;
        }
      | undefined;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          callbacks = input;
          return connection;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    callbacks?.onExplicitOfflineChange?.(true);
    expect(manager.isExplicitlyOffline()).toBe(true);

    const reconnected = manager.waitForReconnect();
    callbacks?.onExplicitOfflineChange?.(false);
    await reconnected;
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("waits for worker transport state only while a follower is attaching", async () => {
    const ready = deferred();
    const connection = {
      ready: vi.fn(() => ready.promise),
      disconnect: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn(() => connection),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    const initial = manager.initialExplicitOfflineState();
    expect(initial).not.toBeNull();
    ready.resolve();
    await initial;
    expect(manager.initialExplicitOfflineState()).toBeNull();
  });
});
