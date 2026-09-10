import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createBrowserSharedWorkerBaseName,
  installWorkerTerminationGenerationHandoff,
  SharedBrowserForegroundNodeLease,
  SharedBrowserWorkerConnection,
} from "./browser-shared-worker-connection.js";
import { BrowserWorkerUnresponsiveError } from "./browser-worker-protocol.js";

type LeaseMessage =
  | { type: "probe-foreground-node-lease-worker"; attemptId: string }
  | {
      type: "acquire-foreground-node-lease";
      attemptId?: string;
      dbName: string;
      storageOwner: string;
    }
  | { type: "cancel-foreground-node-lease" }
  | { type: "retire-foreground-node-lease" };

class DelayedLeasePort {
  private readonly messageListeners = new Set<(event: MessageEvent) => void>();
  readonly sent: LeaseMessage[] = [];
  closed = false;

  constructor(
    private readonly readyDelayMs = 1_100,
    private readonly acknowledgeCancellation = true,
    private readonly acknowledgeProbe = true,
    private readonly probeDelayMs = 0,
    private readonly closing = false,
    private readonly busyMessage: string | null = null,
    private readonly cancellationError: string | null = null,
  ) {}

  start(): void {}

  close(): void {
    this.closed = true;
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    if (type === "message") this.messageListeners.add(listener);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    if (type === "message") this.messageListeners.delete(listener);
  }

  postMessage(message: LeaseMessage): void {
    this.sent.push(message);
    if (message.type === "probe-foreground-node-lease-worker") {
      if (this.acknowledgeProbe) {
        setTimeout(
          () =>
            this.emit({
              type: this.closing
                ? "foreground-node-lease-worker-closing"
                : "foreground-node-lease-worker-alive",
              attemptId: message.attemptId,
            }),
          this.probeDelayMs,
        );
      }
      return;
    }
    if (message.type === "acquire-foreground-node-lease") {
      if (this.busyMessage) {
        setTimeout(
          () => this.emit({ type: "foreground-node-lease-busy", message: this.busyMessage }),
          0,
        );
        return;
      }
      // This is intentionally just beyond the historical one-second budget.
      // Cold IDB admission is allowed to take this long before the foreground
      // has received its first durable identity.
      setTimeout(
        () =>
          this.emit({
            type: "foreground-node-lease-ready",
            leaseId: "00000000-0000-4000-8000-000000000001",
            node: new Uint8Array(16),
            confirmedTxTime: "0",
          }),
        this.readyDelayMs,
      );
      return;
    }
    if (message.type === "cancel-foreground-node-lease") {
      // Model the worker's acknowledgement only after it has dealt with an
      // allocation that races the cancellation request.
      if (this.acknowledgeCancellation) {
        setTimeout(
          () =>
            this.emit({
              type: "foreground-node-lease-cancelled",
              ...(this.cancellationError ? { error: this.cancellationError } : {}),
            }),
          0,
        );
      }
      return;
    }
    setTimeout(() => this.emit({ type: "foreground-node-lease-result" }), 0);
  }

  private emit(data: unknown): void {
    for (const listener of this.messageListeners) listener({ data } as MessageEvent);
  }

  acknowledgeCancellationNow(): void {
    this.emit({ type: "foreground-node-lease-cancelled" });
  }
}

class ScriptedRuntimePort {
  private readonly listeners = new Map<string, Set<(event: MessageEvent) => void>>();
  closed = false;

  constructor(
    private readonly onPost: (message: { type?: string; id?: number; attemptId?: string }) => void,
  ) {}

  start(): void {}

  close(): void {
    this.closed = true;
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    const registered = this.listeners.get(type) ?? new Set();
    registered.add(listener);
    this.listeners.set(type, registered);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: { type?: string; id?: number; attemptId?: string }): void {
    this.onPost(message);
  }

  emit(message: unknown): void {
    for (const listener of this.listeners.get("message") ?? [])
      listener({ data: message } as MessageEvent);
  }

  emitMessageError(): void {
    for (const listener of this.listeners.get("messageerror") ?? [])
      listener(new MessageEvent("messageerror"));
  }
}

function runtimeBootstrapFixture(aliveDelayMs = 0) {
  const sent: Array<{ type?: string; id?: number }> = [];
  const port = new ScriptedRuntimePort((message) => {
    sent.push(message);
    if (message.type === "connect-runtime") {
      if (aliveDelayMs === 0) port.emit({ type: "worker-alive" });
      else setTimeout(() => port.emit({ type: "worker-alive" }), aliveDelayMs);
    }
    if (message.type === "init") port.emit({ type: "result", id: message.id });
  });
  vi.stubGlobal(
    "SharedWorker",
    class {
      readonly port = port;
    },
  );
  const connection = new SharedBrowserWorkerConnection(
    {
      connectUpstreamPeer: () => ({ recvWireFrames: () => [] }),
      onPeerTransportWork: () => () => undefined,
      progressPeerTransport: async () => undefined,
      retirePeerTransport: async () => undefined,
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
      flushLocalSettlements: async () => undefined,
    } as never,
    {
      schema: {},
      dbName: "runtime-budget-controls",
      author: new Uint8Array(16),
      initialSyncFlushEvery: 1,
      appId: "app",
      storageOwner: "owner",
      authSessionKey: "scope",
      authJson: "{}",
      sessionClaims: {},
    },
    "runtime-fingerprint",
    {
      onAuthFailure: vi.fn(),
      onAuthRestored: vi.fn(),
      onExplicitOfflineChange: vi.fn(),
      onFailure: vi.fn(),
      onStorageReset: vi.fn(),
      onStorageInvalidated: vi.fn(),
    },
  );
  return { connection, port, sent };
}

class InspectorPort {
  private readonly messageListeners = new Set<(event: MessageEvent) => void>();

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    if (type === "message") this.messageListeners.add(listener);
  }

  emit(data: unknown): void {
    for (const listener of this.messageListeners) listener({ data } as MessageEvent);
  }
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("browser foreground lease terminal abandonment", () => {
  async function acquireLease(retirePostError?: Error) {
    const sent: string[] = [];
    const port = new ScriptedRuntimePort((message) => {
      sent.push(message.type!);
      if (message.type === "probe-foreground-node-lease-worker") {
        port.emit({ type: "foreground-node-lease-worker-alive", attemptId: message.attemptId });
      } else if (message.type === "acquire-foreground-node-lease") {
        port.emit({
          type: "foreground-node-lease-ready",
          leaseId: "00000000-0000-4000-8000-000000000003",
          node: new Uint8Array(16),
          confirmedTxTime: "0",
        });
      } else if (message.type === "retire-foreground-node-lease" && retirePostError) {
        throw retirePostError;
      }
    });
    const close = vi.spyOn(port, "close");
    const lease = await SharedBrowserForegroundNodeLease.acquireFromPort(
      port as unknown as MessagePort,
      { dbName: "abandoned-foreground-root", storageOwner: "owner" },
    );
    return { lease, port, sent, close };
  }

  it.each(["before finish", "during retirement"] as const)(
    "rejects local finish %s without awaiting a retirement receipt",
    async (phase) => {
      const { lease, port, sent, close } = await acquireLease();
      const error = new BrowserWorkerUnresponsiveError("worker realm stopped responding");
      const pending = phase === "before finish" ? undefined : lease.retire();
      const rejected = pending && expect(pending).rejects.toBe(error);
      lease.abandonAfterWorkerFailure(error);
      lease.abandonAfterWorkerFailure(new Error("duplicate must not replace the cause"));
      await rejected;
      await expect(lease.returnWithHighWater(99n)).rejects.toBe(error);
      await expect(lease.retire()).rejects.toBe(error);
      expect(sent.filter((type) => type === "retire-foreground-node-lease")).toHaveLength(1);
      expect(sent).not.toContain("return-foreground-node-lease");
      expect(close).not.toHaveBeenCalled();
      port.emit({ type: "unrelated-result" });
      expect(close).not.toHaveBeenCalled();
      port.emit({ type: "foreground-node-lease-result" });
      expect(close).toHaveBeenCalledOnce();
      port.emit({ type: "foreground-node-lease-result" });
      lease.abandonAfterWorkerFailure(new Error("late failure"));
      await expect(lease.returnWithHighWater(0n)).rejects.toBe(error);
      expect(close).toHaveBeenCalledOnce();
      expect(sent.filter((type) => type === "retire-foreground-node-lease")).toHaveLength(1);
    },
  );

  it("retains only the already-posted return when abandonment rejects its local caller", async () => {
    const { lease, port, sent, close } = await acquireLease();
    const post = vi.spyOn(port, "postMessage");
    const error = new BrowserWorkerUnresponsiveError("worker realm stopped responding");
    let outcome: unknown = "pending";
    const returned = lease.returnWithHighWater(42n).then(
      () => {
        outcome = "fulfilled";
      },
      (failure) => {
        outcome = failure;
      },
    );
    vi.useFakeTimers();
    try {
      expect(post).toHaveBeenCalledExactlyOnceWith({
        type: "return-foreground-node-lease",
        confirmedTxTime: "42",
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(outcome).toBe("pending");
      lease.abandonAfterWorkerFailure(error);
      lease.abandonAfterWorkerFailure(new Error("duplicate must not replace the cause"));
      await vi.advanceTimersByTimeAsync(0);
      expect(outcome).toBe(error);
      expect(sent.filter((type) => type.endsWith("-foreground-node-lease"))).toEqual([
        "acquire-foreground-node-lease",
        "return-foreground-node-lease",
      ]);
      expect(close).not.toHaveBeenCalled();
      port.emit({ type: "unrelated-result" });
      expect(close).not.toHaveBeenCalled();

      // The original result releases the port, not the caller's unknown outcome.
      port.emit({ type: "foreground-node-lease-result" });
      await returned;
      expect(close).toHaveBeenCalledOnce();
      expect(outcome).toBe(error);
      await expect(lease.returnWithHighWater(99n)).rejects.toBe(error);
      await expect(lease.retire()).rejects.toBe(error);
      port.emit({ type: "foreground-node-lease-result" });
      expect(close).toHaveBeenCalledOnce();
      expect(post).toHaveBeenCalledOnce();
    } finally {
      // Release this actual adapter's controlled port after the observations,
      // including on the baseline's contradictory second-request assertion.
      port.emit({ type: "foreground-node-lease-result" });
      await returned;
      vi.useRealTimers();
    }
  });

  it.each(["result error", "messageerror"] as const)(
    "releases retained cleanup on a late %s without changing the causal failure",
    async (event) => {
      const { lease, port, close } = await acquireLease();
      const error = new BrowserWorkerUnresponsiveError("original worker failure");
      lease.abandonAfterWorkerFailure(error);
      if (event === "messageerror") port.emitMessageError();
      else
        port.emit({
          type: "foreground-node-lease-result",
          error: { name: "Error", message: "late durable retirement failure" },
        });
      expect(close).toHaveBeenCalledOnce();
      await expect(lease.retire()).rejects.toBe(error);
    },
  );

  it("keeps the causal failure when best-effort retirement cannot be posted", async () => {
    const { lease, sent, close } = await acquireLease(new Error("port is gone"));
    const error = new BrowserWorkerUnresponsiveError("original worker failure");
    lease.abandonAfterWorkerFailure(error);
    lease.abandonAfterWorkerFailure(error);
    await expect(lease.returnWithHighWater(0n)).rejects.toBe(error);
    await expect(lease.retire()).rejects.toBe(error);
    expect(sent.filter((type) => type === "retire-foreground-node-lease")).toHaveLength(1);
    expect(close).toHaveBeenCalledOnce();
  });
});

describe("browser SharedWorker realm identity", () => {
  it("advances the shared generation when inspector termination is acknowledged", async () => {
    const values = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    });
    const dbName = "inspector-terminated-root";
    const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
    const inspector = new InspectorPort();
    installWorkerTerminationGenerationHandoff(inspector as unknown as MessagePort, workerName, 0);

    // Ordinary inspector results never affect worker selection.
    inspector.emit({ type: "result", id: 1 });
    expect(values.size).toBe(0);
    inspector.emit({ type: "result", id: 2, workerTerminated: true });

    const leasePort = new DelayedLeasePort(0);
    const workerNames: string[] = [];
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = leasePort;

        constructor(_url: URL, options: { name: string }) {
          workerNames.push(options.name);
        }
      },
    );
    const lease = await SharedBrowserForegroundNodeLease.acquire({
      dbName,
      storageOwner: "owner",
    });

    expect(workerNames).toEqual([expect.stringContaining(":generation-1")]);
    await lease.retire();
  });

  it("waits through cold durable lease admission rather than treating it as a dead worker", async () => {
    const port = new DelayedLeasePort();
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = port;
      },
    );

    const lease = await SharedBrowserForegroundNodeLease.acquire({
      dbName: "slow-cold-root",
      storageOwner: "owner",
    });
    await expect(lease.retire()).resolves.toBeUndefined();
  }, 3_000);

  it("returns the public timeout while retaining cancellation cleanup for a wedged worker", async () => {
    vi.useFakeTimers();
    const port = new DelayedLeasePort(10_001, false);
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = port;
      },
    );

    const acquire = SharedBrowserForegroundNodeLease.acquire({
      dbName: "timed-out-root",
      storageOwner: "owner",
    });
    // Install containment before advancing the synthetic clock. This fake
    // worker never acknowledges cancellation, so it proves public startup
    // does not hang behind background cleanup.
    const rejected = expect(acquire).rejects.toThrow("did not issue a foreground node lease");
    await vi.advanceTimersByTimeAsync(10_000);
    expect(port.sent).toEqual([
      expect.objectContaining({ type: "probe-foreground-node-lease-worker" }),
      expect.objectContaining({
        type: "acquire-foreground-node-lease",
        dbName: "timed-out-root",
        storageOwner: "owner",
      }),
      { type: "cancel-foreground-node-lease" },
    ]);
    await rejected;
    expect(port.closed).toBe(false);
    // Keep the global coalescing registry isolated for following receipts.
    port.acknowledgeCancellationNow();
    expect(port.closed).toBe(true);
  });

  it("drains retained cancellation cleanup when a late worker acknowledgement arrives", async () => {
    vi.useFakeTimers();
    const port = new DelayedLeasePort(10_001, true);
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = port;
      },
    );

    const acquire = SharedBrowserForegroundNodeLease.acquire({
      dbName: "late-cancellation-root",
      storageOwner: "owner",
    });
    const rejected = expect(acquire).rejects.toThrow("did not issue a foreground node lease");
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(port.closed).toBe(false);

    await vi.runAllTimersAsync();
    expect(port.closed).toBe(true);
  });

  it("allows a new foreground after cancellation learns owner admission failed", async () => {
    vi.useFakeTimers();
    const rejectedPort = new DelayedLeasePort(
      10_001,
      true,
      true,
      0,
      false,
      null,
      "IndexedDB admission rejected",
    );
    const retryPort = new DelayedLeasePort(0);
    let workerCount = 0;
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = ++workerCount === 1 ? rejectedPort : retryPort;
      },
    );
    const options = { dbName: "cancelled-owner-admission", storageOwner: "owner" };

    const first = SharedBrowserForegroundNodeLease.acquire(options);
    const firstRejected = expect(first).rejects.toThrow("did not issue a foreground node lease");
    await vi.advanceTimersByTimeAsync(10_000);
    await firstRejected;
    await vi.runAllTimersAsync();
    expect(rejectedPort.closed).toBe(true);

    // Receiving the terminal cancellation reply clears the per-root cleanup
    // coalescer. The next foreground gets a new port instead of a permanent
    // "cleanup is still pending" rejection.
    const retry = SharedBrowserForegroundNodeLease.acquire(options);
    await vi.runAllTimersAsync();
    const lease = await retry;
    expect(workerCount).toBe(2);
    const retired = lease.retire();
    await vi.runAllTimersAsync();
    await retired;
  });

  it("skips a closing worker generation before issuing a durable lease request", async () => {
    vi.useFakeTimers();
    const closingPort = new DelayedLeasePort(0, true, true, 0, true);
    const successorPort = new DelayedLeasePort(0, true, true);
    const workerNames: string[] = [];
    let workerCount = 0;
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = ++workerCount === 1 ? closingPort : successorPort;

        constructor(_url: URL, options: { name: string }) {
          workerNames.push(options.name);
        }
      },
    );

    const acquiring = SharedBrowserForegroundNodeLease.acquire({
      dbName: "closing-generation-root",
      storageOwner: "owner",
    });
    await vi.runAllTimersAsync();
    const lease = await acquiring;

    expect(closingPort.sent).toEqual([
      expect.objectContaining({ type: "probe-foreground-node-lease-worker" }),
    ]);
    expect(successorPort.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: "probe-foreground-node-lease-worker" }),
        expect.objectContaining({ type: "acquire-foreground-node-lease" }),
      ]),
    );
    expect(workerNames[0]).toContain(":generation-0");
    expect(workerNames[1]).toContain(":generation-1");

    const retired = lease.retire();
    await vi.runAllTimersAsync();
    await retired;
  });

  it("retries a safely classified physical-owner busy lease without advancing generations", async () => {
    vi.useFakeTimers();
    const busyPort = new DelayedLeasePort(0, true, true, 0, false, "owner is releasing");
    const readyPort = new DelayedLeasePort(0);
    const workerNames: string[] = [];
    let workerCount = 0;
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = ++workerCount === 1 ? busyPort : readyPort;

        constructor(_url: URL, options: { name: string }) {
          workerNames.push(options.name);
        }
      },
    );

    const acquiring = SharedBrowserForegroundNodeLease.acquire({
      dbName: "busy-physical-owner-root",
      storageOwner: "owner",
    });
    await vi.runAllTimersAsync();
    const lease = await acquiring;

    expect(workerNames).toHaveLength(2);
    expect(workerNames[0]).toContain(":generation-0");
    expect(workerNames[1]).toContain(":generation-0");
    expect(busyPort.closed).toBe(true);
    expect(readyPort.closed).toBe(false);

    const retired = lease.retire();
    await vi.runAllTimersAsync();
    await retired;
  });

  it("rejects readiness when an alive runtime never finishes its five-minute startup grace", async () => {
    vi.useFakeTimers();
    const port = new ScriptedRuntimePort((message) => {
      if (message.type === "connect-runtime") port.emit({ type: "worker-alive" });
    });
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = port;
      },
    );
    const runtime = {
      connectUpstreamPeer: () => ({ recvWireFrames: () => [] }),
      onPeerTransportWork: () => () => undefined,
      progressPeerTransport: async () => undefined,
      retirePeerTransport: async () => undefined,
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
      flushLocalSettlements: async () => undefined,
    };
    const callbacks = {
      onAuthFailure: vi.fn(),
      onAuthRestored: vi.fn(),
      onExplicitOfflineChange: vi.fn(),
      onFailure: vi.fn(),
      onStorageReset: vi.fn(),
      onStorageInvalidated: vi.fn(),
    };
    const connection = new SharedBrowserWorkerConnection(
      runtime as never,
      {
        schema: {},
        dbName: "runtime-startup-grace-root",
        author: new Uint8Array(16),
        initialSyncFlushEvery: 1,
        appId: "app",
        storageOwner: "owner",
        authSessionKey: "scope-a",
        authJson: "{}",
        sessionClaims: {},
      },
      "runtime-fingerprint",
      callbacks,
    );
    let outcome = "pending";
    void connection.ready().then(
      () => {
        outcome = "resolved";
      },
      () => {
        outcome = "rejected";
      },
    );

    try {
      await vi.advanceTimersByTimeAsync(5 * 60 * 1_000);
      expect(outcome).toBe("rejected");
      await expect(connection.ready()).rejects.toBeInstanceOf(BrowserWorkerUnresponsiveError);
    } finally {
      // Release the stalled baseline only after observing public readiness.
      // This terminal reply also completes retained timeout cleanup.
      port.emit({
        type: "runtime-error",
        error: { name: "Error", message: "Runtime fixture finished" },
      });
      await connection.shutdown();
    }
  });

  it("admits a healthy runtime that initializes beyond the old one-second probe", async () => {
    vi.useFakeTimers();
    const { connection, port, sent } = runtimeBootstrapFixture();
    await vi.advanceTimersByTimeAsync(1_100);
    port.emit({ type: "runtime-ready" });
    await expect(connection.ready()).resolves.toBeUndefined();
    const shutdown = connection.shutdown();
    await vi.advanceTimersByTimeAsync(0);
    // The follower has finished admission and owns normal close acknowledgement.
    port.emit({ type: "result", id: sent.find((message) => message.type === "close")?.id });
    await shutdown;
    expect(port.closed).toBe(true);
  });

  it("keeps runtime admission in the same realm when its alive reply is delayed", async () => {
    vi.useFakeTimers();
    const { connection, port, sent } = runtimeBootstrapFixture(1_100);
    await vi.advanceTimersByTimeAsync(1_100);
    expect(sent.filter((message) => message.type === "connect-runtime")).toHaveLength(1);
    expect(sent.filter((message) => message.type === "cancel-runtime-bootstrap")).toHaveLength(0);
    port.emit({ type: "runtime-ready" });
    await expect(connection.ready()).resolves.toBeUndefined();
    const shutdown = connection.shutdown();
    await vi.advanceTimersByTimeAsync(0);
    port.emit({ type: "result", id: sent.find((message) => message.type === "close")?.id });
    await shutdown;
    expect(port.closed).toBe(true);
  });

  it("rejects a silent runtime without opening a competing worker generation", async () => {
    vi.useFakeTimers();
    const { connection, port, sent } = runtimeBootstrapFixture(6 * 60_000);
    const rejected = expect(connection.ready()).rejects.toBeInstanceOf(
      BrowserWorkerUnresponsiveError,
    );
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    await rejected;
    expect(sent.filter((message) => message.type === "connect-runtime")).toHaveLength(1);
    expect(sent.filter((message) => message.type === "cancel-runtime-bootstrap")).toHaveLength(1);
    port.emit({ type: "runtime-bootstrap-cancelled" });
    await connection.shutdown();
    expect(port.closed).toBe(true);
  });

  it("does not restart the startup budget when the first alive reply arrives late", async () => {
    vi.useFakeTimers();
    const { connection, port, sent } = runtimeBootstrapFixture(299_000);
    let outcome = "pending";
    void connection.ready().then(
      () => {
        outcome = "ready";
      },
      () => {
        outcome = "failed";
      },
    );
    await vi.advanceTimersByTimeAsync(299_000);
    expect(outcome).toBe("pending");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(outcome).toBe("failed");
    await expect(connection.ready()).rejects.toBeInstanceOf(BrowserWorkerUnresponsiveError);
    expect(sent.filter((message) => message.type === "connect-runtime")).toHaveLength(1);
    expect(sent.filter((message) => message.type === "cancel-runtime-bootstrap")).toHaveLength(1);
    port.emit({ type: "runtime-bootstrap-cancelled" });
    await connection.shutdown();
    expect(port.closed).toBe(true);
  });

  it("does not let duplicate alive messages extend runtime startup", async () => {
    vi.useFakeTimers();
    const { connection, port, sent } = runtimeBootstrapFixture();
    const rejected = expect(connection.ready()).rejects.toThrow("five minutes");
    await vi.advanceTimersByTimeAsync(299_000);
    port.emit({ type: "worker-alive" });
    await vi.advanceTimersByTimeAsync(1_000);
    await rejected;
    expect(sent.filter((message) => message.type === "cancel-runtime-bootstrap")).toHaveLength(1);
    await connection.shutdown();
    expect(port.closed).toBe(false);
    port.emit({ type: "runtime-bootstrap-cancelled" });
    expect(port.closed).toBe(true);
  });

  it("settles shutdown without a cancellation ack and releases a late attached peer", async () => {
    vi.useFakeTimers();
    const { connection, port, sent } = runtimeBootstrapFixture();
    const rejected = expect(connection.ready()).rejects.toThrow("closed");
    await connection.shutdown();
    await rejected;
    await connection.shutdown();
    expect(sent.filter((message) => message.type === "cancel-runtime-bootstrap")).toHaveLength(1);
    expect(port.closed).toBe(false);
    port.emit({ type: "runtime-ready" });
    expect(sent).toContainEqual({ type: "close", id: 0, releaseContext: true });
    expect(port.closed).toBe(false);
    port.emit({ type: "result", id: 0 });
    expect(port.closed).toBe(true);
    const settledCount = sent.length;
    port.emit({ type: "runtime-ready" });
    expect(sent).toHaveLength(settledCount);
  });

  it("gives a suspended page fresh startup grace before expiring a delayed timer", async () => {
    vi.useFakeTimers();
    const { connection, port } = runtimeBootstrapFixture();
    let outcome = "pending";
    void connection.ready().then(
      () => {
        outcome = "ready";
      },
      () => {
        outcome = "failed";
      },
    );
    // Advance wall time without running scheduled tasks: the first resumed
    // task must not treat time spent asleep as evidence of worker failure.
    vi.setSystemTime(Date.now() + 10 * 60_000);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(outcome).toBe("pending");
    await vi.advanceTimersByTimeAsync(299_000);
    expect(outcome).toBe("pending");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(outcome).toBe("failed");
    port.emit({ type: "runtime-bootstrap-cancelled" });
    await connection.shutdown();
  });

  it("moves a runtime bootstrap out of an inspector-terminating realm", async () => {
    const dbName = "runtime-closing-root";
    const workers: Array<{ name: string; port: ScriptedRuntimePort }> = [];
    const generationStorage = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => generationStorage.get(key) ?? null,
      setItem: (key: string, value: string) => generationStorage.set(key, value),
    });
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port: ScriptedRuntimePort;

        constructor(_url: URL, options: { name: string }) {
          const index = workers.length;
          this.port = new ScriptedRuntimePort((message) => {
            if (message.type === "connect-runtime") {
              this.port.emit(index === 0 ? { type: "worker-closing" } : { type: "runtime-ready" });
            } else if (message.type === "init" || message.type === "close") {
              this.port.emit({ type: "result", id: message.id });
            }
          });
          workers.push({ name: options.name, port: this.port });
        }
      },
    );
    const runtime = {
      connectUpstreamPeer: () => ({ recvWireFrames: () => [] }),
      onPeerTransportWork: () => () => undefined,
      progressPeerTransport: async () => undefined,
      retirePeerTransport: async () => undefined,
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
      flushLocalSettlements: async () => undefined,
    };
    const callbacks = {
      onAuthFailure: vi.fn(),
      onAuthRestored: vi.fn(),
      onExplicitOfflineChange: vi.fn(),
      onFailure: vi.fn(),
      onStorageReset: vi.fn(),
      onStorageInvalidated: vi.fn(),
    };

    const connection = new SharedBrowserWorkerConnection(
      runtime as never,
      {
        schema: {},
        dbName,
        author: new Uint8Array(16),
        initialSyncFlushEvery: 1,
        appId: "app",
        storageOwner: "owner",
        authSessionKey: "scope-a",
        authJson: "{}",
        sessionClaims: {},
      },
      "runtime-fingerprint",
      callbacks,
    );

    await expect(connection.ready()).resolves.toBeUndefined();
    expect(workers.map((worker) => worker.name)).toEqual([
      expect.stringContaining(":generation-0"),
      expect.stringContaining(":generation-1"),
    ]);
    expect(workers[0]?.port.closed).toBe(true);
    await connection.shutdown();
    expect(workers[1]?.port.closed).toBe(true);
  });

  it("does not advance generations when a healthy busy worker delays its probe", async () => {
    vi.useFakeTimers();
    const delayedHealthyPort = new DelayedLeasePort(0, true, true, 1_100);
    const workerNames: string[] = [];
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = delayedHealthyPort;

        constructor(_url: URL, options: { name: string }) {
          workerNames.push(options.name);
        }
      },
    );

    const acquiring = SharedBrowserForegroundNodeLease.acquire({
      dbName: "busy-healthy-generation-root",
      storageOwner: "owner",
    });
    await vi.advanceTimersByTimeAsync(1_100);
    await vi.runAllTimersAsync();
    const lease = await acquiring;

    expect(workerNames).toHaveLength(1);
    expect(workerNames[0]).toContain(":generation-0");
    expect(delayedHealthyPort.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: "probe-foreground-node-lease-worker" }),
        expect.objectContaining({ type: "acquire-foreground-node-lease" }),
      ]),
    );

    const retired = lease.retire();
    await vi.runAllTimersAsync();
    await retired;
  });

  it("coalesces repeated opens behind one wedged cancellation cleanup and retries after acknowledgement", async () => {
    vi.useFakeTimers();
    const wedgedPort = new DelayedLeasePort(10_001, false);
    const recoveredPort = new DelayedLeasePort(0, true);
    let workerCount = 0;
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port = ++workerCount === 1 ? wedgedPort : recoveredPort;
      },
    );
    const options = { dbName: "coalesced-timeout-root", storageOwner: "owner" };

    const first = SharedBrowserForegroundNodeLease.acquire(options);
    const firstRejected = expect(first).rejects.toThrow("did not issue a foreground node lease");
    await vi.advanceTimersByTimeAsync(10_000);
    await firstRejected;
    await expect(SharedBrowserForegroundNodeLease.acquire(options)).rejects.toThrow(
      "cancellation cleanup is still pending",
    );
    expect(workerCount).toBe(1);

    wedgedPort.acknowledgeCancellationNow();
    expect(wedgedPort.closed).toBe(true);

    const recovered = SharedBrowserForegroundNodeLease.acquire(options);
    await vi.runAllTimersAsync();
    const recoveredLease = await recovered;
    expect(recoveredLease).toBeInstanceOf(SharedBrowserForegroundNodeLease);
    const retired = recoveredLease.retire();
    await vi.runAllTimersAsync();
    await retired;
    expect(workerCount).toBe(2);
  });

  it("does not let a concurrent successful admission clear another timeout cleanup", async () => {
    vi.useFakeTimers();
    const timedOutPort = new DelayedLeasePort(10_001, false);
    // This response lands at exactly the first admission's deadline, after
    // its timeout callback but before this admission's own deadline.
    const concurrentSuccessPort = new DelayedLeasePort(9_999, true);
    const retryPort = new DelayedLeasePort(0, true);
    let workerCount = 0;
    vi.stubGlobal(
      "SharedWorker",
      class {
        readonly port =
          ++workerCount === 1
            ? timedOutPort
            : workerCount === 2
              ? concurrentSuccessPort
              : retryPort;
      },
    );
    const options = { dbName: "concurrent-timeout-root", storageOwner: "owner" };

    const first = SharedBrowserForegroundNodeLease.acquire(options);
    const firstRejected = expect(first).rejects.toThrow("did not issue a foreground node lease");
    await vi.advanceTimersByTimeAsync(1);
    const second = SharedBrowserForegroundNodeLease.acquire(options);
    await vi.advanceTimersByTimeAsync(9_999);
    await firstRejected;
    const secondLease = await second;

    // The successful second admission did not own the first timeout's cleanup
    // token. A third caller must still be coalesced rather than opening port 3.
    await expect(SharedBrowserForegroundNodeLease.acquire(options)).rejects.toThrow(
      "cancellation cleanup is still pending",
    );
    expect(workerCount).toBe(2);

    timedOutPort.acknowledgeCancellationNow();
    const retried = SharedBrowserForegroundNodeLease.acquire(options);
    await vi.runAllTimersAsync();
    const retriedLease = await retried;
    expect(workerCount).toBe(3);

    const retiredSecond = secondLease.retire();
    const retiredRetry = retriedLease.retire();
    await vi.runAllTimersAsync();
    await Promise.all([retiredSecond, retiredRetry]);
  });

  it("keeps foreground leases and runtime admission in one physical realm", () => {
    const dbName = "physical-root";

    // A foreground lease is acquired before the complete runtime config is
    // assembled. Its worker identity must therefore depend only on the
    // physical root and worker assets, not on a separately supplied auth
    // scope. The IndexedDB owner marker performs that admission inside this
    // single realm.
    const leaseWorker = createBrowserSharedWorkerBaseName(undefined, dbName);
    const runtimeWorker = createBrowserSharedWorkerBaseName(undefined, dbName);

    expect(leaseWorker).toBe(runtimeWorker);
    expect(leaseWorker).toContain(dbName);
    expect(leaseWorker).not.toContain("authSessionKey");
  });

  it("keeps incompatible worker assets in separate realms", () => {
    const dbName = "physical-root";
    const current = createBrowserSharedWorkerBaseName(
      { wasmUrl: "https://assets.test/current.wasm", wasmVersion: "current" },
      dbName,
    );
    const next = createBrowserSharedWorkerBaseName(
      { wasmUrl: "https://assets.test/next.wasm", wasmVersion: "next" },
      dbName,
    );

    expect(current).not.toBe(next);
    expect(current).toContain('"protocolVersion":"jazz-shared-runtime-v2"');
  });
});
