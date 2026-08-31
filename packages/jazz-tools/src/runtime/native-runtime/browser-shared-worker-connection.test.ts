import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createBrowserSharedWorkerBaseName,
  SharedBrowserForegroundNodeLease,
} from "./browser-shared-worker-connection.js";

type LeaseMessage = {
  type:
    | "acquire-foreground-node-lease"
    | "cancel-foreground-node-lease"
    | "retire-foreground-node-lease";
};

class DelayedLeasePort {
  private readonly messageListeners = new Set<(event: MessageEvent) => void>();
  readonly sent: LeaseMessage[] = [];
  closed = false;

  constructor(
    private readonly readyDelayMs = 1_100,
    private readonly acknowledgeCancellation = true,
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
    if (message.type === "acquire-foreground-node-lease") {
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
        setTimeout(() => this.emit({ type: "foreground-node-lease-cancelled" }), 0);
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

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("browser SharedWorker realm identity", () => {
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
      { type: "acquire-foreground-node-lease", dbName: "timed-out-root", storageOwner: "owner" },
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
  });
});
