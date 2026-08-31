import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createBrowserSharedWorkerBaseName,
  SharedBrowserForegroundNodeLease,
  SharedBrowserWorkerConnection,
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

type ScriptedMessage = {
  type: string;
  id?: number;
  [key: string]: unknown;
};

class ScriptedMessagePort {
  private readonly listeners = new Map<string, Set<(event: MessageEvent) => void>>();
  readonly sent: ScriptedMessage[] = [];
  closeCalls = 0;
  startCalls = 0;

  constructor(
    private readonly handlePost: (message: ScriptedMessage, port: ScriptedMessagePort) => void,
  ) {}

  start(): void {
    this.startCalls += 1;
  }

  close(): void {
    this.closeCalls += 1;
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: ScriptedMessage): void {
    this.sent.push(message);
    this.handlePost(message, this);
  }

  emit(data: unknown): void {
    for (const listener of this.listeners.get("message") ?? []) {
      listener({ data } as MessageEvent);
    }
  }

  listenerCount(): number {
    let count = 0;
    for (const listeners of this.listeners.values()) count += listeners.size;
    return count;
  }
}

function installScriptedSharedWorkers(
  createPort: (name: string, index: number) => ScriptedMessagePort,
): { names: string[]; ports: ScriptedMessagePort[] } {
  const names: string[] = [];
  const ports: ScriptedMessagePort[] = [];
  vi.stubGlobal(
    "SharedWorker",
    class {
      readonly port: ScriptedMessagePort;

      constructor(_url: unknown, options: { name?: string } = {}) {
        const name = options.name;
        if (!name) throw new Error("scripted SharedWorker requires a name");
        names.push(name);
        this.port = createPort(name, ports.length);
        ports.push(this.port);
      }
    },
  );
  return { names, ports };
}

function installGenerationStorage(): Map<string, string> {
  const values = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
    clear: () => values.clear(),
    key: (index: number) => [...values.keys()][index] ?? null,
    get length() {
      return values.size;
    },
  });
  return values;
}

function generationIdentity(dbName: string): { baseName: string; storageKey: string } {
  const baseName = createBrowserSharedWorkerBaseName(undefined, dbName);
  return {
    baseName,
    storageKey: `jazz:shared-worker-generation:${baseName}`,
  };
}

function runtimeOptions(dbName: string) {
  return {
    schema: {},
    dbName,
    author: new Uint8Array(16),
    initialSyncFlushEvery: 1,
    appId: "scripted-app",
    storageOwner: "scripted-owner",
    authSessionKey: "scripted-session",
    authJson: "{}",
    sessionClaims: {},
  };
}

function runtimeHarness() {
  const transport = {
    recvWireFrames: () => [],
    sendWireFrame: () => undefined,
    tick: () => 0,
  };
  const retirePeerTransport = vi.fn(async () => undefined);
  const runtime = {
    connectUpstreamPeer: vi.fn(() => transport),
    onPeerTransportWork: vi.fn(() => () => undefined),
    progressPeerTransport: vi.fn(async () => undefined),
    retirePeerTransport,
    clearRemoteServerTransportError: vi.fn(),
    reportRemoteServerTransportError: vi.fn(),
    reportRemoteMutationError: vi.fn(),
    flushLocalSettlements: vi.fn(async () => undefined),
  };
  const onFailure = vi.fn();
  const callbacks = {
    onAuthFailure: vi.fn(),
    onAuthRestored: vi.fn(),
    onExplicitOfflineChange: vi.fn(),
    onFailure,
    onStorageReset: vi.fn(),
    onStorageInvalidated: vi.fn(),
  };
  return { runtime, callbacks, onFailure, retirePeerTransport };
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("browser SharedWorker realm identity", () => {
  it("moves a runtime bootstrap from a closing realm to generation 1", async () => {
    vi.useFakeTimers();
    const dbName = "runtime-closing-root";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const workers = installScriptedSharedWorkers((_name, index) => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type === "connect-runtime") {
          port.emit(index === 0 ? { type: "worker-closing" } : { type: "runtime-ready" });
          return;
        }
        if (message.type === "init" || (message.type === "close" && message.id !== undefined)) {
          port.emit({ type: "result", id: message.id });
        }
      });
    });
    const harness = runtimeHarness();
    const connection = new SharedBrowserWorkerConnection(
      harness.runtime as never,
      runtimeOptions(dbName) as never,
      "scripted-fingerprint",
      harness.callbacks,
    );

    await vi.advanceTimersByTimeAsync(0);
    expect(workers.names).toEqual([
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-1`,
    ]);
    await expect(connection.ready()).resolves.toBeUndefined();
    expect(storage).toEqual(new Map([[identity.storageKey, "1"]]));
    expect(workers.ports[0]?.closeCalls).toBeGreaterThan(0);
    expect(workers.ports[0]?.listenerCount()).toBe(0);
    expect(workers.ports[1]?.closeCalls).toBe(0);

    await expect(connection.shutdown()).resolves.toBeUndefined();
    await Promise.resolve();
    expect(harness.retirePeerTransport).toHaveBeenCalledOnce();
    expect(workers.ports.every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.every((port) => port.listenerCount() === 0)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("moves a foreground lease bootstrap from a closing realm to generation 1", async () => {
    vi.useFakeTimers();
    const dbName = "lease-closing-root";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const workers = installScriptedSharedWorkers((_name, index) => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type === "acquire-foreground-node-lease") {
          port.emit(
            index === 0
              ? { type: "worker-closing" }
              : {
                  type: "foreground-node-lease-ready",
                  leaseId: "00000000-0000-4000-8000-000000000011",
                  node: Uint8Array.from({ length: 16 }, (_, byte) => byte),
                  confirmedTxTime: "17",
                },
          );
          return;
        }
        if (message.type === "retire-foreground-node-lease") {
          port.emit({ type: "foreground-node-lease-result" });
        }
      });
    });

    const acquire = SharedBrowserForegroundNodeLease.acquire({
      dbName,
      storageOwner: "scripted-owner",
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(workers.names).toEqual([
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-1`,
    ]);
    const lease = await acquire;
    expect(lease.node).toEqual(Uint8Array.from({ length: 16 }, (_, byte) => byte));
    expect(lease.confirmedTxTime).toBe(17n);
    expect(storage).toEqual(new Map([[identity.storageKey, "1"]]));
    expect(workers.ports[0]?.closeCalls).toBeGreaterThan(0);
    expect(workers.ports[0]?.listenerCount()).toBe(0);
    expect(workers.ports[1]?.closeCalls).toBe(0);

    await expect(lease.retire()).resolves.toBeUndefined();
    expect(workers.ports.every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.every((port) => port.listenerCount() === 0)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("carries physical busy backoff across a closing worker generation", async () => {
    vi.useFakeTimers();
    const dbName = "lease-busy-closing-release-root";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const workers = installScriptedSharedWorkers((_name, index) => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type === "acquire-foreground-node-lease") {
          if (index === 0 || index === 2) {
            port.emit({
              type: "foreground-node-lease-busy",
              message: `owner still releasing attempt ${index / 2 + 1}`,
            });
          } else if (index === 1) {
            port.emit({ type: "worker-closing" });
          } else {
            port.emit({
              type: "foreground-node-lease-ready",
              leaseId: "00000000-0000-4000-8000-000000000013",
              node: new Uint8Array(16),
              confirmedTxTime: "0",
            });
          }
          return;
        }
        if (message.type === "retire-foreground-node-lease") {
          port.emit({ type: "foreground-node-lease-result" });
        }
      });
    });

    let settled = false;
    const acquire = SharedBrowserForegroundNodeLease.acquire({
      dbName,
      storageOwner: "scripted-owner",
    }).then((lease) => {
      settled = true;
      return lease;
    });

    await vi.advanceTimersByTimeAsync(74);
    expect(settled).toBe(false);
    expect(workers.names).toEqual([
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-1`,
    ]);

    await vi.advanceTimersByTimeAsync(1);
    const lease = await acquire;
    expect(workers.names).toEqual([
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-1`,
      `${identity.baseName}:generation-1`,
    ]);
    expect(storage).toEqual(new Map([[identity.storageKey, "1"]]));
    expect(workers.ports.slice(0, 3).every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.slice(0, 3).every((port) => port.listenerCount() === 0)).toBe(true);

    await expect(lease.retire()).resolves.toBeUndefined();
    expect(workers.ports.every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.every((port) => port.listenerCount() === 0)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retries an explicitly busy foreground lease on the same generation", async () => {
    vi.useFakeTimers();
    const dbName = "lease-busy-release-root";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const workers = installScriptedSharedWorkers((_name, index) => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type === "acquire-foreground-node-lease") {
          if (index < 2) {
            port.emit({
              type: "foreground-node-lease-busy",
              message: `owner still releasing attempt ${index + 1}`,
            });
          } else {
            port.emit({
              type: "foreground-node-lease-ready",
              leaseId: "00000000-0000-4000-8000-000000000012",
              node: new Uint8Array(16),
              confirmedTxTime: "0",
            });
          }
          return;
        }
        if (message.type === "retire-foreground-node-lease") {
          port.emit({ type: "foreground-node-lease-result" });
        }
      });
    });

    const observed = SharedBrowserForegroundNodeLease.acquire({
      dbName,
      storageOwner: "scripted-owner",
    }).then(
      (lease) => ({ lease, error: null }),
      (error: unknown) => ({ lease: null, error }),
    );
    await vi.runAllTimersAsync();
    const outcome = await observed;
    expect(outcome.error).toBeNull();
    if (!outcome.lease) throw outcome.error;
    expect(workers.names).toEqual([
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-0`,
      `${identity.baseName}:generation-0`,
    ]);
    expect(storage.has(identity.storageKey)).toBe(false);
    expect(workers.ports.slice(0, 2).every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.slice(0, 2).every((port) => port.listenerCount() === 0)).toBe(true);

    await expect(outcome.lease.retire()).resolves.toBeUndefined();
    expect(workers.ports).toHaveLength(3);
    expect(workers.ports.every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.every((port) => port.listenerCount() === 0)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("returns the explicit busy message within the total lease admission budget", async () => {
    vi.useFakeTimers();
    const dbName = "lease-busy-budget-root";
    const busyMessage = "physical database owner is still shutting down";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const startedAt = Date.now();
    const workers = installScriptedSharedWorkers(() => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type === "acquire-foreground-node-lease") {
          port.emit({ type: "foreground-node-lease-busy", message: busyMessage });
        }
      });
    });

    const acquire = SharedBrowserForegroundNodeLease.acquire({
      dbName,
      storageOwner: "scripted-owner",
    });
    const rejected = expect(acquire).rejects.toThrow(busyMessage);
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;

    expect(Date.now() - startedAt).toBe(10_000);
    expect(workers.ports.length).toBeGreaterThan(1);
    expect(workers.ports.length).toBeLessThanOrEqual(8);
    expect(new Set(workers.names)).toEqual(new Set([`${identity.baseName}:generation-0`]));
    expect(storage.has(identity.storageKey)).toBe(false);
    expect(workers.ports.every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.every((port) => port.listenerCount() === 0)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("caps physical busy attempts across closing worker generations", async () => {
    vi.useFakeTimers();
    const dbName = "lease-busy-closing-exhaustion-root";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const startedAt = Date.now();
    const workers = installScriptedSharedWorkers((_name, index) => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type !== "acquire-foreground-node-lease") return;
        if (index % 2 === 0) {
          port.emit({
            type: "foreground-node-lease-busy",
            message: `physical owner still releasing attempt ${index / 2 + 1}`,
          });
        } else {
          port.emit({ type: "worker-closing" });
        }
      });
    });

    const observed = SharedBrowserForegroundNodeLease.acquire({
      dbName,
      storageOwner: "scripted-owner",
    }).then(
      (lease) => ({ lease, error: null }),
      (error: unknown) => ({ lease: null, error }),
    );
    await vi.runAllTimersAsync();
    const outcome = await observed;

    expect(outcome.lease).toBeNull();
    expect(outcome.error).toEqual(new Error("physical owner still releasing attempt 8"));
    expect(Date.now() - startedAt).toBe(1_125);
    expect(workers.ports).toHaveLength(15);
    expect(new Set(workers.names)).toEqual(
      new Set(
        Array.from(
          { length: 8 },
          (_, generation) => `${identity.baseName}:generation-${generation}`,
        ),
      ),
    );
    expect(storage).toEqual(new Map([[identity.storageKey, "7"]]));
    expect(workers.ports.every((port) => port.closeCalls > 0)).toBe(true);
    expect(workers.ports.every((port) => port.listenerCount() === 0)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("keeps generic runtime errors terminal without changing generation", async () => {
    vi.useFakeTimers();
    const dbName = "runtime-terminal-error-root";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const workers = installScriptedSharedWorkers(() => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type === "connect-runtime") {
          port.emit({ type: "runtime-error", message: "permanent runtime configuration failure" });
        }
      });
    });
    const harness = runtimeHarness();
    const connection = new SharedBrowserWorkerConnection(
      harness.runtime as never,
      runtimeOptions(dbName) as never,
      "terminal-fingerprint",
      harness.callbacks,
    );

    await expect(connection.ready()).rejects.toThrow("permanent runtime configuration failure");
    expect(harness.onFailure).toHaveBeenCalledOnce();
    expect(harness.onFailure.mock.calls[0]?.[0]).toEqual(
      new Error("permanent runtime configuration failure"),
    );
    expect(workers.names).toEqual([`${identity.baseName}:generation-0`]);
    expect(storage.has(identity.storageKey)).toBe(false);
    expect(workers.ports[0]?.closeCalls).toBeGreaterThan(0);
    expect(workers.ports[0]?.listenerCount()).toBe(0);
    expect(vi.getTimerCount()).toBe(0);

    await expect(connection.shutdown()).resolves.toBeUndefined();
  });

  it("keeps generic foreground lease errors terminal without changing generation", async () => {
    vi.useFakeTimers();
    const dbName = "lease-terminal-error-root";
    const storage = installGenerationStorage();
    const identity = generationIdentity(dbName);
    const workers = installScriptedSharedWorkers(() => {
      return new ScriptedMessagePort((message, port) => {
        if (message.type === "acquire-foreground-node-lease") {
          port.emit({
            type: "foreground-node-lease-error",
            message: "permanent lease storage failure",
          });
        }
      });
    });

    await expect(
      SharedBrowserForegroundNodeLease.acquire({
        dbName,
        storageOwner: "scripted-owner",
      }),
    ).rejects.toThrow("permanent lease storage failure");
    expect(workers.names).toEqual([`${identity.baseName}:generation-0`]);
    expect(storage.has(identity.storageKey)).toBe(false);
    expect(workers.ports[0]?.closeCalls).toBeGreaterThan(0);
    expect(workers.ports[0]?.listenerCount()).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
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
