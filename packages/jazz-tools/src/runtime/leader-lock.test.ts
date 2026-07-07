import { describe, expect, it } from "vitest";
import {
  acquireWebLockWithRetry,
  monitorWebLockRelease,
  stealAndReleaseWebLock,
  tryAcquireWebLock,
} from "./leader-lock.js";

class FakeLockManager {
  private held = new Set<string>();
  private waiters = new Map<string, Array<() => void>>();

  async request<T>(
    name: string,
    options: {
      ifAvailable?: boolean;
      steal?: boolean;
      signal?: AbortSignal;
    },
    callback: (lock: unknown | null) => Promise<T> | T,
  ): Promise<T> {
    if (options.steal) {
      this.release(name);
      return await callback({});
    }
    if (options.ifAvailable && this.held.has(name)) {
      return await callback(null);
    }
    while (this.held.has(name)) {
      await new Promise<void>((resolve, reject) => {
        const waiters = this.waiters.get(name) ?? [];
        waiters.push(resolve);
        this.waiters.set(name, waiters);
        options.signal?.addEventListener(
          "abort",
          () => reject(new DOMException("aborted", "AbortError")),
          { once: true },
        );
      });
    }
    this.held.add(name);
    try {
      return await callback({});
    } finally {
      this.release(name);
    }
  }

  private release(name: string) {
    this.held.delete(name);
    const waiters = this.waiters.get(name) ?? [];
    this.waiters.delete(name);
    for (const waiter of waiters) waiter();
  }
}

describe("leader-lock helper", () => {
  it("supports fail-fast acquisition and reacquisition", async () => {
    const lockManager = new FakeLockManager();
    const leaseA = await tryAcquireWebLock("leader-a", lockManager);
    expect(leaseA).not.toBeNull();

    const blocked = await tryAcquireWebLock("leader-a", lockManager);
    expect(blocked).toBeNull();

    leaseA!.release();
    const leaseB = await acquireWebLockWithRetry("leader-a", {
      lockManager,
      timeoutMs: 1_000,
      retryDelayMs: 5,
    });
    expect(leaseB).not.toBeNull();
    leaseB!.release();
  });

  it("monitors release and can explicitly steal stale locks", async () => {
    const lockManager = new FakeLockManager();
    const lease = await tryAcquireWebLock("leader-b", lockManager);
    expect(lease).not.toBeNull();

    let granted = 0;
    const monitor = monitorWebLockRelease("leader-b", {
      lockManager,
      onGranted: () => {
        granted++;
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(granted).toBe(0);

    await stealAndReleaseWebLock("leader-b", lockManager);
    await waitFor(() => granted === 1);
    monitor.cancel();
    lease!.release();
  });
});

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 1_000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("condition did not become true");
}
