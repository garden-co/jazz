import { describe, expect, it } from "vitest";
import {
  acquireWebLockWithRetry,
  monitorWebLockRelease,
  stealAndReleaseWebLock,
  tryAcquireWebLock,
} from "../../src/runtime/leader-lock.js";

function uniqueLockName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitForLease(
  tryAcquire: () => Promise<{ release(): void } | null>,
  timeoutMs: number,
) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const lease = await tryAcquire();
    if (lease) return lease;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return null;
}

describe("leader-lock browser integration", () => {
  it("supports fail-fast acquisition through the shared Web Locks helper", async () => {
    const lockName = uniqueLockName("leader-lock-helper");
    const leaseA = await tryAcquireWebLock(lockName);
    expect(leaseA).not.toBeNull();

    const leaseWhileHeld = await tryAcquireWebLock(lockName);
    expect(leaseWhileHeld).toBeNull();

    leaseA!.release();

    const leaseAfterRelease = await waitForLease(() => tryAcquireWebLock(lockName), 1000);
    expect(leaseAfterRelease).not.toBeNull();
    leaseAfterRelease!.release();
  });

  it("retries acquisition until a released lock is observable", async () => {
    const lockName = uniqueLockName("leader-lock-retry");
    const leaseA = await tryAcquireWebLock(lockName);
    expect(leaseA).not.toBeNull();

    const retry = acquireWebLockWithRetry(lockName, {
      timeoutMs: 1_000,
      retryDelayMs: 20,
    });

    setTimeout(() => leaseA!.release(), 50);

    const leaseAfterRelease = await retry;
    expect(leaseAfterRelease).not.toBeNull();
    leaseAfterRelease!.release();
  });

  it("monitors an unexpected lock release and releases the monitor grant", async () => {
    const lockName = uniqueLockName("leader-lock-monitor");
    const lease = await tryAcquireWebLock(lockName);
    expect(lease).not.toBeNull();

    const grants: string[] = [];
    const monitor = monitorWebLockRelease(lockName, {
      onGranted: () => {
        grants.push(lockName);
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(grants).toEqual([]);

    lease!.release();

    await waitForCondition(() => grants.length === 1, 1000, "monitor was not granted");
    monitor.cancel();

    const reacquired = await waitForLease(() => tryAcquireWebLock(lockName), 1000);
    expect(reacquired).not.toBeNull();
    reacquired!.release();
  });

  it("cancels a queued monitor without reporting a release", async () => {
    const lockName = uniqueLockName("leader-lock-monitor-cancel");
    const lease = await tryAcquireWebLock(lockName);
    expect(lease).not.toBeNull();

    const grants: string[] = [];
    const monitor = monitorWebLockRelease(lockName, {
      onGranted: () => {
        grants.push(lockName);
      },
    });
    monitor.cancel();
    lease!.release();

    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(grants).toEqual([]);

    const reacquired = await waitForLease(() => tryAcquireWebLock(lockName), 1000);
    expect(reacquired).not.toBeNull();
    reacquired!.release();
  });

  it("steals and immediately releases a stuck lock only when explicitly requested", async () => {
    const lockName = uniqueLockName("leader-lock-steal");
    const lease = await tryAcquireWebLock(lockName);
    expect(lease).not.toBeNull();

    const blocked = await tryAcquireWebLock(lockName);
    expect(blocked).toBeNull();

    await stealAndReleaseWebLock(lockName);

    const reacquired = await waitForLease(() => tryAcquireWebLock(lockName), 1000);
    expect(reacquired).not.toBeNull();
    reacquired!.release();
    lease!.release();
  });

  it("reports an acquired lock as lost when it is stolen", async () => {
    const lockName = uniqueLockName("leader-lock-on-lost");
    const lostReasons: unknown[] = [];
    const lease = await tryAcquireWebLock(lockName, {
      onLost: (reason) => {
        lostReasons.push(reason);
      },
    });
    expect(lease).not.toBeNull();

    await stealAndReleaseWebLock(lockName);

    await waitForCondition(() => lostReasons.length === 1, 1000, "lock loss was not reported");
    lease!.release();
  });

  it("does not report an intentionally released lock as lost", async () => {
    const lockName = uniqueLockName("leader-lock-on-lost-release");
    const lostReasons: unknown[] = [];
    const lease = await tryAcquireWebLock(lockName, {
      onLost: (reason) => {
        lostReasons.push(reason);
      },
    });
    expect(lease).not.toBeNull();

    lease!.release();
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(lostReasons).toEqual([]);
  });
});

async function waitForCondition(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(message);
}
