import { describe, expect, it } from "vitest";
import { createNavigatorLocksLeaderLockStrategy } from "../../src/runtime/leader-lock.js";

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
  it("acquires an exclusive lease and blocks concurrent acquisition until release", async () => {
    const strategyA = createNavigatorLocksLeaderLockStrategy();
    const strategyB = createNavigatorLocksLeaderLockStrategy();
    expect(strategyA).not.toBeNull();
    expect(strategyB).not.toBeNull();

    const lockName = uniqueLockName("leader-lock");
    const leaseA = await strategyA!.tryAcquire(lockName);
    expect(leaseA).not.toBeNull();

    const leaseWhileHeld = await strategyB!.tryAcquire(lockName);
    expect(leaseWhileHeld).toBeNull();

    leaseA!.release();

    const leaseAfterRelease = await waitForLease(() => strategyB!.tryAcquire(lockName), 1000);
    expect(leaseAfterRelease).not.toBeNull();
    leaseAfterRelease!.release();
  });
});
