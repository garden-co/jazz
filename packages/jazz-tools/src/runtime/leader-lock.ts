export interface LeaderLockLease {
  release(): void;
}

export interface LeaderLockStrategy {
  tryAcquire(lockName: string): Promise<LeaderLockLease | null>;
}

interface LockManagerLike {
  request<T>(
    name: string,
    options: { mode?: "exclusive" | "shared"; ifAvailable?: boolean },
    callback: (lock: unknown | null) => Promise<T> | T,
  ): Promise<T>;
}

function resolveNavigatorLocks(): LockManagerLike | null {
  const nav = (globalThis as { navigator?: { locks?: unknown } }).navigator;
  if (!nav || !nav.locks) return null;
  const locks = nav.locks as { request?: unknown };
  if (typeof locks.request !== "function") return null;
  return locks as unknown as LockManagerLike;
}

export function createNavigatorLocksLeaderLockStrategy(
  lockManager: LockManagerLike | null = resolveNavigatorLocks(),
): LeaderLockStrategy | null {
  if (!lockManager) return null;

  return {
    async tryAcquire(lockName: string): Promise<LeaderLockLease | null> {
      let resolveAcquired: ((lease: LeaderLockLease | null) => void) | null = null;
      const acquiredPromise = new Promise<LeaderLockLease | null>((resolve) => {
        resolveAcquired = resolve;
      });

      let releaseLock: (() => void) | null = null;
      const heldUntilReleased = new Promise<void>((resolve) => {
        releaseLock = () => resolve();
      });

      void lockManager
        .request(lockName, { mode: "exclusive", ifAvailable: true }, async (lock) => {
          if (!lock) {
            resolveAcquired?.(null);
            resolveAcquired = null;
            return;
          }

          resolveAcquired?.({
            release: () => {
              if (!releaseLock) return;
              releaseLock();
              releaseLock = null;
            },
          });
          resolveAcquired = null;
          await heldUntilReleased;
        })
        .catch(() => {
          resolveAcquired?.(null);
          resolveAcquired = null;
        });

      return await acquiredPromise;
    },
  };
}
