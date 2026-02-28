export interface LeaderLockLease {
  release(): void;
}
export interface LeaderLockStrategy {
  tryAcquire(lockName: string): Promise<LeaderLockLease | null>;
}
interface LockManagerLike {
  request<T>(
    name: string,
    options: {
      mode?: "exclusive" | "shared";
      ifAvailable?: boolean;
    },
    callback: (lock: unknown | null) => Promise<T> | T,
  ): Promise<T>;
}
export declare function createNavigatorLocksLeaderLockStrategy(
  lockManager?: LockManagerLike | null,
): LeaderLockStrategy | null;

//# sourceMappingURL=leader-lock.d.ts.map
