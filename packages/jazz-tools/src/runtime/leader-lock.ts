import { normalizePositiveTimeout } from "./browser-broker-protocol.js";

export interface LeaderLockLease {
  release(): void;
}

export interface WebLockAcquireOptions {
  lockManager?: LockManagerLike | null;
  onLost?: (reason: unknown) => void;
}

export interface WebLockRetryOptions extends WebLockAcquireOptions {
  timeoutMs?: number;
  retryDelayMs?: number;
}

const DEFAULT_WEB_LOCK_RETRY_TIMEOUT_MS = 250;
const DEFAULT_WEB_LOCK_RETRY_DELAY_MS = 20;

interface LockManagerLike {
  request<T>(
    name: string,
    options: {
      mode?: "exclusive" | "shared";
      ifAvailable?: boolean;
      steal?: boolean;
      signal?: AbortSignal;
    },
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

export async function tryAcquireWebLock(
  lockName: string,
  optionsOrLockManager?: WebLockAcquireOptions | LockManagerLike | null,
): Promise<LeaderLockLease | null> {
  const { lockManager, onLost } = normalizeAcquireOptions(optionsOrLockManager);
  if (!lockManager) return null;

  let resolveAcquired: ((lease: LeaderLockLease | null) => void) | null = null;
  const acquiredPromise = new Promise<LeaderLockLease | null>((resolve) => {
    resolveAcquired = resolve;
  });

  let acquired = false;
  let releasedIntentionally = false;
  let lossReported = false;
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

      acquired = true;
      resolveAcquired?.({
        release: () => {
          if (!releaseLock) return;
          releasedIntentionally = true;
          releaseLock();
          releaseLock = null;
        },
      });
      resolveAcquired = null;
      await heldUntilReleased;
    })
    .then(
      () => {
        if (acquired && !releasedIntentionally) {
          reportLockLost();
        }
      },
      (error) => {
        if (acquired && !releasedIntentionally) {
          reportLockLost(error);
          return;
        }
        resolveAcquired?.(null);
        resolveAcquired = null;
      },
    );

  function reportLockLost(reason: unknown = new Error(`Web Lock ${lockName} was lost`)): void {
    if (lossReported) return;
    lossReported = true;
    onLost?.(reason);
  }

  return await acquiredPromise;
}

export async function acquireWebLockWithRetry(
  lockName: string,
  options: WebLockRetryOptions = {},
): Promise<LeaderLockLease | null> {
  const timeoutMs = normalizePositiveTimeout(options.timeoutMs, DEFAULT_WEB_LOCK_RETRY_TIMEOUT_MS);
  const retryDelayMs = normalizePositiveTimeout(
    options.retryDelayMs,
    DEFAULT_WEB_LOCK_RETRY_DELAY_MS,
  );
  const deadline = Date.now() + timeoutMs;

  while (true) {
    const lease = await tryAcquireWebLock(lockName, options);
    if (lease) return lease;

    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) return null;
    await sleep(Math.min(retryDelayMs, remainingMs));
  }
}

function normalizeAcquireOptions(
  optionsOrLockManager: WebLockAcquireOptions | LockManagerLike | null | undefined,
): { lockManager: LockManagerLike | null; onLost?: (reason: unknown) => void } {
  if (optionsOrLockManager === undefined) {
    return { lockManager: resolveNavigatorLocks() };
  }
  if (optionsOrLockManager === null) {
    return { lockManager: null };
  }
  if (isLockManagerLike(optionsOrLockManager)) {
    return { lockManager: optionsOrLockManager };
  }
  return {
    lockManager:
      optionsOrLockManager.lockManager === undefined
        ? resolveNavigatorLocks()
        : optionsOrLockManager.lockManager,
    onLost: optionsOrLockManager.onLost,
  };
}

function isLockManagerLike(value: unknown): value is LockManagerLike {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { request?: unknown }).request === "function"
  );
}

export interface WebLockMonitor {
  cancel(): void;
}

export interface WebLockMonitorOptions {
  onGranted(): void;
  onError?(error: unknown): void;
  lockManager?: LockManagerLike | null;
}

export function monitorWebLockRelease(
  lockName: string,
  options: WebLockMonitorOptions,
): WebLockMonitor {
  const lockManager =
    options.lockManager === undefined ? resolveNavigatorLocks() : options.lockManager;
  if (!lockManager) {
    queueMicrotask(() => options.onError?.(new Error("Web Locks are unavailable")));
    return { cancel() {} };
  }

  const controller = new AbortController();
  let cancelled = false;

  void lockManager
    .request(lockName, { mode: "exclusive", signal: controller.signal }, (lock) => {
      if (lock && !cancelled) {
        options.onGranted();
      }
      return undefined;
    })
    .catch((error) => {
      if (cancelled || isAbortError(error)) {
        return;
      }
      options.onError?.(error);
    });

  return {
    cancel() {
      if (cancelled) return;
      cancelled = true;
      controller.abort();
    },
  };
}

export async function stealAndReleaseWebLock(
  lockName: string,
  lockManager: LockManagerLike | null = resolveNavigatorLocks(),
): Promise<void> {
  if (!lockManager) {
    throw new Error("Web Locks are unavailable");
  }

  await lockManager.request(lockName, { mode: "exclusive", steal: true }, () => undefined);
}

function isAbortError(error: unknown): boolean {
  return (
    error instanceof DOMException &&
    (error.name === "AbortError" || error.name === "InvalidStateError")
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
