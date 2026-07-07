import type { Session } from "../runtime/context.js";
import {
  createDb,
  type Db,
  type DbConfig,
  type QueryBuilder,
  type QueryOptions,
} from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import type {
  SubscriptionChannel,
  SubscriptionChannelCallback,
} from "../runtime/subscription-channel.js";
import { acquireWebLockWithRetry, type LeaderLockLease } from "../runtime/leader-lock.js";

/**
 * SubscriptionChannel backed by the browser persistent runtime.
 *
 * In browser persistent mode, createDb opens the OPFS-owning worker runtime and
 * all subscription traffic crosses that postMessage boundary. The channel owner
 * is shared per persistent namespace so multiple tabs attach to one owning
 * worker/node instead of each opening OPFS independently.
 *
 * Multi-tab coordination reuses the battle-tested main-branch Web Locks
 * strategy: the owner acquires one exclusive namespace lock where Web Locks are
 * available, releases it on clean shutdown, and treats lock loss as fatal for
 * future calls. Reconnect resets are safe because subscriptions resubscribe via
 * the normal known-state path.
 */
export class BrowserWorkerSubscriptionChannel implements SubscriptionChannel {
  private readonly owner: SharedBrowserWorkerOwner;
  private closed = false;

  constructor(config: DbConfig) {
    this.owner = acquireSharedBrowserWorkerOwner(config);
  }

  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: SubscriptionChannelCallback<T>,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    let unsubscribe: (() => void) | undefined;
    let cancelled = false;

    void this.owner
      .db()
      .then((db) => {
        if (cancelled || this.closed) return;
        unsubscribe = db.subscribeAll<T>(
          query,
          (delta: SubscriptionDelta<T>) => callback(delta),
          options,
          session,
        );
        if (cancelled || this.closed) {
          unsubscribe();
          unsubscribe = undefined;
        }
      })
      .catch((error: unknown) => {
        setTimeout(() => {
          throw error;
        }, 0);
      });

    return () => {
      cancelled = true;
      unsubscribe?.();
      unsubscribe = undefined;
    };
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await releaseSharedBrowserWorkerOwner(this.owner);
  }
}

export function createBrowserWorkerSubscriptionChannel(
  config: DbConfig,
): BrowserWorkerSubscriptionChannel {
  return new BrowserWorkerSubscriptionChannel(config);
}

type SharedBrowserWorkerOwner = {
  key: string;
  refCount: number;
  dbPromise: Promise<Db>;
  leasePromise: Promise<LeaderLockLease | null>;
  lease: LeaderLockLease | null;
  lockLost: unknown;
  closed: boolean;
  db(): Promise<Db>;
};

const sharedOwners = new Map<string, SharedBrowserWorkerOwner>();

function acquireSharedBrowserWorkerOwner(config: DbConfig): SharedBrowserWorkerOwner {
  const key = browserWorkerOwnerKey(config);
  const existing = sharedOwners.get(key);
  if (existing && !existing.closed && !existing.lockLost) {
    existing.refCount++;
    return existing;
  }

  const lockName = browserWorkerLockName(config);
  let owner!: SharedBrowserWorkerOwner;
  const leasePromise = acquireWebLockWithRetry(lockName, {
    timeoutMs: 1_000,
    retryDelayMs: 25,
    onLost: (reason) => {
      owner.lockLost = reason;
      sharedOwners.delete(key);
    },
  });
  const dbPromise = leasePromise.then((lease) => {
    owner.lease = lease;
    if (owner.closed) {
      lease?.release();
      throw new Error("Browser worker subscription channel closed before open");
    }
    return createDb({ ...config, asyncSubscriptionsOnly: false });
  });

  owner = {
    key,
    refCount: 1,
    dbPromise,
    leasePromise,
    lease: null,
    lockLost: null,
    closed: false,
    async db() {
      if (this.lockLost) {
        throw new Error(`Browser worker leader lock was lost: ${String(this.lockLost)}`);
      }
      return await this.dbPromise;
    },
  };
  sharedOwners.set(key, owner);
  return owner;
}

async function releaseSharedBrowserWorkerOwner(owner: SharedBrowserWorkerOwner): Promise<void> {
  owner.refCount--;
  if (owner.refCount > 0) return;
  if (sharedOwners.get(owner.key) === owner) {
    sharedOwners.delete(owner.key);
  }
  owner.closed = true;
  const db = await owner.dbPromise.catch(() => null);
  await db?.shutdown();
  const lease = owner.lease ?? (await owner.leasePromise.catch(() => null));
  lease?.release();
}

function browserWorkerOwnerKey(config: DbConfig): string {
  return JSON.stringify({
    appId: config.appId,
    dbName: config.dbName ?? config.appId,
    env: config.env ?? null,
    userBranch: config.userBranch ?? null,
    serverUrl: config.serverUrl ?? null,
    driver: config.driver ?? null,
    runtimeSources: config.runtimeSources ?? null,
  });
}

function browserWorkerLockName(config: DbConfig): string {
  return `jazz-worker:${config.appId}:${config.dbName ?? config.appId}`;
}

export function __browserWorkerSubscriptionChannelDiagnostics() {
  return {
    owners: [...sharedOwners.values()].map((owner) => ({
      key: owner.key,
      refCount: owner.refCount,
      hasLease: owner.lease !== null,
      closed: owner.closed,
      lockLost: owner.lockLost !== null,
    })),
  };
}

export async function __resetBrowserWorkerSubscriptionChannelsForTests(): Promise<void> {
  const owners = [...sharedOwners.values()];
  sharedOwners.clear();
  await Promise.all(
    owners.map(async (owner) => {
      owner.refCount = 1;
      await releaseSharedBrowserWorkerOwner(owner);
    }),
  );
}
