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

/**
 * SubscriptionChannel backed by the browser persistent runtime.
 *
 * In browser persistent mode, createDb opens the existing OPFS-owning worker
 * runtime and all subscription traffic crosses that postMessage boundary. This
 * class is deliberately topology-agnostic: it owns no sync public Db surface,
 * only the async subscription channel required by asyncSubscriptionsOnly.
 *
 * Slice 2 uses the existing dedicated-worker OPFS owner. SharedWorker and
 * multi-tab leadership stay a slice-3 concern.
 */
export class BrowserWorkerSubscriptionChannel implements SubscriptionChannel {
  private readonly dbPromise: Promise<Db>;
  private closed = false;

  constructor(config: DbConfig) {
    this.dbPromise = createDb({ ...config, asyncSubscriptionsOnly: false });
  }

  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: SubscriptionChannelCallback<T>,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    let unsubscribe: (() => void) | undefined;
    let cancelled = false;

    void this.dbPromise
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
    const db = await this.dbPromise;
    await db.shutdown();
  }
}

export function createBrowserWorkerSubscriptionChannel(
  config: DbConfig,
): BrowserWorkerSubscriptionChannel {
  return new BrowserWorkerSubscriptionChannel(config);
}
