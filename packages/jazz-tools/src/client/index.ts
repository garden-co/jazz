export {
  BrowserWorkerSubscriptionChannel,
  createBrowserWorkerSubscriptionChannel,
  createExtensionJazzClient,
  createJazzClient,
  type AsyncChannelDb,
  type AsyncOnlyJazzClient,
  type JazzClient,
  type JazzClientConfig,
  type SyncJazzClient,
} from "../web/create-jazz-client.js";
export { BrowserAuthSecretStore } from "../runtime/auth-secret-store.js";
export type {
  AuthSecretStore,
  BrowserAuthSecretStoreOptions,
} from "../runtime/auth-secret-store.js";
export {
  applySubscriptionDelta,
  RowChangeKind,
  type RowDelta,
  type SubscriptionDelta,
} from "../runtime/subscription-manager.js";
export type { QueryBuilder, QueryOptions } from "../runtime/db.js";
export { getSubscriptionStore } from "../subscription-store-internal.js";
export type { CacheEntryHandle, UseAllState } from "../subscription-store-internal.js";

import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";

export function subscribeAll<T extends { id: string }>(
  client: object,
  query: QueryBuilder<T>,
  callback: (delta: SubscriptionDelta<T>) => void,
  options?: QueryOptions,
): () => void {
  const store = getSubscriptionStore(client);
  const key = store.makeQueryKey(query, options);
  const entry = store.getCacheEntry<T>(key);
  return entry.subscribe({
    onfulfilled: (data) => callback({ all: data, delta: [], reset: true }),
    onDelta: callback,
    onError: (error) => {
      setTimeout(() => {
        throw error;
      }, 0);
    },
  });
}
