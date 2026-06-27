import type {
  CacheEntryHandle,
  SubscriptionsOrchestrator,
  UseAllState,
} from "./subscriptions-orchestrator.js";
import type { QueryBuilder, QueryOptions } from "./runtime/db.js";

export type SubscriptionStore = Pick<
  SubscriptionsOrchestrator,
  "computeKey" | "getCacheEntry" | "makeQueryKey" | "peekState"
>;

export const subscriptionStoreKey: unique symbol = Symbol("jazz.subscriptionStore");

export type WithSubscriptionStore = {
  [subscriptionStoreKey]: SubscriptionStore;
};

export function attachSubscriptionStore<T extends object>(
  target: T,
  store: SubscriptionStore,
): T & WithSubscriptionStore {
  Object.defineProperty(target, subscriptionStoreKey, {
    configurable: false,
    enumerable: false,
    value: store,
    writable: false,
  });
  return target as T & WithSubscriptionStore;
}

export function getSubscriptionStore(client: object): SubscriptionStore {
  const store = (client as Partial<WithSubscriptionStore>)[subscriptionStoreKey];
  if (!store) {
    throw new Error("Jazz client is missing its internal subscription store.");
  }
  return store;
}

export type { CacheEntryHandle, QueryBuilder, QueryOptions, UseAllState };
