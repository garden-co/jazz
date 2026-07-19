import { createMemo, createResource, createSignal, onCleanup, type Accessor } from "solid-js";
import type { DbConfig } from "../runtime/db.js";
import { type SyncJazzClient } from "../web/create-jazz-client.js";
import {
  attachSubscriptionStore,
  subscriptionStoreKey,
  type WithSubscriptionStore,
} from "../subscription-store-internal.js";

export type JazzClientFactory = (config: DbConfig) => Promise<SyncJazzClient>;

export function createSolidJazzClientInternal(
  config: Accessor<DbConfig>,
  clientFactory: JazzClientFactory,
) {
  let disposed = false;
  onCleanup(() => {
    disposed = true;
  });

  const [activeRunId, setActiveRunId] = createSignal(0);
  const [connectedRunId, setConnectedRunId] = createSignal<number | undefined>(undefined);

  const stableConfig = createMemo(config, undefined, {
    equals: (prev, next) => JSON.stringify(prev) === JSON.stringify(next),
  });

  const [res, { mutate, refetch }] = createResource(
    stableConfig,
    async (nextConfig): Promise<SyncJazzClient> => {
      const runId = activeRunId() + 1;
      setActiveRunId(runId);

      const connectRunId = () => {
        if (activeRunId() === runId) {
          setConnectedRunId(runId);
        }
      };
      const disconnectRunId = () => {
        if (connectedRunId() === runId) {
          setConnectedRunId(undefined);
        }
      };

      let rawClient: SyncJazzClient | undefined;
      onCleanup(() => {
        disconnectRunId();
        void rawClient?.shutdown();
      });

      rawClient = await clientFactory(nextConfig);
      if (disposed || runId !== activeRunId()) {
        disconnectRunId();
        await rawClient.shutdown();
        return rawClient;
      }
      connectRunId();

      const wrappedClient = {
        ...rawClient,
        shutdown: () => {
          disconnectRunId();
          return rawClient.shutdown();
        },
      };
      const subscriptionStore = (rawClient as Partial<WithSubscriptionStore>)[subscriptionStoreKey];
      return subscriptionStore
        ? attachSubscriptionStore(wrappedClient, subscriptionStore)
        : wrappedClient;
    },
    {
      // Disables Hydration
      ssrLoadFrom: "initial",
      initialValue: undefined,
    },
  );

  return {
    get client() {
      const currentClient = res();
      if (!currentClient || connectedRunId() !== activeRunId()) {
        return undefined;
      }
      return currentClient;
    },

    get loading() {
      return res.loading;
    },

    get error() {
      return res.error;
    },

    get state() {
      return res.state;
    },

    mutate,
    refetch,
  };
}
