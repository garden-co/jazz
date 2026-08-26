import { createMemo, createResource, createSignal, onCleanup, type Accessor } from "solid-js";
import { serializeClientConfig } from "../runtime/client-config-key.js";
import type { DbConfig } from "../runtime/db.js";
import type { JazzClient } from "../web/create-jazz-client.js";
import {
  attachSubscriptionStore,
  subscriptionStoreKey,
  type WithSubscriptionStore,
} from "../subscription-store-internal.js";

export type JazzClientFactory = (config: DbConfig) => Promise<JazzClient>;

export function createSolidJazzClientInternal(
  config: Accessor<DbConfig>,
  clientFactory: JazzClientFactory,
) {
  let disposed = false;
  let lifecycle: Promise<void> | undefined;
  let hasLifecycleError = false;
  let lifecycleError: unknown;
  onCleanup(() => {
    disposed = true;
  });

  const [activeRunId, setActiveRunId] = createSignal(0);
  const [connectedRunId, setConnectedRunId] = createSignal<number | undefined>(undefined);

  const stableConfig = createMemo(config, undefined, {
    equals: (prev, next) => serializeClientConfig(prev) === serializeClientConfig(next),
  });

  const [res, { mutate, refetch }] = createResource(
    stableConfig,
    (nextConfig): Promise<JazzClient | undefined> => {
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

      let rawClient: JazzClient | undefined;
      onCleanup(() => {
        disconnectRunId();
        if (rawClient) {
          const client = rawClient;
          lifecycle = (
            lifecycle ? lifecycle.then(() => client.shutdown()) : client.shutdown()
          ).then(
            () => undefined,
            (error) => {
              hasLifecycleError = true;
              lifecycleError = error;
            },
          );
        }
      });

      const run = async () => {
        if (disposed || runId !== activeRunId()) return undefined;
        if (hasLifecycleError) {
          const error = lifecycleError;
          hasLifecycleError = false;
          lifecycleError = undefined;
          throw error;
        }

        const client = await clientFactory(nextConfig);
        rawClient = client;
        if (disposed || runId !== activeRunId()) {
          disconnectRunId();
          try {
            await client.shutdown();
          } catch (error) {
            hasLifecycleError = true;
            lifecycleError = error;
          }
          return undefined;
        }
        connectRunId();

        const wrappedClient = {
          ...client,
          shutdown: () => {
            disconnectRunId();
            return client.shutdown();
          },
        };
        const subscriptionStore = (client as Partial<WithSubscriptionStore>)[subscriptionStoreKey];
        return subscriptionStore
          ? attachSubscriptionStore(wrappedClient, subscriptionStore)
          : wrappedClient;
      };
      const work = lifecycle ? lifecycle.then(run) : run();
      lifecycle = work.then(
        () => undefined,
        () => undefined,
      );
      return work;
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
