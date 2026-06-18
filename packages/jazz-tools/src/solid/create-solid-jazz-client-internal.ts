import { createMemo, createResource, createSignal, onCleanup, type Accessor } from "solid-js";
import type { DbConfig } from "../runtime/db.js";
import { type JazzClient } from "../web/create-jazz-client.js";

export type JazzClientFactory = (config: DbConfig) => Promise<JazzClient>;

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
    async (nextConfig): Promise<JazzClient> => {
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
        void rawClient?.shutdown();
      });

      rawClient = await clientFactory(nextConfig);
      if (disposed || runId !== activeRunId()) {
        disconnectRunId();
        await rawClient.shutdown();
        return rawClient;
      }
      connectRunId();

      return {
        ...rawClient,
        shutdown: () => {
          disconnectRunId();
          return rawClient.shutdown();
        },
      };
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
