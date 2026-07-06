import type { Session } from "../runtime/context.js";
import { acquireClient, releaseClient } from "../runtime/client-registry.js";
import type { Db, DbConfig, QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { createDb } from "../runtime/db.js";
import type { SubscriptionChannel } from "../runtime/subscription-channel.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import { createDbFromInspectedPage } from "../dev-tools/index.js";
import { registerWindowJazzStorageClient } from "../window-client-storage.js";
import { createBrowserWorkerSubscriptionChannel } from "./browser-subscription-channel.js";

export {
  BrowserWorkerSubscriptionChannel,
  createBrowserWorkerSubscriptionChannel,
} from "./browser-subscription-channel.js";

type AsyncSubscriptionsOnlyConfig<TAsyncSubscriptionsOnly extends boolean> =
  TAsyncSubscriptionsOnly extends false
    ? { asyncSubscriptionsOnly: false }
    : { asyncSubscriptionsOnly?: true };

export type JazzClientConfig<TAsyncSubscriptionsOnly extends boolean = true> = DbConfig &
  AsyncSubscriptionsOnlyConfig<TAsyncSubscriptionsOnly> & {
    /**
     * API-level subscription channel. Slice 1 uses an in-process implementation
     * in tests; browser worker/postMessage transport is slice 2.
     */
    subscriptionChannel?: SubscriptionChannel;
  };

export interface AsyncOnlyJazzClient {
  session: Session | null;
  shutdown(): Promise<void>;
}

export interface SyncJazzClient {
  db: Db;
  session: Session | null;
  shutdown(): Promise<void>;
}

export type JazzClient<TAsyncSubscriptionsOnly extends boolean = true> =
  TAsyncSubscriptionsOnly extends true ? AsyncOnlyJazzClient : SyncJazzClient;

type ChannelBackedClient = AsyncOnlyJazzClient & {
  subscriptionChannel: SubscriptionChannel;
};

type SyncClientWithChannel = SyncJazzClient & {
  subscriptionChannel?: SubscriptionChannel;
};

class DualSubscriptionTarget {
  constructor(
    private readonly db: Db,
    private readonly channel?: SubscriptionChannel,
  ) {}

  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    if (options?.subscriptionMode === "async") {
      if (!this.channel) {
        throw new Error("subscriptionMode='async' requires a subscriptionChannel.");
      }
      return this.channel.subscribeAll(query, callback, options, session);
    }
    return this.db.subscribeAll(query, callback, options, session);
  }
}

async function createSyncJazzClientInternal(
  config: JazzClientConfig<false>,
): Promise<SyncClientWithChannel> {
  const { subscriptionChannel, asyncSubscriptionsOnly: _mode, ...dbConfig } = config;
  const db = await createDb(dbConfig);
  let session = db.getAuthState().session;
  const manager = new SubscriptionsOrchestrator(
    { appId: config.appId },
    new DualSubscriptionTarget(db, subscriptionChannel),
    session,
  );
  await manager.init();
  const stopSessionSync = db.onAuthChanged(({ session: nextSession }) => {
    session = nextSession ?? null;
    manager.setSession(nextSession ?? null);
  });
  const unregisterWindowJazzStorageClient = registerWindowJazzStorageClient(db);

  return attachSubscriptionStore(
    {
      db,
      get session() {
        return session;
      },
      async shutdown() {
        stopSessionSync?.();
        unregisterWindowJazzStorageClient();
        await manager.shutdown();
        await subscriptionChannel?.shutdown?.();
        await db.shutdown();
      },
      subscriptionChannel,
    },
    manager,
  );
}

async function createAsyncOnlyJazzClientInternal(
  config: JazzClientConfig<true>,
): Promise<ChannelBackedClient> {
  const subscriptionChannel =
    config.subscriptionChannel ?? createBrowserWorkerSubscriptionChannel(config);
  const session = config.cookieSession ?? null;
  const manager = new SubscriptionsOrchestrator(
    { appId: config.appId },
    subscriptionChannel,
    session,
  );
  await manager.init();

  return attachSubscriptionStore(
    {
      session,
      async shutdown() {
        await manager.shutdown();
        await subscriptionChannel.shutdown?.();
      },
      subscriptionChannel,
    },
    manager,
  );
}

const channelIds = new WeakMap<object, number>();
let nextChannelId = 1;

function configKey(config: JazzClientConfig<boolean>): string {
  const { subscriptionChannel, ...serializable } = config;
  const channelKey = subscriptionChannel
    ? (channelIds.get(subscriptionChannel) ??
      (() => {
        const id = nextChannelId++;
        channelIds.set(subscriptionChannel, id);
        return id;
      })())
    : null;
  return JSON.stringify({ ...serializable, subscriptionChannel: channelKey });
}

export function createJazzClient(config: JazzClientConfig<false>): Promise<SyncJazzClient>;
export function createJazzClient(config: JazzClientConfig<true>): Promise<AsyncOnlyJazzClient>;
export function createJazzClient(config: JazzClientConfig): Promise<AsyncOnlyJazzClient>;
export function createJazzClient(config: DbConfig): Promise<AsyncOnlyJazzClient | SyncJazzClient>;
export function createJazzClient(
  config: JazzClientConfig<boolean>,
): Promise<AsyncOnlyJazzClient | SyncJazzClient> {
  const key = configKey(config);
  const holder = {};
  const asyncOnly = config.asyncSubscriptionsOnly !== false;
  const shared = acquireClient<AsyncOnlyJazzClient | SyncJazzClient>(
    key,
    () =>
      asyncOnly
        ? createAsyncOnlyJazzClientInternal(config as JazzClientConfig<true>)
        : createSyncJazzClientInternal(config as JazzClientConfig<false>),
    holder,
  );
  return trackPromise(
    shared.then((client) =>
      attachSubscriptionStore(
        hasDb(client)
          ? {
              db: client.db,
              get session() {
                return client.session;
              },
              shutdown() {
                return releaseClient(key, holder);
              },
            }
          : {
              get session() {
                return client.session;
              },
              shutdown() {
                return releaseClient(key, holder);
              },
            },
        getSubscriptionStore(client),
      ),
    ),
  ) as Promise<AsyncOnlyJazzClient | SyncJazzClient>;
}

function hasDb(client: AsyncOnlyJazzClient | SyncJazzClient): client is SyncJazzClient {
  return "db" in client;
}

async function createExtensionJazzClientInternal(): Promise<SyncJazzClient> {
  const db = await createDbFromInspectedPage();
  const connectedConfig = db.getConfig();
  if (!connectedConfig) {
    throw new Error("DevTools bridge did not provide an inspected page config.");
  }
  let session = db.getAuthState().session;
  const manager = new SubscriptionsOrchestrator({ appId: connectedConfig.appId }, db);
  await manager.init();
  const stopSessionSync = db.onAuthChanged(({ session: nextSession }) => {
    session = nextSession ?? null;
    manager.setSession(nextSession ?? null);
  });

  return attachSubscriptionStore(
    {
      db,
      get session() {
        return session;
      },
      async shutdown() {
        stopSessionSync?.();
        await manager.shutdown();
        await db.shutdown();
      },
    },
    manager,
  );
}

export function createExtensionJazzClient(): Promise<SyncJazzClient> {
  return trackPromise(createExtensionJazzClientInternal());
}
