import type { Session } from "../runtime/context.js";
import { acquireClient, releaseClient } from "../runtime/client-registry.js";
import type { Db, DbConfig, QueryBuilder, QueryOptions, TableProxy } from "../runtime/db.js";
import type { CreateOptions, DeleteOptions, UpdateOptions } from "../runtime/client.js";
import type { AuthState } from "../runtime/auth-state.js";
import type {
  BinaryLargeValueFileApp,
  BinaryLargeValueFileRow,
  FileReadOptions,
  FileWriteOptions,
} from "../runtime/file-storage.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { createDb } from "../runtime/db.js";
import type {
  AsyncWriteHandle,
  AsyncWriteResult,
  SubscriptionChannel,
} from "../runtime/subscription-channel.js";
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
  db: AsyncChannelDb;
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

export interface AsyncChannelDb {
  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>;
  one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>;
  getAuthState(): AuthState;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
  updateAuthToken(token: string | null): void;
  getLocalFirstIdentityProof(options?: {
    ttlSeconds?: number;
    audience?: string;
  }): string | null | Promise<string | null>;
  getConfig(): DbConfig;
  /** @internal The channel backing this facade (see AsyncChannelDbFacade). */
  getSubscriptionChannel(): SubscriptionChannel;
  insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: CreateOptions,
  ): Promise<AsyncWriteResult<T>>;
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): Promise<AsyncWriteHandle>;
  delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: DeleteOptions,
  ): Promise<AsyncWriteHandle>;
  canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<boolean>;
  canUpdate<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): Promise<boolean>;
  canDelete<T, Init>(table: TableProxy<T, Init>, id: string): Promise<boolean>;
  createFileFromBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    blob: Blob,
    options?: FileWriteOptions,
  ): Promise<BinaryLargeValueFileRow<TApp>>;
  loadFileAsBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    fileOrId: string | BinaryLargeValueFileRow<TApp>,
    options?: FileReadOptions,
  ): Promise<Blob>;
}

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

class AsyncChannelDbFacade implements AsyncChannelDb {
  private authState: AuthState;

  constructor(
    private readonly channel: SubscriptionChannel,
    initialAuthState: AuthState,
    private readonly config: DbConfig,
  ) {
    this.authState = initialAuthState;
  }

  getAuthState(): AuthState {
    return this.authState;
  }

  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    if (!this.channel.all) {
      throw new Error("Subscription channel does not support one-shot reads.");
    }
    return Promise.resolve(this.channel.all(query, options, this.authState.session ?? undefined));
  }

  one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    if (!this.channel.one) {
      throw new Error("Subscription channel does not support one-shot reads.");
    }
    return Promise.resolve(this.channel.one(query, options, this.authState.session ?? undefined));
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    const unsubscribe = this.channel.onAuthChanged((state) => {
      this.authState = state;
      listener(state);
    });
    listener(this.authState);
    return unsubscribe;
  }

  updateAuthToken(token: string | null): void {
    void this.channel.updateAuthToken(token);
  }

  getLocalFirstIdentityProof(options?: {
    ttlSeconds?: number;
    audience?: string;
  }): Promise<string | null> {
    return Promise.resolve(this.channel.getLocalFirstIdentityProof?.(options) ?? null);
  }

  getConfig(): DbConfig {
    return this.config;
  }

  /**
   * @internal The channel backing this facade. The inspector host handle uses
   * it to reach the shared owner's real Db and to hand the overlay a live
   * channel into the host's store.
   */
  getSubscriptionChannel(): SubscriptionChannel {
    return this.channel;
  }

  insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: CreateOptions,
  ): Promise<AsyncWriteResult<T>> {
    return Promise.resolve(
      this.channel.insert(table, data, options, this.authState.session ?? undefined),
    );
  }

  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): Promise<AsyncWriteHandle> {
    return Promise.resolve(
      this.channel.update(table, id, data, options, this.authState.session ?? undefined),
    );
  }

  delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: DeleteOptions,
  ): Promise<AsyncWriteHandle> {
    return Promise.resolve(
      this.channel.delete(table, id, options, this.authState.session ?? undefined),
    );
  }

  canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<boolean> {
    return Promise.resolve(
      this.channel.canInsert(table, data, this.authState.session ?? undefined),
    );
  }

  canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<boolean> {
    return Promise.resolve(
      this.channel.canUpdate(table, id, data, this.authState.session ?? undefined),
    );
  }

  canDelete<T, Init>(table: TableProxy<T, Init>, id: string): Promise<boolean> {
    return Promise.resolve(this.channel.canDelete(table, id, this.authState.session ?? undefined));
  }

  createFileFromBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    blob: Blob,
    options?: FileWriteOptions,
  ): Promise<BinaryLargeValueFileRow<TApp>> {
    return Promise.resolve(this.channel.createFileFromBlob(app, blob, options));
  }

  loadFileAsBlob<TApp extends BinaryLargeValueFileApp<any, any>>(
    app: TApp,
    fileOrId: string | BinaryLargeValueFileRow<TApp>,
    options?: FileReadOptions,
  ): Promise<Blob> {
    return Promise.resolve(this.channel.loadFileAsBlob(app, fileOrId, options));
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
  const initialAuthState = await subscriptionChannel.getAuthState();
  let session = initialAuthState.session;
  const db = new AsyncChannelDbFacade(subscriptionChannel, initialAuthState, config);
  const manager = new SubscriptionsOrchestrator(
    { appId: config.appId },
    subscriptionChannel,
    session,
  );
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
        stopSessionSync();
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
        {
          db: client.db,
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
