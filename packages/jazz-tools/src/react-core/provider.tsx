import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type { DehydratedSnapshot } from "../backend/ssr.js";
import {
  computeSchemaFingerprint,
  resolveWasmSchema,
  type WasmSchemaInput,
} from "../drivers/schema-wire.js";
import type { AuthState } from "../runtime/auth-state.js";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { applySnapshot } from "./apply-snapshot.js";

type CoreJazzDb = {
  getAuthState(): AuthState;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
  updateAuthToken(token: string): void;
};

type CoreJazzClient = {
  db: CoreJazzDb;
  manager: SubscriptionsOrchestrator;
  session?: Session | null;
  shutdown: () => Promise<void>;
};

export type CreateJazzClient<TClient extends CoreJazzClient = CoreJazzClient> = (
  config: DbConfig,
) => Promise<TClient>;

export type JwtRefreshFn = () => Promise<string | null | undefined>;

export type JazzClientProviderProps = {
  client: Promise<CoreJazzClient> | CoreJazzClient;
  onJWTExpired?: JwtRefreshFn;
  snapshot?: DehydratedSnapshot;
  expectedAppId?: string;
  expectedSchemaFingerprint?: string;
  children: ReactNode;
};

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  createJazzClient: CreateJazzClient;
  onJWTExpired?: JwtRefreshFn;
  snapshot?: DehydratedSnapshot;
  schema?: WasmSchemaInput;
};

type JazzContextValue = {
  // `client` is null only during the synchronous seed phase, before the live
  // client has connected. `manager` is always present (the seeded read-only
  // orchestrator, swapped for the live one once connected). `clientPromise`
  // lets hooks that need the live client suspend until it resolves.
  client: CoreJazzClient | null;
  manager: SubscriptionsOrchestrator;
  clientPromise: Promise<CoreJazzClient>;
};

const JazzContext = createContext<JazzContextValue | null>(null);

// A db that never delivers, for the seed-only orchestrator: seeded entries are
// already fulfilled from the snapshot, and there's no live connection to stream
// updates until the real client swaps in.
const NOOP_SEED_DB = {
  subscribeAll(): () => void {
    return () => {};
  },
};

type CachedClientEntry = {
  configKey: string;
  createJazzClient: CreateJazzClient;
  initPromise: Promise<CoreJazzClient>;
  holders: Set<object>;
  releaseTimer: ReturnType<typeof setTimeout> | null;
};

let cachedClientEntry: CachedClientEntry | null = null;

function acquireClient<TClient extends CoreJazzClient>(
  configKey: string,
  config: DbConfig,
  createJazzClient: CreateJazzClient<TClient>,
  holder: object,
): Promise<TClient> {
  if (
    cachedClientEntry?.configKey !== configKey ||
    cachedClientEntry?.createJazzClient !== createJazzClient
  ) {
    cachedClientEntry = {
      configKey,
      createJazzClient,
      initPromise: createJazzClient(config),
      holders: new Set(),
      releaseTimer: null,
    };
  }

  cachedClientEntry.holders.add(holder);
  if (cachedClientEntry.releaseTimer) {
    clearTimeout(cachedClientEntry.releaseTimer);
    cachedClientEntry.releaseTimer = null;
  }

  return cachedClientEntry.initPromise as Promise<TClient>;
}

function releaseClient(configKey: string, holder: object): void {
  if (!cachedClientEntry || cachedClientEntry.configKey !== configKey) {
    return;
  }

  cachedClientEntry.holders.delete(holder);
  if (cachedClientEntry.holders.size > 0 || cachedClientEntry.releaseTimer) {
    return;
  }

  const entry = cachedClientEntry;
  // Delayed release survives Strict Mode's mount→unmount→remount cycle:
  // without it, the unmount would tear down the client before the remount reuses it.
  entry.releaseTimer = setTimeout(() => {
    if (entry.holders.size > 0) {
      entry.releaseTimer = null;
      return;
    }

    void entry.initPromise.then((resolved) => resolved.shutdown()).catch(() => {});
    if (cachedClientEntry === entry) {
      cachedClientEntry = null;
    }
  }, 0);
}

function useAuthSubscription(
  client: CoreJazzClient | null,
  onJWTExpired: JwtRefreshFn | undefined,
): number {
  // Latch serializes concurrent "expired" rejections into one refresh call.
  const inFlight = useRef(false);
  // Refcell keeps the callback fresh without re-subscribing when callers pass
  // an inline function that changes every render.
  const callbackRef = useRef(onJWTExpired);
  callbackRef.current = onJWTExpired;

  // Bumping this revision flips the context value's object identity so
  // consumers that read `client.session` or `client.db.getAuthState()`
  // directly (e.g. useSession, useDb) re-render on auth changes.
  const [authRev, setAuthRev] = useState(0);

  useEffect(() => {
    if (!client) return;
    return client.db.onAuthChanged((state) => {
      setAuthRev((n) => n + 1);

      if (state.error !== "expired") return;
      const fn = callbackRef.current;
      if (!fn) return;
      if (inFlight.current) return;
      inFlight.current = true;

      Promise.resolve()
        .then(() => fn())
        .then((newToken) => {
          if (newToken) {
            client.db.updateAuthToken(newToken);
          }
        })
        .catch(() => {})
        .finally(() => {
          inFlight.current = false;
        });
    });
  }, [client]);

  return authRev;
}

// Wrap React.use to make it compatible with React 18 and 19
function usePromise<T extends object>(promise: Promise<T> | T): T {
  if (!("then" in promise)) {
    return promise;
  }

  if (React.use !== undefined) {
    return React.use(promise);
  }

  const tracked = trackPromise(promise);

  if (tracked.status === "pending") {
    throw tracked;
  }

  if (tracked.status === "rejected") {
    throw tracked.reason;
  }

  return tracked.value as T;
}

/**
 * Makes a Jazz client available to children components through a React context.
 * Useful if you need to create a Jazz client outside of the React component lifecycle.
 */
export function JazzClientProvider({
  client: clientPromise,
  onJWTExpired,
  snapshot,
  expectedAppId,
  expectedSchemaFingerprint,
  children,
}: JazzClientProviderProps) {
  const client = usePromise(clientPromise);

  // useState initialiser runs once per mount, before the first child render.
  // This ensures the orchestrator is seeded before any useDb hook below
  // calls makeQueryKey / getCacheEntry on it.
  useState(() => {
    if (!snapshot) return null;
    applySnapshot({
      manager: client.manager,
      snapshot,
      expected: {
        appId: expectedAppId ?? snapshot.appId,
        principalId: client.session?.user_id ?? null,
        schemaFingerprint: expectedSchemaFingerprint ?? snapshot.schemaFingerprint,
      },
    });
    return null;
  });

  const authRev = useAuthSubscription(client, onJWTExpired);

  const value = React.useMemo<JazzContextValue>(
    () => ({
      client,
      manager: client.manager,
      clientPromise: Promise.resolve(clientPromise),
    }),
    [client, clientPromise, authRev],
  );

  return <JazzContext.Provider value={value}>{children}</JazzContext.Provider>;
}

/**
 * Snapshot-seeded provider. Renders the prefetched rows synchronously — on the
 * server and on the client's first render — from a read-only orchestrator
 * seeded by the snapshot, without waiting on the async client. So the SSR HTML
 * already contains the data and matches the client's first paint (no hydration
 * re-render). Once the live client connects, the context swaps to it; the live
 * orchestrator is seeded with the same rows first, so the swap is
 * data-identical and live updates stream in from there.
 */
function SeededJazzClientProvider({
  client: clientPromise,
  onJWTExpired,
  snapshot,
  expectedAppId,
  expectedSchemaFingerprint,
  fallback,
  children,
}: JazzClientProviderProps & { snapshot: DehydratedSnapshot; fallback?: ReactNode }) {
  const [seedManager] = useState(() => {
    const manager = new SubscriptionsOrchestrator(
      { appId: expectedAppId ?? snapshot.appId },
      NOOP_SEED_DB as ConstructorParameters<typeof SubscriptionsOrchestrator>[1],
    );
    applySnapshot({
      manager,
      snapshot,
      expected: {
        appId: expectedAppId ?? snapshot.appId,
        // No live session yet, so only public (null-principal) snapshots seed
        // synchronously. User-scoped snapshots wait for the live client, where
        // the principal can be checked.
        principalId: null,
        schemaFingerprint: expectedSchemaFingerprint ?? snapshot.schemaFingerprint,
      },
    });
    return manager;
  });

  useEffect(() => {
    return () => {
      void seedManager.shutdown();
    };
  }, [seedManager]);

  const normalizedPromise = React.useMemo(
    () => Promise.resolve(clientPromise) as Promise<CoreJazzClient>,
    [clientPromise],
  );

  const [liveClient, setLiveClient] = useState<CoreJazzClient | null>(() => {
    const tracked = trackPromise(normalizedPromise);
    return tracked.status === "fulfilled" ? (tracked.value as CoreJazzClient) : null;
  });

  useEffect(() => {
    let cancelled = false;
    normalizedPromise
      .then((resolved) => {
        if (!cancelled) setLiveClient(resolved);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [normalizedPromise]);

  const seededLiveRef = useRef<CoreJazzClient | null>(null);
  if (liveClient && seededLiveRef.current !== liveClient) {
    seededLiveRef.current = liveClient;
    applySnapshot({
      manager: liveClient.manager,
      snapshot,
      expected: {
        appId: expectedAppId ?? snapshot.appId,
        principalId: liveClient.session?.user_id ?? null,
        schemaFingerprint: expectedSchemaFingerprint ?? snapshot.schemaFingerprint,
      },
    });
  }

  const authRev = useAuthSubscription(liveClient, onJWTExpired);

  const value = React.useMemo<JazzContextValue>(
    () => ({
      client: liveClient,
      manager: liveClient?.manager ?? seedManager,
      clientPromise: normalizedPromise,
    }),
    [liveClient, seedManager, normalizedPromise, authRev],
  );

  return (
    <JazzContext.Provider value={value}>
      <React.Suspense fallback={fallback}>{children}</React.Suspense>
    </JazzContext.Provider>
  );
}

/**
 * Default Jazz provider. Creates a Jazz client and makes it available to children
 * components through a React context.
 * If you need to create a Jazz client outside of the React component lifecycle,
 * use {@link JazzClientProvider}.
 */
export function JazzProvider({
  config,
  fallback,
  children,
  createJazzClient,
  onJWTExpired,
  snapshot,
  schema,
}: JazzProviderProps) {
  const expectedSchemaFingerprint = React.useMemo(
    () => (schema ? computeSchemaFingerprint(resolveWasmSchema(schema)) : undefined),
    [schema],
  );
  // Stable per-provider identity; used as the Set key so the useState
  // initializer and useEffect don't double-count the same provider.
  const holder = useRef({}).current;

  const configKey = JSON.stringify(config);

  const [clientPromise, setClientPromise] = useState(() => {
    return acquireClient<CoreJazzClient>(configKey, config, createJazzClient, holder);
  });

  useEffect(() => {
    const clientPromise = acquireClient<CoreJazzClient>(
      configKey,
      config,
      createJazzClient,
      holder,
    );

    setClientPromise(clientPromise);

    return () => {
      releaseClient(configKey, holder);
    };
  }, [configKey, createJazzClient, holder]);

  if (snapshot) {
    return (
      <SeededJazzClientProvider
        client={clientPromise}
        onJWTExpired={onJWTExpired}
        snapshot={snapshot}
        expectedAppId={config.appId}
        expectedSchemaFingerprint={expectedSchemaFingerprint}
        fallback={fallback}
      >
        {children}
      </SeededJazzClientProvider>
    );
  }

  return (
    <React.Suspense fallback={fallback}>
      <JazzClientProvider
        client={clientPromise}
        onJWTExpired={onJWTExpired}
        expectedAppId={config.appId}
        expectedSchemaFingerprint={expectedSchemaFingerprint}
      >
        {children}
      </JazzClientProvider>
    </React.Suspense>
  );
}

function useJazzContext(): JazzContextValue {
  const ctx = useContext(JazzContext);
  if (!ctx) throw new Error("useDb must be used within <JazzProvider>");
  return ctx;
}

export function useJazzClient(): CoreJazzClient {
  const ctx = useJazzContext();
  // During the seed phase the live client isn't ready; suspend on it for hooks
  // that need the real db/session (useDb, useSession, useAuthState).
  return ctx.client ?? usePromise(ctx.clientPromise);
}

/**
 * Get the active subscriptions orchestrator: the seeded read-only one during
 * the seed phase, the live one once connected. Never suspends, so reads can
 * render synchronously from the snapshot.
 */
export function useManager(): SubscriptionsOrchestrator {
  return useJazzContext().manager;
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb<TDb = unknown>(): TDb {
  return useJazzClient().db as TDb;
}

/**
 * Get the current Jazz {@link Session}, including the user's id, claims and auth mode.
 */
export function useSession(): Session | null {
  return useJazzClient().session ?? null;
}
