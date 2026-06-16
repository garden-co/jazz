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
import {
  acquireClient as registryAcquireClient,
  releaseClient as registryReleaseClient,
} from "../runtime/client-registry.js";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import {
  SubscriptionsOrchestrator,
  trackPromise,
  type DbLike,
} from "../subscriptions-orchestrator.js";
import { applySnapshot } from "../ssr/apply-snapshot.js";
import { createDbLessOrchestrator } from "../ssr/seed-orchestrator.js";

// The provider needs the auth surface plus the {@link DbLike} surface
// (subscribeAll / applyQueryBundle) so it can attach the live db to its own
// orchestrator once the client connects.
type CoreJazzDb = DbLike & {
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
  /**
   * Opt into the synchronous seed phase for SSR/hydration. When set, the
   * provider renders without suspending on the async live client, so a child
   * `useAll(query, { snapshot })` can seed its rows for the first paint. The
   * snapshot data lives at the hook call site, not here.
   */
  ssr?: boolean;
  schema?: WasmSchemaInput;
};

type JazzContextValue = {
  // `client` is null only during the synchronous seed phase, before the live
  // client has connected. `manager` is a single stable orchestrator (db-less at
  // first, then the live db attaches to it — identity never changes).
  // `clientPromise` lets hooks that need the live client suspend until it resolves.
  client: CoreJazzClient | null;
  manager: SubscriptionsOrchestrator;
  clientPromise: Promise<CoreJazzClient>;
  // The client's own schema fingerprint, computed from the provider's `schema`
  // prop (undefined when none was given). A per-hook seed compares it against
  // the snapshot's fingerprint and discards on mismatch — a client on a
  // different build than the server drops the snapshot rather than seeding rows
  // its schema can't represent.
  schemaFingerprint?: string;
};

const JazzContext = createContext<JazzContextValue | null>(null);

// Client lifecycle is delegated to the framework-agnostic, refcounted client
// registry (keyed by configKey, with deferred release to survive Strict Mode's
// mount→unmount→remount cycle). Using the shared Map-backed registry — rather
// than a single cached slot — lets distinct configs (e.g. two principals on one
// screen) coexist instead of evicting one another.
function acquireClient<TClient extends CoreJazzClient>(
  configKey: string,
  config: DbConfig,
  createJazzClient: CreateJazzClient<TClient>,
  holder: object,
): Promise<TClient> {
  return registryAcquireClient<TClient>(configKey, () => createJazzClient(config), holder);
}

function releaseClient(configKey: string, holder: object): void {
  void registryReleaseClient(configKey, holder);
}

// Refresh latch keyed on the client, not the component. The client is a
// module-singleton, so a remount or a second provider would otherwise each
// hold their own latch and double-fire the JWT refresh on an "expired" event.
const authRefreshLatches = new WeakMap<object, { inFlight: boolean }>();

// Ceiling on how long the latch stays held for a single refresh. A caller whose
// `onJWTExpired` never settles must not wedge the latch forever — after this we
// release it so a later "expired" event can retry.
const JWT_REFRESH_TIMEOUT_MS = 30_000;

function getAuthRefreshLatch(client: object): { inFlight: boolean } {
  let latch = authRefreshLatches.get(client);
  if (!latch) {
    latch = { inFlight: false };
    authRefreshLatches.set(client, latch);
  }
  return latch;
}

function useAuthSubscription(
  client: CoreJazzClient | null,
  onJWTExpired: JwtRefreshFn | undefined,
): number {
  // Client-scoped latch serializes concurrent "expired" rejections into one
  // refresh call across every provider/remount sharing this client.
  const latch = getAuthRefreshLatch(client);
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
      if (latch.inFlight) return;
      latch.inFlight = true;

      // Release exactly once — whichever of settle or timeout comes first. The
      // `settled` guard also stops a refresh that resolves *after* timing out
      // from applying a now-stale token.
      let settled = false;
      const release = () => {
        if (settled) return;
        settled = true;
        latch.inFlight = false;
      };
      const timeoutId = setTimeout(release, JWT_REFRESH_TIMEOUT_MS);

      Promise.resolve()
        .then(() => fn())
        .then((newToken) => {
          if (!settled && newToken) {
            client.db.updateAuthToken(newToken);
          }
        })
        .catch(() => {})
        .finally(() => {
          clearTimeout(timeoutId);
          release();
        });
    });
  }, [client, latch]);

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
        appId: expectedAppId,
        principalId: client.session?.user_id ?? null,
        schemaFingerprint: expectedSchemaFingerprint,
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
      schemaFingerprint: expectedSchemaFingerprint,
    }),
    [client, clientPromise, expectedSchemaFingerprint, authRev],
  );

  return <JazzContext.Provider value={value}>{children}</JazzContext.Provider>;
}

/**
 * Seed-phase provider for SSR. Renders synchronously — on the server and the
 * client's first render — from a single orchestrator that starts db-less,
 * without suspending on the async live client. The rows come from the per-hook
 * `useAll(query, { snapshot })`, which seeds this orchestrator before its first
 * lookup, so the SSR HTML already contains the data and matches the client's
 * first paint (no hydration re-render). Once the live client connects, its db is
 * *attached* to this same orchestrator — the queued sync bundle hydrates the
 * store and every entry re-subscribes to the live db — so the orchestrator
 * identity is stable across the transition (no swap) and live updates stream in
 * with no flash.
 */
function SeededJazzClientProvider({
  client: clientPromise,
  onJWTExpired,
  appId,
  fallback,
  children,
  schemaFingerprint,
}: {
  client: Promise<CoreJazzClient> | CoreJazzClient;
  onJWTExpired?: JwtRefreshFn;
  appId: string;
  schemaFingerprint?: string;
  fallback?: ReactNode;
  children: ReactNode;
}) {
  // One orchestrator for the whole transition: db-less at first, so the per-hook
  // useAll(query, { snapshot }) seeds it for the synchronous first paint; the
  // live db attaches to this same instance once connected. The provider never
  // seeds — co-locating the snapshot with its query is the hook's job now; here
  // we only need the appId so its keys match the live client's.
  const [manager] = useState(() => createDbLessOrchestrator(appId));

  useEffect(() => {
    return () => {
      manager.shutdown().catch(() => {});
    };
  }, [manager]);

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

  // Attach the live db to the seed orchestrator exactly once: set the session
  // first so re-subscription runs under the right principal, then attachDb
  // drains the queued bundle into the store and re-points every entry at the
  // live db. Auth changes thereafter keep the orchestrator's session current.
  const attachedRef = useRef(false);
  useEffect(() => {
    if (!liveClient || attachedRef.current) return;
    attachedRef.current = true;
    manager.setSession(liveClient.session ?? null);
    manager.attachDb(liveClient.db);
    return liveClient.db.onAuthChanged((state) => {
      manager.setSession(state.session ?? null);
    });
  }, [liveClient, manager]);

  const authRev = useAuthSubscription(liveClient, onJWTExpired);

  const value = React.useMemo<JazzContextValue>(
    () => ({
      client: liveClient,
      manager,
      clientPromise: normalizedPromise,
      schemaFingerprint,
    }),
    [liveClient, manager, normalizedPromise, schemaFingerprint, authRev],
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
  ssr,
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

  if (ssr) {
    return (
      <SeededJazzClientProvider
        client={clientPromise}
        onJWTExpired={onJWTExpired}
        appId={config.appId}
        schemaFingerprint={expectedSchemaFingerprint}
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
 * The client's own schema fingerprint, if the provider was given a `schema`.
 * A per-hook seed compares it against a snapshot's fingerprint to discard
 * snapshots produced by a divergent build. Undefined when no `schema` was set,
 * in which case the seed runs unchecked (opt-in, as before).
 */
export function useSchemaFingerprint(): string | undefined {
  return useJazzContext().schemaFingerprint;
}

/**
 * Whether we're in the synchronous SSR seed phase — the live client has not
 * connected yet. True only under `<JazzProvider ssr>` before connect (and
 * throughout the server render, where no client ever connects); always false on
 * the CSR path, where the provider suspends until the client resolves. A
 * suspense hook uses this to avoid suspending on a query nothing can resolve yet.
 */
export function useIsSeedPhase(): boolean {
  return useJazzContext().client === null;
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
