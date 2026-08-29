import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type { AuthState } from "../runtime/auth-state.js";
import { createClientConfigKey } from "../runtime/client-config-key.js";
import {
  acquireClient as registryAcquireClient,
  releaseClient as registryReleaseClient,
} from "../runtime/client-registry.js";
import type { PublicSession } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { trackPromise } from "../subscriptions-orchestrator.js";

type CoreJazzDb = {
  getAuthState(): AuthState;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
  updateAuthToken(token: string): void;
};

type CoreJazzClient = {
  db: CoreJazzDb;
  session?: PublicSession | null;
  shutdown: () => Promise<void>;
};

export type CreateJazzClient<TClient extends CoreJazzClient = CoreJazzClient> = (
  config: DbConfig,
) => Promise<TClient>;

export type JwtRefreshFn = () => Promise<string | null | undefined>;

export type JazzClientProviderProps = {
  client: Promise<CoreJazzClient> | CoreJazzClient;
  onJWTExpired?: JwtRefreshFn;
  children: ReactNode;
};

export type JazzProviderProps = {
  config: DbConfig;
  /** Rendered during SSR and until client acquisition completes after browser commit. */
  fallback?: ReactNode;
  children: ReactNode;
  createJazzClient: CreateJazzClient;
  onJWTExpired?: JwtRefreshFn;
};

type JazzContextValue = {
  client: CoreJazzClient;
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

function releaseClient(configKey: string, holder: object): Promise<void> {
  return registryReleaseClient(configKey, holder);
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
  client: CoreJazzClient,
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
  children,
}: JazzClientProviderProps) {
  const client = usePromise(clientPromise);

  const authRev = useAuthSubscription(client, onJWTExpired);

  const value = React.useMemo(() => ({ client }), [client, authRev]);

  return <JazzContext.Provider value={value}>{children}</JazzContext.Provider>;
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
}: JazzProviderProps) {
  // Stable per-provider identity, used as the registry holder across effect
  // cleanup and re-acquisition (including React Strict Mode's effect replay).
  const holder = useRef({}).current;

  // Keep the framework-level lease distinct from createJazzClient's own shared
  // client lease. Both use the generic registry; sharing an unqualified key
  // makes provider teardown recursively release itself instead of the runtime.
  const configKey = createClientConfigKey("react", config, [createJazzClient]);

  const [clientLease, setClientLease] = useState<{
    configKey: string;
    promise: Promise<CoreJazzClient>;
  } | null>(null);
  const lifecycle = useRef<{
    configKey: string | null;
    release: Promise<void>;
  }>({ configKey: null, release: Promise.resolve() });

  useEffect(() => {
    let cancelled = false;
    let acquired = false;
    const previous = lifecycle.current;
    // A same-key re-acquire must run immediately so the registry can cancel its
    // deferred release during Strict Mode replay. A different key waits for the
    // complete prior lifecycle, including any replacement that was cancelled
    // before it acquired a lease.
    const ready = previous.configKey === configKey ? Promise.resolve() : previous.release;
    const acquisition = ready.then(() => {
      if (cancelled) return;
      const promise = acquireClient<CoreJazzClient>(configKey, config, createJazzClient, holder);
      acquired = true;
      setClientLease({ configKey, promise });
    });

    return () => {
      cancelled = true;
      lifecycle.current = {
        configKey,
        release: acquisition.then(() => {
          if (acquired) {
            return releaseClient(configKey, holder);
          }
        }),
      };
    };
  }, [configKey, createJazzClient, holder]);

  // Effects do not run during SSR. Rendering the same fallback before the first
  // browser commit keeps hydration stable while deferring all registry work to
  // the committed lifecycle.
  if (clientLease?.configKey !== configKey) {
    return <>{fallback}</>;
  }

  return (
    <React.Suspense fallback={fallback}>
      <JazzClientProvider client={clientLease.promise} onJWTExpired={onJWTExpired}>
        {children}
      </JazzClientProvider>
    </React.Suspense>
  );
}

export function useJazzClient(): CoreJazzClient {
  const ctx = useContext(JazzContext);
  if (!ctx) throw new Error("useDb must be used within <JazzProvider>");
  return ctx.client;
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb<TDb = unknown>(): TDb {
  return useJazzClient().db as TDb;
}

/**
 * Get the current Jazz {@link PublicSession}, including the canonical user,
 * immutable provider claims, and auth mode.
 */
export function useSession(): PublicSession | null {
  const session = useJazzClient().session;
  return session ?? null;
}
