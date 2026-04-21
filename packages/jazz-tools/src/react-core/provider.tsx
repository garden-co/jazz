import React, {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type { AuthState } from "../runtime/auth-state.js";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

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
  children: ReactNode;
};

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  createJazzClient: CreateJazzClient;
  onJWTExpired?: JwtRefreshFn;
};

type JazzContextValue = {
  client: CoreJazzClient;
};

const JazzContext = createContext<JazzContextValue | null>(null);

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
  client: CoreJazzClient,
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

/**
 * Makes a Jazz client available to children components through a React context.
 * Useful if you need to create a Jazz client outside of the React component lifecycle.
 */
export function JazzClientProvider({
  client: clientPromise,
  onJWTExpired,
  children,
}: JazzClientProviderProps) {
  const client = "then" in clientPromise ? React.use(clientPromise) : clientPromise;

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
  // Stable per-provider identity; used as the Set key so the useState
  // initializer and useEffect don't double-count the same provider.
  const holder = useRef({}).current;

  const [clientPromise, setClientPromise] = useState(() => {
    const configKey = JSON.stringify(config);
    return acquireClient<CoreJazzClient>(configKey, config, createJazzClient, holder);
  });

  useEffect(() => {
    const configKey = JSON.stringify(config);
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
  }, [config, createJazzClient, holder]);

  return (
    <React.Suspense fallback={fallback}>
      <JazzClientProvider client={clientPromise} onJWTExpired={onJWTExpired}>
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

export function useDb<TDb = unknown>(): TDb {
  return useJazzClient().db as TDb;
}

export function useSession(): Session | null {
  return useJazzClient().session ?? null;
}
