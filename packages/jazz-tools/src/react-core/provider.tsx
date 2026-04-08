import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import type { AuthState } from "../runtime/auth-state.js";
import type { Session } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";

type CoreJazzDb = {
  getAuthState(): AuthState;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
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

export type JazzClientProviderProps = {
  client: CoreJazzClient;
  children: ReactNode;
};

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  createJazzClient: CreateJazzClient;
};

type JazzContextValue = {
  client: CoreJazzClient;
  authState: AuthState;
};

const JazzContext = createContext<JazzContextValue | null>(null);

type CachedClientEntry = {
  configKey: string;
  createJazzClient: CreateJazzClient;
  initPromise: Promise<CoreJazzClient>;
  refs: number;
  releaseTimer: ReturnType<typeof setTimeout> | null;
};

let cachedClientEntry: CachedClientEntry | null = null;

function acquireClient<TClient extends CoreJazzClient>(
  configKey: string,
  config: DbConfig,
  createJazzClient: CreateJazzClient<TClient>,
): Promise<TClient> {
  if (
    cachedClientEntry?.configKey !== configKey ||
    cachedClientEntry?.createJazzClient !== createJazzClient
  ) {
    cachedClientEntry = {
      configKey,
      createJazzClient,
      initPromise: createJazzClient(config),
      refs: 0,
      releaseTimer: null,
    };
  }

  cachedClientEntry.refs += 1;
  if (cachedClientEntry.releaseTimer) {
    clearTimeout(cachedClientEntry.releaseTimer);
    cachedClientEntry.releaseTimer = null;
  }

  return cachedClientEntry.initPromise as Promise<TClient>;
}

function releaseClient(configKey: string): void {
  if (!cachedClientEntry || cachedClientEntry.configKey !== configKey) {
    return;
  }

  cachedClientEntry.refs = Math.max(0, cachedClientEntry.refs - 1);
  if (cachedClientEntry.refs > 0 || cachedClientEntry.releaseTimer) {
    return;
  }

  const entry = cachedClientEntry;
  // In dev Strict Mode, React does:
  // 1. mount
  // 2. immediately unmount
  // 3. immediately remount
  // Without the delayed release, the fake unmount would tear the client down before the remount could reuse it,
  // causing a double initialization. This way, the remount can reacquire the same cached client before shutdown happens.
  entry.releaseTimer = setTimeout(() => {
    if (entry.refs > 0) {
      entry.releaseTimer = null;
      return;
    }

    void entry.initPromise.then((resolved) => resolved.shutdown()).catch(() => {});
    if (cachedClientEntry === entry) {
      cachedClientEntry = null;
    }
  }, 0);
}

/**
 * Makes a Jazz client available to children components through a React context.
 * Useful if you need to create a Jazz client outside of the React component lifecycle.
 */
export function JazzClientProvider({ client, children }: JazzClientProviderProps) {
  const [authState, setAuthState] = useState(() => client.db.getAuthState());

  useEffect(() => {
    setAuthState(client.db.getAuthState());
    return client.db.onAuthChanged((nextAuthState) => {
      setAuthState(nextAuthState);
    });
  }, [client]);

  return <JazzContext.Provider value={{ client, authState }}>{children}</JazzContext.Provider>;
}

/**
 * Default Jazz provider. Creates a Jazz client and makes it available to children
 * components through a React context.
 * If you need to create a Jazz client outside of the React component lifecycle,
 * use {@link JazzClientProvider}.
 */
export function JazzProvider({ config, fallback, children, createJazzClient }: JazzProviderProps) {
  const configKey = JSON.stringify(config);
  const [client, setClient] = useState<CoreJazzClient | null>(null);
  const [error, setError] = useState<unknown>(null);

  useEffect(() => {
    let active = true;
    const pendingClient = acquireClient<CoreJazzClient>(configKey, config, createJazzClient);

    void pendingClient.then(
      (resolved) => {
        if (!active) {
          return;
        }
        setClient(resolved);
      },
      (reason) => {
        if (!active) {
          return;
        }
        setError(reason);
      },
    );

    return () => {
      active = false;
      releaseClient(configKey);
    };
  }, [config, configKey, createJazzClient]);

  if (error) {
    throw error;
  }

  if (!client) {
    return <>{fallback ?? null}</>;
  }

  return <JazzClientProvider client={client}>{children}</JazzClientProvider>;
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
