import {
  authSecretStore,
  createUseLocalFirstAuth,
  type LocalFirstAuthState,
  type UseLocalFirstAuthOptions,
} from "./auth-secret-store.js";
import {
  createContext,
  createElement,
  useContext,
  useMemo,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import type {
  Db,
  QueryBuilder,
  Table,
} from "./jazz-tools.js";
import {
  type JazzClient,
  type JazzClientOptions,
  type JazzProviderProps as CoreJazzProviderProps,
} from "./jazz-client.js";
export {
  createJazzHooks,
  createJazzClient,
  createUseAll,
  createUseDb,
  createUseJazzClient,
  createUseTable,
  type JazzClient,
  type JazzClientOptions,
  type JazzClientSource,
  type JazzHookHelpers,
  type LiveRows,
} from "./jazz-client.js";

export {
  authSecretStore,
  createAuthSecretStore,
  createUseLocalFirstAuth,
  generateAuthSecret,
  type AuthSecretStore,
  type AuthSecretStoreOptions,
  type AuthSecretStorage,
  type LocalFirstAuthState,
  type UseLocalFirstAuthOptions,
} from "./auth-secret-store.js";

export const useLocalFirstAuth: (options?: UseLocalFirstAuthOptions) => LocalFirstAuthState =
  createUseLocalFirstAuth(authSecretStore);

export type JazzProviderProps = Omit<CoreJazzProviderProps<ReactNode>, "children"> & {
  children?: ReactNode;
};

const JazzClientContext = createContext<JazzClient | null>(null);

export function JazzProvider({ client, children }: JazzProviderProps): ReactNode {
  return createElement(JazzClientContext.Provider, { value: client ?? null }, children);
}

export function useJazzClient(): JazzClient {
  const client = useContext(JazzClientContext);
  if (!client) throw new Error("Jazz client is not available. Render under JazzProvider with a client.");
  return client;
}

export function useDb(): Db {
  return useJazzClient().db;
}

export function useTable<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(name: string): Table<Row, Init> {
  const db = useDb();
  return useMemo(() => db.table<Row, Init>(name), [db, name]);
}

export function useAll<Row>(
  tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
): Row[] {
  const db = useDb();
  const store = useMemo(() => {
    let current: Row[] = [];
    const listeners = new Set<() => void>();
    const subscription = db.subscribe(tableOrQuery, (rows) => {
      current = rows;
      for (const listener of [...listeners]) listener();
    });
    return {
      getSnapshot: () => current,
      subscribe: (onStoreChange: () => void) => {
        listeners.add(onStoreChange);
        return () => {
          listeners.delete(onStoreChange);
          subscription.unsubscribe();
        };
      },
    };
  }, [db, tableOrQuery]);

  return useSyncExternalStore(
    store.subscribe,
    store.getSnapshot,
    store.getSnapshot,
  );
}
