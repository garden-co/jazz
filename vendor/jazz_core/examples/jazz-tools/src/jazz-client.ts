import { authSecretStore, type AuthSecretStore } from "./auth-secret-store.js";
import {
  createDb,
  type AuthState,
  type Db,
  type DbOptions,
  type QueryBuilder,
  type Session,
  type Table,
} from "./jazz-tools.js";

export type JazzClientOptions = DbOptions & {
  authSecretStore?: AuthSecretStore;
};

export type JazzClient = {
  readonly db: Db;
  readonly auth: AuthState;
  getAuthState(): AuthState;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
  updateAuthToken(jwtToken: string | null): void;
};

export type JazzProviderProps<Children = unknown> = {
  client?: JazzClient;
  config?: {
    appId: string;
    serverUrl?: string;
    secret?: string;
  };
  children?: Children | ((client: JazzClient) => Children);
};

export type JazzClientSource = JazzClient | (() => JazzClient | null | undefined);

export type LiveRows<Row> = {
  readonly current: Row[];
  refresh(): Row[];
  unsubscribe(): void;
};

export type JazzHookHelpers = {
  useJazzClient(): JazzClient;
  useDb(): Db;
  useTable<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(
    name: string,
  ): Table<Row, Init>;
  useAll<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
  ): LiveRows<Row>;
};

export async function createJazzClient(options: JazzClientOptions): Promise<JazzClient> {
  const secret = resolveLocalFirstSecret(options);
  const db = await createDb(secret == null ? options : { ...options, secret });

  return {
    db,
    get auth() {
      return db.getAuthState();
    },
    getAuthState() {
      return db.getAuthState();
    },
    onAuthChanged(listener) {
      return db.onAuthChanged(listener);
    },
    updateAuthToken(jwtToken) {
      db.updateAuthToken(jwtToken);
    },
  };
}

export function JazzProvider<Children = unknown>(
  props: JazzProviderProps<Children>,
): Children | JazzClient {
  if (!props.client) {
    if (typeof props.children === "function") {
      throw new Error("JazzProvider client is not available for function children.");
    }
    return props.children as Children;
  }
  if (typeof props.children === "function") {
    return (props.children as (client: JazzClient) => Children)(props.client);
  }
  return props.children ?? props.client;
}

export function createJazzHooks(source: JazzClientSource): JazzHookHelpers {
  const useJazzClient = createUseJazzClient(source);
  const useDb = createUseDb(source);
  return {
    useJazzClient,
    useDb,
    useTable: createUseTable(source),
    useAll: createUseAll(source),
  };
}

export function createUseJazzClient(source: JazzClientSource): () => JazzClient {
  return () => resolveJazzClient(source);
}

export function createUseDb(source: JazzClientSource): () => Db {
  return () => resolveJazzClient(source).db;
}

export function createUseTable(source: JazzClientSource): JazzHookHelpers["useTable"] {
  return <Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(
    name: string,
  ): Table<Row, Init> => resolveJazzClient(source).db.table<Row, Init>(name);
}

export function createUseAll(source: JazzClientSource): JazzHookHelpers["useAll"] {
  return <Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | QueryBuilder<Row>,
  ): LiveRows<Row> => {
    const db = resolveJazzClient(source).db;
    let current: Row[] = [];
    const subscription = db.subscribe(tableOrQuery, (rows) => {
      current = rows;
    });
    return {
      get current() {
        return current;
      },
      refresh() {
        current = db.all(tableOrQuery);
        return current;
      },
      unsubscribe() {
        subscription.unsubscribe();
      },
    };
  };
}

function resolveJazzClient(source: JazzClientSource): JazzClient {
  const client = typeof source === "function" ? source() : source;
  if (!client)
    throw new Error("Jazz client is not available. Pass a client or provider-backed getter.");
  return client;
}

function resolveLocalFirstSecret(options: JazzClientOptions): string | undefined {
  if (options.secret || options.jwtToken || hasCookieSession(options.cookieSession))
    return options.secret;
  return (options.authSecretStore ?? authSecretStore).getOrCreateSecret(options.appId);
}

function hasCookieSession(session: Session | undefined): boolean {
  return session != null;
}
