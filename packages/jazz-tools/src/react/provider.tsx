import { useEffect, type ReactNode } from "react";
import type { PublicSession } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import { jazzDevPluginActive, startInspectorOnce } from "../dev-tools/auto-attach.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
  type CreateJazzClient,
} from "../react-core/provider.js";
import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import {
  BrowserAuthSecretStore,
  browserAuthSecretStore,
  type AuthSecretStore,
} from "../runtime/auth-secret-store.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import { LocalFirstAuthStoreProvider } from "./use-local-first-auth.js";

// In dev builds, pull in a generated module that withJazz (next.ts/vite.ts/...)
// rewrites on every schema push. The bundler tracks this as a dependency of the
// React provider, so any push to the file forces a full reload of the host app
// without each framework plugin needing its own dev-server WebSocket wiring.
if (process.env.NODE_ENV === "development" && typeof window !== "undefined") {
  import("jazz-tools/_dev/schema-hash").catch(() => {});
}

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: CreatedJazzClient["db"];
  session: PublicSession | null;
  shutdown: CreatedJazzClient["shutdown"];
}

const createClient: CreateJazzClient = (config) =>
  createJazzClient(config) as Promise<CreatedJazzClient>;

function withLegacyMigrationLock<T>(
  migration: () => Promise<T>,
  indexedDbFallback: () => Promise<T>,
): Promise<T> {
  // The owner marker is not a compare-and-swap primitive. Only migrate while
  // Web Locks provides origin-wide exclusion; without it, creating a scoped
  // secret is safer than copying an identity into two app namespaces.
  const locks = typeof navigator === "undefined" ? undefined : navigator.locks;
  if (!locks || typeof locks.request !== "function") return indexedDbFallback();

  try {
    return Promise.resolve(
      locks.request("jazz-auth-secret-legacy-migration", { mode: "exclusive" }, migration),
    ).catch(indexedDbFallback);
  } catch {
    return indexedDbFallback();
  }
}

const LEGACY_MIGRATION_DB = "jazz-auth-secret-migration";
const LEGACY_MIGRATION_STORE = "owners";
const LEGACY_MIGRATION_RECORD = "legacy-auth-secret";

async function claimLegacyMigrationInIndexedDb(appId: string): Promise<string> {
  if (typeof indexedDB === "undefined") {
    throw new Error(
      "Jazz cannot safely migrate the legacy local-first secret: Web Locks and IndexedDB are unavailable.",
    );
  }

  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(LEGACY_MIGRATION_DB, 1);
    request.onupgradeneeded = () => request.result.createObjectStore(LEGACY_MIGRATION_STORE);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });

  try {
    return await new Promise<string>((resolve, reject) => {
      const transaction = db.transaction(LEGACY_MIGRATION_STORE, "readwrite");
      const store = transaction.objectStore(LEGACY_MIGRATION_STORE);
      let owner = appId;
      const read = store.get(LEGACY_MIGRATION_RECORD);
      read.onsuccess = () => {
        if (typeof read.result === "string") {
          owner = read.result;
        } else {
          store.put(appId, LEGACY_MIGRATION_RECORD);
        }
      };
      read.onerror = () => reject(read.error);
      transaction.oncomplete = () => resolve(owner);
      transaction.onerror = () => reject(transaction.error);
      transaction.onabort = () => reject(transaction.error);
    });
  } finally {
    db.close();
  }
}

class ProviderAuthSecretStore implements AuthSecretStore {
  constructor(
    private readonly appId: string,
    private readonly scopedStore: BrowserAuthSecretStore,
  ) {}

  loadSecret(): Promise<string | null> {
    return this.scopedStore.loadSecret();
  }

  saveSecret(secret: string): Promise<void> {
    return this.scopedStore.saveSecret(secret);
  }

  clearSecret(): Promise<void> {
    return this.scopedStore.clearSecret();
  }

  getOrCreateSecret(): Promise<string> {
    return withLegacyMigrationLock(
      () => this.migrateLegacySecret(() => claimLegacyMigrationInIndexedDb(this.appId)),
      () => this.migrateLegacySecret(() => claimLegacyMigrationInIndexedDb(this.appId)),
    );
  }

  private async migrateLegacySecret(claimOwner: () => Promise<string>): Promise<string> {
    const scopedSecret = await this.scopedStore.loadSecret();
    if (scopedSecret) return scopedSecret;

    const legacySecret = await browserAuthSecretStore.loadSecret();
    if (legacySecret) {
      const migrationOwner = await claimOwner();
      if (migrationOwner !== this.appId) {
        return this.scopedStore.getOrCreateSecret();
      }

      await this.scopedStore.saveSecret(legacySecret);
      try {
        await browserAuthSecretStore.clearSecret();
      } catch (error) {
        console.warn("Jazz could not remove the legacy local-first secret", error);
      }
      return legacySecret;
    }

    return this.scopedStore.getOrCreateSecret();
  }
}

const providerAuthSecretStores = new WeakMap<BrowserAuthSecretStore, ProviderAuthSecretStore>();

function getProviderAuthSecretStore(appId: string): ProviderAuthSecretStore {
  const scopedStore = BrowserAuthSecretStore.getDefault({ appId });
  let store = providerAuthSecretStores.get(scopedStore);
  if (!store) {
    store = new ProviderAuthSecretStore(appId, scopedStore);
    providerAuthSecretStores.set(scopedStore, store);
  }
  return store;
}

// Dev-only: mount the inspector overlay + publish the host handle for this db.
// Only rendered when shouldAutoAttach is true, so the lazy overlay chunk is
// dropped from production bundles.
function DevToolsAutoAttach() {
  const { db } = useCoreJazzClient() as JazzClientContextValue;
  useEffect(() => {
    startInspectorOnce(db);
  }, [db]);
  return null;
}

type JazzProviderCommonProps = {
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
  /** Dev-only: auto-open the inspector overlay. Default true. */
  autoAttachDevTools?: boolean;
};

type LocalFirstDbConfig = Omit<DbConfig, "secret" | "jwtToken" | "cookieSession">;

export type JazzProviderProps = JazzProviderCommonProps &
  (
    | {
        config: DbConfig;
        auth?: undefined;
      }
    | {
        config: LocalFirstDbConfig;
        auth: "local-first";
      }
  );

type ConfiguredJazzProviderProps = JazzProviderCommonProps & {
  config: DbConfig;
};

function ConfiguredJazzProvider({
  config,
  fallback,
  children,
  onJWTExpired,
  autoAttachDevTools,
}: ConfiguredJazzProviderProps) {
  const shouldAutoAttach = process.env.NODE_ENV !== "production" && autoAttachDevTools !== false;
  // Subscription traces only register while devMode is on at subscribe time,
  // so it must be on from Db construction for the overlay's Subscriptions tab
  // to see the app's startup queries — the host bridge's later setDevMode(true)
  // only covers subscriptions opened after the overlay attached. Default it on
  // exactly when the overlay will mount; an explicit config value always wins.
  const effectiveConfig =
    shouldAutoAttach && config.devMode === undefined && jazzDevPluginActive()
      ? { ...config, devMode: true }
      : config;

  return (
    <CoreJazzProvider
      config={effectiveConfig}
      fallback={fallback}
      createJazzClient={createClient}
      onJWTExpired={onJWTExpired}
    >
      {shouldAutoAttach ? <DevToolsAutoAttach /> : null}
      {children}
    </CoreJazzProvider>
  );
}

function LocalFirstJazzProvider({
  config,
  ...props
}: {
  config: LocalFirstDbConfig;
} & JazzProviderCommonProps) {
  // Scope the provider-owned identity by appId. This is also the store selected
  // by useLocalFirstAuth({ appId }), preserving identities created with that
  // existing hook API.
  const store = getProviderAuthSecretStore(config.appId);

  return <LocalFirstAuthLoader key={config.appId} config={config} store={store} {...props} />;
}

function LocalFirstAuthLoader({
  config,
  store,
  ...props
}: {
  config: LocalFirstDbConfig;
  store: AuthSecretStore;
} & JazzProviderCommonProps) {
  const { secret, isLoading } = useLocalFirstAuthWithStore(store);

  if (isLoading || !secret) return props.fallback ?? null;

  return (
    <LocalFirstAuthStoreProvider appId={config.appId} store={store}>
      <ConfiguredJazzProvider {...props} config={{ ...config, secret }} />
    </LocalFirstAuthStoreProvider>
  );
}

export function JazzProvider(props: JazzProviderProps) {
  if (props.auth === "local-first") {
    const { auth: _auth, ...localFirstProps } = props;
    return <LocalFirstJazzProvider {...localFirstProps} />;
  }

  const { auth: _auth, ...configuredProps } = props;
  return <ConfiguredJazzProvider {...configuredProps} />;
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb(): CreatedJazzClient["db"] {
  return useCoreDb<CreatedJazzClient["db"]>();
}

export { useSession };

export type { JazzClientContextValue };
