import type { PublicSession } from "../runtime/context.js";
import { createClientConfigKey } from "../runtime/client-config-key.js";
import { acquireClient, releaseClient } from "../runtime/client-registry.js";
import type { Db, ShutdownOptions } from "../runtime/db.js";
import { getDbSubscriptionSource } from "../runtime/db.js";
import type { AccountDbConfig } from "../accounts/context.js";
import { createDb } from "../runtime/default-create-db.js";
import { accountRegistry } from "../accounts/enrollment.js";
import { runCleanupSteps } from "../runtime/run-cleanup-steps.js";
import { SubscriptionsOrchestrator, trackPromise } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import { registerWindowJazzStorageClient } from "../window-client-storage.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";

export type JazzClientConfig = AccountDbConfig;

export interface JazzClient {
  db: Db;
  session: PublicSession | null;
  shutdown(options?: ShutdownOptions): Promise<void>;
}

async function createJazzClientInternal(config: AccountDbConfig): Promise<JazzClient> {
  const db = await createDb(config);
  let session = db.getAuthState().session;
  const manager = new SubscriptionsOrchestrator(
    { appId: config.appId },
    getDbSubscriptionSource(db),
    getDbInternalSession(db),
  );
  await manager.init();
  const stopSessionSync = db.onAuthChanged(({ session: nextSession }) => {
    session = nextSession ?? null;
    manager.setSession(getDbInternalSession(db));
  });
  const unregisterWindowJazzStorageClient = registerWindowJazzStorageClient(db);

  return attachSubscriptionStore(
    {
      db,
      get session() {
        return session;
      },
      async shutdown(options?: ShutdownOptions) {
        if (options?.waitForSync) await db.shutdown(options);
        await runCleanupSteps([
          () => stopSessionSync?.(),
          () => unregisterWindowJazzStorageClient(),
          () => manager.shutdown(),
          () => db.shutdown(),
        ]);
      },
    },
    manager,
  );
}

function configKey(config: AccountDbConfig): string {
  // The React provider also uses the generic client registry. Namespace this
  // runtime lease so its wrapper cannot collide with the underlying client.
  return createClientConfigKey("web", config);
}

export function createJazzClient(config: AccountDbConfig): Promise<JazzClient> {
  accountRegistry(config.account);
  const key = configKey(config);
  const holder = {};
  const shared = acquireClient<JazzClient>(key, () => createJazzClientInternal(config), holder);
  return trackPromise(
    shared.then((client) =>
      attachSubscriptionStore(
        {
          db: client.db,
          get session() {
            return client.session;
          },
          shutdown(options?: ShutdownOptions) {
            return releaseClient(key, holder, options);
          },
        },
        getSubscriptionStore(client),
      ),
    ),
  );
}
