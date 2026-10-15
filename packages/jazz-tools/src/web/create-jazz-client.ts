import { createClientConfigKey } from "../runtime/client-config-key.js";
import { acquireClient, releaseClient } from "../runtime/client-registry.js";
import type { Db, ShutdownOptions } from "../runtime/db.js";
import type { AccountDbConfig } from "../accounts/context.js";
import { createDb } from "../runtime/default-create-db.js";
import { accountRegistry } from "../accounts/enrollment.js";
import { trackPromise } from "../subscriptions-orchestrator.js";
import { createObservedJazzClient, type JazzClient } from "../observed-jazz-client.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import { registerWindowJazzStorageClient } from "../window-client-storage.js";

export type JazzClientConfig = AccountDbConfig;

export type { JazzClient };

async function createJazzClientInternal(config: AccountDbConfig): Promise<JazzClient> {
  return createJazzClientFromDb(await createDb(config));
}

/** @internal Wrap an already admitted runtime for framework observers. */
export function createJazzClientFromDb(db: Db): Promise<JazzClient> {
  return createObservedJazzClient(db, db.getConfig().appId, registerWindowJazzStorageClient);
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
