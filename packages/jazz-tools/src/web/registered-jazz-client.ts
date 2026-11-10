import { clientScope } from "../runtime/client-scope.js";
import { createClientConfigKey } from "../runtime/client-config-key.js";
import { acquireClient, releaseClient } from "../runtime/client-registry.js";
import type { Db, ShutdownOptions } from "../runtime/db.js";
import type { AccountDbConfig } from "../accounts/context.js";
import { accountRegistry } from "../accounts/enrollment.js";
import { trackPromise } from "../subscriptions-orchestrator.js";
import { createObservedJazzClient, type JazzClient } from "../observed-jazz-client.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import { registerWindowJazzStorageClient } from "../window-client-storage.js";

export type { JazzClient };

/** Opens the `Db` behind a browser client: local-first or thin. */
export type CreateAccountDb = (config: AccountDbConfig) => Promise<Db>;

/** @internal Wrap an already admitted runtime for framework observers. */
export function createJazzClientFromDb(db: Db): Promise<JazzClient> {
  return createObservedJazzClient(db, clientScope(db.getConfig()), registerWindowJazzStorageClient);
}

/**
 * @internal Share one client per config and kind of `Db` between the
 * providers that ask for it. `kind` keeps a thin client and a local-first one
 * for the same account apart.
 */
export function createRegisteredJazzClient(
  kind: string,
  config: AccountDbConfig,
  createDb: CreateAccountDb,
): Promise<JazzClient> {
  accountRegistry(config.account);
  // The React provider also uses the generic client registry. Namespace this
  // runtime lease so its wrapper cannot collide with the underlying client.
  const key = createClientConfigKey(kind, config);
  const holder = {};
  const shared = acquireClient<JazzClient>(
    key,
    async () => createJazzClientFromDb(await createDb(config)),
    holder,
  );
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
