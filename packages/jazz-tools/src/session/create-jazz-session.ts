import type { BackendAuth } from "../accounts/enrollment.js";
import {
  createAccountManager,
  type AccountManagerConfig,
} from "../accounts/create-account-manager.js";
import {
  createJazzClient,
  type JazzClientConfig,
  type JazzClient,
} from "../web/create-jazz-client.js";
import { createJazzSessionOwner } from "./state.js";
import { clientScope, DEFAULT_SERVER_URL } from "../runtime/client-scope.js";
import { accountRegistryUrl } from "../accounts/context.js";
import { browserBackendWatchEvents } from "./backend-watch.js";
import { browserProviderKeyMemory, followUser, type UserFollowingSession } from "./follow-user.js";

export type JazzSessionConfig = Omit<JazzClientConfig, "account"> &
  AccountManagerConfig & {
    initial?: "local-first" | BackendAuth;
    /**
     * On sign-out (or a switch to another user), wipe the account's local
     * storage instead of keeping it for that user's next sign-in. For shared
     * devices. Jazz first waits up to 5 seconds for pending writes to upload;
     * writes still unsynced then are lost.
     */
    clearOnSignOut?: boolean;
  };

/** A browser's Jazz session, which follows your auth's signed-in user. */
export type BrowserJazzSession = UserFollowingSession<JazzClient>;

/** Own one configured client and serialize its account lifecycle. */
export async function createJazzSession(config: JazzSessionConfig): Promise<BrowserJazzSession> {
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const { initial, clearOnSignOut, store: _store, ...clientConfig } = { ...config, serverUrl };
  const accounts = await createAccountManager({ ...config, serverUrl });
  const registry = accountRegistryUrl(serverUrl, clientScope({ ...config, serverUrl }));
  return createJazzSessionOwner({
    accounts,
    initial,
    // A memory-only client has no storage to wipe.
    clearOnSignOut: clearOnSignOut && (config.driver?.type ?? "persistent") === "persistent",
    openClient: (account) => createJazzClient({ ...clientConfig, account }),
    decorate: followUser({
      registry,
      events: browserBackendWatchEvents(`jazz-account:${registry}`),
      providerKeys: browserProviderKeyMemory(registry),
    }),
  });
}
