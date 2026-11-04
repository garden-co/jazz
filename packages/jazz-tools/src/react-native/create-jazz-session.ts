import { createJazzSessionOwner } from "../session/state.js";
import { followUser, type UserFollowingSession } from "../session/follow-user.js";
import { nativeBackendWatchEvents } from "../session/native-backend-watch.js";
import { accountRegistryUrl } from "../accounts/context.js";
import { clientScope } from "../runtime/client-scope.js";
import { createAccountManager, type AccountManagerConfig } from "./create-account-manager.js";
import {
  createJazzClient,
  type JazzClient,
  type JazzClientConfig,
} from "../react-native/create-jazz-client.js";

export type JazzSessionConfig = Omit<JazzClientConfig, "account"> &
  AccountManagerConfig & {
    /**
     * Who the app starts as before your backend names a user: signed out
     * (the default, also `"signed-out"`), or a `"local-first"` guest, an
     * account on this device that works offline and can later link to your
     * backend's user (`linkWithBackend`).
     */
    initial?: "local-first" | "signed-out";
  };

/** Prepare the host account store once and own client replacement across authentication. */
export async function createJazzSession(
  config: JazzSessionConfig,
): Promise<UserFollowingSession<JazzClient>> {
  const accounts = await createAccountManager(config);
  return createJazzSessionOwner({
    accounts,
    initial: config.initial,
    openClient: (account) => createJazzClient({ ...config, account }),
    decorate: followUser({
      registry: accountRegistryUrl(config.serverUrl, clientScope(config)),
      events: nativeBackendWatchEvents,
    }),
  });
}
