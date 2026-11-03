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
    initial?: "local-first";
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
