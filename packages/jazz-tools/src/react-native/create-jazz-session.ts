import type { BackendAuth } from "../accounts/enrollment.js";
import { createJazzSessionOwner } from "../session/state.js";
import { followUser, type UserFollowingSession } from "../session/follow-user.js";
import { nativeBackendWatchEvents } from "../session/native-backend-watch.js";
import { clientRegistryUrl } from "../accounts/account-manager-core.js";
import { createAccountManager, type AccountManagerConfig } from "./create-account-manager.js";
import {
  createJazzClient,
  type JazzClient,
  type JazzClientConfig,
} from "../react-native/create-jazz-client.js";

export type JazzSessionConfig = Omit<JazzClientConfig, "account" | "serverUrl"> &
  AccountManagerConfig & {
    /**
     * Who the app starts as before your backend names a user: signed out
     * (the default, also `"signed-out"`), or a `"local-first"` guest, an
     * account on this device that works offline and can later link to your
     * backend's user (`linkWithBackend`).
     */
    initial?: "local-first" | "signed-out" | BackendAuth;
  };

/** Prepare the host account store once and own client replacement across authentication. */
export async function createJazzSession(
  config: JazzSessionConfig,
): Promise<UserFollowingSession<JazzClient>> {
  const accounts = await createAccountManager(config);
  const { serverUrl } = config;
  if (serverUrl === null) {
    // Jazz on this device only: no backend names a user, so the session
    // opens this device's local-first account and never asks one.
    if (config.initial !== undefined && config.initial !== "local-first") {
      throw new Error(
        'serverUrl: null has no backend to sign in through, so it always opens this device\'s local-first account: leave out initial or set it to "local-first"',
      );
    }
    return createJazzSessionOwner({
      accounts,
      initial: "local-first",
      openClient: (account) => createJazzClient({ ...config, serverUrl: undefined, account }),
      decorate: () => ({ reauthenticate: async () => {} }),
    });
  }
  return createJazzSessionOwner({
    accounts,
    initial: config.initial,
    openClient: (account) => createJazzClient({ ...config, serverUrl, account }),
    decorate: followUser({
      registry: clientRegistryUrl(config),
      events: nativeBackendWatchEvents,
    }),
  });
}
