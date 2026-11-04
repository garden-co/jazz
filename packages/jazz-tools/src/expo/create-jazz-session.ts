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
import { refuseClientAppId } from "../runtime/client-scope.js";

export type JazzSessionConfig = Omit<JazzClientConfig, "account" | "serverUrl" | "appId"> &
  Omit<AccountManagerConfig, "appId"> & {
    /**
     * Not for a client: the app id belongs to your backend's store
     * (`remote({ appId })`, `sqlite({ appId })`). A client is scoped by its
     * `serverUrl`; use `scope` to keep local-first users created under an
     * app id.
     */
    appId?: never;
    /**
     * Who the app starts as before your backend names a user: signed out
     * (the default, also `"signed-out"`), or a `"local-first"` guest, an
     * account on this device that can later link to your backend's user
     * (`linkWithBackend`). Your backend must accept guests
     * (`acceptLocalFirstAuth`).
     */
    initial?: "local-first" | "signed-out";
    /**
     * When a local-first guest is on this device and the user who signs in
     * has no Jazz account yet (they just signed up, or they are an existing
     * user signing in to Jazz for the first time), link the guest to them:
     * the account and everything the guest wrote become theirs. On by
     * default. Turn it off on shared devices, where the guest may be someone
     * else's: the session then leaves such a guest as it is, and your app
     * decides after its own sign-in, with `linkWithBackend()` (the guest's
     * data becomes the user's) or `loginWithBackend()` (an account of the
     * user's own; the guest stays on this device).
     */
    linkGuestOnSignUp?: boolean;
  };

/** Prepare the host account store once and own client replacement across authentication. */
export async function createJazzSession(
  config: JazzSessionConfig,
): Promise<UserFollowingSession<JazzClient>> {
  refuseClientAppId(config);
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
      linkGuest: config.linkGuestOnSignUp,
    }),
  });
}
