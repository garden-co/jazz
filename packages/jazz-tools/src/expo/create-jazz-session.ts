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
  const accounts = await createAccountManager(config);
  return createJazzSessionOwner({
    accounts,
    initial: config.initial,
    openClient: (account) => createJazzClient({ ...config, account }),
    decorate: followUser({
      registry: accountRegistryUrl(config.serverUrl, clientScope(config)),
      events: nativeBackendWatchEvents,
      linkGuest: config.linkGuestOnSignUp,
    }),
  });
}
