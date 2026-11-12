import type { SignedOutDecision } from "../session/backend-watch.js";
import { createJazzSessionOwner } from "../session/state.js";
import {
  firstBackendAnswer,
  followUser,
  type UserFollowingSession,
} from "../session/follow-user.js";
import { nativeBackendWatchEvents } from "../session/native-backend-watch.js";
import { clientRegistryUrl } from "../accounts/account-manager-core.js";
import { createAccountManager, type AccountManagerConfig } from "./create-account-manager.js";
import {
  openJazzClient,
  type JazzClient,
  type OpenJazzClientConfig,
} from "../react-native/create-jazz-client.js";
import { refuseClientAppId } from "../runtime/client-scope.js";

export type JazzClientConfig = Omit<OpenJazzClientConfig, "account" | "serverUrl" | "appId"> &
  Omit<AccountManagerConfig, "appId"> & {
    /**
     * Not for a client: the app id belongs to your backend's store
     * (`remote({ appId })`, or the id a store keeps). A client is scoped by its
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
    /**
     * Called when your backend says no one is signed in any more (confirmed
     * by a second answer a moment later), before the session signs the
     * user out. Return `"keep"` to stay signed in for now (your app refreshed
     * its session itself, or redirects to sign-in first): the session asks
     * your backend again on the next focus, refusal or `reauthenticate()`.
     * Return `"sign-out"` (or nothing) to sign out. A hook that throws signs
     * out.
     */
    onSignedOut?: () => SignedOutDecision | void | Promise<SignedOutDecision | void>;
  };

/** Prepare the host account store once and own client replacement across authentication. */
export async function createJazzClient(
  config: JazzClientConfig,
): Promise<UserFollowingSession<JazzClient>> {
  refuseClientAppId(config);
  const { serverUrl } = config;
  // Jazz on this device only: no backend names a user, so the session opens
  // this device's local-first account and never asks one.
  if (serverUrl === null && config.initial !== undefined && config.initial !== "local-first") {
    throw new Error(
      'serverUrl: null has no backend to sign in through, so it always opens this device\'s local-first account: leave out initial or set it to "local-first"',
    );
  }
  // The hook is the session's; a Db keeps only cloneable configuration.
  const { onSignedOut, ...clientConfig } = config;
  const accounts = await createAccountManager(config);
  if (serverUrl === null) {
    return createJazzSessionOwner({
      accounts,
      initial: "local-first",
      openClient: (account) => openJazzClient({ ...clientConfig, serverUrl: undefined, account }),
      decorate: () => ({ reauthenticate: async () => {} }),
    });
  }
  const session = await createJazzSessionOwner({
    accounts,
    initial: config.initial,
    openClient: (account) => openJazzClient({ ...clientConfig, serverUrl, account }),
    decorate: followUser({
      registry: clientRegistryUrl(config),
      events: nativeBackendWatchEvents,
      linkGuest: config.linkGuestOnSignUp,
      onSignedOut,
    }),
  });
  await firstBackendAnswer(session);
  return session;
}
