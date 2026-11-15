/**
 * @internal The session core React Native's and Expo's `createJazzClient` (and
 * `<JazzProvider>`) share. The hosts differ only in how they keep accounts
 * (React Native takes an account `store`, Expo keeps them in its secure store
 * under a `profile`), so each passes its account manager and its option list.
 */

import { accountForgotten } from "../accounts/persistence.js";
import type { AppBearer } from "../accounts/enrollment.js";
import type { SignedOutDecision } from "../session/backend-watch.js";
import { createJazzSessionOwner } from "../session/state.js";
import {
  firstBackendAnswer,
  followUser,
  type UserFollowingSession,
} from "../session/follow-user.js";
import { nativeBackendWatchEvents } from "../session/native-backend-watch.js";
import { clientRegistryUrl } from "../accounts/account-manager-core.js";
import {
  openJazzClient,
  type JazzClient,
  type OpenJazzClientConfig,
} from "./create-jazz-client.js";
import {
  refuseInvalidSessionConfig,
  refuseSignedOutWithoutBackend,
} from "../session/config-options.js";
import { refuseClientAppId } from "../runtime/client-scope.js";
import { bearerFetch } from "../runtime/credentialed-transport.js";
import { coreInitial } from "../session/jazz-session.js";

/** What a host's account manager reads from the session config. */
export interface NativeAccountConfig {
  appId?: string;
  scope?: string;
  serverUrl: string | null;
  bearer?: AppBearer;
}

/** A native host's `createJazzClient` config, given its account manager's config. */
export type NativeJazzClientConfig<AccountConfig extends NativeAccountConfig> = Omit<
  OpenJazzClientConfig,
  "account" | "serverUrl" | "appId"
> &
  Omit<AccountConfig, "appId"> & {
    /**
     * Not for a client: the app id belongs to your backend's store
     * (`remote({ appId })`, or the id a store keeps). A client is scoped by its
     * `serverUrl`; use `scope` to keep local-first users created under an
     * app id.
     */
    appId?: never;
    /**
     * Who the app starts as before your backend names a user: signed out
     * (the default, also `"signed-out"`), or a `"device"` account: an
     * account whose key lives on this device, which is how you let people
     * start as guests, and which can later link to your backend's user
     * (`linkDeviceAccount()`). Your backend must accept device accounts
     * (`acceptDeviceAccounts`).
     */
    initial?: "device" | "signed-out";
    /**
     * When a device account is on this device and the user who signs in has
     * no Jazz account yet (they just signed up, or they are an existing user
     * signing in to Jazz for the first time), link the device account to
     * them: the account and everything it wrote become theirs. On by
     * default. Turn it off on shared devices, where the device account may be
     * someone else's guest: the session then leaves it as it is, and your
     * app decides after its own sign-in, with `linkDeviceAccount()` (its data
     * becomes the user's) or `refresh()` (an account of the user's own; the
     * device account stays on this device).
     */
    linkDeviceAccountOnSignUp?: boolean;
    /**
     * Called when your backend says no one is signed in any more (confirmed
     * by a second answer a moment later), before the session signs the
     * user out. Return `"keep"` to stay signed in for now (your app refreshed
     * its session itself, or redirects to sign-in first): the session asks
     * your backend again on the next focus, refusal or `refresh()`.
     * Return `"sign-out"` (or nothing) to sign out. A hook that throws signs
     * out.
     */
    onSignedOut?: () => SignedOutDecision | void | Promise<SignedOutDecision | void>;
  };

/** The account manager a host prepares for its session. */
type SessionAccounts = Parameters<typeof createJazzSessionOwner<JazzClient>>[0]["accounts"];

/** Open a native session with the host's account manager and option list. */
export async function openNativeSessionCoreWith<
  C extends NativeJazzClientConfig<NativeAccountConfig>,
>(
  config: C,
  createAccountManager: (config: C) => Promise<SessionAccounts>,
  options: Readonly<Record<string, true>>,
): Promise<UserFollowingSession<JazzClient>> {
  if (config && typeof config === "object") refuseClientAppId(config);
  refuseInvalidSessionConfig(config, options);
  const { serverUrl } = config;
  const initial = coreInitial(config.initial);
  // Jazz on this device only: no backend names a user, so the session opens
  // this device's device account and never asks one.
  if (serverUrl === null) refuseSignedOutWithoutBackend(initial);
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
    initial,
    openClient: (account) => openJazzClient({ ...clientConfig, serverUrl, account }),
    decorate: followUser({
      registry: clientRegistryUrl(config),
      events: nativeBackendWatchEvents,
      linkGuest: config.linkDeviceAccountOnSignUp,
      forgotten: (account) => accountForgotten(accounts, account.id),
      onSignedOut,
      // Ask "who is signed in" with the app's bearer, as sign-in does: a
      // backend that authenticates by bearer, not cookie, never answers otherwise.
      ...(config.bearer ? { fetch: bearerFetch(config.bearer) } : {}),
    }),
  });
  await firstBackendAnswer(session);
  return session;
}
