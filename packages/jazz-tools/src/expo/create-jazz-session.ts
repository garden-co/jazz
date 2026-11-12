import { accountForgotten } from "../accounts/persistence.js";
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
import { refuseInvalidSessionConfig } from "../session/config-options.js";
import { refuseClientAppId } from "../runtime/client-scope.js";
import {
  coreInitial,
  createStartedJazzSession,
  type JazzSession,
} from "../session/jazz-session.js";

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

/** Every option `createJazzClient` takes on this host, checked against its config type. */
const NATIVE_SESSION_OPTIONS = {
  serverUrl: true,
  initial: true,
  linkDeviceAccountOnSignUp: true,
  onSignedOut: true,
  scope: true,
  driver: true,
  env: true,
  runtimeSources: true,
  dbName: true,
  initialSyncFlushEvery: true,
  logLevel: true,
  telemetryCollectorUrl: true,
  devMode: true,
  slowQueryThresholdMs: true,
  bearer: true,
  nativeRelay: true,
  profile: true,
} as const satisfies Record<Exclude<keyof JazzClientConfig, "appId" | "backendSecret">, true>;

/**
 * Open a Jazz session on this device: who is signed in, and the client that
 * reads and writes as them. Resolves once it started; rejects if it can't.
 * Your backend decides who is signed in; call `session.refresh()` after your
 * own sign-in or sign-out.
 */
export function createJazzClient(config: JazzClientConfig): Promise<JazzSession<JazzClient>> {
  return createStartedJazzSession(config, openNativeSessionCore);
}

/** @internal The session core `createJazzClient` and `<JazzProvider>` own. */
export async function openNativeSessionCore(
  config: JazzClientConfig,
): Promise<UserFollowingSession<JazzClient>> {
  if (config && typeof config === "object") refuseClientAppId(config);
  refuseInvalidSessionConfig(config, NATIVE_SESSION_OPTIONS);
  const { serverUrl } = config;
  const initial = coreInitial(config.initial);
  // Jazz on this device only: no backend names a user, so the session opens
  // this device's device account and never asks one.
  if (serverUrl === null && initial === "signed-out") {
    throw new Error(
      'serverUrl: null has no backend to sign in through, so it always opens this device\'s device account: leave out initial or set it to "device"',
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
    initial,
    openClient: (account) => openJazzClient({ ...clientConfig, serverUrl, account }),
    decorate: followUser({
      registry: clientRegistryUrl(config),
      events: nativeBackendWatchEvents,
      linkGuest: config.linkDeviceAccountOnSignUp,
      forgotten: (account) => accountForgotten(accounts, account.id),
      onSignedOut,
    }),
  });
  await firstBackendAnswer(session);
  return session;
}
