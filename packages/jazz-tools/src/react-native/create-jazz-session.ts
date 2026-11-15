import { createAccountManager, type AccountManagerConfig } from "./create-account-manager.js";
import type { JazzClient } from "./create-jazz-client.js";
import { openNativeSessionCoreWith, type NativeJazzClientConfig } from "./native-session-core.js";
import type { UserFollowingSession } from "../session/follow-user.js";
import { createStartedJazzSession, type JazzSession } from "../session/jazz-session.js";

export type JazzClientConfig = NativeJazzClientConfig<AccountManagerConfig>;

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
  store: true,
  bearer: true,
  nativeRelay: true,
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
export function openNativeSessionCore(
  config: JazzClientConfig,
): Promise<UserFollowingSession<JazzClient>> {
  return openNativeSessionCoreWith(config, createAccountManager, NATIVE_SESSION_OPTIONS);
}
