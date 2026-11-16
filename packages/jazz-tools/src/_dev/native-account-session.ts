/** Installed-device support, using exactly the production account admission. */
import { resolveAccountRuntimeConfig, type AccountDbConfig } from "../accounts/context.js";
import { accountRegistry, onAccountInvalidated, AccountAuthError } from "../accounts/enrollment.js";
import {
  beginNativeAccountSession,
  attachNativeAccountSchema,
  resolveNativeSession,
} from "../react-native/account-session.js";
import type { NativeForegroundFactory } from "../react-native/native-foreground-db.js";

/**
 * Own one native account lease for raw and public foreground acceptance tests.
 * Public clients use the same AccountHandle alongside this capability, and may
 * close independently. Close the lease only after every foreground is closed.
 * The generated Rust schema is passed verbatim; no second schema ABI is added.
 */
export async function createNativeAccountTestSession(
  config: AccountDbConfig,
  schemaSource: string,
) {
  const captured = { ...config };
  const { account } = captured;
  let closed = false;
  let factory: NativeForegroundFactory | undefined;
  let capability: Uint8Array | undefined;
  let unsubscribe = () => {};
  const close = () => {
    if (closed) return;
    closed = true;
    unsubscribe();
    if (capability) factory!.releaseAccountSession!(capability);
    capability = undefined;
  };
  unsubscribe = onAccountInvalidated(account, close);
  try {
    const resolved = await resolveAccountRuntimeConfig(captured);
    const native = await import("jazz-rn/relay");
    // Module loading is asynchronous too. Recheck handle liveness before any
    // native admission; a logout while loading must not create a new lease.
    accountRegistry(account);
    if (closed) throw new AccountAuthError("account_logged_out");
    factory = native.installNativeForegroundRuntime();
    const pending = beginNativeAccountSession(factory, resolved, resolveNativeSession(resolved));
    capability = attachNativeAccountSchema(factory, pending, schemaSource);
    if (!(capability instanceof Uint8Array) || capability.byteLength !== 32) {
      throw new Error("Native account admission returned an invalid capability");
    }
    return Object.freeze({ account, capability: capability.slice(), close });
  } catch (error) {
    close();
    throw error;
  }
}

/** The account-handle client the public session API opens internally, for device acceptance only. */
export { openJazzClient, type OpenJazzClientConfig } from "../react-native/create-jazz-client.js";

/**
 * Expo's account manager, for the device acceptance harness only: apps sign
 * in through the session (`createJazzClient` / `<JazzProvider>` from
 * `jazz-tools/expo`). Loaded lazily so this module stays free of Expo's
 * native packages until a harness asks for it.
 */
export async function createExpoAccountManager(
  config: import("../expo/create-account-manager.js").AccountManagerConfig,
) {
  const { createAccountManager } = await import("../expo/create-account-manager.js");
  return createAccountManager(config);
}
