/**
 * Builds the end-to-end encryption runtime of an app on a backend's engine
 * (see `backendNative`), over the private store the app supplied. It never
 * imports jazz-napi itself, so a Cloudflare Worker's bundle stays free of it.
 */
import { backendNative } from "../backend/backend-native.js";
import type { SchemaSourceInput } from "../schema-source.js";
import { buildEncryption, compiledBindings, declaresEncryption } from "./build.js";
import { EncryptionError } from "./index.js";
import type { PrivateStore } from "./private-store.js";

export { declaresEncryption };

/** What an app passes to turn its encryption on: where the device's private keys live. */
export interface NodeEncryptionConfig {
  /** The private store (see {@link PrivateStore}) holding this device's keys. */
  store: PrivateStore;
}

/**
 * The native runtime for `app` and `accountId`, ready, or `undefined` for an
 * app that declares no encryption. An app that declares it needs the store:
 * without one this throws `EncryptionStoreRequiredError`.
 */
export async function buildNodeEncryption(options: {
  app: SchemaSourceInput;
  appId: string;
  environment: string;
  accountId: string;
  /** The device account's secret (`jazz-auth-v1:...`); it is the account's recovery secret. */
  deviceAccountSecret?: string;
  encryption: NodeEncryptionConfig | undefined;
}): Promise<{ runtime: unknown; store: PrivateStore } | undefined> {
  if (!declaresEncryption(options.app)) return undefined;
  const host = backendNative().EncryptionRuntime;
  if (!host) {
    throw new EncryptionError(
      "unsupported",
      "A backend client of an app that declares end-to-end encryption is not supported in a Cloudflare Worker yet",
    );
  }
  return buildEncryption({
    host,
    app: options.app,
    appId: options.appId,
    environment: options.environment,
    accountId: options.accountId,
    deviceAccountSecret: options.deviceAccountSecret,
    store: options.encryption?.store,
  });
}

/**
 * What a backend opens with for an app that declares encryption: the
 * bindings' compiled app, from which it builds keyless hooks. `undefined`
 * for an app that declares none.
 */
export function keylessBackend(
  app: SchemaSourceInput,
  appId: string,
  environment: string,
): { bindings: string; appId: string; environment: string } | undefined {
  if (!declaresEncryption(app)) return undefined;
  return { bindings: compiledBindings(app), appId, environment };
}
