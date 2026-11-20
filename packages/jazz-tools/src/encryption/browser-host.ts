/**
 * End-to-end encryption in the browser (and in the other places the
 * WebAssembly client runs: Node, Deno, Bun and CLIs with a bearer). Each tab
 * of the app builds its own runtime over the device's private store, which
 * every tab shares, and decrypts and seals itself.
 */
import type { SchemaSourceInput } from "../schema-source.js";
import { buildEncryption, declaresEncryption, type EncryptionRuntimeClass } from "./build.js";
import { browserPrivateStore } from "./browser-store.js";
import type { PrivateStore } from "./private-store.js";
import { accountAppId } from "../accounts/local-first.js";

/** What an app that declares encryption passes to its client. */
export interface BrowserEncryptionConfig {
  /**
   * The app (the `app` of `jazz.generated.ts`): its declarations say which
   * columns are encrypted, so the keys are ready before the first query.
   */
  app: SchemaSourceInput;
  /**
   * Where this device keeps its keys. By default the browser's own store:
   * IndexedDB, protected by a key scripts can use but never export. Pass
   * your own to keep them elsewhere (see the docs).
   */
  store?: PrivateStore;
}

/** The runtime of an account in a tab, ready; `undefined` for an app that declares no encryption. */
export async function buildBrowserEncryption(options: {
  host: EncryptionRuntimeClass;
  config: BrowserEncryptionConfig;
  appId: string;
  environment: string;
  accountId: string;
  /** The device account's secret, which is its encryption recovery secret. */
  deviceAccountSecret?: string;
}) {
  if (!declaresEncryption(options.config.app)) return undefined;
  return buildEncryption({
    host: options.host,
    app: options.config.app,
    appId: options.appId,
    environment: options.environment,
    accountId: options.accountId,
    deviceAccountSecret: options.deviceAccountSecret,
    store: options.config.store ?? browserPrivateStore(),
  });
}

/**
 * The private store scope of `accountId`'s keys in an app: the account domain
 * `json([appId, env, account])` (E2EE_PROTOCOL.md §0), as the runtime names
 * it, with the app id in the runtime's form (a name becomes its UUID).
 */
export function encryptionStoreScope(
  appId: string,
  environment: string,
  accountId: string,
): string {
  return JSON.stringify([accountAppId(appId), environment, accountId]);
}

/**
 * Wipe `accountId`'s encryption keys from this device's private store (the
 * app's, or the browser's own): the device's keys, the space keys it holds
 * and its floors. Nothing for an app that declares no encryption. The device
 * cannot read the account's encrypted rows again until it is approved anew
 * or recovered; a runtime still open on the scope finds it wiped.
 */
export async function wipeBrowserEncryption(options: {
  config: BrowserEncryptionConfig;
  appId: string;
  environment: string;
  accountId: string;
}): Promise<void> {
  if (!declaresEncryption(options.config.app)) return;
  const store = options.config.store ?? browserPrivateStore();
  await store.delete(encryptionStoreScope(options.appId, options.environment, options.accountId));
}
