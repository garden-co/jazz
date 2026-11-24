/**
 * End-to-end encryption in the browser (and in the other places the
 * WebAssembly client runs: Node, Deno, Bun and CLIs with a bearer). Each tab
 * of the app builds its own runtime over the device's private store, which
 * every tab shares, and decrypts and seals itself.
 */
import type { SchemaSourceInput } from "../schema-source.js";
import { buildEncryption, type EncryptionRuntimeClass } from "./build.js";
import { browserPrivateStore } from "./browser-store.js";
import { declaresEncryption } from "./plain-schema.js";
import type { PrivateStore } from "./private-store.js";

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
