import type { SchemaSourceInput } from "../schema-source.js";
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
