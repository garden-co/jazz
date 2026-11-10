import { refuseStoreAppId } from "./sqlite.js";
import { randomUUID } from "node:crypto";
import type { JazzCoreProvider } from "./core-provider.js";
import { openInProcessCore } from "./in-process.js";
/** Open a disposable in-memory core in this process. */
export function memory(options: { appId?: never } = {}): JazzCoreProvider {
  refuseStoreAppId("memory", options);
  return {
    open: async () =>
      openInProcessCore({
        appId: randomUUID(),
        inMemory: true,
      }),
  };
}
