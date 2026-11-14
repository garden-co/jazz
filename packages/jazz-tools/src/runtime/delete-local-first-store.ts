import type { AccountDbConfig } from "../accounts/context.js";
import { resolveDefaultPersistentDbName, type DbConfig } from "./db.js";
import { deleteBrowserStore } from "./native-runtime/browser-shared-worker-connection.js";

/**
 * @internal Delete the browser store a local-first client opened with
 * `config` would use, without opening it: no sign-in, token or schema is
 * needed, so a signed-out account's store can still be wiped (#5250).
 * `"held"`, deleting nothing, while a tab has it open.
 */
export function deleteLocalFirstStore(
  config: Omit<AccountDbConfig, "account"> & {
    accountId: string;
    accountRegistryAuthority: string;
  },
): Promise<"deleted" | "held"> {
  return deleteBrowserStore(
    config.runtimeSources,
    resolveDefaultPersistentDbName(config as DbConfig),
  );
}
