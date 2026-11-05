import type { AccountDbConfig } from "../accounts/context.js";
import { createDb } from "../runtime/default-create-db.js";
import { createRegisteredJazzClient, type JazzClient } from "./registered-jazz-client.js";

export type OpenJazzClientConfig = AccountDbConfig;

export type { JazzClient };
export { createJazzClientFromDb } from "./registered-jazz-client.js";

/** Open (or share) a local-first browser client for an account. */
export function openJazzClient(config: AccountDbConfig): Promise<JazzClient> {
  return createRegisteredJazzClient("web", config, createDb);
}
