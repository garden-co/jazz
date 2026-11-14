import type { AccountDbConfig } from "../accounts/context.js";
import { accountRegistry } from "../accounts/enrollment.js";
import { createDb } from "../runtime/default-create-db.js";
import { deleteLocalFirstStore } from "../runtime/delete-local-first-store.js";
import { holdReplicaStore } from "../session/replica-ledger.js";
import { createRegisteredJazzClient, type JazzClient } from "./registered-jazz-client.js";

export type OpenJazzClientConfig = AccountDbConfig;

export type { JazzClient };
export { createJazzClientFromDb } from "./registered-jazz-client.js";

/**
 * Open (or share) a local-first browser client for an account. It holds the
 * account's store open against wipes, as a session's tab does, until its
 * `shutdown()`.
 */
export async function openJazzClient(config: AccountDbConfig): Promise<JazzClient> {
  const registry = accountRegistry(config.account);
  const { account, ...rest } = config;
  const release = await holdReplicaStore(registry, account.id, () =>
    deleteLocalFirstStore({ ...rest, accountId: account.id, accountRegistryAuthority: registry }),
  );
  let client: JazzClient;
  try {
    client = await createRegisteredJazzClient("web", config, createDb);
  } catch (error) {
    await release();
    throw error;
  }
  const shutdown = client.shutdown.bind(client);
  client.shutdown = async (options) => {
    try {
      await shutdown(options);
    } finally {
      await release();
    }
  };
  return client;
}
