/** Browser-only account fixtures, safe to import from standalone remote pages. */
import { AccountAuthError } from "../../src/accounts/registry-client.js";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import type { AccountHandle } from "../../src/accounts/state.js";
import { createDb as createAccountDb } from "../../src/runtime/default-create-db.js";
import type { Db, DbConfig } from "../../src/runtime/db.js";

const implicitAccounts = new Map<string, Promise<AccountHandle>>();

/**
 * Acquire a genuine local-first account for a browser fixture.
 *
 * Reusing a key models reopening the same signed-in account; distinct keys
 * model distinct users without introducing copied credentials into public Db
 * configuration.
 */
export async function acquireBrowserTestAccount(config: {
  /** Optional, as for a browser Db: without one, the account is scoped by its endpoint. */
  appId?: string;
  serverUrl?: string;
  env?: string;
  key?: string;
}): Promise<AccountHandle> {
  const serverUrl = config.serverUrl ?? "http://127.0.0.1:1";
  const implicitKey = JSON.stringify([
    config.appId ?? null,
    serverUrl,
    config.env ?? "dev",
    config.key ?? "",
  ]);
  let issued = implicitAccounts.get(implicitKey);
  if (!issued) {
    issued = issueImplicitAccount(config.appId, serverUrl, config.env);
    implicitAccounts.set(implicitKey, issued);
  }
  try {
    return await issued;
  } catch (error) {
    implicitAccounts.delete(implicitKey);
    throw error;
  }
}

/** Open browser test Dbs with genuine manager-issued opaque handles. */
export async function createBrowserTestDb(
  config: Omit<DbConfig, "secret" | "jwtToken" | "adminSecret"> & {
    account?: AccountHandle;
    secret?: string;
    adminSecret?: string;
  },
): Promise<Db> {
  const { account: selectedAccount, secret, adminSecret, ...dbConfig } = config;
  if (adminSecret !== undefined)
    throw new Error("Browser test fixtures do not admit backend credentials");

  let account = selectedAccount;
  const serverUrl = config.serverUrl ?? "http://127.0.0.1:1";
  if (!account && secret === undefined) {
    account = await acquireBrowserTestAccount({ appId: config.appId, serverUrl, env: config.env });
  }
  if (!account) {
    let stored: string | null = null;
    const accounts = await createAccountManager({
      appId: config.appId,
      serverUrl,
      env: config.env,
      store: {
        async read() {
          return stored;
        },
        async update(transform) {
          stored = transform(stored);
        },
      },
    });
    account = accounts.restoreLocalFirst(secret!);
  }
  return await createAccountDb({ ...dbConfig, account });
}

async function issueImplicitAccount(
  appId: string | undefined,
  serverUrl: string,
  env: string | undefined,
): Promise<AccountHandle> {
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId,
    serverUrl,
    env,
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  return accounts.createLocalFirst();
}
