/** Browser-only account fixtures, safe to import from standalone remote pages. */
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import type { AccountHandle } from "../../src/accounts/state.js";
import { createDb as createAccountDb } from "../../src/runtime/default-create-db.js";
import type { Db, DbConfig } from "../../src/runtime/db.js";

const implicitAccounts = new Map<string, AccountHandle>();

/** Open browser test Dbs with genuine manager-issued opaque handles. */
export async function createBrowserTestDb(
  config: Omit<DbConfig, "secret" | "jwtToken" | "adminSecret"> & {
    account?: AccountHandle;
    secret?: string;
    jwtToken?: string;
    /** Fixture-only explicit registry enrollment before obtaining a login handle. */
    registerJwt?: boolean;
    adminSecret?: string;
  },
): Promise<Db> {
  const {
    account: selectedAccount,
    secret,
    jwtToken,
    registerJwt,
    adminSecret,
    ...dbConfig
  } = config;
  if (adminSecret !== undefined)
    throw new Error("Browser test fixtures do not admit backend credentials");
  if (secret !== undefined && jwtToken !== undefined)
    throw new Error("Browser test fixtures select either a local-first secret or a JWT");

  let account = selectedAccount;
  if (!account) {
    const serverUrl = config.serverUrl ?? "http://127.0.0.1:1";
    const implicitKey = JSON.stringify([config.appId, serverUrl, config.env ?? "dev"]);
    account =
      secret === undefined && jwtToken === undefined
        ? implicitAccounts.get(implicitKey)
        : undefined;
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
      if (secret !== undefined) {
        account = accounts.restoreLocalFirst(secret);
      } else if (jwtToken !== undefined) {
        if (registerJwt) await accounts.registerJWT(jwtToken);
        account = await accounts.loginJWT(jwtToken);
      } else {
        account = accounts.createLocalFirst();
        implicitAccounts.set(implicitKey, account);
      }
    }
  }
  return await createAccountDb({ ...dbConfig, account });
}
