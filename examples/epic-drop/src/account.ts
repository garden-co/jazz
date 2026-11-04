import { createAccountManager, type DbConfig } from "jazz-tools";

// This application owns its managers. A shared browser scope also survives
// framework development remounts without founding a second account.
const managers = new Map<string, ReturnType<typeof createAccountManager>>();

export async function prepareAccountConfig(overrides: Partial<DbConfig> = {}): Promise<DbConfig> {
  // The app's backend serves Jazz on this origin (`/jazz`); tests may name an app.
  const appId = overrides.appId;
  const serverUrl = overrides.serverUrl ?? `${location.origin}/jazz`;
  if (overrides.account) return { appId, env: "dev", ...overrides, account: overrides.account };
  const scope = JSON.stringify([serverUrl, appId ?? null, overrides.env ?? "dev"]);
  let prepared = managers.get(scope);
  if (!prepared) {
    prepared = createAccountManager({ appId, serverUrl, env: overrides.env });
    managers.set(scope, prepared);
    void prepared.catch(() => {
      if (managers.get(scope) === prepared) managers.delete(scope);
    });
  }
  const accounts = await prepared;
  const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
  return { appId, serverUrl, env: "dev", ...overrides, account };
}
