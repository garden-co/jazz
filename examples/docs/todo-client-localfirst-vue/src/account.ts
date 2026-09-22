import { createAccountManager, accountRegistryUrl, type DbConfig } from "jazz-tools";

// This application owns its managers. A shared browser scope also survives
// framework development remounts without founding a second account.
const managers = new Map<string, ReturnType<typeof createAccountManager>>();

export async function prepareAccountConfig(overrides: Partial<DbConfig> = {}): Promise<DbConfig> {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
  const appId = overrides.appId ?? env?.VITE_JAZZ_APP_ID ?? env?.JAZZ_APP_ID;
  const serverUrl = overrides.serverUrl ?? env?.VITE_JAZZ_SERVER_URL ?? env?.JAZZ_SERVER_URL;
  if (!appId) throw new Error("Missing Jazz appId");
  if (overrides.account) return { appId, env: "dev", ...overrides, account: overrides.account };
  if (!serverUrl) throw new Error("Missing Jazz core serverUrl for account preparation");
  const scope = JSON.stringify([accountRegistryUrl(serverUrl, appId), overrides.env ?? "dev"]);
  let prepared = managers.get(scope);
  if (!prepared) {
    prepared = createAccountManager({
      appId,
      serverUrl,
      env: overrides.env,
      runtimeSources: overrides.runtimeSources,
    });
    managers.set(scope, prepared);
    void prepared.catch(() => {
      if (managers.get(scope) === prepared) managers.delete(scope);
    });
  }
  const accounts = await prepared;
  const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
  return { appId, serverUrl, env: "dev", ...overrides, account };
}
