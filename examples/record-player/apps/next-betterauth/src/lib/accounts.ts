import { accountRegistryUrl, createAccountManager } from "jazz-tools";

const managers = new Map<string, ReturnType<typeof createAccountManager>>();

export function prepareAccounts(appId: string, serverUrl: string) {
  const scope = JSON.stringify([accountRegistryUrl(serverUrl, appId), "dev"]);
  let prepared = managers.get(scope);
  if (!prepared) {
    prepared = createAccountManager({ appId, serverUrl, env: "dev" });
    managers.set(scope, prepared);
    void prepared.catch(() => {
      if (managers.get(scope) === prepared) managers.delete(scope);
    });
  }
  return prepared;
}
