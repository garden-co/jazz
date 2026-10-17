import { accountRegistryUrl, createAccountManager } from "jazz-tools";
import { jazzEnv } from "./jazz-env";

const managers = new Map<string, ReturnType<typeof createAccountManager>>();

export function prepareAccounts(appId: string, serverUrl: string) {
  const scope = JSON.stringify([accountRegistryUrl(serverUrl, appId), jazzEnv]);
  let prepared = managers.get(scope);
  if (!prepared) {
    prepared = createAccountManager({ appId, serverUrl, env: jazzEnv });
    managers.set(scope, prepared);
    void prepared.catch(() => {
      if (managers.get(scope) === prepared) managers.delete(scope);
    });
  }
  return prepared;
}
