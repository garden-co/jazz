import { createJazz, reatomJazzLocalFirstAuth } from "./lib/reatom-jazz";
import { BrowserAuthSecretStore, DbConfig } from "jazz-tools";

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

export function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    secret,
    ...overrides,
  };
}

export const localFirstAuth = reatomJazzLocalFirstAuth(BrowserAuthSecretStore.getDefault());

export const jazz = createJazz(() => {
  const secret = localFirstAuth();
  return defaultConfig(secret);
});
