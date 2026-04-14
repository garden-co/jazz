import { createDb, BrowserAuthSecretStore } from "jazz-tools";

// #region auth-localfirst-ts
export async function createLocalFirstDb() {
  const secret = await BrowserAuthSecretStore.getOrCreateSecret();

  return createDb({
    appId: "my-app",
    env: "dev",
    userBranch: "main",
    auth: { localFirstSecret: secret },
  });
}
// #endregion auth-localfirst-ts

// #region auth-jwt-ts
export async function createJwtDb() {
  return createDb({
    appId: "my-app",
    serverUrl: "http://127.0.0.1:4200",
    jwtToken: "<provider-jwt>",
  });
}
// #endregion auth-jwt-ts
