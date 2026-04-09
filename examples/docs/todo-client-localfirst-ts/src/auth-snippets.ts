import { createDb, loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";

// #region auth-anon-ts
export async function createAnonymousDb() {
  return createDb({
    appId: "my-app",
    env: "dev",
    userBranch: "main",
  });
}
// #endregion auth-anon-ts

// #region auth-self-signed-ts
export async function createSelfSignedDb() {
  const appId = "my-app";
  const seed = loadOrCreateIdentitySeed(appId);
  const jwtToken = mintSelfSignedToken(seed.seed, appId);

  return createDb({
    appId,
    serverUrl: "http://127.0.0.1:4200",
    jwtToken,
  });
}
// #endregion auth-self-signed-ts

// #region auth-jwt-ts
export async function createJwtDb() {
  return createDb({
    appId: "my-app",
    serverUrl: "http://127.0.0.1:4200",
    jwtToken: "<provider-jwt>",
  });
}
// #endregion auth-jwt-ts

// #region auth-offline-ts
export async function createOfflineDb() {
  return createDb({ appId: "my-app" });
}
// #endregion auth-offline-ts
