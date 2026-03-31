import {
  createDb,
  createSyntheticUserSwitcher,
  getActiveSyntheticAuth,
  type JazzClient,
} from "jazz-tools";

// #region auth-anon-ts
export async function createAnonymousDb() {
  return createDb({
    appId: "my-app",
    env: "dev",
    userBranch: "main",
  });
}
// #endregion auth-anon-ts

// #region auth-anon-token-ts
export async function createAnonymousDbWithToken() {
  return createDb({
    appId: "my-app",
    localAuthMode: "anonymous",
    localAuthToken: "device-token-123",
  });
}
// #endregion auth-anon-token-ts

// #region auth-demo-ts
export async function createDemoDb(container: HTMLElement) {
  const appId = "my-app";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  createSyntheticUserSwitcher({
    appId,
    container,
    defaultMode: "demo",
  });

  return createDb({
    appId,
    serverUrl: "http://127.0.0.1:4200",
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
  });
}
// #endregion auth-demo-ts

// #region auth-external-ts
export async function upgradeIdentity(
  client: Pick<JazzClient, "linkExternalIdentity">,
  providerJwt: string,
) {
  return client.linkExternalIdentity({
    jwtToken: providerJwt,
    localAuthMode: "anonymous",
    localAuthToken: "device-token-123",
  });
}
// #endregion auth-external-ts

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
