import { JazzProvider } from "jazz-tools/react";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";

function TodoApp() {
  return null;
}

// #region auth-anon-react
export function AnonymousAuthApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        env: "dev",
        userBranch: "main",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-react

// #region auth-self-signed-react
const selfSignedAppId = "my-app";
const selfSignedSeed = loadOrCreateIdentitySeed(selfSignedAppId);
const selfSignedJwtToken = mintSelfSignedToken(selfSignedSeed.seed, selfSignedAppId);

export function SelfSignedAuthApp() {
  return (
    <JazzProvider
      config={{
        appId: selfSignedAppId,
        serverUrl: "http://127.0.0.1:4200",
        jwtToken: selfSignedJwtToken,
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-self-signed-react

// #region auth-jwt-react
export function JwtAuthApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "http://127.0.0.1:4200",
        jwtToken: "<provider-jwt>",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-jwt-react

// #region auth-offline-react
export function OfflineOnlyAuthApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-offline-react
