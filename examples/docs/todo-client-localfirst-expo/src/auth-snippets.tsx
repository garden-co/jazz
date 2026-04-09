import { JazzProvider } from "jazz-tools/react-native";
import { loadOrCreateIdentitySeed, mintSelfSignedToken } from "jazz-tools";

function TodoApp() {
  return null;
}

// #region auth-anon-expo
export function AnonymousAuthExpoApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "http://127.0.0.1:4200",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-expo

// #region auth-self-signed-expo
const selfSignedExpoAppId = "my-app";
const selfSignedExpoSeed = loadOrCreateIdentitySeed(selfSignedExpoAppId);
const selfSignedExpoJwtToken = mintSelfSignedToken(selfSignedExpoSeed.seed, selfSignedExpoAppId);

export function SelfSignedAuthExpoApp() {
  return (
    <JazzProvider
      config={{
        appId: selfSignedExpoAppId,
        serverUrl: "http://127.0.0.1:4200",
        jwtToken: selfSignedExpoJwtToken,
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-self-signed-expo

// #region auth-offline-expo
export function OfflineOnlyAuthExpoApp() {
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
// #endregion auth-offline-expo
