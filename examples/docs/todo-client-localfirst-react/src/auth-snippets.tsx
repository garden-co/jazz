import { getActiveSyntheticAuth, JazzProvider, SyntheticUserSwitcher } from "jazz-tools/react";

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

// #region auth-anon-token-react
export function AnonymousAuthWithTokenApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        localAuthMode: "anonymous",
        localAuthToken: "device-token-123",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-token-react

// #region auth-demo-react
const demoAuthAppId = "my-app";
const demoAuthActive = getActiveSyntheticAuth(demoAuthAppId, { defaultMode: "demo" });

export function DemoAuthApp() {
  return (
    <>
      <SyntheticUserSwitcher appId={demoAuthAppId} defaultMode="demo" />
      <JazzProvider
        config={{
          appId: demoAuthAppId,
          serverUrl: "http://127.0.0.1:4200",
          localAuthMode: demoAuthActive.localAuthMode,
          localAuthToken: demoAuthActive.localAuthToken,
        }}
      >
        <TodoApp />
      </JazzProvider>
    </>
  );
}
// #endregion auth-demo-react

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
