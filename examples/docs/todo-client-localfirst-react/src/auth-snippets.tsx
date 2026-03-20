import { useState } from "react";
import {
  getActiveSyntheticAuth,
  JazzProvider,
  SyntheticUserSwitcher,
  useLinkExternalIdentity,
} from "jazz-tools/react";

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

// #region auth-external-react
const externalAuthAppId = "my-app";
const externalAuthServerUrl = "http://127.0.0.1:4200";
const externalAuthProviderJwt = "<provider-jwt>";

export function ExternalAuthApp() {
  const [hasJwt, setHasJwt] = useState(false);
  const linkExternalIdentity = useLinkExternalIdentity({
    appId: externalAuthAppId,
    serverUrl: externalAuthServerUrl,
    defaultMode: "anonymous",
  });

  async function onSignedIn() {
    await linkExternalIdentity({ jwtToken: externalAuthProviderJwt });
    setHasJwt(true);
  }

  return (
    <JazzProvider
      key={hasJwt ? "jwt" : "local"}
      config={
        hasJwt
          ? {
              appId: externalAuthAppId,
              serverUrl: externalAuthServerUrl,
              jwtToken: externalAuthProviderJwt,
            }
          : {
              appId: externalAuthAppId,
              serverUrl: externalAuthServerUrl,
            }
      }
    >
      <button onClick={() => onSignedIn()}>Sign in</button>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-external-react

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
